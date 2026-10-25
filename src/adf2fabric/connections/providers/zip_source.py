"""Safe handling of an uploaded workspace / repository export (ZIP).

The archive is untrusted. This module only ever COPIES bytes of ``.json`` entries into a scratch folder; it never executes,
imports, or interprets anything it extracts, and it refuses archives that try to write outside the scratch folder or to expand
without bound. Rejected: absolute paths, drive letters, ``..`` segments, NUL bytes, symlinks, encrypted entries, case-colliding
names, paths too long for Windows, and archives that exceed the size, file-count or compression-ratio limits (checked from the
declared sizes AND again while streaming, because a declared size can lie).
"""

from __future__ import annotations

import re
import stat
import zipfile
import zlib
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from adf2fabric.connections.redaction import redact


@dataclass(frozen=True)
class ZipLimits:
    max_archive_bytes: int = 200 * 1024 * 1024
    max_total_bytes: int = 500 * 1024 * 1024  # uncompressed JSON
    max_file_bytes: int = 50 * 1024 * 1024
    max_files: int = 20_000
    max_ratio: int = 200  # uncompressed / compressed, per entry above 1 MiB
    max_json_depth: int = 100  # ADF exports nest a few dozen levels at most; a deeper file would exhaust the parser's recursion
    max_path: int = 230  # leaves room under Windows' MAX_PATH once the scratch root is added


def json_depth(data: bytes, limit: int) -> int:
    """Deepest bracket nesting of JSON text (string contents ignored); stops counting once it passes ``limit``."""
    depth = deepest = 0
    in_str = esc = False
    for b in data:
        if in_str:
            if esc:
                esc = False
            elif b == 0x5C:
                esc = True
            elif b == 0x22:
                in_str = False
        elif b == 0x22:
            in_str = True
        elif b in (0x5B, 0x7B):
            depth += 1
            if depth > deepest:
                deepest = depth
                if deepest > limit:
                    return deepest
        elif b in (0x5D, 0x7D):
            depth -= 1
    return deepest


class ZipRejected(Exception):
    """The archive cannot be used. The message is safe to show and says why."""

    def __init__(self, message: str):
        super().__init__(message)


@dataclass
class ExtractReport:
    extracted: int = 0
    ignored: int = 0  # non-JSON entries (scripts, binaries, ...) deliberately not extracted
    uncompressed_bytes: int = 0
    ignored_examples: list[str] = field(default_factory=list)


_DRIVE = re.compile(r"^[A-Za-z]:")
_ILLEGAL = re.compile(r'[<>:"|?*\x00-\x1f]')
_RESERVED = re.compile(r"(?i)^(con|prn|aux|nul|com[1-9]|lpt[1-9])(\..*)?$")


def _safe_relative(name: str) -> PurePosixPath:
    raw = name.replace("\\", "/")
    if "\x00" in raw:
        raise ZipRejected("The archive contains a file name with a NUL character.")
    if raw.startswith("/") or _DRIVE.match(raw):
        raise ZipRejected(f"The archive contains an absolute path ('{redact(raw)[:80]}'); refusing to extract it.")
    parts = [p for p in raw.split("/") if p not in ("", ".")]
    if not parts:
        raise ZipRejected("The archive contains an empty file name.")
    if any(p == ".." for p in parts):
        raise ZipRejected(f"The archive tries to write outside its folder ('{redact(raw)[:80]}'); refusing to extract it.")
    if any(_ILLEGAL.search(p) or p.endswith((".", " ")) for p in parts):  # same answer on every OS; Windows cannot create these and used to fail midway with an OS error
        raise ZipRejected(f"The archive has a file name that cannot be created on Windows ('{redact(raw)[:80]}'): it uses one of < > : \" | ? *, a control character, or ends in a dot or space.")
    if any(_RESERVED.match(p) for p in parts):
        raise ZipRejected(f"The archive uses a reserved file name ('{redact(raw)[:80]}').")
    return PurePosixPath(*parts)


def extract_json(zip_path: Path, dest: Path, limits: ZipLimits = ZipLimits()) -> ExtractReport:
    """Validate the whole archive first, then write its ``.json`` entries under ``dest``. Raises :class:`ZipRejected`."""
    if zip_path.stat().st_size > limits.max_archive_bytes:
        raise ZipRejected(f"The archive is larger than {limits.max_archive_bytes // (1024 * 1024)} MB.")
    try:
        zf = zipfile.ZipFile(zip_path)
    except zipfile.BadZipFile:
        raise ZipRejected("The file is not a valid ZIP archive.") from None
    except OSError:
        raise ZipRejected("The archive could not be read.") from None
    report = ExtractReport()
    with zf:
        infos = [i for i in zf.infolist()]
        if len(infos) > limits.max_files:
            raise ZipRejected(f"The archive has more than {limits.max_files} entries.")
        seen: set[str] = set()
        plan: list[tuple[zipfile.ZipInfo, PurePosixPath]] = []
        total = 0
        for info in infos:
            mode = info.external_attr >> 16
            if stat.S_ISLNK(mode):
                raise ZipRejected(f"The archive contains a symbolic link ('{redact(info.filename)[:80]}'); refusing to extract it.")
            if info.flag_bits & 0x1:
                raise ZipRejected("The archive contains encrypted entries, which cannot be inspected.")
            rel = _safe_relative(info.filename)
            if info.is_dir():
                continue
            if rel.suffix.lower() != ".json":
                report.ignored += 1
                if len(report.ignored_examples) < 5:
                    report.ignored_examples.append(rel.as_posix())
                continue
            key = rel.as_posix().lower()
            if key in seen:
                raise ZipRejected(f"The archive contains '{rel.as_posix()}' more than once (names differ only by case).")
            seen.add(key)
            if len(str(dest)) + len(rel.as_posix()) + 1 > limits.max_path:
                raise ZipRejected(f"A path in the archive is too long to extract on Windows ('{rel.as_posix()[:60]}...').")
            if info.file_size > limits.max_file_bytes:
                raise ZipRejected(f"'{rel.as_posix()[:60]}' is larger than {limits.max_file_bytes // (1024 * 1024)} MB.")
            if info.file_size > 1024 * 1024 and info.file_size / max(1, info.compress_size) > limits.max_ratio:
                raise ZipRejected(f"'{rel.as_posix()[:60]}' expands suspiciously ({info.file_size // max(1, info.compress_size)}x); "
                                  "this looks like a decompression bomb.")
            total += info.file_size
            if total > limits.max_total_bytes:
                raise ZipRejected(f"The archive expands to more than {limits.max_total_bytes // (1024 * 1024)} MB.")
            plan.append((info, rel))
        dest.mkdir(parents=True, exist_ok=True)
        root = dest.resolve()
        for info, rel in plan:
            target = (dest / Path(*rel.parts)).resolve()
            if root not in target.parents:  # defence in depth after _safe_relative
                raise ZipRejected("The archive tries to write outside its folder; refusing to extract it.")
            target.parent.mkdir(parents=True, exist_ok=True)
            written = 0
            try:
                with zf.open(info) as src, open(target, "wb") as out:
                    while True:
                        chunk = src.read(64 * 1024)
                        if not chunk:
                            break
                        written += len(chunk)
                        if written > info.file_size or written > limits.max_file_bytes:  # the declared size lied
                            raise ZipRejected(f"'{rel.as_posix()[:60]}' is larger than the archive declared.")
                        out.write(chunk)
            except (zipfile.BadZipFile, zlib.error, EOFError, NotImplementedError, RuntimeError) as exc:
                # a size or CRC that disagrees with the content, an unsupported compression method, or a password-protected entry
                raise ZipRejected(f"'{rel.as_posix()[:60]}' is corrupt or inconsistent ({type(exc).__name__}).") from None
            if json_depth(target.read_bytes(), limits.max_json_depth) > limits.max_json_depth:
                target.unlink(missing_ok=True)
                raise ZipRejected(f"'{rel.as_posix()[:60]}' nests JSON more than {limits.max_json_depth} levels deep; it cannot be a real ADF file.")
            report.extracted += 1
            report.uncompressed_bytes += written
    return report

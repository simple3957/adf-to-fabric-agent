"""Read-only filesystem scanner for Repository Discovery.

Walks the source working tree deterministically and produces a
:class:`RepositoryStructure` plus a sorted list of :class:`FileRecord`. It
excludes ``.git`` internals, never follows symlinks, streams content hashes, and
records skipped/failed paths so incomplete discovery stays visible. It performs
NO ADF-artifact interpretation — that is :mod:`.artifacts`.
"""

from __future__ import annotations

import os
from pathlib import Path

from adf2fabric.agent.discovery.repository import hashing
from adf2fabric.project.discovery import (
    DiscoveryStatus,
    FileRecord,
    RepositoryStructure,
)

# Extension (lowercased, with dot) -> coarse, extension-derived file category.
_FILE_TYPE_BY_EXT = {
    ".json": "json",
    ".yml": "yaml",
    ".yaml": "yaml",
    ".ps1": "powershell",
    ".md": "markdown",
    ".txt": "text",
    ".sql": "sql",
    ".py": "python",
    ".sh": "shell",
    ".xml": "xml",
    ".csv": "csv",
    ".png": "image",
    ".jpg": "image",
    ".jpeg": "image",
    ".gif": "image",
    ".svg": "image",
    ".config": "config",
}

def scan_repository(source_dir: Path) -> tuple[RepositoryStructure, list[FileRecord]]:
    """Return the deterministic structure + file inventory of ``source_dir``.

    ``.git`` is excluded entirely. Every list/dict is sorted so the result is
    reproducible for a given commit regardless of traversal order.
    """
    files: list[FileRecord] = []
    directories: list[str] = []
    ext_counts: dict[str, int] = {}
    total_size = 0

    for root, dirnames, filenames in os.walk(source_dir):
        # Deterministic descent + exclude git internals entirely.
        dirnames[:] = sorted(d for d in dirnames if d != ".git")
        root_path = Path(root)

        # Symlinked directories: record as skipped, never descend/count.
        symlinked_dirs = [d for d in list(dirnames) if (root_path / d).is_symlink()]
        for d in symlinked_dirs:
            dirnames.remove(d)
            rel = (root_path / d).relative_to(source_dir).as_posix()
            files.append(
                FileRecord(
                    path=rel, name=d, extension="", size_bytes=0, sha256=None,
                    file_type="symlink", status=DiscoveryStatus.skipped,
                    note="symlink (not followed)",
                )
            )

        for d in dirnames:
            rel = (root_path / d).relative_to(source_dir).as_posix()
            directories.append(rel)

        for fname in sorted(filenames):
            full = root_path / fname
            rel = full.relative_to(source_dir).as_posix()
            ext = full.suffix.lower()
            file_type = _FILE_TYPE_BY_EXT.get(ext, "other")
            ext_key = ext if ext else "(none)"
            ext_counts[ext_key] = ext_counts.get(ext_key, 0) + 1

            if full.is_symlink():
                files.append(
                    FileRecord(
                        path=rel, name=fname, extension=ext, size_bytes=0,
                        sha256=None, file_type="symlink",
                        status=DiscoveryStatus.skipped, note="symlink (not followed)",
                    )
                )
                continue

            try:
                if not full.is_file():
                    files.append(
                        FileRecord(
                            path=rel, name=fname, extension=ext, size_bytes=0,
                            sha256=None, file_type=file_type,
                            status=DiscoveryStatus.skipped, note="not a regular file",
                        )
                    )
                    continue
                size = full.stat().st_size
                sha = hashing.sha256_file(full)
            except (PermissionError, OSError) as exc:
                files.append(
                    FileRecord(
                        path=rel, name=fname, extension=ext, size_bytes=0,
                        sha256=None, file_type=file_type,
                        status=DiscoveryStatus.failed,
                        note=f"unreadable ({type(exc).__name__})",
                    )
                )
                continue

            total_size += size
            files.append(
                FileRecord(
                    path=rel, name=fname, extension=ext, size_bytes=size,
                    sha256=sha, file_type=file_type, status=DiscoveryStatus.discovered,
                )
            )

    files.sort(key=lambda f: f.path)
    directories.sort()

    structure = RepositoryStructure(
        total_files=sum(1 for f in files if f.status == DiscoveryStatus.discovered),
        total_directories=len(directories),
        total_size_bytes=total_size,
        extension_counts={k: ext_counts[k] for k in sorted(ext_counts)},
        directories=directories,
        excluded_paths=[".git/"],
    )
    return structure, files

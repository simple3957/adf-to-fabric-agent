"""Output check: compare the data a pipeline wrote in Fabric with its source data, when there is no ADF run to compare with.

``verify`` compares a Fabric run with an earlier run by activity facts. With no earlier run (the factory never ran the pipeline, or ran it
on other data) there is nothing to compare, so this reads the two data sets themselves and compares row count, column names and a
content hash. Read-only: it never writes, and the record keeps counts and hashes, never a data value.

A location is an ``abfss://<filesystem>@<host>/<path>`` URL: an ADLS Gen2 account (``<account>.dfs.core.windows.net``) or a OneLake
lakehouse (``onelake.dfs.fabric.microsoft.com``, with workspace and lakehouse ids in the path). Formats: CSV with a header, and Parquet.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Optional
from urllib.parse import urlparse

from adf2fabric.project.locks import lock_for, read_text_retry, write_atomic

MATCHES, COUNTS_MATCH, DIFFERS = "matches", "counts_match", "differs"


class OutputCheckError(Exception):
    """The check could not be made (bad location, no files, unreadable format); it is never a verdict."""


@dataclass(frozen=True)
class Location:
    account_url: str
    filesystem: str
    path: str

    def __str__(self) -> str:
        return f"abfss://{self.filesystem}@{self.account_url.removeprefix('https://')}/{self.path}"


ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"
_ADLS_HOST = re.compile(r"^[a-z0-9]{3,24}\.dfs\.core\.windows\.net$")


def parse_location(url: str) -> Location:
    u = urlparse((url or "").strip())
    if u.scheme != "abfss" or not u.username or not u.hostname:
        raise OutputCheckError(f"'{url}' is not an abfss://<filesystem>@<host>/<path> location.")
    return Location(f"https://{u.hostname}", u.username, u.path.strip("/"))


def storage_host(loc: Location) -> str:
    """``"onelake"`` or ``"adls"`` for the two kinds of store a check may read; anything else (another host, a port, an address) is refused,
    because the server reads it with its own sign-in."""
    host = loc.account_url.removeprefix("https://").lower()
    if host == ONELAKE_HOST:
        return "onelake"
    if _ADLS_HOST.match(host):
        return "adls"
    raise OutputCheckError(f"'{host}' is not an ADLS Gen2 account (<account>.dfs.core.windows.net) or OneLake ({ONELAKE_HOST}).")


@dataclass
class Summary:
    files: int = 0
    rows: int = 0
    columns: list[str] = field(default_factory=list)
    digest: str = ""


def _canon(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def _csv_rows(data: bytes) -> tuple[list[str], Iterator[list[Any]]]:
    reader = csv.reader(io.StringIO(data.decode("utf-8-sig"), newline=""))
    header = next(reader, None)
    if header is None:
        return [], iter(())
    return [h.strip() for h in header], reader


def _parquet_rows(data: bytes) -> tuple[list[str], Iterator[list[Any]]]:
    try:
        import pyarrow.parquet as pq
    except ImportError:
        raise OutputCheckError("pyarrow is not installed: Parquet needs it (pip install -e \".[warehouse]\").") from None
    table = pq.read_table(io.BytesIO(data))
    cols = list(table.column_names)
    return cols, (list(r) for r in zip(*[table.column(i).to_pylist() for i in range(len(cols))])) if cols else iter(())


def detect_format(name: str) -> Optional[str]:
    n = name.lower()
    return "csv" if n.endswith((".csv", ".tsv", ".txt")) else "parquet" if n.endswith(".parquet") else None


def summarize(files: Iterable[tuple[str, bytes]], fmt: Optional[str] = None) -> Summary:
    """Rows, column names and an order-independent hash over every data file. The hash is a sum of per-row hashes, so file order,
    row order and column order do not matter; a changed, missing or extra row does."""
    out, total, seen = Summary(), 0, set()
    for name, data in files:
        f = fmt or detect_format(name)
        if f is None:
            continue
        cols, rows = (_csv_rows if f == "csv" else _parquet_rows)(data)
        order = sorted(range(len(cols)), key=lambda i: cols[i].lower())
        seen.update(c.lower() for c in cols)
        out.files += 1
        for row in rows:
            row = list(row) + [None] * (len(cols) - len(row))
            line = "\x1f".join(f"{cols[i].lower()}={_canon(row[i])}" for i in order)
            total = (total + int.from_bytes(hashlib.sha256(line.encode("utf-8")).digest(), "big")) % (1 << 256)
            out.rows += 1
    out.columns = sorted(seen)
    out.digest = f"{total:064x}"[:32] if out.rows else ""
    if not out.files:
        raise OutputCheckError("No CSV or Parquet data file found there.")
    return out


def judge(source: Summary, target: Summary) -> tuple[str, str]:
    if source.rows != target.rows:
        return DIFFERS, f"{source.rows} source row(s), {target.rows} in the output."
    if source.columns != target.columns:
        return DIFFERS, "The column names differ."
    if source.digest == target.digest:
        return MATCHES, f"{source.rows} row(s), the same columns, the same content."
    return COUNTS_MATCH, (f"{source.rows} row(s) and the same columns, but the content hash differs (a value was changed, or written in a "
                          "different format such as a number or date type); look at a sample.")


def read_location(service: Any, loc: Location) -> list[tuple[str, bytes]]:
    """Every data file under ``loc`` (one file, or a directory read recursively). Skips names starting with ``_`` or ``.`` (markers, logs)."""
    fs = service.get_file_system_client(loc.filesystem)
    names: list[str] = []
    try:
        props = fs.get_file_client(loc.path).get_file_properties()
        is_file = not bool(props.get("metadata", {}).get("hdi_isfolder"))
    except Exception:  # noqa: BLE001  not a file: a directory
        is_file = False
    if is_file:
        names = [loc.path]
    else:
        for p in fs.get_paths(path=loc.path or None, recursive=True):
            if not getattr(p, "is_directory", False) and not any(s.startswith(("_", ".")) for s in p.name.split("/")):
                names.append(p.name)
    return [(n, fs.get_file_client(n).download_file().readall()) for n in sorted(names)]


def service_for(loc: Location, credential: Any) -> Any:
    try:
        from azure.storage.filedatalake import DataLakeServiceClient
    except ImportError:
        raise OutputCheckError("azure-storage-file-datalake is not installed (the 'connectivity' extra).") from None
    return DataLakeServiceClient(loc.account_url, credential=credential)


def check(name: str, pipeline: str, source: Location, target: Location, credential: Any, fmt: Optional[str] = None,
          reader: Optional[Callable[[Any, Location], list[tuple[str, bytes]]]] = None, target_credential: Any = None) -> dict:
    """``target_credential`` reads the target when it needs another sign-in than the source (the Fabric account for OneLake)."""
    reader = reader or read_location
    s = summarize(reader(service_for(source, credential), source), fmt)
    t = summarize(reader(service_for(target, target_credential or credential), target), fmt)
    verdict, detail = judge(s, t)
    return {"name": name, "pipeline": pipeline, "source": str(source), "target": str(target), "verdict": verdict, "detail": detail,
            "source_summary": asdict(s), "target_summary": asdict(t), "checked_at": datetime.now(timezone.utc).isoformat()}


def record_path(project_dir: Path) -> Path:
    return project_dir / "verification" / "output-checks.json"


def load(project_dir: Path) -> dict[str, dict[str, dict]]:
    try:
        data = json.loads(read_text_retry(record_path(project_dir)))
    except (FileNotFoundError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def save(project_dir: Path, result: dict) -> Path:
    p = record_path(project_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    with lock_for(p):  # read, merge and write as one step: two checks finishing together must not drop each other
        data = load(project_dir)
        data.setdefault(result["pipeline"], {})[result["name"]] = result
        write_atomic(p, json.dumps(data, indent=2, sort_keys=True))
    return p


def evidence(project_dir: Path, pipeline: str, ran_at: Optional[datetime]) -> tuple[bool, int, list[str]]:
    """(vouches, number of checks, what is wrong): every check of the pipeline matched, and each was made after the run that wrote the data."""
    rows = list((load(project_dir).get(pipeline) or {}).values())
    if not rows:
        for k, v in load(project_dir).items():  # pipeline names compare case-insensitively
            if k.lower() == pipeline.lower():
                rows = list(v.values())
    bad: list[str] = []
    for r in rows:
        if r.get("verdict") != MATCHES:
            bad.append(f"'{r.get('name')}': {r.get('verdict')} ({r.get('detail')})")
            continue
        try:
            at = datetime.fromisoformat(str(r.get("checked_at")))
        except ValueError:
            at = None
        if ran_at is not None and (at is None or at < ran_at):
            bad.append(f"'{r.get('name')}': checked before the recorded run, so it does not vouch for it")
    return bool(rows) and not bad, len(rows), bad

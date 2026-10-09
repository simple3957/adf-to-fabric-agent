"""ADLS Gen2 path enumeration (Phase 2 behavior).

The ONLY data-plane operations performed are ``get_file_system_client`` and
``get_paths(recursive=True)`` — a recursive path listing of a single filesystem,
followed to completion across pages. It never reads file contents, downloads,
writes, deletes, renames, parses data, or infers schemas. Per-path metadata is
limited to path / name / directory / is_directory / size / last_modified.

The SDK client is built behind a lazy-import seam so tests inject a fake and prove
no other operation is used. Determinism: paths are sorted by path; pagination is
followed fully with a defensive page backstop that flags (never hides) truncation.
"""

from __future__ import annotations

from typing import Any

from adf2fabric.project.discovery import AdlsPath

# Defensive backstop on pagination (far above any real filesystem). If exceeded,
# the listing is flagged truncated/incomplete rather than looping forever.
_MAX_PAGES = 100_000


def build_service_client(account_url: str, credential: Any) -> Any:
    """Build a DataLakeServiceClient (lazy import seam).

    Raises ``ImportError`` if the ``connectivity`` extra is not installed; the
    caller maps that to a ``dependency_unavailable`` outcome. Authentication is
    owned entirely by the injected credential abstraction.
    """
    from azure.storage.filedatalake import DataLakeServiceClient

    return DataLakeServiceClient(account_url, credential=credential)


def enumerate_paths(service: Any, filesystem: str) -> tuple[list[AdlsPath], bool]:
    """Return ``(paths, complete)`` for ``filesystem``, sorted by path.

    Calls ONLY ``service.get_file_system_client(filesystem)`` then
    ``get_paths(recursive=True)``, iterating every page. ``complete`` is False if
    the ``_MAX_PAGES`` backstop stopped iteration. Captures name / is_directory /
    size / last_modified metadata; never touches contents.
    """
    fs_client = service.get_file_system_client(filesystem)
    paths: list[AdlsPath] = []
    complete = True

    pager = fs_client.get_paths(recursive=True).by_page()
    page_count = 0
    for page in pager:
        page_count += 1
        if page_count > _MAX_PAGES:
            complete = False
            break
        for prop in page:
            paths.append(_to_path(prop, filesystem))

    paths.sort(key=lambda p: p.path)
    return paths, complete


def _to_path(prop: Any, filesystem: str) -> AdlsPath:
    """Normalize one SDK ``PathProperties`` into a safe :class:`AdlsPath`."""
    raw = getattr(prop, "name", "") or ""
    full = str(raw).strip("/")
    is_dir = bool(getattr(prop, "is_directory", False))
    segments = full.split("/") if full else []
    name = segments[-1] if segments else ""
    directory = "/".join(segments[:-1]) if len(segments) > 1 else ""
    size = getattr(prop, "content_length", None)
    return AdlsPath(
        filesystem=filesystem,
        path=full,
        name=name,
        directory=directory,
        is_directory=is_dir,
        size_bytes=(int(size) if (size is not None and not is_dir) else None),
        last_modified=_iso(getattr(prop, "last_modified", None)),
    )


def _iso(value: Any) -> str | None:
    """Render a last-modified value as an ISO-8601 string, or None."""
    if value is None:
        return None
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return isoformat()
    return str(value)

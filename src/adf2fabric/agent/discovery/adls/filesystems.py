"""ADLS Gen2 filesystem/container enumeration (Phase 1 behavior).

The ONLY data-plane operation performed here is ``list_file_systems()`` — a
top-level container listing. It never lists paths/directories/files, reads data,
or writes anything. Container metadata captured is limited to name +
last_modified. The SDK client is built behind a seam so tests inject a fake and
prove no other operation is used.
"""

from __future__ import annotations

from typing import Any

from adf2fabric.project.discovery import AdlsFilesystem


def build_service_client(account_url: str, credential: Any) -> Any:
    """Build a DataLakeServiceClient (lazy import seam).

    Raises ``ImportError`` if the ``connectivity`` extra is not installed; the
    caller maps that to a ``dependency_unavailable`` outcome.
    """
    from azure.storage.filedatalake import DataLakeServiceClient

    return DataLakeServiceClient(account_url, credential=credential)


def enumerate_filesystems(service: Any) -> list[AdlsFilesystem]:
    """Return the filesystems/containers visible via ``service``, sorted by name.

    Calls ONLY ``service.list_file_systems()``. Captures name + last_modified
    metadata; never touches paths, directories, files, or contents.
    """
    filesystems: list[AdlsFilesystem] = []
    for fs in service.list_file_systems():
        filesystems.append(
            AdlsFilesystem(
                name=fs.name,
                last_modified=_iso(getattr(fs, "last_modified", None)),
            )
        )
    filesystems.sort(key=lambda f: f.name)
    return filesystems


def _iso(value: Any) -> str | None:
    """Render last-modified metadata as an ISO-8601 string, or None."""
    if value is None:
        return None
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return isoformat()
    return str(value)

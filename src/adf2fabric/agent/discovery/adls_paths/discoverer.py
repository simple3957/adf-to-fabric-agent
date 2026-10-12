"""The ADLS Gen2 Phase-2 Discoverer — directory / path / file inventory.

Responsibility (and hard boundary):

    load project -> require ADLS Phase-1 discovery -> reuse its storage account +
    filesystem list -> build Azure CLI credential -> build data-plane client
    -> enumerate paths (get_paths recursive, fully paginated) inside each
    discovered filesystem -> normalize outcome -> persist discovery/adls-paths.yaml
    -> STOP

It reuses the project's credential abstraction (``_azure.build_credential``) and
the ADLS Gen2 data-plane SDK. It performs enumeration/listing ONLY — no reads,
downloads, writes, deletes, renames, parsing, schema inference, or profiling. The
storage account + filesystems come from the immutable Phase-1 record, which is
never modified. The credential/token is owned by the SDK and never persisted;
error detail is redacted. No ADF<->ADLS reconciliation.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable

from adf2fabric.agent.discovery.adls_paths import paths
from adf2fabric.agent.discovery.adls_paths.models import (
    AdlsPathsDiscoveryRequest,
    AdlsPathsDiscoveryResult,
)
from adf2fabric.connectivity._azure import CredentialUnavailable, build_credential
from adf2fabric.project.discovery import (
    AdlsDiscoveryStatus,
    AdlsFilesystemPaths,
    AdlsPath,
    AdlsPathsDiscovery,
)
from adf2fabric.project.store import ProjectNotFoundError, ProjectStore, slugify
from adf2fabric.repository import authentication

_PROVENANCE = {
    "storage_account": "adls_phase1_discovery",
    "filesystems": "adls_phase1_discovery",
    "status": "adls_data_plane",
    "paths": "adls_data_plane_get_paths_recursive",
}


class AdlsPathsDiscoverer:
    """ADLS Gen2 Phase-2 discoverer, invoked by the phase-level ``DiscoveryAgent``."""

    def __init__(
        self,
        store: ProjectStore,
        service_factory: Callable[[str, Any], Any] = paths.build_service_client,
    ):
        self.store = store
        # Injection point for tests; production builds a real DataLakeServiceClient.
        self.service_factory = service_factory

    def run(self, request: AdlsPathsDiscoveryRequest) -> AdlsPathsDiscoveryResult:
        try:
            project_id = slugify(request.project_name)
            if not self.store.exists(project_id):
                return AdlsPathsDiscoveryResult(
                    success=False,
                    message="ADLS path discovery failed.",
                    error=(
                        f"No project '{project_id}' found. Run `adf2fabric intake` "
                        "first to establish the project."
                    ),
                )

            # Phase 1 is the authoritative input for account + filesystem identity.
            if not self.store.has_adls_discovery(project_id):
                return self._persist_result(
                    project_id, "", "", AdlsDiscoveryStatus.missing_config, [],
                    detail=(
                        "ADLS Phase-1 discovery has not run. Run `adf2fabric "
                        "discovery-adls` first to record the storage account and "
                        "filesystem(s)."
                    ),
                )
            phase1 = self.store.load_adls_discovery(project_id)
            account = phase1.storage_account
            account_url = phase1.account_url

            # Credential — owned entirely by the abstraction; secret only for redaction.
            try:
                credential, secret = build_credential(request.credential, storage_ok=True)
            except CredentialUnavailable as exc:
                status = (
                    AdlsDiscoveryStatus.dependency_unavailable
                    if exc.status.value == "dependency_unavailable"
                    else AdlsDiscoveryStatus.missing_config
                )
                return self._persist_result(
                    project_id, account, account_url, status, [], detail=exc.detail
                )

            try:
                service = self.service_factory(account_url, credential)
                filesystems = [
                    _enumerate_one(service, fs.name) for fs in phase1.filesystems
                ]
            except ImportError:
                return self._persist_result(
                    project_id, account, account_url,
                    AdlsDiscoveryStatus.dependency_unavailable, [],
                    detail="azure-storage-file-datalake is not installed. "
                    "Install the 'connectivity' extra.",
                )
            except Exception as exc:  # noqa: BLE001 - normalized below, redacted
                status = _classify_error(exc)
                detail = authentication.redact(str(exc), secret)
                return self._persist_result(
                    project_id, account, account_url, status, [], detail=detail
                )
            finally:
                credential = None  # drop the credential reference promptly

            return self._persist_result(
                project_id, account, account_url,
                AdlsDiscoveryStatus.discovered, filesystems, detail="",
            )
        except ProjectNotFoundError as exc:
            return AdlsPathsDiscoveryResult(
                success=False, message="ADLS path discovery failed.", error=str(exc)
            )

    # --- assembly + persistence --------------------------------------------

    def _persist_result(
        self, project_id, account, account_url, status,
        filesystems: list[AdlsFilesystemPaths], *, detail: str,
    ) -> AdlsPathsDiscoveryResult:
        discovery = self._assemble(
            project_id, account, account_url, status, filesystems, detail
        )
        self.store.save_adls_paths_discovery(project_id, discovery)
        if status == AdlsDiscoveryStatus.discovered:
            msg = (
                f"ADLS path discovery complete for '{project_id}': "
                f"{discovery.total_file_count} file(s), "
                f"{discovery.total_directory_count} director(ies) across "
                f"{discovery.filesystem_count} filesystem(s)."
            )
            return AdlsPathsDiscoveryResult(success=True, discovery=discovery, message=msg)
        return AdlsPathsDiscoveryResult(
            success=False, discovery=discovery,
            message="ADLS path discovery failed.",
            error=f"{status.value}: {detail}" if detail else status.value,
        )

    def _assemble(
        self, project_id, account, account_url, status, filesystems, detail
    ) -> AdlsPathsDiscovery:
        now = datetime.now(timezone.utc)
        discovery = AdlsPathsDiscovery(
            project_id=project_id,
            storage_account=account,
            account_url=account_url,
            status=status,
            created_at=now,
            updated_at=now,
            filesystem_count=len(filesystems),
            filesystems=filesystems,
            total_path_count=sum(f.path_count for f in filesystems),
            total_directory_count=sum(f.directory_count for f in filesystems),
            total_file_count=sum(f.file_count for f in filesystems),
            total_size_bytes=sum(f.total_size_bytes for f in filesystems),
            detail=detail,
            provenance=dict(_PROVENANCE),
        )
        if self.store.has_adls_paths_discovery(project_id):
            try:
                existing = self.store.load_adls_paths_discovery(project_id)
            except Exception:  # noqa: BLE001 - a corrupt prior record is replaced
                existing = None
            if existing is not None:
                discovery.created_at = existing.created_at
                if _semantic(existing) == _semantic(discovery):
                    discovery.updated_at = existing.updated_at
                else:
                    discovery.updated_at = now
        return discovery


def _enumerate_one(service: Any, filesystem: str) -> AdlsFilesystemPaths:
    """Enumerate one filesystem's paths into a summarized :class:`AdlsFilesystemPaths`."""
    path_list, complete = paths.enumerate_paths(service, filesystem)
    directories = [p for p in path_list if p.is_directory]
    files = [p for p in path_list if not p.is_directory]
    return AdlsFilesystemPaths(
        name=filesystem,
        path_count=len(path_list),
        directory_count=len(directories),
        file_count=len(files),
        total_size_bytes=sum(p.size_bytes or 0 for p in files),
        paths=path_list,
        complete=complete,
        truncated=not complete,
    )


def _classify_error(exc: Exception) -> AdlsDiscoveryStatus:
    """Map an SDK exception to a distinct, deterministic discovery status.

    Duck-typed on ``status_code`` + exception class name so it stays decoupled
    from azure imports (mirrors ADLS Phase 1). A permission/auth failure is NEVER
    collapsed into an empty result.
    """
    name = type(exc).__name__
    status_code = getattr(exc, "status_code", None)
    if status_code == 401 or name == "ClientAuthenticationError":
        return AdlsDiscoveryStatus.authentication_failed
    if status_code == 403:
        return AdlsDiscoveryStatus.permission_denied
    if name in ("ServiceRequestError", "ServiceResponseError") or isinstance(
        exc, (ConnectionError, TimeoutError)
    ):
        return AdlsDiscoveryStatus.connectivity_failed
    return AdlsDiscoveryStatus.connectivity_failed


def _semantic(discovery: AdlsPathsDiscovery) -> dict:
    """Discovery content used to detect meaningful change (excludes timestamps)."""
    data = json.loads(discovery.model_dump_json())
    data.pop("created_at", None)
    data.pop("updated_at", None)
    return data

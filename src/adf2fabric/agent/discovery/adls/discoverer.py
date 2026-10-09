"""The ADLS Gen2 Discoverer — the ADLS domain of the Discovery phase (Phase 1).

Responsibility (and hard boundary):

    load project -> resolve explicit storage account -> build Azure CLI credential
    -> build data-plane client -> list filesystems (ONLY) -> normalize outcome
    -> persist discovery/adls.yaml -> STOP

It uses the project's existing credential abstraction (``_azure.build_credential``)
and the ADLS Gen2 data-plane SDK. It performs NO ARM calls, NO path/file listing,
NO reads/writes, and never touches ``connectivity/adls_gen2.py``. The bearer
token is owned entirely by the SDK/credential and is never logged or persisted;
error detail is redacted.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from adf2fabric.agent.discovery.adls.filesystems import (
    build_service_client,
    enumerate_filesystems,
)
from adf2fabric.agent.discovery.adls.models import (
    AdlsDiscoveryRequest,
    AdlsDiscoveryResult,
)
from adf2fabric.connectivity._azure import CredentialUnavailable, build_credential
from adf2fabric.project.discovery import (
    AdlsDiscovery,
    AdlsDiscoveryStatus,
    AdlsFilesystem,
)
from adf2fabric.project.store import ProjectNotFoundError, ProjectStore, slugify
from adf2fabric.repository import authentication

_PROVENANCE = {
    "storage_account": "explicit_cli_input",
    "status": "adls_data_plane",
    "filesystems": "adls_data_plane_list_file_systems",
}


class AdlsDiscoverer:
    """ADLS Gen2 domain discoverer, invoked by the phase-level ``DiscoveryAgent``."""

    def __init__(
        self,
        store: ProjectStore,
        service_factory: Callable[[str, Any], Any] = build_service_client,
    ):
        self.store = store
        # Injection point for tests; production builds a real DataLakeServiceClient.
        self.service_factory = service_factory

    def run(self, request: AdlsDiscoveryRequest) -> AdlsDiscoveryResult:
        try:
            project_id = slugify(request.project_name)
            if not self.store.exists(project_id):
                return AdlsDiscoveryResult(
                    success=False,
                    message="ADLS Discovery failed.",
                    error=(
                        f"No project '{project_id}' found. Run `adf2fabric intake` "
                        "first to establish the project."
                    ),
                )
            account = (request.storage_account or "").strip()
            if not account:
                return self._persist_result(
                    project_id, account, "",
                    AdlsDiscoveryStatus.missing_config,
                    [], "A storage account must be supplied explicitly.",
                )

            account_url = f"https://{account}.dfs.core.windows.net"

            # Credential — owned entirely by the abstraction; secret only for redaction.
            try:
                credential, secret = build_credential(request.credential)
            except CredentialUnavailable as exc:
                status = (
                    AdlsDiscoveryStatus.dependency_unavailable
                    if exc.status.value == "dependency_unavailable"
                    else AdlsDiscoveryStatus.missing_config
                )
                return self._persist_result(
                    project_id, account, account_url, status, [], exc.detail
                )

            # Data-plane client + the single allowed operation: list filesystems.
            try:
                service = self.service_factory(account_url, credential)
                filesystems = enumerate_filesystems(service)
            except ImportError:
                return self._persist_result(
                    project_id, account, account_url,
                    AdlsDiscoveryStatus.dependency_unavailable, [],
                    "azure-storage-file-datalake is not installed. "
                    "Install the 'connectivity' extra.",
                )
            except Exception as exc:  # noqa: BLE001 - normalized below, redacted
                status = _classify_error(exc)
                detail = authentication.redact(str(exc), secret)
                return self._persist_result(
                    project_id, account, account_url, status, [], detail
                )
            finally:
                credential = None  # drop the credential reference promptly

            return self._persist_result(
                project_id, account, account_url,
                AdlsDiscoveryStatus.discovered, filesystems, "",
            )
        except ProjectNotFoundError as exc:
            return AdlsDiscoveryResult(
                success=False, message="ADLS Discovery failed.", error=str(exc)
            )

    # --- assembly + persistence --------------------------------------------

    def _persist_result(
        self,
        project_id: str,
        account: str,
        account_url: str,
        status: AdlsDiscoveryStatus,
        filesystems: list[AdlsFilesystem],
        detail: str,
    ) -> AdlsDiscoveryResult:
        discovery = self._assemble(
            project_id, account, account_url, status, filesystems, detail
        )
        self.store.save_adls_discovery(project_id, discovery)
        ok = status == AdlsDiscoveryStatus.discovered
        if ok:
            msg = (
                f"ADLS Discovery complete for '{project_id}': "
                f"{discovery.filesystem_count} filesystem(s)."
            )
            return AdlsDiscoveryResult(success=True, discovery=discovery, message=msg)
        return AdlsDiscoveryResult(
            success=False,
            discovery=discovery,
            message="ADLS Discovery failed.",
            error=f"{status.value}: {detail}" if detail else status.value,
        )

    def _assemble(
        self, project_id, account, account_url, status, filesystems, detail
    ) -> AdlsDiscovery:
        now = datetime.now(timezone.utc)
        discovery = AdlsDiscovery(
            project_id=project_id,
            storage_account=account,
            account_url=account_url,
            status=status,
            created_at=now,
            updated_at=now,
            filesystem_count=len(filesystems),
            filesystems=filesystems,
            detail=detail,
            provenance=dict(_PROVENANCE),
        )

        # Timestamp semantics: created_at is set once; updated_at bumps only when
        # the semantic content changes (timestamps excluded from comparison).
        if self.store.has_adls_discovery(project_id):
            try:
                existing = self.store.load_adls_discovery(project_id)
            except Exception:  # noqa: BLE001 - a corrupt prior record is replaced
                existing = None
            if existing is not None:
                discovery.created_at = existing.created_at
                if _semantic(existing) == _semantic(discovery):
                    discovery.updated_at = existing.updated_at
                else:
                    discovery.updated_at = now
        return discovery


def _classify_error(exc: Exception) -> AdlsDiscoveryStatus:
    """Map an SDK exception to a distinct, deterministic discovery status.

    Duck-typed on ``status_code`` + exception class name so it stays decoupled
    from azure imports (and simple to test with plain fakes). A permission or
    auth failure is NEVER collapsed into an empty result.
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


def _semantic(discovery: AdlsDiscovery) -> dict:
    """Discovery content used to detect meaningful change (excludes timestamps)."""
    data = json.loads(discovery.model_dump_json())
    data.pop("created_at", None)
    data.pop("updated_at", None)
    return data

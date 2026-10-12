"""Azure Data Lake Storage Gen2 connectivity check.

Uses ``azure-storage-file-datalake`` with an ``azure-identity`` credential to
confirm the storage account is reachable and the supplied identity can access
it. When a ``filesystem`` is explicitly provided, that single filesystem's
properties are checked. It performs NO enumeration or data inspection:

    authenticate -> reach storage account -> validate required access -> result

It must never list containers/filesystems, list directories, list files, read
file contents, or infer ADF -> ADLS relationships. That is Discovery's job.

``azure-storage-file-datalake`` is imported lazily; if unavailable the connector
returns a deterministic ``dependency_unavailable`` outcome.
"""

from __future__ import annotations

from adf2fabric.connectivity._azure import CredentialUnavailable, build_credential
from adf2fabric.connectivity.base import outcome
from adf2fabric.project.models import (
    AccessDomain,
    AdlsGen2Access,
    ConnectivityOutcome,
    ConnectivityStatus,
    Project,
)
from adf2fabric.repository import authentication


class AdlsGen2Connector:
    domain = AccessDomain.adls_gen2.value

    def validate(self, access: AdlsGen2Access, project: Project) -> ConnectivityOutcome:
        if not access.storage_account:
            return outcome(
                AccessDomain.adls_gen2,
                ConnectivityStatus.missing_config,
                detail="'storage_account' is required.",
            )

        try:
            from azure.storage.filedatalake import DataLakeServiceClient
        except ImportError:
            return outcome(
                AccessDomain.adls_gen2,
                ConnectivityStatus.dependency_unavailable,
                detail=(
                    "azure-storage-file-datalake is not installed. "
                    "Install the 'connectivity' extra."
                ),
            )

        try:
            credential, secret = build_credential(access.credential, storage_ok=True)
        except CredentialUnavailable as exc:
            return outcome(AccessDomain.adls_gen2, exc.status, detail=exc.detail)

        account_url = f"https://{access.storage_account}.dfs.core.windows.net"
        try:
            service = DataLakeServiceClient(account_url, credential=credential)
            # Single account-level metadata call — NOT an enumeration.
            service.get_service_properties()
            if access.filesystem:
                fs = service.get_file_system_client(access.filesystem)
                # Single filesystem-level metadata call — NOT a path listing.
                fs.get_file_system_properties()
        except Exception as exc:  # noqa: BLE001
            return outcome(
                AccessDomain.adls_gen2,
                ConnectivityStatus.failed,
                detail=authentication.redact(str(exc), secret),
            )

        target = (
            f"storage account '{access.storage_account}'"
            + (f", filesystem '{access.filesystem}'" if access.filesystem else "")
        )
        return outcome(
            AccessDomain.adls_gen2,
            ConnectivityStatus.validated,
            reachable=True,
            authenticated=True,
            detail=f"Reachable: {target}.",
        )

"""The ADF Live Discoverer — the ADF domain of the Discovery phase (Phase 1).

Responsibility (and hard boundary):

    load project -> resolve explicit factory target -> build Azure credential
    -> acquire an ARM token -> enumerate the six factory collections (read-only,
    paginated GET) -> extract name + kind ONLY -> normalize outcome
    -> persist discovery/adf.yaml -> STOP

It records the top-level artifact INVENTORY of the deployed factory — pipelines,
datasets, linked services, triggers, data flows, integration runtimes — obtained
from Azure Resource Manager child-collection GETs. It persists identity only; ARM
list responses include full properties (possibly secret for linked services), all
of which are discarded. It performs NO write, no run/activation, no reference
resolution, and no migration interpretation. The bearer token is owned by the
credential abstraction, dropped promptly, and never logged or persisted.

It REUSES the connectivity building blocks (``_azure.build_credential``, and the
ARM scope + api-version from ``connectivity.azure_data_factory``) and never
modifies them. Connectivity answers "can I reach the factory?"; Discovery answers
"what exists inside it?" — the two never merge.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable

from adf2fabric.agent.discovery.adf import arm
from adf2fabric.agent.discovery.adf.models import (
    AdfDiscoveryRequest,
    AdfDiscoveryResult,
)
from adf2fabric.connectivity._azure import CredentialUnavailable, build_credential
from adf2fabric.connectivity.azure_data_factory import _scoped
from adf2fabric.project.discovery import (
    AdfArtifact,
    AdfArtifactKind,
    AdfDiscovery,
    AdfDiscoveryStatus,
)
from adf2fabric.project.models import AdfAccess, ConnectivityStatus
from adf2fabric.project.store import ProjectNotFoundError, ProjectStore, slugify
from adf2fabric.repository import authentication

_PROVENANCE = {
    "subscription_id": "explicit_cli_input",
    "resource_group": "explicit_cli_input",
    "factory_name": "explicit_cli_input",
    "authentication": "explicit_cli_input",
    "status": "arm_rest",
    "artifacts": "arm_factory_child_collections",
}

# CredentialUnavailable carries a ConnectivityStatus; translate to the ADF status.
_CRED_STATUS_MAP = {
    ConnectivityStatus.dependency_unavailable: AdfDiscoveryStatus.dependency_unavailable,
    ConnectivityStatus.missing_credential: AdfDiscoveryStatus.missing_credential,
    ConnectivityStatus.missing_config: AdfDiscoveryStatus.missing_config,
}


class AdfDiscoverer:
    """ADF domain discoverer, invoked by the phase-level ``DiscoveryAgent``."""

    def __init__(
        self,
        store: ProjectStore,
        transport_factory: Callable[[], Any] = arm.default_transport,
        credential_builder: Callable[[Any], tuple[Any, str]] = build_credential,
    ):
        self.store = store
        # Injection points for tests; production uses requests + build_credential.
        self.transport_factory = transport_factory
        self.credential_builder = credential_builder

    def run(self, request: AdfDiscoveryRequest) -> AdfDiscoveryResult:
        try:
            project_id = slugify(request.project_name)
            if not self.store.exists(project_id):
                return AdfDiscoveryResult(
                    success=False,
                    message="ADF Discovery failed.",
                    error=(
                        f"No project '{project_id}' found. Run `adf2fabric intake` "
                        "first to establish the project."
                    ),
                )

            access = request.access
            subscription = (access.subscription_id or "").strip()
            resource_group = (access.resource_group or "").strip()
            factory = (access.factory_name or "").strip()
            auth = _auth_mode(access)

            # 1. Explicit target — never searched, enumerated, or inferred.
            missing = [
                name
                for name, val in (
                    ("subscription_id", subscription),
                    ("resource_group", resource_group),
                    ("factory_name", factory),
                )
                if not val
            ]
            if missing:
                return self._persist_result(
                    project_id, subscription, resource_group, factory, auth,
                    AdfDiscoveryStatus.missing_config, {},
                    detail=f"Missing required config: {', '.join(missing)}.",
                )

            # 2. Credential — owned entirely by the abstraction; secret only for redaction.
            try:
                credential, secret = self.credential_builder(_scoped(access.credential, subscription))
            except CredentialUnavailable as exc:
                status = _CRED_STATUS_MAP.get(
                    exc.status, AdfDiscoveryStatus.missing_config
                )
                return self._persist_result(
                    project_id, subscription, resource_group, factory, auth,
                    status, {}, detail=exc.detail,
                )

            # 3. HTTP transport dependency.
            try:
                transport = self.transport_factory()
            except ImportError:
                return self._persist_result(
                    project_id, subscription, resource_group, factory, auth,
                    AdfDiscoveryStatus.dependency_unavailable, {},
                    detail="requests is not installed. Install the 'connectivity' extra.",
                )

            # 4. Acquire an ARM-scoped token (this scope only).
            try:
                token = credential.get_token(arm.ARM_SCOPE).token
            except Exception as exc:  # noqa: BLE001
                return self._persist_result(
                    project_id, subscription, resource_group, factory, auth,
                    AdfDiscoveryStatus.authentication_failed, {},
                    detail=(
                        "Could not acquire an Azure token. "
                        f"{authentication.redact(str(exc), secret)}"
                    ),
                )
            finally:
                credential = None  # drop the credential reference promptly

            # 5. Enumerate the six factory collections (read-only, paginated GET).
            base_url = arm.factory_base_url(subscription, resource_group, factory)
            try:
                artifacts = arm.enumerate_artifacts(transport, base_url, token)
            except arm.ArmHttpError as exc:
                status = _classify_status(exc.status_code)
                detail = _http_detail(exc.status_code, factory)
                return self._persist_result(
                    project_id, subscription, resource_group, factory, auth,
                    status, {}, detail=detail,
                )
            except Exception as exc:  # noqa: BLE001 - transport/network, redacted
                return self._persist_result(
                    project_id, subscription, resource_group, factory, auth,
                    AdfDiscoveryStatus.connectivity_failed, {},
                    detail=(
                        "Network/connectivity failure contacting Azure Resource "
                        f"Manager: {authentication.redact(str(exc), secret, token)}"
                    ),
                )
            finally:
                token = None  # drop the token reference promptly

            return self._persist_result(
                project_id, subscription, resource_group, factory, auth,
                AdfDiscoveryStatus.discovered, artifacts, detail="",
            )
        except ProjectNotFoundError as exc:
            return AdfDiscoveryResult(
                success=False, message="ADF Discovery failed.", error=str(exc)
            )

    # --- assembly + persistence --------------------------------------------

    def _persist_result(
        self,
        project_id: str,
        subscription: str,
        resource_group: str,
        factory: str,
        authentication_mode: str,
        status: AdfDiscoveryStatus,
        artifacts: dict[AdfArtifactKind, list[AdfArtifact]],
        *,
        detail: str,
    ) -> AdfDiscoveryResult:
        discovery = self._assemble(
            project_id, subscription, resource_group, factory, authentication_mode,
            status, artifacts, detail,
        )
        self.store.save_adf_discovery(project_id, discovery)
        ok = status == AdfDiscoveryStatus.discovered
        if ok:
            msg = (
                f"ADF Discovery complete for '{project_id}': "
                f"{discovery.pipeline_count} pipeline(s), "
                f"{discovery.dataset_count} dataset(s), "
                f"{discovery.linked_service_count} linked service(s), "
                f"{discovery.trigger_count} trigger(s), "
                f"{discovery.dataflow_count} data flow(s), "
                f"{discovery.integration_runtime_count} integration runtime(s)."
            )
            return AdfDiscoveryResult(success=True, discovery=discovery, message=msg)
        return AdfDiscoveryResult(
            success=False,
            discovery=discovery,
            message="ADF Discovery failed.",
            error=f"{status.value}: {detail}" if detail else status.value,
        )

    def _assemble(
        self, project_id, subscription, resource_group, factory, authentication_mode,
        status, artifacts, detail,
    ) -> AdfDiscovery:
        now = datetime.now(timezone.utc)

        def kind_list(kind: AdfArtifactKind) -> list[AdfArtifact]:
            return artifacts.get(kind, [])

        pipelines = kind_list(AdfArtifactKind.pipeline)
        datasets = kind_list(AdfArtifactKind.dataset)
        linked_services = kind_list(AdfArtifactKind.linked_service)
        triggers = kind_list(AdfArtifactKind.trigger)
        dataflows = kind_list(AdfArtifactKind.dataflow)
        integration_runtimes = kind_list(AdfArtifactKind.integration_runtime)
        total = (
            len(pipelines) + len(datasets) + len(linked_services)
            + len(triggers) + len(dataflows) + len(integration_runtimes)
        )
        # Only a successful enumeration records the collections it queried.
        queried = (
            list(arm.COLLECTION_LABELS)
            if status == AdfDiscoveryStatus.discovered
            else []
        )

        discovery = AdfDiscovery(
            project_id=project_id,
            subscription_id=subscription,
            resource_group=resource_group,
            factory_name=factory,
            authentication=authentication_mode,
            status=status,
            created_at=now,
            updated_at=now,
            pipeline_count=len(pipelines),
            pipelines=pipelines,
            dataset_count=len(datasets),
            datasets=datasets,
            linked_service_count=len(linked_services),
            linked_services=linked_services,
            trigger_count=len(triggers),
            triggers=triggers,
            dataflow_count=len(dataflows),
            dataflows=dataflows,
            integration_runtime_count=len(integration_runtimes),
            integration_runtimes=integration_runtimes,
            artifact_count=total,
            collections_queried=queried,
            detail=detail,
            provenance=dict(_PROVENANCE),
        )

        # Timestamp semantics: created_at is set once; updated_at bumps only when
        # the semantic content changes (timestamps excluded from comparison).
        if self.store.has_adf_discovery(project_id):
            try:
                existing = self.store.load_adf_discovery(project_id)
            except Exception:  # noqa: BLE001 - a corrupt prior record is replaced
                existing = None
            if existing is not None:
                discovery.created_at = existing.created_at
                if _semantic(existing) == _semantic(discovery):
                    discovery.updated_at = existing.updated_at
                else:
                    discovery.updated_at = now
        return discovery


def _auth_mode(access: AdfAccess) -> str:
    """The authentication MODE string (never a secret) for the record."""
    if access.credential is None:
        return ""
    return (access.credential.auth or "").strip().lower()


def _classify_status(status_code: int | None) -> AdfDiscoveryStatus:
    """Map an ARM HTTP status to a distinct, deterministic discovery status."""
    if status_code == 401:
        return AdfDiscoveryStatus.authentication_failed
    if status_code == 403:
        return AdfDiscoveryStatus.permission_denied
    if status_code == 404:
        return AdfDiscoveryStatus.factory_not_found
    return AdfDiscoveryStatus.connectivity_failed


def _http_detail(status_code: int | None, factory: str) -> str:
    """A safe, secret-free explanation for a non-200 ARM response."""
    if status_code == 401:
        return "Authentication failed for Azure Resource Manager (HTTP 401)."
    if status_code == 403:
        return (
            f"Permission/authorization failure enumerating factory '{factory}' "
            "(HTTP 403)."
        )
    if status_code == 404:
        return f"Factory '{factory}' not found (HTTP 404)."
    return (
        f"Unexpected response enumerating factory '{factory}' (HTTP {status_code})."
    )


def _semantic(discovery: AdfDiscovery) -> dict:
    """Discovery content used to detect meaningful change (excludes timestamps)."""
    data = json.loads(discovery.model_dump_json())
    data.pop("created_at", None)
    data.pop("updated_at", None)
    return data

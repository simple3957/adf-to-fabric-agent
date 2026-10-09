"""The Consultation Agent — a deterministic readiness gate.

Responsibility (and hard boundary):

    load questionnaire -> validate the fixed migration definition
    -> classify capability fit -> validate connectivity across the five domains
    -> evaluate readiness (READY/BLOCKED) -> persist consultation.yaml -> STOP

CONSULTATION DOES NOT INSPECT THE SOURCE ESTATE. It never reads ``source/``, and
its connectors never enumerate ADF artifacts, SQL schemas/tables, or ADLS
paths, and never create Fabric artifacts. Establishing *what actually exists* is
Discovery's job, a later phase that consumes the handoff contract produced here.

Deterministic / no LLM / no inference: every persisted value is traceable to
explicit questionnaire input or project metadata. Connectivity validation does
require network access (that is its purpose).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml
from pydantic import ValidationError

from adf2fabric.agent.consultation.models import ConsultationRequest, ConsultationResult
from adf2fabric.agent.consultation.readiness import evaluate
from adf2fabric.connectivity import registry
from adf2fabric.connectivity.base import Connector, outcome
from adf2fabric.project.models import (
    SUPPORTED_MODES,
    AccessConfig,
    AccessDomain,
    AccessRequirement,
    CapabilityFit,
    ConnectivityOutcome,
    ConnectivityStatus,
    Consultation,
    Project,
    Questionnaire,
    UnsupportedRequirement,
)
from adf2fabric.project.store import ProjectNotFoundError, ProjectStore, slugify

# Connectivity statuses that mean the config + credential references were
# actually supplied (validation may still have failed afterwards).
_PROVIDED_STATUSES = {
    ConnectivityStatus.validated,
    ConnectivityStatus.failed,
    ConnectivityStatus.dependency_unavailable,
}

# Fixed evaluation order for the five current domains.
_DOMAINS = [
    AccessDomain.repository,
    AccessDomain.adf,
    AccessDomain.sql_server,
    AccessDomain.adls_gen2,
    AccessDomain.fabric,
]

_PROVENANCE = {
    "project_id": "intake_metadata",
    "source_commit_sha": "intake_metadata",
    "migration": "client_questionnaire",
    "requested_scope": "client_questionnaire",
    "success_criteria": "client_questionnaire",
    "constraints": "client_questionnaire",
    "exclusions": "client_questionnaire",
    "capability_fit": "client_questionnaire",
    "access": "explicit_configuration",
    "connectivity": "connectivity_validation",
    "readiness": "connectivity_validation",
}


class ConsultationAgent:
    def __init__(
        self,
        store: ProjectStore,
        connectors: Optional[dict[str, Connector]] = None,
    ):
        self.store = store
        # Optional injection point for tests; production uses the registry.
        self.connectors = connectors

    def run(self, request: ConsultationRequest) -> ConsultationResult:
        try:
            project_id = slugify(request.project_name)
            if not self.store.exists(project_id):
                return ConsultationResult(
                    success=False,
                    message="Consultation failed.",
                    error=(
                        f"No project '{project_id}' found. Run `adf2fabric intake` "
                        "first to establish the source repository."
                    ),
                )
            project = self.store.load(project_id)
            questionnaire = self._load_questionnaire(request.input_path)

            capability = self._capability_fit(questionnaire)
            outcomes = self._validate_connectivity(questionnaire.access, project)
            requirements = self._build_requirements(questionnaire.access, outcomes)
            readiness = evaluate(requirements, capability)

            consultation = self._assemble(
                project_id, project, questionnaire, capability, requirements, outcomes, readiness
            )
            self.store.save_consultation(project_id, consultation)

            verb = "READY" if readiness.status.value == "ready" else "BLOCKED"
            return ConsultationResult(
                success=True,
                consultation=consultation,
                message=f"Consultation complete for '{project_id}': {verb}.",
            )
        except (ProjectNotFoundError, _QuestionnaireError) as exc:
            return ConsultationResult(
                success=False, message="Consultation failed.", error=str(exc)
            )
        except Exception as exc:  # noqa: BLE001 - surface a clean failure
            return ConsultationResult(
                success=False, message="Consultation failed.", error=str(exc)
            )

    # --- questionnaire ------------------------------------------------------

    def _load_questionnaire(self, input_path: str) -> Questionnaire:
        path = Path(input_path)
        if not path.exists():
            raise _QuestionnaireError(f"Questionnaire input file not found: {input_path}")
        with path.open(encoding="utf-8") as f:
            data = yaml.safe_load(f)
        if not isinstance(data, dict):
            raise _QuestionnaireError(
                f"Questionnaire '{input_path}' must be a YAML mapping."
            )
        try:
            return Questionnaire.model_validate(data)
        except ValidationError as exc:
            raise _QuestionnaireError(
                f"Invalid consultation questionnaire:\n{exc}"
            ) from None

    # --- capability fit (deterministic classification of the request) -------

    def _capability_fit(self, questionnaire: Questionnaire) -> CapabilityFit:
        mode = questionnaire.migration.mode
        mode_supported = mode in SUPPORTED_MODES

        unsupported: list[UnsupportedRequirement] = []
        for extra in questionnaire.additional_requirements:
            if extra.kind not in SUPPORTED_MODES:
                unsupported.append(
                    UnsupportedRequirement(
                        description=extra.description,
                        kind=extra.kind.value,
                        reason=(
                            "Outside current like-for-like capability; deferred to a "
                            "future Assessment/Planning phase."
                        ),
                    )
                )

        return CapabilityFit(
            requested_mode=mode,
            mode_supported=mode_supported,
            currently_supported=mode_supported and not unsupported,
            unsupported_requirements=unsupported,
        )

    # --- connectivity -------------------------------------------------------

    def _connector(self, domain: AccessDomain) -> Connector:
        if self.connectors and domain.value in self.connectors:
            return self.connectors[domain.value]
        return registry.get_connector(domain.value)

    def _validate_connectivity(
        self, access: AccessConfig, project: Project
    ) -> dict[AccessDomain, ConnectivityOutcome]:
        results: dict[AccessDomain, ConnectivityOutcome] = {}
        for domain in _DOMAINS:
            domain_access = getattr(access, domain.value)
            if not domain_access.required:
                results[domain] = outcome(
                    domain,
                    ConnectivityStatus.not_required,
                    detail="Not required for this engagement.",
                    stamped=False,
                )
                continue
            connector = self._connector(domain)
            try:
                results[domain] = connector.validate(domain_access, project)
            except Exception as exc:  # noqa: BLE001 - a connector must not crash the gate
                results[domain] = outcome(
                    domain,
                    ConnectivityStatus.failed,
                    detail=f"Connectivity validation raised an error: {exc}",
                )
        return results

    def _build_requirements(
        self, access: AccessConfig, outcomes: dict[AccessDomain, ConnectivityOutcome]
    ) -> list[AccessRequirement]:
        requirements: list[AccessRequirement] = []
        for domain in _DOMAINS:
            domain_access = getattr(access, domain.value)
            conn = outcomes[domain]
            provided = domain_access.required and conn.status in _PROVIDED_STATUSES
            validated = conn.status == ConnectivityStatus.validated
            # Non-secret connection metadata + credential references, echoed for
            # the Discovery handoff (promoted fields are stored separately).
            config = domain_access.model_dump(
                mode="json", exclude={"required", "mandatory", "owner"}
            )
            requirements.append(
                AccessRequirement(
                    domain=domain,
                    required=domain_access.required,
                    mandatory=domain_access.mandatory,
                    provided=provided,
                    validated=validated,
                    owner=domain_access.owner,
                    config=config,
                    connectivity=conn,
                )
            )
        return requirements

    # --- assembly + persistence --------------------------------------------

    def _assemble(
        self,
        project_id,
        project,
        questionnaire,
        capability,
        requirements,
        outcomes,
        readiness,
    ) -> Consultation:
        now = datetime.now(timezone.utc)
        consultation = Consultation(
            project_id=project_id,
            source_commit_sha=project.repository.commit_sha,
            created_at=now,
            updated_at=now,
            migration=questionnaire.migration,
            requested_scope=questionnaire.requested_scope,
            success_criteria=questionnaire.success_criteria,
            constraints=questionnaire.constraints,
            exclusions=questionnaire.exclusions,
            capability_fit=capability,
            access=requirements,
            connectivity=[outcomes[d] for d in _DOMAINS],
            readiness=readiness,
            provenance=dict(_PROVENANCE),
        )

        # Timestamp semantics: created_at is set once; updated_at bumps only when
        # the semantic content changes (excluding timestamps).
        if self.store.has_consultation(project_id):
            try:
                existing = self.store.load_consultation(project_id)
            except Exception:  # noqa: BLE001 - a corrupt prior record is replaced
                existing = None
            if existing is not None:
                consultation.created_at = existing.created_at
                if _semantic(existing) == _semantic(consultation):
                    consultation.updated_at = existing.updated_at
                else:
                    consultation.updated_at = now
        return consultation


class _QuestionnaireError(Exception):
    """A clear, user-facing problem with the questionnaire input."""


def _semantic(consultation: Consultation) -> dict:
    """Consultation content used to detect meaningful change.

    Excludes the timestamps and every volatile ``validated_at`` so that
    re-running with identical input is recognised as *unchanged*.
    """
    data = json.loads(consultation.model_dump_json())
    data.pop("created_at", None)
    data.pop("updated_at", None)
    for conn in data.get("connectivity", []):
        conn.pop("validated_at", None)
    for req in data.get("access", []):
        conn = req.get("connectivity")
        if isinstance(conn, dict):
            conn.pop("validated_at", None)
    return data

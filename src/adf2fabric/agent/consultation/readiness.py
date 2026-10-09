"""Deterministic readiness engine.

A pure function over the computed access requirements and capability fit — no
I/O, no network — so it is fully unit-testable. It decides READY vs BLOCKED:

- A **mandatory** access requirement that is not validated is a blocker.
- An **optional** (non-mandatory) required requirement that is not validated is
  a warning, never a blocker.
- A requested migration **mode** outside the current like-for-like capability is
  a blocker (the engagement as framed cannot be serviced now).
- Unsupported *additional* requirements are recorded as warnings — captured and
  flagged, never executed, deferred to later phases.
"""

from __future__ import annotations

from adf2fabric.project.models import (
    AccessRequirement,
    Blocker,
    CapabilityFit,
    ConnectivityStatus,
    Readiness,
    ReadinessStatus,
)

_STATUS_REASON = {
    ConnectivityStatus.missing_config: "configuration is incomplete",
    ConnectivityStatus.missing_credential: "credential reference is missing or unset",
    ConnectivityStatus.dependency_unavailable: "required connectivity library is not installed",
    ConnectivityStatus.failed: "connectivity could not be validated",
    ConnectivityStatus.not_required: "not required",
}


def _reason(requirement: AccessRequirement) -> str:
    conn = requirement.connectivity
    label = requirement.domain.value
    if conn is None:
        return f"{label} connectivity was not evaluated."
    base = _STATUS_REASON.get(conn.status, "connectivity could not be validated")
    detail = f" ({conn.detail})" if conn.detail else ""
    return f"{label} {base}.{detail}".rstrip()


def evaluate(requirements: list[AccessRequirement], capability: CapabilityFit) -> Readiness:
    blockers: list[Blocker] = []
    warnings: list[str] = []

    if not capability.mode_supported:
        blockers.append(
            Blocker(
                requirement="capability",
                reason=(
                    f"Requested migration mode '{capability.requested_mode.value}' is "
                    "outside the current like-for-like capability."
                ),
            )
        )

    for unsupported in capability.unsupported_requirements:
        warnings.append(
            f"Unsupported requirement recorded (not executed): "
            f"{unsupported.description} [{unsupported.kind}]."
        )

    for req in requirements:
        if not req.required or req.validated:
            continue
        reason = _reason(req)
        if req.mandatory:
            blockers.append(Blocker(requirement=req.domain.value, reason=reason))
        else:
            warnings.append(f"Optional requirement not satisfied: {reason}")

    status = ReadinessStatus.blocked if blockers else ReadinessStatus.ready
    return Readiness(status=status, blockers=blockers, warnings=warnings)

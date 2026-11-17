"""The Fabric data pipeline schema the converter is allowed to emit.

The tables live in the master compatibility catalog (``adf2fabric/catalog/compatibility.json``, section ``fabric_schema``), encoded
from Microsoft's documentation (see ``SOURCES``):

* "DataPipeline definition" (Fabric REST API item definitions): the activity envelope, the policy limits, ``externalReferences``,
  and the ``typeProperties`` of each activity type.
* "Expression language" (Fabric Data Factory): system variables and the function set.
* "Activity overview": the 120-activity-per-pipeline limit.

Anything the converter emits that the documentation does NOT list is declared in ``INFERRED`` and surfaced in every conversion
record as a schema note, so a reader knows exactly which parts were validated against documentation and which were not. The
converter must never emit a Fabric activity type or property that is not in this schema.

A table is not a Fabric service: passing validation here means "consistent with the documented schema", not "the service accepted
it". Runtime executability is tracked separately.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from adf2fabric.catalog import master, source

_S = master()["fabric_schema"]
SOURCES = tuple(source(s)["url"] for s in _S["sources"])

ACTIVITY_LIMIT = _S["activity_limit"]  # per pipeline, nested activities included (Activity overview)
RETRY_INTERVAL_MIN, RETRY_INTERVAL_MAX = _S["retry_interval_seconds"]  # seconds (Activity Policy)
CONDITIONS = tuple(_S["conditions"])
ACTIVITY_STATES = tuple(_S["activity_states"])
ON_INACTIVE_MARK_AS = tuple(_S["on_inactive_mark_as"])
POLICY_KEYS = frozenset(_S["policy_keys"])
SECURE_POLICY_KEYS = frozenset(_S["secure_policy_keys"])  # SetVariable accepts only these

# Activity-level keys of the envelope (DataPipelineActivity) plus those documented per type.
ENVELOPE_KEYS = frozenset(_S["envelope_keys"])

# Pipeline-level properties. ``activities`` and ``description`` are documented; the rest are standard in exported pipelines but not in
# the definition page, so they are INFERRED. ``libraryVariables`` (alias -> {type, variableName, libraryName}, read as
# @pipeline().libraryVariables.<alias>) is the shape Fabric's getDefinition returned and a probe ran with (2026-11-16).
PIPELINE_PROPERTY_KEYS = frozenset(_S["pipeline_property_keys"])
INFERRED_PIPELINE_PROPERTIES = frozenset(_S["inferred_pipeline_properties"])
# Number is not readable from a pipeline (Microsoft); these are the library variable types a pipeline can read.
LIBRARY_VARIABLE_TYPES = tuple(_S["library_variable_types"])


@dataclass(frozen=True)
class FabricActivitySchema:
    type: str
    required: tuple[str, ...] = ()  # required typeProperties keys
    allowed: frozenset = field(default_factory=frozenset)  # documented typeProperties keys
    envelope_required: tuple[str, ...] = ()  # e.g. externalReferences
    envelope_extra: frozenset = field(default_factory=frozenset)  # documented extra envelope keys
    containers: tuple[str, ...] = ()  # typeProperties keys holding nested activities
    secure_policy_only: bool = False
    inferred_envelope: frozenset = field(default_factory=frozenset)  # emitted but not documented
    inferred_type_properties: frozenset = field(default_factory=frozenset)


def _s(type_: str, required=(), allowed=(), envelope_required=(), envelope_extra=(), containers=(),
       secure_policy_only=False, inferred_envelope=(), inferred_tp=()):
    return FabricActivitySchema(
        type_, tuple(required), frozenset(allowed) | frozenset(required) | frozenset(inferred_tp),
        tuple(envelope_required), frozenset(envelope_extra), tuple(containers), secure_policy_only,
        frozenset(inferred_envelope), frozenset(inferred_tp))


FABRIC_ACTIVITIES: dict[str, FabricActivitySchema] = {
    t: _s(t, a["required"], a["allowed"], a["envelope_required"], a["envelope_extra"], a["containers"], a["secure_policy_only"],
          a["inferred_envelope"], a["inferred_type_properties"]) for t, a in _S["activities"].items()}

# --- expression language (Fabric) -------------------------------------------------------------
FABRIC_FUNCTIONS = frozenset(_S["expression_functions"])
# Documented Fabric system variables (Expression language, "Pipeline scope").
FABRIC_PIPELINE_SYSTEM = frozenset(_S["pipeline_system_variables"])
# References with no documented Fabric equivalent. Never silently substituted.
UNSUPPORTED_ACCESSORS: dict[str, str] = dict(_S["unsupported_accessors"])
# Fabric inferred (not documented) parts the converter may emit — reported in every record.
INFERRED: tuple[str, ...] = tuple(_S["inferred"])

"""Source-independent catalog, dependency, manifest and validation models.

Every fact carries its provenance, so a report never presents an inferred or missing value as verified:
``verified`` (read from the source or its authored DDL), ``inferred`` (derived, e.g. columns from a Parquet export),
``missing`` (not available anywhere) and ``needs_live`` (only a live source connection can confirm it).
Secrets never reach these models: definitions are masked before they are stored.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class _M(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Provenance(str, Enum):
    verified = "verified"
    inferred = "inferred"
    missing = "missing"
    needs_live = "needs_live"


class ObjectKind(str, Enum):
    table = "table"
    external_table = "external_table"
    view = "view"
    procedure = "procedure"
    function = "function"
    other = "other"  # master key, external data source, credential ... (server-level, reported only)


class Column(_M):
    name: str
    source_type: str  # as authored, e.g. "varchar(50)", "decimal(10,2)", "datetime"
    nullable: Optional[bool] = None
    identity: bool = False
    provenance: Provenance = Provenance.verified


class CatalogObject(_M):
    schema_name: str
    name: str
    kind: ObjectKind
    columns: list[Column] = Field(default_factory=list)
    definition: Optional[str] = None  # masked source text (views, procedures, functions, other)
    properties: dict[str, Any] = Field(default_factory=dict)  # platform specifics: distribution, index, location ...
    row_count: Optional[int] = None
    row_count_source: str = ""  # e.g. "parquet export 2026-10-13" or "live"
    provenance: Provenance = Provenance.verified  # of the object's existence and structure
    sources: list[str] = Field(default_factory=list)  # artifacts the facts came from
    notes: list[str] = Field(default_factory=list)

    @property
    def key(self) -> str:
        return f"{self.schema_name}.{self.name}".lower()


class SourceCatalog(_M):
    schema_version: str = "warehouse-catalog/1"
    source_type: str  # e.g. "synapse_dedicated"
    mode: str  # "offline" | "live"
    discovered_at: datetime
    objects: list[CatalogObject] = Field(default_factory=list)
    secrets_masked: int = 0  # how many secret values were masked (never the values)
    notes: list[str] = Field(default_factory=list)

    def get(self, key: str) -> Optional[CatalogObject]:
        return next((o for o in self.objects if o.key == key.lower()), None)


class Resolution(str, Enum):
    static = "static"  # the object is named literally
    dynamic = "dynamic"  # resolved only at run time (expressions)
    broken = "broken"  # the asset should name an object but doesn't


class AdfReference(_M):
    object_key: Optional[str] = None  # schema.object (lower case) or None when unresolved
    asset_kind: str  # dataset | activity_query | foreach_items | stored_procedure
    asset: str  # dataset name or pipeline/activity
    pipeline: Optional[str] = None
    linked_service: Optional[str] = None
    resolution: Resolution = Resolution.static
    note: str = ""


class ManifestEntry(_M):
    source_key: str
    kind: ObjectKind
    target_schema: str
    target_name: str
    action: str  # create | exists | manual | skip
    adf_usage: str  # direct | indirect | standalone
    adf_references: list[str] = Field(default_factory=list)
    depends_on: list[str] = Field(default_factory=list)
    transformations: list[str] = Field(default_factory=list)  # type changes, dropped options ...
    ddl: Optional[str] = None  # dry-run DDL (tables only); never executed by the planner
    load_method: str = ""
    warnings: list[str] = Field(default_factory=list)
    validation: str = "not_run"  # match | differs | missing_in_target | not_run


class TargetObject(_M):
    schema_name: str
    name: str
    kind: ObjectKind
    columns: list[Column] = Field(default_factory=list)
    row_count: Optional[int] = None

    @property
    def key(self) -> str:
        return f"{self.schema_name}.{self.name}".lower()


class Check(_M):
    object_key: str
    check: str  # presence | columns | types | row_count
    status: str  # match | differs | missing | review | not_applicable
    detail: str = ""


class MigrationReport(_M):
    schema_version: str = "warehouse-migration-report/1"
    project_id: str
    created_at: datetime
    source: dict[str, Any] = Field(default_factory=dict)
    target: dict[str, Any] = Field(default_factory=dict)
    catalog_summary: dict[str, int] = Field(default_factory=dict)
    manifest: list[ManifestEntry] = Field(default_factory=list)
    references: list[AdfReference] = Field(default_factory=list)
    checks: list[Check] = Field(default_factory=list)
    unresolved: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
    adf: dict[str, Any] = Field(default_factory=dict)  # which ADF sources were read, counts, live-vs-Git differences
    scope: dict[str, Any] = Field(default_factory=dict)  # mode, why each object is in, why the rest are out
    deploy: Optional[dict[str, Any]] = None  # the deploy run (steps, status), when one ran

"""Migration configuration (YAML). Holds names and paths only: secrets are never accepted here."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from adf2fabric.connections.redaction import _is_secret_key


class _C(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SourceConfig(_C):
    adapter: str  # registered adapter name, e.g. synapse_offline
    database: Optional[str] = None  # source database name (flags ADF linked services that point elsewhere)
    options: dict[str, Any] = Field(default_factory=dict)  # adapter-specific (paths, pools ...)


class AdfLiveConfig(_C):
    subscription_id: str
    resource_group: str
    factory: str


class AdfConfig(_C):
    project: Optional[str] = None  # project whose stored ADF semantic record (from the Git repo) is analysed
    live: Optional[AdfLiveConfig] = None  # the live factory (read-only ARM GETs); preferred when both are set


class TargetConfig(_C):
    tenant_id: Optional[str] = None  # tenant of the Fabric workspace (cross-tenant guest access is fine)
    workspace: Optional[str] = None
    warehouse: Optional[str] = None
    sql_endpoint: Optional[str] = None
    read_only: bool = True  # the framework never deploys or loads while this is true
    create_if_missing: bool = False  # never create a Warehouse unless explicitly approved
    # Collation for a NEW Warehouse. Fabric's default is case-sensitive (Latin1_General_100_BIN2_UTF8); a source
    # with a case-insensitive collation keeps its behaviour with Latin1_General_100_CI_AS_KS_WS_SC_UTF8.
    collation: Optional[str] = None


class ScopeConfig(_C):
    mode: str = "adf_related"  # adf_related (ADF-used objects + definition dependencies) | all
    include: list[str] = Field(default_factory=list)  # schema.object; empty = everything
    exclude: list[str] = Field(default_factory=list)
    schema_map: dict[str, str] = Field(default_factory=dict)


class LoadingConfig(_C):
    offline_method: str = "copy_into_from_parquet_export"  # label shown in the manifest
    method: str = "copy_into"  # how deploy loads tables: copy_into | insert_from_parquet
    # Where COPY INTO reads each export file, e.g. a OneLake path through a Lakehouse shortcut:
    # https://onelake.dfs.fabric.microsoft.com/<ws-id>/<lakehouse-id>/Files/<shortcut>/<folder>/{file}
    # Placeholders: {file} (<schema>_<object>.parquet), {schema}, {name}.
    source_url_template: Optional[str] = None
    conflict_policy: str = "fail"  # fail | skip | if_empty (deploy); replace is not supported


class RepointConfig(_C):
    pipelines: list[str] = Field(default_factory=list)  # Fabric pipelines to point at the target Warehouse
    from_connections: list[str] = Field(default_factory=list)  # Fabric connection ids of the old source warehouse
    workspace: Optional[str] = None  # where the pipelines are (default: target.workspace)


class MigrationConfig(_C):
    project: str
    source: SourceConfig
    adf: AdfConfig = Field(default_factory=AdfConfig)
    target: TargetConfig = Field(default_factory=TargetConfig)
    scope: ScopeConfig = Field(default_factory=ScopeConfig)
    loading: LoadingConfig = Field(default_factory=LoadingConfig)
    repoint: RepointConfig = Field(default_factory=RepointConfig)
    default_varchar_length: int = 8000

    @model_validator(mode="before")
    @classmethod
    def _no_secrets(cls, data: Any) -> Any:
        def walk(v: Any, path: str) -> None:
            if isinstance(v, dict):
                for k, x in v.items():
                    if _is_secret_key(str(k)) and x not in (None, ""):
                        raise ValueError(f"Secret-like key '{path}{k}' in the migration config: use an environment "
                                         "variable or Key Vault reference outside the file.")
                    walk(x, f"{path}{k}.")
            elif isinstance(v, list):
                for x in v:
                    walk(x, path)

        walk(data, "")
        return data


def load(path: Path) -> MigrationConfig:
    return MigrationConfig.model_validate(yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {})

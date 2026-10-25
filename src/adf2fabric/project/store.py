"""Per-project persistence.

Each project lives under::

    projects/<id>/project.yaml       - Project record (identity + source metadata)
    projects/<id>/consultation.yaml  - Consultation record (readiness gate result)
    projects/<id>/source/            - Canonical local copy of the source repository

Slugified project names are used as the id so paths stay human-readable. Only
the ``Project`` and ``Consultation`` models are serialized, and neither holds a
credential, so no secret is ever written to disk here.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


import yaml
from pydantic import BaseModel

from adf2fabric.project.dependencies import DependencyInventory
from adf2fabric.project.validation import ValidationReport
from adf2fabric.project.workloads import WorkloadManifest
from adf2fabric.project.discovery import (
    AdfDiscovery,
    AdfSemanticDiscovery,
    AdlsDiscovery,
    AdlsPathsDiscovery,
    RepositoryDiscovery,
    SqlDiscovery,
    SqlStructuralDiscovery,
)
from adf2fabric.project.conversion import ProjectConversion
from adf2fabric.project.reconstruction import ProjectReconstruction
from adf2fabric.project.source import SourceManifest
from adf2fabric.project.models import Consultation, Project

# Repo root is three parents up from this file: src/adf2fabric/project/store.py
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PROJECTS_ROOT = REPO_ROOT / "projects"

# Name of the source subfolder inside a project directory. This is the
# canonical local copy of the fetched repository.
SOURCE_DIRNAME = "source"

# Subfolder holding Discovery outputs (an inventory alongside the immutable
# source, never a copy or rewrite of it).
DISCOVERY_DIRNAME = "discovery"


# The C (libyaml) safe loader reads the same YAML several times faster than the pure-Python one (a project's discovery
# record is read on most screens); it is equally safe. Without libyaml installed, the pure-Python loader is used.
_SAFE_LOADER = getattr(yaml, "CSafeLoader", yaml.SafeLoader)


def _safe_load(stream):
    return yaml.load(stream, Loader=_SAFE_LOADER)  # noqa: S506  # nosec B506: a safe loader, never the full one


def slugify(name: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", name.strip().lower()).strip("-")
    if not slug:
        raise ValueError(f"Project name {name!r} has no usable characters for an id")
    return slug


class ProjectPathError(OSError):
    """A project file could not be written. On Windows the usual cause is a path over 260 characters: say so, and what to do."""

    def __init__(self, target: Path, cause: OSError):
        n = len(str(target))
        if n >= 240 or getattr(cause, "winerror", None) in (3, 206):
            msg = (f"The project's file path is {n} characters long, over what Windows allows (260). Move the projects folder closer to the "
                   "drive root (projects_root in config/config.yaml), or turn on Windows long-path support, then run this step again.")
        else:
            msg = f"A project file could not be written ({type(cause).__name__}). Check the projects folder exists and is writable, and that the disk is not full."
        super().__init__(msg)


class ProjectNotFoundError(Exception):
    pass


class ProjectStore:
    def __init__(self, projects_root: Path = DEFAULT_PROJECTS_ROOT):
        self.projects_root = Path(projects_root)

    def project_dir(self, project_id: str) -> Path:
        return self.projects_root / project_id

    def source_dir(self, project_id: str) -> Path:
        """Canonical local copy of the source repository for this project."""
        return self.project_dir(project_id) / SOURCE_DIRNAME

    # --- Source manifest (how Intake obtained the source) --------------------

    def source_manifest_path(self, project_id: str) -> Path:
        return self.project_dir(project_id) / "source-manifest.yaml"

    def has_source_manifest(self, project_id: str) -> bool:
        return self.source_manifest_path(project_id).exists()

    def save_source_manifest(self, project_id: str, manifest: SourceManifest) -> Path:
        self.project_dir(project_id).mkdir(parents=True, exist_ok=True)
        data = json.loads(manifest.model_dump_json())
        path = self.source_manifest_path(project_id)
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, sort_keys=False)
        return path

    def load_source_manifest(self, project_id: str) -> SourceManifest:
        path = self.source_manifest_path(project_id)
        if not path.exists():
            raise ProjectNotFoundError(
                f"No source manifest for '{project_id}' under {self.projects_root}. "
                "Run `adf2fabric intake` first."
            )
        with path.open(encoding="utf-8") as f:
            data = _safe_load(f)
        return SourceManifest.model_validate(data)

    def source_root(self, project_id: str) -> Path:
        """Directory Discovery scans: ``source/`` or its recorded subdirectory.

        Equals :meth:`source_dir` unless Intake recorded a repository
        subdirectory in the source manifest.
        """
        base = self.source_dir(project_id)
        if self.has_source_manifest(project_id):
            try:
                sub = self.load_source_manifest(project_id).subdirectory
            except Exception:  # noqa: BLE001 - a corrupt manifest never blocks discovery
                sub = None
            if sub:
                return base / sub
        return base

    def exists(self, project_id: str) -> bool:
        return (self.project_dir(project_id) / "project.yaml").exists()

    def save(self, project: Project) -> Path:
        project_dir = self.project_dir(project.id)
        project_dir.mkdir(parents=True, exist_ok=True)
        # Round-trip through JSON so datetimes etc. serialize cleanly, then dump
        # as human-readable YAML.
        data = json.loads(project.model_dump_json())
        path = project_dir / "project.yaml"
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, sort_keys=False)
        return path

    def load(self, project_id: str) -> Project:
        path = self.project_dir(project_id) / "project.yaml"
        if not path.exists():
            raise ProjectNotFoundError(
                f"No project '{project_id}' under {self.projects_root}. "
                "Run `adf2fabric intake` first."
            )
        with path.open(encoding="utf-8") as f:
            data = _safe_load(f)
        return Project.model_validate(data)

    def list_ids(self) -> list[str]:
        if not self.projects_root.exists():
            return []
        return sorted(
            p.name
            for p in self.projects_root.iterdir()
            if p.is_dir() and (p / "project.yaml").exists()
        )

    # --- Consultation record ------------------------------------------------

    def consultation_path(self, project_id: str) -> Path:
        return self.project_dir(project_id) / "consultation.yaml"

    def has_consultation(self, project_id: str) -> bool:
        return self.consultation_path(project_id).exists()

    def save_consultation(self, project_id: str, consultation: Consultation) -> Path:
        project_dir = self.project_dir(project_id)
        project_dir.mkdir(parents=True, exist_ok=True)
        # Round-trip through JSON so datetimes/enums serialize cleanly, then dump
        # as human-readable YAML. No secrets are present on the model by design.
        data = json.loads(consultation.model_dump_json())
        path = self.consultation_path(project_id)
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, sort_keys=False)
        return path

    def load_consultation(self, project_id: str) -> Consultation:
        path = self.consultation_path(project_id)
        if not path.exists():
            raise ProjectNotFoundError(
                f"No consultation for '{project_id}' under {self.projects_root}. "
                "Run `adf2fabric consultation` first."
            )
        with path.open(encoding="utf-8") as f:
            data = _safe_load(f)
        return Consultation.model_validate(data)

    # --- Repository Discovery record ----------------------------------------

    def discovery_dir(self, project_id: str) -> Path:
        return self.project_dir(project_id) / DISCOVERY_DIRNAME

    def repository_discovery_path(self, project_id: str) -> Path:
        return self.discovery_dir(project_id) / "repository.yaml"

    def has_repository_discovery(self, project_id: str) -> bool:
        return self.repository_discovery_path(project_id).exists()

    def save_repository_discovery(
        self, project_id: str, discovery: RepositoryDiscovery
    ) -> Path:
        discovery_dir = self.discovery_dir(project_id)
        discovery_dir.mkdir(parents=True, exist_ok=True)
        # Round-trip through JSON so datetimes/enums serialize cleanly, then dump
        # as human-readable YAML. No secrets are present on the model by design.
        data = json.loads(discovery.model_dump_json())
        path = self.repository_discovery_path(project_id)
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, sort_keys=False)
        return path

    def load_repository_discovery(self, project_id: str) -> RepositoryDiscovery:
        path = self.repository_discovery_path(project_id)
        if not path.exists():
            raise ProjectNotFoundError(
                f"No repository discovery for '{project_id}' under "
                f"{self.projects_root}. Run `adf2fabric discovery` first."
            )
        with path.open(encoding="utf-8") as f:
            data = _safe_load(f)
        return RepositoryDiscovery.model_validate(data)

    # --- ADLS Gen2 Discovery record -----------------------------------------

    def adls_discovery_path(self, project_id: str) -> Path:
        return self.discovery_dir(project_id) / "adls.yaml"

    def has_adls_discovery(self, project_id: str) -> bool:
        return self.adls_discovery_path(project_id).exists()

    def save_adls_discovery(self, project_id: str, discovery: AdlsDiscovery) -> Path:
        discovery_dir = self.discovery_dir(project_id)
        discovery_dir.mkdir(parents=True, exist_ok=True)
        # Round-trip through JSON so datetimes/enums serialize cleanly, then dump
        # as human-readable YAML. No secrets are present on the model by design.
        data = json.loads(discovery.model_dump_json())
        path = self.adls_discovery_path(project_id)
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, sort_keys=False)
        return path

    def load_adls_discovery(self, project_id: str) -> AdlsDiscovery:
        path = self.adls_discovery_path(project_id)
        if not path.exists():
            raise ProjectNotFoundError(
                f"No ADLS discovery for '{project_id}' under {self.projects_root}. "
                "Run `adf2fabric discovery-adls` first."
            )
        with path.open(encoding="utf-8") as f:
            data = _safe_load(f)
        return AdlsDiscovery.model_validate(data)

    # --- ADLS Gen2 Discovery Phase 2 record (paths) -------------------------

    def adls_paths_discovery_path(self, project_id: str) -> Path:
        return self.discovery_dir(project_id) / "adls-paths.yaml"

    def has_adls_paths_discovery(self, project_id: str) -> bool:
        return self.adls_paths_discovery_path(project_id).exists()

    def save_adls_paths_discovery(
        self, project_id: str, discovery: AdlsPathsDiscovery
    ) -> Path:
        discovery_dir = self.discovery_dir(project_id)
        discovery_dir.mkdir(parents=True, exist_ok=True)
        # Round-trip through JSON so datetimes/enums serialize cleanly, then dump
        # as human-readable YAML. No secrets are present on the model by design.
        data = json.loads(discovery.model_dump_json())
        path = self.adls_paths_discovery_path(project_id)
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, sort_keys=False)
        return path

    def load_adls_paths_discovery(self, project_id: str) -> AdlsPathsDiscovery:
        path = self.adls_paths_discovery_path(project_id)
        if not path.exists():
            raise ProjectNotFoundError(
                f"No ADLS path discovery for '{project_id}' under "
                f"{self.projects_root}. Run `adf2fabric discovery-adls-paths` first."
            )
        with path.open(encoding="utf-8") as f:
            data = _safe_load(f)
        return AdlsPathsDiscovery.model_validate(data)

    # --- SQL Server Discovery record ----------------------------------------

    def sql_discovery_path(self, project_id: str) -> Path:
        return self.discovery_dir(project_id) / "sql.yaml"

    def has_sql_discovery(self, project_id: str) -> bool:
        return self.sql_discovery_path(project_id).exists()

    def save_sql_discovery(self, project_id: str, discovery: SqlDiscovery) -> Path:
        discovery_dir = self.discovery_dir(project_id)
        discovery_dir.mkdir(parents=True, exist_ok=True)
        # Round-trip through JSON so datetimes/enums serialize cleanly, then dump
        # as human-readable YAML. No secrets are present on the model by design.
        data = json.loads(discovery.model_dump_json())
        path = self.sql_discovery_path(project_id)
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, sort_keys=False)
        return path

    def load_sql_discovery(self, project_id: str) -> SqlDiscovery:
        path = self.sql_discovery_path(project_id)
        if not path.exists():
            raise ProjectNotFoundError(
                f"No SQL discovery for '{project_id}' under {self.projects_root}. "
                "Run `adf2fabric discovery-sql` first."
            )
        with path.open(encoding="utf-8") as f:
            data = _safe_load(f)
        return SqlDiscovery.model_validate(data)

    # --- SQL Server Discovery Phase 2 record (structural) -------------------

    def sql_structural_discovery_path(self, project_id: str) -> Path:
        return self.discovery_dir(project_id) / "sql-structural.yaml"

    def has_sql_structural_discovery(self, project_id: str) -> bool:
        return self.sql_structural_discovery_path(project_id).exists()

    def save_sql_structural_discovery(
        self, project_id: str, discovery: SqlStructuralDiscovery
    ) -> Path:
        discovery_dir = self.discovery_dir(project_id)
        discovery_dir.mkdir(parents=True, exist_ok=True)
        # Round-trip through JSON so datetimes/enums serialize cleanly, then dump
        # as human-readable YAML. No secrets are present on the model by design.
        data = json.loads(discovery.model_dump_json())
        path = self.sql_structural_discovery_path(project_id)
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, sort_keys=False)
        return path

    def load_sql_structural_discovery(self, project_id: str) -> SqlStructuralDiscovery:
        path = self.sql_structural_discovery_path(project_id)
        if not path.exists():
            raise ProjectNotFoundError(
                f"No SQL structural discovery for '{project_id}' under "
                f"{self.projects_root}. Run `adf2fabric discovery-sql-structural` first."
            )
        with path.open(encoding="utf-8") as f:
            data = _safe_load(f)
        return SqlStructuralDiscovery.model_validate(data)

    # --- ADF Live Discovery record ------------------------------------------

    def adf_discovery_path(self, project_id: str) -> Path:
        return self.discovery_dir(project_id) / "adf.yaml"

    def has_adf_discovery(self, project_id: str) -> bool:
        return self.adf_discovery_path(project_id).exists()

    def save_adf_discovery(self, project_id: str, discovery: AdfDiscovery) -> Path:
        discovery_dir = self.discovery_dir(project_id)
        discovery_dir.mkdir(parents=True, exist_ok=True)
        # Round-trip through JSON so datetimes/enums serialize cleanly, then dump
        # as human-readable YAML. No secrets are present on the model by design.
        data = json.loads(discovery.model_dump_json())
        path = self.adf_discovery_path(project_id)
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, sort_keys=False)
        return path

    def load_adf_discovery(self, project_id: str) -> AdfDiscovery:
        path = self.adf_discovery_path(project_id)
        if not path.exists():
            raise ProjectNotFoundError(
                f"No ADF discovery for '{project_id}' under {self.projects_root}. "
                "Run `adf2fabric discovery-adf` first."
            )
        with path.open(encoding="utf-8") as f:
            data = _safe_load(f)
        return AdfDiscovery.model_validate(data)

    # --- ADF Semantic Discovery record --------------------------------------

    def adf_semantic_discovery_path(self, project_id: str) -> Path:
        return self.discovery_dir(project_id) / "adf-semantic.yaml"

    def has_adf_semantic_discovery(self, project_id: str) -> bool:
        return self.adf_semantic_discovery_path(project_id).exists()

    def save_adf_semantic_discovery(
        self, project_id: str, discovery: AdfSemanticDiscovery
    ) -> Path:
        discovery_dir = self.discovery_dir(project_id)
        discovery_dir.mkdir(parents=True, exist_ok=True)
        # Round-trip through JSON so datetimes/enums serialize cleanly, then dump
        # as human-readable YAML. No secrets are present on the model by design.
        data = json.loads(discovery.model_dump_json())
        path = self.adf_semantic_discovery_path(project_id)
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, sort_keys=False)
        return path

    def load_adf_semantic_discovery(self, project_id: str) -> AdfSemanticDiscovery:
        path = self.adf_semantic_discovery_path(project_id)
        if not path.exists():
            raise ProjectNotFoundError(
                f"No ADF semantic discovery for '{project_id}' under "
                f"{self.projects_root}. Run `adf2fabric discovery-adf-semantic` first."
            )
        with path.open(encoding="utf-8") as f:
            data = _safe_load(f)
        return AdfSemanticDiscovery.model_validate(data)

    # --- Dependency inventory (built from the semantic record + the source manifest) ---------------------------

    def dependency_inventory_path(self, project_id: str) -> Path:
        return self.discovery_dir(project_id) / "dependencies.yaml"

    def dependency_report_path(self, project_id: str) -> Path:
        return self.discovery_dir(project_id) / "dependency-report.md"

    def has_dependency_inventory(self, project_id: str) -> bool:
        return self.dependency_inventory_path(project_id).exists()

    def save_dependency_inventory(self, project_id: str, inventory: DependencyInventory, report_markdown: str = "") -> Path:
        self.discovery_dir(project_id).mkdir(parents=True, exist_ok=True)
        path = self.dependency_inventory_path(project_id)
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(json.loads(inventory.model_dump_json()), f, sort_keys=False)
        if report_markdown:
            self.dependency_report_path(project_id).write_text(report_markdown, encoding="utf-8")
        return path

    def load_dependency_inventory(self, project_id: str) -> DependencyInventory:
        path = self.dependency_inventory_path(project_id)
        if not path.exists():
            raise ProjectNotFoundError(
                f"No dependency inventory for '{project_id}' under {self.projects_root}. "
                "Run `adf2fabric discovery-dependencies` first.")
        with path.open(encoding="utf-8") as f:
            return DependencyInventory.model_validate(_safe_load(f))

    # --- Workload migration manifest (Spark workloads: assessment, generated Fabric artifacts, evidence) ------------

    def workloads_dir(self, project_id: str) -> Path:
        return self.project_dir(project_id) / "workloads"

    def workload_manifest_path(self, project_id: str) -> Path:
        return self.workloads_dir(project_id) / "manifest.yaml"

    def workload_report_path(self, project_id: str) -> Path:
        return self.workloads_dir(project_id) / "report.md"

    def has_workload_manifest(self, project_id: str) -> bool:
        return self.workload_manifest_path(project_id).exists()

    def save_workload_manifest(self, project_id: str, manifest: WorkloadManifest, report_markdown: str = "") -> Path:
        self.workloads_dir(project_id).mkdir(parents=True, exist_ok=True)
        path = self.workload_manifest_path(project_id)
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(json.loads(manifest.model_dump_json()), f, sort_keys=False)
        if report_markdown:
            self.workload_report_path(project_id).write_text(report_markdown, encoding="utf-8")
        return path

    def load_workload_manifest(self, project_id: str) -> WorkloadManifest:
        path = self.workload_manifest_path(project_id)
        if not path.exists():
            raise ProjectNotFoundError(
                f"No workload manifest for '{project_id}' under {self.projects_root}. "
                "Run `adf2fabric workloads-assess` first.")
        with path.open(encoding="utf-8") as f:
            return WorkloadManifest.model_validate(_safe_load(f))

    # --- Migration validation report -----------------------------------------------------------------------------

    def validation_path(self, project_id: str) -> Path:
        return self.project_dir(project_id) / "validation" / "validation.yaml"

    def save_validation_report(self, project_id: str, report: ValidationReport) -> Path:
        path = self.validation_path(project_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(json.loads(report.model_dump_json()), f, sort_keys=False)
        return path

    def load_validation_report(self, project_id: str) -> ValidationReport:
        path = self.validation_path(project_id)
        if not path.exists():
            raise ProjectNotFoundError(f"No validation report for '{project_id}'. Run `adf2fabric validate` first.")
        with path.open(encoding="utf-8") as f:
            return ValidationReport.model_validate(_safe_load(f))

    # --- Fabric target provisioning records (target spec, state, plan, approval, run journal, verification) -------

    def provisioning_dir(self, project_id: str) -> Path:
        return self.project_dir(project_id) / "provisioning"

    def provisioning_path(self, project_id: str, name: str) -> Path:
        return self.provisioning_dir(project_id) / f"{name}.yaml"

    def has_provisioning(self, project_id: str, name: str) -> bool:
        return self.provisioning_path(project_id, name).exists()

    def save_provisioning(self, project_id: str, name: str, model: BaseModel) -> Path:
        """Atomically replace one provisioning record (write a temp file, then rename), so a crash never leaves half a journal."""
        self.provisioning_dir(project_id).mkdir(parents=True, exist_ok=True)
        path = self.provisioning_path(project_id, name)
        tmp = path.with_suffix(".yaml.tmp")
        with tmp.open("w", encoding="utf-8") as f:
            yaml.safe_dump(json.loads(model.model_dump_json()), f, sort_keys=False)
        tmp.replace(path)
        return path

    def load_provisioning(self, project_id: str, name: str, cls: Any) -> Any:
        path = self.provisioning_path(project_id, name)
        if not path.exists():
            raise ProjectNotFoundError(f"No provisioning '{name}' for '{project_id}'. Run `adf2fabric provision-plan` first.")
        with path.open(encoding="utf-8") as f:
            return cls.model_validate(_safe_load(f))

    # --- Conversion record + generated Fabric artifacts ----------------------

    def conversion_dir(self, project_id: str) -> Path:
        return self.project_dir(project_id) / "conversion"

    def conversion_path(self, project_id: str) -> Path:
        return self.conversion_dir(project_id) / "conversion.yaml"

    def has_conversion(self, project_id: str) -> bool:
        return self.conversion_path(project_id).exists()

    def save_conversion(self, project_id: str, conversion: ProjectConversion) -> Path:
        self.conversion_dir(project_id).mkdir(parents=True, exist_ok=True)
        # Round-trip through JSON so datetimes/enums serialize cleanly. No secrets exist
        # on the model by design (it is built from the secret-masked semantic record).
        data = json.loads(conversion.model_dump_json())
        path = self.conversion_path(project_id)
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, sort_keys=False)
        return path

    def load_conversion(self, project_id: str) -> ProjectConversion:
        path = self.conversion_path(project_id)
        if not path.exists():
            raise ProjectNotFoundError(
                f"No conversion for '{project_id}' under {self.projects_root}. "
                "Run `adf2fabric convert` first."
            )
        with path.open(encoding="utf-8") as f:
            data = _safe_load(f)
        return ProjectConversion.model_validate(data)

    def write_conversion_file(self, project_id: str, relative_path: str, text: str) -> Path:
        """Write a generated artifact under ``conversion/`` (never under ``source/``)."""
        target = (self.conversion_dir(project_id) / relative_path).resolve()
        root = self.conversion_dir(project_id).resolve()
        if root not in target.parents:
            raise ValueError(f"Refusing to write outside the conversion folder: {relative_path}")
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text, encoding="utf-8")
        except OSError as exc:
            raise ProjectPathError(target, exc) from None
        return target

    # --- Reconstruction record + proposed completion -------------------------

    def reconstruction_dir(self, project_id: str) -> Path:
        return self.project_dir(project_id) / "reconstruction"

    def reconstruction_path(self, project_id: str) -> Path:
        return self.reconstruction_dir(project_id) / "reconstruction.yaml"

    def has_reconstruction(self, project_id: str) -> bool:
        return self.reconstruction_path(project_id).exists()

    def save_reconstruction(self, project_id: str, record: ProjectReconstruction) -> Path:
        self.reconstruction_dir(project_id).mkdir(parents=True, exist_ok=True)
        data = json.loads(record.model_dump_json())
        path = self.reconstruction_path(project_id)
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, sort_keys=False)
        return path

    def load_reconstruction(self, project_id: str) -> ProjectReconstruction:
        path = self.reconstruction_path(project_id)
        if not path.exists():
            raise ProjectNotFoundError(
                f"No reconstruction for '{project_id}' under {self.projects_root}. "
                "Run `adf2fabric reconstruct` first."
            )
        with path.open(encoding="utf-8") as f:
            data = _safe_load(f)
        return ProjectReconstruction.model_validate(data)

    def write_reconstruction_file(self, project_id: str, relative_path: str, text: str) -> Path:
        """Write a reconstruction artifact under ``reconstruction/`` (never under ``source/``)."""
        root = self.reconstruction_dir(project_id).resolve()
        target = (root / relative_path).resolve()
        if root not in target.parents:
            raise ValueError(f"Refusing to write outside the reconstruction folder: {relative_path}")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
        return target

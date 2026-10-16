"""Spark compute inventory and the Fabric compute that would replace it.

Read-only and deterministic. It answers two questions about an ADF estate:

1. How much external Spark compute does it run on (Databricks clusters, Synapse Spark pools)?
2. What Fabric custom Spark pool + Environment would give an equivalent, and what cannot be carried over?

Sizing comes from the ADF source where ADF states it (a Databricks linked service's new-cluster settings, a Synapse
notebook activity's executor overrides) and from an optional *compute sources* file for what ADF only references
(an existing Databricks cluster id, a Synapse Spark pool name). Nothing is guessed: when a size is not available the
proposal says so and asks for it. Nothing is created here; each pool proposal carries the exact request body the
documented ``Create Workspace Custom Pool`` API takes.

Rules and the documents they rest on are listed in ``BASIS`` and copied into every plan.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from adf2fabric.agent.discovery.adf_semantic.pipeline_analysis import walk
from adf2fabric.project.compute import (
    ActivityTarget,
    ComputeGap,
    ComputePlan,
    ComputeUse,
    FabricEnvironmentProposal,
    FabricPoolProposal,
)
from adf2fabric.project.discovery import AdfActivity, AdfSemanticDiscovery, AdfSparkCompute

DATABRICKS_TYPES = ("DatabricksNotebook", "DatabricksSparkJar", "DatabricksSparkPython")
SYNAPSE_TYPES = ("SynapseNotebook", "SparkJob")

# Microsoft Learn, "Apache Spark compute for Data Engineering and Data Science" (node sizes): vCores per node.
# The names are the Create Workspace Custom Pool API's NodeSize enum.
FABRIC_NODE_SIZES: tuple[tuple[int, str], ...] = ((4, "Small"), (8, "Medium"), (16, "Large"), (32, "XLarge"), (64, "XXLarge"))
# Synapse executor / driver / pool node sizes, by vCores.
SYNAPSE_SIZE_VCORES = {"small": 4, "medium": 8, "large": 16, "xlarge": 32, "xxlarge": 64}
# Microsoft Learn, "Databricks Runtime release notes versions and compatibility": Apache Spark version per runtime.
DBR_SPARK = {"14.3": "3.5.0", "15.4": "3.5.0", "16.4": "3.5.2", "17.3": "4.0.0", "18": "4.1.0", "19": "4.2.0"}
# Microsoft Learn, "Apache Spark runtime in Fabric": runtime -> (Spark version, release stage).
FABRIC_RUNTIMES = {"1.3": ("3.5.5", "EOSA"), "2.0": ("4.1", "GA")}
# Legacy Azure D/DS (v1/v2) sizes are numbered by size index, not vCPU count.
_LEGACY_D_VCORES = {1: 1, 2: 2, 3: 4, 4: 8, 5: 16, 11: 2, 12: 4, 13: 8, 14: 16, 15: 20}

BASIS = [
    "Fabric node sizes and vCores (Small 4, Medium 8, Large 16, X-Large 32, XX-Large 64) and the 1 capacity unit = 2 Spark "
    "vCores ratio: learn.microsoft.com/fabric/data-engineering/spark-compute",
    "Custom pool request body (name, nodeFamily, nodeSize, autoScale, dynamicExecutorAllocation) and the admin-role "
    "requirement: learn.microsoft.com/rest/api/fabric/spark/custom-pools/create-workspace-custom-pool",
    "Fabric runtimes (1.3 = Spark 3.5.5 EOSA, 2.0 = Spark 4.1 GA): learn.microsoft.com/fabric/data-engineering/runtime",
    "Databricks Runtime -> Apache Spark version (supported releases only): "
    "learn.microsoft.com/azure/databricks/release-notes/runtime/",
    "In Fabric a pool has one head node (the driver) plus executors, 1:1 with nodes, so Databricks workers + 1 driver = "
    "Fabric nodes (documented for Fabric; the same rule is assumed for a Synapse pool's node count - not verified).",
    "vCPUs of an Azure VM size are read from its name (Standard_E8ds_v5 = 8; legacy D/DS v1/v2 by size index). "
    "Not independently verified against Azure's size tables - confirm for unusual sizes.",
    "A fixed-size cluster becomes a pool with autoscale min = max (unambiguous, behaves as a fixed size); the "
    "documented API does not say which field holds the node count when autoscale is disabled.",
    "Environment compute settings (Sparkcompute.yml) pair cores with memory by position in the two documented allowed-value "
    "lists (4/28g, 8/56g, 16/112g, 32/224g, 64/400g); only 4/28g appears in Microsoft's example. Native execution engine is "
    "written off, so a Photon source is never silently switched to it.",
    "Peak capacity units are peak Spark vCores / 2, before any burst; the SKU that fits depends on Microsoft's burst and "
    "concurrency rules, which this plan does not model.",
]


# --- sizing helpers -------------------------------------------------------------------------------------------------


def azure_vm_vcores(name: Optional[str]) -> tuple[Optional[int], bool]:
    """(vCPUs, is_gpu) from an Azure VM size name such as ``Standard_E8ds_v5``; vCPUs is None when not readable."""
    if not name:
        return None, False
    m = re.fullmatch(r"Standard_([A-Za-z]+?)(\d+)(?:-(\d+))?([a-z]*)(?:_v(\d+))?", name.strip(), re.IGNORECASE)
    if not m:
        return None, False
    family, size, constrained, version = m.group(1).upper(), int(m.group(2)), m.group(3), m.group(5)
    gpu = family.startswith("N")
    if family in ("D", "DS") and version in (None, "2"):
        return _LEGACY_D_VCORES.get(size), gpu
    return (int(constrained) if constrained else size), gpu


def fabric_node_size(vcores: int) -> tuple[Optional[str], Optional[int]]:
    """The smallest Fabric node that has at least ``vcores`` vCores; (None, None) when even XX-Large is too small."""
    for cores, label in FABRIC_NODE_SIZES:
        if vcores <= cores:
            return label, cores
    return None, None


def parse_databricks_runtime(version: Optional[str]) -> tuple[Optional[str], Optional[str], bool, bool, bool]:
    """(runtime 'major.minor', Apache Spark version, photon, ml, gpu) from e.g. ``14.3.x-photon-scala2.12``."""
    if not version:
        return None, None, False, False, False
    low = version.lower()
    m = re.match(r"(\d+)\.(\d+)", low)
    runtime = f"{m.group(1)}.{m.group(2)}" if m else None
    spark = DBR_SPARK.get(runtime or "") or (DBR_SPARK.get(m.group(1)) if m else None)
    return runtime, spark, "photon" in low, "-ml" in low or "ml-" in low, "gpu" in low


def suggest_runtime(spark_version: Optional[str]) -> tuple[str, list[str]]:
    """The Fabric runtime to use for a source Spark version, and why."""
    r13, r20 = FABRIC_RUNTIMES["1.3"][0], FABRIC_RUNTIMES["2.0"][0]
    if not spark_version:
        return "2.0", ["The source Apache Spark version is not known, so the GA runtime (2.0, Spark %s) is proposed. "
                       "Confirm the version the code was written for." % r20]
    m = re.match(r"(\d+)\.(\d+)", spark_version)
    major, minor = (int(m.group(1)), int(m.group(2))) if m else (0, 0)
    if (major, minor) == (3, 5):
        return "1.3", [f"Same Spark line (3.5; Fabric Runtime 1.3 is Spark {r13}). Microsoft lists Runtime 1.3 as EOSA (end "
                       f"of support announced) and Runtime 2.0 (Spark {r20}) as GA: plan the move to 2.0 after testing on "
                       "Spark 4."]
    if major == 4:
        extra = " The source is newer than Fabric's Spark 4.1; features added after 4.1 are not available." if minor > 1 else ""
        return "2.0", [f"Runtime 2.0 is Spark {r20}; the source runs Spark {spark_version}.{extra}"]
    return "1.3", [f"Fabric has no runtime for Spark {spark_version} in Microsoft's current runtime table "
                   f"(1.3 = Spark {r13}, 2.0 = Spark {r20}); the nearest, 1.3, is proposed. This is a Spark upgrade: test "
                   "for behavior changes."]


# --- the compute sources file ---------------------------------------------------------------------------------------


@dataclass
class _Sizing:
    node_type: Optional[str] = None
    driver_node_type: Optional[str] = None
    vcores: Optional[int] = None
    driver_vcores: Optional[int] = None
    min_workers: Optional[int] = None
    max_workers: Optional[int] = None
    runtime_version: Optional[str] = None
    spark_version: Optional[str] = None
    spark_conf: dict[str, str] = field(default_factory=dict)
    init_scripts: int = 0
    photon: bool = False
    gpu: bool = False
    libraries: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


@dataclass
class ComputeSources:
    """What the user supplied about computes ADF only references: Databricks clusters by id, Synapse pools by name."""

    databricks_clusters: dict[str, dict] = field(default_factory=dict)
    synapse_pools: dict[str, dict] = field(default_factory=dict)
    problems: list[str] = field(default_factory=list)
    path: Optional[str] = None


def load_sources(path: Optional[str]) -> ComputeSources:
    """Read the optional compute sources file. A problem is reported, never raised: the plan still builds without it."""
    if not path:
        return ComputeSources()
    out = ComputeSources(path=str(path))
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        out.problems.append(f"Could not read the compute sources file: {exc}")
        return out
    if not isinstance(data, dict):
        out.problems.append("The compute sources file must be a JSON object.")
        return out
    for key, target in (("databricks_clusters", out.databricks_clusters), ("synapse_spark_pools", out.synapse_pools)):
        block = data.get(key, {})
        if isinstance(block, dict):
            target.update({str(k): v for k, v in block.items() if isinstance(v, dict)})
        elif block:
            out.problems.append(f"'{key}' must be an object keyed by {'cluster id' if 'cluster' in key else 'pool name'}.")
    unknown = sorted(set(data) - {"databricks_clusters", "synapse_spark_pools"})
    if unknown:
        out.problems.append(f"Ignored unknown key(s): {', '.join(unknown)}.")
    return out


def _sizing_from_cluster_api(c: dict) -> _Sizing:
    """A Databricks Clusters API ``get`` response (node_type_id, num_workers | autoscale, spark_version, ...)."""
    s = _Sizing(node_type=c.get("node_type_id"), driver_node_type=c.get("driver_node_type_id"),
                runtime_version=c.get("spark_version"))
    auto = c.get("autoscale") if isinstance(c.get("autoscale"), dict) else None
    if auto:
        s.min_workers, s.max_workers = auto.get("min_workers"), auto.get("max_workers")
    elif isinstance(c.get("num_workers"), int):
        s.min_workers = s.max_workers = c["num_workers"]
    if isinstance(c.get("spark_conf"), dict):
        s.spark_conf = {str(k): str(v) for k, v in c["spark_conf"].items()}
    s.init_scripts = len(c["init_scripts"]) if isinstance(c.get("init_scripts"), list) else 0
    s.photon = str(c.get("runtime_engine", "")).upper() == "PHOTON"
    return s


def _sizing_from_pool(p: dict) -> _Sizing:
    """A Synapse Big Data pool (the ARM resource, or just its ``properties``)."""
    props = p.get("properties") if isinstance(p.get("properties"), dict) else p
    size = str(props.get("nodeSize", "")).lower()
    s = _Sizing(vcores=SYNAPSE_SIZE_VCORES.get(size), node_type=props.get("nodeSize"),
                runtime_version=str(props["sparkVersion"]) if props.get("sparkVersion") else None)
    s.spark_version = s.runtime_version
    auto = props.get("autoScale") if isinstance(props.get("autoScale"), dict) else {}
    if auto.get("enabled"):
        lo, hi = auto.get("minNodeCount"), auto.get("maxNodeCount")
    else:
        lo = hi = props.get("nodeCount")
    if isinstance(lo, int) and isinstance(hi, int):  # node counts include the driver, as in Fabric
        s.min_workers, s.max_workers = max(lo - 1, 0), max(hi - 1, 0)
    if isinstance(props.get("sparkConfigProperties"), dict):
        content = props["sparkConfigProperties"].get("content")
        if isinstance(content, str):
            for line in content.splitlines():
                parts = line.strip().split(None, 1)
                if len(parts) == 2 and parts[0].startswith("spark."):
                    s.spark_conf[parts[0]] = parts[1]
    return s


# --- gathering the inventory ----------------------------------------------------------------------------------------


def _slug(*parts: Any) -> str:
    return ":".join(re.sub(r"[^A-Za-z0-9_.-]+", "_", str(p)) for p in parts if p not in (None, ""))


def _library_names(raw: Any) -> list[str]:
    out: list[str] = []
    for lib in raw if isinstance(raw, list) else []:
        if not isinstance(lib, dict):
            continue
        for kind, val in lib.items():
            if kind in ("pypi", "cran") and isinstance(val, dict):
                out.append(f"{kind}:{val.get('package', '?')}")
            elif kind == "maven" and isinstance(val, dict):
                out.append(f"maven:{val.get('coordinates', '?')}")
            elif isinstance(val, str):
                out.append(f"{kind}:{val}")
    return out


def _ref_name(v: Any) -> Optional[str]:
    return v.get("referenceName") if isinstance(v, dict) and isinstance(v.get("referenceName"), str) else None


def _plain(v: Any) -> Optional[str]:
    return v if isinstance(v, str) else None


class _Inventory:
    def __init__(self, d: AdfSemanticDiscovery, selected: Optional[set[str]], sources: ComputeSources):
        self.d, self.sources = d, sources
        self.selected = selected
        self.ls = {x.name.lower(): x for x in d.linked_services}
        self.uses: dict[str, ComputeUse] = {}
        self.activity_log: list[tuple[str, AdfActivity, str]] = []  # (pipeline, activity, compute id)
        self.gaps: list[ComputeGap] = []
        self._parents: dict[str, set[str]] = {}
        for p in d.pipelines:
            for child in p.pipeline_dependencies:
                self._parents.setdefault(child.lower(), set()).add(p.name.lower())
        self._triggers: dict[str, set[str]] = {}
        for t in d.triggers:
            for pr in t.pipeline_refs:
                self._triggers.setdefault(pr.lower(), set()).add(t.name)

    # triggers that start a pipeline, directly or through a parent that invokes it
    def triggers_for(self, pipeline: str) -> list[str]:
        seen, stack, found = set(), [pipeline.lower()], set()
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            found |= self._triggers.get(cur, set())
            stack.extend(self._parents.get(cur, ()))
        return sorted(found)

    def gap(self, code: str, cid: str, message: str, action: str) -> None:
        if not any(g.code == code and g.compute_id == cid and g.message == message for g in self.gaps):
            self.gaps.append(ComputeGap(code=code, compute_id=cid, message=message, action=action))

    def build(self) -> None:
        for p in sorted(self.d.pipelines, key=lambda x: x.name.lower()):
            if self.selected is not None and p.name.lower() not in self.selected:
                continue
            for a in walk(p.activities):
                if a.type in DATABRICKS_TYPES:
                    self._databricks(p.name, a)
                elif a.type in SYNAPSE_TYPES:
                    self._synapse(p.name, a)
        for u in self.uses.values():
            u.activities, u.pipelines = sorted(set(u.activities)), sorted(set(u.pipelines))
            u.triggers = sorted({t for pn in u.pipelines for t in self.triggers_for(pn)})

    def _use(self, cid: str, **init: Any) -> ComputeUse:
        if cid not in self.uses:
            self.uses[cid] = ComputeUse(id=cid, **init)
        return self.uses[cid]

    def _attach(self, u: ComputeUse, pipeline: str, a: AdfActivity, libs: list[str]) -> None:
        u.activities.append(f"{pipeline}/{a.name}")
        u.pipelines.append(pipeline)
        self.activity_log.append((pipeline, a, u.id))
        for lib in libs:
            if lib not in u.libraries:
                u.libraries.append(lib)

    # -- Databricks ----------------------------------------------------------------------------------------------
    def _databricks(self, pipeline: str, a: AdfActivity) -> None:
        ls_name = a.linked_service_refs[0] if a.linked_service_refs else None
        ls = self.ls.get((ls_name or "").lower())
        libs = _library_names(a.raw_properties.get("libraries"))
        if ls is None or ls.compute is None:
            u = self._use(_slug("databricks", ls_name or "unknown", "unresolved"), platform="databricks",
                          source_ref=ls_name or "(no linked service)", mode="unspecified", sizing_known=False,
                          sizing_source="not_available")
            self._attach(u, pipeline, a, libs)
            self.gap("linked_service_missing", u.id, f"The Databricks linked service '{ls_name}' is not in the source, so "
                     "the cluster is unknown.", "Add the linked service to the source, or supply the cluster in the "
                     "compute sources file.")
            return
        c: AdfSparkCompute = ls.compute
        sizing, sizing_source = _Sizing(), "adf_linked_service"
        if c.mode == "new_cluster":
            sizing = _Sizing(node_type=c.node_type, driver_node_type=c.driver_node_type, min_workers=c.min_workers,
                             max_workers=c.max_workers, runtime_version=c.runtime_version, spark_conf=dict(c.spark_conf),
                             init_scripts=c.init_script_count)
            cid = _slug("databricks", ls.name, "new", c.node_type, f"{c.min_workers}-{c.max_workers}")
        elif c.mode == "existing_cluster":
            cid = _slug("databricks", ls.name, "existing", c.existing_cluster_id)
            api = self.sources.databricks_clusters.get(c.existing_cluster_id or "")
            if api is not None:
                sizing, sizing_source = _sizing_from_cluster_api(api), "compute_sources_file"
            else:
                sizing_source = "not_available"
        elif c.mode == "instance_pool":
            cid, sizing_source = _slug("databricks", ls.name, "pool", c.instance_pool_id), "not_available"
        else:
            cid, sizing_source = _slug("databricks", ls.name, "unspecified"), "not_available"
        u = self._use(cid, platform="databricks", source_ref=ls.name, mode=c.mode, sizing_known=False,
                      sizing_source=sizing_source)
        first = not u.activities
        self._attach(u, pipeline, a, libs)
        if first:
            self._fill_databricks(u, c, sizing)

    def _fill_databricks(self, u: ComputeUse, c: AdfSparkCompute, s: _Sizing) -> None:
        u.node_type, u.driver_node_type = s.node_type, s.driver_node_type
        u.min_workers, u.max_workers = s.min_workers, s.max_workers
        u.autoscale = s.min_workers is not None and s.max_workers is not None and s.min_workers != s.max_workers
        u.runtime_version = s.runtime_version
        u.spark_conf, u.init_script_count = s.spark_conf, s.init_scripts
        _, u.spark_version, u.photon, ml, u.gpu = parse_databricks_runtime(s.runtime_version)
        u.photon = u.photon or s.photon
        if c.unresolved_fields:
            u.notes.append("Authored as an expression or linked-service parameter, so not read: "
                           + ", ".join(c.unresolved_fields) + ".")
            self.gap("unresolved_cluster_setting", u.id, "Cluster settings are parameterized in ADF ("
                     + ", ".join(c.unresolved_fields) + ").", "Supply the values the pipeline passes at run time.")
        if u.sizing_source == "not_available":
            what = {"existing_cluster": f"existing cluster '{c.existing_cluster_id}'",
                    "instance_pool": f"instance pool '{c.instance_pool_id}'"}.get(c.mode, "an unspecified cluster")
            self.gap("sizing_unavailable", u.id, f"The linked service points at {what}; ADF does not hold its size.",
                     "Supply it in the compute sources file (a Databricks Clusters API 'get' response keyed by cluster "
                     "id), or export it from the Databricks workspace.")
            return
        if s.node_type is None or s.min_workers is None:
            u.notes.append("Node type or worker count missing from the cluster definition.")
            self.gap("sizing_incomplete", u.id, "The cluster definition has no node type or worker count.",
                     "Add them to the compute sources file.")
            return
        vc, gpu_w = azure_vm_vcores(s.node_type)
        dvc, gpu_d = azure_vm_vcores(s.driver_node_type) if s.driver_node_type else (vc, gpu_w)
        u.gpu = u.gpu or gpu_w or gpu_d
        if vc is None:
            self.gap("node_type_unresolved", u.id, f"Cannot read the vCPU count of node type '{s.node_type}'.",
                     "State its vCPUs in the compute sources file, or choose the Fabric node size by hand.")
            return
        u.vcores_per_node, u.driver_vcores = vc, dvc
        u.source_peak_vcores = (dvc or vc) + (s.max_workers or 0) * vc
        u.sizing_known = True
        if ml:
            u.notes.append("An ML runtime: its bundled ML libraries differ from the Fabric runtime's.")

    # -- Synapse -------------------------------------------------------------------------------------------------
    def _synapse(self, pipeline: str, a: AdfActivity) -> None:
        raw = a.raw_properties
        pool = _ref_name(raw.get("sparkPool")) or _ref_name(raw.get("targetBigDataPool"))
        ex, dr = _plain(raw.get("executorSize")), _plain(raw.get("driverSize"))
        n_ex = raw.get("numExecutors") if isinstance(raw.get("numExecutors"), int) else None
        override = ex is not None or dr is not None or n_ex is not None
        pdef = self.sources.synapse_pools.get(pool or "")
        if pdef is not None:
            s, source = _sizing_from_pool(pdef), "compute_sources_file"
            cid, mode = _slug("synapse", pool), "spark_pool"
        elif override and not pool:
            s, source = _Sizing(), "activity_override"
            cid, mode = _slug("synapse", "session", ex, dr, n_ex), "session_override"
        elif override:
            s, source = _Sizing(), "activity_override"
            cid, mode = _slug("synapse", pool, "override", ex, dr, n_ex), "session_override"
        else:
            s, source = _Sizing(), "not_available"
            cid, mode = _slug("synapse", pool or "unresolved"), "spark_pool" if pool else "unspecified"
        u = self._use(cid, platform="synapse", source_ref=pool or "(none)", mode=mode, sizing_known=False,
                      sizing_source=source)
        first = not u.activities
        self._attach(u, pipeline, a, [])
        if override:
            u.notes.append(f"Activity '{pipeline}/{a.name}' overrides the session: executorSize={ex}, driverSize={dr}, "
                           f"numExecutors={n_ex}. The Fabric Notebook activity has no per-activity size; set it in the "
                           "pool/environment or with the notebook's configure cell.")
            self.gap("synapse_session_override", cid, f"'{pipeline}/{a.name}' sets executor/driver size or count per run.",
                     "Fabric sizes sessions through the pool and environment; keep the override in the notebook "
                     "configuration if the run needs it.")
        if not first:
            return
        if source == "activity_override" and ex and n_ex is not None:
            vc = SYNAPSE_SIZE_VCORES.get(ex.lower())
            dvc = SYNAPSE_SIZE_VCORES.get((dr or ex).lower())
            if vc and dvc:
                s = _Sizing(node_type=ex, driver_node_type=dr or ex, vcores=vc, driver_vcores=dvc, min_workers=n_ex,
                            max_workers=n_ex)
        if raw.get("conf") or raw.get("sparkConfig"):
            u.notes.append("The activity carries Spark configuration (conf / sparkConfig) that is not read here.")
        if s.vcores is None or s.min_workers is None:
            if source != "not_available":
                u.notes.append("The size could not be fully determined from the activity.")
            self.gap("sizing_unavailable", cid, f"Synapse Spark pool '{pool or '(none)'}' has no size in the source: "
                     "ADF only references it.", "Supply the pool in the compute sources file under synapse_spark_pools "
                     "(its Big Data pool properties: nodeSize, autoScale or nodeCount, sparkVersion).")
            return
        u.node_type, u.driver_node_type = s.node_type, s.driver_node_type
        u.vcores_per_node, u.driver_vcores = s.vcores, s.driver_vcores or s.vcores
        u.min_workers, u.max_workers = s.min_workers, s.max_workers
        u.autoscale = s.min_workers != s.max_workers
        u.runtime_version, u.spark_version, u.spark_conf = s.runtime_version, s.spark_version, s.spark_conf
        u.source_peak_vcores = (u.driver_vcores or s.vcores) + s.max_workers * s.vcores
        u.sizing_known = True


# --- proposing Fabric compute ---------------------------------------------------------------------------------------


def _propose(inv: _Inventory) -> tuple[list[FabricPoolProposal], list[FabricEnvironmentProposal]]:
    pools: dict[tuple, FabricPoolProposal] = {}
    unsized: list[ComputeUse] = []
    pool_of: dict[str, str] = {}
    for u in sorted(inv.uses.values(), key=lambda x: x.id):
        if u.gpu:
            inv.gap("gpu_not_available", u.id, "The source compute uses GPU nodes. The documented Fabric custom pool API "
                    "offers only the MemoryOptimized node family.", "Keep this workload on its current platform, or "
                    "choose a non-GPU design.")
            pools[("gpu", u.id)] = FabricPoolProposal(name=f"(no equivalent for {u.id})", status="not_possible",
                                                       replaces=[u.id], rationale=["GPU nodes have no Fabric pool equivalent."])
            continue
        if not u.sizing_known or u.vcores_per_node is None:
            unsized.append(u)
            continue
        size, cores = fabric_node_size(u.vcores_per_node)
        if size is None:
            inv.gap("exceeds_largest_node", u.id, f"{u.vcores_per_node} vCores per node is larger than Fabric's largest "
                    "node (XX-Large, 64).", "Spread the work over more, smaller nodes or keep it on the source platform.")
            pools[("big", u.id)] = FabricPoolProposal(name=f"(too large: {u.id})", status="not_possible", replaces=[u.id],
                                                       rationale=["No Fabric node size has that many vCores."])
            continue
        lo = max(1, (u.min_workers or 0) + 1)
        hi = max(lo, (u.max_workers or 0) + 1)
        key = (size, lo, hi)
        pool = pools.get(key)
        if pool is None:
            scaling = lo != hi
            pool = FabricPoolProposal(
                name=f"ADF-{size}-n{lo}" + (f"-{hi}" if scaling else ""), status="proposed", node_size=size,
                vcores_per_node=cores, min_nodes=lo, max_nodes=hi, peak_spark_vcores=hi * cores,
                peak_capacity_units=hi * cores / 2,
                request_body={
                    "name": f"ADF-{size}-n{lo}" + (f"-{hi}" if scaling else ""), "nodeFamily": "MemoryOptimized",
                    "nodeSize": size, "autoScale": {"enabled": True, "minNodeCount": lo, "maxNodeCount": hi},
                    "dynamicExecutorAllocation": {"enabled": scaling, "minExecutors": max(1, lo - 1),
                                                  "maxExecutors": max(1, hi - 1)}})
            pools[key] = pool
        pool.replaces.append(u.id)
        pool_of[u.id] = pool.name
        why = (f"{u.platform} {u.node_type} ({u.vcores_per_node} vCPU) x {u.min_workers}"
               + (f"-{u.max_workers}" if u.autoscale else "") + f" workers + driver -> {lo}"
               + (f"-{hi}" if lo != hi else "") + f" Fabric {size} node(s) ({cores} vCores each).")
        if cores != u.vcores_per_node:
            why += f" Rounded up from {u.vcores_per_node} to {cores} vCores to reach a Fabric node size."
        pool.rationale.append(why)
        if u.driver_vcores and u.vcores_per_node and u.driver_vcores > u.vcores_per_node:
            inv.gap("driver_larger_than_workers", u.id, f"The source driver ({u.driver_node_type}) is larger than its "
                    f"workers ({u.node_type}); a Fabric pool has one node size.", "Raise the pool's node size if the "
                    "workload needs a large driver (collect, broadcast joins).")
        if u.photon:
            u.notes.append("Photon is on. The nearest Fabric feature is the Native Execution Engine (spark.native.enabled, "
                           "Runtime 1.3 and 2.0); performance and operator coverage are not identical.")
    out_pools = [p for p in pools.values()]
    for u in unsized:
        out_pools.append(FabricPoolProposal(
            name=f"(needs sizing: {u.id})", status="needs_input", replaces=[u.id],
            rationale=["The size of the source compute is not available. Until it is known, run on the workspace's "
                       "Starter Pool (Medium nodes) and size a custom pool from measured usage."]))
    return out_pools, _environments(inv, pool_of)


_DBX_ONLY_PREFIX = "spark.databricks."


def _environments(inv: _Inventory, pool_of: dict[str, str]) -> list[FabricEnvironmentProposal]:
    groups: dict[tuple, FabricEnvironmentProposal] = {}
    for u in sorted(inv.uses.values(), key=lambda x: x.id):
        if u.gpu:
            continue
        runtime, notes = suggest_runtime(u.spark_version)
        props: dict[str, str] = {}
        dropped: list[str] = []
        for k, v in sorted(u.spark_conf.items()):
            if k.startswith(_DBX_ONLY_PREFIX) or k == "spark.master":
                dropped.append(k)
            elif v == "[masked]" or "{{secrets/" in v:
                inv.gap("secret_in_spark_conf", u.id, f"Spark setting '{k}' holds a secret or a secret-scope reference.",
                        "Read the secret from Azure Key Vault in the notebook instead of a Spark property.")
            else:
                props[k] = v
        if dropped:
            inv.gap("platform_spark_conf", u.id, "Databricks-specific Spark settings are not carried: "
                    + ", ".join(dropped) + ".", "Review each; most tune the Databricks engine and have no Fabric setting.")
        libs = list(u.libraries)
        custom = [x for x in libs if not x.startswith("pypi:")]
        if custom:
            inv.gap("custom_library", u.id, "Libraries that are not PyPI packages: " + ", ".join(custom) + ".",
                    "Upload the jar/wheel/egg to the Fabric Environment, or replace Maven/CRAN libraries by hand.")
        if u.init_script_count:
            inv.gap("init_scripts", u.id, f"{u.init_script_count} cluster init script(s) are not carried.",
                    "Re-express what each script does as Environment libraries, Spark properties or notebook code.")
        key = (pool_of.get(u.id), runtime, tuple(props.items()), tuple(sorted(libs)))
        env = groups.get(key)
        if env is None:
            env = FabricEnvironmentProposal(
                name=f"ENV-{runtime}-{len(groups) + 1}", runtime_version=runtime,
                runtime_spark_version=FABRIC_RUNTIMES[runtime][0], pool=pool_of.get(u.id), spark_properties=props,
                libraries=libs, notes=list(notes))
            groups[key] = env
        env.replaces.append(u.id)
    return list(groups.values())


_CONVERSION_TODAY = {"DatabricksNotebook": "reuse_externally", "DatabricksSparkJar": "reuse_externally",
                     "DatabricksSparkPython": "reuse_externally", "SynapseNotebook": "replace", "SparkJob": "replace"}
_JOB_TYPES = ("DatabricksSparkJar", "DatabricksSparkPython", "SparkJob")
_POOL_STATUS = {"proposed": "pending_approval", "needs_input": "needs_input", "not_possible": "not_possible"}


def _source_item(a: AdfActivity) -> Optional[str]:
    raw = a.raw_properties
    for key in ("notebookPath", "mainClassName", "pythonFile"):
        if isinstance(raw.get(key), str):
            return raw[key]
    return _ref_name(raw.get("notebook")) or _ref_name(raw.get("sparkJob"))


def _parameter_names(a: AdfActivity) -> list[str]:
    raw = a.raw_properties
    block = raw.get("baseParameters") if a.type == "DatabricksNotebook" else raw.get("parameters")
    return sorted(str(k) for k in block) if isinstance(block, dict) else []


def _targets(inv: _Inventory, pools: list[FabricPoolProposal],
             envs: list[FabricEnvironmentProposal]) -> list[ActivityTarget]:
    """Per activity: where it would run in Fabric and what is still needed. Pipeline conversion is not changed by this."""
    pool_for = {rid: p for p in pools for rid in p.replaces}
    env_of = {rid: e.name for e in envs for rid in e.replaces}
    out: list[ActivityTarget] = []
    for pipeline, a, cid in inv.activity_log:
        u, pool = inv.uses[cid], pool_for.get(cid)
        is_job = a.type in _JOB_TYPES
        blockers: list[str] = []
        if u.gpu:
            blockers.append("GPU nodes have no Fabric Spark pool equivalent.")
        if pool is not None and pool.status == "needs_input":
            blockers.append("The size of the source compute is not known yet.")
        reasons: list[str] = []
        if a.type.startswith("Databricks"):
            reasons.append("The code is not scanned yet: dbutils, mounts, Unity Catalog paths, secret scopes and "
                           "Databricks-only libraries must be reviewed by hand.")
        else:
            reasons.append("Synapse-specific code (mssparkutils, linked-service access, notebook magics) must be "
                           "reviewed by hand.")
        if is_job:
            reasons.append("The jar / Python file must be uploaded and a Spark Job Definition created; the tool does not "
                           "have the file.")
        else:
            reasons.append("The notebook source must be retrieved and imported as a Fabric notebook.")
        if u.libraries:
            reasons.append("Libraries: " + ", ".join(u.libraries) + ".")
        if u.init_script_count:
            reasons.append(f"{u.init_script_count} cluster init script(s) are not carried.")
        out.append(ActivityTarget(
            pipeline=pipeline, activity=a.name, activity_type=a.type or "", source_compute_id=cid,
            source_item=_source_item(a), parameters=_parameter_names(a),
            conversion_today=_CONVERSION_TODAY.get(a.type or "", "redesign"),
            target_execution="FabricSparkJobDefinition" if is_job else "FabricNotebook",
            spark_pool=pool.name if pool is not None and pool.status == "proposed" else None,
            environment=env_of.get(cid), requires_notebook_migration=not is_job, requires_job_definition=is_job,
            requires_compatibility_review=True, review_reasons=reasons, blockers=blockers,
            provisioning_status=_POOL_STATUS.get(pool.status, "needs_input") if pool is not None else "needs_input"))
    return sorted(out, key=lambda x: (x.pipeline.lower(), x.activity.lower()))


def build_compute_plan(d: AdfSemanticDiscovery, selected: Optional[set[str]] = None,
                       sources: Optional[ComputeSources] = None) -> Optional[ComputePlan]:
    """The plan for the pipelines in ``selected`` (lower-case names; None = all). None when the estate has no Spark compute."""
    sources = sources or ComputeSources()
    inv = _Inventory(d, selected, sources)
    inv.build()
    if not inv.uses:
        return None
    pools, envs = _propose(inv)
    proposed = [p for p in pools if p.status == "proposed"]
    used = []
    if sources.path:
        used.append(sources.path)
    return ComputePlan(
        targets=_targets(inv, pools, envs),
        inventory=sorted(inv.uses.values(), key=lambda x: x.id), pools=pools, environments=envs, gaps=inv.gaps,
        total_peak_spark_vcores=sum(p.peak_spark_vcores or 0 for p in proposed),
        max_single_pool_vcores=max((p.peak_spark_vcores or 0 for p in proposed), default=0),
        sources_used=used, sources_problems=list(sources.problems), basis=list(BASIS))


# --- the report section ---------------------------------------------------------------------------------------------


def _c(text: Any) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ")


def render_compute_section(plan: ComputePlan) -> list[str]:
    out = ["", "## Spark compute plan", "",
           "External Spark compute the pipelines run on, and the Fabric compute that would replace it. **Nothing here is "
           "created.** Sizes are from the ADF source or the compute sources file; where neither has one the proposal says "
           "so instead of guessing.", ""]
    for prob in plan.sources_problems:
        out.append(f"- Compute sources file: {prob}")
    if plan.targets:
        out += ["### Where each Spark activity would run in Fabric", "",
                "`convert` does not change these activities' compute: a Databricks notebook is still reused externally and a "
                "Synapse notebook is a scaffold. This is the plan for moving them.", "",
                "| Activity | Today | Fabric target | Pool | Environment | Still needed | Status |",
                "| --- | --- | --- | --- | --- | --- | --- |"]
        for x in plan.targets:
            need = ["notebook migration" if x.requires_notebook_migration else "Spark job definition",
                    "compatibility review"] + (["unblock: " + "; ".join(x.blockers)] if x.blockers else [])
            out.append(f"| `{_c(x.pipeline)}/{_c(x.activity)}` ({x.activity_type}) | {x.conversion_today} | {x.target_execution} | "
                       f"{_c(x.spark_pool or '-')} | {_c(x.environment or '-')} | {_c(', '.join(need))} | {x.provisioning_status} |")
        out.append("")
    out += ["### Compute in use", "",
            "| Compute | Platform | Node | Workers | Source peak vCores | Runtime | Activities | Triggers | Sizing from |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    for u in plan.inventory:
        workers = "-" if u.min_workers is None else (f"{u.min_workers}-{u.max_workers}" if u.autoscale else str(u.min_workers))
        out.append(f"| `{_c(u.id)}` | {u.platform} | {_c(u.node_type or '-')} ({u.vcores_per_node or '?'} vCPU) | {workers} | "
                   f"{u.source_peak_vcores or '?'} | {_c(u.runtime_version or '-')} (Spark {_c(u.spark_version or '?')}) | "
                   f"{len(u.activities)} in {len(u.pipelines)} pipeline(s) | {_c(', '.join(u.triggers) or '-')} | "
                   f"{u.sizing_source} |")
    for u in plan.inventory:
        for n in u.notes:
            out.append(f"- `{_c(u.id)}`: {n}")
    out += ["", "### Proposed Fabric custom Spark pools", ""]
    out += ["| Pool | Status | Node | Nodes (min-max) | Peak Spark vCores | Peak capacity units | Replaces |",
            "| --- | --- | --- | --- | --- | --- | --- |"]
    for p in plan.pools:
        node = f"{p.node_size} ({p.vcores_per_node} vCores)" if p.node_size else "-"
        nodes = (f"{p.min_nodes}-{p.max_nodes}" if p.min_nodes != p.max_nodes else str(p.min_nodes)) if p.min_nodes else "-"
        out.append(f"| `{_c(p.name)}` | {p.status} | {node} | "
                   f"{nodes} | {p.peak_spark_vcores or '-'} | {p.peak_capacity_units or '-'} | "
                   f"{_c(', '.join(f'`{r}`' for r in p.replaces))} |")
    if plan.max_single_pool_vcores:
        out += ["", f"Largest pool at peak: **{plan.max_single_pool_vcores} Spark vCores** "
                f"(= {plan.max_single_pool_vcores / 2:g} capacity units before burst). All proposed pools at peak together: "
                f"{plan.total_peak_spark_vcores} vCores. Pools rarely peak together; size the capacity from the concurrency "
                "you expect."]
    for p in plan.pools:
        for r in p.rationale:
            out.append(f"- `{_c(p.name)}`: {r}")
    if plan.environments:
        out += ["", "### Proposed Fabric Environments", "",
                "| Environment | Runtime | Pool | Spark properties | Libraries | Replaces |", "| --- | --- | --- | --- | --- | --- |"]
        for e in plan.environments:
            out.append(f"| `{e.name}` | {e.runtime_version} (Spark {e.runtime_spark_version}) | {_c(e.pool or '-')} | "
                       f"{len(e.spark_properties)} | {len(e.libraries)} | {_c(', '.join(f'`{r}`' for r in e.replaces))} |")
        for e in plan.environments:
            for n in e.notes:
                out.append(f"- `{e.name}`: {n}")
    if plan.gaps:
        out += ["", "### Needs a person", "", "| Code | Compute | Issue | Action |", "| --- | --- | --- | --- |"]
        for g in plan.gaps:
            out.append(f"| {g.code} | `{_c(g.compute_id)}` | {_c(g.message)} | {_c(g.action)} |")
    out += ["", "### Basis", ""] + [f"- {b}" for b in plan.basis]
    return out

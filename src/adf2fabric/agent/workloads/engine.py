"""The workload migration and compatibility assessment engine.

``build_manifest`` turns the compute inventory (the compute plan built from semantic discovery) into one manifest entry per
distinct Spark workload: it resolves the environment the workload ran in, retrieves its code only as far as the caller is
authorised, analyzes it, classifies it, and (when asked to generate) writes the Fabric artifact and checks it statically.

Nothing here talks to Fabric. Generating an artifact is not deploying it, and deploying it is not running it: those are
separate steps, and only a recorded Fabric run may set ``execution.status``.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urlparse

from adf2fabric.agent.conversion.compute_plan import (DATABRICKS_TYPES, SYNAPSE_TYPES, ComputeSources, build_compute_plan,
                                                      suggest_runtime)
from adf2fabric.agent.dependencies.access import TokenSource
from adf2fabric.agent.deployment import sjd_deploy
from adf2fabric.agent.deployment.fabric_deploy import DeployError
from adf2fabric.agent.discovery.adf_semantic.pipeline_analysis import walk
from adf2fabric.agent.workloads import analyzer, notebook_format, transform
from adf2fabric.agent.workloads.source import Code, SourceResolver
from adf2fabric.project.compute import ComputeUse
from adf2fabric.project.discovery import AdfActivity, AdfSemanticDiscovery
from adf2fabric.project.store import ProjectStore
from adf2fabric.project.workloads import (AlternativeOption, Approval, Classification as K, CodeSource, CodeStatus as C, ExecutionEnvironment,
                                          ExecutionEvidence, Finding, FindingCategory as F, GeneratedArtifact, GenerationEvidence,
                                          LibraryResolution, OutputContract, ParameterContract, RequiredResource, Rewrite, Severity as S,
                                          StaticCheck, WorkloadEntry, WorkloadKind as W, WorkloadManifest, WorkloadSummary)

# Where a notebook's exit value is in the Fabric pipeline activity output, and whether a live run confirmed it.
FABRIC_EXIT_PROPERTY = "output.result.exitValue"
FABRIC_EXIT_PROPERTY_VERIFIED = True  # confirmed by a live run on 2026-10-22: activityRun output.result.exitValue held the exit string
_ANALYZABLE = {C.retrieved, C.cached, C.in_project, C.supplied}
_UNSAFE = re.compile(r"[^A-Za-z0-9_.-]+")


@dataclass
class Options:
    compute_sources: Optional[str] = None
    tokens: TokenSource = field(default_factory=TokenSource)
    fetch: bool = False  # retrieve code from the source platform (needs a credential)
    local_files: dict[str, str] = field(default_factory=dict)  # source path -> local file with its code
    artifacts: dict[str, str] = field(default_factory=dict)  # source file / main class -> uploaded abfss:// URI
    secret_map: dict[str, str] = field(default_factory=dict)  # Databricks secret scope -> Key Vault URL
    lakehouse_id: Optional[str] = None  # default lakehouse for generated Spark job definitions
    pipelines: Optional[set[str]] = None  # lower-case names; None = all
    generate: bool = False  # write Fabric artifacts (explicit opt-in)
    client_factory: Optional[Callable[[str, str], Any]] = None
    storage_routes: Optional[Any] = None  # agent.storage.routes.Routes: rewrite literal ADLS paths to their OneLake shortcut (opt-in)


# =====================================================================================================================
# discovering the workloads
# =====================================================================================================================


def _text(v: Any) -> Optional[str]:
    return v if isinstance(v, str) else None


def _ref(v: Any) -> Optional[str]:
    return v.get("referenceName") if isinstance(v, dict) and isinstance(v.get("referenceName"), str) else None


def _slug(text: str) -> str:
    return _UNSAFE.sub("_", text).strip("_") or "workload"


@dataclass
class _Spec:
    id: str
    kind: W
    name: str
    source_item: str
    activity: AdfActivity
    pipeline: str
    used_by: list[str] = field(default_factory=list)
    passed: list[str] = field(default_factory=list)  # parameter names the pipeline passes (values never read)
    args: list[str] = field(default_factory=list)  # job arguments (Python / JAR), as literals only
    libraries: list[dict] = field(default_factory=list)
    compute_id: str = ""


def _literal_args(raw: Any) -> tuple[list[str], bool]:
    """(the literal string arguments, whether any argument is an expression)."""
    out, dynamic = [], False
    for x in raw if isinstance(raw, list) else []:
        if isinstance(x, str):
            out.append(x)
        else:
            dynamic = True
    return out, dynamic


def _specs(d: AdfSemanticDiscovery, plan: Any, selected: Optional[set[str]]) -> list[_Spec]:
    cid_of = {(t.pipeline, t.activity): t.source_compute_id for t in plan.targets}
    specs: dict[str, _Spec] = {}
    for p in sorted(d.pipelines, key=lambda x: x.name.lower()):
        if selected is not None and p.name.lower() not in selected:
            continue
        for a in walk(p.activities):
            raw = a.raw_properties
            t = a.type
            if t == "DatabricksNotebook":
                path = _text(raw.get("notebookPath"))
                item = path if path is not None else f"(dynamic: {p.name}/{a.name})"
                kind, name = W.databricks_notebook, (path or a.name).rstrip("/").rsplit("/", 1)[-1] or a.name
                passed = sorted(raw["baseParameters"]) if isinstance(raw.get("baseParameters"), dict) else []
                args: list[str] = []
            elif t == "DatabricksSparkPython":
                path = _text(raw.get("pythonFile"))
                item = path if path is not None else f"(dynamic: {p.name}/{a.name})"
                kind, name = W.databricks_python, (path or a.name).rstrip("/").rsplit("/", 1)[-1]
                passed, args = [], _literal_args(raw.get("parameters"))[0]
            elif t == "DatabricksSparkJar":
                item = _text(raw.get("mainClassName")) or f"(dynamic: {p.name}/{a.name})"
                kind, name = W.databricks_jar, item.rsplit(".", 1)[-1]
                passed, args = [], _literal_args(raw.get("parameters"))[0]
            elif t == "SynapseNotebook":
                item = _ref(raw.get("notebook")) or f"(dynamic: {p.name}/{a.name})"
                kind, name = W.synapse_notebook, item
                params = raw.get("parameters")
                passed, args = (sorted(params) if isinstance(params, dict) else []), []
            elif t == "SparkJob":
                item = _ref(raw.get("sparkJob")) or f"(dynamic: {p.name}/{a.name})"
                kind, name = W.synapse_spark_job, item
                passed, args = [], _literal_args(raw.get("args"))[0]
            else:
                continue
            wid = f"{kind.value}:{item.lower()}"
            spec = specs.get(wid)
            if spec is None:
                libs = raw.get("libraries") if isinstance(raw.get("libraries"), list) else []
                spec = specs[wid] = _Spec(wid, kind, name, item, a, p.name, passed=list(passed), args=args,
                                          libraries=[x for x in libs if isinstance(x, dict)],
                                          compute_id=cid_of.get((p.name, a.name), ""))
            spec.used_by.append(f"{p.name}/{a.name}")
            for x in passed:
                if x not in spec.passed:
                    spec.passed.append(x)
    return sorted(specs.values(), key=lambda s: s.id)


# =====================================================================================================================
# the execution environment
# =====================================================================================================================


def _environment(spec: _Spec, d: AdfSemanticDiscovery, plan: Any) -> ExecutionEnvironment:
    use: Optional[ComputeUse] = next((u for u in plan.inventory if u.id == spec.compute_id), None)
    platform = "databricks" if spec.kind.value.startswith("databricks") else "synapse"
    env = ExecutionEnvironment(platform=platform)
    if use is None:
        env.gaps.append("The compute this workload runs on could not be resolved from the source.")
        return env
    env.source_ref, env.mode, env.runtime_version, env.spark_version = use.source_ref, use.mode, use.runtime_version, use.spark_version
    env.node_type, env.max_workers, env.sizing_known = use.node_type, use.max_workers, use.sizing_known
    env.spark_conf, env.libraries = sorted(use.spark_conf), list(use.libraries)
    if platform == "databricks":
        ls = next((x for x in d.linked_services if x.name.lower() == use.source_ref.lower()), None)
        raw_ws = ls.compute.workspace if ls is not None and ls.compute is not None else None
        env.workspace = (urlparse(raw_ws if "://" in raw_ws else f"https://{raw_ws}").hostname or None) if raw_ws and "@" not in raw_ws else None
        if not env.workspace:
            env.gaps.append("The Databricks workspace host is not in the linked service.")
    if not use.sizing_known:
        env.gaps.append("The compute size is not known from the source" + (": supply --compute-sources." if use.sizing_source == "not_available" else "."))
    if not use.runtime_version:
        env.gaps.append("The source runtime version is not known, so runtime differences cannot be assessed.")
    env.fabric_runtime = suggest_runtime(use.spark_version)[0]
    for g in plan.gaps:
        if g.compute_id == use.id and g.message not in env.gaps:
            env.gaps.append(g.message)
    return env


# =====================================================================================================================
# libraries, parameters, outputs
# =====================================================================================================================


def _library_items(raw: list[dict]) -> list[tuple[str, str, str]]:
    """(name, kind, origin) for the libraries an activity declares."""
    out = []
    for lib in raw:
        for kind, val in lib.items():
            if kind in ("pypi", "cran") and isinstance(val, dict) and isinstance(val.get("package"), str):
                out.append((val["package"], kind, "activity"))
            elif kind == "maven" and isinstance(val, dict) and isinstance(val.get("coordinates"), str):
                out.append((val["coordinates"], "maven", "activity"))
            elif kind in ("jar", "whl", "egg", "requirements") and isinstance(val, str):
                out.append((val, kind, "activity"))
    return out


def _resolve_libraries(spec: _Spec, an: Optional[analyzer.Analysis], env: ExecutionEnvironment) -> list[LibraryResolution]:
    out: list[LibraryResolution] = []
    seen: set[tuple[str, str]] = set()

    def add(name: str, kind: str, origin: str, decision: str, note: str = "") -> None:
        if (name, origin) not in seen:
            seen.add((name, origin))
            out.append(LibraryResolution(name=name, origin=origin, kind=kind, decision=decision, note=note))

    for name, kind, origin in _library_items(spec.libraries):
        if kind == "pypi":
            add(name, "pypi", origin, "environment_public_library",
                "Add it to the Fabric Environment's public libraries. Compatibility with the chosen runtime's Python version is not checked here.")
        elif kind == "maven":
            add(name, "maven", origin, "needs_review", "Fabric Environments are documented for public (PyPI/conda) libraries and uploaded custom libraries; "
                "download the JAR from Maven and upload it as a custom library, then confirm its Scala version matches the runtime.")
        elif kind in ("jar", "whl", "egg"):
            add(name, kind, origin, "environment_custom_library", "The file lives in DBFS, which Fabric cannot read: upload it to the Environment as a custom library.")
        else:
            add(name, kind, origin, "needs_review", "Not a library type this tool resolves for Fabric.")
    for lib in env.libraries:  # cluster-level libraries
        if (lib, "activity") not in seen and not any(l.name == lib for l in out):
            add(lib, "pypi", "cluster", "needs_review", "Installed on the source cluster; decide whether the Fabric Environment needs it.")
    if an is not None:
        for req in an.pip_requirements:
            add(req, "pypi", "pip_magic", "environment_public_library", "Installed inline with %pip; works per session in Fabric, or move it to the Environment.")
        for mod in analyzer.third_party_imports(an.imports):
            add(mod, "pypi", "import", "needs_review", "Imported by the code. Confirm it is in the Fabric runtime's library list, or add it to the Environment.")
        for mod in sorted({i for i in an.imports if i in ("dlt", "dbruntime", "dbutils")}):
            add(mod, "databricks_only", "import", "not_available_in_fabric", "Exists only on Databricks clusters.")
    return sorted(out, key=lambda l: (l.origin, l.name))


def _parameters(spec: _Spec, an: Optional[analyzer.Analysis], tr: Optional[transform.Transformed]) -> list[ParameterContract]:
    names = list(spec.passed)
    declared = dict(an.widgets) if an is not None else {}
    for n in (tr.source_parameters if tr else ()):
        declared.setdefault(n, "")
    names += [w for w in declared if w not in names]
    in_cell = {n for n, _ in (tr.parameters if tr else [])}
    out = []
    for n in sorted(names):
        passed = n in spec.passed
        code_declares = n in declared
        mapped = n in in_cell
        note = ""
        if passed and an is not None and not code_declares:
            note = "The pipeline passes it but the code never reads it as a widget."
        elif code_declares and not mapped:
            note = "The code reads it, but it could not be turned into a parameter-cell variable (name clash or computed name)."
        out.append(ParameterContract(name=n, passed_by_pipeline=passed, declared_in_code=code_declares,
                                     has_default=declared.get(n) is not None, mapped=mapped, note=note))
    return out


def _outputs(spec: _Spec, d: AdfSemanticDiscovery) -> list[OutputContract]:
    pipeline_name, act_name = spec.used_by[0].split("/", 1)
    out: list[OutputContract] = []
    for user in spec.used_by:
        pn, an_ = user.split("/", 1)
        p = next((x for x in d.pipelines if x.name == pn), None)
        if p is None:
            continue
        for a in walk(p.activities):
            for e in a.expressions:
                for r in e.references:
                    if r.kind == "activity_output" and (r.name or "").lower() == an_.lower():
                        path = r.path or ""
                        is_run_output = ".runoutput" in path.lower()
                        out.append(OutputContract(
                            consumer=f"{pn}/{a.name}", source_property=f"activity('{an_}'){path}",
                            fabric_property=(FABRIC_EXIT_PROPERTY if is_run_output and spec.kind is W.databricks_notebook else None),
                            action=("Rewrite the expression to read the notebook exit value from "
                                    f"activity('{an_}').{FABRIC_EXIT_PROPERTY}" + ("" if FABRIC_EXIT_PROPERTY_VERIFIED else
                                                                                   " (this path is not yet confirmed by a live run)") + ".")
                            if is_run_output and spec.kind is W.databricks_notebook else
                            f"The Fabric activity output has no documented '{path or '.output'}' equivalent: review this expression."))
    uniq: dict[tuple, OutputContract] = {(o.consumer, o.source_property): o for o in out}
    return [uniq[k] for k in sorted(uniq)]


# =====================================================================================================================
# alternatives
# =====================================================================================================================

_ALTERNATIVES: dict[str, AlternativeOption] = {
    "delta_live_tables": AlternativeOption(
        summary="Rewrite the pipeline as Fabric notebooks: Structured Streaming or batch reads, Delta MERGE/overwrite writes, orchestrated by a Fabric pipeline.",
        limitations=["Declarative expectations, automatic dependency ordering and the DLT event log are not reproduced.", "Each table needs explicit write logic and a trigger."]),
    "auto_loader": AlternativeOption(
        summary="Use Structured Streaming's file source with a checkpoint location, or trigger a batch notebook from a storage event / schedule.",
        limitations=["Auto Loader's schema inference/evolution and file-notification mode are not available.", "Listing very large folders is slower than Auto Loader's incremental listing."]),
    "task_values": AlternativeOption(
        summary="Return the value with notebookutils.notebook.exit(value) and read it from the pipeline activity output.",
        limitations=["Only one string value is returned per run; structured values must be serialised (for example JSON)."]),
    "synapsesql": AlternativeOption(
        summary="Copy the dedicated pool's tables into a lakehouse (a pipeline Copy activity) and read them as Delta.",
        limitations=["The data is copied, so it can drift from the source until the copy is scheduled.", "The Fabric connector is user-identity only."]),
    "adls_gen1_path": AlternativeOption(summary="Move the data to ADLS Gen2 (or OneLake) and change the path.", limitations=["Requires a data migration first."]),
    "databricks_ml_service": AlternativeOption(
        summary="Keep this notebook on Azure Databricks (the existing default): the Fabric pipeline's Azure Databricks activity calls it.",
        limitations=["The workload keeps running on, and being billed by, Databricks."]),
    "dotnet_for_spark": AlternativeOption(
        summary="Port the code to PySpark or Scala (a rewrite, not a conversion).", limitations=["No automated translation exists; behaviour must be re-tested."]),
}
_KEEP_ON_SOURCE = AlternativeOption(
    summary="Keep running it where it is: the converted pipeline calls Azure Databricks (reuse_externally) as it does today.",
    limitations=["No Fabric compute is used for this workload; Databricks keeps billing it."])


# =====================================================================================================================
# classification
# =====================================================================================================================


_MISSING_INPUT = {"incomplete_definition", "unreadable_notebook"}  # blocker findings that mean "supply the input", not "no equivalent"


def _classify(entry: WorkloadEntry, an: Optional[analyzer.Analysis]) -> tuple[K, list[str]]:
    f = entry.findings
    blockers = [x for x in f if x.severity is S.blocker and x.code not in _MISSING_INPUT]
    if blockers:
        return K.unsupported, [f"{b.message}" for b in blockers[:3]]
    missing = [x for x in f if x.code in _MISSING_INPUT]
    if missing:
        return K.blocked, [m.message for m in missing[:3]]
    if entry.code.status not in _ANALYZABLE:
        if entry.code.status is C.binary:
            return (K.blocked if "jar_not_uploaded" in {x.code for x in f} else K.convertible_with_changes,
                    ["Compiled code cannot be scanned; it needs a Spark job definition pointing at an uploaded JAR."])
        if entry.kind is W.synapse_spark_job and entry.code.status is C.unsupported_location:
            return K.convertible_with_changes, ["The job's code sits in storage and was not read; the Spark job definition can still reference it."]
        return K.blocked, [f"The code is not available ({entry.code.status.value}): {entry.code.detail or 'no detail'}".strip()]
    alts = [x for x in f if x.severity is S.alternative]
    if alts:
        return K.requires_alternative, sorted({x.message for x in alts})[:3]
    changes = [x for x in f if x.severity is S.change]
    if changes or entry.rewrites or any(l.decision in ("environment_custom_library", "not_available_in_fabric") for l in entry.libraries):
        return K.convertible_with_changes, [f"{len(changes)} construct(s) need a change" + (f", {sum(r.count for r in entry.rewrites)} rewritten automatically." if entry.rewrites else ".")]
    return K.directly_convertible, ["No construct needs a change; review items still apply."]


# =====================================================================================================================
# per-kind assessment
# =====================================================================================================================


def _finding(code: str, cat: F, sev: S, message: str, action: str = "", construct: str = "") -> Finding:
    return Finding(code=code, category=cat, severity=sev, message=message, construct=construct, action=action)


def _runtime_findings(env: ExecutionEnvironment, is_jvm: bool) -> list[Finding]:
    out = []
    if env.spark_version and env.fabric_runtime:
        out.append(_finding("runtime_spark_version", F.runtime, S.info,
                            f"The source runs Apache Spark {env.spark_version}; the suggested Fabric runtime is {env.fabric_runtime}. "
                            "Spark behaviour differs between versions: run the workload's tests on the Fabric runtime."))
    elif not env.runtime_version:
        out.append(_finding("runtime_unknown", F.runtime, S.review, "The source runtime version is not known, so runtime differences cannot be assessed.",
                            "Supply the cluster details with --compute-sources."))
    if is_jvm:
        out.append(_finding("scala_version", F.runtime, S.review,
                            "A JAR is only compatible with the Scala/Java version it was built for. Fabric Runtime 1.3 has Scala 2.12.17 and Java 11; "
                            "Runtime 2.0 has Scala 2.13.16 and Java 21 (Microsoft's runtime table).", "Confirm which Scala version the JAR targets and pick the runtime that matches."))
    for k in env.spark_conf:
        if k.lower().startswith("spark.databricks."):
            out.append(_finding("cluster_databricks_conf", F.spark_config, S.info, f"The source cluster sets '{k}', a Databricks-only setting; it is not carried.", construct=k))
    return out


def _assess_notebook(spec: _Spec, code: Code, env: ExecutionEnvironment, opts: Options) -> tuple[Optional[analyzer.Analysis], Optional[transform.Transformed], list[Finding], Optional[list]]:
    """(analysis, transformed notebook, extra findings, parsed cells)."""
    if code.text is None:
        return None, None, [], None
    platform = env.platform
    try:
        if code.data is not None:
            cells, _ = notebook_format.parse_ipynb(code.data)
        else:
            cells, _ = notebook_format.load(code.text, code.filename or "notebook.py")
    except ValueError as exc:
        return None, None, [_finding("unreadable_notebook", F.source, S.blocker, f"The notebook source cannot be read: {exc}.")], None
    an = analyzer.analyze(cells, platform)
    tr = transform.transform(cells, an, platform, opts.secret_map, spec.passed)
    findings = list(an.findings)
    # mark what the generator fixed
    counts = {r.code: r.count for r in tr.rewrites}
    for fnd in findings:
        if fnd.code == "dbutils_secrets" and counts.get("secrets_get") and "dbutils_secrets" not in tr.unrewritten:
            fnd.rewritten, fnd.action = True, ""
        if fnd.code in ("databricks_spark_conf", "synapse_spark_conf") and counts.get("source_only_spark_conf"):
            fnd.rewritten, fnd.action = True, ""
    if opts.storage_routes:
        _route_storage(tr, findings, opts.storage_routes)
    # parameters read but not convertible
    ok = {n for n, _ in tr.parameters}
    for w in an.widgets:
        if w not in ok:
            findings.append(_finding("widget_not_convertible", F.contract, S.change, f"Widget '{w}' cannot become a parameter-cell variable "
                                     "(its name is not a plain identifier, or the code assigns the same name).", "Rename the variable and read the parameter by name.", w))
    if "dbutils_fs_mv_args" in tr.unrewritten:
        findings.append(_finding("dbutils_fs_mv_args", F.filesystem, S.change, "dbutils.fs.mv is called with a third argument; Fabric's mv(src, dest, create_path, overwrite) "
                                 "means something different, so it was not rewritten.", "Port the call by hand."))
    return an, tr, findings, cells


def _route_storage(tr: transform.Transformed, findings: list[Finding], routes: Any) -> None:
    """Rewrite literal ADLS paths in the generated cells to the OneLake shortcut the storage map planned for them."""
    total, locations = 0, set()
    for cell in tr.cells:
        if cell.get("cell_type") != "code":
            continue
        r = routes.rewrite("".join(cell["source"]))
        if r.routed:
            cell["source"] = r.text.splitlines(keepends=True)
            total += r.routed
            locations |= set(r.routed_locations)
    if not total:
        return
    tr.rewrites.append(Rewrite(code="storage_routed", count=total,
                               description=f"ADLS paths -> OneLake shortcut paths ({', '.join(sorted(locations))}); the shortcuts must exist before the notebook runs"))
    for fnd in findings:
        m = re.match(r"^abfss?://([\w.-]+)@([\w-]+)$", fnd.construct or "") if fnd.code == "adls_direct_path" else None
        if m and f"{m.group(2).lower()}/{m.group(1).lower()}" in locations:
            fnd.rewritten, fnd.action = True, "Create the OneLake shortcut (storage-map / create-shortcut) before running."


def _synapse_session(code: Code, env: ExecutionEnvironment) -> list[Finding]:
    out = []
    props = (code.data or {}).get("properties", {}) if isinstance(code.data, dict) else {}
    sess = props.get("sessionProperties") if isinstance(props.get("sessionProperties"), dict) else {}
    if sess:
        keys = ", ".join(sorted(sess))
        out.append(_finding("synapse_session_properties", F.spark_config, S.review,
                            f"The notebook sets session properties ({keys}). Fabric sizes sessions through the pool and Environment, or a %%configure cell as the first cell.",
                            "Reproduce the sizes in the Environment/pool the plan proposes."))
    pool = props.get("bigDataPool") if isinstance(props.get("bigDataPool"), dict) else {}
    if pool.get("referenceName"):
        env.source_ref = env.source_ref or str(pool["referenceName"])
    return out


def _assess_synapse_job(code: Code, spec: _Spec, opts: Options) -> tuple[list[Finding], dict]:
    """Findings and the resolved job properties of a Synapse Spark job definition."""
    props = (code.data or {}).get("properties", {}) if isinstance(code.data, dict) else {}
    jp = props.get("jobProperties") if isinstance(props.get("jobProperties"), dict) else {}
    findings: list[Finding] = []
    info = {"file": jp.get("file"), "className": jp.get("className"), "args": [a for a in jp.get("args", []) if isinstance(a, str)] if isinstance(jp.get("args"), list) else [],
            "jars": [j for j in jp.get("jars", []) if isinstance(j, str)] if isinstance(jp.get("jars"), list) else [],
            "pyFiles": [j for j in jp.get("pyFiles", []) if isinstance(j, str)] if isinstance(jp.get("pyFiles"), list) else [],
            "language": props.get("language")}
    if not jp.get("file"):
        findings.append(_finding("incomplete_definition", F.source, S.blocker, "The Spark job definition does not say what to run: it has no jobProperties.file in the source.",
                                 "Re-export the definition from Synapse (the file in the repository is incomplete)."))
    if str(props.get("language") or "").lower() in ("csharp", "dotnet", "dotnetspark"):
        findings.append(_finding("dotnet_for_spark", F.language, S.blocker, ".NET for Spark is not supported in Fabric Spark.", "Port the job to PySpark or Scala."))
    conf = jp.get("conf") if isinstance(jp.get("conf"), dict) else {}
    for k in sorted(conf):
        if k.lower().startswith(("spark.synapse.", "spark.storage.synapse.")):
            findings.append(_finding("synapse_spark_conf", F.spark_config, S.change, "A Synapse linked-service Spark setting; Microsoft documents it as removed in Fabric.", "Remove it.", k))
    if info["file"] and str(info["file"]).startswith(("abfss://", "wasbs://", "abfs://")):
        m = analyzer._RE_ABFSS.search(str(info["file"]))
        if m:
            findings.append(_finding("adls_direct_path", F.data_access, S.review, "The job's files are in ADLS; the identity that runs the job in Fabric needs access to the account.",
                                     "Grant the running identity data access.", f"abfss://{m.group(1)}@{m.group(2)}"))
    return findings, info


# =====================================================================================================================
# generation
# =====================================================================================================================


def _check(name: str, ok: bool, detail: str = "") -> StaticCheck:
    return StaticCheck(name=name, passed=ok, detail=detail)


def _notebook_checks(tr: transform.Transformed, entry: WorkloadEntry, ipynb: dict, an: analyzer.Analysis) -> list[StaticCheck]:
    import ast as _ast

    checks = []
    cells = ipynb["cells"]
    checks.append(_check("valid_ipynb", isinstance(cells, list) and ipynb.get("nbformat") == 4, f"{len(cells)} cells"))
    bad = []
    left = []
    for i, c in enumerate(cells):
        if c["cell_type"] != "code":
            continue
        src = "".join(c["source"])
        if src.lstrip().startswith(("%%", "%")):
            continue
        text = "\n".join("" if l.lstrip().startswith(("%", "!")) else l for l in src.split("\n"))
        try:
            _ast.parse(text)
        except SyntaxError as exc:
            bad.append(f"cell {i}: line {exc.lineno}")
        for ln, l in enumerate(text.split("\n"), 1):
            if re.search(r"\bdbutils\.", l) and not l.lstrip().startswith("#"):
                left.append(f"cell {i}:{ln}")
    if an.syntax_errors:
        checks.append(_check("python_syntax", False, "the source itself did not parse: not checked"))
    else:
        checks.append(_check("python_syntax", not bad, ", ".join(bad[:5])))
    checks.append(_check("no_databricks_utils_left", not left, ("still present at " + ", ".join(left[:5])) if left else ""))
    for p in entry.parameters:
        if p.mapped:
            checks.append(_check(f"parameter_declared:{p.name}", any(n == p.name for n, _ in tr.parameters), ""))
    if tr.parameters:
        tagged = [c for c in cells if c["cell_type"] == "code" and transform.PARAMETERS_TAG in c.get("metadata", {}).get("tags", [])]
        checks.append(_check("parameter_cell_tagged", len(tagged) == 1, "the 'parameters' tag was confirmed by a live Fabric run (2026-10-22)" if tagged else "no parameter cell"))
    size = len(json.dumps(ipynb))
    checks.append(_check("size_under_limit", size < 8 * 1024 * 1024, f"{size} bytes"))
    return checks


def _write(project_dir: Path, rel: str, content: bytes) -> tuple[str, str]:
    path = project_dir / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return rel.replace("\\", "/"), hashlib.sha256(content).hexdigest()


# =====================================================================================================================
# the entry point
# =====================================================================================================================


def build_manifest(store: ProjectStore, project_id: str, opts: Optional[Options] = None, now: Optional[datetime] = None) -> WorkloadManifest:
    opts = opts or Options()
    now = now or datetime.now(timezone.utc)
    d = store.load_adf_semantic_discovery(project_id)
    plan = build_compute_plan(d, opts.pipelines, _sources(opts))
    previous = store.load_workload_manifest(project_id) if store.has_workload_manifest(project_id) else None
    prior = {w.id: w for w in previous.workloads} if previous else {}
    entries: list[WorkloadEntry] = []
    if plan is not None:
        project_dir = store.project_dir(project_id)
        resolver = SourceResolver(project_dir, opts.tokens, opts.fetch, dict(opts.local_files), opts.client_factory)
        used_names: dict[str, str] = {}
        for spec in _specs(d, plan, opts.pipelines):
            entry = _assess(spec, d, plan, resolver, opts, project_dir, used_names, now)
            old = prior.get(entry.id)
            if old is not None and old.code.sha256 == entry.code.sha256:
                if not opts.generate and old.artifact.kind != "none" and entry.generation.status == "not_generated"                         and entry.classification in (K.directly_convertible, K.convertible_with_changes):
                    entry.artifact, entry.generation = old.artifact, old.generation  # an assessment does not forget what was generated
                if entry.artifact.sha256 is not None and entry.artifact.sha256 == old.artifact.sha256:
                    entry.approval, entry.artifact.fabric_item_id, entry.execution = old.approval, old.artifact.fabric_item_id, old.execution
            entries.append(entry)
    manifest = WorkloadManifest(project_id=project_id, source_commit_sha=d.source_commit_sha, created_at=previous.created_at if previous else now,
                                updated_at=now, workloads=sorted(entries, key=lambda e: e.id))
    manifest.summary = summarize(manifest)
    if plan is None:
        manifest.notes.append("The estate has no Databricks or Synapse Spark activity, so there are no workloads.")
    return manifest


def _sources(opts: Options) -> ComputeSources:
    from adf2fabric.agent.conversion.compute_plan import load_sources

    return load_sources(opts.compute_sources)


def summarize(m: WorkloadManifest) -> WorkloadSummary:
    s = WorkloadSummary(total=len(m.workloads))
    for w in m.workloads:
        s.by_classification[w.classification.value] = s.by_classification.get(w.classification.value, 0) + 1
        s.by_kind[w.kind.value] = s.by_kind.get(w.kind.value, 0) + 1
        s.generated += w.generation.status == "generated"
        s.generated_with_open_issues += w.generation.status == "generated_with_open_issues"
        s.not_generated += w.generation.status == "not_generated"
        s.executed_in_fabric += w.execution.status == "succeeded"
        s.approved += w.approval.status == "approved"
    s.by_classification, s.by_kind = dict(sorted(s.by_classification.items())), dict(sorted(s.by_kind.items()))
    return s


def _assess(spec: _Spec, d: AdfSemanticDiscovery, plan: Any, resolver: SourceResolver, opts: Options, project_dir: Path,
            used_names: dict[str, str], now: datetime) -> WorkloadEntry:
    env = _environment(spec, d, plan)
    raw = spec.activity.raw_properties
    findings: list[Finding] = []
    an: Optional[analyzer.Analysis] = None
    tr: Optional[transform.Transformed] = None
    job: dict = {}
    jar_uri: Optional[str] = None
    # 1. the code
    if spec.kind is W.databricks_notebook:
        code = resolver.databricks_notebook(env.workspace, _text(raw.get("notebookPath")))
    elif spec.kind is W.databricks_python:
        code = resolver.databricks_python(env.workspace, _text(raw.get("pythonFile")))
    elif spec.kind is W.databricks_jar:
        code = resolver.jar(spec.source_item)
        jar_uri = opts.artifacts.get(spec.source_item) or next((opts.artifacts[k] for k in opts.artifacts if k.lower() == spec.source_item.lower()), None)
    elif spec.kind is W.synapse_notebook:
        code = resolver.synapse_artifact("notebook", _ref(raw.get("notebook")))
    else:
        code = resolver.synapse_artifact("sparkJobDefinition", _ref(raw.get("sparkJob")))
    # 2. analysis
    cells = None
    if spec.kind in (W.databricks_notebook, W.synapse_notebook):
        an, tr, fs, cells = _assess_notebook(spec, code, env, opts)
        findings += fs
        if spec.kind is W.synapse_notebook and code.data is not None:
            findings += _synapse_session(code, env)
    elif spec.kind is W.databricks_python and code.text is not None:
        an = analyzer.analyze([notebook_format.Cell(0, "python", code.text, 1)], "databricks")
        findings += an.findings
        if any(f.code.startswith(("dbutils", "mssparkutils")) or f.code == "dbutils_secrets" for f in an.findings):
            findings.append(_finding("utils_in_job", F.api, S.change, "A Spark job cannot use dbutils/notebookutils (Microsoft: notebook utilities do not apply to Spark job definitions).",
                                     "Pass values as arguments and read secrets with the Azure SDK."))
    elif spec.kind is W.synapse_spark_job and code.data is not None:
        fs, job = _assess_synapse_job(code, spec, opts)
        findings += fs
        if job.get("file") and not str(job["file"]).startswith(("abfss://", "https://")):
            code.source = CodeSource(status=C.unsupported_location, location=str(job["file"]), detail="The job's file is not at an abfss:// location this tool can reference.")
        elif job.get("file"):
            code.source = CodeSource(status=C.unsupported_location, location=str(job["file"]),
                                     permission="Storage: a data-plane read role (for example Storage Blob Data Reader) on the container",
                                     detail="The job's code is in storage and was not read; the definition still references it.",
                                     action="Grant a read role and pass the file with --source to have its code scanned.")
    if spec.kind is W.databricks_jar and jar_uri is None:
        findings.append(_finding("jar_not_uploaded", F.source, S.change, "The JAR must be uploaded to OneLake/ADLS: a Fabric Spark job definition stores a reference to it.",
                                 "Upload it and pass --artifact <main class>=<abfss URI>."))
    findings += _runtime_findings(env, spec.kind is W.databricks_jar)
    libs = _resolve_libraries(spec, an, env)
    params = _parameters(spec, an, tr)
    outputs = _outputs(spec, d)
    # unresolved callee notebooks
    deps = (an.run_targets + an.notebook_run_targets) if an is not None else []
    entry = WorkloadEntry(
        id=spec.id, kind=spec.kind, name=spec.name, source_item=spec.source_item, used_by=sorted(spec.used_by), environment=env, code=code.source,
        classification=K.blocked, findings=sorted(findings, key=lambda f: (f.line or 0, f.code, f.construct)), rewrites=tr.rewrites if tr else [],
        libraries=libs, parameters=params, outputs=outputs)
    for target in sorted(set(deps)):
        entry.required_resources.append(RequiredResource(kind="notebook", name=target, reason="Called by this workload; it must be migrated and deployed too, and referenced by NAME."))
    if an is not None and an.accounts:
        for acct in an.accounts:
            entry.required_resources.append(RequiredResource(kind="identity_access", name=acct, reason="The code reads this storage account directly; the identity running the item needs data access (or use a shortcut)."))
    if any(f.code == "metastore_tables" for f in entry.findings):
        entry.required_resources.append(RequiredResource(kind="lakehouse", name="default lakehouse", reason="Metastore tables resolve against the notebook's default lakehouse."))
    if any(f.code in ("mount_point", "dbfs_path", "uc_volume") for f in entry.findings):
        entry.required_resources.append(RequiredResource(kind="shortcut", name="OneLake shortcut(s)", reason="Replace mounts / DBFS / volumes with shortcuts to the same storage."))
    if any(l.decision in ("environment_public_library", "environment_custom_library") for l in entry.libraries) or env.fabric_runtime:
        entry.required_resources.append(RequiredResource(kind="environment", name=f"runtime {env.fabric_runtime}", reason="Libraries and runtime version are set through a Fabric Environment."))
    if (an is not None and any(f.code == "dbutils_secrets" for f in entry.findings)) or tr and counts_has(tr, "secrets_get"):
        entry.required_resources.append(RequiredResource(kind="key_vault", name="Key Vault secrets", reason="Secrets are read with notebookutils.credentials.getSecret; the running identity needs secret access."))
    if spec.kind in (W.databricks_jar, W.databricks_python, W.synapse_spark_job):
        entry.required_resources.append(RequiredResource(kind="lakehouse", name="default lakehouse", reason="Every Fabric Spark job definition needs at least one lakehouse.",
                                                         status="provided" if opts.lakehouse_id else "needed"))
        if spec.kind is not W.synapse_spark_job:
            entry.required_resources.append(RequiredResource(kind="file_upload", name=spec.source_item, reason="The main file must be in OneLake/ADLS; the definition stores only its URI.",
                                                             status="provided" if (jar_uri or opts.artifacts.get(spec.source_item)) else "needed"))
    entry.classification, entry.classification_reasons = _classify(entry, an)
    # alternatives
    if entry.classification in (K.requires_alternative, K.unsupported, K.blocked):
        for code in sorted({f.code for f in entry.findings if f.severity in (S.alternative, S.blocker)}):
            if code in _ALTERNATIVES:
                entry.alternatives.append(_ALTERNATIVES[code])
        if spec.kind.value.startswith("databricks"):
            entry.alternatives.append(_KEEP_ON_SOURCE)
    # 3. generation
    _generate(entry, spec, code, tr, an, cells, job, jar_uri, opts, project_dir, used_names, now)
    entry.unresolved = _unresolved(entry)
    return entry


def counts_has(tr: transform.Transformed, key: str) -> bool:
    return any(r.code == key for r in tr.rewrites)


def _unresolved(e: WorkloadEntry) -> list[str]:
    out: list[str] = []
    if e.code.status not in _ANALYZABLE:
        out.append(f"Source code not available ({e.code.status.value}): {e.code.detail}".strip() + (f" Needs: {e.code.permission}." if e.code.permission else ""))
    groups: dict[str, list[Finding]] = {}
    for f in e.findings:
        if f.severity in (S.change, S.alternative, S.blocker) and not f.rewritten:
            groups.setdefault(f.code, []).append(f)
    for code, fs in sorted(groups.items()):
        lines = sorted({str(f.line) for f in fs if f.line})
        out.append(f"{fs[0].message}" + (f" (lines {', '.join(lines[:6])})" if lines else "") + (f" Action: {fs[0].action}" if fs[0].action else ""))
    for p in e.parameters:
        if p.note:
            out.append(f"Parameter '{p.name}': {p.note}")
    for o in e.outputs:
        out.append(f"Output contract: {o.consumer} reads {o.source_property}. {o.action}")
    for r in e.required_resources:
        if r.status == "needed" and r.kind in ("notebook", "file_upload", "lakehouse", "identity_access", "key_vault"):
            out.append(f"Needs {r.kind} '{r.name}': {r.reason}")
    for l in e.libraries:
        if l.decision in ("environment_custom_library", "not_available_in_fabric"):
            out.append(f"Library '{l.name}': {l.note}")
    for c in e.generation.checks:
        if not c.passed:
            out.append(f"Static check '{c.name}' failed. {c.detail}".strip())
    return out


def _generate(entry: WorkloadEntry, spec: _Spec, code: Code, tr: Optional[transform.Transformed], an: Optional[analyzer.Analysis],
              cells: Optional[list], job: dict, jar_uri: Optional[str], opts: Options, project_dir: Path, used_names: dict[str, str],
              now: datetime) -> None:
    ev = entry.generation
    if entry.classification in (K.requires_alternative, K.unsupported):
        ev.status, ev.reason = "not_generated", f"Classified {entry.classification.value}: no Fabric artifact is generated; see the alternatives."
        return
    if entry.classification is K.blocked:
        ev.status, ev.reason = "not_generated", "Blocked: " + (entry.classification_reasons[0] if entry.classification_reasons else "an input is missing.")
        return
    if not opts.generate:
        ev.status, ev.reason = "not_generated", "Assessment only: run workloads-generate to write the Fabric artifact."
        return
    slug = _slug(entry.name)
    display = entry.name
    if used_names.get(display.lower(), entry.id) != entry.id:  # two workloads with one display name
        display = f"{display}_{hashlib.sha1(entry.id.encode()).hexdigest()[:6]}"
        entry.unresolved.append(f"Two source workloads are named '{entry.name}'; this one is deployed as '{display}'.")
    used_names[display.lower()] = entry.id
    base = f"workloads/artifacts/{slug}"
    try:
        if spec.kind in (W.databricks_notebook, W.synapse_notebook) and tr is not None and an is not None:
            ipynb = transform.to_ipynb(tr.cells)
            rel, sha = _write(project_dir, f"{base}/{display}.ipynb", json.dumps(ipynb, indent=1, ensure_ascii=False).encode("utf-8"))
            entry.artifact = GeneratedArtifact(kind="fabric_notebook", file=rel, sha256=sha, display_name=display,
                                               configuration={"runtime": entry.environment.fabric_runtime or "", "parameter_cell": "yes" if tr.parameters else "none"})
            ev.checks = _notebook_checks(tr, entry, ipynb, an)
        else:
            _generate_job(entry, spec, code, job, jar_uri, opts, project_dir, base, display)
    except DeployError as exc:
        ev.status, ev.reason = "not_generated", str(exc)
        entry.classification = K.blocked
        entry.classification_reasons = [str(exc)]
        return
    ev.generated_at = now
    ev.status = "generated"  # downgraded below when anything is still open
    open_items = [c for c in ev.checks if not c.passed]
    if open_items or _unresolved_pre(entry):
        ev.status = "generated_with_open_issues"
        ev.reason = "The artifact was written, but items remain open (see unresolved)."


def _unresolved_pre(e: WorkloadEntry) -> bool:
    return bool(_unresolved(e))


def _generate_job(entry: WorkloadEntry, spec: _Spec, code: Code, job: dict, jar_uri: Optional[str], opts: Options, project_dir: Path,
                  base: str, display: str) -> None:
    if spec.kind is W.databricks_jar:
        if jar_uri is None:
            raise DeployError("The JAR has not been uploaded: pass --artifact <main class>=<abfss URI>.")
        props = sjd_deploy.build_properties(language="scala", executable_file=jar_uri, main_class=spec.source_item, arguments=spec.args,
                                            lakehouse_id=opts.lakehouse_id, library_uris=[opts.artifacts[l.name] for l in entry.libraries if l.name in opts.artifacts])
    elif spec.kind is W.databricks_python:
        uri = opts.artifacts.get(spec.source_item) or opts.artifacts.get(spec.source_item.rsplit("/", 1)[-1])
        if code.text is None or uri is None:
            raise DeployError("The Python file's code or its uploaded URI is missing: pass --source <file> and --artifact <file>=<abfss URI>.")
        _write(project_dir, f"{base}/{display}.py", code.text.encode("utf-8"))
        props = sjd_deploy.build_properties(language="python", executable_file=uri, main_class=None, arguments=spec.args,
                                            lakehouse_id=opts.lakehouse_id, library_uris=[])
    else:  # synapse spark job definition: reuse the definition's own file reference
        file, lang = job.get("file"), job.get("language") or ""
        if not file:
            raise DeployError("The Spark job definition has no main file.")
        props = sjd_deploy.build_properties(language=lang or "python", executable_file=str(file), main_class=job.get("className"),
                                            arguments=job.get("args", []), lakehouse_id=opts.lakehouse_id,
                                            library_uris=[u for u in job.get("jars", []) + job.get("pyFiles", []) if u.startswith(("abfss://", "https://"))])
    definition = sjd_deploy.build_definition(props)
    rel_def, sha = _write(project_dir, f"{base}/sjd-definition.json", json.dumps(definition, indent=2).encode("utf-8"))
    rel_props, _ = _write(project_dir, f"{base}/sjd-properties.json", json.dumps(props, indent=2).encode("utf-8"))
    entry.artifact = GeneratedArtifact(kind="fabric_spark_job_definition", file=rel_props, definition_file=rel_def, sha256=sha, display_name=display,
                                       configuration={"language": props["language"], "lakehouse": "set" if props["defaultLakehouseArtifactId"] else "missing"})
    entry.generation.checks = [_check("definition_valid", True, "passed the documented constraints"),
                               _check("lakehouse_attached", bool(props["defaultLakehouseArtifactId"]), "every Fabric Spark job definition needs a lakehouse"),
                               _check("main_file_is_abfss", props["executableFile"].startswith("abfss://") or props["executableFile"].startswith("https://"))]

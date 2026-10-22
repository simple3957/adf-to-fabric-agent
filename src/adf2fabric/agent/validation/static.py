"""STATIC validation: source files vs generated files. No network, no credentials, no clock.

Every function here answers one question about one property with PASS / FAIL / BLOCKED / NOT_RUN and the evidence. They read the
raw source JSON and the raw generated JSON themselves (``views``). A static PASS says the generated files are consistent with the
source and with the documented schema. It says nothing about whether Fabric accepts them or whether they run.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from adf2fabric.agent.conversion.fabric_schema import FABRIC_ACTIVITIES
from adf2fabric.agent.conversion.registry import rule_for
from adf2fabric.agent.conversion.validation import validate_content
from adf2fabric.agent.validation import views
from adf2fabric.agent.validation.diffing import diff, has_plaintext_secret, redact_value
from adf2fabric.agent.validation.views import ActView, SourceScan, TargetPipeline, TargetScan
from adf2fabric.project.discovery import AdfSemanticDiscovery
from adf2fabric.project.source import SourceManifest
from adf2fabric.project.validation import Category as K, CheckResult, ComponentComparison, ComponentStatus as S, Result as R, Stage
from adf2fabric.project.workloads import WorkloadManifest

_TOKEN = re.compile(r"^<(connection|item|workspace-id|gateway|database)[:>]")
_GUID = re.compile(r"^[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$")
_PARAM = re.compile(r"pipeline\(\)\.parameters\.([A-Za-z_]\w*)", re.I)
_VAR = re.compile(r"variables\('([^']+)'\)", re.I)
_ACT = re.compile(r"activity\('([^']+)'\)", re.I)
_REF_TYPES = {"DatasetReference": "dataset", "LinkedServiceReference": "linked_service", "PipelineReference": "pipeline",
              "DataFlowReference": "dataflow", "IntegrationRuntimeReference": "integration_runtime"}
CONNECTION_ACTIVITIES = {"copy", "lookup", "getmetadata", "delete", "webactivity", "script", "sqlserverstoredprocedure", "databricksnotebook"}
LIMIT = 8  # evidence lines kept per check


@dataclass
class Inputs:
    project_id: str
    src: SourceScan
    tgt: TargetScan
    paired: dict[str, TargetPipeline]
    unpaired: list[str]
    manifest: Optional[SourceManifest] = None
    discovery: Optional[AdfSemanticDiscovery] = None
    workloads: Optional[WorkloadManifest] = None
    provisioning: Any = None  # a VerificationReport, when provisioning was verified
    source_dir: Optional[Path] = None


@dataclass
class Collector:
    checks: list[CheckResult] = field(default_factory=list)
    components: dict[str, ComponentComparison] = field(default_factory=dict)

    def add(self, id_: str, category: K, result: R, summary: str, *, component: Optional[str] = None, evidence=(), remediation: str = "",
            required: bool = True, stage: Stage = Stage.static) -> None:
        self.checks.append(CheckResult(id=id_, category=category, stage=stage, result=result, summary=summary, component=component,
                                       evidence=[str(e)[:300] for e in list(evidence)[:LIMIT]] + (["..."] if len(list(evidence)) > LIMIT else []),
                                       remediation=remediation, required=required))

    def comp(self, id_: str, kind: str, status: S, source: Optional[str] = None, target: Optional[str] = None, diff_=(), note: str = "") -> None:
        old = self.components.get(id_)
        rank = [S.matched, S.not_applicable, S.transformed, S.unverified, S.altered, S.extra, S.missing]
        if old is not None and rank.index(old.status) >= rank.index(status):
            return  # the worst status wins
        self.components[id_] = ComponentComparison(id=id_, kind=kind, status=status, source=source, target=target, diff=list(diff_)[:LIMIT], note=note)


def _tokens(node: Any) -> list[str]:
    found: list[str] = []
    for path, v in __import__("adf2fabric.agent.validation.diffing", fromlist=["flatten"]).flatten(node):
        if isinstance(v, str) and _TOKEN.match(v):
            found.append(v)
    return sorted(set(found))


def _is_placeholder(a: ActView) -> bool:
    return a.type == "Fail" and str(a.type_properties.get("errorCode", "")).startswith("ADF2FABRIC_")


# =====================================================================================================================
# 1. source inventory completeness
# =====================================================================================================================


def source_inventory(inp: Inputs, c: Collector) -> None:
    src = inp.src
    if src.root is None:
        c.add("source_inventory.root", K.source_inventory, R.BLOCKED, "No ADF source folders were found, so nothing can be validated.",
              evidence=src.problems, remediation="Run intake first (projects/<name>/source must hold pipeline/, dataset/, linkedService/ ...).")
        return
    counts = {k: len(src.of(k)) for k in ("pipeline", "dataset", "linked_service", "trigger", "integration_runtime", "dataflow")}
    bad = [f"{f.path}: {f.error}" for f in src.files if f.data is None]
    c.add("source_inventory.readable", K.source_inventory, R.FAIL if bad else R.PASS,
          f"{len(bad)} source file(s) cannot be read as JSON." if bad else f"All {len(src.files)} source files are readable ({', '.join(f'{v} {k}' for k, v in counts.items() if v)}).",
          evidence=bad, remediation="Fix or re-export the unreadable files, then run discovery and conversion again." if bad else "")
    for f in src.files:
        if f.data is None:
            c.comp(f"{f.kind}:{f.name}", f.kind, S.unverified, note=f"the source file {f.path} is unreadable")
    m = inp.manifest
    if m is None:
        c.add("source_inventory.manifest", K.source_inventory, R.NOT_RUN, "There is no source manifest to compare the files with.", required=False)
    else:
        on_disk = {f.path for f in src.files}
        listed = {a.path for a in m.artifacts}
        # the manifest lists paths relative to the source root; compare in that form
        absent = sorted(listed - on_disk)
        extra = sorted(on_disk - listed)
        retrieval = [f"{x.kind} {x.name or ''}: {x.error}" for x in m.failures]
        if retrieval or not m.complete:
            c.add("source_inventory.manifest", K.source_inventory, R.BLOCKED, "The source was not fully retrieved, so completeness cannot be established.",
                  evidence=retrieval or ["the manifest says complete = false"], remediation="Resolve the retrieval failures listed in the manifest and run intake again.")
        elif absent or extra:
            c.add("source_inventory.manifest", K.source_inventory, R.FAIL, f"The files on disk differ from the manifest: {len(absent)} listed but absent, {len(extra)} present but not listed.",
                  evidence=[f"absent: {p}" for p in absent] + [f"unlisted: {p}" for p in extra], remediation="Run intake again; do not edit the imported source by hand.")
        else:
            c.add("source_inventory.manifest", K.source_inventory, R.PASS, f"All {len(listed)} manifest artifacts are present and nothing else is.")
    d = inp.discovery
    if d is None:
        c.add("source_inventory.discovery", K.source_inventory, R.NOT_RUN, "No semantic discovery exists to cross-check the file counts.", required=False)
        return
    pairs = [("pipeline", len(d.pipelines)), ("dataset", len(d.datasets)), ("linked_service", len(d.linked_services)), ("trigger", len(d.triggers)),
             ("integration_runtime", len(d.integration_runtimes)), ("dataflow", len(d.dataflows))]
    diffs = [f"{k}: {counts[k]} files, discovery recorded {n}" for k, n in pairs if counts.get(k, 0) != n]
    acts_raw = sum(len(views.pipeline_activities(f.data)) for f in src.of("pipeline") if f.data)
    acts_disc = sum(p.activity_total for p in d.pipelines)
    if acts_raw != acts_disc:
        diffs.append(f"activities: {acts_raw} found by an independent walk, discovery recorded {acts_disc}")
    c.add("source_inventory.discovery", K.source_inventory, R.FAIL if diffs else R.PASS,
          "Discovery and an independent read of the files disagree." if diffs else f"Discovery's inventory matches an independent read ({acts_raw} activities).",
          evidence=diffs, remediation="Run `discovery-adf-semantic` again; if it still differs, discovery has a defect." if diffs else "")


# =====================================================================================================================
# 2. reference integrity
# =====================================================================================================================


def _refs(node: Any) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    if isinstance(node, dict):
        if isinstance(node.get("referenceName"), str) and node.get("type") in _REF_TYPES:
            out.append((_REF_TYPES[node["type"]], node["referenceName"]))
        for v in node.values():
            out += _refs(v)
    elif isinstance(node, list):
        for v in node:
            out += _refs(v)
    return out


def reference_integrity(inp: Inputs, c: Collector) -> None:
    src = inp.src
    if src.root is None:
        return
    known = {k: set(src.names(k)) for k in ("pipeline", "dataset", "linked_service", "dataflow", "integration_runtime")}
    broken: list[str] = []
    for f in src.files:
        if f.data is None:
            continue
        for kind, name in _refs(f.data):
            if kind == "integration_runtime" and name.lower() == "autoresolveintegrationruntime":
                continue
            if name.lower() not in known.get(kind, set()):
                broken.append(f"{f.kind} '{f.name}' references {kind} '{name}', which is not in the source")
    incomplete = inp.manifest is not None and (inp.manifest.failures or not inp.manifest.complete)
    if broken and incomplete:
        c.add("reference_integrity.source", K.reference_integrity, R.BLOCKED, f"{len(broken)} reference(s) do not resolve, but the source was not fully retrieved, so they may exist.",
              evidence=broken, remediation="Resolve the source retrieval failures first, then validate again.")
    elif broken:
        c.add("reference_integrity.source", K.reference_integrity, R.FAIL, f"{len(broken)} reference(s) in the source point at components that are not in it.",
              evidence=broken, remediation="Add the missing definitions to the source (or fix the references); the migration cannot be complete without them.")
    else:
        c.add("reference_integrity.source", K.reference_integrity, R.PASS, "Every dataset, linked service, pipeline, data flow and runtime reference in the source resolves.")
    # target: unbound placeholders and internal references
    unbound: dict[str, list[str]] = {}
    for low, tp in inp.paired.items():
        if tp.content is not None:
            t = _tokens(tp.content)
            if t:
                unbound[tp.name] = t
    if unbound:
        c.add("reference_integrity.target_unbound", K.reference_integrity, R.BLOCKED,
              f"{len(unbound)} generated pipeline(s) still contain unbound placeholders (connection / item / workspace ids).",
              evidence=[f"{p}: {', '.join(t[:4])}" for p, t in sorted(unbound.items())],
              remediation="Create or bind the missing connections and items, record their ids in a resolutions file, and run `convert --resolutions` again.")
        for p in unbound:
            c.comp(f"pipeline:{p}", "pipeline", S.unverified, note="contains unbound placeholders")
    else:
        c.add("reference_integrity.target_unbound", K.reference_integrity, R.PASS, "No generated pipeline contains an unbound placeholder.")
    for low, tp in sorted(inp.paired.items()):
        if tp.content is None:
            continue
        acts = views.pipeline_activities(tp.content)
        names = {a.name.lower() for a in acts}
        props = tp.content.get("properties") or {}
        params, variables = {k.lower() for k in (props.get("parameters") or {})}, {k.lower() for k in (props.get("variables") or {})}
        problems: list[str] = []
        for a in acts:
            for pred in a.depends:
                if pred not in names:
                    problems.append(f"{a.name} depends on '{pred}', which does not exist")
            blob = " ".join(t for _, t in views.expressions(a.raw))
            problems += [f"{a.name} uses parameter '{m}' that is not declared" for m in _PARAM.findall(blob) if m.lower() not in params]
            problems += [f"{a.name} uses variable '{m}' that is not declared" for m in _VAR.findall(blob) if m.lower() not in variables]
            problems += [f"{a.name} reads activity '{m}', which does not exist" for m in _ACT.findall(blob) if m.lower() not in names]
        c.add("reference_integrity.target_internal", K.reference_integrity, R.FAIL if problems else R.PASS,
              f"{len(problems)} internal reference(s) in the generated pipeline do not resolve." if problems else "Every dependency, parameter, variable and activity reference in the generated pipeline resolves.",
              component=f"pipeline:{tp.name}", evidence=problems, remediation="Re-run conversion; if the problem persists the converter produced an inconsistent pipeline." if problems else "")


# =====================================================================================================================
# 3. target schema and artifact validity
# =====================================================================================================================


def target_schema(inp: Inputs, c: Collector) -> None:
    if not inp.tgt.pipelines and inp.unpaired:  # nothing was generated at all: conversion has not run, which is a missing precondition, not a defect
        c.add("target_schema.generated", K.target_schema, R.BLOCKED, "Nothing has been generated yet, so there is nothing to validate against the source.",
              evidence=inp.tgt.problems or [f"{len(inp.unpaired)} source pipeline(s) have no artifact"], remediation="Run `adf2fabric convert --project <name>` first.")
        for name in inp.unpaired:
            c.comp(f"pipeline:{name}", "pipeline", S.unverified, note="not converted yet")
        return
    if inp.unpaired:
        c.add("target_schema.generated", K.target_schema, R.FAIL, f"{len(inp.unpaired)} source pipeline(s) have no generated artifact.",
              evidence=inp.unpaired, remediation="Run `convert`; a pipeline with no artifact was not converted.")
    for name in inp.unpaired:
        c.comp(f"pipeline:{name}", "pipeline", S.missing, note="no generated artifact")
    src_names = {p.lower() for p in inp.src.names("pipeline")}
    for tp in inp.tgt.pipelines:
        if tp.name.lower() not in src_names and tp.name.lower() not in {p.name.lower() for p in inp.paired.values()}:
            c.comp(f"pipeline:{tp.name}", "pipeline", S.extra, note="a generated pipeline with no source pipeline")
    for low, tp in sorted(inp.paired.items()):
        comp = f"pipeline:{tp.name}"
        if tp.content is None:
            c.add("target_schema.pipeline", K.target_schema, R.FAIL, f"The generated file cannot be read ({tp.error}).", component=comp,
                  remediation="Re-run conversion.")
            continue
        v = validate_content(tp.content)
        errors = [f"{code.code}: {code.message}" for _, code in v.issues if code.severity.value == "error"]
        warns = [f"{code.code}: {code.message}" for _, code in v.issues if code.severity.value == "warning"]
        bad_types = [a.name + ": " + a.type for a in views.pipeline_activities(tp.content) if a.type not in FABRIC_ACTIVITIES]
        errors += [f"unknown Fabric activity type {x}" for x in bad_types]
        c.add("target_schema.pipeline", K.target_schema, R.FAIL if errors else R.PASS,
              f"The generated pipeline breaks the documented Fabric schema in {len(errors)} place(s)." if errors else "The generated pipeline is consistent with the documented Fabric pipeline schema.",
              component=comp, evidence=errors or warns, remediation="Fix the conversion defect named in the evidence; Fabric would reject or mis-run this file." if errors else
              "Static schema consistency only: Fabric has not accepted this file (see the API and deployment stages).")
    started = {n: str(((f.data or {}).get("properties") or {}).get("runtimeState", "")).lower() == "started" for n, f in inp.src.names("trigger").items()}
    for s in inp.tgt.schedules:
        problems = []
        if not s.get("pipelines"):
            problems.append("no pipeline is named")
        for sch in s.get("schedules", []) if isinstance(s.get("schedules"), list) else []:
            # the file records whether the SOURCE trigger was running; deployment still creates it disabled unless --enable is passed
            if bool(sch.get("enabled")) != started.get(str(s.get("trigger", "")).lower(), False):
                problems.append("the schedule's enabled flag does not match the source trigger's state")
            cfg = sch.get("configuration") if isinstance(sch.get("configuration"), dict) else {}
            if not cfg.get("type") or not cfg.get("startDateTime") or not cfg.get("endDateTime"):
                problems.append("a schedule lacks type, startDateTime or endDateTime (Fabric requires an end time)")
        c.add("target_schema.schedule", K.target_schema, R.FAIL if problems else R.PASS,
              f"Schedule for trigger '{s.get('trigger')}' has problems." if problems else f"Schedule for trigger '{s.get('trigger')}' is well formed and disabled.",
              component=f"trigger:{s.get('trigger')}", evidence=problems, remediation="Re-run conversion." if problems else "")


# =====================================================================================================================
# 4, 5, 8: parameters / expressions, activity graph, semantics (per pipeline)
# =====================================================================================================================


def _source_pipeline(inp: Inputs, low: str) -> Optional[dict]:
    f = inp.src.names("pipeline").get(low)
    return f.data if f else None


def _desc(a: ActView) -> str:
    return f"{a.type}" + (f" in {a.parent}" if a.parent else "")


def pipelines(inp: Inputs, c: Collector) -> None:
    for low, tp in sorted(inp.paired.items()):
        sdata = _source_pipeline(inp, low)
        if sdata is None or tp.content is None:
            continue
        s_acts, t_acts = views.pipeline_activities(sdata), views.pipeline_activities(tp.content)
        _parameters(inp, c, sdata, tp)
        _graph(inp, c, tp, s_acts, t_acts)
        _expressions(inp, c, tp, s_acts, t_acts)
        _semantics(inp, c, tp, s_acts, t_acts, sdata)


def _parameters(inp: Inputs, c: Collector, sdata: dict, tp: TargetPipeline) -> None:
    sp, tpp = (sdata.get("properties") or {}), (tp.content.get("properties") or {})
    comp = f"pipeline:{tp.name}"
    problems: list[str] = []
    for kind in ("parameters", "variables"):
        s, t = sp.get(kind) or {}, tpp.get(kind) or {}
        tl = {k.lower(): v for k, v in t.items()}
        for k, v in s.items():
            if k.lower() not in tl:
                problems.append(f"{kind[:-1]} '{k}' is missing from the generated pipeline")
            elif isinstance(v, dict) and isinstance(tl[k.lower()], dict) and v.get("type") != tl[k.lower()].get("type"):
                problems.append(f"{kind[:-1]} '{k}' type changed: {v.get('type')} -> {tl[k.lower()].get('type')}")
            elif isinstance(v, dict) and isinstance(tl[k.lower()], dict) and v.get("defaultValue") != tl[k.lower()].get("defaultValue"):
                problems.append(f"{kind[:-1]} '{k}' default value changed: {redact_value(k, v.get('defaultValue'))} -> {redact_value(k, tl[k.lower()].get('defaultValue'))}")
    added = sorted(k for k in (tpp.get("parameters") or {}) if k.lower() not in {x.lower() for x in (sp.get("parameters") or {})})
    c.add("expression_preservation.parameters_variables", K.expression_preservation, R.FAIL if problems else R.PASS,
          f"{len(problems)} parameter/variable difference(s)." if problems else "Every source parameter and variable is present with the same type and default.",
          component=comp, evidence=problems or ([f"added: {', '.join(added)} (global parameters become gp_<name>)"] if added else []),
          remediation="Re-run conversion; a dropped parameter changes what callers can pass." if problems else "")


def _graph(inp: Inputs, c: Collector, tp: TargetPipeline, s_acts: list[ActView], t_acts: list[ActView]) -> None:
    comp = f"pipeline:{tp.name}"
    s = {a.name.lower(): a for a in s_acts}
    t = {a.name.lower(): a for a in t_acts}
    missing = sorted(set(s) - set(t))
    extra = sorted(set(t) - set(s))
    placeholders = [a for a in t_acts if _is_placeholder(a)]
    ph_names = {a.name.lower() for a in placeholders}
    for low, a in s.items():
        cid = f"pipeline:{tp.name}/activity:{a.name}"
        ta = t.get(low)
        if ta is None:
            c.comp(cid, "activity", S.missing, _desc(a), note="absent from the generated pipeline")
        elif low in ph_names:
            c.comp(cid, "activity", S.missing, _desc(a), "Fail (placeholder)", note=ta.type_properties.get("message", "not converted")[:200])
        else:
            expected = set(rule_for(a.type).fabric_types) if rule_for(a.type) else set()
            if ta.type == a.type or ta.type in expected:
                st = S.matched if ta.type == a.type else S.transformed
                d = [] if st is S.matched else [f"~ type: {a.type} -> {ta.type}"]
                if (a.parent or "").lower() != (ta.parent or "").lower():
                    st, d = S.altered, d + [f"~ parent: {a.parent} -> {ta.parent}"]
                c.comp(cid, "activity", st, _desc(a), _desc(ta), d)
            else:
                c.comp(cid, "activity", S.altered, _desc(a), _desc(ta), [f"~ type: {a.type} -> {ta.type} (no conversion rule produces this)"])
    for low in extra:
        c.comp(f"pipeline:{tp.name}/activity:{t[low].name}", "activity", S.extra, None, _desc(t[low]), note="in the generated pipeline only")
    bad = [s[x].name for x in missing]
    c.add("activity_graph.inventory", K.activity_graph, R.FAIL if (missing or extra) else R.PASS,
          (f"Source has {len(s)} activities (nested included); target has {len(t)}: {len(missing)} missing, {len(extra)} extra." if (missing or extra)
           else f"All {len(s)} activities (nested included) are present in the generated pipeline, none extra."),
          component=comp, evidence=[f"missing: {n}" for n in bad] + [f"extra: {t[n].name}" for n in extra],
          remediation="Re-run conversion; every source activity must either convert or be stood in for by a visible placeholder." if (missing or extra) else "")
    if placeholders:
        c.add("activity_graph.conversion_complete", K.activity_graph, R.BLOCKED,
              f"{len(placeholders)} activity(ies) were NOT converted: a fail-fast placeholder stands in, so this pipeline cannot run to completion.",
              component=comp, evidence=[f"{a.name}: {str(a.type_properties.get('message', ''))[:200]}" for a in placeholders],
              remediation="Apply the alternative named in the conversion report (re-author the activity in Fabric), or keep this pipeline on ADF.")
    else:
        c.add("activity_graph.conversion_complete", K.activity_graph, R.PASS, "No activity is replaced by a placeholder.", component=comp)
    # nesting and dependency equivalence, on the activities both sides have
    both = sorted(set(s) & set(t))
    nest = [f"{s[n].name}: parent {s[n].parent} -> {t[n].parent}" for n in both if (s[n].parent or "").lower() != (t[n].parent or "").lower()]
    edges_s = {(p, n, s[n].depends[p]) for n in both for p in s[n].depends}
    edges_t = {(p, n, t[n].depends[p]) for n in both for p in t[n].depends}
    lost, added = sorted(edges_s - edges_t), sorted(edges_t - edges_s)
    evid = nest + [f"dependency lost: {p} -> {n} {list(cond)}" for p, n, cond in lost] + [f"dependency added: {p} -> {n} {list(cond)}" for p, n, cond in added]
    c.add("activity_graph.dependencies", K.activity_graph, R.FAIL if evid else R.PASS,
          f"The execution order differs in {len(evid)} place(s)." if evid else f"The dependency graph is equivalent ({len(edges_s)} edge(s) with the same conditions) and nesting is preserved.",
          component=comp, evidence=evid, remediation="Re-run conversion; a changed dependency changes what runs when." if evid else "")


def _expressions(inp: Inputs, c: Collector, tp: TargetPipeline, s_acts: list[ActView], t_acts: list[ActView]) -> None:
    comp = f"pipeline:{tp.name}"
    t = {a.name.lower(): a for a in t_acts}
    dropped, altered, kept = [], [], 0
    datasets = {k: json.dumps(f.data).lower() for k, f in inp.src.names("dataset").items() if f.data}
    unused: list[str] = []
    for a in s_acts:
        own = views.own_properties(a.raw)
        s_ex = []
        for p, x in views.expressions({"typeProperties": own.get("typeProperties"), "inputs": own.get("inputs"), "outputs": own.get("outputs")}):
            if _unused_dataset_parameter(own, p, datasets):
                unused.append(f"{a.name}: {p} feeds a dataset parameter the dataset never uses")
                continue
            s_ex.append((p, views.normalise_expression(x)))
        if not s_ex:
            continue
        ta = t.get(a.name.lower())
        if ta is None or _is_placeholder(ta):
            dropped.append(f"{a.name}: {len(s_ex)} expression(s) not carried (activity absent or not converted)")
            continue
        t_own = views.own_properties(ta.raw)
        t_ex = [views.normalise_expression(x) for _, x in views.expressions({"typeProperties": t_own.get("typeProperties"), "inputs": t_own.get("inputs"), "outputs": t_own.get("outputs")})]
        blob = "\n".join(t_ex)
        lost = [x for _, x in s_ex if x not in blob]
        if not lost:
            kept += len(s_ex)
        elif len(t_ex) >= len(s_ex):
            kept += len(s_ex) - len(lost)
            for (p, x) in s_ex:
                if x in lost:
                    altered.append(f"{a.name}: expression at {p} is rewritten: {redact_value(p, x)[:100]}")
            c.comp(f"pipeline:{tp.name}/activity:{a.name}", "activity", S.altered if ta.type == a.type else S.transformed,
                   note="expressions rewritten", diff_=[f"~ {p}: {redact_value(p, x)[:80]}" for p, x in s_ex if x in lost])
        else:
            dropped.append(f"{a.name}: {len(s_ex)} source expression(s), only {len(t_ex)} in the target")
    res = R.FAIL if dropped else R.PASS
    evid = dropped + altered + [f"ignored (unused dataset parameter): {u}" for u in unused[:3]]
    c.add("expression_preservation.expressions", K.expression_preservation, res,
          (f"{len(dropped)} activity(ies) lost expressions." if dropped else
           f"Every source expression is carried ({kept} identical" + (f", {len(altered)} rewritten: review them" if altered else "") + ")."),
          component=comp, evidence=evid, remediation="Reproduce the dropped dynamic content in Fabric; a lost expression changes behaviour." if dropped else
          ("Rewritten expressions are listed as evidence: confirm each one means the same." if altered else ""))


def _unused_dataset_parameter(own: dict, path: str, datasets: dict[str, str]) -> bool:
    """True for an expression passed as a dataset PARAMETER that the dataset's definition never reads (nothing is lost by not carrying it)."""
    m = re.match(r"(inputs|outputs)\[(\d+)\]\.parameters\.(\w+)", path)
    ref = None
    if m:
        lst = own.get(m.group(1))
        idx = int(m.group(2))
        ref = lst[idx].get("referenceName") if isinstance(lst, list) and idx < len(lst) and isinstance(lst[idx], dict) else None
        param = m.group(3)
    else:
        m = re.match(r"typeProperties\.dataset\.parameters\.(\w+)", path)
        if m:
            d = (own.get("typeProperties") or {}).get("dataset")
            ref, param = (d.get("referenceName") if isinstance(d, dict) else None), m.group(1)
    if not ref:
        return False
    text = datasets.get(str(ref).lower())
    return text is not None and f"dataset().{param.lower()}" not in text


def _semantics(inp: Inputs, c: Collector, tp: TargetPipeline, s_acts: list[ActView], t_acts: list[ActView], sdata: dict) -> None:
    comp = f"pipeline:{tp.name}"
    t = {a.name.lower(): a for a in t_acts}
    diffs: list[str] = []
    for a in s_acts:
        ta = t.get(a.name.lower())
        if ta is None or _is_placeholder(ta):
            continue
        for key in ("timeout", "retry", "retryIntervalInSeconds"):
            if key in a.policy and a.policy.get(key) != ta.policy.get(key):
                diffs.append(f"{a.name}: policy.{key} {a.policy.get(key)} -> {ta.policy.get(key)}")
        if a.type == "ForEach" and ta.type == "ForEach":
            for key in ("isSequential", "batchCount"):
                if key in a.type_properties and a.type_properties.get(key) != ta.type_properties.get(key):
                    diffs.append(f"{a.name}: {key} {a.type_properties.get(key)} -> {ta.type_properties.get(key)}")
        if a.type == "Switch" and ta.type == "Switch":
            sc = [str(x.get("value")) for x in a.type_properties.get("cases", []) if isinstance(x, dict)]
            tc = [str(x.get("value")) for x in ta.type_properties.get("cases", []) if isinstance(x, dict)]
            if sc != tc:
                diffs.append(f"{a.name}: switch cases {sc} -> {tc}")
        if a.type == "Copy" and ta.type == "Copy":
            for side in ("source", "sink"):
                s_t = (a.type_properties.get(side) or {}).get("type")
                t_t = (ta.type_properties.get(side) or {}).get("type") or (ta.type_properties.get("destination") or {}).get("type") if side == "sink" else (ta.type_properties.get(side) or {}).get("type")
                if s_t and t_t and s_t != t_t:
                    diffs.append(f"{a.name}: copy {side} type {s_t} -> {t_t}")
        if a.type_properties.get("dataset") or a.raw.get("inputs") or a.raw.get("outputs"):
            if "datasetSettings" not in str(ta.raw) and ta.type in ("Copy", "Lookup", "GetMetadata", "Delete"):
                diffs.append(f"{a.name}: the activity read a dataset in the source, but the generated activity carries no datasetSettings")
    c.add("execution_semantics.static", K.execution_semantics, R.FAIL if diffs else R.PASS,
          f"{len(diffs)} execution setting(s) changed." if diffs else "Retry/timeout policy, loop settings, switch cases, copy source/sink types and dataset settings are preserved.",
          component=comp, evidence=diffs or ["Static comparison only: this does not prove the pipeline behaves the same when run."],
          remediation="Restore the setting in the generated pipeline (or change it deliberately in the source); it changes failure or concurrency behaviour." if diffs else "")


# =====================================================================================================================
# 6. linked services, datasets, connections, secrets
# =====================================================================================================================


def connections_and_secrets(inp: Inputs, c: Collector) -> None:
    leaks: list[str] = []
    for low, tp in sorted(inp.paired.items()):
        if tp.content is not None:
            leaks += [f"{tp.name}: {p}" for p in has_plaintext_secret(tp.content)]
    for s in inp.tgt.schedules:
        leaks += [f"schedule {s.get('trigger')}: {p}" for p in has_plaintext_secret(s)]
    c.add("connections_secrets.plaintext_secrets", K.connections_secrets, R.FAIL if leaks else R.PASS,
          f"{len(leaks)} generated value(s) look like plaintext secrets." if leaks else "No generated artifact holds a plaintext secret (values are never printed here).",
          evidence=leaks, remediation="Remove the literal, rotate the exposed secret, and reference a Fabric connection or Key Vault instead." if leaks else "")
    src_leaks = []
    for f in inp.src.files:
        if f.data is not None and f.kind == "linked_service":
            src_leaks += [f"{f.name}: {p}" for p in has_plaintext_secret(f.data.get("properties", {}))]
    if src_leaks:
        c.add("connections_secrets.source_secrets", K.connections_secrets, R.FAIL, f"{len(src_leaks)} linked-service value(s) in the SOURCE look like plaintext secrets.",
              evidence=src_leaks, remediation="Rotate those secrets and move them to Key Vault; they should not be in the repository.", required=False)
    # per linked service: is it bound in every generated activity that used it?
    ds_ls = {f.name.lower(): _ds_ls(f.data) for f in inp.src.of("dataset") if f.data}
    users: dict[str, list[tuple[str, str]]] = {}  # linked service lower -> [(pipeline, activity)]
    ds_users: dict[str, list[tuple[str, str]]] = {}
    for low, f in inp.src.names("pipeline").items():
        for a in views.pipeline_activities(f.data):
            for kind, name in _refs(a.raw):
                if kind == "linked_service":
                    users.setdefault(name.lower(), []).append((f.name, a.name))
                elif kind == "dataset":
                    ds_users.setdefault(name.lower(), []).append((f.name, a.name))
                    ls = ds_ls.get(name.lower())
                    if ls:
                        users.setdefault(ls.lower(), []).append((f.name, a.name))
    for lsf in inp.src.of("linked_service"):
        low = lsf.name.lower()
        comp = f"linked_service:{lsf.name}"
        used = users.get(low, [])
        if not used:
            c.comp(comp, "linked_service", S.not_applicable, note="no pipeline uses it")
            continue
        states: list[str] = []
        for pname, aname in sorted(set(used)):
            tp = inp.paired.get(pname.lower())
            ta = next((a for a in views.pipeline_activities(tp.content) if a.name.lower() == aname.lower()), None) if tp and tp.content else None
            if ta is None or _is_placeholder(ta):
                states.append(f"{pname}/{aname}: not converted")
                continue
            vals = _connection_values(ta.raw)
            if not vals:
                states.append(f"{pname}/{aname}: no connection reference in the generated activity" if ta.type.lower() in CONNECTION_ACTIVITIES else f"{pname}/{aname}: n/a")
            elif all(_TOKEN.match(v) for v in vals):
                states.append(f"{pname}/{aname}: unbound ({', '.join(vals)})")
            else:
                states.append(f"{pname}/{aname}: bound")
        unbound = [x for x in states if "unbound" in x or "not converted" in x]
        absent = [x for x in states if "no connection reference" in x]
        if absent:
            c.add("connections_secrets.linked_service", K.connections_secrets, R.FAIL, f"Linked service '{lsf.name}' is used, but a generated activity has no connection.",
                  component=comp, evidence=absent, remediation="Re-run conversion; the activity must reference a Fabric connection.")
            c.comp(comp, "linked_service", S.missing, note="no connection in the generated activity")
        elif unbound:
            c.add("connections_secrets.linked_service", K.connections_secrets, R.BLOCKED, f"Linked service '{lsf.name}' has no Fabric connection bound yet.",
                  component=comp, evidence=unbound, remediation="Create the Fabric connection (credentials are entered again), record its id, and run `convert --resolutions`.")
            c.comp(comp, "linked_service", S.unverified, note="connection not bound")
        else:
            kv = "AzureKeyVaultSecret" in str(lsf.data)
            c.add("connections_secrets.linked_service", K.connections_secrets, R.PASS,
                  f"Linked service '{lsf.name}' is bound to a Fabric connection in every generated activity that used it.", component=comp,
                  evidence=(["The source held Key Vault secret references: the credential was re-entered in Fabric. This cannot be verified here."] if kv else []) +
                           ["Binding is verified by id only: the API stage checks the connection exists; credentials are never read."])
            c.comp(comp, "linked_service", S.transformed, note="became a Fabric connection (id bound)")
    for dsf in inp.src.of("dataset"):
        used = ds_users.get(dsf.name.lower(), [])
        comp = f"dataset:{dsf.name}"
        if not used:
            c.comp(comp, "dataset", S.not_applicable, note="no activity uses it")
            continue
        miss, inspected = [], 0
        for pname, aname in sorted(set(used)):
            tp = inp.paired.get(pname.lower())
            ta = next((a for a in views.pipeline_activities(tp.content) if a.name.lower() == aname.lower()), None) if tp and tp.content else None
            if ta is None or _is_placeholder(ta):
                continue
            inspected += 1
            if ta.type in ("Copy", "Lookup", "GetMetadata", "Delete") and "datasetSettings" not in str(ta.raw):
                miss.append(f"{pname}/{aname}")
        if not inspected:
            c.comp(comp, "dataset", S.unverified, note="none of the activities that use it were converted")
            continue
        if miss:
            c.add("connections_secrets.dataset", K.connections_secrets, R.FAIL, f"Dataset '{dsf.name}' was not inlined into {len(miss)} activity(ies).", component=comp, evidence=miss,
                  remediation="Fabric has no datasets: each activity must carry datasetSettings. Re-run conversion.")
            c.comp(comp, "dataset", S.missing, note="not inlined")
        else:
            c.comp(comp, "dataset", S.transformed, note="inlined into activities as datasetSettings")
    for f in inp.src.of("integration_runtime"):
        c.comp(f"integration_runtime:{f.name}", "integration_runtime", S.not_applicable, note="Fabric has no integration runtime to migrate to (see the dependency inventory)")
    for f in inp.src.of("dataflow"):
        c.comp(f"dataflow:{f.name}", "dataflow", S.unverified, note="mapping data flows are scaffolded, not converted")
    c.add("connections_secrets.identity", K.connections_secrets, R.NOT_RUN, "Identity (workspace identity, service principal) usage of connections is not stored in generated files; the API stage reads connection credential types.",
          required=False)


def _ds_ls(data: dict) -> Optional[str]:
    ref = (data.get("properties") or {}).get("linkedServiceName")
    return ref.get("referenceName") if isinstance(ref, dict) else None


def _connection_values(node: Any) -> list[str]:
    out: list[str] = []
    if isinstance(node, dict):
        ext = node.get("externalReferences")
        if isinstance(ext, dict) and isinstance(ext.get("connection"), str):
            out.append(ext["connection"])
        for v in node.values():
            out += _connection_values(v)
    elif isinstance(node, list):
        for v in node:
            out += _connection_values(v)
    return out


# =====================================================================================================================
# 6b. triggers, global parameters, 7. compute
# =====================================================================================================================


def triggers_and_globals(inp: Inputs, c: Collector) -> None:
    sched = {t.lower(): s for s in inp.tgt.schedules for t in [str(s.get("trigger", ""))]}
    if not inp.tgt.pipelines:  # conversion has not run
        for f in inp.src.of("trigger"):
            c.comp(f"trigger:{f.name}", "trigger", S.unverified, note="not converted yet")
        return
    for f in inp.src.of("trigger"):
        props = (f.data or {}).get("properties", {})
        ttype, comp = str(props.get("type", "")), f"trigger:{f.name}"
        s = sched.get(f.name.lower())
        if ttype in ("ScheduleTrigger", "TumblingWindowTrigger"):
            if s is None:
                c.add("execution_semantics.trigger", K.execution_semantics, R.FAIL, f"Trigger '{f.name}' has no generated schedule.", component=comp,
                      remediation="Re-run conversion.")
                c.comp(comp, "trigger", S.missing, note="no generated schedule")
                continue
            src_pipes = sorted(str((p.get("pipelineReference") or {}).get("referenceName", "")).lower() for p in props.get("pipelines", []) if isinstance(p, dict))
            tgt_pipes = sorted(str(p).lower() for p in s.get("pipelines", []))
            rec = (props.get("typeProperties") or {}).get("recurrence") or {}
            cfg = ((s.get("schedules") or [{}])[0].get("configuration") or {}) if s.get("schedules") else {}
            probs = []
            if src_pipes != tgt_pipes:
                probs.append(f"pipelines {src_pipes} -> {tgt_pipes}")
            freq = str(rec.get("frequency", "")).lower() or str((props.get("typeProperties") or {}).get("frequency", "")).lower()
            ok = {"minute": {"byminute"}, "hour": {"hourly", "byminute"}, "day": {"daily"}, "week": {"weekly"}, "month": {"monthly"}}.get(freq)
            if ok and cfg.get("type") and str(cfg.get("type")).lower() not in ok:
                probs.append(f"frequency {rec.get('frequency')} -> {cfg.get('type')}")
            c.add("execution_semantics.trigger", K.execution_semantics, R.FAIL if probs else R.PASS,
                  f"Trigger '{f.name}' differs from its schedule." if probs else f"Trigger '{f.name}' maps to a disabled Fabric schedule for the same pipeline(s).", component=comp, evidence=probs,
                  remediation="Re-run conversion." if probs else "")
            c.comp(comp, "trigger", S.altered if probs else S.transformed, note="became a Fabric schedule (created disabled)", diff_=probs)
        else:
            c.add("execution_semantics.trigger", K.execution_semantics, R.BLOCKED, f"Trigger '{f.name}' ({ttype}) has no automatic Fabric pipeline schedule.", component=comp,
                  remediation="Recreate it by hand (for event triggers, a Fabric Activator rule).")
            c.comp(comp, "trigger", S.missing, note=f"{ttype} is not converted")
    used_gp: set[str] = set()
    for tp in inp.tgt.pipelines:
        for k in ((tp.content or {}).get("properties") or {}).get("parameters", {}) or {}:
            if k.startswith("gp_"):
                used_gp.add(k[3:].lower())
    for f in inp.src.of("factory"):
        for g in ((f.data or {}).get("properties") or {}).get("globalParameters", {}) or {}:
            referenced = any(f"globalparameters.{g.lower()}" in str(pf.data).lower() for pf in inp.src.of("pipeline") if pf.data)
            c.comp(f"global_parameter:{g}", "global_parameter", S.matched if g.lower() in used_gp else (S.missing if referenced else S.not_applicable),
                   note="became pipeline parameter gp_" + g if g.lower() in used_gp else ("referenced but not carried" if referenced else "not referenced by any pipeline"))


def compute(inp: Inputs, c: Collector) -> None:
    trident = []
    for tp in inp.tgt.pipelines:
        for a in views.pipeline_activities(tp.content or {}):
            if a.type in ("TridentNotebook", "SparkJobDefinition"):
                trident.append((tp.name, a))
    wm = inp.workloads
    if not trident:
        c.add("compute.fabric_compute", K.compute, R.PASS, "No generated activity runs on Fabric Spark compute (Databricks / Synapse compute is called as before, or there is none).",
              required=False)
    for pname, a in trident:
        item = a.type_properties.get("notebookId") or a.type_properties.get("sparkJobDefinitionId") or ""
        comp = f"pipeline:{pname}/activity:{a.name}"
        if _TOKEN.match(str(item)):
            c.add("compute.fabric_compute", K.compute, R.BLOCKED, f"'{a.name}' points at a Fabric item that does not exist yet ({item}).", component=comp,
                  remediation="Generate and deploy the notebook / Spark job (workloads-generate, workloads-approve, workloads-deploy), then convert with --resolutions.")
            continue
        entry = next((w for w in (wm.workloads if wm else []) if w.artifact.fabric_item_id and w.artifact.fabric_item_id.lower() == str(item).lower()), None)
        if entry is None:
            c.add("compute.fabric_compute", K.compute, R.BLOCKED, f"'{a.name}' runs a Fabric item ({item}) that has no migration record, so its content was never validated.", component=comp,
                  remediation="Run `workloads-assess` / `workloads-generate` so the item's source, rewrites and static checks are on record.")
        elif entry.generation.status != "generated":
            c.add("compute.fabric_compute", K.compute, R.BLOCKED, f"'{a.name}' runs a generated item with open issues ({len(entry.unresolved)}).", component=comp,
                  evidence=entry.unresolved, remediation="Resolve the open issues in the workload report, regenerate, and redeploy.")
        elif entry.approval.status != "approved":
            c.add("compute.fabric_compute", K.compute, R.FAIL, f"'{a.name}' runs an item whose artifact was not approved.", component=comp, remediation="Run workloads-approve.")
        else:
            c.add("compute.fabric_compute", K.compute, R.PASS, f"'{a.name}' runs a generated, approved item ('{entry.name}') whose static checks passed.", component=comp,
                  evidence=[f"classification: {entry.classification.value}", f"rewrites: {sum(r.count for r in entry.rewrites)}",
                            "Static evidence only: execution in Fabric is a runtime-stage check (" + entry.execution.status + ")."])
    pv = inp.provisioning
    if pv is None:
        c.add("compute.required_resources", K.compute, R.NOT_RUN, "No provisioning verification exists for the pools, environments and lakehouses the migration needs.", required=False)
    else:
        bad = [f"{r.step_id}: {r.status.value} {r.detail}" for r in pv.results if r.status.value in ("mismatch", "failed")]
        c.add("compute.required_resources", K.compute, R.PASS if pv.all_verified else R.FAIL if bad else R.BLOCKED,
              "Every provisioned target resource was read back and verified." if pv.all_verified else "Not every required resource is verified.",
              evidence=bad, remediation="Run provision-plan / provision-approve / provision-apply and provision-verify." if not pv.all_verified else "")


def run_static(inp: Inputs) -> Collector:
    c = Collector()
    source_inventory(inp, c)
    if inp.src.root is None:
        return c
    reference_integrity(inp, c)
    target_schema(inp, c)
    pipelines(inp, c)
    connections_and_secrets(inp, c)
    triggers_and_globals(inp, c)
    compute(inp, c)
    for low, tp in inp.paired.items():  # a pipeline is matched only if nothing about it was missing / altered
        cid = f"pipeline:{tp.name}"
        kids = [x for k, x in c.components.items() if k.startswith(cid + "/")]
        worst = max((x.status for x in kids), key=lambda s: [S.matched, S.not_applicable, S.transformed, S.unverified, S.altered, S.extra, S.missing].index(s), default=S.matched)
        status = S.matched if worst in (S.matched, S.not_applicable, S.transformed) else (S.altered if worst in (S.altered, S.extra, S.missing) else S.unverified)
        c.comp(cid, "pipeline", status, note=f"{len(kids)} activities compared")
    return c

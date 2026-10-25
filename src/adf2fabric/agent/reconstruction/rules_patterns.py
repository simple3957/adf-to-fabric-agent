"""Reconstruction rules for recognised ingestion / orchestration patterns.

A pattern is something to RECOGNISE, not a template to insert. A missing step is proposed only when
the project's own properties, expressions and related pipelines supply every part of it; otherwise
the gap is reported with the exact information still needed.

    watermark_update_from_lookups   watermark lookup (stored value) + MAX lookup (new value) + a Copy
                                    filtered by them, but nothing writes the new watermark back
    staging_load_from_peers         a Copy lands data in a staging table and nothing loads it, while
                                    other pipelines load their staging tables with a derivable procedure
    property_from_shape_siblings    a required property is absent where >= 2 structurally identical
                                    pipelines all carry the same value
    error_handling_convention       (template only) activities that every comparable pipeline guards
                                    with a failure handler, here without one
"""

from __future__ import annotations

import copy
import re
from typing import Any, Optional

from adf2fabric.agent.reconstruction.context import PipelineCtx
from adf2fabric.agent.reconstruction.evidence import ev
from adf2fabric.agent.reconstruction.rules_structure import _is_copy
from adf2fabric.project.discovery import AdfActivity
from adf2fabric.project.reconstruction import (
    Alternative,
    EvidenceKind as K,
    IncompletenessClass as C,
    Severity,
)

_AGG = re.compile(r"\b(max|min)\s*\(", re.I)
_SIMPLE_SELECT = re.compile(
    r"^\s*select\s+(?:top\s*\(?\d+\)?\s+)?(?P<col>\[?[\w]+\]?)\s*(?:as\s+\[?\w+\]?\s*)?from\s+"
    r"(?P<table>[\[\]\w\.]+)(?:\s+where\s+(?P<where>.+?))?\s*;?\s*$", re.I | re.S)
_MAX_ALIAS = re.compile(r"\b(?:max|min)\s*\(\s*(?P<col>\[?\w+\]?)\s*\)\s*(?:as\s+(?P<alias>\[?\w+\]?))?", re.I)
_NUMERIC = re.compile(r"^(tiny|small|big)?int(eger)?$|^decimal|^numeric|^float|^double|^real|^money|^number", re.I)
_STAGING = re.compile(r"^(stg|stage|staging)$", re.I)


def _static_query(io) -> Optional[str]:
    q = io.location.get("query") if io is not None else None
    return q if isinstance(q, str) and q and not io.location.get("query_is_expression") else None


def _unique_name(pc: PipelineCtx, base: str) -> str:
    used = {a.name.lower() for a in pc.ev.acts(pc.pipe)}
    name, n = base, 1
    while name.lower() in used:
        n += 1
        name = f"{base} {n}"
    return name


def _writes(a: AdfActivity, lookups: list[AdfActivity], tables: set[str]) -> bool:
    if (a.type or "").lower() not in ("script", "sqlserverstoredprocedure"):
        return False
    names = {x.name.lower() for x in lookups}
    if any(r.kind == "activity_output" and (r.name or "").lower() in names for e in a.expressions for r in e.references):
        return True
    text = " ".join([str(a.configuration.get("stored_procedure") or ""),
                     *[str(b.get("text") or "") for b in a.configuration.get("scripts") or []]]).lower()
    text = text.replace("[", "").replace("]", "")
    # lenient on purpose: treating an equivalent update as missing would propose a DUPLICATE write
    names = {n for t in tables for n in (t.replace("[", "").replace("]", "").lower(),
                                         t.replace("[", "").replace("]", "").lower().rsplit(".", 1)[-1])}
    return any(n and n in text for n in names)


# =============================================================================================================
# incremental load: watermark update
# =============================================================================================================


def rule_watermark_update(pc: PipelineCtx) -> None:
    ev_, p = pc.ev, pc.pipe
    names = {x.name.lower(): x for x in ev_.acts(p)}
    for copy_act in ev_.acts(p):
        if not _is_copy(copy_act) or "incremental_filter" not in copy_act.semantic_tags:
            continue
        refd = []
        for e in copy_act.expressions:
            if e.role in ("source_query", "source_selection", "source_path", "filter_condition"):
                for r in e.references:
                    t = names.get((r.name or "").lower()) if r.kind == "activity_output" else None
                    if t is not None and (t.type or "").lower() == "lookup" and t not in refd:
                        refd.append(t)
        if not refd:
            continue
        queries = {lk.activity_id: _static_query(lk.inputs[0] if lk.inputs else None) or "" for lk in refd}
        new = [lk for lk in refd if _AGG.search(queries[lk.activity_id])]
        old = [lk for lk in refd if lk not in new]
        ctrl_tables = {m.group("table") for lk in old for m in [_SIMPLE_SELECT.match(queries[lk.activity_id])] if m}
        down = ev_.downstream_names(p, copy_act)
        if any(x.name.lower() in down and _writes(x, refd, ctrl_tables) for x in ev_.acts(p)):
            continue  # the pattern is complete: something already writes the watermark back

        f = pc.out.finding(
            C.incomplete_configuration, "incremental_watermark_update_missing",
            f"Copy '{copy_act.name}' loads incrementally from the watermark(s) read by "
            f"{', '.join(repr(x.name) for x in refd)}, but no activity writes the new watermark back.",
            pipeline=p.name, activity=copy_act, severity=Severity.warning, source_path=p.path,
            detail="every run would re-read the same window")
        evidence = [ev(K.same_pipeline_reference, copy_act.activity_id,
                       f"{e.property_path} filters on the output of {', '.join(x.name for x in refd)}")
                    for e in copy_act.expressions if e.role in ("source_query", "source_selection", "filter_condition")][:1]
        evidence += [ev(K.explicit_property, lk.activity_id, f"query: {queries[lk.activity_id]!r}") for lk in refd]

        parsed_old = _SIMPLE_SELECT.match(queries[old[0].activity_id]) if len(old) == 1 else None
        parsed_new = _MAX_ALIAS.search(queries[new[0].activity_id]) if len(new) == 1 else None
        alias = (parsed_new.group("alias") or "").strip("[]") if parsed_new else ""
        info: list[str] = []
        if len(old) != 1:
            info.append("which Lookup holds the stored watermark (exactly one expected)")
        elif not parsed_old:
            info.append("the control table, column and key of the stored watermark (its query is not a simple "
                        "single-table SELECT)")
        if len(new) != 1:
            info.append("where the new watermark value comes from (exactly one MAX/MIN Lookup expected)")
        elif not alias:
            info.append("the alias of the new watermark column in its MAX query (the output property to read)")

        scope_list = pc.tokens(copy_act)[:-1]
        if info:
            pc.out.proposal(
                rule="watermark_update_from_lookups", finding=f, pipe=p, activity=copy_act,
                missing_element="watermark update after the incremental Copy", action="add_activity",
                target_path=pc.pointer(scope_list), proposed={
                    "type": "Script", "scripts": [{"type": "NonQuery", "text": "<UPDATE statement - not derivable>"}]},
                evidence=evidence, information_required=info, derived_sql=False, patches=[], generate=False,
                not_generated_reason="the statement cannot be derived from the available evidence: " + "; ".join(info),
                alternatives=[Alternative(description="a stored procedure that persists the watermark")],
                affects=[copy_act.activity_id])
            pc.out.blocker(
                [f], pipeline=p.name, activity=copy_act, reason="the watermark write-back cannot be derived",
                information_required=info,
                conversion_impact="The Fabric pipeline loads incrementally but never advances the watermark; "
                                  "it is converted as authored and flagged, never reported as a complete load pattern.",
                minimum_information="the statement or procedure that stores the new watermark",
                closest_safe_implementation="the pipeline exactly as authored, with the gap reported")
            continue

        assert parsed_old and parsed_new
        col, table, where = parsed_old.group("col"), parsed_old.group("table"), parsed_old.group("where")
        ctrl_ds = ev_.datasets.get((old[0].inputs[0].name or "").lower()) if old[0].inputs else None
        col_type = next((c.type for c in (ctrl_ds.columns if ctrl_ds else []) if c.name.lower() == col.strip("[]").lower()), None)
        numeric = bool(col_type and _NUMERIC.match(col_type))
        value = f"@{{activity('{new[0].name}').output.firstRow.{alias}}}"
        literal = value if numeric else f"'{value}'"
        text = f"UPDATE {table} SET {col} = {literal}" + (f" WHERE {where}" if where else "")  # nosec B608: a proposal a person reviews, built from the source's own statement; never executed here
        ls_ref = ctrl_ds.linked_service_ref.name if ctrl_ds and ctrl_ds.linked_service_ref else None
        assumptions = [
            f"the new watermark is the value returned by '{new[0].name}' (alias {alias})",
            "the control table is updated in place: " + (f"rows matching {where!r}" if where else "every row (the stored "
                                                                                               "watermark query has no WHERE)"),
        ]
        assumptions.append("the watermark column is numeric (unquoted)" if numeric else
                           ("the watermark column is " + (f"{col_type}-typed; the value is quoted" if col_type else
                                                          "of unknown type; the value is quoted as a string/datetime")))
        if ls_ref is None:
            info.append("the linked service of the control table")
        name = _unique_name(pc, f"Update Watermark - {copy_act.name}")
        script = {
            "name": name, "type": "Script",
            "dependsOn": [{"activity": copy_act.name, "dependencyConditions": ["Succeeded"]}],
            "typeProperties": {"scripts": [{"type": "NonQuery", "text": {"value": text.replace("@", "@@").replace(
                "@@{", "@{"), "type": "Expression"}}]},
        }
        if ls_ref:
            script["linkedServiceName"] = {"referenceName": ls_ref, "type": "LinkedServiceReference"}
        peers = [(q.name, b) for q in ev_.discovery.pipelines if q.name != p.name for b in ev_.acts(q)
                 if "watermark_lookup" in b.semantic_tags]
        peer_ev = [ev(K.repeated_pattern, f"{q}/{b.name}", "uses the same watermark-lookup pattern") for q, b in peers[:3]]
        evidence += [ev(K.referenced_artifact, f"dataset:{ctrl_ds.name}" if ctrl_ds else old[0].activity_id,
                        f"control table {table}, column {col}" + (f" ({col_type})" if col_type else ""))] + peer_ev
        alt = [Alternative(description="persist the watermark with a stored procedure instead of an inline UPDATE")]
        if peers:
            alt.append(Alternative(description="mirror how the other pipelines using this pattern store the watermark"))
        pc.out.proposal(
            rule="watermark_update_from_lookups", finding=f, pipe=p, activity=copy_act,
            missing_element="watermark update after the incremental Copy", action="add_activity",
            target_path=pc.pointer(scope_list), proposed=script, evidence=evidence, information_required=info,
            material_assumptions=True, derived_sql=True, assumptions=assumptions, alternatives=alt,
            patches=[pc.patch_append(scope_list, script)],
            affects=[pc.new_activity_id(copy_act, name)],
            creates=pc.new_activity_id(copy_act, name))
        # an unsafe proposal (information still required) leaves the gap open
        if info:
            pc.out.blocker([f], pipeline=p.name, activity=copy_act, reason="; ".join(info),
                           information_required=info,
                           conversion_impact="The watermark update is not generated.",
                           minimum_information=info[0], closest_safe_implementation="the pipeline as authored, gap reported")


# =============================================================================================================
# staging -> curated load
# =============================================================================================================


def _sink_table(pc: PipelineCtx, a: AdfActivity) -> Optional[tuple[str, str]]:
    if not _is_copy(a) or not a.outputs or not a.outputs[0].name:
        return None
    ds = pc.ev.datasets.get(a.outputs[0].name.lower())
    asset = ds.asset if ds else None
    if asset and asset.sql_schema and asset.sql_table and _STAGING.match(asset.sql_schema.strip("[]")):
        return asset.sql_schema.strip("[]"), asset.sql_table.strip("[]")
    return None


def _loaders(pc: PipelineCtx, a: AdfActivity, table: str, schema: str) -> list[AdfActivity]:
    down = pc.ev.downstream_names(pc.pipe, a)
    hits = []
    for x in pc.ev.acts(pc.pipe):
        if x.name.lower() not in down or (x.type or "").lower() not in ("script", "sqlserverstoredprocedure"):
            continue
        text = " ".join([str(x.configuration.get("stored_procedure") or ""),
                         *[str(b.get("text") or "") for b in x.configuration.get("scripts") or []]]).lower()
        if table.lower() in text:
            hits.append(x)
    return hits


def rule_staging_load(pc: PipelineCtx) -> None:
    ev_, p = pc.ev, pc.pipe
    for a in ev_.acts(p):
        st = _sink_table(pc, a)
        if st is None:
            continue
        schema, table = st
        if _loaders(pc, a, table, schema):
            continue
        # a calling pipeline may perform the load after invoking this one
        called_by = [q for q in ev_.discovery.pipelines if p.name in q.pipeline_dependencies]
        if any(_loaders_in_parent(pc, q, p, table) for q in called_by):
            continue
        f = pc.out.finding(C.incomplete_configuration, "staging_load_missing",
                           f"Copy '{a.name}' lands data in {schema}.{table} but nothing in this pipeline or its callers "
                           "loads it onward.", pipeline=p.name, activity=a, severity=Severity.warning, source_path=p.path)
        peers = []
        for q in ev_.discovery.pipelines:
            if q.name == p.name:
                continue
            for b in ev_.acts(q):
                st2 = _sink_table(PipelineCtx(ev_, pc.out, q, None), b)
                if st2 is None:
                    continue
                for sp in [x for x in ev_.acts(q) if (x.type or "").lower() == "sqlserverstoredprocedure"
                           and x.name.lower() in ev_.downstream_names(q, b)]:
                    proc = str(sp.configuration.get("stored_procedure") or "")
                    if _derive_proc_name(proc, st2[1], table) is not None:
                        peers.append((q, b, sp, st2[1], proc))
        derived: dict[str, list] = {}
        for q, b, sp, t2, proc in peers:
            derived.setdefault(_derive_proc_name(proc, t2, table), []).append((q, sp, t2, proc))
        if len(derived) == 1:
            proc_new, items = next(iter(derived.items()))
            sp0 = items[0][1]
            params = sp0.raw_properties.get("storedProcedureParameters") or {}
            info = [] if not params else [f"how to fill procedure parameters ({', '.join(sorted(params))}), "
                                          "which differ per pipeline"]
            evidence = [ev(K.repeated_pattern, f"{q.name}/{sp.name}",
                           f"loads staging table {t2} with {proc} (name derived by replacing {t2} with {table})")
                        for q, sp, t2, proc in items]
            in_sql = proc_new.replace("[", "").replace("]", "").lower() in ev_.sql_procedures()
            if in_sql:
                evidence.append(ev(K.project_artifact, "sql-discovery", f"procedure {proc_new} exists in the discovered database"))
            name = _unique_name(pc, f"Load {table} from staging")
            ls = sp0.linked_service_refs[0] if sp0.linked_service_refs else None
            act_json = {"name": name, "type": "SqlServerStoredProcedure",
                        "dependsOn": [{"activity": a.name, "dependencyConditions": ["Succeeded"]}],
                        "typeProperties": {"storedProcedureName": proc_new}}
            if ls:
                act_json["linkedServiceName"] = {"referenceName": ls, "type": "LinkedServiceReference"}
            scope_list = pc.tokens(a)[:-1]
            pc.out.proposal(
                rule="staging_load_from_peers", finding=f, pipe=p, activity=a,
                missing_element=f"load of {schema}.{table} into the curated layer", action="add_activity",
                target_path=pc.pointer(scope_list), proposed=act_json, evidence=evidence, information_required=info,
                material_assumptions=True, patches=[pc.patch_append(scope_list, act_json)],
                assumptions=[f"procedure {proc_new} exists and loads {table}; its name follows the other pipelines' convention"],
                alternatives=[Alternative(description="the load happens elsewhere (another pipeline or a schedule)")],
                affects=[pc.new_activity_id(a, name)], creates=pc.new_activity_id(a, name))
            if not info:
                continue
        pc.out.blocker([f], pipeline=p.name, activity=a,
                       reason="no unique, evidenced load step can be derived for the staging table",
                       information_required=[f"the stored procedure or script that loads {schema}.{table}"],
                       conversion_impact="Staged data is copied but never moved to the curated layer.",
                       minimum_information=f"the load procedure name for {schema}.{table}",
                       closest_safe_implementation="the pipeline as authored, gap reported")


def _derive_proc_name(proc: str, peer_table: str, table: str) -> Optional[str]:
    """Replace the peer's table token inside the UNQUALIFIED procedure name (last occurrence); else None."""
    cut = proc.rfind(".") + 1
    head, unq = proc[:cut], proc[cut:]
    hits = list(re.finditer(re.escape(peer_table), unq, re.I))
    if not hits:
        return None
    h = hits[-1]
    return head + unq[: h.start()] + table + unq[h.end():]


def _loaders_in_parent(pc: PipelineCtx, parent, child, table: str) -> bool:
    """True when the calling pipeline loads ``table`` after invoking ``child``.

    A call usually sits inside a loop or branch while the load runs after that container, so the
    activities downstream of the call AND of each container around it are considered.
    """
    ev_ = pc.ev
    calls = [x for x in ev_.acts(parent) if (x.type or "").lower() == "executepipeline"
             and str(x.configuration.get("pipeline") or "").lower() == child.name.lower()]
    for call in calls:
        for owner in [call, *ev_.ancestors(parent, call)]:
            down = ev_.downstream_names(parent, owner)
            for x in ev_.siblings(parent, owner):
                if x.name.lower() in down and (x.type or "").lower() in ("script", "sqlserverstoredprocedure"):
                    text = " ".join([str(x.configuration.get("stored_procedure") or ""),
                                     *[str(b.get("text") or "") for b in x.configuration.get("scripts") or []]]).lower()
                    if table.lower() in text:
                        return True
    return False


# =============================================================================================================
# related pipelines of matching structure
# =============================================================================================================


def rule_shape_siblings(pc: PipelineCtx) -> None:
    ev_, p = pc.ev, pc.pipe
    sig = ev_.signature(p)
    sibs = [q for q in ev_.discovery.pipelines if q.name != p.name and len(sig) >= 2 and ev_.signature(q) == sig]
    if not sibs:
        return
    mine = ev_.acts(p)
    for i, a in enumerate(mine):
        for path in a.missing_properties:
            if not path.startswith("typeProperties.") or path.count(".") != 1:
                continue
            key = path.split(".", 1)[1]
            values = []
            for q in sibs:
                b = ev_.acts(q)[i]
                node = ev_.act_json(q, b)
                tp = node.get("typeProperties") if isinstance(node, dict) else None
                if isinstance(tp, dict) and key in tp:
                    values.append((q, b, tp[key]))
            if not values:
                continue
            if _secretish(values):
                continue
            distinct = []
            for _, _, v in values:
                if v not in distinct:
                    distinct.append(v)
            f = pc.out.finding(C.incomplete_configuration, "missing_required_property",
                               f"{a.type} '{a.name}' has no {path}.", pipeline=p.name, activity=a, property_path=path,
                               severity=Severity.error, source_path=p.path)
            toks = pc.at(a, "typeProperties", key)
            # Two or more structurally identical pipelines agreeing is "related pipelines" evidence;
            # a single one is just one instance of a pattern (weak), so it can never grade above LOW.
            kind = K.related_pipeline if len(values) >= 2 else K.repeated_pattern
            evidence = [ev(kind, f"{q.name}/{b.name}", f"has the same structure and sets {key} = {_short(v)}")
                        for q, b, v in values]
            pc.out.proposal(
                rule="property_from_shape_siblings", finding=f, pipe=p, activity=a,
                missing_element=f"{path}", action="set_property", target_path=pc.pointer(toks),
                proposed=copy.deepcopy(distinct[0]) if len(distinct) == 1 else None, evidence=evidence,
                candidates=len(distinct), patches=[pc.patch_set(toks, copy.deepcopy(distinct[0]))] if len(distinct) == 1 else [],
                assumptions=["pipelines with an identical activity structure share this setting"],
                alternatives=[Alternative(description=f"{_short(v)} (as in {q.name})", proposed=v)
                              for q, _, v in values] if len(distinct) > 1 else [],
                material_assumptions=True)


def _secretish(values) -> bool:
    text = repr([v for _, _, v in values]).lower()
    return any(m in text for m in ("password", "secret", "accountkey", "sas", "bearer", "connectionstring"))


def _short(v: Any) -> str:
    t = repr(v)
    return t if len(t) <= 60 else t[:57] + "..."


# =============================================================================================================
# error-handling convention (template only)
# =============================================================================================================

_GUARDED = ("copy", "sqlserverstoredprocedure", "script", "executepipeline")


def _handler_shape(pc: PipelineCtx, q, a: AdfActivity) -> Optional[tuple]:
    for x in pc.ev.siblings(q, a):
        if any(d.activity.lower() == a.name.lower() and ({"Failed", "Completed"} & set(d.conditions)) for d in x.depends_on):
            return ((x.type or "").lower(), str(x.configuration.get("pipeline") or ""),
                    tuple(sorted((x.configuration.get("parameters") or {}).keys())))
    return None


def rule_error_handling(pc: PipelineCtx) -> None:
    ev_, p = pc.ev, pc.pipe
    for a in ev_.acts(p):
        t = (a.type or "").lower()
        if t not in _GUARDED or _handler_shape(pc, p, a) is not None:
            continue
        peers = []
        for q in ev_.discovery.pipelines:
            if q.name == p.name:
                continue
            same = [b for b in ev_.acts(q) if (b.type or "").lower() == t and b.parent_activity_id is None]
            if same and all(_handler_shape(pc, q, b) is not None for b in same):
                peers.append((q, _handler_shape(pc, q, same[0])))
        shapes = {s for _, s in peers}
        if len(peers) < 2 or len(shapes) != 1:
            continue
        shape = next(iter(shapes))
        f = pc.out.finding(
            C.incomplete_configuration, "error_handling_gap",
            f"{a.type} '{a.name}' has no failure handler, but every {a.type} in {len(peers)} other pipelines has one.",
            pipeline=p.name, activity=a, severity=Severity.info, source_path=p.path,
            detail="a project convention, not evidence about this activity")
        pc.out.proposal(
            rule="error_handling_convention", finding=f, pipe=p, activity=a,
            missing_element="failure handler", action="add_activity", target_path=pc.pointer(pc.tokens(a)[:-1]),
            proposed={"handler_type": shape[0], "child_pipeline": shape[1] or None, "parameter_names": list(shape[2]),
                      "dependency": "Failed"},
            evidence=[ev(K.repeated_pattern, q.name, f"every {a.type} is followed by a {shape[0]} on Failure") for q, _ in peers],
            patches=[], generate=False,
            not_generated_reason="a project convention is not evidence of this activity's own behavior; adding a "
                                 "handler changes how failures are treated and needs a person's decision",
            assumptions=["this pipeline is meant to follow the same convention"],
            alternatives=[Alternative(description="failures here are deliberately unhandled")])

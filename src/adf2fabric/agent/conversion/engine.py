"""The conversion engine: semantic representation in, validated Fabric pipeline content out.

For each pipeline the engine

1. converts every activity (recursively, scope by scope) through the registry's handlers,
2. maps ``dependsOn`` onto the generated components (a source activity may become several
   components: its first components take its dependencies, its last components satisfy its
   dependents),
3. DERIVES each activity's status from recorded facts — strategy, differences, manual steps,
   structural validation — never from the mere fact that JSON was produced,
4. proves nothing was lost: every source activity has exactly one record, every source operation
   is covered by a component or explicitly reported uncovered, every source expression is
   accounted for,
5. assembles the pipeline content (parameters, variables, global-parameter replacements),
6. validates the content and keeps structural validity separate from runtime readiness.

It reads only the semantic representation; the original ADF source is never touched.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Optional

from adf2fabric.agent.conversion import handlers  # noqa: F401  (registers every rule)
from adf2fabric.agent.conversion.context import (
    ActivityBuilder,
    PipelineContext,
    ProjectContext,
    strip_nested,
)
from adf2fabric.agent.conversion.expr_convert import global_param_name
from adf2fabric.agent.conversion.handlers._common import downgrade_if_not_direct, has_masked_secret
from adf2fabric.agent.conversion import compatibility as compat
from adf2fabric.agent.conversion.triggers import build_schedules
from adf2fabric.agent.conversion.registry import ConversionRule, rule_for
from adf2fabric.agent.conversion.pipeline_semantics import outcome_notes, verify_graph
from adf2fabric.agent.conversion.validation import Validation, validate_content
from adf2fabric.agent.discovery.adf_semantic.pipeline_analysis import walk
from adf2fabric.project.conversion import (
    ActivityConversion,
    ConversionIssue,
    ConversionStatus,
    ConversionStrategy,
    CoverageReport,
    DependencyMapping,
    FabricComponent,
    Impact,
    InferredElement,
    IssueSeverity,
    PipelineConversion,
    Prerequisite,
    TriggerNote,
    ValidationResult,
)
from adf2fabric.project.discovery import AdfActivity, AdfPipeline, AdfTrigger

_SEVERITY_ORDER = {
    ConversionStatus.converted: 0, ConversionStatus.converted_with_differences: 1,
    ConversionStatus.manual_required: 2, ConversionStatus.blocked: 3,
}
_PLACEHOLDER_NOTE = ("A Fail activity stands in for it: the dependency graph stays intact and any run stops here "
                     "loudly instead of silently skipping the operation.")


@dataclass
class _Built:
    act: AdfActivity
    cx: ActivityBuilder
    rule: Optional[ConversionRule]
    slot: int
    top: list[str] = field(default_factory=list)  # top-level component names (not nested in another)
    entry: list[str] = field(default_factory=list)
    exit: list[str] = field(default_factory=list)
    dep_mappings: list[DependencyMapping] = field(default_factory=list)
    extra_issues: list[ConversionIssue] = field(default_factory=list)
    extra_limitations: list[str] = field(default_factory=list)


def worst(*statuses: ConversionStatus) -> ConversionStatus:
    return max(statuses, key=lambda s: _SEVERITY_ORDER[s], default=ConversionStatus.converted)


class ConversionEngine:
    def __init__(self, project: ProjectContext, inferences: Optional[dict[str, list]] = None):
        self.project = project
        # activity id -> reconstruction proposals applied to it (empty unless --apply-reconstruction)
        self.inferences = inferences or {}

    # -- pipeline ---------------------------------------------------------------------------------------
    def convert_pipeline(self, p: AdfPipeline) -> tuple[PipelineConversion, dict]:
        pc = PipelineContext(project=self.project, pipeline=p)
        pc.param_names = {x.name.lower() for x in p.parameters}
        for a in walk(p.activities):
            pc.reserve(a.name)

        top_json = self._scope(p.activities, pc)
        records: list[ActivityConversion] = [r for r in pc.registry if r is not None]

        content, added, notes = self._assemble(p, pc, top_json)
        validation = validate_content(content)
        self._apply_validation(records, validation)

        coverage = self._coverage(p, records)
        for r in records:
            r.runtime_ready = (r.structural_valid and r.status is not ConversionStatus.blocked
                               and not any(e.review_required for e in r.inferred_elements)
                               and all(q.bound for q in r.prerequisites)
                               and not validation.unbound.get(r.source_name)
                               and not any(validation.unbound.get(c.name) for c in r.components))

        structural = [i for _, i in validation.issues]
        graph_issues = verify_graph(p, records, content)  # source graph vs the generated JSON
        structural += graph_issues
        notes.extend(outcome_notes(p))
        pipeline_validation = ValidationResult(
            valid=not validation.errors and not any(i.severity is IssueSeverity.error for i in graph_issues),
            issues=structural)
        status = worst(*(r.status for r in records)) if records else ConversionStatus.converted
        if not coverage.complete:
            status = worst(status, ConversionStatus.manual_required)
            notes.append("Coverage check found activities, operations or expressions that were not accounted "
                         "for; see the coverage report.")
        if not pipeline_validation.valid:
            status = worst(status, ConversionStatus.manual_required)

        prereqs: dict[tuple[str, str], Prerequisite] = {}
        for r in records:
            for q in r.prerequisites:
                prereqs.setdefault((q.kind, q.name.lower()), q)
        strategy_counts: dict[str, int] = {}
        status_counts: dict[str, int] = {}
        for r in records:
            strategy_counts[r.strategy.value] = strategy_counts.get(r.strategy.value, 0) + 1
            status_counts[r.status.value] = status_counts.get(r.status.value, 0) + 1

        return PipelineConversion(
            source_pipeline=p.name, source_path=p.path, fabric_pipeline_name=p.name, status=status,
            strategy_counts=dict(sorted(strategy_counts.items())), status_counts=dict(sorted(status_counts.items())),
            parameters_added=added, activities=records, validation=pipeline_validation, coverage=coverage,
            prerequisites=sorted(prereqs.values(), key=lambda q: (q.kind, q.name.lower())),
            triggers=self._triggers(p), notes=notes,
            structural_valid=pipeline_validation.valid,
            runtime_ready=bool(records) and all(r.runtime_ready for r in records) and pipeline_validation.valid,
        ), content

    # -- scope -----------------------------------------------------------------------------------------------
    def _scope(self, activities: list[AdfActivity], pc: PipelineContext) -> list[dict]:
        built: list[_Built] = []
        by_name: dict[str, _Built] = {}
        for a in activities:
            b = self._convert_activity(a, pc)
            built.append(b)
            by_name.setdefault(a.name.lower(), b)
        for b in built:
            self._map_dependencies(b, by_name)
        out: list[dict] = []
        for b in built:
            pc.registry[b.slot] = self._finalize(b)
            out.extend(b.cx.full[n] for n in b.top)
        return out

    def _convert_activity(self, a: AdfActivity, pc: PipelineContext) -> _Built:
        slot = len(pc.registry)
        pc.registry.append(None)  # keep source (pre-order) order; the parent is filled after its children

        def kids(branch: str) -> list[dict]:
            return self._scope([c for c in a.activities if c.branch == branch], pc)

        rule = rule_for(a.type)
        cx = ActivityBuilder(a, pc, kids)
        if any(w.code == "max_depth_exceeded" for w in a.warnings):
            # Discovery stopped reading below this activity: converting it would silently leave its nested activities out.
            from adf2fabric.agent.conversion.handlers._common import block
            cx.disposition = "redesign"
            block(cx, "nesting_too_deep", "its nested activities were not read: they sit deeper than the discovery depth limit, "
                  "so converting it would silently drop them.", "Flatten the nesting by moving the inner activities into a child pipeline.")
        elif rule is None:
            self._unregistered(cx)
        else:
            cx.disposition = rule.runtime_disposition
            try:
                rule.handler(cx)
            except Exception as exc:  # noqa: BLE001 - one activity never aborts the pipeline
                cx = ActivityBuilder(a, pc, lambda _b: [])
                from adf2fabric.agent.conversion.handlers._common import block
                block(cx, "converter_error", f"an internal error occurred while converting it ({type(exc).__name__}).")
        if not cx.components:  # defensive: a handler must always leave a component behind
            from adf2fabric.agent.conversion.handlers._common import block
            block(cx, "no_components", "the conversion produced no components.")
        downgrade_if_not_direct(cx)
        self._post_checks(cx)
        self._compatibility(cx)

        nested = {n for c in cx.components for n in c.children}
        b = _Built(act=a, cx=cx, rule=rule, slot=slot)
        b.top = [c.name for c in cx.components if c.name not in nested]
        b.entry = cx.entry or b.top[:1]
        b.exit = cx.exit or b.top[:1]
        return b

    def _compatibility(self, cx: ActivityBuilder) -> None:
        """Retired / deprecated / preview activity types, from the catalog (entries the handlers block are skipped)."""
        for e in compat.for_activity(cx.act.type):
            if e.emit:
                cx.issue(compat.issue_code(e), compat.issue_message(e), impact=e.impact,
                         action=e.alternatives[0] if e.alternatives else "")
                for alt in e.alternatives:
                    cx.alternative(alt)

    def _unregistered(self, cx: ActivityBuilder) -> None:
        from adf2fabric.agent.conversion.handlers._common import block
        cx.disposition = "redesign"
        block(cx, "no_conversion_rule",
              f"no conversion rule exists for activity type '{cx.act.type}'.",
              "Re-create the activity manually in Fabric, or add a conversion rule for this type.")

    def _post_checks(self, cx: ActivityBuilder) -> None:
        a = cx.act
        # any secret masked by the semantic stage is a value a person must re-enter
        if any(has_masked_secret(c) for c in cx.full.values()):
            cx.manual("secret_masked", "A secret value was masked in the semantic record and cannot be carried over.",
                      "Re-enter the secret through a Fabric connection or a secure parameter; never in pipeline JSON.")
        covered = {c.operation for c in cx.coverage if c.covered_by}
        if cx.strategy is not ConversionStrategy.manual:
            for op in a.operations:
                if op.value not in covered:
                    cx.issue("operation_not_covered", f"Source operation '{op.value}' is not covered by any generated "
                             "component.", impact=Impact.manual, severity=IssueSeverity.warning,
                             action="Review the generated components.")

    # -- dependencies ---------------------------------------------------------------------------------------------
    def _map_dependencies(self, b: _Built, by_name: dict[str, _Built]) -> None:
        for dep in b.act.depends_on:
            d = by_name.get(dep.activity.lower())
            if d is None:
                b.extra_issues.append(ConversionIssue(
                    code="dependency_unresolved", severity=IssueSeverity.warning, impact=Impact.manual,
                    message=f"Depends on '{dep.activity}', which is not in the same activities list.",
                    action="Fix the dependency in the source."))
                b.dep_mappings.append(DependencyMapping(source_activity=dep.activity, conditions=dep.conditions,
                                                        note="unresolved dependency; not mapped"))
                continue
            conds = list(dep.conditions)
            if dep.unknown_conditions or not conds:
                b.extra_issues.append(ConversionIssue(
                    code="dependency_condition_unknown", severity=IssueSeverity.warning, impact=Impact.manual,
                    message=f"Dependency on '{dep.activity}' has unknown or missing conditions "
                            f"{dep.unknown_conditions or '[]'}.", action="Use Succeeded, Failed, Skipped or Completed."))
                conds = conds or ["Succeeded"]
            targets = list(d.exit)
            for entry in b.entry:
                json_ = b.cx.full[entry]
                for t in targets:
                    edge = {"activity": t, "dependencyConditions": conds}
                    if edge not in json_["dependsOn"]:
                        json_["dependsOn"].append(copy.deepcopy(edge))
            note = ""
            non_success = [c for c in conds if c != "Succeeded"]
            if d.cx.strategy is ConversionStrategy.manual and non_success:
                msg = (f"It runs on {'/'.join(non_success)} of '{dep.activity}', which could not be converted and is "
                       "replaced by a Fail placeholder that always fails - this branch would run unexpectedly.")
                b.extra_issues.append(ConversionIssue(
                    code="depends_on_blocked_activity", severity=IssueSeverity.warning, impact=Impact.difference,
                    message=msg, action=f"Convert '{dep.activity}' before enabling this pipeline."))
                b.extra_limitations.append(msg)
                note = "depends on a blocked activity"
            elif non_success and (len(d.top) > 1 or d.cx.strategy is ConversionStrategy.composite):
                msg = (f"'{dep.activity}' became several components; the {'/'.join(non_success)} condition is applied to "
                       f"its final component only ({', '.join(targets)}), so a failure earlier in that chain does not "
                       "trigger it.")
                b.extra_issues.append(ConversionIssue(
                    code="composite_dependency_condition", severity=IssueSeverity.warning, impact=Impact.difference,
                    message=msg, action="Review the failure path of this dependency."))
                b.extra_limitations.append(msg)
                note = "condition applied to the final component of a composite"
            b.dep_mappings.append(DependencyMapping(source_activity=dep.activity, conditions=conds,
                                                    target_activities=targets, note=note))

    # -- one activity's record --------------------------------------------------------------------------------------
    def _finalize(self, b: _Built) -> ActivityConversion:
        cx, a = b.cx, b.act
        issues = [*cx.issues, *b.extra_issues]
        limitations = [*cx.limitations, *b.extra_limitations]
        if cx.output_notes and a.output_consumers:
            readers = sorted({u.activity_id.rsplit("/", 1)[-1] for u in a.output_consumers})
            issues.append(ConversionIssue(
                code="output_shape_unverified", severity=IssueSeverity.info, impact=Impact.none,
                message=f"Activities {', '.join(readers)} read this activity's output. " + " ".join(cx.output_notes),
                action="Run it once in Fabric and compare the output fields those expressions read."))
        inferred = self._inferred(a, cx, issues, limitations)
        impacts = {i.impact for i in issues}
        if cx.strategy is ConversionStrategy.manual or Impact.blocker in impacts:
            status = ConversionStatus.blocked
        elif Impact.manual in impacts:
            status = ConversionStatus.manual_required
        elif Impact.difference in impacts:
            status = ConversionStatus.converted_with_differences
        else:
            status = ConversionStatus.converted
        created = {p.creates_activity_id for p in self.inferences.get(a.activity_id, []) if p.creates_activity_id}
        components = [
            FabricComponent(name=c.name, fabric_type=c.fabric_type, role=c.role,
                            origin="inferred" if a.activity_id in created else "source",
                            configuration=strip_nested(cx.full[c.name])[0], children=c.children)
            for c in cx.components
        ]
        manual = list(cx.manual_steps) + [
            f"Review inferred element {e.proposal_id} ({e.confidence}): {e.missing_element}. "
            f"Assumptions: {'; '.join(e.assumptions) or 'none'}." for e in inferred if e.review_required]
        if cx.strategy is ConversionStrategy.manual:
            manual.append(_PLACEHOLDER_NOTE)
        return ActivityConversion(
            source_activity_id=a.activity_id, source_name=a.name, source_type=a.type,
            source_pointer=a.source_location.pointer if a.source_location else None,
            parent_activity_id=a.parent_activity_id, children=[c.activity_id for c in a.activities],
            strategy=cx.strategy, status=status, runtime_disposition=cx.disposition, summary=cx.summary or
            f"{a.type} '{a.name}': {status.value}.",
            components=components, entry_components=list(b.entry), exit_components=list(b.exit),
            property_mappings=cx.property_mappings, expression_conversions=cx.expression_conversions,
            dependency_mappings=b.dep_mappings, operation_coverage=cx.coverage, warnings=issues,
            limitations=limitations, manual_steps=manual, alternatives=cx.alternatives,
            prerequisites=cx.prerequisites, inferred_elements=inferred,
        )

    def _inferred(self, a: AdfActivity, cx: ActivityBuilder, issues: list, limitations: list) -> list[InferredElement]:
        """Record, and mark in the generated JSON, everything that came from a reconstruction proposal."""
        props = sorted(self.inferences.get(a.activity_id, []), key=lambda p: p.id)
        if not props:
            return []
        out = []
        for p in props:
            review = p.confidence.value != "high"  # HIGH rests on explicit evidence; anything lower needs a person
            out.append(InferredElement(
                proposal_id=p.id, rule=p.rule, confidence=p.confidence.value, missing_element=p.missing_element,
                target_path=p.target_path, assumptions=list(p.assumptions), review_required=review))
            msg = (f"{p.missing_element} was INFERRED by reconstruction rule '{p.rule}' ({p.confidence.value} "
                   f"confidence), not authored in the source.")
            limitations.append(msg)
            issues.append(ConversionIssue(
                code="inferred_element", severity=IssueSeverity.warning, impact=Impact.difference, message=msg,
                action="Review the proposal's evidence and assumptions in the reconstruction report."
                if review else "Evidence is explicit; spot-check it in the reconstruction report."))
        marker = "; ".join(f"{e.proposal_id} ({e.confidence}): {e.missing_element}" for e in out)
        for c in cx.components:
            if c.role in ("primary", "placeholder"):
                cx.full[c.name]["userProperties"] = [{"name": "adf2fabric.inferred", "value": marker}]
        return out

    # -- validation feedback --------------------------------------------------------------------------------------------
    def _apply_validation(self, records: list[ActivityConversion], v: Validation) -> None:
        by_component = {c.name: r for r in records for c in r.components}
        per: dict[str, list[ConversionIssue]] = {}
        for comp, issue in v.issues:
            rec = by_component.get(comp or "")
            if rec is not None:
                per.setdefault(rec.source_activity_id, []).append(issue)
        for r in records:
            issues = per.get(r.source_activity_id, [])
            errors = [i for i in issues if i.severity is IssueSeverity.error]
            r.validation = ValidationResult(valid=not errors, issues=issues)
            r.structural_valid = not errors
            if errors and r.status in (ConversionStatus.converted, ConversionStatus.converted_with_differences):
                r.status = ConversionStatus.manual_required
                r.warnings.append(ConversionIssue(
                    code="structural_validation_failed", severity=IssueSeverity.error, impact=Impact.manual,
                    message=f"The generated components fail validation: {errors[0].message}",
                    action=errors[0].action or "Fix the reported problem, then re-convert."))
                r.manual_steps.append(errors[0].action or errors[0].message)

    # -- coverage ----------------------------------------------------------------------------------------------------------
    def _coverage(self, p: AdfPipeline, records: list[ActivityConversion]) -> CoverageReport:
        src = [a for a in walk(p.activities)]
        ids = [r.source_activity_id for r in records]
        rec_by_id = {r.source_activity_id: r for r in records}
        missing = sorted(a.activity_id for a in src if a.activity_id not in rec_by_id)
        duplicated = sorted({i for i in ids if ids.count(i) > 1})
        blocked = sorted(r.source_activity_id for r in records if r.status is ConversionStatus.blocked)
        uncovered, unaccounted = [], []
        for a in src:
            r = rec_by_id.get(a.activity_id)
            if r is None:
                continue
            if r.status is not ConversionStatus.blocked:
                covered = {c.operation for c in r.operation_coverage if c.covered_by}
                uncovered += [f"{a.activity_id}:{op.value}" for op in a.operations if op.value not in covered]
            accounted = {e.source_path for e in r.expression_conversions}
            dropped = [m.source for m in r.property_mappings if not m.preserved]
            for e in a.expressions:
                if e.is_dynamic and r.status is not ConversionStatus.blocked and e.property_path not in accounted \
                        and not any(e.property_path.startswith(d) for d in dropped):
                    unaccounted.append(f"{a.activity_id}:{e.property_path}")
        for r in records:
            ua = [u for u in unaccounted if u.startswith(r.source_activity_id + ":")]
            if ua:
                r.warnings.append(ConversionIssue(
                    code="expression_not_converted", severity=IssueSeverity.warning, impact=Impact.manual,
                    message=f"Dynamic content at {', '.join(u.split(':', 1)[1] for u in ua)} was not carried through "
                            "conversion.", action="Review the generated activity."))
                if r.status in (ConversionStatus.converted, ConversionStatus.converted_with_differences):
                    r.status = ConversionStatus.manual_required
        complete = not (missing or duplicated or uncovered or unaccounted)
        return CoverageReport(source_activities=len(src), accounted=len(set(ids) & {a.activity_id for a in src}),
                              missing=missing, duplicated=duplicated, blocked=blocked,
                              uncovered_operations=sorted(uncovered), unaccounted_expressions=sorted(unaccounted),
                              complete=complete)

    # -- pipeline content ----------------------------------------------------------------------------------------------------
    def _assemble(self, p: AdfPipeline, pc: PipelineContext, activities: list[dict]) -> tuple[dict, list[str], list[str]]:
        notes: list[str] = []
        parameters: dict[str, Any] = {}
        for prm in p.parameters:
            spec: dict[str, Any] = {"type": prm.type or "String"}
            if prm.has_default:
                spec["defaultValue"] = prm.default_raw
            parameters[prm.name] = spec
            if (prm.type or "").lower() == "securestring":
                notes.append(f"Parameter '{prm.name}' is a SecureString; Fabric's documentation does not describe "
                             "secure pipeline parameters - verify how it is stored.")
        added: list[str] = []
        needed = sorted({*self.project.globals_needed.get(p.name.lower(), []), *pc.used_globals})
        for g in needed:
            gp = self.project.global_params.get(g.lower())
            target = global_param_name(g, pc.param_names)
            added.append(target)
            spec = {"type": (gp.type if gp and gp.type else "String")}
            if gp is None:
                notes.append(f"Global parameter '{g}' is read by this pipeline but not defined in the discovered "
                             "factory configuration; its value must be supplied.")
            elif gp.has_default:
                spec["defaultValue"] = gp.default_raw
            parameters[target] = spec
        if added:
            notes.append("Global parameters do not exist in Fabric: " + ", ".join(sorted(added)) +
                         " carry the factory default values as pipeline parameters (supply per-environment values "
                         "when invoking or scheduling).")

        variables = {}
        for v in p.variables:
            spec = {"type": v.type or "String"}
            if v.has_default:
                spec["defaultValue"] = v.default_raw
            variables[v.name] = spec

        props: dict[str, Any] = {}
        if p.description:
            props["description"] = p.description
        props["activities"] = activities
        if parameters:
            props["parameters"] = parameters
        if variables:
            props["variables"] = variables
        if p.annotations:
            props["annotations"] = list(p.annotations)
        if p.concurrency is not None:
            notes.append(f"Pipeline concurrency ({p.concurrency}) has no documented Fabric property in the "
                         "definition and was not carried over.")
        for key in p.unknown_properties:
            notes.append(f"Pipeline property '{key}' was not carried over (not part of the documented definition).")
        return {"properties": props}, sorted(added), notes

    # -- triggers --------------------------------------------------------------------------------------------------------------
    def _triggers(self, p: AdfPipeline) -> list[TriggerNote]:
        out = []
        for t in self.project.triggers:
            if p.name.lower() not in {r.lower() for r in t.pipeline_refs}:
                continue
            out.append(_trigger_note(t))
        return sorted(out, key=lambda n: n.name.lower())


def _trigger_note(t: AdfTrigger) -> TriggerNote:
    rec = t.recurrence
    when = ""
    if rec:
        when = f"every {rec.interval or '?'} {rec.frequency or '?'}" + (f" from {rec.start_time}" if rec.start_time else "") \
            + (f" ({rec.time_zone})" if rec.time_zone else "")
    kind = (t.type or "").lower()
    if kind == "scheduletrigger":
        action = "Create a Fabric schedule on the pipeline with the same recurrence."
    elif kind == "tumblingwindowtrigger":
        action = ("Create an interval schedule. Tumbling-window semantics (window start/end values, dependencies, "
                  "backfill) do not exist in Fabric; windowing logic must move into the pipeline.")
    elif "event" in kind:
        action = "Create a Fabric event trigger (Activator) for the same storage event."
    else:
        action = "Create the equivalent Fabric trigger or schedule manually."
    sched = build_schedules(t)
    if sched.status in ("generated", "generated_with_differences"):
        action += (" A Fabric schedule definition was generated (deploy with --schedules; it is created disabled)."
                   + (" It differs from the source in documented ways - read the schedule notes."
                      if sched.status == "generated_with_differences" else ""))
    elif sched.status == "manual":
        action = action + " (A schedule could not be generated exactly: " + " ".join(sched.notes) + ")"
    return TriggerNote(
        name=t.name, trigger_type=t.type, runtime_state=t.runtime_state, pipelines=sorted(t.pipeline_refs),
        summary=f"{t.type or 'Trigger'} {when}".strip() + (f"; state {t.runtime_state}" if t.runtime_state else ""),
        action=action, details=_trigger_details(t),
        schedule_status=sched.status, schedules=sched.schedules, schedule_notes=sched.notes)


_TRIGGER_SCOPE_VALUES = ("@trigger(", "@triggerbody(", "@triggeroutputs(")


def _shown(value: str, limit: int = 120) -> str:
    return value if len(value) <= limit else value[:limit] + "..."


def _trigger_details(t: AdfTrigger) -> list[str]:
    """What a person needs to recreate this trigger in Fabric: event settings and the parameters it passed."""
    out: list[str] = []
    e = t.event
    if e is not None:
        if e.scope:
            out.append(f"Listens to: `{e.scope}`")
        if e.events:
            out.append("Events: " + ", ".join(e.events))
        for label, value in (("Blob path begins with", e.blob_path_begins_with),
                             ("Blob path ends with", e.blob_path_ends_with),
                             ("Subject begins with", e.subject_begins_with),
                             ("Subject ends with", e.subject_ends_with)):
            if value:
                out.append(f"{label}: `{value}`")
        if e.ignore_empty_blobs is not None:
            out.append(f"Ignore empty blobs: {e.ignore_empty_blobs}")
    for pipeline, params in sorted(t.pipeline_parameters.items()):
        for name, value in sorted(params.items()):
            line = f"Parameter `{name}` of `{pipeline}` was set to `{_shown(value)}`"
            if value.lower().startswith(_TRIGGER_SCOPE_VALUES):
                line += (". Trigger-scope expressions have no Fabric equivalent; Fabric offers the system variables "
                         "pipeline().TriggerTime and pipeline().TriggerName, which may cover it")
            out.append(line)
    return out

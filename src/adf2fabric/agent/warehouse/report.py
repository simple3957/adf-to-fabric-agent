"""Markdown report for a warehouse migration run (the JSON report is the MigrationReport model itself)."""

from __future__ import annotations

from collections import Counter

from adf2fabric.agent.warehouse.models import MigrationReport, ObjectKind, Resolution


def markdown(r: MigrationReport) -> str:
    L: list[str] = []
    add = L.append
    add(f"# Warehouse migration report: {r.project_id}\n")
    add(f"Generated {r.created_at:%Y-%m-%d %H:%M} UTC. Source: {r.source.get('type')} ({r.source.get('mode')}). "
        f"Target: {r.target.get('warehouse') or 'not configured'}"
        f"{' in workspace ' + r.target['workspace'] if r.target.get('workspace') else ''} "
        f"({'read-only inspection' if r.target.get('inspected') else 'not inspected'}).\n")
    d = r.deploy
    if d and d.get("applied"):
        add(f"**Migration run: {d.get('status')}.** Objects were created and loaded as listed under *Deploy run*.\n")
    elif d:
        add(f"Dry run ({d.get('status')}): nothing was created or loaded.\n")
    else:
        add("Nothing was deployed or loaded: this run is discovery, planning (dry-run DDL) and validation only.\n")

    add("## Summary\n")
    add("| | Count |\n|---|---|")
    for k, v in r.catalog_summary.items():
        add(f"| {k} | {v} |")
    add("")
    usage = Counter(e.adf_usage for e in r.manifest if e.kind != ObjectKind.other)
    vals = Counter(e.validation for e in r.manifest if e.kind != ObjectKind.other)
    add(f"ADF usage: {', '.join(f'{k} {v}' for k, v in sorted(usage.items()))}. "
        f"Validation: {', '.join(f'{k} {v}' for k, v in sorted(vals.items()))}.\n")

    if r.adf:
        add("## ADF sources\n")
        for k in ("live", "git"):
            v = r.adf.get(k)
            add(f"- **{k}**: " + (", ".join(f"{x} {y}" for x, y in v.items()) if v else "not read")
                + (" (used for the analysis)" if r.adf.get("used") == k else ""))
        if r.adf.get("live_error"):
            add(f"- live read failed: {r.adf['live_error']}")
        diffs = r.adf.get("differences") or []
        if r.adf.get("live") and r.adf.get("git"):
            add(f"- live vs Git: {len(diffs)} difference(s)" + ("" if diffs else " - they match"))
            for x in diffs:
                add(f"  - {x}")
        add("")
    if r.scope:
        add(f"## Scope ({r.scope.get('mode')})\n")
        add("| Object | In / out | Why |\n|---|---|---|")
        for k, why in r.scope.get("included", {}).items():
            add(f"| {k} | in | {why} |")
        for k, why in r.scope.get("excluded", {}).items():
            add(f"| {k} | out | {why} |")
        add("")

    add("## Objects\n")
    add("| Object | Kind | ADF usage | Action | Validation | Source rows | Notes |\n|---|---|---|---|---|---|---|")
    rows = {c.object_key: c.detail for c in r.checks if c.check == "row_count"}
    for e in r.manifest:
        note = "; ".join(e.warnings[:2]).replace("|", "/")
        add(f"| {e.source_key} | {e.kind.value} | {e.adf_usage} | {e.action} | {e.validation} | "
            f"{rows.get(e.source_key, '')} | {note} |")
    add("")

    issues = [c for c in r.checks if c.status in ("differs", "missing", "review")]
    add("## Validation findings\n")
    if not issues:
        add("No differences.\n")
    for c in issues:
        add(f"- **{c.object_key}** {c.check}: {c.status} - {c.detail}")
    add("")

    add("## ADF references\n")
    add("| Asset | Kind | Object | Resolution | Note |\n|---|---|---|---|---|")
    for x in r.references:
        if x.asset_kind == "catalog_query":
            continue
        add(f"| {x.asset} | {x.asset_kind} | {x.object_key or '-'} | {x.resolution.value} | {x.note.replace('|', '/')} |")
    dyn = [x for x in r.references if x.asset_kind == "catalog_query"]
    if dyn:
        add(f"\nPlus {len(dyn)} run-time references from {', '.join(sorted({x.asset for x in dyn}))} "
            f"({dyn[0].note})")
    add("")

    add("## Transformations (dry run)\n")
    for e in r.manifest:
        if e.transformations:
            add(f"- **{e.source_key}**: " + " ".join(t if t.endswith(".") else t + "." for t in e.transformations))
    add("")
    broken = [x for x in r.references if x.resolution == Resolution.broken]
    if r.unresolved or broken:
        add("## Unresolved\n")
        for u in r.unresolved:
            add(f"- {u}")
        for x in broken:
            add(f"- {x.asset}: {x.note}")
        add("")
    if d:
        add("## Deploy run\n")
        add(f"Status **{d.get('status')}**, started {str(d.get('started_at'))[:19]} UTC"
            + (f"; already in target: {', '.join(d['conflicts'])}" if d.get("conflicts") else "") + ".\n")
        add("| Step | Object | Status | Detail |\n|---|---|---|---|")
        for st in d.get("steps", []):
            add(f"| {st['action']} | {st['object_key']} | {st['status']} | {st['detail'].replace('|', '/')[:160]} |")
        add("")
    if r.notes:
        add("## Notes\n")
        for n in r.notes:
            add(f"- {n}")
    return "\n".join(L) + "\n"

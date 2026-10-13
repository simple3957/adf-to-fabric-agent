# Skill: Conversion

Operating instructions for the Conversion Agent: it turns the ADF semantic record into Microsoft Fabric pipeline
definitions and says, honestly, what did not convert.

## Purpose

Answer one question per activity:

> "What does this become in Fabric, how faithfully, and what must a person still do?"

## Responsibilities

- Convert every discovered activity (nested ones included) through the registry's handlers. Nothing is dropped.
- Report each activity with a strategy (direct, transformed, composite, alternative, manual) and a status
  (converted, converted with differences, manual required, blocked). *Converted* is never "JSON was generated".
- Record connections, workspace items and gateways that still have to be bound, and never copy a secret.
- Check the generated pipeline against the source dependency graph and Microsoft's documented schema.
- **Check every component against the compatibility catalog** (`agent/conversion/compatibility.py`) and report
  retired, deprecated, preview and unavailable components with Microsoft's date and a way forward.
- Generate Fabric schedule definitions from triggers (exact mappings only); never guess a recurrence.
- Stop after writing `projects/<id>/conversion/`. Conversion never calls Azure or Fabric; only `deploy` does.

## How failures are handled (read this before touching the converter)

| Situation | What the agent does | Where it shows up |
|---|---|---|
| No Fabric equivalent (SSIS, U-SQL, classic ML, unknown type) | blocker plus a fail-fast placeholder activity, alternatives listed | status `blocked`, "Needs attention" |
| Needs redesign (Mapping Data Flow, Wrangling Data Flow, Synapse notebooks) | a scaffold bound to a Fabric item you create, alternatives listed | status `manual_required` |
| Retired / removed connector or connector version | dated finding; blocker if no replacement exists, manual if a current version exists | "Compatibility findings" section, issue `compat_*` |
| Deprecated or preview feature | dated finding with limitations | `compat_*` issue, `converted_with_differences` or `manual_required` |
| ADF trigger | a Fabric schedule definition is generated where the recurrence maps exactly (`conversion/schedules/`); `manual` with the reason where it does not; event triggers are not generated | `Fabric schedule:` line in the report, `deploy --schedules` |
| ADF change data capture resource | reported with its source, target and tables; no Copy job is generated | "Compatibility findings" (`artifact.adf_cdc`) |
| Unsupported expression function / accessor | expression kept verbatim, never guessed | `expression_not_converted`, `manual_required` |
| Over a documented limit (120 activities) | structural error, no automatic split | pipeline `structural_valid: false` |
| Missing / incomplete source | not converted silently; run `reconstruct` | reconstruction record |
| Runtime-only failures (SQL syntax, permissions, reserved words) | **cannot be detected statically**; found by running the pipeline | `deploy` + run, then activity-level results |

Rules the agent follows:

1. Never invent a Fabric component or property. Extend `fabric_schema.py` from Microsoft's pages.
2. Absence from the compatibility catalog means "not known to be a problem", not "supported".
3. A finding without a source page and a verified-on date does not belong in the catalog.
4. A removed component is fixed in the **source** (upgrade or replace it), not papered over in the output.
5. Never silently rewrite SQL, expressions or settings; report the difference.

## Keeping the catalog current

Microsoft changes lifecycle dates. When a page changes:

1. Re-read the page (links are in the catalog entry's `source_url`).
2. Update the entry, and `VERIFIED_ON` in `compatibility.py`, together.
3. Add or update the matching section in [`compatibility-playbook.md`](compatibility-playbook.md); a test fails if a
   catalog id is not documented there.
4. Run the tests; add a regression test shaped like the real case.

## Deterministic, no LLM

Every finding is a fixed function of the semantic record and the catalog. No model is used or needed.

## Success criterion

```text
Convert -> Validate -> Verify graph -> Scan compatibility -> Write report + record -> STOP
```

See [`compatibility-playbook.md`](compatibility-playbook.md) for what to do about each finding, and
[`../../docs/conversion.md`](../../docs/conversion.md) and
[`../../docs/conversion-failure-modes.md`](../../docs/conversion-failure-modes.md) for the reference.

# Pipeline parser (activity-level semantic representation)

`src/adf2fabric/agent/discovery/adf_semantic/parser_pipelines.py` turns an ADF pipeline definition into the semantic
representation a deterministic converter consumes. It **describes** what the pipeline does;
it contains no Fabric knowledge and uses no LLM — every field is a fixed function of the JSON.

Both intake modes (GitHub, live Azure) reach it through the same entry point:
`parser_pipelines.parse_pipeline(data, path)` (the old `parser.parse_pipeline` delegates to it).
Discovery calls `parser_pipelines.resolve_pipelines(...)` once all artifacts are parsed.

## Stages

| # | Stage | Where |
|---|-------|-------|
| 1-2 | JSON/schema validation, pipeline identity | `parser_pipelines` |
| 3 | Recursive activity discovery (ForEach / If / Switch cases / Until) | `parser_pipelines` |
| 4 | Type identification | `activity_registry` |
| 5 | Property, reference and expression extraction | `parser_pipelines`, `expressions` |
| 6 | Dependencies, control flow, reference checks, output consumers | `pipeline_analysis` |
| 7 | Dataset / linked-service / child-pipeline resolution | `pipeline_analysis.resolve_pipelines` |
| 8-9 | Classification, summaries, confidence | `activity_semantics` |
| 10 | Issues and ambiguity reporting | `AdfParseIssue` on activities and pipelines |

## Output (`AdfActivity` in `project/discovery.py`)

The original structural fields (`dataset_refs`, `pipeline_refs`, `activities`, ...) are unchanged;
the semantic fields are additive and defaulted. Mapping to the logical schema:

| Logical field | `AdfActivity` field |
|---|---|
| activity_id / name / activity_type | `activity_id` (`<pipeline>/<ancestors>/<name>`), `name`, `type` |
| semantic_category / operation | `semantic_category`, `operation`, `operations`, `semantic_tags` |
| parent_activity_id / nesting_path | `parent_activity_id`, `nesting_path`, `branch` (`activities`/`ifTrue`/`ifFalse`/`default`/`case:<v>`) |
| inputs / outputs | `inputs`, `outputs` (`AdfIO`: dataset, connector, store, format, linked service, location, query) |
| dependencies | `depends_on` (normalized conditions), `dependents`, `execution_order`, `stage` |
| expressions | `expressions` (`raw` is verbatim; plus references, functions, role, parse status) |
| parameters / variables | `parameters`, `global_parameters`, `variables_read`, `variables_written` |
| configuration | `configuration` (interpreted), `raw_properties` (as authored, secret-masked), `policy`, `state` |
| references | `references` (typed, with `resolved`), `output_consumers` |
| semantic_summary / confidence / warnings | `semantic_summary`, `confidence` (high/medium/low/unsupported), `warnings` |
| source_location | `source_location` (file + JSON pointer) |

Pipeline level (`AdfPipeline`): `issues` (cycles, duplicate names, ...), `patterns`
(metadata-driven ForEach, incremental load, parent/child orchestration, failure handling,
recurring pipeline shape), `activity_type_counts`, `unknown_activity_types`.

## Rules the parser follows

* **Names are hints.** Tags come from properties, expressions and downstream use. A name that
  disagrees with the properties raises `name_contradicts_properties` /
  `name_suggests_different_type`; a name implying a watermark with no supporting query or
  comparison raises `name_hint_unverified`.
* **Nothing is guessed.** Expressions are parsed, never evaluated. Unparsable text is kept
  verbatim (`parse_status: unparsed`); functions outside the documented set are listed in
  `unknown_functions`.
* **Nothing is dropped.** Unknown activity types get a full record (`unsupported`, structure,
  references, expressions, dependencies preserved). Unknown properties go to `unknown_properties`.
* **Missing is not empty.** `missing_properties` (absent) vs `empty_properties` (present, empty).
* **Secrets are masked** in every preserved value (`SecureString`, credential-named fields, marker
  strings); each masking is a `value_redacted` info warning.

## Completeness: nothing seen in the source disappears

Every artifact and every activity is either a record or an explicit entry saying why not.

* **Independent inventory check.** `inventory.py` scans the raw pipeline JSON by a more general rule than the parser
  (every object in any list under a key ending in `activities`, plus any list object that looks like an activity) and
  compares pointer sets with what was parsed. `AdfPipeline.inventory` holds both counts and any difference;
  a difference also raises the pipeline issue `inventory_mismatch` and sets `inventory_mismatch_count`.
* **Explicit dependency graph.** `AdfPipeline.dependency_graph` lists every activity (nested included) and one edge per
  `dependsOn` entry with its conditions. Unresolved, cross-container (`outside_scope`) and `self` edges are kept, flagged,
  not dropped. `cycles` holds the exact members of each cycle (Tarjan). Dependencies never cross a container boundary.
* **Stable source path.** `activity_id` (names from the pipeline down, `#2` for a duplicate) and
  `source_location.pointer` (the JSON path, e.g. `properties.activities[2].typeProperties.cases[0].activities[1]`).
* **Ledger of unprocessed artifacts.** `AdfSemanticDiscovery.unprocessed_artifacts` lists each source artifact that did not
  become a record, with `reason`: `malformed_json`, `unreadable`, `not_a_json_object`, `parse_error`, `skipped`, or the
  informational `kind_not_parsed` (credentials, managed virtual networks). Any loss makes the run `partial`, and a conversion
  made from a lossy discovery is `partial` with a note per lost artifact. Details never carry file content.
* **Data flows** (`dataflow/*.json`: `MappingDataFlow`, `Flowlet`, `WranglingDataFlow`) are parsed into `AdfDataFlow`
  (sources, sinks, transformations, what they reference, script size only), linked from `ExecuteDataFlow`, and checked for
  unresolved references and flowlet cycles.
* **Malformed shapes never raise.** A field of the wrong type is a warning (`invalid_<field>`) and the activity is kept.
  Exhaustive single-value mutation of every fixture (about 17,000 cases) is part of the test suite's evidence.

Activity types follow Microsoft's ARM schema for `Microsoft.DataFactory/factories/pipelines` (40 types) plus the Synapse-only
`SqlPoolStoredProcedure`. HTTP and REST are connectors used through Copy (`HttpSource`, `RestSource`), and through the Web
activity; they are not activity types. Tests: `tests/test_discovery_activity_inventory.py`, fixtures in
`tests/fixtures/adf_activities/`.

## Extending

Add an `ActivitySpec` to `activity_registry.py` (category, operations, required and known keys).
A specific interpretation is a `_h_<name>` handler in `parser_pipelines.py` registered in
`_HANDLERS`; activities without a handler still get the generic extraction.

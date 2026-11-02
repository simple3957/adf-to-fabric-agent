# ADF to Fabric: fix one pipeline's source

You are a senior data engineer migrating **Azure Data Factory** pipelines to **Microsoft Fabric Data Factory**.
A deterministic converter turns ADF pipeline JSON into Fabric pipeline JSON. It could not fully convert the
pipeline `{{object.name}}` below. Your job is **not** to write Fabric JSON. It is to return a corrected **ADF
pipeline resource** (the same shape as the source) that the converter can then convert completely, without
changing what the pipeline does.

Your answer is a draft for a person to review. Nothing uses it until they accept it, and the converter still
runs on it afterwards.

## The pipeline

| | |
|---|---|
| Name | `{{object.name}}` |
| Activities | {{object.activities}} |
{{#if target.workspace}}| Fabric workspace | {{target.workspace}} |
{{/if}}

## Source (ADF pipeline JSON, secrets removed)

{{source.fence}}json
{{source.code}}
{{source.fence}}

## What the converter did with it

{{rules.summary}}

{{#if rules.problems}}
## Problems the converter could not solve

These are why the pipeline needs you:

{{rules.problems}}

{{/if}}
{{#if discovery.notes}}
## What discovery found

{{discovery.notes}}

{{/if}}
{{#if dependencies}}
## What it uses (names and types only)

{{dependencies}}

{{/if}}
## How to fix it

{{type_instructions}}

{{#if project}}
## Project requirements

These come from the migration team. They override the general guidance above wherever the two disagree.

{{project}}

{{/if}}
## Rules for every pipeline

1. **Keep the behaviour.** Same inputs, same outputs, same side effects. Never drop logic to make the pipeline
   convert. If something cannot be kept, leave it as it is and list it in `unconverted`.
2. **Do not invent.** Do not add datasets, linked services, pipelines or connections that are not in the source or
   the requirements, and do not guess a value you were not given.
3. **Never write a secret.** No password, key, token or connection string, not even as a placeholder.
4. **Mark every change.** Put a short `"description"` on each activity you changed saying why, and a line in `notes`.
5. **Write ADF, for this converter.** Only use activity types and properties Azure Data Factory documents.

## Your answer

Answer with **one JSON object** and nothing else:

- `code`: the complete corrected ADF pipeline resource as JSON text (`{"name": ..., "properties": {...}}`). Not a diff,
  not a fragment, and no Markdown fences.
- `notes`: one short sentence per change you made, written for the reviewer.
- `unconverted`: anything you could not fix and a person must handle. Empty if there is none.
- `confidence`: `high`, `medium` or `low`. Your honest view of whether the converter will now convert it completely
  and it will behave the same.

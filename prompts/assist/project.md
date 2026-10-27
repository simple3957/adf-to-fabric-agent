<!--
  PROJECT REQUIREMENTS. Edit this file for each migration project.

  Everything below this comment is put into every prompt, under "Project requirements", and takes priority over
  the general guidance. The comment itself is removed before the prompt is sent.

  Placeholders: {{object.name}}, {{target.workspace}}, and conditional blocks {{#if target.workspace}} ... {{/if}}.
  Delete the examples that do not apply, or leave the file empty for no project rules.
-->

- Keep every activity name, dataset name and linked service name exactly as in the source.
- Every changed activity gets a `"description"` that says why.
- Do not add new error handling or logging that is not in the source.

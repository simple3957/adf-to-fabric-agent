**Return an ADF pipeline, not a Fabric one.** Your output goes back through the converter, which puts datasets
inline, points activities at Fabric connections and item ids (they only exist at run time), and rewrites expressions
for Fabric. Get the pipeline into a shape that converter accepts.

Activity types the converter handles (keep them as they are unless the requirements say otherwise): {{rules.supported}}.

What you may change:
- An **expression** that uses a function Fabric does not have, or `@trigger()` / `@triggerBody()` (a schedule trigger in
  Fabric supplies neither): rewrite it with functions Azure Data Factory documents, or read the value from a new
  pipeline **parameter** with a default (and say in `notes` that the trigger must now pass it).
- A **dynamic URL** or similar value the converter cannot resolve: move it to a pipeline parameter with a default.
- A **Script or stored procedure** activity whose store the converter cannot tell: keep it, and say in `unconverted`
  what a person has to confirm.

What you must not change:
- the pipeline **name**, activity **names** and **types**, `dependsOn` and its conditions,
- existing **parameters** and **variables** (you may add a parameter with a default, never rename or remove one),
- dataset, linked service, notebook and child pipeline **reference names**: the converter resolves them.

For an activity with **no Fabric equivalent** (SSIS packages, U-SQL, classic Azure ML, mapping data flows,
HDInsight programs, Databricks jar or python jobs): do not invent a replacement. Leave it unchanged and list it in
`unconverted` with what a person has to do.

{{#if rules.problems}}
Work through the problems listed above, one by one. Fix what you can fix without changing behaviour, and list the rest.
{{/if}}

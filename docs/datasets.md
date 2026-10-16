# Datasets

Code: `src/adf2fabric/agent/conversion/handlers/_common.py`.

Fabric Data Factory pipelines have no dataset objects. Each ADF dataset is read at conversion time and its type, location and linked service are inlined into the activity as `datasetSettings`. Dataset parameters are substituted with the activity's `parameters`.

Raised as blockers instead of guessed:

- A referenced dataset that does not exist in the source.
- An inline endpoint with no dataset.
- A dataset with no type, or no linked service.

The linked service behind a dataset is resolved to a Fabric connection by the connections layer. See `docs/connections.md`.

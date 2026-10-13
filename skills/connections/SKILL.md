# Skill: Connections

Operating instructions for the connection layer: connect a source, connect Fabric, and configure one Fabric connection per
linked service without ever mishandling a credential.

## Rules

1. Sign-in happens through the CLIs on the backend machine (`az login`, `fab auth login`). Never ask for an Azure password; never
   return, store or log a token.
2. Secrets live only in `SecretVault` (memory, TTL). They never appear in project files, reports, generated code, job events,
   errors or API responses. Only field *names* held are reported.
3. Report a state only for what was exercised: `connection_tested` requires a credential actually used. List what was **not**
   verified alongside what was.
4. Do not invent server/database/account values; use explicit ADF properties. Never parse connection strings.
5. Do not assume a self-hosted IR is reusable in Fabric; map it to an on-premises gateway the user must configure.
6. OAuth and gateway-backed connections cannot be created by API: guide the user to create them in Fabric and reuse them.
7. Azure authentication does not prove Fabric authorization; check the workspace explicitly.

## Where things live

`connections/catalog.py` (connectors, auth methods, field specs), `resolution.py` (cards), `integration_runtime.py`,
`testing.py`, `service.py` (choices, apply, report), `providers/*`, `redaction.py`, `secret_vault.py`; HTTP in `api/`.
Add a connector by adding a `ConnectorSpec` to the catalog and tests; do not add logic to `parser_pipelines.py`.

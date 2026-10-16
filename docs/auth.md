# Auth and sign-in

Code: `src/adf2fabric/connections/providers/` (`azure_cli.py`, `fabric_cli.py`, `_login.py`), `connectivity/_azure.py`, `connectivity/fabric.py`, `repository/authentication.py`. Tests: `tests/test_azure_auth.py`.

- **Azure and Fabric** use the machine's CLI sign-in: `az login` and `fab auth login`. Tokens come from the CLI; the accelerator does not ask for a password.
- **Starting a sign-in** (`_login.py`) runs the CLI in the background on the machine running the accelerator. A browser or device code opens on that machine. Output is redacted and capped, and only the program name is recorded, not its arguments. One sign-in per manager at a time.
- **Git sources** take a token from an env var whose name you give. The token is not stored in project files.
- **Other secrets** (SQL and storage credentials, Key Vault references) are also read by env-var name. Key Vault handles the public cloud and sovereign-cloud vault suffixes.
- **Limits:** ARM and Fabric hosts are fixed to the public cloud (`management.azure.com`, `api.fabric.microsoft.com`).

See `docs/connections.md` for the UI side.

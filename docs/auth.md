# Auth and sign-in

Code: `src/adf2fabric/connections/providers/` (`azure_cli.py`, `fabric_cli.py`, `_login.py`), `connectivity/_azure.py`, `connectivity/fabric.py`, `repository/authentication.py`. Tests: `tests/test_azure_auth.py`.

- **Azure and Fabric** use the machine's CLI sign-in: `az login` and `fab auth login`. Tokens come from the CLI; the accelerator does not ask for a password.
- **Starting a sign-in** (`_login.py`) runs the CLI in the background on the machine running the accelerator. A browser or device code opens on that machine. Output is redacted and capped, and only the program name is recorded, not its arguments. One sign-in per manager at a time.
- **In the UI**, each side (ADF source, Fabric destination) has its own **account** dropdown, listing everyone signed in to the Azure CLI on the API machine. Nobody is pre-selected. Signing in with another account is the last choice in that same list (a device code by default). The tenant (directory) is asked for last, under Advanced, because most accounts do not need it. Signing out removes only the chosen account.
- **Two accounts.** ADF and Fabric can sit under different signed-in accounts. A command that names a subscription (`intake --factory --subscription`, ADF discovery, the ADF connection check) asks the Azure CLI for the account that owns that subscription (`AzureCliCredential(subscription=...)`); the API (and so the UI) also reaches Fabric as the account chosen for the destination, whatever the CLI default is. The command-line commands (`deploy`, `run`, `deploy-notebook` and so on) still use the CLI's default account, so keep the Fabric account as the default (`az account set --subscription <fabric tenant id>`); signing in to another account can change the default.
- **Git sources** take a token from an env var whose name you give. The token is not stored in project files.
- **Other secrets** (SQL and storage credentials, Key Vault references) are also read by env-var name. Key Vault handles the public cloud and sovereign-cloud vault suffixes.
- **Endpoints:** the ARM and Fabric hosts default to the public cloud and come from `src/adf2fabric/cloud.py` (`ADF2FABRIC_ARM_ENDPOINT`, `ADF2FABRIC_FABRIC_ENDPOINT`).

See `docs/connections.md` for the UI side.

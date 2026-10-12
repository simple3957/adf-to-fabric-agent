"""Create a Fabric cloud connection (ADLS Gen2 or SQL/Azure Synapse) with any API-supported credential type.

Run this yourself in a terminal where ``az login`` is the Fabric account. Secrets are read from a hidden
prompt, or from the environment variable named below; they are sent only to https://api.fabric.microsoft.com
and are never printed, logged or written to disk.

  ADLS Gen2   --auth key                (secret: ADLS_ACCOUNT_KEY)
              --auth sas                (secret: ADLS_SAS_TOKEN)
              --auth service-principal  (needs --sp-tenant --sp-client; secret: SP_SECRET)
              --auth workspace-identity (no secret; the workspace identity needs a storage role)
  SQL/Synapse --auth basic              (needs --username; secret: SQL_PASSWORD)
              --auth service-principal  (needs --sp-tenant --sp-client; secret: SP_SECRET)
              --auth workspace-identity (no secret)

  python scripts/create_fabric_connection.py adls --tenant <fabric-tenant-id> --name Sathvik_ADF \\
      --account datamigrationadftofabric --container migrationcontainer --auth key
  python scripts/create_fabric_connection.py sql  --tenant <fabric-tenant-id> --name Synapse_SQL \\
      --server transportationmigration.sql.azuresynapse.net --database transportationdedicated \\
      --auth basic --username transportationmk

OAuth2 (signing in as yourself) and other interactive credential types cannot be created through the API:
create those in the Fabric UI (Manage connections and gateways) and use the connection id it shows.

Endpoint and body follow Microsoft's "Connections - Create Connection" reference (connectivityType
ShareableCloud; ADLS = type/creationMethod AzureDataLakeStorage with `server` + `path`; SQL = Sql with `server` +
`database`; credential shapes Key, SharedAccessSignature, ServicePrincipal, Basic, WorkspaceIdentity).
"""

from __future__ import annotations

import argparse
import getpass
import os
import subprocess
import sys

API = "https://api.fabric.microsoft.com/v1"
AUTH_BY_KIND = {
    "adls": ["key", "sas", "service-principal", "workspace-identity"],
    "sql": ["basic", "service-principal", "workspace-identity"],
}


def fabric_token(tenant: str) -> str:
    out = subprocess.run(
        ["az", "account", "get-access-token", "--resource", "https://api.fabric.microsoft.com",
         "--tenant", tenant, "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True, shell=(os.name == "nt"))
    if out.returncode != 0 or len(out.stdout.strip()) < 100:
        sys.exit("Could not get a Fabric token. Run `az login --tenant <id> --allow-no-subscriptions` first.")
    return out.stdout.strip()


def secret(env: str, prompt: str) -> str:
    value = (os.environ.get(env) or getpass.getpass(f"{prompt} (hidden): ")).strip()
    if not value:
        sys.exit("No secret given.")
    return value


def text(name: str, value: str) -> dict:
    return {"dataType": "Text", "name": name, "value": value}


def build_credentials(a: argparse.Namespace) -> tuple[dict, str]:
    """(credentials object, the secret to scrub from any echoed response)."""
    if a.auth == "workspace-identity":
        return {"credentialType": "WorkspaceIdentity"}, ""
    if a.auth == "key":
        key = secret("ADLS_ACCOUNT_KEY", "Storage account key")
        return {"credentialType": "Key", "key": key}, key
    if a.auth == "sas":
        token = secret("ADLS_SAS_TOKEN", "SAS token").lstrip("?")
        return {"credentialType": "SharedAccessSignature", "token": token}, token
    if a.auth == "basic":
        pw = secret("SQL_PASSWORD", f"Password for SQL login {a.username}")
        return {"credentialType": "Basic", "username": a.username, "password": pw}, pw
    pw = secret("SP_SECRET", "Service principal secret")
    return {"credentialType": "ServicePrincipal", "tenantId": a.sp_tenant, "servicePrincipalClientId": a.sp_client,
            "servicePrincipalSecret": pw}, pw


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("kind", choices=list(AUTH_BY_KIND))
    ap.add_argument("--tenant", required=True, help="Fabric (Entra) tenant id, used to get the Fabric token.")
    ap.add_argument("--name", required=True, help="Connection display name.")
    ap.add_argument("--auth", help="Credential type (default: key for adls, basic for sql).")
    ap.add_argument("--privacy", default="Organizational", choices=["Organizational", "Private", "Public", "None"])
    ap.add_argument("--account", help="adls: storage account name.")
    ap.add_argument("--container", help="adls: container (file system) name.")
    ap.add_argument("--server", help="sql: server host, e.g. <workspace>.sql.azuresynapse.net.")
    ap.add_argument("--database", help="sql: database / dedicated pool name.")
    ap.add_argument("--username", help="sql, --auth basic: SQL login.")
    ap.add_argument("--sp-tenant", help="service-principal: the service principal's tenant id.")
    ap.add_argument("--sp-client", help="service-principal: the service principal's client (application) id.")
    a = ap.parse_args()

    a.auth = a.auth or AUTH_BY_KIND[a.kind][0]
    if a.auth not in AUTH_BY_KIND[a.kind]:
        ap.error(f"--auth for {a.kind} must be one of: {', '.join(AUTH_BY_KIND[a.kind])}")
    if a.kind == "adls":
        if not (a.account and a.container):
            ap.error("adls needs --account and --container")
        details = {"type": "AzureDataLakeStorage", "creationMethod": "AzureDataLakeStorage",
                   "parameters": [text("server", f"{a.account}.dfs.core.windows.net"), text("path", a.container)]}
    else:
        if not (a.server and a.database):
            ap.error("sql needs --server and --database")
        details = {"type": "SQL", "creationMethod": "SQL",
                   "parameters": [text("server", a.server), text("database", a.database)]}
    if a.auth == "basic" and not a.username:
        ap.error("--auth basic needs --username")
    if a.auth == "service-principal" and not (a.sp_tenant and a.sp_client):
        ap.error("--auth service-principal needs --sp-tenant and --sp-client")

    credentials, scrub = build_credentials(a)

    try:
        import requests
    except ImportError:
        sys.exit("The 'requests' package is required: pip install requests")

    body = {"connectivityType": "ShareableCloud", "displayName": a.name, "privacyLevel": a.privacy,
            "connectionDetails": details,
            "credentialDetails": {"singleSignOnType": "None", "connectionEncryption": "NotEncrypted",
                                  "skipTestConnection": False, "credentials": credentials}}
    r = requests.post(f"{API}/connections", json=body, timeout=60,
                      headers={"Authorization": f"Bearer {fabric_token(a.tenant)}"})
    shown = r.text.replace(scrub, "***") if scrub else r.text  # never echo the secret, even if the service does
    if r.status_code != 201:
        sys.exit(f"Fabric refused the request (HTTP {r.status_code}): {shown[:600]}")
    info = r.json()
    print(f"Created connection '{info.get('displayName')}'")
    print(f"Connection ID: {info.get('id')}")


if __name__ == "__main__":
    main()

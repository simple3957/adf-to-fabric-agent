"""The Azure Resource Manager and Fabric service endpoints, in one place.

Both default to the public cloud. A sovereign or private cloud sets one or both of these before the process starts:

* ``ADF2FABRIC_ARM_ENDPOINT``    e.g. ``https://management.<your-cloud-host>``
* ``ADF2FABRIC_FABRIC_ENDPOINT`` e.g. ``https://api.fabric.<your-cloud-host>``

Each must be ``https://<host>`` with nothing after the host: bearer tokens are only ever sent to these hosts, so a value
with a path, query, credentials or another scheme is refused instead of being trusted. The values are read once, at
import. The endpoints for a given sovereign cloud come from that cloud's own documentation; none are assumed here.
"""

from __future__ import annotations

import os
from urllib.parse import urlsplit

ENV_ARM = "ADF2FABRIC_ARM_ENDPOINT"
ENV_FABRIC = "ADF2FABRIC_FABRIC_ENDPOINT"

_PUBLIC_ARM = "https://management.azure.com"
_PUBLIC_FABRIC = "https://api.fabric.microsoft.com"


def _endpoint(env: str, default: str) -> str:
    raw = os.environ.get(env, "").strip().rstrip("/")
    if not raw:
        return default
    parts = urlsplit(raw)
    if (parts.scheme != "https" or not parts.hostname or parts.path or parts.query or parts.fragment
            or parts.username or parts.password):
        raise ValueError(f"{env} must look like https://<host> (no path, query or credentials); got {raw!r}.")
    return f"https://{parts.netloc.lower()}"


ARM_ENDPOINT = _endpoint(ENV_ARM, _PUBLIC_ARM)
ARM_HOST = urlsplit(ARM_ENDPOINT).netloc
ARM_RESOURCE = ARM_ENDPOINT + "/"          # for `az account get-access-token --resource`
ARM_SCOPE = ARM_ENDPOINT + "/.default"     # for azure-identity get_token

FABRIC_ENDPOINT = _endpoint(ENV_FABRIC, _PUBLIC_FABRIC)
FABRIC_API = FABRIC_ENDPOINT + "/v1"
FABRIC_SCOPE = FABRIC_ENDPOINT + "/.default"

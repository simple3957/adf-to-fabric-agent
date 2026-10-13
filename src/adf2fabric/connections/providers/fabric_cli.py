"""Fabric CLI provider (``fab``, the ms-fabric-cli package).

Built from the installed CLI's own help, not from assumptions (inspected: ``fab`` 1.7.0):

* ``fab auth status --output_format json`` -> ``result.data[0]`` with ``logged_in``, ``account``, ``tenant_id``, ``principal_id``,
  ``app_id`` and ``token_*`` presence fields. The ``token_*`` fields are never read or returned.
* ``fab auth login`` -> interactive browser (no flags), ``-u/-p/-t`` service principal secret, ``--certificate`` service
  principal certificate, ``--federated-token``, ``-I`` managed identity.
* ``fab api <endpoint> -X get`` -> authenticated Fabric REST (``FabCliApi``), used for workspace discovery.

Sign-in methods offered here: interactive, managed identity, service principal with a certificate. A service principal SECRET is
deliberately NOT offered: the CLI only accepts it as a command-line argument (``-p``), which other local processes can read.

The Fabric CLI's sign-in is a separate state from the Azure CLI's; neither is inferred from the other. ``capabilities()`` reads
``fab --help`` so an older or different CLI that lacks the commands we need is reported as unsupported, not assumed to work.
"""

from __future__ import annotations

import re
from typing import Optional

from adf2fabric.connections.fabric import FabCliApi, FabricService, _first_json
from adf2fabric.connections.models import Check, CheckStatus, ConnectionState, ProviderStatus
from adf2fabric.connections.providers._login import LoginAttempt, LoginManager
from adf2fabric.connections.redaction import redact
from adf2fabric.connections.runner import Runner, run_cli

EXE_ENV = "ADF2FABRIC_FAB_PATH"  # optional: full path to fab when it is not on PATH
INSTALL_HINT = "Install the Fabric CLI (`pip install ms-fabric-cli`), then run `fab auth login`."
REQUIRED_COMMANDS = ("auth", "api")  # what this provider relies on
LOGIN_METHODS = {
    "interactive": "Opens a browser on this machine to choose the account.",
    "managed_identity": "Uses the machine's managed identity (Azure VM / App Service); optional client id for a user-assigned one.",
    "service_principal_certificate": "A service principal with a PEM certificate file (tenant id, client id, certificate path).",
}
UNSUPPORTED_LOGIN_METHODS = {
    "service_principal_secret": ("The Fabric CLI only accepts a service principal secret as a command-line argument (-p), which "
                                 "other processes on this machine can read. Use a certificate, a managed identity, or the "
                                 "interactive sign-in."),
}
_GUID = re.compile(r"^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")


class FabricCliProvider:
    id = "fabric_cli"

    def __init__(self, runner: Runner = run_cli, login_manager: Optional[LoginManager] = None):
        self._run = runner
        self.logins = login_manager or LoginManager()

    def _fab(self, args: list[str], timeout: float = 60):
        return self._run(["fab", *args], timeout=timeout, exe_env=EXE_ENV)

    # ---- availability + capabilities ------------------------------------------------------------------------
    def version(self) -> Optional[str]:
        r = self._fab(["--version"], 40)
        if not r.found:
            return None
        m = re.search(r"version\s+([\w.\-+]+)", r.stdout)
        return m.group(1) if m else ("unknown" if r.ok else None)

    def capabilities(self) -> dict[str, bool]:
        """Which of the commands we rely on this CLI actually has (parsed from ``fab --help``)."""
        r = self._fab(["--help"], 40)
        names = set(re.findall(r"^\s{2}([a-z][a-z-]*)\s*:", r.stdout, re.M)) if r.found else set()
        return {c: c in names for c in REQUIRED_COMMANDS}

    # ---- sign-in state ---------------------------------------------------------------------------------------
    def status(self) -> ProviderStatus:
        st = ProviderStatus(provider=self.id)
        ver = self._fab(["--version"], 40)
        if not ver.found:
            st.state, st.error, st.actions = ConnectionState.not_configured, "The Fabric CLI is not installed.", [INSTALL_HINT]
            st.checks.append(Check(name="fab installed", status=CheckStatus.failed, detail="'fab' was not found on PATH"))
            return st
        st.available, st.version = True, self.version()
        st.checks.append(Check(name="fab installed", status=CheckStatus.passed, detail=st.version or ""))
        caps = self.capabilities()
        missing = [c for c, ok in caps.items() if not ok]
        if missing:
            st.state = ConnectionState.manual_action_required
            st.error = f"This Fabric CLI does not provide the command(s) needed: {', '.join(missing)}."
            st.actions = ["Upgrade the Fabric CLI (`pip install -U ms-fabric-cli`)."]
            st.checks.append(Check(name="required commands", status=CheckStatus.failed, detail=", ".join(missing)))
            return st
        st.checks.append(Check(name="required commands", status=CheckStatus.passed, detail=", ".join(REQUIRED_COMMANDS)))
        r = self._fab(["auth", "status", "--output_format", "json"], 60)
        data = _first_json(r.stdout)
        row = None
        if isinstance(data, dict):
            rows = (data.get("result") or {}).get("data")
            row = rows[0] if isinstance(rows, list) and rows else None
        if not isinstance(row, dict):
            st.state, st.error = ConnectionState.validation_failed, "`fab auth status` returned an unreadable result."
            st.checks.append(Check(name="auth status", status=CheckStatus.failed, detail=redact(r.text)[:200]))
            return st
        if not row.get("logged_in"):
            st.state, st.error = ConnectionState.authentication_required, "The Fabric CLI is not signed in."
            st.actions = ["Start the sign-in (`fab auth login`)."]
            st.checks.append(Check(name="signed in", status=CheckStatus.failed))
            return st
        # Only these fields are read; token_* presence fields are intentionally ignored.
        st.account = _clean(row.get("account"))
        st.tenant_id = _clean(row.get("tenant_id"))
        st.checks.append(Check(name="signed in", status=CheckStatus.passed, detail=st.account or ""))
        st.authenticated, st.state = True, ConnectionState.authenticated
        return st

    # ---- sign-in ---------------------------------------------------------------------------------------------
    def start_login(self, method: str = "interactive", *, tenant: Optional[str] = None, client_id: Optional[str] = None,
                    certificate_path: Optional[str] = None) -> LoginAttempt:
        if method in UNSUPPORTED_LOGIN_METHODS:
            raise ValueError(UNSUPPORTED_LOGIN_METHODS[method])
        if method not in LOGIN_METHODS:
            raise ValueError(f"Unknown Fabric CLI sign-in method '{method}'. Use one of: {', '.join(LOGIN_METHODS)}.")
        for name, value in (("tenant", tenant), ("client id", client_id)):
            if value and not _GUID.match(value):
                raise ValueError(f"The {name} must be a GUID.")
        argv = ["fab", "auth", "login"]
        if method == "managed_identity":
            argv.append("--identity")
            if client_id:
                argv += ["-u", client_id]
        elif method == "service_principal_certificate":
            if not (tenant and client_id and certificate_path):
                raise ValueError("A certificate sign-in needs the tenant id, client id and certificate path.")
            argv += ["-u", client_id, "--tenant", tenant, "--certificate", certificate_path]
        elif tenant:
            argv += ["--tenant", tenant]
        if self._fab(["--version"], 40).found is False:
            raise ValueError("The Fabric CLI is not installed. " + INSTALL_HINT)
        return self.logins.start(argv)

    # ---- discovery -------------------------------------------------------------------------------------------
    def service(self) -> FabricService:
        """Workspace/capacity/connection operations through this CLI's authenticated session."""
        return FabricService(FabCliApi(self._run))


def _clean(value: object) -> Optional[str]:
    text = str(value) if value not in (None, "", "N/A") else ""
    return redact(text) or None

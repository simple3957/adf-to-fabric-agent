"""The API's shared state: providers, per-session selections, the secret vault, jobs and the project store.

One container per server. ``Session`` holds what the UI has selected (source, destination) and nothing sensitive; secrets live
only in the container's ``SecretVault``. Live and Demo modes differ only in which providers the container builds, so every
route is identical in both.
"""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from adf2fabric.api.jobs import JobRegistry
from adf2fabric.config import load_config, projects_root
from adf2fabric.connections.fabric import RestFabricApi, FabricService
from adf2fabric.connections.keyvault import read_secret
from adf2fabric.connections.providers.azure_cli import AzureCliProvider
from adf2fabric.connections.providers.fabric_cli import FabricCliProvider
from adf2fabric.connections.providers.git_source import GitProvider
from adf2fabric.connections.secret_vault import SecretVault
from adf2fabric.connections.service import ConnectionService
from adf2fabric.connections.testing import ConnectorTester
from adf2fabric.project.store import ProjectStore

FABRIC_RESOURCE = "https://api.fabric.microsoft.com"
MODES = ("live", "demo")


@dataclass
class Session:
    id: str
    source: dict[str, Any] = field(default_factory=dict)  # kind, subscription, resource_group, ... (no secrets)
    destination: dict[str, Any] = field(default_factory=dict)  # provider (azure_cli|fabric_cli), tenant_id, workspace_id
    tested: dict[str, bool] = field(default_factory=dict)  # source / destination test outcomes


@dataclass
class Settings:
    projects_dir: Optional[Path] = None
    token_env: str = "ADF2FABRIC_API_TOKEN"
    origins_env: str = "ADF2FABRIC_ALLOWED_ORIGINS"
    max_upload_bytes: int = 200 * 1024 * 1024

    @property
    def token(self) -> Optional[str]:
        return os.environ.get(self.token_env) or None

    @property
    def allowed_origins(self) -> list[str]:
        raw = os.environ.get(self.origins_env) or "http://localhost:8501,http://127.0.0.1:8501"
        return [o.strip() for o in raw.split(",") if o.strip()]


class Container:
    def __init__(self, settings: Optional[Settings] = None, *, store: Optional[ProjectStore] = None, mode: str = "live",
                 azure: Optional[AzureCliProvider] = None, fabric_cli: Optional[FabricCliProvider] = None,
                 git: Optional[GitProvider] = None, tester: Optional[ConnectorTester] = None,
                 vault_reader: Optional[Callable[[str, str], str]] = None,
                 fabric_factory: Optional[Callable[["Session", str], FabricService]] = None):
        self.settings = settings or Settings()
        self.mode = mode
        self.store = store or ProjectStore(self.settings.projects_dir or projects_root(load_config()))
        self.azure = azure or AzureCliProvider()
        self.fabric_cli = fabric_cli or FabricCliProvider()
        self.git = git or GitProvider()
        self.vault = SecretVault()
        self.jobs = JobRegistry()
        self.sessions: dict[str, Session] = {}
        self._vault_reader = vault_reader
        self._fabric_factory = fabric_factory
        self.connections = ConnectionService(self.store, self.vault, tester or ConnectorTester(secret_reader=self.read_vault_secret),
                                             secret_reader=self.read_vault_secret)
        self.upload_dir = Path(tempfile.gettempdir()) / "adf2fabric-uploads"
        self.upload_dir.mkdir(parents=True, exist_ok=True)

    def session(self, session_id: str) -> Session:
        return self.sessions.setdefault(session_id, Session(id=session_id))

    # ---- Fabric destination -------------------------------------------------------------------------------------
    def fabric(self, session: Session, provider: Optional[str] = None) -> FabricService:
        """A FabricService through the chosen sign-in: the Azure CLI session (REST) or the Fabric CLI (``fab api``)."""
        provider = provider or session.destination.get("provider") or "azure_cli"
        if self._fabric_factory:
            return self._fabric_factory(session, provider)
        if provider == "fabric_cli":
            return self.fabric_cli.service()
        if provider != "azure_cli":
            raise ValueError(f"Unknown destination sign-in '{provider}'. Use azure_cli or fabric_cli.")
        tenant = session.destination.get("tenant_id")
        return FabricService(RestFabricApi(lambda: self.azure.token(tenant=tenant, resource=FABRIC_RESOURCE)))

    # ---- Key Vault ---------------------------------------------------------------------------------------------
    def read_vault_secret(self, vault_url: str, secret_name: str) -> str:
        """The secret's value, read with the user's own Azure sign-in. The caller must not log or store it."""
        if self._vault_reader:
            return self._vault_reader(vault_url, secret_name)
        from azure.identity import AzureCliCredential

        value, _info = read_secret(vault_url, secret_name, AzureCliCredential())
        return value

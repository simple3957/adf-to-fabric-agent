"""Who may touch which project: every project belongs to one organisation, and every request is checked against it.

Ownership is recorded when a project is created (``projects/<id>/access.yaml``: organisation, creating user, time). A principal may
see or change a project only if its organisation owns it, whatever its role; roles say WHAT it may do, ownership says to WHICH
projects. A project from another organisation is reported as not found (404), never forbidden, so its existence is not disclosed.
A project created before ownership was recorded belongs to the configured legacy organisation (``ADF2FABRIC_LEGACY_ORG``).

Jobs and uploads carry their creator's organisation and user and are checked the same way.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml

from adf2fabric.api.security.principals import Principal
from adf2fabric.project.store import ProjectStore


class ProjectAccess:
    def __init__(self, store: ProjectStore, legacy_org: str):
        self.store, self.legacy_org = store, legacy_org

    def _path(self, pid: str) -> Path:
        return self.store.project_dir(pid) / "access.yaml"

    def owner_org(self, pid: str) -> str:
        try:
            doc = yaml.safe_load(self._path(pid).read_text(encoding="utf-8")) or {}
            org = doc.get("org")
            return str(org) if org else self.legacy_org
        except (OSError, yaml.YAMLError):
            return self.legacy_org

    def record(self, pid: str, principal: Principal) -> None:
        """Record the owner of a NEW project. An existing record is never overwritten (a re-import keeps its owner)."""
        p = self._path(pid)
        if p.exists():
            return
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(yaml.safe_dump({"org": principal.org, "created_by": principal.id, "created_at": datetime.now(timezone.utc).isoformat()}), encoding="utf-8")

    def allowed(self, principal: Principal, pid: str) -> bool:
        return self.owner_org(pid) == principal.org

    def visible(self, principal: Principal) -> list[str]:
        return [p for p in self.store.list_ids() if self.allowed(principal, p)]

    def would_collide(self, principal: Principal, pid: str) -> bool:
        """True when a project with this id exists but belongs to another organisation (an import must not overwrite or reveal it)."""
        return self.store.exists(pid) and not self.allowed(principal, pid)

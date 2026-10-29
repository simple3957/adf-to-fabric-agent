"""Who is calling: principals, the user store, and token authentication.

Credentials are bearer tokens issued to a user. The store keeps only a SHA-256 hash of each token (tokens are 256-bit random
values, so a fast hash is appropriate), compares in constant time, and shows a token exactly once, when it is created or rotated.
A token is shown to the person; it is never written to the store, an audit record or a log.

Modes (``SecuritySettings.mode``):

* ``disabled``  the historical local mode: one implicit principal with every role. Acceptable only on loopback for one person;
                the server refuses to start in this mode on any other address.
* ``token``     every request must carry a valid token of an enabled user. A single legacy ``ADF2FABRIC_API_TOKEN`` still works
                and maps to one principal, but has no roles beyond ``legacy_roles`` (default: admin of the ``default`` org), so
                prefer named users.

Authentication ends here; authorisation (what the principal may do, and to which project) is ``access`` and ``permissions``.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field

from adf2fabric.api.security.permissions import Permission, permissions_for, valid_role

WILDCARD = "*"
TOKEN_PREFIX = "a2f_"
_USER_ID = __import__("re").compile(r"^[A-Za-z0-9][A-Za-z0-9_.@-]{0,63}$")
_ORG_ID = __import__("re").compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Grants(_Strict):
    """What the principal may point the server at. A client names a subscription, tenant, account or workspace; the server only
    accepts it if it is listed here (or the list holds ``*``). Empty means none."""

    subscriptions: list[str] = Field(default_factory=list)
    tenants: list[str] = Field(default_factory=list)
    accounts: list[str] = Field(default_factory=list)  # signed-in Azure CLI accounts the principal may act as
    workspaces: list[str] = Field(default_factory=list)  # Fabric workspace ids


class UserRecord(_Strict):
    id: str
    org: str
    roles: list[str]
    token_sha256: str  # hash only
    token_hint: str = ""  # the last 4 characters of the token, to tell tokens apart; not enough to use it
    disabled: bool = False
    grants: Grants = Field(default_factory=Grants)
    created_at: datetime
    created_by: str = ""
    token_rotated_at: Optional[datetime] = None


@dataclass(frozen=True)
class Principal:
    id: str
    org: str
    roles: frozenset[str]
    grants: Grants
    method: str  # token | legacy_token | local
    permissions: frozenset[Permission] = field(default_factory=frozenset)

    def can(self, perm: Permission) -> bool:
        return perm in self.permissions

    def granted(self, kind: str, value: Optional[str]) -> bool:
        """Whether this principal may target ``value`` for ``kind`` (subscriptions | tenants | accounts | workspaces)."""
        allowed = getattr(self.grants, kind)
        return bool(value) and (WILDCARD in allowed or value.lower() in {a.lower() for a in allowed})

    def public(self) -> dict[str, Any]:
        return {"id": self.id, "org": self.org, "roles": sorted(self.roles), "permissions": sorted(p.value for p in self.permissions),
                "method": self.method}


def make_principal(id_: str, org: str, roles: Iterable[str], grants: Grants, method: str) -> Principal:
    rs = frozenset(roles)
    return Principal(id_, org, rs, grants, method, permissions_for(rs))


LOCAL = make_principal("local", "default", ["admin"], Grants(subscriptions=[WILDCARD], tenants=[WILDCARD], accounts=[WILDCARD], workspaces=[WILDCARD]),
                       "local")


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def new_token() -> str:
    return TOKEN_PREFIX + secrets.token_urlsafe(32)


class UserError(Exception):
    pass


class UserStore:
    """Users in ``<projects_root>/_auth/users.yaml``. Writes are atomic; a lock serialises them inside the process."""

    def __init__(self, root: Path):
        self.path = Path(root) / "_auth" / "users.yaml"
        self._lock = threading.Lock()
        self._cache: tuple[tuple[int, int], dict[str, UserRecord]] | None = None  # ((mtime_ns, size), users): parsed again only when the file changed

    def _load(self) -> dict[str, UserRecord]:
        try:
            st = self.path.stat()
        except OSError:
            self._cache = None
            return {}
        stamp = (st.st_mtime_ns, st.st_size)
        cached = self._cache
        if cached is not None and cached[0] == stamp:
            return dict(cached[1])  # a copy: callers change it before saving
        try:
            raw = yaml.safe_load(self.path.read_text(encoding="utf-8")) or {}
            users = {u["id"].lower(): UserRecord.model_validate(u) for u in raw.get("users", [])}
            self._cache = (stamp, users)
            return dict(users)
        except Exception as exc:  # noqa: BLE001 - a damaged file must lock everyone out, not let everyone in
            self._cache = None
            raise UserError(f"The user store {self.path.name} cannot be read ({type(exc).__name__}); refusing all sign-ins until it is fixed.") from None

    def _save(self, users: dict[str, UserRecord]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".yaml.tmp")
        tmp.write_text(yaml.safe_dump({"users": [yaml.safe_load(u.model_dump_json()) for u in users.values()]}, sort_keys=False), encoding="utf-8")
        try:
            tmp.chmod(0o600)  # owner-only where the platform supports it (hashes, but still not for everyone to read)
        except OSError:
            pass
        tmp.replace(self.path)
        self._cache = None  # a write in this process is never hidden by a timestamp that did not move

    def has_users(self) -> bool:
        """Whether any user has ever been created (a cheap stat, so it can be asked on every request)."""
        try:
            return self.path.exists() and self.path.stat().st_size > 8
        except OSError:
            return False

    def list(self, org: Optional[str] = None) -> list[UserRecord]:
        with self._lock:
            return sorted((u for u in self._load().values() if org is None or u.org == org), key=lambda u: u.id)

    def get(self, id_: str) -> Optional[UserRecord]:
        with self._lock:
            return self._load().get(id_.lower())

    def create(self, id_: str, org: str, roles: list[str], grants: Optional[Grants] = None, created_by: str = "") -> tuple[UserRecord, str]:
        """(record, token). The token is returned here and nowhere else."""
        if not _USER_ID.match(id_):
            raise UserError("A user id is 1-64 characters: letters, digits, '_', '.', '@' or '-', starting with a letter or digit.")
        if not _ORG_ID.match(org):
            raise UserError("An organisation id is 1-64 characters: letters, digits, '_', '.' or '-'.")
        bad = [r for r in roles if not valid_role(r)]
        if bad or not roles:
            raise UserError(f"Unknown or missing role(s): {', '.join(bad) or '(none given)'}.")
        with self._lock:
            users = self._load()
            if id_.lower() in users:
                raise UserError(f"User '{id_}' already exists.")
            token = new_token()
            rec = UserRecord(id=id_, org=org, roles=sorted(set(roles)), token_sha256=hash_token(token), token_hint=token[-4:], grants=grants or Grants(),
                             created_at=datetime.now(timezone.utc), created_by=created_by)
            users[id_.lower()] = rec
            self._save(users)
            return rec, token

    def update(self, id_: str, *, roles: Optional[list[str]] = None, disabled: Optional[bool] = None, grants: Optional[Grants] = None) -> UserRecord:
        with self._lock:
            users = self._load()
            rec = users.get(id_.lower())
            if rec is None:
                raise UserError(f"No user '{id_}'.")
            if roles is not None:
                bad = [r for r in roles if not valid_role(r)]
                if bad or not roles:
                    raise UserError(f"Unknown or missing role(s): {', '.join(bad) or '(none given)'}.")
                rec = rec.model_copy(update={"roles": sorted(set(roles))})
            if disabled is not None:
                rec = rec.model_copy(update={"disabled": disabled})
            if grants is not None:
                rec = rec.model_copy(update={"grants": grants})
            users[id_.lower()] = rec
            self._save(users)
            return rec

    def rotate(self, id_: str) -> tuple[UserRecord, str]:
        with self._lock:
            users = self._load()
            rec = users.get(id_.lower())
            if rec is None:
                raise UserError(f"No user '{id_}'.")
            token = new_token()
            rec = rec.model_copy(update={"token_sha256": hash_token(token), "token_hint": token[-4:], "token_rotated_at": datetime.now(timezone.utc)})
            users[id_.lower()] = rec
            self._save(users)
            return rec, token

    def authenticate(self, token: str) -> Optional[Principal]:
        """The principal for a token, or None. Constant-time over all users so timing does not reveal which hash was close."""
        digest = hash_token(token)
        found: Optional[UserRecord] = None
        for rec in self._load().values():  # all users are compared on purpose: no early exit to time
            if hmac.compare_digest(rec.token_sha256, digest) and not rec.disabled:
                found = rec
        return make_principal(found.id, found.org, found.roles, found.grants, "token") if found else None

    def admin_count(self, org: str) -> int:
        return sum(1 for u in self._load().values() if u.org == org and "admin" in u.roles and not u.disabled)

"""Rewrite storage paths in code so they read through OneLake shortcuts instead of the storage account.

In the source, a notebook reaches ``abfss://<container>@<account>.dfs.core.windows.net/...`` with its own credential. In Fabric
it runs as the user or workspace identity, which often has no access to that account (a different tenant refuses every Fabric
token). A OneLake shortcut reaches it through a Fabric connection, so the path is rewritten to go through the shortcut:

    abfss://c@acct.dfs.core.windows.net/x   ->  abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Files/<shortcut>/x
    https://acct.dfs.core.windows.net/c/x   ->  https://onelake.dfs.fabric.microsoft.com/<ws>/<lakehouse>/Files/<shortcut>/x

OneLake URI forms (Microsoft Learn, "How do I connect to OneLake?", checked 2026-10-22): either GUIDs for BOTH the workspace
and the item, with no item type, or names, with the item type as an extension (``<lakehouse>.Lakehouse``). Names are not
allowed to contain spaces in the abfs form. Mixing a GUID and a name is refused here rather than guessed.

Only a literal ``(account, container)`` that the storage map gave a shortcut is rewritten. Paths built from variables,
``wasbs://`` / Blob endpoints, and accounts without a shortcut are left exactly as written and reported, never guessed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, Optional

ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"
_GUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_NAME = re.compile(r"^[A-Za-z0-9_.\-]{1,256}$")  # no spaces or special characters: the abfs form does not allow them
_ACCOUNT = r"[a-z0-9]{3,24}"
_CONTAINER = r"[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?"
_END = r"(?=[/\s'\"`)\]},;?]|$)"  # the host or container ends here: a lookalike such as ...windows.net.evil.example does not match
_ABFSS = re.compile(rf"\b(abfss?)://({_CONTAINER})@({_ACCOUNT})\.dfs\.core\.windows\.net{_END}", re.I)
_HTTPS = re.compile(rf"\bhttps://({_ACCOUNT})\.dfs\.core\.windows\.net/({_CONTAINER}){_END}", re.I)
# Recognised but never rewritten: a variable in place of the container or account, or the Blob endpoint / wasb driver.
_DYNAMIC = re.compile(r"\b(?:abfss?)://[^\s'\"@]*\{[^}]*\}[^\s'\"@]*@[^\s'\"]*\.dfs\.core\.windows\.net|\b(?:abfss?)://[^\s'\"@]+@[^\s'\"]*\{[^}]*\}[^\s'\"]*\.dfs\.core\.windows\.net", re.I)
# Any direct ADLS Gen2 reference, as the workload analyzer sees it: whatever the rewrite did not consume is reported.
_ANY_ADLS = re.compile(r"\b(?:abfss?://([\w.-]+)@([\w-]+)\.dfs\.core\.windows\.net|https://([\w-]+)\.dfs\.core\.windows\.net/([\w.-]+))", re.I)
_BLOB = re.compile(rf"\b(?:wasbs?://({_CONTAINER})@({_ACCOUNT})\.blob\.core\.windows\.net|https://({_ACCOUNT})\.blob\.core\.windows\.net/({_CONTAINER}))", re.I)


class RouteError(ValueError):
    pass


@dataclass(frozen=True)
class Target:
    """The lakehouse that holds the shortcuts: both GUIDs, or both names."""

    workspace: str
    lakehouse: str

    def __post_init__(self) -> None:
        ws_guid, lh_guid = bool(_GUID.match(self.workspace or "")), bool(_GUID.match(self.lakehouse or ""))
        if ws_guid != lh_guid:
            raise RouteError("Give the workspace and the lakehouse both as GUIDs or both as names (OneLake does not accept a mix).")
        if not ws_guid and not (_NAME.match(self.workspace or "") and _NAME.match(self.lakehouse or "")):
            raise RouteError("Workspace and lakehouse names may use letters, digits, _ . - only (no spaces); otherwise use their GUIDs.")

    @property
    def by_id(self) -> bool:
        return bool(_GUID.match(self.workspace))

    @property
    def item(self) -> str:
        return self.lakehouse if self.by_id else f"{self.lakehouse}.Lakehouse"

    def abfss(self, shortcut: str) -> str:
        return f"abfss://{self.workspace}@{ONELAKE_HOST}/{self.item}/Files/{shortcut}"

    def https(self, shortcut: str) -> str:
        return f"https://{ONELAKE_HOST}/{self.workspace}/{self.item}/Files/{shortcut}"


@dataclass
class Routing:
    text: str
    routed: int = 0
    routed_locations: list[str] = field(default_factory=list)  # account/container, never a path
    left: list[tuple[str, str]] = field(default_factory=list)  # (location, why), for findings


class Routes:
    """(account, container) -> shortcut name, from the storage map's shortcuts."""

    def __init__(self, pairs: Iterable[tuple[str, str, str]], target: Target):
        self.by_pair = {(a.lower(), c.lower()): s for a, c, s in pairs}
        self.target = target

    @classmethod
    def from_storage_map(cls, storage_map: dict, target: Target) -> "Routes":
        return cls(((s["account"], s["container"], s["shortcut"]) for s in storage_map.get("shortcuts", [])), target)

    def __bool__(self) -> bool:
        return bool(self.by_pair)

    def rewrite(self, code: str) -> Routing:
        out = Routing(code)
        seen: set[str] = set()

        def abfss(m: re.Match) -> str:
            container, account = m.group(2).lower(), m.group(3).lower()
            sc = self.by_pair.get((account, container))
            if sc is None:
                out.left.append((f"{account}/{container}", "no shortcut is planned for this account and container"))
                return m.group(0)
            out.routed += 1
            seen.add(f"{account}/{container}")
            return self.target.abfss(sc)

        def https(m: re.Match) -> str:
            account, container = m.group(1).lower(), m.group(2).lower()
            tail = re.match(r"[^\s'\"`)\]}]*", m.string[m.end():]).group(0)
            if "?" in tail:  # a query string (often a SAS token) must never be sent to OneLake
                out.left.append((f"{account}/{container}", "the URL carries a query string (for example a SAS token), so it is not rewritten"))
                return m.group(0)
            sc = self.by_pair.get((account, container))
            if sc is None:
                out.left.append((f"{account}/{container}", "no shortcut is planned for this account and container"))
                return m.group(0)
            out.routed += 1
            seen.add(f"{account}/{container}")
            return self.target.https(sc)

        text = _ABFSS.sub(abfss, code)
        text = _HTTPS.sub(https, text)
        for m in _ANY_ADLS.finditer(text):
            if _DYNAMIC.match(text, m.start()) or "{" in m.group(0):
                continue
            acc, cont = (m.group(2) or m.group(3) or "").lower(), (m.group(1) or m.group(4) or "").lower()
            if (acc, cont) in self.by_pair and not any(loc == f"{acc}/{cont}" and "query string" in why for loc, why in out.left):
                out.left.append((f"{acc}/{cont}", "written in a form that is not rewritten (a port, a fragment or an unusual separator after the host)"))
        for m in _DYNAMIC.finditer(text):
            out.left.append(("(built from variables)", "the container or account comes from a variable, so it is not rewritten"))
        for m in _BLOB.finditer(text):
            acc, cont = (m.group(2) or m.group(3) or "").lower(), (m.group(1) or m.group(4) or "").lower()
            out.left.append((f"{acc}/{cont}", "a Blob endpoint (wasbs:// or .blob.core.windows.net) is not rewritten; use the dfs endpoint"))
        out.text, out.routed_locations = text, sorted(seen)
        return out

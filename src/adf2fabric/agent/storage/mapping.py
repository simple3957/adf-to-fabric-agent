"""Where each ADF storage dataset lands in Fabric: OneLake shortcuts to the existing storage, and the Lakehouse path to use instead.

Read-only. Nothing is created and no data is moved: a shortcut is a pointer to the account that already holds the data.
The map says, per dataset, one of:

* ``mapped``         every part of the location is a literal; the Lakehouse path is exact.
* ``mapped_dynamic`` the container is literal but the folder or file is an expression; the Lakehouse prefix is exact, the rest is shown as authored.
* ``needs_decision`` the agent will not guess: dynamic container or account, an account it cannot read, a store a shortcut is not built for here.

Only ADLS Gen2 (``AzureBlobFSLocation``) is mapped to a shortcut, because that is the shortcut target this project has verified
(``deployment/shortcut_deploy.py``). Blob, S3 and the rest are listed with a reason, never silently skipped. Datasets that are not
files (SQL tables and so on) are counted and left out.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable, Optional

from adf2fabric.agent.storage import stores
from adf2fabric.project.discovery import AdfActivity, AdfDataset, AdfLinkedService, AdfSemanticDiscovery

_ADLS_URL = re.compile(r"^https://([a-z0-9]{3,24})\.dfs\.core\.windows\.net/?$", re.I)
_SHORTCUT_NAME = re.compile(r"^[A-Za-z0-9 _.\-]{1,128}$")
_FILE_LOCATIONS = stores.LOCATIONS


@dataclass
class Use:
    pipeline: str
    activity: str
    role: str  # input | output


@dataclass
class DatasetMap:
    dataset: str
    format: str
    linked_service: Optional[str]
    store: str  # adls_gen2 | blob | ...
    status: str  # mapped | mapped_dynamic | needs_decision
    account: Optional[str] = None
    container: Optional[str] = None
    folder: Optional[str] = None  # as authored: a literal or an expression text
    file: Optional[str] = None
    lakehouse_path: Optional[str] = None  # Files/<shortcut>/<folder>/<file>, with <dynamic: ...> where an expression stands
    dynamic: list[str] = field(default_factory=list)  # which of container/folder/file are expressions
    reason: str = ""
    action: str = ""
    alternatives: list[str] = field(default_factory=list)  # what Fabric offers for this store, best first (agent/storage/stores.py)
    used_by: list[Use] = field(default_factory=list)


@dataclass
class ShortcutPlan:
    shortcut: str
    account: str
    account_url: str
    container: str
    path: str  # where in the lakehouse: Files
    subpath: str  # /<container>
    linked_services: list[str]
    datasets: int
    lakehouse_path: str  # Files/<shortcut>
    connection: str  # what is needed: an existing Fabric connection to the account


@dataclass
class StorageMap:
    project_id: str
    lakehouse: Optional[str]
    datasets: list[DatasetMap]
    shortcuts: list[ShortcutPlan]
    not_file_datasets: int
    summary: dict[str, int]
    to_decide: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _text(v: Any) -> tuple[Optional[str], bool]:
    """(value, is_expression). An authored expression is an object {value, type: Expression} or a string starting with '@'."""
    if v is None:
        return None, False
    if isinstance(v, dict):
        if str(v.get("type")) == "Expression":
            return str(v.get("value", "")), True
        return None, False
    s = str(v)
    if s.startswith("@@"):  # ADF's escape for a literal leading '@'
        return s[1:], False
    return s, s.startswith("@") or "@{" in s


def _clean(path: Optional[str]) -> str:
    return "/".join(p for p in (path or "").replace("\\", "/").split("/") if p)


def _uses(disc: AdfSemanticDiscovery) -> dict[str, list[Use]]:
    found: dict[str, list[Use]] = {}

    def walk(pipeline: str, acts: Iterable[AdfActivity]) -> None:
        for a in acts:
            for role, refs in (("input", a.inputs), ("output", a.outputs)):
                for r in refs or []:
                    if getattr(r, "kind", None) == "dataset" and getattr(r, "name", None):
                        found.setdefault(r.name.lower(), []).append(Use(pipeline, a.name, role))
            walk(pipeline, a.activities or [])

    for p in disc.pipelines:
        walk(p.name, p.activities)
    return found


def _account(ls: Optional[AdfLinkedService]) -> tuple[Optional[str], Optional[str], str]:
    """(account name, account URL, problem). Only an explicit, literal ADLS Gen2 endpoint counts."""
    if ls is None:
        return None, None, "The dataset's linked service was not found in the source."
    if not ls.endpoint:
        return None, None, f"Linked service '{ls.name}' states no account URL (it may use a parameter or a connection string, which are never read)."
    m = _ADLS_URL.match(ls.endpoint.strip())
    if not m:
        return None, None, f"Linked service '{ls.name}' does not point at a literal https://<account>.dfs.core.windows.net URL."
    return m.group(1).lower(), f"https://{m.group(1).lower()}.dfs.core.windows.net", ""


def _map_dataset(ds: AdfDataset, ls: Optional[AdfLinkedService], used: list[Use]) -> Optional[DatasetMap]:
    loc = (ds.type_properties or {}).get("location")
    if not isinstance(loc, dict):
        return None  # not a file dataset
    kind = _FILE_LOCATIONS.get(str(loc.get("type")), "other")
    base = dict(dataset=ds.name, format=ds.type or "?", linked_service=ds.linked_service_ref.name if ds.linked_service_ref else None, store=kind, used_by=used)
    if kind != "adls_gen2":
        st = stores.STORES.get(kind)
        if st is None:
            return DatasetMap(**base, status="needs_decision", reason=f"Location type '{loc.get('type')}' is not a file store this tool knows, so it is not mapped.",
                              action="Decide where this data lives in Fabric, then map it by hand.")
        return DatasetMap(**base, status="needs_decision", reason=f"{st.summary()} This tool maps only ADLS Gen2 to a shortcut.",
                          action="Choose one of the alternatives, then map it by hand.", alternatives=[*st.alternatives, *([st.gateway] if st.gateway else [])])
    container, c_dyn = _text(loc.get("fileSystem"))
    if container and not c_dyn:
        container = container.strip().lower()  # Azure container names are lowercase and case-insensitive: 'Raw' and 'raw' are one container
    folder, f_dyn = _text(loc.get("folderPath"))
    file_, n_dyn = _text(loc.get("fileName"))
    account, url, problem = _account(ls)
    dm = DatasetMap(**base, status="mapped", account=account, container=container, folder=folder, file=file_)
    if problem:
        dm.status, dm.reason, dm.action = "needs_decision", problem, "State the account in the linked service (a literal ADLS Gen2 URL) or map this dataset by hand."
        return dm
    if not container or c_dyn:
        dm.status, dm.reason = "needs_decision", "The container is empty or an expression, so no shortcut can be named for it."
        dm.action = "Name the container(s) this expression can produce; each needs its own shortcut."
        return dm
    if not _SHORTCUT_NAME.match(container) or ".." in (folder or "") or ".." in container:
        dm.status, dm.reason, dm.action = "needs_decision", "The container or folder name cannot be used as a shortcut or path as written.", "Rename or map by hand."
        return dm
    dm.dynamic = [n for n, d in (("folder", f_dyn), ("file", n_dyn)) if d]
    parts = [f"Files/{container}"]
    parts.append(f"<dynamic: {folder}>" if f_dyn else _clean(folder))
    parts.append(f"<dynamic: {file_}>" if n_dyn else _clean(file_))
    dm.lakehouse_path = "/".join(p for p in parts if p)
    if dm.dynamic:
        dm.status = "mapped_dynamic"
        dm.reason = f"The {' and '.join(dm.dynamic)} is an expression: it is shown as authored and resolved when the pipeline runs."
        dm.action = "Check that every value the expression can produce exists under this container."
    return dm


class StorageMapError(ValueError):
    pass


def build_storage_map(disc: AdfSemanticDiscovery, lakehouse: Optional[str] = None,
                      names: Optional[dict[tuple[str, str], str]] = None) -> StorageMap:
    """``names``: (account, container) -> the shortcut name to use, e.g. one that already exists in the lakehouse."""
    names = {(a.lower(), c.lower()): n for (a, c), n in (names or {}).items()}
    for n in names.values():
        if not _SHORTCUT_NAME.match(n) or "'" in n or "/" in n:
            raise StorageMapError(f"'{n}' cannot be a shortcut name (letters, digits, space, _ . - only).")
    lss = {l.name.lower(): l for l in disc.linked_services}
    uses = _uses(disc)
    rows: list[DatasetMap] = []
    not_file = 0
    for ds in disc.datasets:
        ls = lss.get(ds.linked_service_ref.name.lower()) if ds.linked_service_ref else None
        row = _map_dataset(ds, ls, uses.get(ds.name.lower(), []))
        if row is None:
            not_file += 1
        else:
            rows.append(row)

    groups: dict[tuple[str, str], list[DatasetMap]] = {}
    for r in rows:
        if r.status != "needs_decision" and r.account and r.container:
            groups.setdefault((r.account, r.container), []).append(r)
    per_container: dict[str, int] = {}
    for _, container in groups:
        per_container[container] = per_container.get(container, 0) + 1
    shortcuts: list[ShortcutPlan] = []
    for (account, container), members in sorted(groups.items()):
        name = names.get((account.lower(), container.lower())) or (
            container if per_container[container] == 1 else f"{container}-{account}")  # the same container name in two accounts: both get the account suffix
        for m in members:
            if name != container and m.lakehouse_path:
                m.lakehouse_path = m.lakehouse_path.replace(f"Files/{container}", f"Files/{name}", 1)
        shortcuts.append(ShortcutPlan(shortcut=name, account=account, account_url=f"https://{account}.dfs.core.windows.net", container=container, path="Files",
                                      subpath=f"/{container}", linked_services=sorted({m.linked_service for m in members if m.linked_service}),
                                      datasets=len(members), lakehouse_path=f"Files/{name}",
                                      connection=f"An existing Fabric connection to {account} (from linked service {', '.join(sorted({m.linked_service for m in members if m.linked_service}))})."))
    taken: dict[str, str] = {}
    for sc in shortcuts:
        other = taken.setdefault(sc.shortcut.lower(), f"{sc.account}/{sc.container}")
        if other != f"{sc.account}/{sc.container}":
            raise StorageMapError(f"Two storage locations would share the shortcut name '{sc.shortcut}' ({other} and {sc.account}/{sc.container}).")
    unknown = sorted(f"{a}/{c}" for a, c in names if (a, c) not in groups)
    if unknown:
        raise StorageMapError(f"No mapped dataset uses {', '.join(unknown)}; check the account/container names.")
    to_decide = [{"dataset": r.dataset, "used_by": sorted({u.pipeline for u in r.used_by}), "reason": r.reason, "action": r.action, "alternatives": r.alternatives}
                 for r in rows if r.status == "needs_decision"]
    summary = {"file_datasets": len(rows), "mapped": sum(r.status == "mapped" for r in rows), "mapped_dynamic": sum(r.status == "mapped_dynamic" for r in rows),
               "needs_decision": len(to_decide), "shortcuts": len(shortcuts), "not_file_datasets": not_file}
    return StorageMap(disc.project_id, lakehouse, rows, shortcuts, not_file, summary, to_decide)


def render_markdown(m: StorageMap, workspace_hint: str = "<workspace-id>") -> str:
    s = m.summary
    out = [f"# Storage map: {m.project_id}", "",
           "Read-only. No data is moved and nothing is created: a shortcut points at the storage that already holds the data. "
           "A map row is a proposal from what the ADF source states; it is not verified until the shortcut exists and a run reads through it.", "",
           f"* File datasets: **{s['file_datasets']}** (mapped {s['mapped']}, mapped with an expression {s['mapped_dynamic']}, **need a decision {s['needs_decision']}**)",
           f"* Shortcuts to create: **{s['shortcuts']}**" + (f" in lakehouse `{m.lakehouse}`" if m.lakehouse else " (no lakehouse named yet: pass --lakehouse)"),
           f"* Datasets that are not files (SQL and so on, not part of this map): {s['not_file_datasets']}", ""]
    out += ["## Shortcuts", ""]
    if not m.shortcuts:
        out.append("None.")
    else:
        out += ["| Shortcut | Lakehouse path | Storage account | Container | Linked services | Datasets |", "|---|---|---|---|---|---|"]
        for sc in m.shortcuts:
            out.append(f"| {sc.shortcut} | `{sc.lakehouse_path}` | {sc.account} | {sc.container} | {', '.join(sc.linked_services)} | {sc.datasets} |")
        out += ["", "Each shortcut needs an existing Fabric connection to its account (never a key). Dry run first; add `--apply` only after you have read it:", "", "```"]
        for sc in m.shortcuts:
            out.append(f"adf2fabric create-shortcut --workspace-id {workspace_hint} --lakehouse {m.lakehouse or '<lakehouse>'} --name \"{sc.shortcut}\" "
                       f"--storage-account {sc.account_url} --subpath {sc.subpath} --connection-id <connection-id for {sc.account}>")
        out += ["```", ""]
    out += ["## For you to deal with", ""]
    if not m.to_decide:
        out.append("Nothing: every file dataset could be mapped.")
    else:
        out += ["| Dataset | Used by | Why it is not mapped | What to do |", "|---|---|---|---|"]
        out += [f"| {d['dataset']} | {', '.join(d['used_by']) or '-'} | {d['reason']} | {d['action']} |" for d in m.to_decide]
        out += ["", "### Alternatives, per dataset (from Microsoft's documentation, read " + stores.VERIFIED_ON + ")", ""]
        for d in m.to_decide:
            if d.get("alternatives"):
                out += [f"**{d['dataset']}**", ""] + [f"* {x}" for x in d["alternatives"]] + [""]
        out += ["Sources: " + "; ".join(f"<{u}>" for u in stores.SOURCES), ""]
    out += ["", "## Every dataset", "", "| Dataset | Status | Storage | Lakehouse path | Note |", "|---|---|---|---|---|"]
    for r in m.datasets:
        out.append(f"| {r.dataset} | {r.status} | {r.store} | {('`' + r.lakehouse_path + '`') if r.lakehouse_path else '-'} | {r.reason} |")
    out += ["", "## What is not done by this map", "",
            "* Pipelines read the Lakehouse only when you ask: `convert --lakehouse <name>` repoints ADLS Gen2 CSV and Parquet datasets (live-verified).",
            "* Data is never copied or deleted. Loading files into Lakehouse Tables (Delta) is a separate decision.", ""]
    return "\n".join(out)

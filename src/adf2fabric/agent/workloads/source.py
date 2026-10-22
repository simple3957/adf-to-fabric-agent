"""Retrieve a workload's source code, only as far as the caller is authorised, and say precisely what is missing otherwise.

Rules:

* Nothing is fetched unless a credential was explicitly offered (an environment variable NAME, or an ``az`` token for that
  provider's own audience when the caller allows it). Being signed in to ``az`` does not grant Databricks access, so a missing
  token is reported as ``missing_credential`` with the permission the call needs; nothing is guessed or tried.
* Code already in the project (a Synapse notebook definition from the imported repository, an earlier authorised export) is
  used without any call.
* A failed retrieval returns the permission CATEGORY and what to do. No token, header or URL query reaches a message.
* JAR files are compiled code: there is no source to read, so they are reported as ``binary``.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from adf2fabric.agent.dependencies.access import TokenSource, redact
from adf2fabric.agent.discovery.databricks_notebooks import DatabricksClient, DatabricksError, local_path, validate_host
from adf2fabric.project.workloads import CodeSource, CodeStatus as C

_ERROR_STATUS = {"not_found": C.not_found, "unauthorized": C.denied, "forbidden": C.denied, "unreachable": C.unreachable,
                 "rate_limited": C.unreachable, "too_large": C.error, "not_a_notebook": C.error, "not_a_file": C.error,
                 "bad_workspace_url": C.error, "no_token": C.missing_credential, "dependency_missing": C.error}
_EXT = (".py", ".scala", ".sql", ".r")


@dataclass
class Code:
    """What was retrieved: the text (None when nothing was) and its description for the manifest."""

    source: CodeSource
    text: Optional[str] = None
    filename: str = ""
    data: Any = None  # a parsed definition (Synapse notebook / Spark job definition JSON)


@dataclass
class SourceResolver:
    project_dir: Path  # projects/<id>
    tokens: TokenSource = field(default_factory=TokenSource)
    fetch: bool = False  # retrieval from the source platform is explicit
    local_files: dict[str, str] = field(default_factory=dict)  # source path (or its last segment) -> local file
    client_factory: Optional[Callable[[str, str], Any]] = None  # (host, token) -> client; tests inject a fake

    # -- helpers -----------------------------------------------------------------------------------------------------------
    def _describe(self, status: C, location: str, text: Optional[str] = None, *, language: Optional[str] = None, **kw: Any) -> CodeSource:
        raw = (text or "").encode("utf-8")
        return CodeSource(status=status, location=location, language=language, sha256=hashlib.sha256(raw).hexdigest() if text else None,
                          lines=text.count("\n") + 1 if text else 0, **kw)

    def _supplied(self, path: str) -> Optional[Path]:
        for key in (path, path.rstrip("/").rsplit("/", 1)[-1]):
            f = self.local_files.get(key)
            if f and Path(f).is_file():
                return Path(f)
        return None

    def _client(self, host: str, token: str) -> Any:
        return self.client_factory(host, token) if self.client_factory else DatabricksClient(host, token)

    @staticmethod
    def _failure(exc: DatabricksError, permission: str, location: str) -> Code:
        status = _ERROR_STATUS.get(exc.code, C.error)
        return Code(CodeSource(status=status, location=location, permission=permission if status in (C.denied, C.missing_credential, C.not_found) else None,
                               detail=redact(str(exc)), action=(exc.actions[0] if exc.actions else "")))

    # -- Databricks notebook ---------------------------------------------------------------------------------------------
    def databricks_notebook(self, host: Optional[str], path: Optional[str]) -> Code:
        perm = f"Databricks workspace: CAN READ on notebook '{path}' (workspace/export)"
        if not path or "@{" in path or path.startswith("@"):
            return Code(CodeSource(status=C.dynamic, location=path or "", detail="The notebook path is computed at run time.",
                                   action="List the notebooks this activity can run and migrate each."))
        if (f := self._supplied(path)) is not None:
            text = f.read_text(encoding="utf-8", errors="replace")
            return Code(self._describe(C.supplied, path, text), text, f.name)
        cached = self._cached(path)
        if cached is not None:
            return Code(self._describe(C.cached, path, cached[0]), cached[0], cached[1])
        if not self.fetch:
            return Code(CodeSource(status=C.not_requested, location=path, permission=perm,
                                   detail="Retrieval was not requested (it needs a Databricks credential).",
                                   action="Run again with a Databricks token (--databricks-token-env NAME), or pass the exported notebook with --source."))
        if not host:
            return Code(CodeSource(status=C.error, location=path, detail="The workspace host is not known from the linked service.",
                                   action="Add the Databricks linked service (domain) to the source."))
        token = self.tokens("databricks")
        if not token:
            return Code(CodeSource(status=C.missing_credential, location=path, permission=perm,
                                   detail="No Databricks token is available. An az login does not grant Databricks access.",
                                   action="Set the environment variable that holds a Databricks token and pass its NAME."))
        try:
            validate_host(f"https://{host}")
            raw, language = self._client(host, token).export_source(path)
        except DatabricksError as exc:
            return self._failure(exc, perm, path)
        text = raw.decode("utf-8", errors="replace")
        target = local_path(self.project_dir / "databricks" / "notebooks", path, language)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
        return Code(self._describe(C.retrieved, path, text, language=language.lower() or None), text, target.name)

    def _cached(self, path: str) -> Optional[tuple[str, str]]:
        base = self.project_dir / "databricks" / "notebooks"
        for ext in _EXT:
            target = local_path(base, path, ext.lstrip(".").upper() if ext != ".py" else "PYTHON")
            if target.is_file():
                return target.read_text(encoding="utf-8", errors="replace"), target.name
        return None

    # -- Databricks Python file / JAR ----------------------------------------------------------------------------------------
    def databricks_python(self, host: Optional[str], location: Optional[str]) -> Code:
        if not location or "@{" in location or location.startswith("@"):
            return Code(CodeSource(status=C.dynamic, location=location or "", detail="The file location is computed at run time."))
        if (f := self._supplied(location)) is not None:
            text = f.read_text(encoding="utf-8", errors="replace")
            return Code(self._describe(C.supplied, location, text, language="python"), text, f.name)
        if not location.startswith("dbfs:/"):
            kind = "a cloud storage URI" if "://" in location else "a path this tool cannot read"
            perm = "Storage: a data-plane read role (for example Storage Blob Data Reader) on the container" if "://" in location else None
            return Code(CodeSource(status=C.unsupported_location, location=location, permission=perm,
                                   detail=f"The script is at {kind}; only dbfs:/ files can be read here.",
                                   action="Download the file and pass it with --source."))
        perm = f"Databricks DBFS: read access to '{location}' (dbfs/read)"
        cache = self.project_dir / "databricks" / "files" / location[len("dbfs:/"):]
        if cache.is_file():
            text = cache.read_text(encoding="utf-8", errors="replace")
            return Code(self._describe(C.cached, location, text, language="python"), text, cache.name)
        if not self.fetch:
            return Code(CodeSource(status=C.not_requested, location=location, permission=perm,
                                   detail="Retrieval was not requested (it needs a Databricks credential).",
                                   action="Run again with a Databricks token, or pass the file with --source."))
        token = self.tokens("databricks")
        if not host or not token:
            return Code(CodeSource(status=C.missing_credential if host else C.error, location=location, permission=perm,
                                   detail="No Databricks token is available." if host else "The workspace host is not known.",
                                   action="Set the environment variable that holds a Databricks token and pass its NAME."))
        try:
            raw = self._client(host, token).read_dbfs(location)
        except DatabricksError as exc:
            return self._failure(exc, perm, location)
        text = raw.decode("utf-8", errors="replace")
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_bytes(raw)
        return Code(self._describe(C.retrieved, location, text, language="python"), text, cache.name)

    def jar(self, location: Optional[str]) -> Code:
        return Code(CodeSource(status=C.binary, location=location or "", detail="A JAR is compiled code: there is no source to read, so it "
                               "cannot be scanned for Fabric compatibility.",
                               action="Upload the JAR to OneLake/ADLS and pass its URI (--artifact NAME=URI)."))

    # -- Synapse -------------------------------------------------------------------------------------------------------------------
    def synapse_artifact(self, folder: str, name: Optional[str]) -> Code:
        """A notebook or Spark job definition of the imported Synapse repository (``notebook/<name>.json``)."""
        perm = f"Microsoft.Synapse/workspaces/{'notebooks' if folder == 'notebook' else 'sparkjobdefinitions'}/read"
        if not name:
            return Code(CodeSource(status=C.error, location="", detail="The activity does not name the item."))
        found = None
        src = self.project_dir / "source"
        for candidate in (src / folder / f"{name}.json", *(src.glob(f"**/{folder}/{name}.json") if src.is_dir() else [])):
            if candidate.is_file():
                found = candidate
                break
        if found is None:
            return Code(CodeSource(status=C.not_found, location=f"{folder}/{name}.json", permission=perm,
                                   detail=f"'{name}' is not in the imported source.",
                                   action="Import the Synapse workspace repository (or export) that holds it, then run again."))
        text = found.read_text(encoding="utf-8", errors="replace")
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            return Code(CodeSource(status=C.error, location=f"{folder}/{name}.json", detail=f"Not valid JSON ({exc.msg}, line {exc.lineno}).",
                                   action="Fix or re-export the file."))
        return Code(self._describe(C.in_project, f"{folder}/{name}.json", text), text, found.name, data)

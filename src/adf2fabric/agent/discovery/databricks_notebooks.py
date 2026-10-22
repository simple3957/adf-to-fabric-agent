"""Retrieve the Databricks notebooks an ADF pipeline runs, and say what Fabric will need changed in them.

ADF's Databricks notebook activity only holds a ``notebookPath``; the code lives in the Databricks workspace. This module
exports that code over the workspace REST API (verified against docs.databricks.com, 2026-10-16):

* ``GET /api/2.0/workspace/get-status?path=``  -> ``object_type`` (NOTEBOOK, DIRECTORY, ...) and ``language``
* ``GET /api/2.0/workspace/export?path=&format=SOURCE`` -> ``{content (base64), file_type}``; 10 MB limit
  (``MAX_NOTEBOOK_SIZE_EXCEEDED``); ``RESOURCE_DOES_NOT_EXIST`` when the path is wrong.

Rules this module keeps:

* Read-only. Nothing in the Databricks workspace is created or changed.
* The token is supplied by the caller (from an environment variable the caller names) and is only ever sent to the one
  https host given. It never appears in a result, an error, or a file.
* Notebooks are written under the workspace path they came from (``/Shared/etl/clean`` becomes
  ``<out>/Shared/etl/clean.py``) so the structure is the one people already know.
* Every failure is a :class:`DatabricksError` with a stable ``code`` and what to do about it.

The compatibility scan is textual and says only what it saw (a ``dbutils`` call, a ``/dbfs`` path ...); it does not rewrite
anything and it is not a guarantee that code without findings runs on Fabric.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Optional
from urllib.parse import quote, urlsplit

_LANG_EXT = {"PYTHON": ".py", "SCALA": ".scala", "SQL": ".sql", "R": ".r"}
_MAX_BYTES = 10 * 1024 * 1024
_BAD_CHARS = re.compile(r'[<>:"|?*\\\x00-\x1f]')


class DatabricksError(Exception):
    """A retrieval problem. ``code`` is stable; ``actions`` say what to do; the message never holds a token."""

    def __init__(self, code: str, message: str, actions: Optional[list[str]] = None, retriable: bool = False):
        super().__init__(message)
        self.code, self.actions, self.retriable = code, actions or [], retriable

    def as_dict(self) -> dict:
        return {"code": self.code, "message": str(self), "actions": self.actions, "retriable": self.retriable}


# --- the ADF linked service --------------------------------------------------------------------------------------


@dataclass
class LinkedServiceInfo:
    name: str
    workspace_url: Optional[str]
    authentication: str  # access_token | managed_identity | key_vault_token | unknown
    cluster: str  # existing_cluster | new_job_cluster | instance_pool | unspecified
    existing_cluster_id: Optional[str] = None
    notes: list[str] = field(default_factory=list)


def inspect_linked_service(data: dict) -> LinkedServiceInfo:
    """What an ADF ``AzureDatabricks`` linked service says about the workspace, its authentication and its cluster.

    Secret values are never read into the result: an access token is reported only as present or as a Key Vault
    reference.
    """
    props = data.get("properties") if isinstance(data.get("properties"), dict) else data
    tp = props.get("typeProperties") if isinstance(props.get("typeProperties"), dict) else {}
    name = str(data.get("name") or props.get("name") or "")
    notes: list[str] = []
    if str(props.get("type", "")).lower() not in ("azuredatabricks", "azuredatabricksdeltalake"):
        notes.append(f"The linked service type is '{props.get('type')}', not AzureDatabricks.")
    domain = tp.get("domain")
    url = domain.strip().rstrip("/") if isinstance(domain, str) and domain.strip() else None
    if url and not url.lower().startswith("https://"):
        url = "https://" + url.split("://", 1)[-1]
    token = tp.get("accessToken")
    auth = "unknown"
    if isinstance(token, dict):
        auth = "key_vault_token" if str(token.get("type", "")).lower() == "azurekeyvaultsecret" else "access_token"
    elif str(tp.get("authentication", "")).upper() == "MSI":
        auth = "managed_identity"
    if auth == "access_token":
        notes.append("ADF stores the token encrypted and does not return it, so it cannot be reused here; supply a token.")
    elif auth == "managed_identity":
        notes.append("ADF authenticates with its managed identity; supply a token for an identity that can read the workspace.")
    elif auth == "key_vault_token":
        notes.append("The token is a Key Vault secret; read it there and supply it here.")
    existing = tp.get("existingClusterId") if isinstance(tp.get("existingClusterId"), str) else None
    cluster = ("existing_cluster" if existing else "instance_pool" if tp.get("instancePoolId")
               else "new_job_cluster" if tp.get("newClusterVersion") or tp.get("newClusterNodeType") else "unspecified")
    return LinkedServiceInfo(name, url, auth, cluster, existing, notes)


def notebook_paths_in_pipelines(pipelines: list[Any]) -> tuple[list[tuple[str, str, str]], list[str]]:
    """``(pipeline, activity, notebookPath)`` for each Databricks notebook activity, plus activities skipped.

    A path that is an expression cannot be resolved without running the pipeline, so it is reported, not guessed.
    """
    found: list[tuple[str, str, str]] = []
    skipped: list[str] = []

    def walk(pipeline: str, acts: list[Any]) -> None:
        for a in acts:
            if getattr(a, "type", None) == "DatabricksNotebook":
                raw = (getattr(a, "raw_properties", None) or {}).get("notebookPath")
                if isinstance(raw, str) and raw.startswith("/") and "@{" not in raw:
                    found.append((pipeline, a.name, raw))
                else:
                    skipped.append(f"{pipeline}/{a.name}: the notebook path is {'an expression' if raw else 'missing'}")
            walk(pipeline, getattr(a, "activities", None) or [])

    for p in pipelines:
        walk(p.name, getattr(p, "activities", None) or [])
    return found, skipped


# --- the workspace API -------------------------------------------------------------------------------------------


def validate_host(url: str) -> str:
    parts = urlsplit((url or "").strip().rstrip("/"))
    if (parts.scheme != "https" or not parts.hostname or parts.path not in ("", "/") or parts.query
            or parts.username or parts.password):
        raise DatabricksError("bad_workspace_url", "The workspace URL must look like https://adb-123.4.azuredatabricks.net "
                              "(https, no path or credentials).", ["Copy the 'domain' value from the linked service."])
    return f"https://{parts.netloc.lower()}"


class DatabricksClient:
    def __init__(self, workspace_url: str, token: str, session: Any = None,
                 sleep: Callable[[float], None] = time.sleep):
        self.host = validate_host(workspace_url)
        if not token or not token.strip():
            raise DatabricksError("no_token", "No Databricks token was supplied.",
                                  ["Set the environment variable you named to a personal access token or an Entra token."])
        if session is None:
            try:
                import requests
            except ImportError as exc:
                raise DatabricksError("dependency_missing", "The 'requests' package is not installed.",
                                      ["Install the 'connectivity' extra."]) from exc
            session = requests.Session()
        self._s, self._sleep = session, sleep
        self._h = {"Authorization": f"Bearer {token.strip()}"}

    def _get(self, path: str, params: dict[str, str]) -> Any:
        url = f"{self.host}{path}?" + "&".join(f"{k}={quote(v, safe='')}" for k, v in params.items())
        for _ in range(5):
            try:
                resp = self._s.request("GET", url, headers=self._h, timeout=60)
            except Exception as exc:  # noqa: BLE001 - no token in the message
                raise DatabricksError("unreachable", f"Could not reach the workspace ({type(exc).__name__}).",
                                      ["Check the workspace URL and the network."], retriable=True) from None
            if resp.status_code == 429:
                self._sleep(float(resp.headers.get("Retry-After", 5)))
                continue
            return resp
        raise DatabricksError("rate_limited", "The workspace kept rate-limiting the request (HTTP 429).",
                              ["Retry later."], retriable=True)

    @staticmethod
    def _fail(resp: Any, path: str) -> DatabricksError:
        try:
            body = resp.json()
        except Exception:  # noqa: BLE001
            body = {}
        code, message = str(body.get("error_code", "")), str(body.get("message", ""))[:300]
        status = resp.status_code
        if code == "RESOURCE_DOES_NOT_EXIST" or status == 404:
            return DatabricksError("not_found", f"No notebook at '{path}'.",
                                   ["Check the notebookPath in the ADF activity.", "A path under /Repos or /Users may need "
                                    "the exact folder name."])
        if code == "MAX_NOTEBOOK_SIZE_EXCEEDED":
            return DatabricksError("too_large", f"'{path}' is larger than the 10 MB export limit.",
                                   ["Split the notebook or export it another way."])
        if status == 401:
            return DatabricksError("unauthorized", "The workspace rejected the token (HTTP 401).",
                                   ["The token is invalid or expired.", "An Entra token must be for the Azure Databricks resource."])
        if status == 403:
            return DatabricksError("forbidden", f"The token is not allowed to read '{path}' (HTTP 403).",
                                   ["Give the identity read access to the notebook's folder."])
        return DatabricksError("api_error", f"The workspace returned HTTP {status} {code}: {message}".strip(),
                               retriable=status >= 500)

    def get_status(self, path: str) -> dict:
        resp = self._get("/api/2.0/workspace/get-status", {"path": path})
        if resp.status_code != 200:
            raise self._fail(resp, path)
        return resp.json()

    def export_source(self, path: str) -> tuple[bytes, str]:
        """``(source bytes, language)`` of one notebook. Anything that is not a notebook is refused."""
        info = self.get_status(path)
        kind = info.get("object_type")
        if kind != "NOTEBOOK":
            raise DatabricksError("not_a_notebook", f"'{path}' is a {kind or 'unknown object'}, not a notebook.",
                                  ["Point the activity at a notebook, not a folder or file."])
        language = str(info.get("language") or "").upper()
        resp = self._get("/api/2.0/workspace/export", {"path": path, "format": "SOURCE"})
        if resp.status_code != 200:
            raise self._fail(resp, path)
        try:
            raw = base64.b64decode(resp.json().get("content", ""), validate=True)
        except Exception:  # noqa: BLE001
            raise DatabricksError("bad_response", "The export returned content that is not valid base64.") from None
        if len(raw) > _MAX_BYTES:
            raise DatabricksError("too_large", f"'{path}' is larger than the 10 MB export limit.")
        return raw, language


    def read_dbfs(self, path: str, max_bytes: int = 5 * 1024 * 1024) -> bytes:
        """The bytes of one DBFS file (``dbfs:/...``), read in the API's 1 MB pieces. Read-only; refused above ``max_bytes``."""
        dbfs = path[len("dbfs:"):] if path.startswith("dbfs:") else path
        if not dbfs.startswith("/"):
            raise DatabricksError("bad_path", "A DBFS path must look like dbfs:/folder/file.")
        info_resp = self._get("/api/2.0/dbfs/get-status", {"path": dbfs})
        if info_resp.status_code != 200:
            raise self._fail(info_resp, path)
        info = info_resp.json()
        if info.get("is_dir"):
            raise DatabricksError("not_a_file", f"'{path}' is a folder, not a file.")
        size = int(info.get("file_size") or 0)
        if size > max_bytes:
            raise DatabricksError("too_large", f"'{path}' is {size} bytes, above the {max_bytes}-byte limit for reading code.")
        chunks: list[bytes] = []
        offset = 0
        while offset < size:
            resp = self._get("/api/2.0/dbfs/read", {"path": dbfs, "offset": str(offset), "length": str(min(1024 * 1024, size - offset))})
            if resp.status_code != 200:
                raise self._fail(resp, path)
            body = resp.json()
            try:
                piece = base64.b64decode(body.get("data", ""), validate=True)
            except Exception:  # noqa: BLE001
                raise DatabricksError("bad_response", "The DBFS read returned content that is not valid base64.") from None
            if not piece:
                break
            chunks.append(piece)
            offset += len(piece)
        return b"".join(chunks)


# --- writing files and scanning them -----------------------------------------------------------------------------


def local_path(out_dir: Path, workspace_path: str, language: str) -> Path:
    """Where a notebook is written: its workspace path under ``out_dir``, with the language's extension."""
    pure = PurePosixPath(workspace_path)
    parts = [p for p in pure.parts if p not in ("/", "")]
    if not parts or any(p in (".", "..") or _BAD_CHARS.search(p) for p in parts):
        raise DatabricksError("unsafe_path", f"The notebook path '{workspace_path}' cannot be mapped to a local file.",
                              ["Fetch it by another name or fix the path."])
    target = Path(out_dir).joinpath(*parts)
    ext = _LANG_EXT.get(language.upper(), ".txt")
    return target.with_name(target.name + ext) if not target.name.lower().endswith(ext) else target


@dataclass
class Finding:
    code: str
    line: int
    text: str  # what was seen, trimmed
    why: str
    severity: str  # rewrite | review


_RULES: list[tuple[str, re.Pattern, str, str]] = [
    ("dbutils_secrets", re.compile(r"\bdbutils\.secrets\b"), "rewrite",
     "Databricks secret scopes do not exist in Fabric; read the secret from Azure Key Vault another way."),
    ("dbutils_widgets", re.compile(r"\bdbutils\.widgets\b"), "rewrite",
     "Databricks widgets do not exist in Fabric; pass values as notebook/pipeline parameters."),
    ("dbutils_notebook", re.compile(r"\bdbutils\.notebook\.(run|exit)\b"), "rewrite",
     "dbutils.notebook.run/exit are Databricks calls; Fabric notebooks have their own notebook utilities."),
    ("dbutils_fs", re.compile(r"\bdbutils\.fs\b"), "rewrite",
     "dbutils.fs is Databricks-specific and points at DBFS; Fabric notebooks use their own file utilities and OneLake paths."),
    ("dbutils_other", re.compile(r"\bdbutils\.(?!secrets|widgets|notebook|fs)\w+"), "rewrite",
     "dbutils is Databricks-specific and has no direct name in Fabric."),
    ("dbfs_path", re.compile(r"(dbfs:/|/dbfs/|\"/FileStore/|'/FileStore/)"), "rewrite",
     "DBFS paths do not exist in Fabric; data must be reachable through OneLake or an ADLS path."),
    ("mount_point", re.compile(r"[\"'](/mnt/[^\"']*)"), "rewrite",
     "A Databricks mount point; recreate access as a OneLake shortcut or a direct ADLS path."),
    ("databricks_conf", re.compile(r"spark\.(conf\.set|conf\.get)?\(?[\"']?spark\.databricks\.[\w.]+"), "review",
     "A Databricks-only Spark setting; it has no effect outside Databricks."),
    ("adls_direct_path", re.compile(r"abfss?://[^\s\"']+@[\w-]+\.dfs\.core\.windows\.net"), "review",
     "A direct ADLS path. Databricks reached it through its own credential (a Unity Catalog external location, a secret or a "
     "cluster setting); Fabric uses the identity that runs the notebook, so that identity needs access to the account, or the "
     "data should be read through a OneLake shortcut. Verified live: a Fabric run of such a notebook failed with HTTP 401."),
    ("magic_run", re.compile(r"^\s*(#\s*)?(MAGIC\s+)?%run\b", re.M), "review",
     "%run chains another notebook by its Databricks path; that notebook must exist in Fabric with a path to match."),
    ("unity_catalog", re.compile(r"\b(?:USE|FROM|INTO|TABLE)\s+CATALOG\b|\bcatalog\.schema\.table\b", re.I), "review",
     "Unity Catalog objects are referenced; Fabric uses lakehouse schemas, so names need mapping."),
]


def analyze_source(text: str) -> list[Finding]:
    """Databricks-specific constructs found in notebook source, with the line they were found on."""
    out: list[Finding] = []
    lines = text.splitlines()
    for code, pattern, severity, why in _RULES:
        for i, line in enumerate(lines, 1):
            if pattern.search(line):
                out.append(Finding(code, i, line.strip()[:160], why, severity))
    return sorted(out, key=lambda f: (f.line, f.code))


@dataclass
class FetchResult:
    workspace_path: str
    status: str  # ok | error
    local_file: Optional[str] = None
    language: Optional[str] = None
    sha256: Optional[str] = None
    findings: list[Finding] = field(default_factory=list)
    error: Optional[dict] = None
    used_by: list[str] = field(default_factory=list)  # "pipeline/activity"


def fetch_notebooks(client: DatabricksClient, paths: list[str], out_dir: Path,
                    used_by: Optional[dict[str, list[str]]] = None) -> list[FetchResult]:
    """Export each notebook under ``out_dir``. One failure never stops the others; each is reported."""
    results: list[FetchResult] = []
    for path in sorted(set(paths)):
        res = FetchResult(path, "ok", used_by=sorted((used_by or {}).get(path, [])))
        try:
            raw, language = client.export_source(path)
            target = local_path(Path(out_dir), path, language)
            resolved = target.resolve()
            if Path(out_dir).resolve() not in resolved.parents:
                raise DatabricksError("unsafe_path", f"'{path}' would be written outside the output folder.")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(raw)
            res.local_file = target.relative_to(out_dir).as_posix()
            res.language = language or None
            res.sha256 = hashlib.sha256(raw).hexdigest()
            res.findings = analyze_source(raw.decode("utf-8", errors="replace"))
        except DatabricksError as exc:
            res.status, res.error = "error", exc.as_dict()
        results.append(res)
    return results


def results_to_json(results: list[FetchResult], workspace_url: str) -> str:
    return json.dumps({
        "workspace": workspace_url,
        "notebooks": [{**{k: v for k, v in r.__dict__.items() if k != "findings"},
                       "findings": [f.__dict__ for f in r.findings]} for r in results],
    }, indent=2)

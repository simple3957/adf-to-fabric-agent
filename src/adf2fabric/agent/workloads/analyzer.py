"""Static analysis of Spark notebook / script source for a move to Fabric.

Input: cells (``notebook_format``) and which platform the code ran on. Output: findings (a construct, a line, a severity, what
to do), the libraries the code needs, the parameters it declares, and what it exits with. Nothing is executed.

Every claim about Fabric here is one Microsoft documents (the pages are listed in ``docs/workload-migration.md``); where
the documentation is silent the finding says "review", never "supported". A finding never stores the source line, because
a line can hold a literal credential: it stores the construct's name and the line number.

Python cells are parsed with ``ast`` (precise: imports, calls, string constants, try-blocks). A cell that does not parse
falls back to line patterns and is itself reported, so a syntax problem cannot hide the other findings.
"""

from __future__ import annotations

import ast
import re
import sys
from dataclasses import dataclass, field
from typing import Iterable, Optional
from urllib.parse import urlparse

from adf2fabric.agent.workloads.notebook_format import Cell
from adf2fabric.project.workloads import Finding, FindingCategory as K, Severity as S

STDLIB = getattr(sys, "stdlib_module_names", frozenset())
_PYSPARK_ROOTS = {"pyspark", "py4j", "delta", "mlflow", "pandas", "numpy", "pyarrow"}  # named, but presence is NOT assumed
_DATABRICKS_ONLY_ROOTS = {"dbruntime", "dbutils", "databricks_cli", "dlt"}
_IDENT = re.compile(r"^[A-Za-z_]\w*$")

# --- string-literal scanning (all languages) -------------------------------------------------------------------------
_RE_ABFSS = re.compile(r"abfss?://([\w.-]+)@([\w-]+)\.dfs\.core\.windows\.net", re.I)
_RE_WASB = re.compile(r"wasbs?://([\w.-]+)@([\w-]+)\.blob\.core\.windows\.net", re.I)
_RE_ADL = re.compile(r"\badl://([\w-]+)\.azuredatalakestore\.net", re.I)
_RE_S3 = re.compile(r"\bs3[an]?://([\w.-]+)", re.I)
_RE_GS = re.compile(r"\bgs://([\w.-]+)", re.I)
_RE_DBFS = re.compile(r"(?:\bdbfs:/|(?<![\w:])/dbfs/|(?<![\w/])/FileStore/)")
_RE_MNT = re.compile(r"(?<![\w/.:-])/mnt/[^\s\"']*")
_RE_VOLUME = re.compile(r"(?<![\w/.:-])/Volumes/[^\s\"']+")
_RE_JDBC = re.compile(r"\bjdbc:([a-z0-9]+)://([^\s;:/\"'?]+)(?::(\d+))?", re.I)
_RE_URL = re.compile(r"\bhttps?://([A-Za-z0-9.-]+)", re.I)
_RE_URL_CRED = re.compile(r"\b(?:password|pwd|accountkey|sharedaccesssignature|sig)\s*=", re.I)
_RE_CRED_NAME = re.compile(r"(?i)(password|passwd|pwd|secret|token|apikey|api_key|accountkey|account_key|sas)")
_RE_STORAGE_KEY_CONF = re.compile(r"fs\.azure\.(?:account\.key|sas\.|account\.oauth)", re.I)

_RE_PIP = re.compile(r"^\s*%?(?:pip|conda)\s+install\s+(.+)$", re.I)
_RE_RUN = re.compile(r"^\s*%run\s+(?:-\w+\s+)?(\S+)", re.I)

# --- documented Fabric names ---------------------------------------------------------------------------------------------
FABRIC_FS = {"ls", "mkdirs", "cp", "mv", "rm", "put", "head", "append"}
SYNAPSE_UTILS_OK = ("fs", "notebook", "runtime")  # mssparkutils.<x> -> notebookutils.<x> (documented, names unchanged)


@dataclass
class Analysis:
    findings: list[Finding] = field(default_factory=list)
    pip_requirements: list[str] = field(default_factory=list)  # from %pip / %conda install
    imports: list[str] = field(default_factory=list)  # distinct top-level module names, sorted
    widgets: dict[str, Optional[str]] = field(default_factory=dict)  # name -> default (None = declared without one)
    widgets_used: list[str] = field(default_factory=list)
    run_targets: list[str] = field(default_factory=list)  # %run
    notebook_run_targets: list[str] = field(default_factory=list)  # dbutils/mssparkutils.notebook.run
    exits: bool = False
    exit_in_try: bool = False
    hosts: dict[str, str] = field(default_factory=dict)  # host -> why it is contacted (jdbc/http); no credentials
    accounts: list[str] = field(default_factory=list)  # storage account names reached directly
    syntax_errors: list[int] = field(default_factory=list)  # absolute start lines of cells that did not parse
    languages: list[str] = field(default_factory=list)
    assigned_names: set[str] = field(default_factory=set)  # top-level Python names assigned (collision check)
    spark_conf_keys: list[str] = field(default_factory=list)


class _Collector:
    def __init__(self) -> None:
        self.a = Analysis()
        self._seen: set[tuple] = set()

    def add(self, code: str, cat: K, sev: S, message: str, line: Optional[int], construct: str = "", action: str = "") -> None:
        key = (code, line, construct)
        if key in self._seen:
            return
        self._seen.add(key)
        self.a.findings.append(Finding(code=code, category=cat, severity=sev, message=message, line=line,
                                       construct=construct[:120], action=action))


# =====================================================================================================================
# literals and generic patterns (every language)
# =====================================================================================================================


def _scan_text(c: _Collector, text: str, base_line: int, platform: str) -> None:
    """Line-oriented scan used for non-Python cells, Python fallbacks and for string constants. Skips full-line comments."""
    for off, raw in enumerate(text.split("\n")):
        line = raw.strip()
        if not line or line.startswith(("#", "//", "--")) and not line.lower().startswith(("# magic",)):
            continue
        n = base_line + off
        for m in _RE_ABFSS.finditer(raw):
            c.a.accounts.append(m.group(2).lower())
            c.add("adls_direct_path", K.data_access, S.review,
                  "A direct ADLS Gen2 path. The source reached it with its own credential; in Fabric the identity that runs "
                  "the item needs access to the account, or the data should be read through a OneLake shortcut.", n,
                  f"abfss://{m.group(1)}@{m.group(2)}", "Grant the running identity data access, or create a OneLake shortcut.")
        for m in _RE_WASB.finditer(raw):
            c.a.accounts.append(m.group(2).lower())
            c.add("blob_direct_path", K.data_access, S.review, "A direct Blob Storage path; the running identity needs access "
                  "(or use a shortcut).", n, f"wasb://{m.group(1)}@{m.group(2)}", "Grant access or create a shortcut.")
        if _RE_ADL.search(raw):
            c.add("adls_gen1_path", K.data_access, S.alternative, "Azure Data Lake Storage Gen1 is retired; the data must move to "
                  "ADLS Gen2 before Fabric can read it.", n, "adl://", "Migrate the data to ADLS Gen2.")
        if _RE_S3.search(raw):
            c.add("s3_path", K.data_access, S.review, "An Amazon S3 path; Fabric reaches S3 through a OneLake shortcut or "
                  "connection, not through the Databricks cluster's keys.", n, "s3://", "Create an S3 shortcut or supply access.")
        if _RE_GS.search(raw):
            c.add("gcs_path", K.data_access, S.review, "A Google Cloud Storage path; reach it through a shortcut or connection.", n, "gs://")
        if _RE_DBFS.search(raw):
            c.add("dbfs_path", K.filesystem, S.change, "DBFS paths do not exist in Fabric; the data must be reachable through "
                  "OneLake or an ADLS path.", n, "dbfs:/", "Move the files to OneLake/ADLS and change the path.")
        for m in _RE_MNT.finditer(raw):
            c.add("mount_point", K.filesystem, S.change, "A Databricks mount point. Recreate access as a OneLake shortcut, or "
                  "mount with notebookutils.fs.mount (session-level).", n, "/mnt/", "Create a shortcut for the mounted location.")
        for m in _RE_VOLUME.finditer(raw):
            c.add("uc_volume", K.filesystem, S.change, "A Unity Catalog volume path; Fabric has no volumes. Use a lakehouse Files "
                  "path or a shortcut.", n, "/Volumes/", "Copy or shortcut the volume's data into a lakehouse.")
        for m in _RE_JDBC.finditer(raw):
            host = m.group(2).lower()
            c.a.hosts.setdefault(host, f"jdbc:{m.group(1).lower()}")
            c.add("jdbc_target", K.networking, S.review, f"A JDBC connection to {host}: it must be reachable from Fabric "
                  "(public endpoint, private endpoint or a gateway).", n, f"jdbc:{m.group(1).lower()}://{host}")
            if _RE_URL_CRED.search(raw):
                c.add("credential_in_url", K.authentication, S.change, "A credential appears to be written into the connection "
                      "string in the code.", n, "jdbc credential", "Move it to Key Vault and read it with notebookutils.credentials.getSecret.")
        for m in _RE_URL.finditer(raw):
            host = m.group(1).lower()
            if host not in c.a.hosts and not host.endswith(("microsoft.com", "databricks.com", "apache.org", "github.com")):
                c.a.hosts[host] = "http"
                c.add("http_target", K.networking, S.info, f"The code calls {host}: it must be reachable from Fabric.", n, host)
        if _RE_STORAGE_KEY_CONF.search(raw):
            c.add("storage_credential_config", K.authentication, S.change, "Storage credentials are set in code (an account key, "
                  "SAS token or OAuth setting). Fabric runs as the executing identity; replace this with identity access, a "
                  "shortcut, or a key read from Key Vault.", n, "fs.azure credential setting",
                  "Remove the setting and grant the running identity access.")
        m = re.match(r"""^\s*([A-Za-z_]\w*)\s*=\s*["'][^"']{12,}["']\s*$""", raw)
        if m and _RE_CRED_NAME.search(m.group(1)):
            c.add("hardcoded_credential", K.authentication, S.change, "A variable named like a credential is assigned a long "
                  "literal. The value is not recorded.", n, m.group(1), "Read it from Key Vault instead of the source.")
        if re.search(r"\bCREATE\s+(?:OR\s+REFRESH\s+)?(?:LIVE|STREAMING)\s+TABLE\b|\bCREATE\s+LIVE\s+VIEW\b", raw, re.I):
            c.add("delta_live_tables", K.api, S.alternative, "Delta Live Tables (declarative pipelines) have no Fabric equivalent.",
                  n, "CREATE LIVE/STREAMING TABLE", "Rewrite as Spark notebooks with Structured Streaming / Delta merges.")
        if re.search(r"""cloudFiles""", raw):
            c.add("auto_loader", K.api, S.alternative, "Databricks Auto Loader (cloudFiles) is not available in Fabric.", n, "cloudFiles",
                  "Use Structured Streaming's file source with a checkpoint, or a pipeline trigger on new files.")
        if re.search(r"\bCOPY\s+INTO\b", raw, re.I) and platform == "databricks":
            c.add("copy_into", K.api, S.review, "COPY INTO is a Databricks SQL command; verify it exists for your Fabric runtime.", n, "COPY INTO")
        if re.search(r"\b(?:USE|FROM|INTO|TABLE|JOIN)\s+CATALOG\b|\b[A-Za-z_]\w*\.[A-Za-z_]\w*\.[A-Za-z_]\w*\b(?=\s*(?:\(|where|join|limit|$))",
                     raw, re.I) and re.search(r"(?i)\bcatalog\b|\bselect\b|\bfrom\b", raw):
            if re.search(r"(?i)\buse\s+catalog\b|\bcatalog\b\s*[=:]", raw):
                c.add("unity_catalog", K.data_access, S.review, "Unity Catalog objects are referenced; Fabric uses lakehouse "
                      "schemas, so names need mapping.", n, "catalog", "Map each catalog.schema.table to a lakehouse table.")
        if re.search(r"\bsaveAsTable\s*\(|\bspark\.table\s*\(|\bCREATE\s+(?:OR\s+REPLACE\s+)?TABLE\b|\bINSERT\s+(?:INTO|OVERWRITE)\b", raw, re.I):
            c.add("metastore_tables", K.data_access, S.review, "The code reads or writes metastore tables. In Fabric they resolve "
                  "against the notebook's default lakehouse, so one must be attached (and hold, or shortcut to, the data).", n,
                  "metastore table", "Attach a default lakehouse and make the tables available in it.")
        if re.search(r"\.synapsesql\s*\(", raw):
            c.add("synapsesql", K.data_access, S.alternative, "spark.read.synapsesql: Microsoft documents it as not compatible with Synapse "
                  "dedicated SQL pools; in Fabric it reads a warehouse or lakehouse SQL endpoint (user identity only, no service "
                  "principal), and the documented replacement for Synapse code is a Delta read.", n, "synapsesql",
                  "Copy the data into a lakehouse and read it as Delta.")


# =====================================================================================================================
# Python (ast)
# =====================================================================================================================


def _chain(node: ast.AST) -> Optional[str]:
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
        return ".".join(reversed(parts))
    return None


def _str_arg(call: ast.Call, i: int) -> Optional[str]:
    if len(call.args) > i and isinstance(call.args[i], ast.Constant) and isinstance(call.args[i].value, str):
        return call.args[i].value
    return None


class _PyVisitor(ast.NodeVisitor):
    def __init__(self, c: _Collector, cell: Cell, platform: str):
        self.c, self.cell, self.platform = c, cell, platform
        self.try_depth = 0

    def _line(self, node: ast.AST) -> int:
        return self.cell.start_line + getattr(node, "lineno", 1) - 1

    def visit_Try(self, node: ast.Try) -> None:
        self.try_depth += 1
        for n in node.body:
            self.visit(n)
        self.try_depth -= 1
        for n in [*node.handlers, *node.orelse, *node.finalbody]:
            self.visit(n)

    def visit_Import(self, node: ast.Import) -> None:
        for a in node.names:
            self._module(a.name, node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.level == 0 and node.module:
            self._module(node.module, node)

    def _module(self, name: str, node: ast.AST) -> None:
        root = name.split(".")[0]
        self.c.a.imports.append(root)
        n = self._line(node)
        if root == "dlt":
            self.c.add("delta_live_tables", K.api, S.alternative, "Delta Live Tables (import dlt) has no Fabric equivalent.", n,
                       "import dlt", "Rewrite as Spark notebooks with Structured Streaming / Delta merges.")
        elif root in ("dbruntime", "dbutils") or name.startswith("pyspark.dbutils"):
            self.c.add("databricks_runtime_import", K.api, S.change, f"'{name}' exists only on Databricks clusters.", n, f"import {name}",
                       "Remove it and use notebookutils.")
        elif name.startswith(("databricks.feature_store", "databricks.feature_engineering", "databricks.automl")):
            self.c.add("databricks_ml_service", K.api, S.alternative, f"'{name}' is a Databricks service with no Fabric equivalent.", n,
                       f"import {name}", "Use Fabric's data science tooling (MLflow tracking is available in Fabric notebooks).")
        elif root == "databricks" and name.split(".")[:2] == ["databricks", "connect"]:
            self.c.add("databricks_connect", K.api, S.change, "databricks-connect talks to a Databricks cluster; it does not apply in Fabric.", n, "databricks.connect")
        elif root == "databricks":
            self.c.add("databricks_sdk", K.library, S.review, "The Databricks SDK is a separate pip package; add it to the Environment "
                       "and supply a token that is valid for the workspace it calls.", n, f"import {name}")
        elif root == "Microsoft" or root == "microsoft" and name.startswith("microsoft.spark"):
            pass

    def visit_Call(self, node: ast.Call) -> None:
        chain = _chain(node.func) or ""
        attr = node.func.attr if isinstance(node.func, ast.Attribute) else ""
        n = self._line(node)
        if chain.startswith(("dbutils.", "mssparkutils.", "notebookutils.")):
            self._util(chain, node, n)
        elif chain in ("spark.conf.set", "spark.conf.get", "spark.sql.conf.set"):
            key = _str_arg(node, 0)
            if key:
                self.c.a.spark_conf_keys.append(key)
                self._conf(key, n)
        elif chain.endswith(("hadoopConfiguration.set", "hadoopConfiguration().set")):
            key = _str_arg(node, 0) or ""
            if key:
                self.c.a.spark_conf_keys.append(key)
                self._conf(key, n)
        elif attr == "synapsesql":
            self.c.add("synapsesql", K.data_access, S.alternative, "spark.read.synapsesql: Microsoft documents it as not compatible with Synapse "
                       "dedicated SQL pools; in Fabric it reads a warehouse or lakehouse SQL endpoint (user identity only, no service "
                       "principal), and the documented replacement for Synapse code is a Delta read.", n, "synapsesql",
                       "Copy the data into a lakehouse and read it as Delta.")
        elif attr in ("saveAsTable", "insertInto") or chain == "spark.table":
            self.c.add("metastore_tables", K.data_access, S.review, "The code reads or writes metastore tables. In Fabric they resolve "
                       "against the notebook's default lakehouse, so one must be attached (and hold, or shortcut to, the data).", n,
                       "metastore table", "Attach a default lakehouse and make the tables available in it.")
        elif chain == "getArgument" or chain.endswith(".getArgument"):
            self.c.add("legacy_widget_argument", K.api, S.change, "getArgument is the legacy widget call; pass the value as a parameter.", n, "getArgument")
        self.generic_visit(node)

    def _conf(self, key: str, n: int) -> None:
        low = key.lower()
        if low.startswith("spark.databricks."):
            self.c.add("databricks_spark_conf", K.spark_config, S.change, "A Databricks-only Spark setting; it has no effect in Fabric.", n, key,
                       "Remove it; check whether a Fabric setting does the same job.")
        elif low.startswith(("spark.synapse.", "spark.storage.synapse.")):
            self.c.add("synapse_spark_conf", K.spark_config, S.change, "A Synapse linked-service Spark setting; Microsoft documents these as removed in Fabric.", n, key,
                       "Remove it and use identity access, a shortcut, or Key Vault.")

    def _util(self, chain: str, node: ast.Call, n: int) -> None:
        parts = chain.split(".")
        util, sub = parts[0], parts[1] if len(parts) > 1 else ""
        method = parts[-1]
        if util == "dbutils":
            self._dbutils(sub, method, parts, node, n)
        elif util == "mssparkutils":
            self._mssparkutils(sub, method, parts, node, n)
        elif util == "notebookutils" and sub == "notebook" and method == "exit":
            self.c.a.exits = True
        if sub == "notebook" and method == "run":
            target = _str_arg(node, 0)
            if target:
                self.c.a.notebook_run_targets.append(target)

    def _dbutils(self, sub: str, method: str, parts: list[str], node: ast.Call, n: int) -> None:
        c = self.c
        if sub == "widgets":
            name = _str_arg(node, 0)
            if method in ("text", "dropdown", "combobox", "multiselect") and name:
                default = _str_arg(node, 1)
                c.a.widgets[name] = default
            elif method == "get" and name:
                c.a.widgets_used.append(name)
                c.a.widgets.setdefault(name, None)
            elif method in ("remove", "removeAll", "getAll", "getAllWidgets"):
                c.add("dbutils_widgets_other", K.api, S.change, f"dbutils.widgets.{method} has no Fabric equivalent; use parameters.", n, f"dbutils.widgets.{method}")
            if method == "get" and not name:
                c.add("dbutils_widgets_dynamic", K.api, S.change, "A widget is read by a computed name, so its parameter cannot be derived.", n, "dbutils.widgets.get(<expr>)")
        elif sub == "secrets":
            c.add("dbutils_secrets", K.authentication, S.change, "Databricks secret scopes do not exist in Fabric; read the secret from Azure Key Vault with "
                  "notebookutils.credentials.getSecret(<vault URL>, <secret name>).", n, f"dbutils.secrets.{method}",
                  "Provide a scope -> vault URL mapping, and give the running identity Key Vault secret access.")
        elif sub == "notebook" and method == "exit":
            c.a.exits = True
            if self.try_depth:
                c.a.exit_in_try = True
                c.add("exit_in_try", K.contract, S.change, "notebook.exit is called inside try/except; Microsoft documents that exit must not be placed there.", n,
                      "dbutils.notebook.exit", "Move the exit call outside the try block.")
        elif sub == "notebook" and method == "run":
            t = _str_arg(node, 0)
            c.add("dbutils_notebook_run", K.external_dependency, S.change, "dbutils.notebook.run calls another notebook by Databricks path. Fabric's "
                  "notebookutils.notebook.run takes the notebook NAME in the same workspace, so the callee must be migrated too.", n,
                  "dbutils.notebook.run", f"Migrate '{t}' and call it by name." if t else "Migrate the called notebook and call it by name.")
        elif sub == "notebook":
            c.add("dbutils_notebook_context", K.api, S.change, "The Databricks notebook context has no direct equivalent; notebookutils.runtime.context "
                  "carries Fabric's own context keys.", n, f"dbutils.notebook.{method}")
        elif sub == "fs":
            if method in FABRIC_FS:
                pass  # rewritten when the call shape allows it (see transform); otherwise reported there
            elif method in ("mount", "unmount", "mounts", "refreshMounts", "updateMount"):
                c.add("dbutils_fs_mount", K.filesystem, S.change, f"dbutils.fs.{method}: Fabric has notebookutils.fs.mount/unmount/mounts "
                      "(session-level, different options and credentials). Prefer a OneLake shortcut.", n, f"dbutils.fs.{method}",
                      "Create a shortcut, or port the call to notebookutils.fs.mount.")
            else:
                c.add("dbutils_fs_other", K.filesystem, S.change, f"dbutils.fs.{method} has no documented Fabric equivalent.", n, f"dbutils.fs.{method}")
        elif sub == "library":
            c.add("dbutils_library", K.library, S.change, "dbutils.library is not available; manage libraries through a Fabric Environment or %pip.", n,
                  f"dbutils.library.{method}", "Move the library to the Environment.")
        elif sub == "jobs":
            c.add("task_values", K.contract, S.alternative, "dbutils.jobs.taskValues passes values between Databricks job tasks; Fabric has no equivalent. "
                  "Return the value from notebookutils.notebook.exit and read it from the activity output.", n, "dbutils.jobs.taskValues",
                  "Use the notebook exit value and pipeline expressions.")
        else:
            c.add("dbutils_other", K.api, S.change, f"dbutils.{sub} has no documented Fabric equivalent.", n, f"dbutils.{sub}")

    def _mssparkutils(self, sub: str, method: str, parts: list[str], node: ast.Call, n: int) -> None:
        c = self.c
        if sub == "env":
            c.add("mssparkutils_env", K.api, S.change, "mssparkutils.env is not supported in Fabric.", n, f"mssparkutils.env.{method}",
                  "Use notebookutils.runtime.context for workspace and notebook details.")
        elif sub == "credentials" and method in ("getSecretWithLS", "getConnectionStringOrCreds", "getFullConnectionString", "getPropertiesAll",
                                                 "getSecretWithLS"):
            c.add("synapse_linked_service_credential", K.authentication, S.change, f"credentials.{method} resolves a Synapse linked service, which "
                  "does not exist in Fabric. getSecretWithLS(ls, key) becomes getSecret(<vault URL>, key); other uses need Key Vault or a Fabric connection.", n,
                  f"mssparkutils.credentials.{method}", "Replace the linked-service lookup with Key Vault.")
        elif sub == "credentials" and method not in ("getToken", "getSecret", "putSecret", "isValidToken"):
            c.add("mssparkutils_credentials_other", K.authentication, S.change, f"credentials.{method} is not available; Fabric has only getToken and getSecret "
                  "(plus putSecret/isValidToken).", n, f"mssparkutils.credentials.{method}")
        elif sub == "notebook" and method == "exit" and self.try_depth:
            c.a.exit_in_try = True
            c.add("exit_in_try", K.contract, S.change, "notebook.exit is called inside try/except; Microsoft documents that it must not be.", n, "notebook.exit")
        elif sub == "notebook" and method == "exit":
            c.a.exits = True
        elif sub not in SYNAPSE_UTILS_OK + ("credentials", "notebook", "fs", "runtime"):
            c.add("mssparkutils_other", K.api, S.review, f"mssparkutils.{sub} is not in the documented notebookutils set.", n, f"mssparkutils.{sub}")

    def visit_Assign(self, node: ast.Assign) -> None:
        # `x = dbutils.widgets.get("x")` is the widget itself becoming a variable, not a competing assignment
        v = node.value
        same = (isinstance(v, ast.Call) and _chain(v.func) == "dbutils.widgets.get" and len(v.args) == 1
                and isinstance(v.args[0], ast.Constant) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == v.args[0].value)
        if not same:
            for t in node.targets:
                if isinstance(t, ast.Name):
                    self.c.a.assigned_names.add(t.id)
                    if (isinstance(v, ast.Constant) and isinstance(v.value, str) and len(v.value) >= 12 and _RE_CRED_NAME.search(t.id)
                            and "://" not in v.value and " " not in v.value):
                        self.c.add("hardcoded_credential", K.authentication, S.change, "A variable named like a credential is assigned a long "
                                   "literal. The value is not recorded.", self._line(node), t.id, "Read it from Key Vault instead of the source.")
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        pass  # assignments are recorded by visit_Assign / visit_FunctionDef; loop and with targets are rare for parameters

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.c.a.assigned_names.add(node.name)
        self.generic_visit(node)


def _python_cell(c: _Collector, cell: Cell, platform: str) -> None:
    code = "\n".join("" if l.lstrip().startswith(("%", "!")) else l for l in cell.code.split("\n"))
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        c.a.syntax_errors.append(cell.start_line)
        c.add("syntax_error", K.language, S.review, f"A Python cell does not parse (line {exc.lineno}); it was scanned by pattern only and the "
              "generated code is not syntax-checked.", cell.start_line + (exc.lineno or 1) - 1, "SyntaxError")
        _scan_text(c, cell.code, cell.start_line, platform)
        _fallback_python(c, cell, platform)
        return
    _PyVisitor(c, cell, platform).visit(tree)
    for node in ast.walk(tree):  # string constants carry paths and hosts; comments are not visited
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value.strip():
            _scan_text(c, node.value, cell.start_line + node.lineno - 1, platform)
    _special_lines(c, cell, platform)


def _fallback_python(c: _Collector, cell: Cell, platform: str) -> None:
    for off, line in enumerate(cell.code.split("\n")):
        for m in re.finditer(r"\b(dbutils|mssparkutils)\.(\w+)\.?(\w*)", line):
            c.add(f"{m.group(1)}_{m.group(2)}", K.api, S.change, f"{m.group(1)}.{m.group(2)} is used (cell did not parse).", cell.start_line + off,
                  f"{m.group(1)}.{m.group(2)}.{m.group(3)}")


def _special_lines(c: _Collector, cell: Cell, platform: str) -> None:
    """Magic and command lines that ``ast`` cannot see."""
    for off, line in enumerate(cell.code.split("\n")):
        n = cell.start_line + off
        m = _RE_PIP.match(line)
        if m:
            c.a.pip_requirements += [p for p in re.split(r"\s+", m.group(1).strip()) if p and not p.startswith("-")]


# =====================================================================================================================
# the entry point
# =====================================================================================================================


def analyze(cells: Iterable[Cell], platform: str) -> Analysis:
    c = _Collector()
    for cell in cells:
        if cell.source_type == "markdown" or cell.language == "markdown":
            continue
        c.a.languages.append(cell.language)
        if cell.language == "python":
            _python_cell(c, cell, platform)
        elif cell.language == "pip":
            m = _RE_PIP.match(cell.code.strip().lstrip("%"))
            line = cell.code.strip().lstrip("%")
            m = re.match(r"^(?:pip|conda)\s+install\s+(.+)$", line, re.I)
            if m:
                c.a.pip_requirements += [p for p in re.split(r"\s+", m.group(1).strip()) if p and not p.startswith("-")]
            else:
                c.add("pip_other", K.library, S.review, "A %pip/%conda command other than install; confirm Fabric's inline library management supports it.", cell.start_line, "%pip")
        elif cell.language == "run":
            m = _RE_RUN.match(cell.code.strip())
            target = m.group(1) if m else ""
            c.a.run_targets.append(target)
            c.add("magic_run", K.external_dependency, S.change, "%run chains another notebook. Fabric's %run takes the notebook NAME (same workspace "
                  "only, up to 5 levels, no recursion), so the callee must be migrated too and the path replaced by its name.", cell.start_line,
                  f"%run {target}"[:80], "Migrate the target notebook and reference it by name.")
        elif cell.language == "shell":
            c.add("shell_magic", K.api, S.review, "A %sh shell cell; Fabric documents no equivalent magic, so move the work into Python or a pipeline step.", cell.start_line, "%sh")
        elif cell.language == "fs":
            c.add("fs_magic", K.filesystem, S.change, "A %fs magic works on DBFS; use notebookutils.fs in a Python cell.", cell.start_line, "%fs")
        elif cell.language == "csharp":
            c.add("dotnet_for_spark", K.language, S.blocker, ".NET for Spark (C#) is not supported in Fabric Spark.", cell.start_line, "%%csharp",
                  "Rewrite the cell in PySpark or Scala.")
        elif cell.language == "configure":
            c.add("configure_magic", K.spark_config, S.review, "%%configure: in a pipeline run it must be the first cell and is ignored when the notebook is "
                  "called with notebookutils.notebook.run.", cell.start_line, "%%configure")
        else:  # sql, scala, r
            _scan_text(c, cell.code, cell.start_line, platform)
            if cell.language in ("scala", "r"):
                for off, line in enumerate(cell.code.split("\n")):
                    if "dbutils." in line:
                        c.add("dbutils_other_language", K.api, S.change, f"dbutils is used in a {cell.language} cell; Fabric has notebookutils.", cell.start_line + off, "dbutils")
                    if re.search(r"com\.databricks\.|import\s+com\.databricks", line):
                        c.add("databricks_scala_api", K.api, S.change, "A com.databricks library is imported.", cell.start_line + off, "com.databricks")
                    if "TokenLibrary" in line:
                        c.add("token_library", K.authentication, S.change, "TokenLibrary (Synapse linked-service tokens) is removed in Fabric.", cell.start_line + off, "TokenLibrary")
                    if "using Microsoft.Spark" in line:
                        c.add("dotnet_for_spark", K.language, S.blocker, ".NET for Spark is not supported in Fabric Spark.", cell.start_line + off, "Microsoft.Spark")
    c.a.imports = sorted(set(c.a.imports))
    c.a.accounts = sorted(set(c.a.accounts))
    c.a.languages = sorted(set(c.a.languages))
    c.a.findings.sort(key=lambda f: (f.line or 0, f.code, f.construct))
    return c.a


def third_party_imports(imports: Iterable[str]) -> list[str]:
    """Imported modules that are neither the standard library nor Databricks-only: the ones an Environment may need."""
    return sorted({i for i in imports if i not in STDLIB and i not in _DATABRICKS_ONLY_ROOTS and i not in ("pyspark", "py4j")})

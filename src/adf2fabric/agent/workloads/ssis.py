"""ADF Execute SSIS Package -> Fabric Invoke SSIS package (preview): where the package is, reading it, and what Fabric cannot run.

Fabric runs a package with its Invoke SSIS Package activity (preview) only from a ``.dtsx`` (and optional ``.dtsConfig``) stored in
OneLake. So the work for each ADF activity is (1) find the package wherever ADF keeps it, (2) check that Fabric can run it, and (3) put
it in OneLake. This module does (1) and (2); nothing here talks to Fabric or writes anywhere.

Where ADF keeps a package (``typeProperties.packageLocation.type``):

* ``InlinePackage`` (Embedded package): the package itself is in the pipeline JSON (``typeProperties.packageContent``, child packages in
  ``childPackages``). The ADF portal says it compresses it; the encoding is not documented, so every plausible one is tried (plain XML,
  base64 of XML / gzip / zip / zlib / raw deflate) and an unreadable one is reported, never guessed.
* ``File``: a ``.dtsx`` on a file share (Azure Files), or (File System (Project)) a ``.ispac`` project plus ``typeProperties.packageName``.
* ``SSISDB``: deployed to an SSIS catalog; ``packagePath`` is ``<folder>/<project>/<package>.dtsx``.
* ``PackageStore``: a package store attached to the Azure-SSIS runtime; ``packagePath`` is ``<store>\\<folder>\\<package>`` (no .dtsx).

What Fabric cannot run (Microsoft, preview limitations): on-premises sources or destinations, private-network endpoints, custom or
third-party components. Packages are read with the standard library parser after refusing any DTD / entity declaration (a ``.dtsx``
never has one), with a size limit, so a hostile file cannot expand or reach out. Connection strings are read in memory to find the host
and the authentication; they are never stored or printed (they can hold a password).

Sources (read 2026-11-18):
* ADF REST spec, azure-rest-api-specs ``DataFactory/models.tsp``: ExecuteSSISPackageActivityTypeProperties, SSISPackageLocation,
  SSISPackageLocationTypeProperties, SSISChildPackage, SSISExecutionParameter, SSISPropertyOverride, SSISLogLocation,
  IntegrationRuntimeSsisProperties / ComputeProperties / VNetProperties / DataProxyProperties.
* ADF "Use the Execute SSIS Package activity" (page updated 2026-08-05): the five portal locations, protection levels.
* Fabric "Use the Invoke SSIS Package activity in a pipeline (Preview)" (page updated 2026-06-02).
* The ``.dtsx`` layout of Microsoft's sample package (sql-server-samples, WideWorldImporters ``DailyETLMain.dtsx``, MIT).

Not documented by Microsoft, so not generated: the Fabric JSON inside ``packageLocation`` / ``logLocation`` of the Fabric activity.
"""

from __future__ import annotations

import base64
import binascii
import io
import re
import zipfile
import zlib
import xml.etree.ElementTree as ET
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Optional

from adf2fabric.connections.integration_runtime import classify_host
from adf2fabric.project.workloads import Finding, FindingCategory as F, Severity as S

_D = "{www.microsoft.com/SqlServer/Dts}"
_SSIS = "{www.microsoft.com/SqlServer/SSIS}"
MAX_PACKAGE_BYTES = 64 * 1024 * 1024
LOCATION_TYPES = ("SSISDB", "File", "InlinePackage", "PackageStore")
PROTECTION = {0: "DontSaveSensitive", 1: "EncryptSensitiveWithUserKey", 2: "EncryptSensitiveWithPassword", 3: "EncryptAllWithPassword",
              4: "EncryptAllWithUserKey", 5: "ServerStorage"}
# Finding codes that mean "supply a readable package", not "Fabric has no equivalent" (the engine classifies them as blocked).
MISSING_INPUT = {"ssis_encrypted"}

_BUILTIN_PREFIXES = ("microsoft.", "stock:", "ssis.")
_LOOPBACK = {".", "(local)", "localhost", "127.0.0.1", "::1"}
_FABRIC_SUFFIXES = (".fabric.microsoft.com", ".pbidedicated.windows.net")
_HOST_KEYS = ("data source", "server", "address", "addr", "network address", "host", "hostname", "smtpserver")
_FILE_TYPES = {"FLATFILE", "MULTIFLATFILE", "FILE", "MULTIFILE"}
_CHECKED_TYPES = _FILE_TYPES | {"OLEDB", "ODBC", "EXCEL", "SMTP", "MSOLAP100"}
_DRIVE = re.compile(r"^[A-Za-z]:[\\/]")
_UNC = re.compile(r"^\\\\([^\\/]+)[\\/]")
_CM_OVERRIDE = re.compile(r"Connections\[([^\]]+)\]\.Properties\[ConnectionString\]", re.I)
# Built-in tasks a person should look at before running in Fabric: what they reach or run lives outside the package.
_TASKS_TO_REVIEW = {
    "microsoft.executeprocess": ("ssis_execute_process", F.external_dependency, S.change,
                                 "runs a program on the machine the package runs on; Fabric's SSIS runtime has no such program",
                                 "Replace the Execute Process task, or keep this package on Azure-SSIS."),
    "microsoft.scripttask": ("ssis_script_task", F.language, S.review, "runs custom .NET code",
                             "Check that the script uses no local files, registry or machine-specific resources."),
    "microsoft.executepackagetask": ("ssis_child_package", F.external_dependency, S.review, "runs another (child) package",
                                     "Upload the child package(s) to OneLake next to this one; a child referenced by a file path must be re-pointed."),
    "microsoft.sendmailtask": ("ssis_network_task", F.networking, S.review, "sends mail through an SMTP server",
                               "Check that Fabric can reach the SMTP server (no private network or on-premises endpoints)."),
    "microsoft.ftptask": ("ssis_network_task", F.networking, S.review, "connects to an FTP server",
                          "Check that Fabric can reach the FTP server (no private network or on-premises endpoints)."),
    "microsoft.webservicetask": ("ssis_network_task", F.networking, S.review, "calls a web service",
                                 "Check that Fabric can reach the service (no private network or on-premises endpoints)."),
    "microsoft.wmidatareadertask": ("ssis_machine_task", F.external_dependency, S.change, "reads WMI data from a machine",
                                    "Replace the WMI task: Fabric's SSIS runtime cannot query your machines."),
    "microsoft.wmieventwatchertask": ("ssis_machine_task", F.external_dependency, S.change, "watches WMI events on a machine",
                                      "Replace the WMI task: Fabric's SSIS runtime cannot query your machines."),
    "microsoft.messagequeuetask": ("ssis_machine_task", F.external_dependency, S.change, "uses Microsoft Message Queuing (MSMQ)",
                                   "Replace the Message Queue task: MSMQ is a machine-local service."),
}


def _finding(code: str, cat: F, sev: S, message: str, action: str = "", construct: str = "") -> Finding:
    return Finding(code=code, category=cat, severity=sev, message=message, construct=construct, action=action)


def _text(v: Any) -> Optional[str]:
    """A literal string (an ADF expression, ``@...`` or ``{"type": "Expression"}``, is not one)."""
    return v if isinstance(v, str) and not v.lstrip().startswith("@") else None


def file_name(path: str) -> str:
    return re.split(r"[\\/]", path.rstrip("\\/"))[-1]


# =====================================================================================================================
# where the package is
# =====================================================================================================================


@dataclass
class PackageRef:
    location_type: str  # SSISDB | File | InlinePackage | PackageStore | unspecified | other value as authored
    path: Optional[str]  # packagePath as authored (None: absent, or an expression)
    package_file: str  # the .dtsx file name Fabric will run
    project_file: Optional[str] = None  # File System (Project): the .ispac
    configuration_path: Optional[str] = None
    dynamic: bool = False  # the path or the embedded content is an expression
    password_set: bool = False
    identity: str = ""  # stable, for the workload id
    content: Any = None  # InlinePackage: packageContent as authored
    children: list[dict] = field(default_factory=list)  # InlinePackage: childPackages as authored

    @property
    def name(self) -> str:
        return self.package_file[:-5] if self.package_file.lower().endswith(".dtsx") else self.package_file

    @property
    def source_item(self) -> str:
        """What the report and the converter show for this package."""
        if self.location_type == "InlinePackage":
            return f"embedded:{self.identity.split(':', 1)[1]}"
        if self.project_file:
            return f"{self.project_file}#{self.package_file}"
        return self.path or f"(dynamic: {self.identity})"


def package_ref(tp: dict, pipeline: str, activity: str) -> PackageRef:
    """Where an Execute SSIS Package activity's package is, from its typeProperties."""
    loc = tp.get("packageLocation") if isinstance(tp.get("packageLocation"), dict) else {}
    ltp = loc.get("typeProperties") if isinstance(loc.get("typeProperties"), dict) else {}
    kind = loc.get("type") if isinstance(loc.get("type"), str) and loc.get("type") else "unspecified"
    raw_path = loc.get("packagePath")
    path = _text(raw_path)
    dynamic = raw_path is not None and path is None
    name = ltp.get("packageName") if isinstance(ltp.get("packageName"), str) else None
    ref = PackageRef(location_type=kind, path=path, package_file="", dynamic=dynamic, password_set="packagePassword" in ltp,
                     configuration_path=_text(ltp.get("configurationPath")))
    here = f"{pipeline}/{activity}"
    if kind == "InlinePackage":
        ref.content = ltp.get("packageContent")
        ref.dynamic = ref.dynamic or not isinstance(ref.content, str)
        ref.children = [c for c in ltp.get("childPackages") or [] if isinstance(c, dict)]
        ref.package_file = name or (file_name(path) if path else f"{activity}.dtsx")
        ref.identity = f"inline:{here}"
    elif kind == "File" and path and path.lower().endswith(".ispac"):
        ref.project_file, ref.package_file = path, name or ""
        ref.identity = f"file:{path}#{ref.package_file}"
    else:
        ref.package_file = file_name(path) if path else (name or f"{activity}.dtsx")
        if kind == "PackageStore" and not ref.package_file.lower().endswith(".dtsx"):
            ref.package_file += ".dtsx"
        ref.identity = f"{kind.lower()}:{path}" if path else f"{kind.lower()}:(dynamic) {here}"
    if ref.package_file and not ref.package_file.lower().endswith(".dtsx"):
        ref.package_file += ".dtsx"
    return ref


# =====================================================================================================================
# reading the package
# =====================================================================================================================


class PackageError(ValueError):
    """The package cannot be read (encoding, size, or a document that is not a package)."""


def _inflate(data: bytes, wbits: int) -> bytes:
    d = zlib.decompressobj(wbits)
    out = d.decompress(data, MAX_PACKAGE_BYTES + 1)
    if len(out) > MAX_PACKAGE_BYTES:
        raise PackageError(f"it expands to more than {MAX_PACKAGE_BYTES // (1024 * 1024)} MB")
    return out


def _looks_xml(data: bytes) -> bool:
    head = data[:64].lstrip(b"\xef\xbb\xbf \t\r\n")
    return head.startswith(b"<") or data[:2] in (b"\xff\xfe", b"\xfe\xff")


def _first_dtsx(data: bytes, wanted: Optional[str] = None) -> Optional[bytes]:
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        names = [i for i in z.infolist() if i.filename.lower().endswith(".dtsx")]
        pick = next((i for i in names if wanted and file_name(i.filename).lower() == wanted.lower()), names[0] if names else None)
        if pick is None:
            return None
        if pick.file_size > MAX_PACKAGE_BYTES:
            raise PackageError("the package in the archive is too large")
        return z.read(pick)


def unpack(data: bytes, wanted: Optional[str] = None) -> tuple[bytes, str]:
    """(the package XML, how it was encoded) from bytes that are XML, gzip, zip, zlib or raw deflate."""
    if len(data) > MAX_PACKAGE_BYTES:
        raise PackageError("it is larger than the size limit")
    if _looks_xml(data):
        return data, "xml"
    if data[:2] == b"\x1f\x8b":
        return _inflate(data, 31), "gzip"
    if data[:4] == b"PK\x03\x04":
        x = _first_dtsx(data, wanted)
        if x is None:
            raise PackageError("the archive holds no .dtsx")
        return x, "zip"
    for wbits, how in ((15, "zlib"), (-15, "deflate")):
        try:
            x = _inflate(data, wbits)
        except zlib.error:
            continue
        if _looks_xml(x):
            return x, how
    raise PackageError("it is not XML, gzip, zip, zlib or deflate")


def decode_content(value: Any) -> tuple[bytes, str]:
    """An embedded package's content (``packageContent``) as XML bytes, and how it was encoded. Raises PackageError."""
    if not isinstance(value, str) or not value.strip():
        raise PackageError("the embedded content is missing or an expression")
    s = value.strip()
    if s.lstrip("﻿").startswith("<"):
        return s.encode("utf-8"), "xml"
    try:
        raw = base64.b64decode(s, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise PackageError("the embedded content is neither XML nor base64") from exc
    x, how = unpack(raw)
    return x, f"base64+{how}"


def from_ispac(data: bytes, package_file: str) -> tuple[Optional[bytes], dict[str, bytes]]:
    """(the named package, the project files: Project.params, *.conmgr, @Project.manifest) from a .ispac project (a zip)."""
    files: dict[str, bytes] = {}
    pkg = None
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        for i in z.infolist():
            if i.file_size > MAX_PACKAGE_BYTES:
                raise PackageError(f"'{i.filename}' in the project is too large")
            low = i.filename.lower()
            if low.endswith((".conmgr", ".params", ".manifest")):
                files[file_name(i.filename)] = z.read(i)
            elif low.endswith(".dtsx") and file_name(i.filename).lower() == package_file.lower():
                pkg = z.read(i)
    return pkg, files


def _parse(xml: bytes) -> ET.Element:
    if len(xml) > MAX_PACKAGE_BYTES:
        raise PackageError("it is larger than the size limit")
    if re.search(rb"<!\s*(DOCTYPE|ENTITY)", xml, re.I):
        raise PackageError("it declares a DTD or entities, which a package never does")
    try:
        return ET.fromstring(xml)
    except ET.ParseError as exc:
        raise PackageError(f"it is not well-formed XML ({exc})") from exc


# =====================================================================================================================
# what is in the package
# =====================================================================================================================


@dataclass
class Connection:
    name: str
    creation: str  # OLEDB, ADO.NET:..., FLATFILE, ...
    scope: str  # package | project
    host: Optional[str] = None
    exposure: str = "unknown"  # public | private | unknown (the host's), or "" when there is no host
    evidence: str = ""
    windows_auth: bool = False
    path_kind: str = ""  # local | network_share | azure_files ('' : not a file path)
    expression: bool = False  # the connection string is set by an expression at run time
    overridden: str = ""  # literal | runtime ('' : not overridden by the ADF activity)


@dataclass
class PackageAnalysis:
    name: str
    protection_level: int
    encrypted: bool
    format_version: Optional[int]
    product_version: str
    executables: Counter = field(default_factory=Counter)  # task type -> count (the package itself excluded)
    components: Counter = field(default_factory=Counter)  # data flow component class -> count
    connections: list[Connection] = field(default_factory=list)
    project_connection_ids: set[str] = field(default_factory=set)  # project connection managers the package refers to
    parameters: list[tuple[str, bool]] = field(default_factory=list)  # (name, sensitive)

    @property
    def protection(self) -> str:
        return PROTECTION.get(self.protection_level, f"unknown ({self.protection_level})")


def _kv(cs: str) -> dict[str, str]:
    out = {}
    for part in cs.split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip().lower()] = v.strip().strip('"').strip("'")
    return out


def _host_value(raw: str) -> str:
    h = raw.strip().lower()
    for prefix in ("tcp:", "np:", "lpc:"):
        if h.startswith(prefix):
            h = h[len(prefix):]
    if h.startswith("(localdb)"):
        return "(local)"
    return re.split(r"[\\,]", h)[0].strip()


def _classify(host: str) -> tuple[str, str]:
    if host in _LOOPBACK:
        return "private", f"'{host}' is the machine the package runs on"
    if host.endswith(_FABRIC_SUFFIXES):
        return "public", f"{host} is a Fabric endpoint"
    exposure, evidence = classify_host(host)
    return exposure, " ".join(evidence)


def describe_connection_string(c: Connection, cs: str) -> None:
    """Fill the host, exposure, Windows-auth and file-path facts of ``c`` from a connection string (kept in memory only)."""
    kind = c.creation.upper()
    kv = _kv(cs)
    path = cs.strip() if kind in _FILE_TYPES else (kv.get("data source", "") if kind == "EXCEL" else "")
    if path:
        unc = _UNC.match(path)
        if _DRIVE.match(path):
            c.path_kind = "local"
        elif unc:
            share = unc.group(1).lower()
            c.path_kind = "azure_files" if share.endswith(".file.core.windows.net") else "network_share"
            c.host = share
        return
    raw = next((kv[k] for k in _HOST_KEYS if kv.get(k)), "")
    if raw:
        c.host = _host_value(raw)
        c.exposure, c.evidence = _classify(c.host)
    c.windows_auth = kv.get("integrated security", "").lower() in ("sspi", "true", "yes") or kv.get("trusted_connection", "").lower() in ("yes", "true")


def _cm_connections(root: ET.Element, scope: str) -> list[tuple[Connection, Optional[str]]]:
    """(connection, its connection string) for each connection manager under ``root`` (a package, or a project .conmgr)."""
    out = []
    cms = [root] if root.tag == f"{_D}ConnectionManager" else list(root.iter(f"{_D}ConnectionManager"))
    for cm in cms:
        creation = cm.get(f"{_D}CreationName")
        if not creation:  # the inner <DTS:ObjectData><DTS:ConnectionManager> that holds the string
            continue
        inner = next((x for x in cm.iter(f"{_D}ConnectionManager") if x is not cm), None)
        cs = inner.get(f"{_D}ConnectionString") if inner is not None else None
        expr = any(p.get(f"{_D}Name", "").lower() == "connectionstring" for p in cm.iter(f"{_D}PropertyExpression"))
        out.append((Connection(name=cm.get(f"{_D}ObjectName") or "?", creation=creation, scope=scope, expression=expr), cs))
    return out


def analyze_package(xml: bytes, project_files: Optional[dict[str, bytes]] = None) -> tuple[PackageAnalysis, dict[str, Optional[str]]]:
    """(what is in the package, each connection's connection string by name: in memory only). Raises PackageError."""
    root = _parse(xml)
    if root.tag != f"{_D}Executable":
        raise PackageError("its root element is not an SSIS package")
    fmt = next((p.text for p in root.iter(f"{_D}Property") if p.get(f"{_D}Name") == "PackageFormatVersion"), None)
    a = PackageAnalysis(name=root.get(f"{_D}ObjectName") or "", protection_level=int(root.get(f"{_D}ProtectionLevel", "1") or 1),
                        encrypted=any(e.tag.rsplit("}", 1)[-1] == "EncryptedData" for e in root.iter()),
                        format_version=int(fmt) if fmt and fmt.strip().isdigit() else None,
                        product_version=root.get(f"{_D}LastModifiedProductVersion") or "")
    for e in root.iter(f"{_D}Executable"):
        if e is not root:
            a.executables[(e.get(f"{_D}ExecutableType") or "?").split(",")[0].strip()] += 1
    for comp in root.iter("component"):
        cls = comp.get("componentClassID") or "?"
        if cls.lower() == "microsoft.managedcomponenthost":  # a managed component: its real class is a property
            utn = next((p.text for p in comp.iter("property") if p.get("name") == "UserComponentTypeName" and p.text), None)
            cls = utn.split(",")[0].strip() if utn else cls
        a.components[cls] += 1
    for c in root.iter("connection"):
        ref = c.get("connectionManagerID") or ""
        if ref.endswith(":external"):
            a.project_connection_ids.add(ref[: -len(":external")])
    strings: dict[str, Optional[str]] = {}
    for conn, cs in _cm_connections(root, "package"):
        a.connections.append(conn)
        strings[conn.name] = cs
    for p in root.iter(f"{_D}PackageParameter"):
        a.parameters.append((p.get(f"{_D}ObjectName") or "?", (p.get(f"{_D}Sensitive") or "").lower() == "true"))
    for fname, data in sorted((project_files or {}).items()):
        if fname.lower().endswith(".conmgr"):
            for conn, cs in _cm_connections(_parse(data), "project"):
                a.connections.append(conn)
                strings[conn.name] = cs
        elif fname.lower() == "project.params":
            for p in _parse(data).iter(f"{_SSIS}Parameter"):
                sens = next((x.text for x in p.iter(f"{_SSIS}Property") if x.get(f"{_SSIS}Name") == "Sensitive"), "0")
                a.parameters.append((p.get(f"{_SSIS}Name") or "?", sens in ("1", "True", "true")))
    return a, strings


# =====================================================================================================================
# what Fabric cannot run, and what changes
# =====================================================================================================================


def _overrides(tp: dict) -> dict[str, Any]:
    """Connection manager name (lower) -> the ADF activity's ConnectionString override value as authored."""
    out: dict[str, Any] = {}
    for key in ("packageConnectionManagers", "projectConnectionManagers"):
        for cm, props in (tp.get(key) or {}).items() if isinstance(tp.get(key), dict) else []:
            for prop, v in (props or {}).items() if isinstance(props, dict) else []:
                if prop.lower() == "connectionstring":
                    out[cm.lower()] = v.get("value") if isinstance(v, dict) else v
    for path, v in (tp.get("propertyOverrides") or {}).items() if isinstance(tp.get("propertyOverrides"), dict) else []:
        m = _CM_OVERRIDE.search(path)
        if m:
            out[m.group(1).lower()] = v.get("value") if isinstance(v, dict) else v
    return out


def package_findings(a: PackageAnalysis, strings: dict[str, Optional[str]], tp: dict, project_files_known: bool) -> list[Finding]:
    """Findings for a package's contents, after applying the ADF activity's connection string overrides."""
    out: list[Finding] = []
    pl = a.protection_level
    if a.encrypted or pl in (3, 4):
        why = ("ADF does not support EncryptAllWithUserKey either" if pl == 4 else
               "Fabric's Invoke SSIS package activity shows no package password setting")
        out.append(_finding("ssis_encrypted", F.authentication, S.blocker,
                            f"The whole package is encrypted ({a.protection}), so its contents cannot be checked; {why}.",
                            "Re-save it in SSDT with DontSaveSensitive (pass secrets as connection manager or property overrides), then assess again."))
        return out
    if pl == 1:
        out.append(_finding("ssis_sensitive_user_key", F.authentication, S.change,
                            "Sensitive values (passwords) are encrypted with the developer's Windows key (EncryptSensitiveWithUserKey); no other "
                            "machine can decrypt them.", "Give them as connection manager or property overrides on the Fabric activity, or re-save "
                            "the package with DontSaveSensitive."))
    elif pl == 2:
        out.append(_finding("ssis_package_password", F.authentication, S.change,
                            "Sensitive values are encrypted with a package password (EncryptSensitiveWithPassword); Fabric's Invoke SSIS package "
                            "activity shows no package password setting.", "Re-save the package with DontSaveSensitive and pass the secrets as overrides."))
    elif pl == 5:
        out.append(_finding("ssis_server_storage", F.authentication, S.review, "The package relies on SQL Server storage roles (ServerStorage).",
                            "Re-save it with DontSaveSensitive before uploading the file to OneLake."))
    if a.format_version is not None and a.format_version < 6:
        out.append(_finding("ssis_old_format", F.runtime, S.review, f"The package is in a pre-2012 format (PackageFormatVersion {a.format_version}); "
                            "its tasks and connections were not analysed in detail.", "Upgrade it in a current SSDT and assess again."))
        return out
    for t, n in sorted(a.executables.items()):
        low = t.lower()
        if not low.startswith(_BUILTIN_PREFIXES):
            out.append(_finding("ssis_third_party_task", F.library, S.alternative,
                                f"Task type '{t}' ({n}x) is not a built-in SSIS task; Fabric's Invoke SSIS package cannot run custom or third-party "
                                "components.", "Replace it with built-in tasks, or keep this package on Azure-SSIS.", t))
        elif low in _TASKS_TO_REVIEW:
            code, cat, sev, what, action = _TASKS_TO_REVIEW[low]
            out.append(_finding(code, cat, sev, f"A task ({t}, {n}x) {what}.", action, t))
    for c, n in sorted(a.components.items()):
        low = c.lower()
        if c.startswith("{"):
            out.append(_finding("ssis_component_class_id", F.library, S.review, f"A data flow component is named only by a class id ({c}, {n}x).",
                                "Check in SSDT that it is a built-in component.", c))
        elif not low.startswith(_BUILTIN_PREFIXES):
            out.append(_finding("ssis_third_party_component", F.library, S.alternative,
                                f"Data flow component '{c}' ({n}x) is not a built-in SSIS component; Fabric cannot run custom or third-party "
                                "components.", "Replace it with built-in components, or keep this package on Azure-SSIS.", c))
        elif "scriptcomponent" in low:
            out.append(_finding("ssis_script_component", F.language, S.review, f"A data flow Script Component ({n}x) runs custom .NET code.",
                                "Check that the script uses no local files, registry or machine-specific resources.", c))
    overrides = _overrides(tp)
    for c in a.connections:
        ov = overrides.get(c.name.lower())
        if ov is not None:
            if isinstance(ov, str) and not ov.lstrip().startswith("@"):
                c.overridden = "literal"
                describe_connection_string(c, ov)
            else:
                c.overridden = "runtime"
        elif strings.get(c.name):
            describe_connection_string(c, strings[c.name] or "")
        out += _connection_findings(c)
    if a.project_connection_ids and not project_files_known:
        out.append(_finding("ssis_project_connections_unknown", F.data_access, S.review,
                            f"The package uses {len(a.project_connection_ids)} project-level connection manager(s) that are not in the package file.",
                            "Pass the project (.ispac) with --source so they are checked too."))
    for name, sensitive in a.parameters:
        if sensitive:
            out.append(_finding("ssis_sensitive_parameter", F.authentication, S.review, f"Parameter '{name}' is sensitive.",
                                "Give its value on the Fabric activity at run time; a sensitive value is not stored in the package.", name))
    return out


def _connection_findings(c: Connection) -> list[Finding]:
    where = f"Connection manager '{c.name}' ({c.creation}, {c.scope})"
    if c.overridden == "runtime" or (c.expression and not c.overridden):
        how = "an ADF override set at run time" if c.overridden == "runtime" else "an expression"
        return [_finding("ssis_connection_runtime", F.data_access, S.review, f"{where} gets its connection string from {how}, so its target was not checked.",
                         "Check that Fabric can reach the target: no on-premises or private-network endpoints.", c.name)]
    out = []
    if c.path_kind in ("local", "network_share"):
        place = "a local disk path" if c.path_kind == "local" else f"a network share on {c.host}"
        out.append(_finding("ssis_local_file", F.filesystem, S.change, f"{where} reads or writes {place}, which Fabric's SSIS runtime cannot see.",
                            "Point it at ADLS Gen2 / OneLake (Microsoft's Fabric tutorial writes files through ADLS Gen2) with an override.", c.name))
    elif c.path_kind == "azure_files":
        out.append(_finding("ssis_azure_files", F.filesystem, S.review, f"{where} uses an Azure Files share ({c.host}); Fabric documents no Azure Files "
                            "access from SSIS.", "Prefer ADLS Gen2 / OneLake, or test the share from Fabric first.", c.name))
    if c.host and c.exposure == "private":
        out.append(_finding("ssis_private_source", F.networking, S.alternative,
                            f"{where} connects to '{c.host}': {c.evidence}. Fabric's Invoke SSIS package cannot reach on-premises or private-network "
                            "endpoints.", "Override the connection to a cloud endpoint Fabric can reach, or keep this package on Azure-SSIS.", c.name))
    elif c.host and c.exposure == "unknown" and not c.path_kind:
        out.append(_finding("ssis_source_reachability", F.networking, S.review, f"{where} connects to '{c.host}'; {c.evidence}",
                            "Confirm Fabric can reach it: no on-premises or private-network endpoints.", c.name))
    if c.windows_auth:
        out.append(_finding("ssis_windows_auth", F.authentication, S.change,
                            f"{where} uses Windows authentication; Fabric's documented SSIS scenarios use Microsoft Entra (service principal) or SQL "
                            "authentication.", "Override the connection string to use Microsoft Entra or SQL authentication.", c.name))
    if not c.host and not c.path_kind and c.creation.upper() not in _CHECKED_TYPES and not c.creation.upper().startswith("ADO.NET"):
        out.append(_finding("ssis_connection_unchecked", F.data_access, S.info, f"{where}: this connection type is not checked by this tool.",
                            construct=c.name))
    return out


def activity_findings(tp: dict, ref: PackageRef, runtime: Optional[dict]) -> list[Finding]:
    """Findings for the ADF activity's own settings and its Azure-SSIS runtime (``runtime``: the runtime's JSON, or None)."""
    out: list[Finding] = []
    if ref.location_type not in LOCATION_TYPES:
        out.append(_finding("ssis_location_type", F.source, S.review, f"The package location type is '{ref.location_type}'.",
                            "Check the activity in ADF Studio: it should be SSISDB, File, InlinePackage or PackageStore."))
    if str(tp.get("runtime") or "").lower() == "x86":
        out.append(_finding("ssis_32_bit", F.runtime, S.review, "The package runs in the 32-bit runtime in ADF; Fabric's activity documents no 32-bit option.",
                            "Check that it runs in 64-bit (32-bit-only drivers such as old Excel/Access providers will not)."))
    if isinstance(tp.get("executionCredential"), dict):
        out.append(_finding("ssis_windows_execution_credential", F.authentication, S.change,
                            "The package runs under Windows credentials in ADF (executionCredential); Fabric's activity documents no execution "
                            "credential.", "Use connection-level credentials (Microsoft Entra or SQL authentication) through overrides."))
    if tp.get("environmentPath") is not None:
        out.append(_finding("ssis_environment", F.contract, S.change,
                            "The package takes values from an SSISDB environment (environmentPath); Fabric has no SSIS catalog.",
                            "Give those values as parameters or property overrides on the Fabric activity."))
    if ref.password_set:
        out.append(_finding("ssis_package_password_set", F.authentication, S.review, "ADF passes a package password.",
                            "Fabric shows no package password setting: re-save the package with DontSaveSensitive."))
    if ref.configuration_path:
        out.append(_finding("ssis_configuration_file", F.contract, S.review, "The package reads a configuration file (.dtsConfig).",
                            "Upload the .dtsConfig to OneLake next to the package and select it as the activity's configuration path."))
    if isinstance(tp.get("logLocation"), dict) or tp.get("loggingLevel") is not None:
        out.append(_finding("ssis_logging", F.contract, S.info, "ADF writes package logs to its own log location / level; Fabric's 'Enable logging' "
                            "writes them to OneLake and the run output gives the path."))
    for scope in ("projectParameters", "packageParameters", "packageConnectionManagers", "projectConnectionManagers", "propertyOverrides"):
        block = tp.get(scope) if isinstance(tp.get(scope), dict) else {}
        values = [v for x in block.values() for v in ((x.values() if scope.endswith("ConnectionManagers") and isinstance(x, dict) else [x]))]
        if any(isinstance(v, dict) and isinstance(v.get("value"), dict) and v["value"].get("type") == "AzureKeyVaultSecret" for v in values):
            out.append(_finding("ssis_key_vault_value", F.authentication, S.review, f"Some {scope} values come from Azure Key Vault.",
                                "Check how the Fabric activity receives the secret before running it.", scope))
    if runtime is None:
        return out
    tpr = (runtime.get("properties") or {}).get("typeProperties") or {}
    ssis = tpr.get("ssisProperties") if isinstance(tpr.get("ssisProperties"), dict) else {}
    compute = tpr.get("computeProperties") if isinstance(tpr.get("computeProperties"), dict) else {}
    vnet = compute.get("vNetProperties") if isinstance(compute.get("vNetProperties"), dict) else {}
    if isinstance(ssis.get("dataProxyProperties"), dict):
        out.append(_finding("ssis_ir_onprem_proxy", F.networking, S.alternative,
                            "The Azure-SSIS runtime reaches on-premises data through a self-hosted runtime proxy; Fabric's Invoke SSIS package cannot "
                            "connect to on-premises systems.", "Move the on-premises data to the cloud first, or keep this package on Azure-SSIS."))
    if any(vnet.get(k) for k in ("vNetId", "subnetId", "subnet")):
        out.append(_finding("ssis_ir_vnet", F.networking, S.review,
                            "The Azure-SSIS runtime is joined to a virtual network; anything the package reaches through it is out of Fabric's reach "
                            "(no private-network endpoints).", "Check each connection's target is reachable from the internet."))
    setups = [str(x.get("type")) for x in ssis.get("expressCustomSetupProperties") or [] if isinstance(x, dict)]
    if setups or isinstance(ssis.get("customSetupScriptProperties"), dict):
        what = ", ".join(sorted(set(setups))) or "a custom setup script"
        out.append(_finding("ssis_ir_custom_setup", F.library, S.review, f"The Azure-SSIS runtime installs extra software ({what}); Fabric cannot install "
                            "custom or third-party components.", "Check the package does not need what the setup installs."))
    if str(ssis.get("edition") or "").lower() == "enterprise":
        out.append(_finding("ssis_ir_enterprise", F.runtime, S.review, "The Azure-SSIS runtime is the Enterprise edition; Fabric does not document "
                            "which SSIS edition it runs.", "Check the package uses no Enterprise-only components (for example CDC or fuzzy matching)."))
    return out


def summary(a: PackageAnalysis) -> str:
    """One line for the report: protection level, tasks, data flow components and connections."""
    tasks = sum(a.executables.values())
    comps = sum(a.components.values())
    return (f"{a.name or '(unnamed)'}: {a.protection}, {tasks} task(s) ({len(a.executables)} type(s)), {comps} data flow component(s), "
            f"{len(a.connections)} connection manager(s)")

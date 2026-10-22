"""Data flow definitions (``dataflow/*.json``): sources, sinks, transformations and what they reference.

The structure follows Microsoft's ARM schema for ``Microsoft.DataFactory/factories/dataflows`` (2018-06-01):

* ``properties.type`` is ``MappingDataFlow``, ``Flowlet`` or ``WranglingDataFlow``;
* ``typeProperties`` holds ``sources``, ``sinks``, ``transformations`` (Mapping and Flowlet), ``script`` and
  ``scriptLines`` (the data flow script), and for a Wrangling flow ``sources`` (each may carry a Power Query
  ``script``), ``script`` and ``documentLocale``;
* a node (source / sink / transformation) has ``name``, ``description`` and the references ``dataset``,
  ``linkedService``, ``flowlet``, ``schemaLinkedService`` and, on a sink, ``rejectedDataLinkedService``.

Only the DESCRIPTION of a flow is read. The script text itself is never stored (it can embed literals and expressions);
its size is. Anything the schema does not list is kept under ``unknown_properties``, never dropped. Like the pipeline
parser, :func:`parse_dataflow` never raises: problems become ``issues``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from adf2fabric.agent.discovery.adf_semantic.parser_pipelines import _Sanitizer, _ref_name
from adf2fabric.project.discovery import (
    AdfDataFlow,
    AdfDataFlowNode,
    AdfIssueSeverity,
    AdfParseIssue,
    AdfSourceLocation,
    DiscoveryStatus,
)

FLOW_TYPES = ("MappingDataFlow", "Flowlet", "WranglingDataFlow")
_NODE_LISTS = (("sources", "source"), ("sinks", "sink"), ("transformations", "transformation"))
_KNOWN_PROPERTIES = {"annotations", "description", "folder", "type", "typeProperties"}
_KNOWN_TYPE_PROPERTIES = {"sources", "sinks", "transformations", "script", "scriptLines", "documentLocale"}
_KNOWN_NODE_KEYS = {"name", "description", "script", "dataset", "linkedService", "flowlet", "schemaLinkedService",
                    "rejectedDataLinkedService"}
_ARM_ENVELOPE = {"name", "properties", "id", "type", "etag", "apiVersion"}


def _issue(code: str, message: str, path: str | None = None, severity: AdfIssueSeverity = AdfIssueSeverity.warning):
    return AdfParseIssue(code=code, severity=severity, message=message, property_path=path)


def parse_dataflow(data: Any, path: str) -> AdfDataFlow:
    """Parse one data flow definition. Never raises."""
    if not isinstance(data, dict):
        return AdfDataFlow(name=Path(path).stem, path=path, parse_status=DiscoveryStatus.failed,
                           note="Top-level JSON is not an object.",
                           issues=[_issue("invalid_dataflow_json", "Top-level JSON is not an object.",
                                          severity=AdfIssueSeverity.error)])
    issues: list[AdfParseIssue] = []
    name = data.get("name") if isinstance(data.get("name"), str) and data.get("name") else ""
    if not name:
        name = Path(path).stem
        issues.append(_issue("missing_dataflow_name", f"No 'name'; using the file name '{name}'.", "name"))
    props = data.get("properties")
    if not isinstance(props, dict):
        issues.append(_issue("missing_properties", "No 'properties' object; the data flow has no readable definition.",
                             "properties", AdfIssueSeverity.error))
        props = {}
    san = _Sanitizer()

    flow_type = props.get("type") if isinstance(props.get("type"), str) and props.get("type") else None
    if flow_type is None:
        issues.append(_issue("missing_dataflow_type", "properties.type is absent.", "properties.type",
                             AdfIssueSeverity.error))
    elif flow_type not in FLOW_TYPES:
        issues.append(_issue("unknown_dataflow_type",
                             f"Data flow type '{flow_type}' is not one of {', '.join(FLOW_TYPES)}; its content is "
                             "preserved but not interpreted.", "properties.type"))

    tp_raw = props.get("typeProperties")
    if tp_raw is not None and not isinstance(tp_raw, dict):
        issues.append(_issue("invalid_type_properties", "'typeProperties' is not an object.",
                             "properties.typeProperties", AdfIssueSeverity.error))
    tp = tp_raw if isinstance(tp_raw, dict) else {}

    unknown: dict[str, Any] = {}
    for k, v in props.items():
        if k not in _KNOWN_PROPERTIES:
            unknown[f"properties.{k}"] = san.clean(v, k)
    for k, v in tp.items():
        if k not in _KNOWN_TYPE_PROPERTIES:
            unknown[f"typeProperties.{k}"] = san.clean(v, k)
    for k, v in data.items():
        if k not in _ARM_ENVELOPE:
            unknown[f"resource.{k}"] = san.clean(v, k)

    nodes: dict[str, list[AdfDataFlowNode]] = {"source": [], "sink": [], "transformation": []}
    for key, role in _NODE_LISTS:
        if key not in tp:
            continue
        items = tp[key]
        base = f"properties.typeProperties.{key}"
        if not isinstance(items, list):
            issues.append(_issue("invalid_nodes", f"'{key}' is not a list.", base, AdfIssueSeverity.error))
            continue
        for i, item in enumerate(items):
            pointer = f"{base}[{i}]"
            if not isinstance(item, dict):
                issues.append(_issue("invalid_node", f"An entry of '{key}' is not an object; recorded, not parsed.",
                                     pointer, AdfIssueSeverity.error))
                continue
            node_name = item.get("name") if isinstance(item.get("name"), str) else ""
            if not node_name:
                issues.append(_issue("missing_node_name", f"A {role} has no 'name'.", pointer, AdfIssueSeverity.error))
            script = item.get("script") if isinstance(item.get("script"), str) else None
            nodes[role].append(AdfDataFlowNode(
                role=role, name=node_name,
                description=item.get("description") if isinstance(item.get("description"), str) else None,
                dataset=_ref_name(item.get("dataset")), linked_service=_ref_name(item.get("linkedService")),
                flowlet=_ref_name(item.get("flowlet")),
                schema_linked_service=_ref_name(item.get("schemaLinkedService")),
                rejected_data_linked_service=_ref_name(item.get("rejectedDataLinkedService")),
                has_script=bool(script and script.strip()), pointer=pointer))
            if script is not None and not script.strip():
                issues.append(_issue("empty_script", f"The {role} '{node_name}' has an empty script.", pointer))
            for k, v in item.items():
                if k not in _KNOWN_NODE_KEYS:
                    unknown[f"{key}[{i}].{k}"] = san.clean(v, k)

    seen: dict[str, str] = {}
    for role in ("source", "sink", "transformation"):
        for n in nodes[role]:
            if not n.name:
                continue
            low = n.name.lower()
            if low in seen:
                issues.append(_issue("duplicate_node_name",
                                     f"Name '{n.name}' is used by a {seen[low]} and a {role}.", n.pointer,
                                     AdfIssueSeverity.error))
            else:
                seen[low] = role

    script_lines = tp.get("scriptLines")
    script = tp.get("script")
    if isinstance(script_lines, list):
        line_count = len(script_lines)
    elif isinstance(script, str) and script.strip():
        line_count = script.count("\n") + 1
    else:
        line_count = 0
    has_script = line_count > 0
    if flow_type in ("MappingDataFlow", "Flowlet") and not has_script:
        issues.append(_issue("no_script", "The data flow has no script / scriptLines, so its transformation logic "
                                          "cannot be described.", "properties.typeProperties"))
    if flow_type == "MappingDataFlow" and not nodes["source"]:
        issues.append(_issue("no_sources", "A mapping data flow lists no sources.", "properties.typeProperties.sources"))
    if flow_type == "MappingDataFlow" and not nodes["sink"]:
        issues.append(_issue("no_sinks", "A mapping data flow lists no sinks.", "properties.typeProperties.sinks"))

    every = [n for role in nodes.values() for n in role]
    folder = props.get("folder")
    return AdfDataFlow(
        name=name, path=path, flow_type=flow_type,
        folder=folder.get("name") if isinstance(folder, dict) and isinstance(folder.get("name"), str) else None,
        description=props.get("description") if isinstance(props.get("description"), str) else None,
        annotations=[a for a in props.get("annotations") or [] if isinstance(a, str)]
        if isinstance(props.get("annotations"), list) else [],
        sources=nodes["source"], sinks=nodes["sink"], transformations=nodes["transformation"],
        has_script=has_script, script_line_count=line_count,
        document_locale=tp.get("documentLocale") if isinstance(tp.get("documentLocale"), str) else None,
        dataset_refs=sorted({n.dataset for n in every if n.dataset}),
        linked_service_refs=sorted({r for n in every for r in (n.linked_service, n.schema_linked_service,
                                                                n.rejected_data_linked_service) if r}),
        flowlet_refs=sorted({n.flowlet for n in every if n.flowlet}),
        unknown_properties=unknown, issues=sorted(issues, key=lambda i: (i.severity.value, i.code, i.message)),
        source_location=AdfSourceLocation(file=path, pointer="properties"))

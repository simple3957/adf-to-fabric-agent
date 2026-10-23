"""The migration consultation: the questions agreed with the client after discovery, before anything moves.

Readiness (``agent.py``) asks "can we start?". This asks "how should the migration happen?", and turns the answers
into a versioned brief that, once approved, sets what the run does. It follows three rules:

* A question about a workload or feature is asked only when discovery found it (``when``). Nothing is asked about
  what the estate does not have.
* What the configuration already says (the Fabric workspace chosen, a lakehouse bound, the Invoke-pipeline connection)
  is read, not asked (``resolve``).
* Options are limited to what Fabric and this tool can actually do, and every question says whether its answer drives
  the run (``run``), makes the agent stop for a person (``gate``), or is only recorded in the brief (``record``).

Every recommendation is computed from discovered counts and says why. A recommendation is never an answer: it is used
where nothing was answered and is shown as such, and accepting it records it as an answer. Deterministic, no LLM.
Secrets are never asked for: credentials are referenced by environment-variable name elsewhere.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Union

AnswerValue = Union[str, list[str]]
Answers = dict[str, AnswerValue]

EFFECT_LABEL = {"run": "Drives the run", "gate": "Agent stops for a person", "record": "Recorded in the brief"}

NOTEBOOK_TYPES = ("DatabricksNotebook", "DatabricksSparkPython", "DatabricksSparkJar", "SynapseNotebook", "SparkJob")
DATAFLOW_TYPES = ("ExecuteDataFlow", "ExecuteWranglingDataflow")
EVENT_TRIGGERS = ("BlobEventsTrigger", "CustomEventsTrigger")

#: What the consultation may ask about but discovery cannot see, so it is never inferred.
NOT_RECORDED = ["private endpoints and firewall rules", "data volumes and row counts", "items already in the Fabric workspace",
                "the Fabric capacity size actually needed", "who approves production changes"]


# =====================================================================================================================
# facts: what discovery and the configuration say, never guessed (0 = discovery found none, None = not known)
# =====================================================================================================================


@dataclass
class Facts:
    discovered: bool = False
    pipelines: list[str] = field(default_factory=list)
    activities: dict[str, int] = field(default_factory=dict)
    linked_services: int = 0
    linked_service_types: dict[str, int] = field(default_factory=dict)
    self_hosted: int = 0  # linked services through a self-hosted integration runtime
    key_vault: int = 0  # linked services reading secrets from Azure Key Vault
    ls_params_no_default: int = 0  # linked-service parameters with no value
    triggers: dict[str, int] = field(default_factory=dict)  # by type
    triggers_started: int = 0
    dataflows: int = 0
    file_datasets: int = 0  # ADLS / storage file datasets (storage map)
    storage_undecided: int = 0  # file datasets the storage map cannot map
    shortcuts_planned: int = 0
    pipeline_waves: list[list[str]] = field(default_factory=list)  # dependency waves holding pipelines, in order
    blocked_activities: Optional[int] = None  # activities whose type has no automatic Fabric route (conversion rules)
    blocked_pipelines: list[str] = field(default_factory=list)
    reconstruction_proposals: Optional[int] = None  # safe proposals; None before reconstruction
    notebooks_with_storage_paths: Optional[int] = None  # from the workload manifest; None before assessment
    notebooks_with_secrets: Optional[int] = None
    fabric_workspace: Optional[str] = None  # the destination chosen on the Connections step
    lakehouse_bound: Optional[str] = None  # a lakehouse bound in the resolutions
    invoke_connection_bound: bool = False
    not_recorded: list[str] = field(default_factory=lambda: list(NOT_RECORDED))

    @property
    def notebook_activities(self) -> int:
        return sum(self.activities.get(t, 0) for t in NOTEBOOK_TYPES)

    @property
    def event_triggers(self) -> int:
        return sum(self.triggers.get(t, 0) for t in EVENT_TRIGGERS)

    @property
    def schedule_triggers(self) -> int:
        return self.triggers.get("ScheduleTrigger", 0) + self.triggers.get("TumblingWindowTrigger", 0)

    def to_dict(self) -> dict[str, Any]:
        out = {k: v for k, v in self.__dict__.items()}
        out.update(notebook_activities=self.notebook_activities, event_triggers=self.event_triggers, schedule_triggers=self.schedule_triggers)
        return out


def read_facts(store: Any, pid: str, destination: Optional[dict] = None) -> Facts:
    """Facts from the project's own records. Each source is read on its own: one unreadable record hides nothing else."""
    from adf2fabric.agent.discovery.adf_semantic.pipeline_analysis import walk

    f = Facts(fabric_workspace=(destination or {}).get("workspace_name") or (destination or {}).get("workspace_id"))
    if not store.has_adf_semantic_discovery(pid):
        return f
    d = store.load_adf_semantic_discovery(pid)
    f.discovered = True
    f.pipelines = sorted(p.name for p in d.pipelines)
    f.activities = dict(Counter(a.type for p in d.pipelines for a in walk(p.activities) if a.type))
    f.linked_services = len(d.linked_services)
    f.linked_service_types = dict(Counter(ls.type or "?" for ls in d.linked_services))
    f.self_hosted = sum(1 for ls in d.linked_services if ls.integration_runtime_kind == "self_hosted")
    f.key_vault = sum(1 for ls in d.linked_services if ls.key_vault_refs)
    f.ls_params_no_default = sum(1 for ls in d.linked_services for p in ls.parameters if not p.has_default)
    f.triggers = dict(Counter(t.type or "?" for t in d.triggers))
    f.triggers_started = sum(1 for t in d.triggers if (t.runtime_state or "").lower() == "started")
    f.dataflows = max(len(getattr(d, "dataflows", []) or []), sum(f.activities.get(t, 0) for t in DATAFLOW_TYPES))
    try:
        from adf2fabric.agent.storage.mapping import build_storage_map

        sm = build_storage_map(d)
        f.file_datasets, f.storage_undecided, f.shortcuts_planned = sm.summary["file_datasets"], sm.summary["needs_decision"], sm.summary["shortcuts"]
    except Exception:  # noqa: BLE001 - the storage questions are then not asked, never guessed
        pass
    try:
        if store.has_dependency_inventory(pid):
            inv = store.load_dependency_inventory(pid)
            f.pipeline_waves = [list(w.items) for w in inv.waves if w.kind == "pipelines" and w.items]
    except Exception:  # noqa: BLE001
        pass
    try:  # from the conversion rules by activity type, over the whole estate: not from the last conversion (which may cover a subset)
        import adf2fabric.agent.conversion.engine  # noqa: F401 - registers every handler
        from adf2fabric.agent.conversion.registry import rule_for
        from adf2fabric.project.conversion import ConversionStrategy

        def no_route(t: Optional[str]) -> bool:
            rule = rule_for(t)
            return rule is None or rule.strategy == ConversionStrategy.manual

        per = {p.name: sum(1 for a in walk(p.activities) if a.type and no_route(a.type)) for p in d.pipelines}
        f.blocked_activities = sum(per.values())
        f.blocked_pipelines = sorted(n for n, k in per.items() if k)
    except Exception:  # noqa: BLE001
        pass
    try:
        if store.has_reconstruction(pid):
            f.reconstruction_proposals = int(store.load_reconstruction(pid).safe_proposal_count or 0)
    except Exception:  # noqa: BLE001
        pass
    try:
        if store.has_workload_manifest(pid):
            ws = store.load_workload_manifest(pid).workloads
            f.notebooks_with_storage_paths = sum(1 for w in ws if any(x.code in ("adls_direct_path", "blob_direct_path") for x in w.findings))
            f.notebooks_with_secrets = sum(1 for w in ws if any("secret" in x.code for x in w.findings))
    except Exception:  # noqa: BLE001
        pass
    try:
        res = store.conversion_dir(pid) / "resolutions.json"
        entries = json.loads(res.read_text(encoding="utf-8")) if res.exists() else []
        for e in entries if isinstance(entries, list) else []:
            key = str(e.get("key", "")).lower() if isinstance(e, dict) else ""
            if key.startswith("lakehouse:"):
                f.lakehouse_bound = str(e["key"]).split(":", 1)[1]
            if key == "connection:invoke-pipeline":
                f.invoke_connection_bound = True
    except Exception:  # noqa: BLE001
        pass
    return f


def plural(n: int, noun: str, many: Optional[str] = None) -> str:
    return f"{n:,} {noun if n == 1 else (many or noun + 's')}"


# =====================================================================================================================
# the catalog
# =====================================================================================================================


@dataclass
class Option:
    value: str
    label: str
    description: str = ""


@dataclass
class Recommendation:
    value: AnswerValue
    reason: str


@dataclass
class Question:
    id: str
    topic: str
    text: str
    help: str
    kind: str  # single | multi | text
    required: bool
    effect: str  # run | gate | record
    applies: str  # what the answer changes, in words
    options: Callable[[Facts], list[Option]] = lambda f: []
    when: Optional[Callable[[Facts, Answers], bool]] = None
    resolve: Optional[Callable[[Facts], Optional[Recommendation]]] = None
    recommend: Optional[Callable[[Facts, Answers], Optional[Recommendation]]] = None
    placeholder: str = ""


TOPICS = [
    ("scope", "Scope and approach", "What moves, in what order, and how much risk the client accepts."),
    ("target", "Target in Fabric", "Where it lands. Settings the configuration already holds are read, not asked."),
    ("storage", "Storage and OneLake", "Asked because discovery found file datasets on storage."),
    ("compute", "Notebooks and Spark", "Asked because discovery found notebook or Spark activities."),
    ("pipelines", "Pipelines and triggers", "Asked for what discovery found in the pipelines."),
    ("connections", "Connections", "Asked because discovery found linked services."),
    ("security", "Security and networking", "Access, networks and sensitive data. Nothing here weakens a control."),
    ("validation", "Validation and cutover", "When the migration counts as successful, and who says so."),
    ("context", "Context (notes only)", "Who signs off, and anything the team should know."),
]
TOPIC_TITLE = {t[0]: t[1] for t in TOPICS}

O = Option
R = Recommendation


def _waves(f: Facts) -> int:
    return len(f.pipeline_waves)


QUESTIONS: list[Question] = [
    # ---- scope -------------------------------------------------------------------------------------------------------
    Question("scope.mode", "scope", "What is the migration scope?",
             "A pilot moves the first wave of pipelines (those no other pipeline calls first), so the approach is proven before the rest.",
             "single", True, "run", "Which pipelines conversion and deployment take.",
             options=lambda f: [O("all", "Migrate every discovered pipeline", plural(len(f.pipelines), "pipeline") + "."),
                                O("selected", "Only selected pipelines", "Choose them on the next question."),
                                O("pilot", "Pilot: the first wave of pipelines only",
                                  f"{plural(len(f.pipeline_waves[0]), 'pipeline')} in wave 1 of {len(f.pipeline_waves)}." if f.pipeline_waves
                                  else "Needs the dependency inventory (Review step).")],
             when=lambda f, a: len(f.pipelines) > 0,
             recommend=lambda f, a: R("pilot", f"{plural(len(f.pipelines), 'pipeline')} in {plural(_waves(f), 'wave')}: prove the approach on the first wave first.")
             if (len(f.pipelines) > 50 or _waves(f) > 4) and f.pipeline_waves
             else R("all", f"{plural(len(f.pipelines), 'pipeline')} is a size one migration handles, children before callers.")),
    Question("scope.pipelines", "scope", "Which pipelines should this migration move?",
             "Only discovered pipelines are offered. A pipeline that calls another needs that one too.",
             "multi", True, "run", "The pipelines conversion and deployment take.",
             options=lambda f: [O(p, p) for p in f.pipelines],
             when=lambda f, a: a.get("scope.mode") == "selected",
             recommend=lambda f, a: R(list(f.pipelines), "Every discovered pipeline; untick what this migration leaves out.")),
    Question("scope.approach", "scope", "What is the preferred migration approach?",
             "Deployment always goes children first. Cutting over wave by wave, or running ADF and Fabric side by side, is organised by the team.",
             "single", True, "record", "The brief and the cutover plan.",
             options=lambda f: [O("bigbang", "Big bang", "Cut over everything in scope together."),
                                O("phased", "Phased", "Cut over in dependency waves."),
                                O("parallel", "Parallel", "Run ADF and Fabric together before cutover.")],
             recommend=lambda f, a: R("phased", f"{plural(_waves(f), 'wave')} of pipelines: cutting over wave by wave limits what one problem can stop.")
             if _waves(f) > 2 else R("bigbang", "Few waves: one cutover is simpler to run and to roll back.")),
    Question("scope.downtime", "scope", "What downtime is acceptable during cutover?", "Decides when triggers switch from ADF to Fabric.",
             "single", False, "record", "The brief and the cutover plan.",
             options=lambda f: [O("zero", "Near-zero downtime"), O("1h", "Less than 1 hour"), O("4h", "1-4 hours"),
                                O("window", "A maintenance window is available")]),
    Question("scope.environment", "scope", "Which environment should be migrated first?", "The connected factory is the environment this migration reads.",
             "single", True, "record", "The brief.",
             options=lambda f: [O("devtest", "Development / test first, then production"), O("pilot", "A controlled production pilot"),
                                O("single", "Single environment only")],
             recommend=lambda f, a: R("devtest", "Proving the migration on development first keeps production untouched until it works.")),
    Question("scope.rollback", "scope", "What happens if migration or validation fails?",
             "This tool never changes or deletes anything in ADF and never deletes in Fabric, so ADF stays a working fallback until cutover.",
             "single", True, "record", "The brief.",
             options=lambda f: [O("fallback", "Keep ADF as the fallback until validation passes"), O("repair", "Rerun or repair the migration"),
                                O("formal", "Define formal rollback steps", "Recorded for the team to write.")],
             recommend=lambda f, a: R("fallback", "ADF is untouched by the migration, so it is the simplest rollback.")),

    # ---- target ------------------------------------------------------------------------------------------------------
    Question("target.workspace", "target", "Which Fabric workspace should receive the migrated items?",
             "The run writes only to the workspace chosen on the Connections step. Creating a workspace is an approved provisioning step.",
             "single", True, "gate", "The agent waits until a Fabric workspace is chosen.",
             options=lambda f: [O("existing", "An existing workspace", "Choose it on the Connections step before the run."),
                                O("new", "A new workspace", "Planned and approved on the Plan step first.")],
             resolve=lambda f: R(f.fabric_workspace, "The workspace chosen on the Connections step.") if f.fabric_workspace else None),
    Question("target.capacity", "target", "Which Fabric capacity should support the workload?",
             "A workspace needs a Fabric capacity before items can be created; assigning one needs a capacity administrator.",
             "single", True, "gate", "A prerequisite a person confirms before the run.",
             options=lambda f: [O("assigned", "The workspace already has a capacity"), O("existing", "Assign an existing capacity"),
                                O("provision", "Provision an approved capacity"), O("assess", "Assess the capacity needed first")]),
    Question("target.existing", "target", "If a pipeline of the same name already exists in the workspace, what should happen?",
             "Deploy updates an existing pipeline in place (same item, new definition). It never deletes anything.",
             "single", True, "gate", "Whether deploy may update existing pipelines without a person checking first.",
             options=lambda f: [O("update", "Update it from ADF", "What deploy does; changes made in Fabric are overwritten."),
                                O("check", "Stop and let a person check first", "The dry run lists every update before it happens.")],
             when=lambda f, a: len(f.pipelines) > 0,
             recommend=lambda f, a: R("check", "The dry run shows each update; a person confirms nothing made by hand in Fabric is lost.")),
    Question("target.git", "target", "After deployment, should the migrated items be committed to the workspace's Git branch?",
             "Only when the workspace is connected to Git. Commits are selective (the migration's items), never deletions, dry run first.",
             "single", True, "run", "Whether git-commit is part of the run.",
             options=lambda f: [O("commit", "Commit after deployment"), O("off", "Don't commit", "Commit later by hand.")],
             recommend=lambda f, a: R("off", "Whether the workspace is connected to Git is checked at commit time; commit once it is.")),
    Question("target.environments", "target", "Are separate development, test and production workspaces required?",
             "With Fabric Git integration, what the run creates in development is promoted through pull requests.",
             "single", True, "record", "The brief.",
             options=lambda f: [O("separate", "Yes, separate workspaces"), O("single", "No, one workspace"),
                                O("existing", "Follow the existing workspace strategy")],
             recommend=lambda f, a: R("existing", "Follow the strategy already in place unless the client asks for a change.")),
    Question("target.names", "target", "Should pipeline and activity names be preserved?",
             "The run keeps every name. Renaming afterwards means every reference must follow.",
             "single", True, "record", "The brief.",
             options=lambda f: [O("preserve", "Preserve names", "What the run does."), O("standards", "Apply naming standards afterwards")],
             recommend=lambda f, a: R("preserve", "Keeps every caller, trigger and report that names a pipeline working.")),

    # ---- storage -----------------------------------------------------------------------------------------------------
    Question("storage.route", "storage", "How should pipelines reach the files they read and write?",
             "Repointing reads and writes a lakehouse through OneLake shortcuts to the same storage (CSV and Parquet with Copy, Lookup and "
             "GetMetadata, live-verified); the rest stays on its connection. Keeping connections reads the original storage directly.",
             "single", True, "run", "Whether conversion repoints datasets to the lakehouse (convert --lakehouse).",
             options=lambda f: [O("lakehouse", "Repoint to a lakehouse through shortcuts", f"{plural(f.shortcuts_planned, 'shortcut')} planned."),
                                O("connections", "Keep Fabric connections to the original storage")],
             when=lambda f, a: f.file_datasets > 0,
             recommend=lambda f, a: R("lakehouse", f"{plural(f.file_datasets, 'file dataset')} on storage; shortcuts keep the data in place and governed in OneLake.")
             if f.shortcuts_planned else R("connections", "No shortcut can be planned from what discovery found.")),
    Question("storage.lakehouse", "storage", "Which lakehouse holds the shortcuts?",
             "A lakehouse name is never invented. Bind its id in the resolutions (ItemToId lakehouse:<name>) or create it on the Plan step.",
             "text", True, "run", "The lakehouse conversion repoints to.", placeholder="e.g. LH_Migration",
             when=lambda f, a: a.get("storage.route") == "lakehouse",
             resolve=lambda f: R(f.lakehouse_bound, "A lakehouse already bound in the resolutions.") if f.lakehouse_bound else None),
    Question("storage.undecided", "storage", "Some file datasets cannot be mapped to a shortcut. What should happen to them?",
             "The storage map lists them with the reason: Blob, S3, SFTP, file shares, ADLS Gen1 or a dynamic container.",
             "single", True, "gate", "Whether the agent stops until a person decides where that data lives.",
             options=lambda f: [O("decide", "A person decides each before the run", f"{plural(f.storage_undecided, 'dataset')}."),
                                O("keep", "Leave them on their connections")],
             when=lambda f, a: f.storage_undecided > 0,
             recommend=lambda f, a: R("keep", "They keep working on their connections; a person can move them later.")),
    Question("storage.tables", "storage", "Should files also be loaded into lakehouse tables (Delta)?",
             "This tool never copies data. Loading files into tables is a separate piece of work the brief records.",
             "single", False, "record", "The brief.",
             options=lambda f: [O("no", "No, keep them as files"), O("later", "Yes, as a follow-up")],
             when=lambda f, a: f.file_datasets > 0,
             recommend=lambda f, a: R("no", "A like-for-like migration keeps files as files.")),

    # ---- compute -----------------------------------------------------------------------------------------------------
    Question("compute.engine", "compute", "Where should the notebooks and Spark jobs run after migration?",
             "By default the Fabric pipeline keeps calling Databricks / Synapse. Moving them to Fabric Spark generates Fabric notebooks, "
             "each approved before it is deployed and verified only by a real run.",
             "single", True, "run", "Whether Spark workloads are converted to Fabric (workloads-generate) or keep their source compute.",
             options=lambda f: [O("keep", "Keep the source compute (Databricks / Synapse)", "What conversion does by default."),
                                O("fabric", "Move them to Fabric Spark", "Approval per workload.")],
             when=lambda f, a: f.notebook_activities > 0,
             recommend=lambda f, a: R("keep", f"{plural(f.notebook_activities, 'notebook or Spark activity', 'notebook or Spark activities')}: keep them running where they work "
                                              "today, move them once the pipelines are proven.")),
    Question("compute.code", "compute", "May the agent read the notebook code?",
             "Code is read only with your authorisation: a token named by environment variable, your Azure CLI sign-in, or files you supply.",
             "single", True, "gate", "The agent waits for access to the code.",
             options=lambda f: [O("token", "Yes, with a token or Azure CLI sign-in"), O("files", "We supply the code as files")],
             when=lambda f, a: a.get("compute.engine") == "fabric"),
    Question("compute.paths", "compute", "Notebooks read storage directly. How should that access work in Fabric?",
             "Routing rewrites literal ADLS paths in the generated code to the planned OneLake shortcuts. Direct paths need the Fabric identity "
             "to have access to the storage account.",
             "single", True, "run", "Whether workload generation routes storage paths through shortcuts.",
             options=lambda f: [O("shortcuts", "Route through OneLake shortcuts"), O("direct", "Keep the direct paths", "An administrator grants access.")],
             when=lambda f, a: a.get("compute.engine") == "fabric" and bool(f.notebooks_with_storage_paths),
             recommend=lambda f, a: R("shortcuts", f"{plural(f.notebooks_with_storage_paths or 0, 'notebook')} read storage directly; the Fabric identity often cannot.")),
    Question("compute.secrets", "compute", "Notebooks read secrets. How should they be handled?",
             "Secret calls are rewritten to Key Vault only with a scope-to-vault map you provide; nothing is copied.",
             "single", True, "record", "The brief.",
             options=lambda f: [O("keyvault", "Map each secret scope to a Key Vault, with access for the Fabric identity"), O("review", "Review each notebook")],
             when=lambda f, a: a.get("compute.engine") == "fabric" and bool(f.notebooks_with_secrets),
             recommend=lambda f, a: R("keyvault", "The vault stays the single place secrets live.")),

    # ---- pipelines ---------------------------------------------------------------------------------------------------
    Question("pipelines.unsupported", "pipelines", "Some activities have no Fabric equivalent. How should their pipelines be handled?",
             "Conversion turns each into an explicit blocker. A person replaces it with a supported activity, a notebook or custom logic.",
             "single", True, "run", "Whether pipelines with blocked activities are deployed.",
             options=lambda f: [O("hold", "Hold those pipelines back until redesigned", ", ".join(f.blocked_pipelines[:5])),
                                O("flag", "Deploy the rest and flag them", "Deploy refuses a pipeline with a blocker anyway.")],
             when=lambda f, a: bool(f.blocked_activities),
             recommend=lambda f, a: R("hold", f"{plural(f.blocked_activities or 0, 'activity', 'activities')} in {plural(len(f.blocked_pipelines), 'pipeline')} cannot run as converted.")),
    Question("pipelines.reconstruct", "pipelines", "Discovery found incomplete pipelines. Should the agent's safe proposals be applied?",
             "Proposals fill in what the source leaves out; every inferred element is marked. Fixing the source instead keeps it the single truth.",
             "single", True, "run", "Whether conversion applies the safe reconstruction proposals.",
             options=lambda f: [O("apply", "Apply the safe proposals", "Every inferred element is marked."), O("source", "Fix them in the ADF source")],
             when=lambda f, a: bool(f.reconstruction_proposals),
             recommend=lambda f, a: R("source", "A fixed source keeps ADF and Fabric the same; proposals are a fallback.")),
    Question("pipelines.dataflows", "pipelines", "Mapping data flows were found. How should they be handled?",
             "Fabric has no mapping data flows. Conversion generates a placeholder activity only.",
             "single", True, "record", "The brief.",
             options=lambda f: [O("notebooks", "Rebuild them as Fabric notebooks"), O("gen2", "Rebuild them as Dataflow Gen2"),
                                O("adf", "Keep them in ADF for now")],
             when=lambda f, a: f.dataflows > 0,
             recommend=lambda f, a: R("notebooks", f"{plural(f.dataflows, 'data flow')}: Spark notebooks carry their transformation logic most directly.")),
    Question("pipelines.triggers", "pipelines", "How should schedules be recreated?",
             "Schedule triggers become Fabric pipeline schedules, created switched off so nothing runs twice while ADF still runs.",
             "single", True, "run", "Whether deploy creates the schedules.",
             options=lambda f: [O("disabled", "Recreate them switched off until cutover", f"{plural(f.schedule_triggers, 'schedule trigger')}."),
                                O("skip", "Leave them out", "Created by hand later.")],
             when=lambda f, a: f.schedule_triggers > 0,
             recommend=lambda f, a: R("disabled", f"{plural(f.schedule_triggers, 'trigger')}{f', {f.triggers_started} running in ADF today' if f.triggers_started else ''}: "
                                                  "the timings are kept without running anything early.")),
    Question("pipelines.events", "pipelines", "Event triggers were found. How should they be recreated?",
             "Fabric runs pipelines on storage events through Activator rules, set up by hand. They are not converted.",
             "single", True, "record", "The brief.",
             options=lambda f: [O("activator", "Recreate them as Activator rules by hand"), O("schedule", "Replace them with schedules"),
                                O("adf", "Keep them in ADF for now")],
             when=lambda f, a: f.event_triggers > 0,
             recommend=lambda f, a: R("activator", f"{plural(f.event_triggers, 'event trigger')}: closest to how they behave today.")),
    Question("pipelines.invoke", "pipelines", "Pipelines call other pipelines. Which connection should Fabric's Invoke pipeline step use?",
             "Every Execute pipeline step needs a Fabric Data Pipelines connection (OAuth), created by a person once.",
             "single", True, "gate", "The agent waits until that connection is bound.",
             options=lambda f: [O("create", "A person creates it and binds its id"), O("exists", "It exists; bind its id")],
             when=lambda f, a: f.activities.get("ExecutePipeline", 0) > 0,
             resolve=lambda f: R("Bound in the resolutions", "The Invoke pipeline connection is already bound.") if f.invoke_connection_bound else None),

    # ---- connections -------------------------------------------------------------------------------------------------
    Question("connections.credentials", "connections", "Linked services must become Fabric connections. How are their credentials provided?",
             "ADF never gives up stored secrets. A person chooses each sign-in method on the Connect step; a secret is named by environment "
             "variable and never stored.",
             "single", True, "gate", "The agent waits until every connection is bound or skipped.",
             options=lambda f: [O("person", "A person sets up each connection before the run"),
                                O("existing", "Reuse existing Fabric connections", "Bind their ids on the Connect step."),
                                O("skip", "Leave connections out", "Pipelines that need them are refused at deploy.")],
             when=lambda f, a: f.linked_services > 0,
             recommend=lambda f, a: R("person", f"{plural(f.linked_services, 'linked service')}; pipelines cannot run without their connections.")),
    Question("connections.gateway", "connections", "Some linked services use a self-hosted integration runtime. How should Fabric reach them?",
             "Fabric reaches private systems through an on-premises data gateway, installed by an administrator. The agent never sets one up.",
             "single", True, "gate", "A prerequisite a person confirms before the run.",
             options=lambda f: [O("gateway", "An administrator sets up a data gateway first"), O("exclude", "Leave them out until access is approved")],
             when=lambda f, a: f.self_hosted > 0,
             recommend=lambda f, a: R("gateway", f"{plural(f.self_hosted, 'linked service')} use a self-hosted runtime.")),
    Question("connections.keyvault", "connections", "Some linked services read secrets from Azure Key Vault. How should Fabric get them?",
             "A Fabric connection can use a Key Vault reference, or a person types the secret once.",
             "single", True, "record", "The brief.",
             options=lambda f: [O("reference", "Use Key Vault references"), O("typed", "A person enters the secrets once")],
             when=lambda f, a: f.key_vault > 0,
             recommend=lambda f, a: R("reference", f"{plural(f.key_vault, 'linked service')}: the vault stays the only place the secret lives.")),
    Question("connections.parameters", "connections", "Linked-service parameters have no value. Who provides them?",
             "Fabric connections are not parameterised: each needs a fixed value.",
             "single", True, "gate", "The agent waits for the values.",
             options=lambda f: [O("person", "A person gives each value before the run")],
             when=lambda f, a: f.ls_params_no_default > 0,
             recommend=lambda f, a: R("person", f"{plural(f.ls_params_no_default, 'parameter')} without a value.")),

    # ---- security ----------------------------------------------------------------------------------------------------
    Question("security.network", "security", "Does the source use private networking or firewall restrictions?",
             "Discovery cannot see private endpoints or firewall rules. Where Fabric must reach a private network, an administrator configures it.",
             "single", True, "gate", "A connectivity check is a prerequisite (Yes or Unknown).",
             options=lambda f: [O("yes", "Yes"), O("no", "No"), O("unknown", "Unknown: check connectivity first")],
             recommend=lambda f, a: R("unknown", "Discovery cannot see network rules, so a connectivity check is the safe default.")),
    Question("security.sensitive", "security", "Are there restrictions on what Fabric may read or write?",
             "Pipelines move data when they run; validation by real runs reads sources and writes targets.",
             "single", True, "run", "Whether validation may start real runs.",
             options=lambda f: [O("none", "No additional restrictions"), O("rules", "Approved data classification rules apply"),
                                O("blocked", "No real runs until a data owner approves")],
             recommend=lambda f, a: R("none", "No restriction was stated.")),
    Question("security.audit", "security", "What evidence must the migration produce?",
             "Produced either way: the audit log, the conversion and validation reports and the migration report.",
             "multi", False, "record", "The brief.",
             options=lambda f: [O("audit", "Audit log"), O("mapping", "Source-target mapping"), O("validation", "Validation report"),
                                O("exceptions", "Exception report")],
             recommend=lambda f, a: R(["audit", "mapping", "validation", "exceptions"], "All of these come from the run.")),

    # ---- validation --------------------------------------------------------------------------------------------------
    Question("validation.depth", "validation", "What level of validation is required?",
             "Static checks re-read source and generated files. API and deployment checks read the workspace. Run evidence compares recorded "
             "Fabric runs with ADF. Running pipelines starts real work in Fabric. Performance is not measured.",
             "single", True, "run", "Which validation stages run, and whether real runs may be started.",
             options=lambda f: [O("static", "Static checks only", "Nothing is read from Fabric."),
                                O("deployed", "Static, API and deployment checks", "Read-only."),
                                O("evidence", "Also compare recorded runs with ADF", "Read-only."),
                                O("run", "Also run chosen pipelines in Fabric and compare", "Real runs: they read and write data.")],
             recommend=lambda f, a: R("evidence", "Proves the pipelines with runs that already happened, without starting anything that writes data.")),
    Question("validation.tolerance", "validation", "What reconciliation tolerance is acceptable?",
             "Run comparison checks status, Copy row counts and Lookup results exactly.", "single", False, "record", "The brief.",
             options=lambda f: [O("exact", "Exact match"), O("numeric", "An approved numeric tolerance"), O("business", "A business-defined tolerance")],
             recommend=lambda f, a: R("exact", "A like-for-like migration should match exactly.")),
    Question("validation.gate", "validation", "Must every migrated pipeline pass validation before cutover?",
             "Only a recorded real run that matches ADF makes a pipeline runtime-validated.", "single", True, "gate",
             "Whether the agent stops on a mismatch until a person decides.",
             options=lambda f: [O("all", "Yes, every pipeline must pass"), O("exceptions", "Allow approved exceptions"),
                                O("critical", "Only critical pipelines must pass")],
             recommend=lambda f, a: R("exceptions", "Nothing is cut over unseen, without blocking on a mismatch a person can explain.")),
    Question("validation.source", "validation", "What should happen to the ADF factory after migration?",
             "The migration never changes or disables ADF; decommissioning is a separate, approved step.", "single", True, "record", "The brief.",
             options=lambda f: [O("temporary", "Retain it temporarily"), O("permanent", "Retain it permanently"),
                                O("decommission", "Decommission after formal approval")],
             recommend=lambda f, a: R("temporary", "Keeps the fallback until Fabric has run in production for a while.")),

    # ---- context -----------------------------------------------------------------------------------------------------
    Question("context.owner", "context", "Who approves production cutover for the client?", "Named on the brief. Only a person can answer this.",
             "text", True, "record", "The brief.", placeholder="Name and role, e.g. Priya Shah, data platform lead"),
    Question("context.notes", "context", "Anything else the team should know?", "Kept with the brief. The agent does not act on it.",
             "text", False, "record", "The brief only.", placeholder="e.g. cut over on a Sunday; finance reports must keep working"),
]
QUESTION_BY_ID = {q.id: q for q in QUESTIONS}


# =====================================================================================================================
# resolving the answers, the settings they produce, and the brief
# =====================================================================================================================


@dataclass
class Resolved:
    q: Question
    applies: bool
    value: Optional[AnswerValue]
    source: str  # config | person | recommendation | open
    rec: Optional[Recommendation]


def _blank(v: Any) -> bool:
    return v is None or (isinstance(v, str) and not v.strip())


def resolve(facts: Facts, given: Answers) -> list[Resolved]:
    """Each question with the answer the brief uses: the configuration's, else the person's, else the recommendation.

    Questions resolve in order, so a later ``when`` sees the earlier answers."""
    effective: Answers = {}
    out: list[Resolved] = []
    for q in QUESTIONS:
        applies = q.when(facts, effective) if q.when else True
        config = q.resolve(facts) if applies and q.resolve else None
        rec = config or (q.recommend(facts, effective) if applies and q.recommend else None)
        mine = given.get(q.id)
        has = config is None and not _blank(mine)
        value = config.value if config else mine if has else (rec.value if rec else None)
        if applies and value is not None:
            effective[q.id] = value
        source = "open" if not applies else "config" if config else "person" if has else "recommendation" if rec else "open"
        out.append(Resolved(q, applies, value if applies else None, source, rec))
    return out


def effective_answers(rs: list[Resolved]) -> Answers:
    return {r.q.id: r.value for r in rs if r.applies and r.value is not None}


def validate_answer(q: Question, facts: Facts, value: Any) -> AnswerValue:
    """The answer, checked against the question's own options (never an option the question does not offer)."""
    if q.kind == "text":
        if not isinstance(value, str) or len(value) > 2000:
            raise ValueError(f"'{q.id}' takes text of up to 2000 characters.")
        return value.strip()
    allowed = {o.value for o in q.options(facts)}
    if q.kind == "multi":
        if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
            raise ValueError(f"'{q.id}' takes a list of options.")
        bad = sorted(set(value) - allowed)
        if bad:
            raise ValueError(f"'{q.id}' does not offer {', '.join(bad[:5])}.")
        return list(dict.fromkeys(value))
    if not isinstance(value, str) or value not in allowed:
        raise ValueError(f"'{q.id}' takes one of: {', '.join(sorted(allowed))}.")
    return value


@dataclass
class Settings:
    """What the approved brief makes the agent do. Only these fields are read by the run; the rest of the brief is a record."""

    pipelines: Optional[list[str]]  # None = every pipeline
    lakehouse: Optional[str]  # repoint datasets to this lakehouse (convert --lakehouse)
    apply_reconstruction: bool
    hold_blocked: list[str]  # pipelines held back
    workloads_to_fabric: bool
    route_notebook_paths: bool
    schedules: bool
    git_commit: bool
    validate_stages: list[str]
    allow_real_runs: bool
    check_before_update: bool
    prerequisites: list[str]
    gates: list[str]

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


def _pick(a: Answers, qid: str, default: str) -> str:
    v = a.get(qid)
    return v if isinstance(v, str) and v else default


def settings_from(facts: Facts, rs: list[Resolved]) -> Settings:
    a = effective_answers(rs)
    mode = _pick(a, "scope.mode", "all")
    pipelines: Optional[list[str]] = None
    if mode == "selected" and isinstance(a.get("scope.pipelines"), list):
        pipelines = list(a["scope.pipelines"])  # type: ignore[arg-type]
    elif mode == "pilot" and facts.pipeline_waves:
        pipelines = list(facts.pipeline_waves[0])
    hold = list(facts.blocked_pipelines) if _pick(a, "pipelines.unsupported", "hold") == "hold" and facts.blocked_activities else []
    if hold:
        pipelines = [p for p in (pipelines if pipelines is not None else facts.pipelines) if p not in hold]
    depth = _pick(a, "validation.depth", "evidence")
    stages = {"static": ["static"], "deployed": ["static", "api", "deployment"]}.get(depth, ["static", "api", "deployment", "runtime"])
    real = depth == "run" and _pick(a, "security.sensitive", "none") != "blocked"
    to_fabric = _pick(a, "compute.engine", "keep") == "fabric"
    prereq: list[str] = []
    if a.get("target.workspace") == "new" or (not facts.fabric_workspace and a.get("target.workspace")):
        prereq.append("A Fabric workspace is chosen on the Connections step (a new one is planned and approved on the Plan step).")
    if a.get("target.capacity") in ("existing", "provision", "assess"):
        prereq.append("The target workspace is assigned a Fabric capacity.")
    if a.get("connections.gateway") == "gateway":
        prereq.append("An administrator installs and registers an on-premises data gateway for the self-hosted linked services.")
    if a.get("security.network") in ("yes", "unknown"):
        prereq.append("An administrator confirms Fabric can reach the source systems (gateway or managed private endpoints).")
    if a.get("pipelines.invoke") in ("create", "exists"):
        prereq.append("A Fabric Data Pipelines connection is bound for Invoke pipeline (ItemToId connection:invoke-pipeline).")
    if a.get("compute.code"):
        prereq.append("Access to the notebook code is given (a token named by environment variable, Azure CLI, or files).")
    gates = [f"{r.q.text} -> {answer_label(r.q, facts, r.value)}" for r in rs if r.applies and r.q.effect == "gate" and r.value is not None]
    return Settings(pipelines=pipelines, lakehouse=(str(a.get("storage.lakehouse")).strip() or None) if a.get("storage.route") == "lakehouse" and a.get("storage.lakehouse") else None,
                    apply_reconstruction=_pick(a, "pipelines.reconstruct", "source") == "apply", hold_blocked=hold,
                    workloads_to_fabric=to_fabric, route_notebook_paths=to_fabric and _pick(a, "compute.paths", "direct") == "shortcuts",
                    schedules=_pick(a, "pipelines.triggers", "disabled") == "disabled", git_commit=_pick(a, "target.git", "off") == "commit",
                    validate_stages=stages, allow_real_runs=real, check_before_update=_pick(a, "target.existing", "check") == "check",
                    prerequisites=prereq, gates=gates)


def conflicts(facts: Facts, rs: list[Resolved]) -> list[str]:
    """Answers that cannot all be carried out together."""
    a = effective_answers(rs)
    out: list[str] = []
    if a.get("validation.depth") == "run" and a.get("security.sensitive") == "blocked":
        out.append("Validation is set to start real runs, but real runs are blocked until a data owner approves: choose a read-only depth, or lift the restriction.")
    if a.get("scope.mode") == "selected" and not a.get("scope.pipelines"):
        out.append("The scope is 'selected pipelines' but none is selected.")
    if a.get("scope.mode") == "pilot" and not facts.pipeline_waves:
        out.append("A pilot needs the dependency waves: run 'Resolve dependencies' on the Review step first.")
    if a.get("storage.route") == "lakehouse" and not facts.shortcuts_planned:
        out.append("Repointing to a lakehouse needs at least one planned shortcut, and the storage map plans none.")
    if a.get("compute.engine") == "fabric" and a.get("compute.paths") == "shortcuts" and a.get("storage.route") != "lakehouse":
        out.append("Notebook paths are routed through shortcuts, but no lakehouse is chosen for them: repoint storage to a lakehouse too.")
    if a.get("scope.approach") == "parallel" and a.get("validation.source") == "decommission" and a.get("scope.rollback") == "fallback":
        out.append("ADF is both the fallback and to be decommissioned: say when the fallback ends.")
    return out


def counts(facts: Facts, rs: list[Resolved]) -> dict[str, int]:
    live = [r for r in rs if r.applies]
    from_config = sum(1 for r in live if r.source == "config")
    return {"applicable": len(live), "from_config": from_config, "answered": sum(1 for r in live if r.source == "person") + from_config,
            "assumed": sum(1 for r in live if r.source == "recommendation"), "need_person": sum(1 for r in live if r.q.required and r.source == "open"),
            "conflicts": len(conflicts(facts, rs))}


def fingerprint(rs: list[Resolved]) -> str:
    """Same answers (as the brief uses them), same fingerprint: tells whether an approved version still says what the answers say."""
    text = json.dumps([[r.q.id, r.value] for r in rs if r.applies], sort_keys=False, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def answer_label(q: Question, facts: Facts, v: Optional[AnswerValue]) -> str:
    if v is None or (isinstance(v, str) and not v.strip()):
        return "-"
    if q.kind == "text" or q.resolve and isinstance(v, str) and v not in {o.value for o in q.options(facts)}:
        return str(v).strip()
    labels = {o.value: o.label for o in q.options(facts)}
    if isinstance(v, list):
        return ", ".join(labels.get(x, x) for x in v) if v else "None"
    return labels.get(v, v)


def brief_markdown(facts: Facts, rs: list[Resolved], project: str, version: Optional[int], status: str) -> str:
    lines = [f"# Migration brief: {project}", "",
             f"Azure Data Factory to Microsoft Fabric{f' ({facts.fabric_workspace})' if facts.fabric_workspace else ''}. "
             f"{f'Version {version}, {status}.' if version else 'Draft, not yet submitted.'}",
             f"Discovery: {plural(len(facts.pipelines), 'pipeline')}, {plural(sum(facts.activities.values()), 'activity', 'activities')}, "
             f"{plural(facts.linked_services, 'linked service')}{f', {len(facts.pipeline_waves)} waves of pipelines' if facts.pipeline_waves else ''}.", ""]
    for tid, title, _ in TOPICS:
        items = [r for r in rs if r.applies and r.q.topic == tid]
        if not items:
            continue
        lines += [f"## {title}", ""]
        for r in items:
            how = {"config": " _(from the configuration)_", "recommendation": " _(recommendation)_", "open": " _(open)_"}.get(r.source, "")
            lines += [f"- **{r.q.text}**  ", f"  {answer_label(r.q, facts, r.value)}{how} · {EFFECT_LABEL[r.q.effect]}"]
        lines.append("")
    s = settings_from(facts, rs)
    if s.prerequisites:
        lines += ["## Prerequisites before the run", "", *[f"- {x}" for x in s.prerequisites], ""]
    c = conflicts(facts, rs)
    if c:
        lines += ["## Conflicts", "", *[f"- {x}" for x in c], ""]
    lines += ["## Not visible to discovery", "", ", ".join(facts.not_recorded).capitalize() + ": asked, never answered from discovery.", ""]
    return "\n".join(lines)


def questionnaire(facts: Facts, rs: list[Resolved]) -> dict[str, Any]:
    """The applicable questions as a JSON questionnaire to hand to the client (answers come back through ``import``)."""
    return {"format": "adf2fabric-consultation/1", "questions": [
        {"id": r.q.id, "topic": TOPIC_TITLE[r.q.topic], "text": r.q.text, "help": r.q.help, "kind": r.q.kind, "required": r.q.required,
         "options": [{"value": o.value, "label": o.label} for o in r.q.options(facts)], "recommended": r.rec.value if r.rec else None,
         "answer": r.value if r.source in ("person", "config") else None}
        for r in rs if r.applies and r.source != "config"]}

"""The migration steps and where each one stands. Pure logic (no Streamlit) so it can be tested on its own.

Same rules as the Synapse accelerator: a step opens once the step before it is confirmed; a finished step waits for the
user to confirm it ("Ready to confirm"); a confirmed step whose results are gone asks to be run again.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class Step:
    key: str
    title: str
    icon: str
    purpose: str


STEPS: tuple[Step, ...] = (
    Step("discover", "Discover", "🔎", "Read the pipelines, linked services and runtimes in the ADF source."),
    Step("review", "Review", "🧭", "Check discovery is complete; review pipelines, activities, dependencies, compute and compatibility."),
    Step("consult", "Consult", "💬", "Agree with the client how the migration should happen: scope, target, storage, compute and validation."),
    Step("connect", "Connect", "🔌", "Decide how each linked service becomes a Fabric connection, and test it."),
    Step("plan", "Plan target", "🏗️", "See what the target needs, what exists, and approve what may be created."),
    Step("convert", "Convert", "🔁", "Turn the ADF pipelines into Fabric pipelines."),
    Step("validate", "Validate", "🧪", "Re-read the source and the generated files and compare them, independently of the converter."),
    Step("deploy", "Deploy", "🚀", "Publish the converted pipelines to the Fabric workspace."),
    Step("report", "Report", "📋", "The honest account: what is confirmed, what is not, and what is left."),
)
KEYS = tuple(s.key for s in STEPS)


@dataclass
class StepFacts:
    """What the API says about one step right now."""

    complete: bool = False
    metric: str = ""
    outcome: str = ""  # one sentence once complete
    waiting: str = ""  # what is still needed, while not complete
    attention: Optional[str] = None  # short warning, e.g. "2 need manual action"
    headline: Optional[str] = None  # replaces "<step> finished" when that would overstate it (a dry run is not a deploy)


@dataclass
class StepStatus:
    step: Step
    index: int
    facts: StepFacts
    confirmed: bool = False
    locked: bool = False
    stale: bool = False  # confirmed earlier, but its results are gone
    extra: dict = field(default_factory=dict)

    @property
    def label(self) -> str:
        if self.locked:
            return "Locked"
        if self.stale:
            return "Run again"
        if self.confirmed:
            return "Done"
        if self.facts.complete:
            return "Ready to confirm"
        return "Needs attention" if self.facts.attention else "To do"

    @property
    def tone(self) -> str:
        """CSS class for the state pill: done | current | attention | locked."""
        if self.locked:
            return "locked"
        if self.stale or (self.facts.attention and not self.confirmed):
            return "attention"
        return "done" if self.confirmed else "current"


def statuses(facts: dict[str, StepFacts], confirmed: set[str]) -> list[StepStatus]:
    out: list[StepStatus] = []
    open_ = True  # the first step is always open
    for i, step in enumerate(STEPS):
        f = facts.get(step.key, StepFacts())
        is_confirmed = step.key in confirmed
        s = StepStatus(step=step, index=i, facts=f, confirmed=is_confirmed and f.complete, locked=not open_,
                       stale=is_confirmed and not f.complete)
        out.append(s)
        open_ = open_ and s.confirmed
    return out


def first_open(steps: list[StepStatus]) -> str:
    """The step to show by default: the first unlocked one that isn't done yet."""
    for s in steps:
        if not s.locked and not s.confirmed:
            return s.step.key
    return steps[-1].step.key


def done_count(steps: list[StepStatus]) -> int:
    return sum(1 for s in steps if s.confirmed)


# ---- facts from API payloads ---------------------------------------------------------------------------------------------
READY_STATES = {"connection_tested", "connection_configured", "skipped"}
PROBLEM_STATES = {"manual_action_required", "validation_failed"}


def discover_facts(project: Optional[dict], found: Optional[dict]) -> StepFacts:
    if project and project.get("semantic_stale"):
        return StepFacts(waiting="The source changed since discovery ran. Run discovery again.", metric="Out of date")
    if not project or not project.get("has_semantic"):
        return StepFacts(waiting="Run discovery to read the ADF source.", metric="Not run yet")
    ls = len((found or {}).get("linked_services", []))
    ir = len((found or {}).get("integration_runtimes", []))
    metric = f"{ls} linked services · {ir} runtimes"
    return StepFacts(complete=True, metric=metric, outcome=f"Found {metric}.")


def connect_facts(cards: Optional[list[dict]]) -> StepFacts:
    if cards is None:
        return StepFacts(waiting="Run discovery first.", metric="-")
    if not cards:
        return StepFacts(complete=True, metric="No linked services", outcome="Nothing to connect.")
    ready = sum(1 for c in cards if c.get("state") in READY_STATES)
    problems = sum(1 for c in cards if c.get("state") in PROBLEM_STATES)
    open_ = len(cards) - ready - problems
    metric = f"{ready} of {len(cards)} ready"
    attention = f"{problems} need attention" if problems else None
    if open_:
        return StepFacts(metric=metric, attention=attention,
                         waiting=f"{open_} linked service(s) still need a sign-in method and details.")
    return StepFacts(complete=True, metric=metric, attention=attention,
                     outcome=f"{ready} of {len(cards)} linked services are configured or tested.")


def review_facts(inv: Optional[dict]) -> StepFacts:
    if not inv:
        return StepFacts(waiting="Run discovery first.", metric="-")
    c = inv["completeness"]
    metric = f"{c['pipelines']} pipelines · {c['activities']} activities"
    if c["complete"]:
        return StepFacts(complete=True, metric=metric, outcome=f"Discovery is complete: {metric}.")
    return StepFacts(complete=True, metric=metric, attention="discovery has gaps",
                     outcome=f"{metric}, but discovery has gaps (see the Review step).")


def consult_facts(brief: Optional[dict]) -> StepFacts:
    """Complete only when the current answers are approved: an approval of answers that changed since does not count."""
    if not brief:
        return StepFacts(waiting="Run discovery first: the consultation asks only about what discovery found.", metric="-")
    c = brief["counts"]
    if brief.get("mode") == "simple":  # nothing to approve: complete once nothing conflicts, saved or not (unsaved means the run's normal defaults)
        if c["conflicts"]:
            return StepFacts(waiting="Resolve the conflicting settings.", metric=f"{c['conflicts']} conflict(s)", attention=f"{c['conflicts']} conflict(s)")
        if brief["state"] == "approved":
            return StepFacts(complete=True, metric="Settings saved", outcome="Settings saved; the run follows them.")
        return StepFacts(complete=True, metric="Nothing saved", outcome="Nothing saved: the run uses its normal defaults. Save settings on this step to change them.")
    metric = f"{c['answered']}/{c['applicable']} answered"
    if brief["state"] == "approved":
        v = brief["current_version"]
        return StepFacts(complete=True, metric=f"Brief v{v['version']} approved",
                         outcome=f"Brief v{v['version']} approved by {v['approved_by']}; the run follows it.")
    if c["conflicts"]:
        return StepFacts(waiting="Resolve the conflicting answers.", metric=metric, attention=f"{c['conflicts']} conflict(s)")
    if brief["state"] == "submitted":
        return StepFacts(waiting="An approver approves the submitted brief.", metric=f"v{brief['current_version']['version']} awaiting approval")
    if brief.get("changed_since_approval"):
        return StepFacts(waiting="The answers changed since approval: submit them again.", metric=metric, attention="changed since approval")
    open_ = c["need_person"]
    return StepFacts(waiting=f"{open_} required question(s) are open." if open_ else "Submit the brief for approval.", metric=metric)


def plan_facts(prov: Optional[dict]) -> StepFacts:
    """Complete only when the target is ready: nothing left to create, or everything approved was applied AND read back."""
    if not prov or not prov.get("available"):
        return StepFacts(waiting="Build the target plan (read-only).", metric="No plan yet")
    s = prov["summary"]
    changes = s["to_create"] + s["to_configure"]
    problems = s["blocked"] + s["conflicts"]
    attention = (f"{problems} blocked or in conflict" if problems else None)
    if prov["blockers"]:
        return StepFacts(metric=f"{changes} to change · {len(prov['blockers'])} blocker(s)", attention=attention or f"{len(prov['blockers'])} blocker(s)",
                         waiting="Clear the blockers shown in the plan.")
    if not changes:
        return StepFacts(complete=True, metric="Nothing to create", attention=attention, outcome="The target already has everything this plan needs.")
    ver = prov.get("verification") or {}
    if ver.get("all_verified") and (prov.get("run") or {}).get("dry_run") is False:
        return StepFacts(complete=True, metric=f"{changes} change(s) applied and verified", attention=attention, outcome="Everything approved was created and read back.")
    # Creating the missing items (a Spark pool, an environment) is optional for converting and deploying pipelines: it only matters to Spark workloads.
    # So the step can be passed with the gap stated, instead of forcing writes to Fabric just to get to Convert.
    note = f"{changes} item(s) not created yet"
    return StepFacts(complete=True, metric=f"{changes} to create", attention=attention or note,
                     outcome=f"Nothing was created in Fabric: {changes} item(s) the plan lists are still to create. Pipelines can be converted and deployed without them; "
                             "approve and apply them here before running Spark workloads.")


def validate_facts(val: Optional[dict]) -> StepFacts:
    if not val or not val.get("available"):
        return StepFacts(waiting="Run validation.", metric="Not run yet")
    st_ = {s["stage"]: s["status"] for s in val["stages"]}
    bad = [k for k, v in st_.items() if v in ("FAIL", "BLOCKED") and k == "static"]
    metric = f"Static: {st_.get('static', 'NOT_RUN').replace('_', ' ').lower()}"
    return StepFacts(complete=st_.get("static") not in (None, "NOT_RUN"), metric=metric, attention=("static checks failed or are blocked" if bad else None),
                     outcome=f"Static validation finished ({metric}). This does not prove the pipelines run.", waiting="Run validation.")


def report_facts(rep: Optional[dict]) -> StepFacts:
    """Finished only when validation included the deployment stage; 'verified' only when the backend says every required check passed."""
    if not rep or not rep.get("validation", {}).get("available"):
        return StepFacts(waiting="Validate the migration to get the report.", metric="Not validated")
    stages = {s["stage"]: s["status"] for s in rep["validation"]["stages"]}
    if stages.get("deployment") in (None, "NOT_RUN"):
        return StepFacts(waiting="Press \"Validate the migration now\" above: it also checks what is deployed in Fabric (needs a connected workspace).", metric=rep["overall"].replace("_", " ").title())
    attention = None if rep["full_success"] else rep["headline"]
    return StepFacts(complete=True, metric=rep["overall"].replace("_", " ").title(), attention=attention, outcome=rep["headline"])


def convert_facts(result: Optional[dict]) -> StepFacts:
    if not result:
        return StepFacts(waiting="Run the conversion.", metric="Not run yet")
    pipes = result.get("pipelines", [])
    ready = sum(1 for p in pipes if p.get("runtime_ready"))
    blocked = (result.get("status_counts") or {}).get("blocked", 0)
    unbound = len(result.get("unbound", []))
    errors = sum(1 for f in result.get("preflight", []) if f.get("severity") == "error")
    parts = [f"{blocked} blocked activities"] if blocked else []
    parts += [f"{errors} source problem(s)"] if errors else []
    parts += [f"{unbound} unbound"] if unbound else []
    metric = f"{ready} of {len(pipes)} ready to run"
    return StepFacts(complete=True, metric=metric, attention=", ".join(parts) or None,
                     outcome=f"Converted {len(pipes)} pipeline(s); {ready} ready to run.")


def deploy_facts(result: Optional[dict]) -> StepFacts:
    if not result:
        return StepFacts(waiting="Run a dry run, then deploy.", metric="Not run yet")
    rows = result.get("results", [])
    failed = sum(1 for r in rows if r.get("action") in ("failed", "refused"))
    if not result.get("applied"):
        # A dry run changes nothing, so this step can be passed (the Report then says honestly that nothing is deployed) rather than being a dead end
        # when pipelines are refused (an unbound connection) and the person wants to see the report first.
        return StepFacts(complete=True, metric=f"Dry run: {len(rows)} pipeline(s)", headline="Dry run done · nothing deployed yet",
                         attention=f"{failed} would be refused" if failed else "nothing deployed yet",
                         outcome=(f"{failed} pipeline(s) would be refused (see the table), and " if failed else "") +
                                 "nothing is in Fabric yet. Deploy for real to publish; until then the Report shows these as not deployed.")
    return StepFacts(complete=True, metric=f"{len(rows) - failed} of {len(rows)} deployed",
                     attention=f"{failed} failed" if failed else None,
                     outcome=f"Deployed {len(rows) - failed} pipeline(s) to Fabric.")

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
    Step("connect", "Connect", "🔌", "Decide how each linked service becomes a Fabric connection, and test it."),
    Step("convert", "Convert", "🔁", "Turn the ADF pipelines into Fabric pipelines."),
    Step("deploy", "Deploy", "🚀", "Publish the converted pipelines to the Fabric workspace."),
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


def convert_facts(result: Optional[dict]) -> StepFacts:
    if not result:
        return StepFacts(waiting="Run the conversion.", metric="Not run yet")
    pipes = result.get("pipelines", [])
    ready = sum(1 for p in pipes if p.get("runtime_ready"))
    blocked = (result.get("status_counts") or {}).get("blocked", 0)
    unbound = len(result.get("unbound", []))
    parts = [f"{blocked} blocked activities"] if blocked else []
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
        return StepFacts(metric=f"Dry run: {len(rows)} pipeline(s)",
                         attention=f"{failed} would be refused" if failed else None,
                         waiting="The dry run is done. Deploy for real to finish.")
    return StepFacts(complete=True, metric=f"{len(rows) - failed} of {len(rows)} deployed",
                     attention=f"{failed} failed" if failed else None,
                     outcome=f"Deployed {len(rows) - failed} pipeline(s) to Fabric.")

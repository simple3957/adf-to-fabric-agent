"""A small, persisted record of what a deploy actually did (names and outcomes only), so the next screen, CLI run or report can say
"deployed" because a deploy reported it, not because someone clicked a button in a browser.

The record is evidence of the CALL, not of the result: whether the deployed definition is the generated one is what
``adf2fabric validate --stage deployment`` establishes. A dry run never writes a record of deployment; it writes the time of the last
dry run, which the API requires before it will apply (a changed target needs a preview first).
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Iterable, Optional

_NAME = "deployment-record.json"


def path(project_dir: Path) -> Path:
    return project_dir / "deployment" / _NAME


def load(project_dir: Path) -> dict[str, Any]:
    try:
        data = json.loads(path(project_dir).read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _write(project_dir: Path, data: dict[str, Any]) -> None:
    p = path(project_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(p)


def note_dry_run(project_dir: Path, workspace: str, pipelines: Iterable[str], failed: int, now: Optional[float] = None,
                 allow_unbound: bool = False) -> None:
    data = load(project_dir)
    data["last_dry_run"] = {"at": now or time.time(), "workspace": workspace, "pipelines": sorted(set(pipelines)), "refused": failed,
                            "allow_unbound": bool(allow_unbound)}
    _write(project_dir, data)


def note_apply(project_dir: Path, workspace: str, results: Iterable[Any], now: Optional[float] = None) -> None:
    """Merge a real deploy: each pipeline keeps its latest action. ``results`` are DeployResult-like (name, action, detail)."""
    data = load(project_dir)
    rows = data.setdefault("pipelines", {})
    for r in results:
        rows[str(r.name)] = {"action": str(r.action), "at": now or time.time(), "workspace": workspace,
                             "ok": str(r.action) not in ("failed", "refused", "skipped")}
    data["last_apply"] = {"at": now or time.time(), "workspace": workspace}
    _write(project_dir, data)


def dry_run_ok(project_dir: Path, workspace: str, pipelines: Iterable[str], max_age: float = 3600.0, now: Optional[float] = None,
               allow_unbound: bool = False) -> bool:
    """True when a dry run of (at least) these pipelines in this workspace finished, with nothing refused, within ``max_age`` seconds.

    A dry run that allowed unbound connections does not vouch for a real deploy that does not: that one would refuse what the preview let through."""
    last = load(project_dir).get("last_dry_run") or {}
    if not last or last.get("workspace") != workspace or last.get("refused"):
        return False
    if last.get("allow_unbound") and not allow_unbound:
        return False
    if (now or time.time()) - float(last.get("at", 0)) > max_age:
        return False
    return {p.lower() for p in pipelines} <= {p.lower() for p in last.get("pipelines", [])}

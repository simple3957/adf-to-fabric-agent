"""The consultation's record on disk: answers, notes and submitted / approved versions of the brief.

Stored at ``projects/<id>/brief/brief.json``. A version is bound to the fingerprint of the answers it was submitted
with: change an answer (or let discovery change what applies) and the approved version no longer matches, so the run
stops treating it as approved until it is submitted and approved again. Approval names who approved it.
"""

from __future__ import annotations

import functools
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from adf2fabric.agent.consultation import brief as B
from adf2fabric.project.locks import lock_for, write_atomic


class BriefError(ValueError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def path(store: Any, pid: str) -> Path:
    return store.project_dir(pid) / "brief" / "brief.json"


def load(store: Any, pid: str) -> dict[str, Any]:
    p = path(store, pid)
    data: dict[str, Any] = {}
    if p.exists():
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            data = {}
    return {"answers": dict(data.get("answers") or {}), "notes": list(data.get("notes") or []), "versions": list(data.get("versions") or []),
            "config": dict(data.get("config") or {})}


def _config(data: dict, destination: Optional[dict], capture: bool) -> dict:
    """The configuration the brief reads (the Fabric workspace), kept WITH the brief so it says the same to everyone who looks at it.

    Only a person who changes the brief records their session's destination; viewers and approvers read what was recorded.
    The workspace id stays in the record (it is compared at deploy time) and is never part of a view."""
    dest = destination or {}
    ws = dest.get("workspace_id") or dest.get("workspace_name")
    if capture and ws and data["config"].get("workspace") != ws:
        data["config"]["workspace"] = ws
    return {"workspace_id": data["config"].get("workspace")} if data["config"].get("workspace") else {}


def _stamp(fp: str, data: dict) -> str:
    """The answers' fingerprint AND the workspace the brief was agreed for: another workspace is another brief."""
    return hashlib.sha256(f"{fp}|{data['config'].get('workspace') or ''}".encode("utf-8")).hexdigest()[:16]


def workspace_changed(data: dict, destination: Optional[dict]) -> bool:
    now = (destination or {}).get("workspace_id") or (destination or {}).get("workspace_name")
    then = data["config"].get("workspace")
    return bool(now and then and str(now).lower() != str(then).lower())


def workspace_mismatch(store: Any, pid: str, target_workspace: Optional[str]) -> Optional[str]:
    """When an approved, current brief was agreed for another workspace than ``target_workspace``, why that must stop; else None."""
    data = load(store, pid)
    agreed = data["config"].get("workspace")
    if not (target_workspace and agreed) or str(target_workspace).lower() == str(agreed).lower():
        return None
    if approved_settings(store, pid) is None:
        return None
    return ("The approved consultation brief was agreed for a different Fabric workspace than the one chosen now. Review the brief for this "
            "workspace and approve it again (Consult step: 'Use the workspace chosen now'), or choose the workspace it was agreed for.")


def save(store: Any, pid: str, data: dict[str, Any]) -> None:
    write_atomic(path(store, pid), json.dumps(data, indent=2, ensure_ascii=False))


def _auto_approve(store: Any, pid: str, by: str, destination: Optional[dict]) -> None:
    """Simple mode: saving the settings is the whole step. They become the version the run follows, under the signed-in person's name.
    Open required questions or conflicts leave it a draft, and the screen says why."""
    if not B.simple():
        return
    try:
        submit(store, pid, by, destination)
        approve(store, pid, view(store, pid, destination)["fingerprint"], by.strip() or "local", destination)
    except BriefError:
        pass


def _locked(fn):
    """Every change to the brief is load -> change -> save: one at a time per project, or concurrent requests lose each other's work."""
    @functools.wraps(fn)
    def run(store, pid, *a, **kw):
        with lock_for(path(store, pid)):
            return fn(store, pid, *a, **kw)
    return run


def _note(data: dict, text: str, kind: str = "system", by: str = "") -> None:
    data["notes"] = (data["notes"] + [{"at": _now(), "kind": kind, "by": by, "text": text[:2000]}])[-200:]


def view(store: Any, pid: str, destination: Optional[dict] = None) -> dict[str, Any]:
    """Everything the consultation shows: facts, every applicable question with its answer and recommendation, counts,
    conflicts, the brief's state, and (only when the current answers are approved) the settings the run follows."""
    data = load(store, pid)
    facts = B.read_facts(store, pid, _config(data, destination, capture=False))
    rs = B.resolve(facts, data["answers"])
    fp = _stamp(B.fingerprint(rs), data)
    latest = data["versions"][-1] if data["versions"] else None
    current = latest if latest and latest.get("fingerprint") == fp else None
    state = "draft" if not current else "approved" if current.get("approved_at") else "submitted"
    return {
        "mode": "simple" if B.simple() else "full",
        "facts": facts.to_dict(),
        "topics": [{"id": t, "title": title, "summary": s} for t, title, s in B.TOPICS],
        "questions": [{
            "id": r.q.id, "topic": r.q.topic, "text": r.q.text, "help": r.q.help, "kind": r.q.kind, "required": r.q.required,
            "effect": r.q.effect, "effect_label": B.EFFECT_LABEL[r.q.effect], "applies_to": r.q.applies, "placeholder": r.q.placeholder,
            "options": [{"value": o.value, "label": o.label, "description": o.description} for o in r.q.options(facts)] if r.q.kind != "text" else [],
            "value": r.value, "label": B.answer_label(r.q, facts, r.value), "source": r.source, "problem": r.problem,
            "recommendation": {"value": r.rec.value, "reason": r.rec.reason, "label": B.answer_label(r.q, facts, r.rec.value)} if r.rec else None,
        } for r in rs if r.applies],
        "counts": B.counts(facts, rs),
        "conflicts": B.conflicts(facts, rs),
        "state": state,
        "fingerprint": fp,
        "workspace_changed": workspace_changed(data, destination),
        "current_version": current,
        "versions": data["versions"],
        "changed_since_approval": current is None and any(v.get("approved_at") for v in data["versions"]),
        "notes": data["notes"][-50:],
        "settings": B.settings_from(facts, rs).to_dict() if state == "approved" else None,
        "draft_settings": B.settings_from(facts, rs).to_dict(),
    }


def approved_settings(store: Any, pid: str, destination: Optional[dict] = None) -> Optional[dict[str, Any]]:
    """The settings of the approved brief when the current answers are still the approved ones; else None."""
    return view(store, pid, destination)["settings"]


@_locked
def set_answers(store: Any, pid: str, answers: dict[str, Any], by: str = "", destination: Optional[dict] = None) -> int:
    data = load(store, pid)
    facts = B.read_facts(store, pid, _config(data, destination, capture=True))
    rs = {r.q.id: r for r in B.resolve(facts, {**data["answers"], **{k: v for k, v in answers.items() if v is not None}})}
    for qid, value in answers.items():
        q = B.QUESTION_BY_ID.get(qid)
        if q is None:
            raise BriefError(f"No question '{qid}'.")
        if value is None:
            data["answers"].pop(qid, None)
            continue
        if not rs[qid].applies:
            raise BriefError(f"'{qid}' does not apply to this estate (or to the answers given so far).")
        if rs[qid].source == "config":
            raise BriefError(f"'{qid}' is read from the configuration; change it there.")
        try:
            data["answers"][qid] = B.validate_answer(q, facts, value)
        except ValueError as exc:
            raise BriefError(str(exc)) from None
    _note(data, f"{len(answers)} answer(s) changed." if answers else "Workspace recorded again.", "person", by)
    save(store, pid, data)
    _auto_approve(store, pid, by, destination)
    return len(answers)


@_locked
def accept_recommendations(store: Any, pid: str, by: str = "", destination: Optional[dict] = None) -> int:
    data = load(store, pid)
    facts = B.read_facts(store, pid, _config(data, destination, capture=True))
    taken = [r for r in B.resolve(facts, data["answers"]) if r.applies and r.source == "recommendation" and r.rec]
    for r in taken:
        data["answers"][r.q.id] = r.rec.value  # type: ignore[union-attr]
    if taken:
        _note(data, f"Accepted {len(taken)} recommended answer(s).", "person", by)
    save(store, pid, data)  # also keeps the configuration just recorded
    _auto_approve(store, pid, by, destination)
    return len(taken)


@_locked
def submit(store: Any, pid: str, by: str = "", destination: Optional[dict] = None) -> dict[str, Any]:
    data = load(store, pid)
    _config(data, destination, capture=True)
    save(store, pid, data)
    v = view(store, pid)
    if v["counts"]["need_person"]:
        raise BriefError(f"{v['counts']['need_person']} required question(s) are still open.")
    if v["conflicts"]:
        raise BriefError("Resolve the conflicts first: " + " ".join(v["conflicts"]))
    if v["state"] != "draft":
        raise BriefError(f"These answers are already {v['state']} (version {v['current_version']['version']}).")
    data = load(store, pid)
    facts = B.read_facts(store, pid, _config(data, None, capture=False))
    rs = B.resolve(facts, data["answers"])
    version = {"version": (data["versions"][-1]["version"] if data["versions"] else 0) + 1, "fingerprint": v["fingerprint"],
               "answers": B.effective_answers(rs), "submitted_at": _now(), "submitted_by": by, "approved_at": None, "approved_by": None}
    data["versions"].append(version)
    _note(data, f"Brief v{version['version']} submitted for approval.", "system", by)
    save(store, pid, data)
    return version


@_locked
def approve(store: Any, pid: str, fingerprint: str, by: str, destination: Optional[dict] = None) -> dict[str, Any]:
    """Approve the submitted version, only if it is still what the answers say (the fingerprint the approver saw)."""
    if not by.strip():
        raise BriefError("Name who approves the brief.")
    v = view(store, pid)  # what was recorded with the brief, not the approver's own session
    if v["state"] != "submitted":
        raise BriefError("Nothing to approve: submit the current answers first." if v["state"] == "draft" else "These answers are already approved.")
    if fingerprint != v["fingerprint"]:
        raise BriefError("The answers changed after you read them: review the brief again before approving.")
    data = load(store, pid)
    submitter = str(data["versions"][-1].get("submitted_by") or "")
    if not B.simple() and submitter and submitter == by.strip() and by.strip() != "local":  # four eyes; "local" is the single user of an unauthenticated install
        raise BriefError("The person who submitted the brief cannot also approve it: another approver must.")
    data["versions"][-1].update(approved_at=_now(), approved_by=by.strip())
    _note(data, f"Brief v{data['versions'][-1]['version']} approved by {by.strip()}.", "system", by)
    save(store, pid, data)
    return data["versions"][-1]


@_locked
def import_answers(store: Any, pid: str, payload: Any, by: str = "", destination: Optional[dict] = None) -> int:
    """Answers from a filled questionnaire (``brief.questionnaire`` format, or a plain {id: value} map). Unknown or
    inapplicable questions are skipped; each value is checked against the question's own options."""
    if isinstance(payload, dict) and isinstance(payload.get("questions"), list):
        pairs = {q.get("id"): q.get("answer") for q in payload["questions"] if isinstance(q, dict)}
    elif isinstance(payload, dict):
        pairs = dict(payload)
    else:
        raise BriefError("Not a questionnaire: expected the exported JSON or a map of question id to answer.")
    data = load(store, pid)
    facts = B.read_facts(store, pid, _config(data, destination, capture=True))
    taken = 0
    for qid, value in pairs.items():
        q = B.QUESTION_BY_ID.get(str(qid))
        if q is None or value is None or (isinstance(value, str) and not value.strip()):
            continue
        rs = {r.q.id: r for r in B.resolve(facts, data["answers"])}
        if not rs[q.id].applies or rs[q.id].source == "config":
            continue
        try:
            data["answers"][q.id] = B.validate_answer(q, facts, value)
            taken += 1
        except ValueError:
            continue
    if taken:
        _note(data, f"Imported {taken} answer(s) from a questionnaire.", "person", by)
        save(store, pid, data)
        _auto_approve(store, pid, by, destination)
    return taken


@_locked
def add_note(store: Any, pid: str, text: str, by: str = "") -> None:
    if not text.strip():
        raise BriefError("The note is empty.")
    data = load(store, pid)
    _note(data, text.strip(), "note", by)
    save(store, pid, data)


@_locked
def reset(store: Any, pid: str, by: str = "") -> None:
    """Clear the answers (versions and notes are kept as history)."""
    data = load(store, pid)
    data["answers"] = {}
    _note(data, "Answers cleared.", "system", by)
    save(store, pid, data)

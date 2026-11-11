"""Regressions for what the black-box security probe found: lockout scope, Host header parsing, hostile Git URLs, four-eyes approval, lost updates."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from adf2fabric.agent.consultation import brief_record
from adf2fabric.api.security import guards


@pytest.mark.parametrize("url", ["https://github.com/o/r\n--upload-pack=x", "https://github.com/o/r --upload-pack=touch", "https://github.com\\@127.0.0.1/r.git",
                                 "https://github.com/o/r\x00"])
def test_a_git_url_with_whitespace_or_control_characters_is_refused(url):
    with pytest.raises(guards.Rejected):
        guards.check_public_https_url(url, resolver=lambda h: ["140.82.112.3"])


def test_two_processes_changing_one_record_lose_nothing(tmp_path):
    """Four separate Python processes each add 15 notes to one brief at once; every note must survive."""
    import subprocess
    import sys

    from adf2fabric.project.store import ProjectStore

    store = ProjectStore(tmp_path)
    store.project_dir("p1").mkdir(parents=True, exist_ok=True)
    code = ("import sys\nfrom pathlib import Path\nfrom adf2fabric.agent.consultation import brief_record as b\n"
            "from adf2fabric.project.store import ProjectStore\ns = ProjectStore(Path(sys.argv[1]))\n"
            "for i in range(15):\n    b.add_note(s, 'p1', f'{sys.argv[2]}-{i}', 'x')\n")
    procs = [subprocess.Popen([sys.executable, "-W", "ignore", "-c", code, str(tmp_path), f"w{n}"]) for n in range(4)]
    assert [p.wait(timeout=120) for p in procs] == [0, 0, 0, 0]
    notes = brief_record.load(store, "p1")["notes"]
    assert len(notes) == 60 and len({n["text"] for n in notes}) == 60


def test_processes_arriving_together_at_a_record_that_has_no_lock_file_yet_all_get_the_lock(tmp_path):
    """Found when this failed once in the full suite: each process wrote a byte into the new lock file "if it looked empty", and a late writer hit the
    byte another process had locked (PermissionError on Windows, about 6% of acquisitions). Nothing is written now. Six processes are released at the
    same instant onto a fresh path, several times over."""
    import subprocess
    import sys
    import time

    code = chr(10).join(["import sys, time", "from pathlib import Path", "from adf2fabric.project.locks import lock_for",
                         "t = float(sys.argv[2])", "while time.time() < t: pass", "with lock_for(Path(sys.argv[1])): pass"])
    for trial in range(12):
        target = tmp_path / f"t{trial}" / "record.yaml"
        target.parent.mkdir()
        start = time.time() + 0.6
        procs = [subprocess.Popen([sys.executable, "-W", "ignore", "-c", code, str(target), str(start)]) for _ in range(6)]
        assert [p.wait(timeout=60) for p in procs] == [0] * 6, f"trial {trial}"


def test_a_record_can_be_replaced_while_other_threads_read_it(tmp_path):
    """Found as a 500 from the journey route in the full suite: on Windows a replace fails while another handle has the file open (WinError 5), and a read
    fails while the replace is in flight. Four writers replace one record while four readers poll it; neither side may see an error."""
    import time

    from adf2fabric.project.locks import read_text_retry, write_atomic

    target = tmp_path / "record.json"
    write_atomic(target, "0")
    stop = time.time() + 2.0
    errors: list[str] = []

    def writer(n: int) -> None:
        i = 0
        while time.time() < stop:
            try:
                write_atomic(target, f"{n}-{i}" * 50)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"write {type(exc).__name__}")
            i += 1

    def reader() -> None:
        while time.time() < stop:
            try:
                assert read_text_retry(target)  # never empty, never a half file
            except Exception as exc:  # noqa: BLE001
                errors.append(f"read {type(exc).__name__}")
            time.sleep(0.001)

    threads = [threading.Thread(target=writer, args=(n,)) for n in range(4)] + [threading.Thread(target=reader) for _ in range(4)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert not errors, sorted(set(errors))


# ---- shared files: no lost updates, no torn chain ---------------------------------------------------------------------------


def test_many_notebook_deploys_at_once_lose_no_binding(tmp_path):
    """bind_resolutions was an unlocked read-modify-write on the file the connections merge and the bindings list also edit."""
    import json

    from adf2fabric.agent.deployment.notebook_deploy import bind_resolutions

    path = tmp_path / "resolutions.json"
    ids = {f"NB_{i}": f"{i:08d}-0000-4000-8000-000000000000" for i in range(12)}
    with ThreadPoolExecutor(12) as ex:
        list(ex.map(lambda kv: bind_resolutions(path, kv[0], kv[1], "53d67e72-8124-4ca3-ae55-c4b266935faa"), ids.items()))
    rows = json.loads(path.read_text(encoding="utf-8"))
    assert {r["key"]: r["value"] for r in rows if r["type"] == "ItemToId"} == {f"notebook:{k}": v for k, v in ids.items()}
    assert sum(1 for r in rows if r["type"] == "WorkspaceId") == 1


def test_two_verifications_finishing_together_keep_each_others_results(tmp_path):
    import json

    from adf2fabric.agent.deployment import verify as V

    path = tmp_path / "verification" / "verification.json"

    def one(n):
        v = V.Verification(f"PL_{n}", "matches", fabric_status="Completed", job_id=f"job-{n}", ran_at="2026-11-02T00:00:00")
        V.save_record(path, [v])

    with ThreadPoolExecutor(10) as ex:
        list(ex.map(one, range(10)))
    assert sorted(json.loads(path.read_text(encoding="utf-8"))) == sorted(f"PL_{n}" for n in range(10))


def test_output_checks_finishing_together_keep_each_others_results(tmp_path):

    from adf2fabric.agent.deployment import output_check as O

    def one(n):
        O.save(tmp_path, {"pipeline": f"PL_{n % 3}", "name": f"out-{n}", "verdict": O.MATCHES})

    with ThreadPoolExecutor(10) as ex:
        list(ex.map(one, range(12)))
    data = O.load(tmp_path)
    assert sorted(name for checks in data.values() for name in checks) == sorted(f"out-{n}" for n in range(12))


def test_the_audit_chain_stays_intact_when_two_writers_append_to_it(tmp_path):
    """The CLI and the API are two writers of one hash chain: each used to remember its own last hash, so the other's events broke the chain."""
    from adf2fabric.api.security.audit import AuditLog

    api, cli = AuditLog(tmp_path), AuditLog(tmp_path)
    for i in range(20):
        (api if i % 2 == 0 else cli).event("test.event", "ok", actor=f"w{i % 2}", resource_id=str(i))
    assert api.verify() is None and cli.verify() is None
    assert len(api.read(limit=100)) == 20

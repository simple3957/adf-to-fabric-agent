"""Regressions for what the black-box security probe found: lockout scope, Host header parsing, hostile Git URLs, four-eyes approval, lost updates."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from adf2fabric.agent.consultation import brief_record
from adf2fabric.api.security import guards
from tests.test_api_security import ALL, World, make_zip


@pytest.fixture()
def world(tmp_path, monkeypatch):
    monkeypatch.delenv("ADF2FABRIC_API_TOKEN", raising=False)
    return World(tmp_path)


def _discovered(world, who="ann"):
    world.call(who, "POST", "/mode", json_body={"mode": "demo"})
    for stage in ("discovery", "semantic", "dependencies"):
        r = world.call(who, "POST", f"/projects/demo-estate/stages/{stage}")
        if r.status_code == 202:
            world.wait(who, r.json())


def test_a_legacy_token_guess_stays_locked_but_a_real_user_is_not(tmp_path, monkeypatch):
    monkeypatch.setenv("ADF2FABRIC_API_TOKEN", "a-weak-shared-token")
    w = World(tmp_path, auth_fail_limit=3, auth_fail_window=60)
    for _ in range(3):
        assert w.http.get("/me", headers={"Authorization": "Bearer nope"}).status_code == 401
    assert w.http.get("/me", headers={"Authorization": "Bearer a-weak-shared-token"}).status_code == 429  # the guessable token waits
    assert w.http.get("/me", headers={"Authorization": f"Bearer {w.tokens['ann']}"}).status_code == 200  # a 160-bit user token cannot be guessed


@pytest.mark.parametrize("host", ["127.0.0.1:8777@evil.example", "localhost@evil.example", "127.0.0.1:abc", "127.0.0.1/", "localhost:8501:1", "local host", "localhost\\evil"])
def test_a_host_header_that_only_starts_like_a_local_host_is_refused(world, host):
    assert world.http.get("/health", headers={"Host": host}).status_code == 400


@pytest.mark.parametrize("host", ["localhost", "localhost:8501", "127.0.0.1:8777", "[::1]", "[::1]:8777"])
def test_local_host_headers_are_still_accepted(world, host):
    assert world.http.get("/health", headers={"Host": host}).status_code == 200


@pytest.mark.parametrize("url", ["https://github.com/o/r\n--upload-pack=x", "https://github.com/o/r --upload-pack=touch", "https://github.com\\@127.0.0.1/r.git",
                                 "https://github.com/o/r\x00"])
def test_a_git_url_with_whitespace_or_control_characters_is_refused(url):
    with pytest.raises(guards.Rejected):
        guards.check_public_https_url(url, resolver=lambda h: ["140.82.112.3"])


@pytest.mark.parametrize("field,value", [("revision", "../../x"), ("revision", "a..b"), ("revision", "-c core.sshCommand=x"), ("subdirectory", "a\x00b"), ("subdirectory", "a\nb")])
def test_a_hostile_revision_or_subdirectory_is_refused(world, field, value):
    body = {"kind": "git", "repository_url": "https://github.com/o/r", field: value}
    assert world.call("ann", "POST", "/test/source", json_body=body).status_code == 422


def test_the_person_who_submitted_a_brief_cannot_approve_it(world):
    _, world.tokens["dual"] = world.users.create("dual", "default", ["analyst", "approver"], ALL)
    _discovered(world, "ann")
    P = "/projects/demo-estate/brief"
    for who in ("dual", "abe"):
        world.call(who, "POST", "/mode", json_body={"mode": "demo"})
    world.call("dual", "POST", f"{P}/accept")
    v = world.call("dual", "GET", P).json()
    fill = {q["id"]: ("LH_Test" if q["id"] == "storage.lakehouse" else "Dana, lead" if q["kind"] == "text" else [q["options"][0]["value"]] if q["kind"] == "multi" else q["options"][0]["value"])
            for q in v["questions"] if q["required"] and q["source"] == "open"}
    world.call("dual", "PUT", f"{P}/answers", json_body={"answers": fill})
    world.call("dual", "POST", f"{P}/accept")
    sub = world.call("dual", "POST", f"{P}/submit")
    assert sub.status_code == 200, sub.text
    fp = sub.json()["fingerprint"]
    own = world.call("dual", "POST", f"{P}/approve", json_body={"fingerprint": fp})
    assert own.status_code == 409 and "another approver" in own.json()["error"]
    assert world.call("abe", "POST", f"{P}/approve", json_body={"fingerprint": fp}).status_code == 200


def test_concurrent_brief_notes_and_journey_confirmations_lose_nothing(world):
    _discovered(world, "ann")
    store, P = world.demo.store, "/projects/demo-estate"
    before = len(brief_record.load(store, "demo-estate")["notes"])
    with ThreadPoolExecutor(12) as ex:
        list(ex.map(lambda i: brief_record.add_note(store, "demo-estate", f"note {i}", "ann"), range(20)))
    assert len(brief_record.load(store, "demo-estate")["notes"]) == before + 20

    steps = ["discover", "review", "consult", "connect", "plan", "convert", "validate", "deploy", "report"]
    gate = threading.Barrier(len(steps))

    def confirm(step):
        gate.wait()
        return world.call("ann", "PUT", f"{P}/journey", json_body={"steps": [step]}).status_code

    with ThreadPoolExecutor(len(steps)) as ex:
        assert set(ex.map(confirm, steps)) == {200}
    assert world.call("ann", "GET", f"{P}/journey").json()["confirmed"] == steps


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


def _hostile_zip(slip: bool = False) -> bytes:
    import json as _json

    def pipe(name, acts):
        return _json.dumps({"name": name, "properties": {"activities": acts}})

    script = "<script>alert(1)</script>"
    img = '<img src=x onerror="alert(1)">'
    files = {
        "pipeline/PL_Script.json": pipe(f"PL_{script}", [{"name": img, "type": "Copy", "typeProperties": {
            "source": {"type": "DelimitedTextSource"}, "sink": {"type": "DelimitedTextSink"}},
            "inputs": [{"referenceName": "../../evil", "type": "DatasetReference"}],
            "outputs": [{"referenceName": "DS_Out", "type": "DatasetReference"}]}]),
        "pipeline/PL_Traversal.json": pipe("../../../escape", [{"name": "A", "type": "Wait", "typeProperties": {"waitTimeInSeconds": 1}}]),
        "pipeline/PL_Big.json": pipe("PL_Big", [{"name": f"W{i}", "type": "Wait", "typeProperties": {"waitTimeInSeconds": 1}} for i in range(3000)]),
        "dataset/DS_Out.json": _json.dumps({"name": "DS_Out", "properties": {"type": "DelimitedText", "linkedServiceName": {"referenceName": "LS_X", "type": "LinkedServiceReference"},
                                                                                "typeProperties": {"location": {"type": "AzureBlobFSLocation", "fileSystem": "C\x00ont", "folderPath": "../../x"}}}}),
        "linkedService/LS_X.json": _json.dumps({"name": "LS_X", "properties": {"type": "AzureBlobFS", "typeProperties": {"url": "https://acct.dfs.core.windows.net", "accountKey": "CANARY-SECRET-123"}}}),
        "pipeline/not json.json": "{",
    }
    if slip:
        files["../evil.json"] = "{}"
    return make_zip(files)


def test_a_hostile_source_cannot_break_out_crash_or_leak_through_the_stages(world, tmp_path):
    from adf2fabric.api.stages import STAGES  # noqa: F401  (the list the API accepts)

    up = world.call("ann", "PUT", "/uploads/zip", content=_hostile_zip(), headers={"Content-Type": "application/zip"})
    assert up.status_code == 200, up.text
    world.call("ann", "POST", "/mode", json_body={"mode": "live"})
    job = world.call("ann", "POST", "/projects", json_body={"project_name": "hostile", "source": {"kind": "zip", "upload_id": up.json()["upload_id"]}}).json()
    done = world.wait("ann", job, timeout=180)
    assert done["state"] == "succeeded", done  # bad files are findings, not a crashed import
    for stage in ("discovery", "semantic", "dependencies", "workloads", "reconstruct", "convert", "validate", "deploy_dry_run"):
        r = world.call("ann", "POST", f"/projects/hostile/stages/{stage}")
        assert r.status_code < 500, (stage, r.status_code, r.text[:200])
        if r.status_code == 202:
            j = world.wait("ann", r.json(), timeout=180)
            assert j["state"] in ("succeeded", "failed"), stage
    for sub in ("", "/inventory", "/waves", "/brief", "/provisioning", "/report", "/validation", "/connections", "/handover", "/conversion"):
        r = world.call("ann", "GET", "/projects/hostile" + sub)
        assert r.status_code < 500, (sub, r.status_code, r.text[:200])
        assert "CANARY-SECRET-123" not in r.text and "Traceback" not in r.text, sub
    assert not list(tmp_path.rglob("escape*")) and not list(tmp_path.rglob("evil*")) and not (tmp_path.parent / "evil.json").exists()
    audit = "\n".join(str(e) for e in world.audit.read())
    assert "CANARY-SECRET-123" not in audit


def test_an_archive_with_an_entry_outside_its_folder_is_refused_whole(world, tmp_path):
    up = world.call("ann", "PUT", "/uploads/zip", content=_hostile_zip(slip=True), headers={"Content-Type": "application/zip"}).json()
    job = world.call("ann", "POST", "/projects", json_body={"project_name": "slip", "source": {"kind": "zip", "upload_id": up["upload_id"]}}).json()
    done = world.wait("ann", job)
    assert done["state"] == "failed" and "outside its folder" in done["error"]
    assert world.call("ann", "GET", "/projects/slip").status_code == 404 and not list(tmp_path.rglob("evil*")) and not (tmp_path.parent / "evil.json").exists()


@pytest.mark.parametrize("name", ["pipeline/<script>.json", 'pipeline/a"b.json', "pipeline/a|b.json", "pipeline/a?.json", "pipeline/trailing.json.", "pipeline/ctl\x01.json"])
def test_a_file_name_the_os_cannot_create_is_refused_with_a_plain_reason(world, name):
    up = world.call("ann", "PUT", "/uploads/zip", content=make_zip({name: "{}"}), headers={"Content-Type": "application/zip"}).json()
    done = world.wait("ann", world.call("ann", "POST", "/projects", json_body={"project_name": "badname", "source": {"kind": "zip", "upload_id": up["upload_id"]}}).json())
    assert done["state"] == "failed" and "cannot be created on Windows" in done["error"] and "OSError" not in done["error"]


def test_absurdly_nested_json_is_refused_with_a_plain_reason_not_a_recursion_error(world):
    deep = '{"name": "PL_Deep", "properties": {"activities": ' + "[" * 4000 + "]" * 4000 + "}}"
    up = world.call("ann", "PUT", "/uploads/zip", content=make_zip({"pipeline/PL_Deep.json": deep}), headers={"Content-Type": "application/zip"}).json()
    done = world.wait("ann", world.call("ann", "POST", "/projects", json_body={"project_name": "deep", "source": {"kind": "zip", "upload_id": up["upload_id"]}}).json())
    assert done["state"] == "failed" and "levels deep" in done["error"] and "Recursion" not in done["error"]


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
    import threading
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
    from concurrent.futures import ThreadPoolExecutor

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
    from concurrent.futures import ThreadPoolExecutor

    from adf2fabric.agent.deployment import verify as V

    path = tmp_path / "verification" / "verification.json"

    def one(n):
        v = V.Verification(f"PL_{n}", "matches", fabric_status="Completed", job_id=f"job-{n}", ran_at="2026-11-02T00:00:00")
        V.save_record(path, [v])

    with ThreadPoolExecutor(10) as ex:
        list(ex.map(one, range(10)))
    assert sorted(json.loads(path.read_text(encoding="utf-8"))) == sorted(f"PL_{n}" for n in range(10))


def test_output_checks_finishing_together_keep_each_others_results(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

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

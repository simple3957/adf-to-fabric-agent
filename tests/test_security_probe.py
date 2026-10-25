"""Regressions for what the black-box security probe found: lockout scope, Host header parsing, hostile Git URLs, four-eyes approval, lost updates."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from adf2fabric.agent.consultation import brief_record
from adf2fabric.api.security import guards
from tests.test_api_security import ALL, World


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


@pytest.mark.parametrize("host", ["127.0.0.1:8777@evil.example", "localhost@evil.example", "127.0.0.1:abc", "127.0.0.1/", "localhost:8501:1", "local host", "localhost\evil"])
def test_a_host_header_that_only_starts_like_a_local_host_is_refused(world, host):
    assert world.http.get("/health", headers={"Host": host}).status_code == 400


@pytest.mark.parametrize("host", ["localhost", "localhost:8501", "127.0.0.1:8777", "[::1]", "[::1]:8777"])
def test_local_host_headers_are_still_accepted(world, host):
    assert world.http.get("/health", headers={"Host": host}).status_code == 200


@pytest.mark.parametrize("url", ["https://github.com/o/r\n--upload-pack=x", "https://github.com/o/r --upload-pack=touch", "https://github.com\@127.0.0.1/r.git",
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

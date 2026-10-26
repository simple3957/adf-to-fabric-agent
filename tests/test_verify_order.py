"""verify --apply: a pipeline another one calls is judged after its caller has run, whatever order the names were given in."""

from __future__ import annotations

from adf2fabric.agent.deployment import verify as V


class _Client:
    def list_pipelines(self, _workspace):
        return {}


def test_called_pipelines_are_verified_after_their_callers_and_results_keep_the_requested_order(monkeypatch):
    seen = []

    def fake(client, workspace, name, **kw):
        seen.append(name)
        return V.Verification(name, "matches")

    monkeypatch.setattr(V, "verify_pipeline", fake)
    out = V.verify_all(_Client, "w", ["PL_Child", "PL_Other", "PL_Parent"], start=True, called={"PL_Child": ["PL_Parent"]})
    assert seen == ["PL_Other", "PL_Parent", "PL_Child"]  # alphabetical order used to read the child's OLD runs before the parent started new ones
    assert [v.pipeline for v in out] == ["PL_Child", "PL_Other", "PL_Parent"]


def test_without_starting_runs_the_order_is_unchanged(monkeypatch):
    seen = []
    monkeypatch.setattr(V, "verify_pipeline", lambda c, w, n, **kw: (seen.append(n), V.Verification(n, "matches"))[1])
    V.verify_all(_Client, "w", ["PL_Child", "PL_Parent"], start=False, workers=1, called={"PL_Child": ["PL_Parent"]})
    assert seen == ["PL_Child", "PL_Parent"]

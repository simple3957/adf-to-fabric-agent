"""The API behind the guided UI: authorisation, server-side redaction, failure states, progress, duplicate submissions, confirmations,
production mode. Every UI promise ("a hidden button is not protection", "nothing is shown that an ordinary view must not show") is
checked here, on the server, against real responses."""

from __future__ import annotations

import json


from adf2fabric.api import views


# =====================================================================================================================
# 1. authorisation of every new route (server-side; the UI hiding a button is not the protection)
# =====================================================================================================================


# =====================================================================================================================
# 2. server-side redaction
# =====================================================================================================================


def test_sanitize_drops_paths_secrets_and_code_and_bounds_strings(tmp_path):
    data = {"name": "PL_A", "source_path": "C:\\data\\x.json", "token": "abc", "connection_string": "Server=x;Password=hunter2", "artifact": "a/b",
            "nested": [{"file": "x", "message": f"opened {tmp_path}\\secret.txt with Bearer abcdefghijkl", "configuration": {"q": "SELECT 1"}}],
            "long": "x" * 5000, "count": 3}
    out = views.sanitize(data, (tmp_path,))
    blob = json.dumps(out)
    assert out["name"] == "PL_A" and out["count"] == 3 and len(out["long"]) <= 600
    assert "source_path" not in out and "token" not in out and "connection_string" not in out and "artifact" not in out
    assert "configuration" not in out["nested"][0] and "file" not in out["nested"][0]
    assert str(tmp_path) not in blob and "abcdefghijkl" not in blob and "hunter2" not in blob and "SELECT" not in blob


# =====================================================================================================================
# 3. failure states are explicit and say what to do
# =====================================================================================================================


# =====================================================================================================================
# 4. deploy: dry run first, confirmation, honest states
# =====================================================================================================================


# =====================================================================================================================
# 5. progress
# =====================================================================================================================


# =====================================================================================================================
# 6. duplicate submissions
# =====================================================================================================================


# =====================================================================================================================
# 8. running pipelines to prove them: opt-in, authorised, confirmed, audited
# =====================================================================================================================


# =====================================================================================================================
# 9. the consultation brief: who may answer, submit and approve; the run follows only an approved, current brief
# =====================================================================================================================


# =====================================================================================================================
# 10. waves and execution order
# =====================================================================================================================



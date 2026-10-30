"""Each element has one home: a definition that was copied around the code base stays defined once."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _sources(*roots: str):
    for r in roots:
        yield from (ROOT / r).rglob("*.py")


def test_the_guid_pattern_is_defined_once():
    pat = re.compile(r're\.compile\(r"\^\[0-9a-fA-F\]\{8\}')
    owners = [str(p.relative_to(ROOT)) for p in _sources("src", "ui") if pat.search(p.read_text(encoding="utf-8"))]
    assert owners == [str(Path("src/adf2fabric/ids.py"))], owners


def test_a_record_without_its_timestamps_has_one_implementation():
    owners = [str(p.relative_to(ROOT)) for p in _sources("src") if 'data.pop("updated_at", None)' in p.read_text(encoding="utf-8")]
    assert owners == [str(Path("src/adf2fabric/project/semantic.py"))], owners


def test_the_shared_helpers_behave():
    from adf2fabric.ids import is_guid, normalize_guid
    from adf2fabric.project.semantic import semantic
    from pydantic import BaseModel

    class M(BaseModel):
        a: int = 1
        created_at: str = "x"
        updated_at: str = "y"

    assert semantic(M()) == {"a": 1}
    g = "6c1485aa-525f-490f-b2a3-737d4421fd4d"
    assert is_guid(g) and not is_guid("nope") and not is_guid(None)
    assert normalize_guid(" {" + g.upper() + "} ") == g


def test_the_ui_has_one_list_of_ready_connection_states():
    import sys

    sys.path.insert(0, str(ROOT / "ui"))
    import journey
    import panels

    assert set(panels.READY) == set(journey.READY_STATES) and set(panels.PROBLEM) == set(journey.PROBLEM_STATES)

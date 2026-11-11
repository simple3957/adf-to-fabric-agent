"""Tests for the Consultation Agent.

The tests that used a stand-in (a fake service, a mock, demo data) were removed on 2026-11-11; what remains runs the real code.
Real Azure and Fabric calls are in ``tests/test_live.py`` (docs/testing.md).
"""

from __future__ import annotations

import pytest

from adf2fabric.agent.consultation import ConsultationAgent, ConsultationRequest
from adf2fabric.project.store import ProjectStore


# --- fixtures ---------------------------------------------------------------

@pytest.fixture
def store(tmp_path):
    return ProjectStore(tmp_path / "projects")


def _run(store, path, connectors=None, name="Demo"):
    agent = ConsultationAgent(store, connectors=connectors)
    return agent.run(ConsultationRequest(project_name=name, input_path=path))


# --- happy path -------------------------------------------------------------


# --- capability boundary ----------------------------------------------------


# --- readiness / blockers ---------------------------------------------------


# --- questionnaire validation ----------------------------------------------


# --- reproducibility / timestamp semantics ----------------------------------


# --- security ---------------------------------------------------------------


# --- boundary: no source/ access, no discovery ------------------------------


def test_consultation_agent_does_not_import_source_readers():
    """Static guard: the consultation agent must not pull in git / source readers."""
    import inspect

    import adf2fabric.agent.consultation.agent as agent_mod

    src = inspect.getsource(agent_mod)
    assert "repository.git" not in src
    assert "source_dir" not in src

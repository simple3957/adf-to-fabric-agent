"""The Intake Agent: authenticate, fetch a source repository, record metadata."""

from adf2fabric.agent.intake.agent import IntakeAgent
from adf2fabric.agent.intake.models import IntakeRequest, IntakeResult

__all__ = ["IntakeAgent", "IntakeRequest", "IntakeResult"]

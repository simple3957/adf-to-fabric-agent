"""The Intake Agent: obtain ADF source (GitHub or live Azure) and record metadata."""

from adf2fabric.agent.intake.agent import IntakeAgent
from adf2fabric.agent.intake.models import AzureIntakeRequest, IntakeRequest, IntakeResult

__all__ = ["AzureIntakeRequest", "IntakeAgent", "IntakeRequest", "IntakeResult"]

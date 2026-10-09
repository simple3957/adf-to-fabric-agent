"""The Consultation Agent: a deterministic readiness gate before Discovery."""

from adf2fabric.agent.consultation.agent import ConsultationAgent
from adf2fabric.agent.consultation.models import ConsultationRequest, ConsultationResult

__all__ = ["ConsultationAgent", "ConsultationRequest", "ConsultationResult"]

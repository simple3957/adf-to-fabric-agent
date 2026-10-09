"""Input/output models for the Consultation Agent.

Neither model carries a credential. The questionnaire supplies only credential
*references* (env-var names); actual secrets are resolved from the environment
at validation time and never round-trip through these models.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from adf2fabric.project.models import Consultation


class ConsultationRequest(BaseModel):
    """What the user asks Consultation to do."""

    project_name: str = Field(description="Existing project name (Intake must have run).")
    input_path: str = Field(description="Path to the consultation questionnaire YAML.")


class ConsultationResult(BaseModel):
    """The outcome of a consultation run. Contains no secrets."""

    success: bool
    consultation: Optional[Consultation] = None
    message: str = ""
    error: Optional[str] = None

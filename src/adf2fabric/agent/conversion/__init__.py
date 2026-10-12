"""The deterministic ADF -> Microsoft Fabric conversion stage (registry-driven, no LLM)."""

from adf2fabric.agent.conversion.agent import ConversionAgent
from adf2fabric.agent.conversion.models import ConversionRequest, ConversionResult

__all__ = ["ConversionAgent", "ConversionRequest", "ConversionResult"]

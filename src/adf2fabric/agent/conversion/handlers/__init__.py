"""Activity conversion handlers. Importing this package registers every rule."""

from adf2fabric.agent.conversion.handlers import compute, control, data, sql_web  # noqa: F401

__all__ = ["compute", "control", "data", "sql_web"]

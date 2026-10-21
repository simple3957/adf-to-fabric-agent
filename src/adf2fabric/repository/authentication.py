"""Credential handling for Intake.

Rules (non-negotiable):
- The token is read from the environment, never hardcoded.
- The token is never persisted to project files or metadata.
- The token is never printed. Any text that *might* contain it must be passed
  through :func:`redact` before it is logged or surfaced in an error.
"""

from __future__ import annotations

import os

DEFAULT_TOKEN_ENV = "GITHUB_TOKEN"


class MissingTokenError(Exception):
    """Raised when the expected token environment variable is not set."""


def resolve_token(env_var: str = DEFAULT_TOKEN_ENV) -> str:
    """Return the token from ``env_var`` or raise a clear, secret-free error."""
    token = os.environ.get(env_var)
    if not token:
        raise MissingTokenError(
            f"No GitHub token found. Set the {env_var} environment variable to a "
            "token with read access to the repository. The token is used only for "
            "the git operation and is never stored."
        )
    return token


def optional_token(env_var: str = DEFAULT_TOKEN_ENV) -> str | None:
    """The token from ``env_var``, or None: a public repository (or one git can already reach) needs none."""
    return os.environ.get(env_var) or None


NO_TOKEN_HINT = ("If the repository is private, set the {env} environment variable to a token with read access to it "
                 "(it is used only for the git operation and never stored).")


def redact(text: str, *secrets: str) -> str:
    """Mask every non-empty secret occurring in ``text`` with ``***``.

    Used to sanitize command output and error messages so a token embedded in a
    clone URL can never leak into logs or an :class:`IntakeResult`.
    """
    redacted = text
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "***")
    return redacted

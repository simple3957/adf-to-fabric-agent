"""Where the working folders (``projects/``, ``config/``, ``prompts/``) live.

In a source checkout they sit next to ``pyproject.toml``. After a normal ``pip install`` the package is in ``site-packages`` and
that guess would point into the Python installation, so there the current directory is used. ``ADF2FABRIC_HOME`` overrides both.
"""

from __future__ import annotations

import os
from pathlib import Path

ENV_HOME = "ADF2FABRIC_HOME"


def repo_root() -> Path:
    home = (os.environ.get(ENV_HOME) or "").strip()
    if home:
        return Path(home).expanduser().resolve()
    checkout = Path(__file__).resolve().parents[2]
    if (checkout / "pyproject.toml").exists():  # an editable install or a plain checkout
        return checkout
    return Path.cwd()

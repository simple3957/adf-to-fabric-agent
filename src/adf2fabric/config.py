"""Load the minimal Intake configuration from config/config.yaml.

Falls back to built-in defaults if the file is absent so the CLI still works
out of the box.
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "config.yaml"

_DEFAULTS = {
    "default_provider": "github",
    "default_branch": None,  # None: use the repository's own default branch
    "projects_root": "projects",
    "github_token_env": "GITHUB_TOKEN",
}


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> dict:
    config = dict(_DEFAULTS)
    if Path(path).exists():
        with Path(path).open(encoding="utf-8") as f:
            loaded = yaml.safe_load(f) or {}
        config.update({k: v for k, v in loaded.items() if v is not None})
    return config


def projects_root(config: dict) -> Path:
    """Resolve the configured projects root to an absolute path."""
    root = Path(config["projects_root"])
    return root if root.is_absolute() else REPO_ROOT / root

"""The content of a stored record without its volatile timestamps: what "did anything meaningful change?" compares."""

from __future__ import annotations

import json
from typing import Any


def semantic(model: Any) -> dict:
    data = json.loads(model.model_dump_json())
    data.pop("created_at", None)
    data.pop("updated_at", None)
    return data

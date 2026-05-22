"""Load secrets from .env and expand ${env:VAR} placeholders in config values."""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

_ENV_REF = re.compile(r"\$\{env:([A-Z0-9_]+)\}")


def load_env(repo_root: Path) -> None:
    """Load .env from the repo root (no-op if missing)."""
    env_file = repo_root / ".env"
    if env_file.exists():
        load_dotenv(env_file, override=False)


def expand(value: Any) -> Any:
    """Recursively expand ${env:VAR} references inside strings/lists/dicts."""
    if isinstance(value, str):
        def _sub(m: re.Match) -> str:
            name = m.group(1)
            v = os.getenv(name)
            if v is None:
                raise KeyError(f"Environment variable {name!r} referenced in config is not set")
            return v
        return _ENV_REF.sub(_sub, value)
    if isinstance(value, list):
        return [expand(v) for v in value]
    if isinstance(value, dict):
        return {k: expand(v) for k, v in value.items()}
    return value

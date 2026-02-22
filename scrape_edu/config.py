"""Layered configuration: YAML < .env < CLI args."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
import os


def load_config(
    config_path: Path | None = None,
    cli_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Load configuration with layered precedence.

    Priority (highest to lowest):
    1. CLI argument overrides
    2. Environment variables (.env)
    3. YAML config file

    Args:
        config_path: Path to YAML config file. Defaults to config/default.yaml
        cli_overrides: Dict of CLI argument overrides (e.g. {"workers": 8})

    Returns:
        Merged configuration dict.
    """
    # 1. Load YAML defaults
    if config_path is None:
        config_path = Path("config/default.yaml")

    config: dict[str, Any] = {}
    if config_path.exists():
        with open(config_path) as f:
            config = yaml.safe_load(f) or {}

    # 2. Load .env and apply environment variable overrides
    load_dotenv()
    env_mappings: dict[str, tuple[str, ...]] = {
        "SERPER_API_KEY": ("search", "api_key"),
        "OUTPUT_DIR": ("output_dir",),
        "IPEDS_DIR": ("ipeds_dir",),
    }
    for env_var, config_path_tuple in env_mappings.items():
        value = os.environ.get(env_var)
        if value:
            _set_nested(config, config_path_tuple, value)

    # 3. Apply CLI overrides (only non-None values)
    if cli_overrides:
        for key, value in cli_overrides.items():
            if value is not None:
                config[key] = value

    return config


def _set_nested(d: dict, keys: tuple[str, ...], value: Any) -> None:
    """Set a value in a nested dict using a tuple of keys."""
    for key in keys[:-1]:
        d = d.setdefault(key, {})
    d[keys[-1]] = value

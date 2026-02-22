"""Tests for scrape_edu.config module."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from scrape_edu.config import load_config, _set_nested


# ======================================================================
# _set_nested tests
# ======================================================================


class TestSetNested:
    """Test the _set_nested helper function."""

    def test_single_key(self) -> None:
        d: dict = {}
        _set_nested(d, ("output_dir",), "/tmp/out")
        assert d == {"output_dir": "/tmp/out"}

    def test_nested_keys(self) -> None:
        d: dict = {}
        _set_nested(d, ("search", "api_key"), "abc123")
        assert d == {"search": {"api_key": "abc123"}}

    def test_deeply_nested_keys(self) -> None:
        d: dict = {}
        _set_nested(d, ("a", "b", "c"), "value")
        assert d == {"a": {"b": {"c": "value"}}}

    def test_preserves_existing_sibling_keys(self) -> None:
        d: dict = {"search": {"provider": "serper"}}
        _set_nested(d, ("search", "api_key"), "abc123")
        assert d == {"search": {"provider": "serper", "api_key": "abc123"}}

    def test_overwrites_existing_value(self) -> None:
        d: dict = {"output_dir": "/old"}
        _set_nested(d, ("output_dir",), "/new")
        assert d == {"output_dir": "/new"}


# ======================================================================
# load_config tests
# ======================================================================


class TestLoadConfigYAML:
    """Test loading configuration from YAML files."""

    def test_loads_yaml_defaults(self, tmp_path: Path) -> None:
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump({"workers": 10, "retries": 5}))

        config = load_config(config_path=config_file)
        assert config["workers"] == 10
        assert config["retries"] == 5

    def test_loads_nested_yaml(self, tmp_path: Path) -> None:
        config_file = tmp_path / "test.yaml"
        config_data = {
            "search": {"provider": "serper", "queries_per_school": 3},
            "logging": {"level": "DEBUG"},
        }
        config_file.write_text(yaml.dump(config_data))

        config = load_config(config_path=config_file)
        assert config["search"]["provider"] == "serper"
        assert config["search"]["queries_per_school"] == 3
        assert config["logging"]["level"] == "DEBUG"

    def test_missing_config_file_returns_empty(self, tmp_path: Path) -> None:
        config_file = tmp_path / "nonexistent.yaml"
        config = load_config(config_path=config_file)
        assert config == {}

    def test_empty_yaml_file(self, tmp_path: Path) -> None:
        config_file = tmp_path / "empty.yaml"
        config_file.write_text("")

        config = load_config(config_path=config_file)
        assert config == {}

    def test_yaml_with_null_value(self, tmp_path: Path) -> None:
        config_file = tmp_path / "null.yaml"
        config_file.write_text("workers: null\nretries: 3\n")

        config = load_config(config_path=config_file)
        assert config["workers"] is None
        assert config["retries"] == 3


class TestLoadConfigEnvVars:
    """Test environment variable overrides."""

    def test_serper_api_key_override(self, tmp_path: Path, monkeypatch) -> None:
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump({"search": {"provider": "serper"}}))

        monkeypatch.setenv("SERPER_API_KEY", "test-key-123")

        config = load_config(config_path=config_file)
        assert config["search"]["api_key"] == "test-key-123"
        assert config["search"]["provider"] == "serper"

    def test_output_dir_override(self, tmp_path: Path, monkeypatch) -> None:
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump({"output_dir": "./output"}))

        monkeypatch.setenv("OUTPUT_DIR", "/custom/output")

        config = load_config(config_path=config_file)
        assert config["output_dir"] == "/custom/output"

    def test_ipeds_dir_override(self, tmp_path: Path, monkeypatch) -> None:
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump({"ipeds_dir": "./data/ipeds"}))

        monkeypatch.setenv("IPEDS_DIR", "/custom/ipeds")

        config = load_config(config_path=config_file)
        assert config["ipeds_dir"] == "/custom/ipeds"

    def test_empty_env_var_not_applied(self, tmp_path: Path, monkeypatch) -> None:
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump({"output_dir": "./output"}))

        monkeypatch.setenv("OUTPUT_DIR", "")

        config = load_config(config_path=config_file)
        assert config["output_dir"] == "./output"

    def test_env_var_creates_nested_structure(self, tmp_path: Path, monkeypatch) -> None:
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump({}))  # Empty config

        monkeypatch.setenv("SERPER_API_KEY", "my-key")

        config = load_config(config_path=config_file)
        assert config["search"]["api_key"] == "my-key"

    def test_unset_env_vars_not_applied(self, tmp_path: Path, monkeypatch) -> None:
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump({"output_dir": "./output"}))

        # Ensure the env vars are NOT set
        monkeypatch.delenv("SERPER_API_KEY", raising=False)
        monkeypatch.delenv("OUTPUT_DIR", raising=False)
        monkeypatch.delenv("IPEDS_DIR", raising=False)

        config = load_config(config_path=config_file)
        assert config["output_dir"] == "./output"
        assert "search" not in config or "api_key" not in config.get("search", {})


class TestLoadConfigCLIOverrides:
    """Test CLI argument overrides."""

    def test_cli_overrides_applied(self, tmp_path: Path) -> None:
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump({"workers": 5}))

        config = load_config(
            config_path=config_file,
            cli_overrides={"workers": 12},
        )
        assert config["workers"] == 12

    def test_cli_none_values_ignored(self, tmp_path: Path) -> None:
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump({"workers": 5}))

        config = load_config(
            config_path=config_file,
            cli_overrides={"workers": None},
        )
        assert config["workers"] == 5

    def test_cli_adds_new_keys(self, tmp_path: Path) -> None:
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump({"workers": 5}))

        config = load_config(
            config_path=config_file,
            cli_overrides={"new_option": "hello"},
        )
        assert config["new_option"] == "hello"
        assert config["workers"] == 5

    def test_empty_cli_overrides(self, tmp_path: Path) -> None:
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump({"workers": 5}))

        config = load_config(config_path=config_file, cli_overrides={})
        assert config["workers"] == 5

    def test_no_cli_overrides(self, tmp_path: Path) -> None:
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump({"workers": 5}))

        config = load_config(config_path=config_file, cli_overrides=None)
        assert config["workers"] == 5


class TestLoadConfigPrecedence:
    """Test that precedence order is CLI > env > YAML."""

    def test_env_overrides_yaml(self, tmp_path: Path, monkeypatch) -> None:
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump({"output_dir": "./yaml-output"}))

        monkeypatch.setenv("OUTPUT_DIR", "/env-output")

        config = load_config(config_path=config_file)
        assert config["output_dir"] == "/env-output"

    def test_cli_overrides_yaml(self, tmp_path: Path) -> None:
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump({"workers": 5}))

        config = load_config(
            config_path=config_file,
            cli_overrides={"workers": 20},
        )
        assert config["workers"] == 20

    def test_cli_overrides_env(self, tmp_path: Path, monkeypatch) -> None:
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump({"output_dir": "./yaml-output"}))

        monkeypatch.setenv("OUTPUT_DIR", "/env-output")

        config = load_config(
            config_path=config_file,
            cli_overrides={"output_dir": "/cli-output"},
        )
        assert config["output_dir"] == "/cli-output"

    def test_full_precedence_chain(self, tmp_path: Path, monkeypatch) -> None:
        """CLI > env > YAML, all three layers active."""
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump({
            "workers": 5,
            "output_dir": "./yaml-output",
            "ipeds_dir": "./yaml-ipeds",
        }))

        monkeypatch.setenv("OUTPUT_DIR", "/env-output")
        monkeypatch.setenv("IPEDS_DIR", "/env-ipeds")
        monkeypatch.delenv("SERPER_API_KEY", raising=False)

        config = load_config(
            config_path=config_file,
            cli_overrides={"workers": 20, "output_dir": "/cli-output"},
        )

        # workers: CLI overrides YAML
        assert config["workers"] == 20
        # output_dir: CLI overrides env which overrides YAML
        assert config["output_dir"] == "/cli-output"
        # ipeds_dir: env overrides YAML (no CLI override)
        assert config["ipeds_dir"] == "/env-ipeds"


class TestLoadConfigDefaultPath:
    """Test behavior with the default config path."""

    def test_default_path_used_when_none(self, monkeypatch) -> None:
        """When config_path is None, it uses config/default.yaml."""
        # Clear env vars so they don't interfere
        monkeypatch.delenv("SERPER_API_KEY", raising=False)
        monkeypatch.delenv("OUTPUT_DIR", raising=False)
        monkeypatch.delenv("IPEDS_DIR", raising=False)

        # Run from the project root so config/default.yaml exists
        monkeypatch.chdir("/Users/stacey/Documents/GitHub/scrape_edu")

        config = load_config()
        # Should have loaded the default YAML with known keys
        assert config.get("workers") == 5
        assert config.get("output_dir") == "./output"

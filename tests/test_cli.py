"""Tests for scrape_edu.cli module."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from scrape_edu.cli import main, cmd_status, cmd_run, cmd_rescrape
from scrape_edu.data.manifest import ManifestManager, SchoolStatus


# ======================================================================
# Test: no command shows help and returns 1
# ======================================================================


class TestMainNoCommand:
    """Test behavior when no subcommand is given."""

    def test_no_args_prints_help(self, capsys) -> None:
        result = main([])
        assert result == 1
        captured = capsys.readouterr()
        assert "usage:" in captured.out.lower() or "scrape_edu" in captured.out

    def test_no_args_returns_1(self) -> None:
        result = main([])
        assert result == 1


# ======================================================================
# Test: status command
# ======================================================================


class TestCmdStatus:
    """Test the 'status' subcommand."""

    def test_status_no_ipeds_data(self, capsys, tmp_path: Path) -> None:
        """Status should show a helpful message when IPEDS data is missing."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            f"ipeds_dir: '{tmp_path / 'ipeds'}'\n"
            f"output_dir: '{tmp_path / 'output'}'\n"
            "logging:\n  level: WARNING\n"
        )

        result = main(["status", "--config", str(config_file)])
        assert result == 0

        captured = capsys.readouterr()
        assert "IPEDS data not found" in captured.out
        assert "download_ipeds" in captured.out

    def test_status_with_ipeds_no_manifest(
        self, capsys, tmp_path: Path, monkeypatch
    ) -> None:
        """Status shows school count even without a manifest."""
        # Create minimal IPEDS data
        ipeds_dir = tmp_path / "ipeds"
        ipeds_dir.mkdir()
        _write_hd_csv(
            ipeds_dir / "hd2023.csv",
            [
                {
                    "UNITID": "100000",
                    "INSTNM": "Test University",
                    "WEBADDR": "test.edu",
                    "CITY": "TestCity",
                    "STABBR": "TS",
                    "ICLEVEL": "1",
                }
            ],
        )
        _write_c_csv(
            ipeds_dir / "c2023_a.csv",
            [{"UNITID": "100000", "CIPCODE": "11.0101", "AWLEVEL": "5"}],
        )

        output_dir = tmp_path / "output"
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            f"ipeds_dir: '{ipeds_dir}'\n"
            f"output_dir: '{output_dir}'\n"
            "logging:\n  level: WARNING\n"
        )

        result = main(["status", "--config", str(config_file)])
        assert result == 0

        captured = capsys.readouterr()
        assert "Schools loaded from IPEDS: 1" in captured.out
        assert "No manifest found" in captured.out

    def test_status_with_manifest(self, capsys, tmp_path: Path) -> None:
        """Status shows manifest summary when available."""
        ipeds_dir = tmp_path / "ipeds"
        # Don't create IPEDS data; we just want to test manifest display

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Create a manifest with some schools
        mm = ManifestManager(output_dir)
        mm.init_school("mit", {"name": "MIT"})
        mm.init_school("stanford", {"name": "Stanford"})
        mm.update_school_status("mit", SchoolStatus.COMPLETED)

        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            f"ipeds_dir: '{ipeds_dir}'\n"
            f"output_dir: '{output_dir}'\n"
            "logging:\n  level: WARNING\n"
        )

        result = main(["status", "--config", str(config_file)])
        assert result == 0

        captured = capsys.readouterr()
        assert "Manifest status:" in captured.out
        assert "completed: 1" in captured.out
        assert "pending: 1" in captured.out
        assert "total: 2" in captured.out


# ======================================================================
# Test: run command
# ======================================================================


class TestCmdRun:
    """Test the 'run' subcommand."""

    def _make_config(self, tmp_path: Path, **overrides) -> Path:
        """Create a config file pointing at tmp dirs."""
        ipeds_dir = tmp_path / "ipeds"
        ipeds_dir.mkdir(exist_ok=True)
        output_dir = tmp_path / "output"
        content = (
            f"ipeds_dir: '{ipeds_dir}'\n"
            f"output_dir: '{output_dir}'\n"
            "workers: 5\n"
            "logging:\n  level: WARNING\n"
        )
        for k, v in overrides.items():
            content += f"{k}: {v}\n"
        config_file = tmp_path / "config.yaml"
        config_file.write_text(content)
        return config_file

    def test_run_no_ipeds_data(self, capsys, tmp_path: Path) -> None:
        """Run without IPEDS data shows helpful error."""
        config_file = self._make_config(tmp_path)
        result = main(["run", "--config", str(config_file)])
        assert result == 1
        captured = capsys.readouterr()
        assert "IPEDS data not found" in captured.out

    @patch("scrape_edu.cli.Orchestrator")
    @patch("scrape_edu.cli.load_schools")
    def test_run_default(self, mock_load, mock_orch_cls, capsys, tmp_path: Path) -> None:
        mock_load.return_value = []
        mock_orch = mock_orch_cls.return_value
        mock_orch.run.return_value = {"completed": 0, "failed": 0, "skipped": 0}

        config_file = self._make_config(tmp_path)
        result = main(["run", "--config", str(config_file)])
        assert result == 0
        mock_orch_cls.assert_called_once()
        assert mock_orch_cls.call_args.kwargs["workers"] == 5

    @patch("scrape_edu.cli.Orchestrator")
    @patch("scrape_edu.cli.load_schools")
    def test_run_with_workers(self, mock_load, mock_orch_cls, capsys, tmp_path: Path) -> None:
        mock_load.return_value = []
        mock_orch = mock_orch_cls.return_value
        mock_orch.run.return_value = {"completed": 0, "failed": 0, "skipped": 0}

        config_file = self._make_config(tmp_path)
        result = main(["run", "--workers", "8", "--config", str(config_file)])
        assert result == 0
        assert mock_orch_cls.call_args.kwargs["workers"] == 8

    @patch("scrape_edu.cli.Orchestrator")
    @patch("scrape_edu.cli.load_schools")
    def test_run_with_schools_filter(self, mock_load, mock_orch_cls, capsys, tmp_path: Path) -> None:
        mock_load.return_value = []
        mock_orch = mock_orch_cls.return_value
        mock_orch.run.return_value = {"completed": 0, "failed": 0, "skipped": 0}

        config_file = self._make_config(tmp_path)
        result = main(["run", "--schools", "mit,stanford", "--config", str(config_file)])
        assert result == 0
        call_kwargs = mock_orch.run.call_args.kwargs
        assert call_kwargs["schools_filter"] == ["mit", "stanford"]

    @patch("scrape_edu.cli.Orchestrator")
    @patch("scrape_edu.cli.load_schools")
    def test_run_with_phase_filter(self, mock_load, mock_orch_cls, capsys, tmp_path: Path) -> None:
        from scrape_edu.pipeline.phases import Phase
        mock_load.return_value = []
        mock_orch = mock_orch_cls.return_value
        mock_orch.run.return_value = {"completed": 0, "failed": 0, "skipped": 0}

        config_file = self._make_config(tmp_path)
        result = main(["run", "--phase", "discovery", "--config", str(config_file)])
        assert result == 0
        call_kwargs = mock_orch.run.call_args.kwargs
        assert call_kwargs["phases_filter"] == [Phase.DISCOVERY]

    def test_run_invalid_phase_rejected(self) -> None:
        """An invalid --phase value should cause argparse to error."""
        with pytest.raises(SystemExit) as exc_info:
            main(["run", "--phase", "invalid_phase"])
        assert exc_info.value.code == 2

    @patch("scrape_edu.cli.Orchestrator")
    @patch("scrape_edu.cli.load_schools")
    def test_run_shows_results(self, mock_load, mock_orch_cls, capsys, tmp_path: Path) -> None:
        mock_load.return_value = []
        mock_orch = mock_orch_cls.return_value
        mock_orch.run.return_value = {"completed": 3, "failed": 1, "skipped": 0}

        config_file = self._make_config(tmp_path)
        result = main(["run", "--config", str(config_file)])
        assert result == 0
        captured = capsys.readouterr()
        assert "Completed: 3" in captured.out
        assert "Failed:    1" in captured.out


# ======================================================================
# Test: rescrape command
# ======================================================================


class TestCmdRescrape:
    """Test the 'rescrape' subcommand."""

    def test_rescrape_no_manifest(self, capsys, tmp_path: Path, monkeypatch) -> None:
        """Rescrape should fail gracefully when no manifest exists."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            f"output_dir: '{tmp_path / 'output'}'\n"
        )
        # Patch load_config to use our temp config
        monkeypatch.chdir(tmp_path)
        with patch("scrape_edu.cli.load_config", return_value={"output_dir": str(tmp_path / "output")}):
            result = main(["rescrape", "--all"])
        assert result == 1

        captured = capsys.readouterr()
        assert "No manifest found" in captured.out

    def test_rescrape_all(self, capsys, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        mm = ManifestManager(output_dir)
        mm.init_school("mit", {"name": "MIT"})
        mm.init_school("stanford", {"name": "Stanford"})
        mm.update_school_status("mit", SchoolStatus.COMPLETED)
        mm.update_school_status("stanford", SchoolStatus.COMPLETED)

        with patch("scrape_edu.cli.load_config", return_value={"output_dir": str(output_dir)}):
            result = main(["rescrape", "--all"])
        assert result == 0

        captured = capsys.readouterr()
        assert "Flagged 2 schools" in captured.out

        # Verify the manifest was actually updated
        mm2 = ManifestManager(output_dir)
        assert mm2.get_school_status("mit") == SchoolStatus.FLAGGED_RESCRAPE
        assert mm2.get_school_status("stanford") == SchoolStatus.FLAGGED_RESCRAPE

    def test_rescrape_specific_schools(self, capsys, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        mm = ManifestManager(output_dir)
        mm.init_school("mit", {"name": "MIT"})
        mm.init_school("stanford", {"name": "Stanford"})
        mm.update_school_status("mit", SchoolStatus.COMPLETED)
        mm.update_school_status("stanford", SchoolStatus.COMPLETED)

        with patch("scrape_edu.cli.load_config", return_value={"output_dir": str(output_dir)}):
            result = main(["rescrape", "--schools", "mit"])
        assert result == 0

        captured = capsys.readouterr()
        assert "Flagged 1 schools" in captured.out

        mm2 = ManifestManager(output_dir)
        assert mm2.get_school_status("mit") == SchoolStatus.FLAGGED_RESCRAPE
        assert mm2.get_school_status("stanford") == SchoolStatus.COMPLETED

    def test_rescrape_no_flags_shows_error(self, capsys, tmp_path: Path) -> None:
        """Rescrape without --schools or --all should show an error."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        mm = ManifestManager(output_dir)
        mm.init_school("mit", {"name": "MIT"})

        with patch("scrape_edu.cli.load_config", return_value={"output_dir": str(output_dir)}):
            result = main(["rescrape"])
        assert result == 1

        captured = capsys.readouterr()
        assert "Specify --schools or --all" in captured.out


# ======================================================================
# Test: argument parsing edge cases
# ======================================================================


class TestArgParsing:
    """Test edge cases in argument parsing."""

    def test_help_flag_exits_with_zero(self) -> None:
        with pytest.raises(SystemExit) as exc_info:
            main(["--help"])
        assert exc_info.value.code == 0

    def test_run_help_flag(self) -> None:
        with pytest.raises(SystemExit) as exc_info:
            main(["run", "--help"])
        assert exc_info.value.code == 0

    def test_status_help_flag(self) -> None:
        with pytest.raises(SystemExit) as exc_info:
            main(["status", "--help"])
        assert exc_info.value.code == 0

    def test_rescrape_help_flag(self) -> None:
        with pytest.raises(SystemExit) as exc_info:
            main(["rescrape", "--help"])
        assert exc_info.value.code == 0

    def test_unknown_command_exits(self) -> None:
        with pytest.raises(SystemExit) as exc_info:
            main(["unknown_command"])
        assert exc_info.value.code == 2


# ======================================================================
# Test: --dry-run mode
# ======================================================================


class TestDryRun:
    """Test the --dry-run flag on the 'run' subcommand."""

    def _make_ipeds(self, tmp_path: Path, schools: list[dict] | None = None) -> Path:
        """Create IPEDS data dir with one or more schools."""
        ipeds_dir = tmp_path / "ipeds"
        ipeds_dir.mkdir(exist_ok=True)
        if schools is None:
            schools = [
                {
                    "UNITID": "100000",
                    "INSTNM": "Test University",
                    "WEBADDR": "test.edu",
                    "CITY": "TestCity",
                    "STABBR": "TS",
                    "ICLEVEL": "1",
                }
            ]
        _write_hd_csv(ipeds_dir / "hd2023.csv", schools)
        _write_c_csv(
            ipeds_dir / "c2023_a.csv",
            [{"UNITID": s["UNITID"], "CIPCODE": "11.0101", "AWLEVEL": "5"} for s in schools],
        )
        return ipeds_dir

    def _make_config(self, tmp_path: Path, ipeds_dir: Path, **extra) -> Path:
        """Create a config file pointing to tmp dirs."""
        output_dir = tmp_path / "output"
        content = (
            f"ipeds_dir: '{ipeds_dir}'\n"
            f"output_dir: '{output_dir}'\n"
            "workers: 5\n"
            "rate_limit:\n  min_delay: 1.0\n  max_delay: 3.0\n"
            "logging:\n  level: WARNING\n"
        )
        for k, v in extra.items():
            content += f"{k}: {v}\n"
        config_file = tmp_path / "config.yaml"
        config_file.write_text(content)
        return config_file

    def test_dry_run_returns_zero(self, capsys, tmp_path: Path) -> None:
        """--dry-run should return 0 and not crash."""
        ipeds_dir = self._make_ipeds(tmp_path)
        config_file = self._make_config(tmp_path, ipeds_dir)
        result = main(["run", "--dry-run", "--config", str(config_file)])
        assert result == 0

    def test_dry_run_shows_header(self, capsys, tmp_path: Path) -> None:
        """Output should contain the DRY RUN banner."""
        ipeds_dir = self._make_ipeds(tmp_path)
        config_file = self._make_config(tmp_path, ipeds_dir)
        main(["run", "--dry-run", "--config", str(config_file)])
        captured = capsys.readouterr()
        assert "DRY RUN" in captured.out

    def test_dry_run_shows_config(self, capsys, tmp_path: Path) -> None:
        """Output should display workers and rate limits."""
        ipeds_dir = self._make_ipeds(tmp_path)
        config_file = self._make_config(tmp_path, ipeds_dir)
        main(["run", "--dry-run", "--config", str(config_file)])
        captured = capsys.readouterr()
        assert "Workers:          5" in captured.out
        assert "1.0" in captured.out and "3.0" in captured.out
        assert str(tmp_path / "output") in captured.out

    def test_dry_run_shows_all_phases(self, capsys, tmp_path: Path) -> None:
        """Without --phase, all 5 phases should be listed."""
        ipeds_dir = self._make_ipeds(tmp_path)
        config_file = self._make_config(tmp_path, ipeds_dir)
        main(["run", "--dry-run", "--config", str(config_file)])
        captured = capsys.readouterr()
        assert "Phases to run (5):" in captured.out
        for phase_name in ["robots", "discovery", "catalog", "faculty", "syllabi"]:
            assert f"- {phase_name}" in captured.out

    def test_dry_run_with_phase_filter(self, capsys, tmp_path: Path) -> None:
        """--phase should restrict to a single phase."""
        ipeds_dir = self._make_ipeds(tmp_path)
        config_file = self._make_config(tmp_path, ipeds_dir)
        main(["run", "--dry-run", "--phase", "discovery", "--config", str(config_file)])
        captured = capsys.readouterr()
        assert "Phases to run (1):" in captured.out
        assert "- discovery" in captured.out
        # Other phases should NOT appear as bullet items
        assert "- catalog" not in captured.out

    def test_dry_run_shows_schools(self, capsys, tmp_path: Path) -> None:
        """Output should list the school slugs."""
        ipeds_dir = self._make_ipeds(tmp_path)
        config_file = self._make_config(tmp_path, ipeds_dir)
        main(["run", "--dry-run", "--config", str(config_file)])
        captured = capsys.readouterr()
        assert "Schools to process (1):" in captured.out
        assert "test-university" in captured.out
        assert "Test University" in captured.out

    def test_dry_run_with_schools_filter(self, capsys, tmp_path: Path) -> None:
        """--schools filter should restrict displayed schools."""
        schools_data = [
            {"UNITID": "100000", "INSTNM": "Alpha University", "WEBADDR": "alpha.edu", "CITY": "A", "STABBR": "AA", "ICLEVEL": "1"},
            {"UNITID": "100001", "INSTNM": "Beta University", "WEBADDR": "beta.edu", "CITY": "B", "STABBR": "BB", "ICLEVEL": "1"},
        ]
        ipeds_dir = self._make_ipeds(tmp_path, schools_data)
        config_file = self._make_config(tmp_path, ipeds_dir)
        main(["run", "--dry-run", "--schools", "alpha-university", "--config", str(config_file)])
        captured = capsys.readouterr()
        assert "1 of 2 loaded" in captured.out
        assert "alpha-university" in captured.out

    def test_dry_run_unknown_school_slug_warning(self, capsys, tmp_path: Path) -> None:
        """Filtering by a slug not in IPEDS should show a warning."""
        ipeds_dir = self._make_ipeds(tmp_path)
        config_file = self._make_config(tmp_path, ipeds_dir)
        main(["run", "--dry-run", "--schools", "nonexistent-school", "--config", str(config_file)])
        captured = capsys.readouterr()
        assert "WARNING" in captured.out
        assert "nonexistent-school" in captured.out

    def test_dry_run_truncates_many_schools(self, capsys, tmp_path: Path) -> None:
        """When there are >20 schools, only first 20 slugs should be shown."""
        schools_data = [
            {
                "UNITID": str(100000 + i),
                "INSTNM": f"University Number {i:03d}",
                "WEBADDR": f"uni{i}.edu",
                "CITY": "City",
                "STABBR": "ST",
                "ICLEVEL": "1",
            }
            for i in range(25)
        ]
        ipeds_dir = self._make_ipeds(tmp_path, schools_data)
        config_file = self._make_config(tmp_path, ipeds_dir)
        main(["run", "--dry-run", "--config", str(config_file)])
        captured = capsys.readouterr()
        assert "Schools to process (25):" in captured.out
        assert "... and 5 more" in captured.out

    def test_dry_run_shows_manifest_status(self, capsys, tmp_path: Path) -> None:
        """When a manifest exists, dry-run should show the status breakdown."""
        ipeds_dir = self._make_ipeds(tmp_path)
        config_file = self._make_config(tmp_path, ipeds_dir)

        # Create a manifest with some entries
        output_dir = tmp_path / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        mm = ManifestManager(output_dir)
        mm.init_school("test-university", {"name": "Test University"})
        mm.update_school_status("test-university", SchoolStatus.COMPLETED)

        main(["run", "--dry-run", "--config", str(config_file)])
        captured = capsys.readouterr()
        assert "Existing manifest status:" in captured.out
        assert "completed: 1" in captured.out

    def test_dry_run_no_manifest_message(self, capsys, tmp_path: Path) -> None:
        """Without an existing manifest, dry-run says 'first run'."""
        ipeds_dir = self._make_ipeds(tmp_path)
        config_file = self._make_config(tmp_path, ipeds_dir)
        main(["run", "--dry-run", "--config", str(config_file)])
        captured = capsys.readouterr()
        assert "No existing manifest found" in captured.out

    @patch("scrape_edu.cli.Orchestrator")
    @patch("scrape_edu.cli.HttpClient")
    def test_dry_run_does_not_create_http_or_orchestrator(
        self, mock_http_cls, mock_orch_cls, capsys, tmp_path: Path
    ) -> None:
        """--dry-run must NOT instantiate HttpClient or Orchestrator."""
        ipeds_dir = self._make_ipeds(tmp_path)
        config_file = self._make_config(tmp_path, ipeds_dir)
        result = main(["run", "--dry-run", "--config", str(config_file)])
        assert result == 0
        mock_http_cls.assert_not_called()
        mock_orch_cls.assert_not_called()

    def test_dry_run_with_workers_override(self, capsys, tmp_path: Path) -> None:
        """--workers override should be reflected in dry-run output."""
        ipeds_dir = self._make_ipeds(tmp_path)
        config_file = self._make_config(tmp_path, ipeds_dir)
        main(["run", "--dry-run", "--workers", "10", "--config", str(config_file)])
        captured = capsys.readouterr()
        assert "Workers:          10" in captured.out


# ======================================================================
# Helpers (reused from test_ipeds_loader.py)
# ======================================================================


def _write_hd_csv(path: Path, rows: list[dict]) -> None:
    """Write a minimal HD (institutional characteristics) CSV."""
    cols = ["UNITID", "INSTNM", "WEBADDR", "CITY", "STABBR", "ICLEVEL"]
    lines = [",".join(cols)]
    for row in rows:
        lines.append(",".join(str(row.get(c, "")) for c in cols))
    path.write_text("\n".join(lines) + "\n", encoding="latin-1")


def _write_c_csv(path: Path, rows: list[dict]) -> None:
    """Write a minimal C (completions) CSV."""
    cols = ["UNITID", "CIPCODE", "AWLEVEL"]
    lines = [",".join(cols)]
    for row in rows:
        lines.append(",".join(str(row.get(c, "")) for c in cols))
    path.write_text("\n".join(lines) + "\n", encoding="latin-1")

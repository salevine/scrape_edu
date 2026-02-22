"""Tests for scrape_edu.interactive module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from scrape_edu.interactive import (
    DISPATCH,
    MAIN_MENU_CHOICES,
    interactive_menu,
    show_main_menu,
    run_setup,
    run_pipeline_flow,
    check_status_flow,
    rescrape_flow,
    analyze_flow,
    dry_run_flow,
)


# ======================================================================
# Test: main menu display and choice
# ======================================================================


class TestShowMainMenu:
    """Test that show_main_menu renders and returns a valid choice."""

    @patch("scrape_edu.interactive.Prompt.ask", return_value="1")
    @patch("scrape_edu.interactive.console")
    def test_returns_user_choice(self, _mock_console, mock_ask) -> None:
        result = show_main_menu()
        assert result == "1"

    @patch("scrape_edu.interactive.Prompt.ask", return_value="7")
    @patch("scrape_edu.interactive.console")
    def test_returns_exit_choice(self, _mock_console, mock_ask) -> None:
        result = show_main_menu()
        assert result == "7"

    def test_all_dispatch_keys_in_menu(self) -> None:
        """Every DISPATCH key should appear in MAIN_MENU_CHOICES."""
        for key in DISPATCH:
            assert key in MAIN_MENU_CHOICES


# ======================================================================
# Test: interactive_menu loop
# ======================================================================


class TestInteractiveMenu:
    """Test the main menu loop dispatches correctly and exits."""

    @patch("scrape_edu.interactive.show_main_menu", return_value="7")
    @patch("scrape_edu.interactive.console")
    def test_exit_immediately(self, _mock_console, mock_menu) -> None:
        result = interactive_menu()
        assert result == 0

    @patch("scrape_edu.interactive.show_main_menu", side_effect=["3", "7"])
    @patch("scrape_edu.interactive.console")
    def test_dispatches_status_then_exits(self, _mock_console, mock_menu) -> None:
        mock_handler = MagicMock()
        with patch.dict("scrape_edu.interactive.DISPATCH", {"3": mock_handler}):
            result = interactive_menu()
        assert result == 0
        mock_handler.assert_called_once()

    @patch("scrape_edu.interactive.show_main_menu", side_effect=["1", "2", "7"])
    @patch("scrape_edu.interactive.console")
    def test_dispatches_multiple_then_exits(self, _mock_console, mock_menu) -> None:
        mock_setup = MagicMock()
        mock_run = MagicMock()
        with patch.dict("scrape_edu.interactive.DISPATCH", {"1": mock_setup, "2": mock_run}):
            result = interactive_menu()
        assert result == 0
        mock_setup.assert_called_once()
        mock_run.assert_called_once()


# ======================================================================
# Test: run pipeline flow
# ======================================================================


class TestRunPipelineFlow:
    """Test the run-pipeline interactive sub-flow."""

    @patch("scrape_edu.interactive.subprocess.run")
    @patch("scrape_edu.interactive.Confirm.ask", return_value=True)
    @patch("scrape_edu.interactive.Prompt.ask", side_effect=["", "all"])
    @patch("scrape_edu.interactive.IntPrompt.ask", return_value=5)
    @patch("scrape_edu.interactive.console")
    def test_default_run(self, _con, mock_int, mock_prompt, mock_confirm, mock_subproc) -> None:
        run_pipeline_flow()
        mock_subproc.assert_called_once()
        cmd = mock_subproc.call_args[0][0]
        assert "run" in cmd
        assert "--workers" in cmd
        assert "5" in cmd
        # No --schools or --phase when defaults used
        assert "--schools" not in cmd
        assert "--phase" not in cmd

    @patch("scrape_edu.interactive.subprocess.run")
    @patch("scrape_edu.interactive.Confirm.ask", return_value=True)
    @patch("scrape_edu.interactive.Prompt.ask", side_effect=["mit,stanford", "discovery"])
    @patch("scrape_edu.interactive.IntPrompt.ask", return_value=3)
    @patch("scrape_edu.interactive.console")
    def test_with_filters(self, _con, mock_int, mock_prompt, mock_confirm, mock_subproc) -> None:
        run_pipeline_flow()
        cmd = mock_subproc.call_args[0][0]
        assert "--workers" in cmd
        assert "3" in cmd
        assert "--schools" in cmd
        assert "mit,stanford" in cmd
        assert "--phase" in cmd
        assert "discovery" in cmd

    @patch("scrape_edu.interactive.subprocess.run")
    @patch("scrape_edu.interactive.Confirm.ask", return_value=False)
    @patch("scrape_edu.interactive.Prompt.ask", side_effect=["", "all"])
    @patch("scrape_edu.interactive.IntPrompt.ask", return_value=5)
    @patch("scrape_edu.interactive.console")
    def test_cancelled(self, _con, mock_int, mock_prompt, mock_confirm, mock_subproc) -> None:
        run_pipeline_flow()
        mock_subproc.assert_not_called()

    @patch("scrape_edu.interactive.subprocess.run")
    @patch("scrape_edu.interactive.IntPrompt.ask", return_value=0)
    @patch("scrape_edu.interactive.console")
    def test_invalid_workers_zero(self, _con, mock_int, mock_subproc) -> None:
        run_pipeline_flow()
        mock_subproc.assert_not_called()

    @patch("scrape_edu.interactive.subprocess.run")
    @patch("scrape_edu.interactive.IntPrompt.ask", return_value=-1)
    @patch("scrape_edu.interactive.console")
    def test_invalid_workers_negative(self, _con, mock_int, mock_subproc) -> None:
        run_pipeline_flow()
        mock_subproc.assert_not_called()


# ======================================================================
# Test: check status flow
# ======================================================================


class TestCheckStatusFlow:
    """Test the status sub-flow."""

    @patch("scrape_edu.interactive.subprocess.run")
    @patch("scrape_edu.interactive.console")
    def test_calls_status_command(self, _con, mock_subproc) -> None:
        check_status_flow()
        mock_subproc.assert_called_once()
        cmd = mock_subproc.call_args[0][0]
        assert "status" in cmd


# ======================================================================
# Test: rescrape flow
# ======================================================================


class TestRescrapeFlow:
    """Test the rescrape interactive sub-flow."""

    @patch("scrape_edu.interactive.subprocess.run")
    @patch("scrape_edu.interactive.Confirm.ask", return_value=True)
    @patch("scrape_edu.interactive.Prompt.ask", return_value="all")
    @patch("scrape_edu.interactive.console")
    def test_rescrape_all(self, _con, mock_prompt, mock_confirm, mock_subproc) -> None:
        rescrape_flow()
        cmd = mock_subproc.call_args[0][0]
        assert "--all" in cmd

    @patch("scrape_edu.interactive.subprocess.run")
    @patch("scrape_edu.interactive.Confirm.ask", return_value=True)
    @patch("scrape_edu.interactive.Prompt.ask", side_effect=["specific", "mit,stanford"])
    @patch("scrape_edu.interactive.console")
    def test_rescrape_specific(self, _con, mock_prompt, mock_confirm, mock_subproc) -> None:
        rescrape_flow()
        cmd = mock_subproc.call_args[0][0]
        assert "--schools" in cmd
        assert "mit,stanford" in cmd

    @patch("scrape_edu.interactive.subprocess.run")
    @patch("scrape_edu.interactive.Prompt.ask", side_effect=["specific", "  "])
    @patch("scrape_edu.interactive.console")
    def test_rescrape_specific_empty_slugs(self, _con, mock_prompt, mock_subproc) -> None:
        rescrape_flow()
        mock_subproc.assert_not_called()

    @patch("scrape_edu.interactive.subprocess.run")
    @patch("scrape_edu.interactive.Confirm.ask", return_value=False)
    @patch("scrape_edu.interactive.Prompt.ask", return_value="all")
    @patch("scrape_edu.interactive.console")
    def test_rescrape_cancelled(self, _con, mock_prompt, mock_confirm, mock_subproc) -> None:
        rescrape_flow()
        mock_subproc.assert_not_called()


# ======================================================================
# Test: analyze flow
# ======================================================================


class TestAnalyzeFlow:
    """Test the analyze sub-flow."""

    @patch("scrape_edu.interactive.subprocess.run")
    @patch("scrape_edu.interactive.console")
    def test_calls_analyze_script(self, _con, mock_subproc) -> None:
        analyze_flow()
        mock_subproc.assert_called_once()
        cmd = mock_subproc.call_args[0][0]
        assert "analyze_manifest.py" in cmd[-1]


# ======================================================================
# Test: dry run flow
# ======================================================================


class TestDryRunFlow:
    """Test the dry-run interactive sub-flow."""

    @patch("scrape_edu.interactive.subprocess.run")
    @patch("scrape_edu.interactive.Prompt.ask", side_effect=["", "all"])
    @patch("scrape_edu.interactive.IntPrompt.ask", return_value=5)
    @patch("scrape_edu.interactive.console")
    def test_default_dry_run(self, _con, mock_int, mock_prompt, mock_subproc) -> None:
        dry_run_flow()
        cmd = mock_subproc.call_args[0][0]
        assert "--dry-run" in cmd
        assert "--workers" in cmd
        assert "5" in cmd

    @patch("scrape_edu.interactive.subprocess.run")
    @patch("scrape_edu.interactive.Prompt.ask", side_effect=["mit", "catalog"])
    @patch("scrape_edu.interactive.IntPrompt.ask", return_value=8)
    @patch("scrape_edu.interactive.console")
    def test_dry_run_with_filters(self, _con, mock_int, mock_prompt, mock_subproc) -> None:
        dry_run_flow()
        cmd = mock_subproc.call_args[0][0]
        assert "--dry-run" in cmd
        assert "--workers" in cmd
        assert "8" in cmd
        assert "--schools" in cmd
        assert "mit" in cmd
        assert "--phase" in cmd
        assert "catalog" in cmd

    @patch("scrape_edu.interactive.subprocess.run")
    @patch("scrape_edu.interactive.IntPrompt.ask", return_value=0)
    @patch("scrape_edu.interactive.console")
    def test_invalid_workers(self, _con, mock_int, mock_subproc) -> None:
        dry_run_flow()
        mock_subproc.assert_not_called()


# ======================================================================
# Test: setup flow
# ======================================================================


class TestSetupFlow:
    """Test the setup interactive sub-flow."""

    @patch("scrape_edu.interactive.load_config", return_value={"ipeds_dir": "/tmp/test_ipeds", "search": {"api_key": "test-key"}})
    @patch("scrape_edu.interactive.console")
    def test_all_ready(self, mock_console, mock_config, tmp_path: Path) -> None:
        """When IPEDS data and API key exist, show green checkmarks."""
        # Create fake IPEDS file
        ipeds_dir = Path("/tmp/test_ipeds")
        ipeds_dir.mkdir(exist_ok=True)
        (ipeds_dir / "hd2023.csv").write_text("test")

        try:
            run_setup()
            # Should print "All set!"
            printed = [str(c) for c in mock_console.print.call_args_list]
            all_text = " ".join(printed)
            assert "All set" in all_text
        finally:
            (ipeds_dir / "hd2023.csv").unlink(missing_ok=True)
            ipeds_dir.rmdir()

    @patch("scrape_edu.interactive.Confirm.ask", return_value=False)
    @patch("scrape_edu.interactive.Prompt.ask", return_value="")
    @patch("scrape_edu.interactive.load_config", return_value={"ipeds_dir": "/tmp/test_no_ipeds", "search": {}})
    @patch("scrape_edu.interactive.console")
    def test_nothing_ready(self, mock_console, mock_config, mock_prompt, mock_confirm) -> None:
        """When nothing is set up, show red crosses."""
        run_setup()
        printed = [str(c) for c in mock_console.print.call_args_list]
        all_text = " ".join(printed)
        assert "need attention" in all_text


# ======================================================================
# Test: CLI integration (no args → menu, menu subcommand → menu)
# ======================================================================


class TestCLIMenuIntegration:
    """Test that cli.main() dispatches to interactive_menu."""

    @patch("scrape_edu.interactive.interactive_menu", return_value=0)
    def test_no_args_launches_menu(self, mock_menu) -> None:
        from scrape_edu.cli import main
        result = main([])
        assert result == 0
        mock_menu.assert_called_once()

    @patch("scrape_edu.interactive.interactive_menu", return_value=0)
    def test_menu_subcommand_launches_menu(self, mock_menu) -> None:
        from scrape_edu.cli import main
        result = main(["menu"])
        assert result == 0
        mock_menu.assert_called_once()

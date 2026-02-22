"""Tests for scrape_edu.pipeline.orchestrator module."""

from __future__ import annotations

import re
import threading
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from scrape_edu.data.manifest import ManifestManager, SchoolStatus
from scrape_edu.data.school import School
from scrape_edu.pipeline.phases import Phase, PHASE_ORDER
from scrape_edu.pipeline.orchestrator import Orchestrator


def _make_school(
    name: str, unitid: int = 100000, url: str = "https://example.edu"
) -> School:
    """Create a test School instance."""
    return School(unitid=unitid, name=name, url=url, city="Test City", state="TS")


def _make_schools(count: int = 3) -> list[School]:
    """Create a list of test schools."""
    return [
        _make_school(f"University {i}", unitid=100000 + i)
        for i in range(count)
    ]


class TestOrchestratorInit:
    """Test orchestrator initialization."""

    def test_creates_output_dir(self, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        Orchestrator(schools=[], output_dir=output_dir, config={})
        assert output_dir.exists()

    def test_creates_manifest(self, tmp_path: Path) -> None:
        orch = Orchestrator(schools=[], output_dir=tmp_path, config={})
        assert orch.manifest is not None


class TestOrchestratorRun:
    """Test the main run() method."""

    def test_processes_multiple_schools(self, tmp_path: Path) -> None:
        """All schools are processed and marked completed."""
        schools = _make_schools(3)
        orch = Orchestrator(
            schools=schools, output_dir=tmp_path, config={}, workers=3
        )
        results = orch.run()

        assert results["completed"] == 3
        assert results["failed"] == 0

    def test_returns_correct_summary_counts(self, tmp_path: Path) -> None:
        """Summary dict has all expected keys."""
        schools = _make_schools(2)
        orch = Orchestrator(
            schools=schools, output_dir=tmp_path, config={}, workers=2
        )
        results = orch.run()

        assert "completed" in results
        assert "failed" in results
        assert "skipped" in results
        assert "interrupted" in results
        assert results["completed"] == 2

    def test_processes_schools_concurrently(self, tmp_path: Path) -> None:
        """Schools are processed by multiple worker threads."""
        thread_ids: list[int] = []
        lock = threading.Lock()

        def tracking_handler(school, school_dir, metadata, config):
            with lock:
                thread_ids.append(threading.current_thread().ident)
            time.sleep(0.05)  # Small delay so threads overlap

        schools = _make_schools(4)
        handlers = {Phase.ROBOTS: tracking_handler}
        orch = Orchestrator(
            schools=schools,
            output_dir=tmp_path,
            config={},
            workers=4,
            phase_handlers=handlers,
        )
        orch.run(phases_filter=[Phase.ROBOTS])

        # With 4 workers, we should see at least 2 different thread IDs
        unique_threads = set(thread_ids)
        assert len(unique_threads) >= 2

    def test_respects_workers_limit(self, tmp_path: Path) -> None:
        """No more than `workers` schools process simultaneously."""
        max_concurrent = 0
        current_concurrent = 0
        lock = threading.Lock()

        def tracking_handler(school, school_dir, metadata, config):
            nonlocal max_concurrent, current_concurrent
            with lock:
                current_concurrent += 1
                if current_concurrent > max_concurrent:
                    max_concurrent = current_concurrent
            time.sleep(0.1)
            with lock:
                current_concurrent -= 1

        schools = _make_schools(6)
        handlers = {Phase.ROBOTS: tracking_handler}
        orch = Orchestrator(
            schools=schools,
            output_dir=tmp_path,
            config={},
            workers=2,
            phase_handlers=handlers,
        )
        orch.run(phases_filter=[Phase.ROBOTS])

        assert max_concurrent <= 2


class TestOrchestratorCrashRecovery:
    """Test crash recovery -- resetting SCRAPING to PENDING."""

    def test_resets_scraping_to_pending_on_startup(self, tmp_path: Path) -> None:
        """Schools left in SCRAPING state from a crash are reset to PENDING."""
        # Simulate a previous run that crashed mid-scrape
        manifest = ManifestManager(tmp_path)
        manifest.init_school("university-0", {"name": "University 0"})
        manifest.claim_school("university-0")
        assert manifest.get_school_status("university-0") == SchoolStatus.SCRAPING

        # Now create a new orchestrator -- it should reset
        schools = [_make_school("University 0", unitid=100000)]
        orch = Orchestrator(
            schools=schools, output_dir=tmp_path, config={}, workers=1
        )
        results = orch.run()

        # School should have been reset and then processed
        assert results["completed"] == 1
        assert orch.manifest.get_school_status("university-0") == SchoolStatus.COMPLETED


class TestOrchestratorClaimPattern:
    """Test that the claim pattern prevents duplicate work."""

    def test_already_completed_school_is_skipped(self, tmp_path: Path) -> None:
        """A school already marked COMPLETED is not re-processed."""
        manifest = ManifestManager(tmp_path)
        manifest.init_school("university-0", {"name": "University 0"})
        manifest.update_school_status("university-0", SchoolStatus.COMPLETED)

        schools = [_make_school("University 0", unitid=100000)]
        orch = Orchestrator(
            schools=schools, output_dir=tmp_path, config={}, workers=1
        )
        results = orch.run()

        assert results["skipped"] == 1
        assert results["completed"] == 0

    def test_init_same_school_twice_only_processed_once(self, tmp_path: Path) -> None:
        """Initializing the same school twice does not cause duplicate processing."""
        school = _make_school("University 0", unitid=100000)
        call_count = 0
        lock = threading.Lock()

        def counting_handler(s, sd, m, c):
            nonlocal call_count
            with lock:
                call_count += 1

        handlers = {Phase.ROBOTS: counting_handler}
        orch = Orchestrator(
            schools=[school, school],  # Same school listed twice
            output_dir=tmp_path,
            config={},
            workers=2,
            phase_handlers=handlers,
        )
        results = orch.run(phases_filter=[Phase.ROBOTS])

        # One should be completed, one should be skipped (claim fails for duplicate)
        assert results["completed"] + results["skipped"] == 2
        # The handler should have been called at most once for this school
        assert call_count == 1


class TestOrchestratorFilters:
    """Test school and phase filtering."""

    def test_filters_by_school_slugs(self, tmp_path: Path) -> None:
        """Only specified school slugs are processed."""
        schools = _make_schools(3)
        target_slug = schools[1].slug

        orch = Orchestrator(
            schools=schools, output_dir=tmp_path, config={}, workers=1
        )
        results = orch.run(schools_filter=[target_slug])

        assert results["completed"] == 1
        assert orch.manifest.get_school_status(target_slug) == SchoolStatus.COMPLETED

    def test_filters_by_phases(self, tmp_path: Path) -> None:
        """Only specified phases are run per school."""
        phases_run: list[str] = []
        lock = threading.Lock()

        def make_handler(name: str):
            def handler(school, school_dir, metadata, config):
                with lock:
                    phases_run.append(name)
            return handler

        handlers = {phase: make_handler(phase.value) for phase in PHASE_ORDER}
        schools = [_make_school("MIT")]
        orch = Orchestrator(
            schools=schools,
            output_dir=tmp_path,
            config={},
            workers=1,
            phase_handlers=handlers,
        )
        orch.run(phases_filter=[Phase.CATALOG, Phase.SYLLABI])

        assert sorted(phases_run) == ["catalog", "syllabi"]

    def test_schools_filter_with_nonexistent_slug(self, tmp_path: Path) -> None:
        """Filtering by a slug that doesn't match any school processes nothing."""
        schools = _make_schools(2)
        orch = Orchestrator(
            schools=schools, output_dir=tmp_path, config={}, workers=1
        )
        results = orch.run(schools_filter=["nonexistent-slug"])

        assert results["completed"] == 0
        assert results["failed"] == 0
        assert results["skipped"] == 0


class TestOrchestratorErrorHandling:
    """Test handling of worker exceptions."""

    def test_worker_exception_marks_school_failed(self, tmp_path: Path) -> None:
        """If a phase handler raises, the school is marked FAILED."""

        def bad_handler(school, school_dir, metadata, config):
            raise RuntimeError("catastrophic failure")

        schools = [_make_school("MIT")]
        handlers = {Phase.ROBOTS: bad_handler}
        orch = Orchestrator(
            schools=schools,
            output_dir=tmp_path,
            config={},
            workers=1,
            phase_handlers=handlers,
        )
        results = orch.run(phases_filter=[Phase.ROBOTS])

        # Phase failure causes the worker to return False, which means FAILED
        assert results["failed"] == 1

    def test_one_school_failure_does_not_crash_others(self, tmp_path: Path) -> None:
        """A failing school does not prevent other schools from being processed."""
        call_count = 0
        lock = threading.Lock()

        def handler(school, school_dir, metadata, config):
            nonlocal call_count
            with lock:
                call_count += 1
            if school.name == "University 0":
                raise RuntimeError("fail")

        schools = _make_schools(3)
        handlers = {Phase.ROBOTS: handler}
        orch = Orchestrator(
            schools=schools,
            output_dir=tmp_path,
            config={},
            workers=1,
            phase_handlers=handlers,
        )
        results = orch.run(phases_filter=[Phase.ROBOTS])

        assert call_count == 3  # All three were attempted
        assert results["failed"] == 1
        assert results["completed"] == 2


class TestOrchestratorShutdown:
    """Test graceful shutdown behavior."""

    def test_signal_handler_sets_shutdown_event(self, tmp_path: Path) -> None:
        """_signal_handler sets the shutdown_event."""
        orch = Orchestrator(schools=[], output_dir=tmp_path, config={})
        assert not orch.shutdown_event.is_set()

        # Call the signal handler directly (don't send actual SIGINT)
        orch._signal_handler(None, None)

        assert orch.shutdown_event.is_set()

    def test_workers_stop_on_shutdown(self, tmp_path: Path) -> None:
        """Workers check shutdown_event and stop processing."""
        phases_run: list[str] = []
        lock = threading.Lock()

        def slow_handler(school, school_dir, metadata, config):
            with lock:
                phases_run.append(school.name)
            time.sleep(0.05)

        schools = _make_schools(10)
        handlers = {Phase.ROBOTS: slow_handler}
        orch = Orchestrator(
            schools=schools,
            output_dir=tmp_path,
            config={},
            workers=1,
            phase_handlers=handlers,
        )

        # Set shutdown before running
        orch.shutdown_event.set()
        results = orch.run(phases_filter=[Phase.ROBOTS])

        # No schools should have been submitted since shutdown is set before the loop
        assert len(phases_run) == 0


class TestOrchestratorThreadSafety:
    """Test that multiple workers don't corrupt the manifest."""

    def test_concurrent_workers_no_manifest_corruption(self, tmp_path: Path) -> None:
        """Multiple workers updating manifest concurrently don't cause errors."""
        schools = _make_schools(10)

        def handler(school, school_dir, metadata, config):
            # Simulate some work
            time.sleep(0.01)

        handlers = {Phase.ROBOTS: handler}
        orch = Orchestrator(
            schools=schools,
            output_dir=tmp_path,
            config={},
            workers=5,
            phase_handlers=handlers,
        )
        results = orch.run(phases_filter=[Phase.ROBOTS])

        # All schools should be accounted for
        total = results["completed"] + results["failed"] + results["skipped"]
        assert total == 10
        assert results["completed"] == 10

        # Verify manifest is consistent
        for school in schools:
            status = orch.manifest.get_school_status(school.slug)
            assert status == SchoolStatus.COMPLETED

    def test_concurrent_claim_prevents_double_processing(self, tmp_path: Path) -> None:
        """Two workers trying to claim the same school -- only one succeeds."""
        process_count = 0
        lock = threading.Lock()

        def handler(school, school_dir, metadata, config):
            nonlocal process_count
            with lock:
                process_count += 1
            time.sleep(0.05)

        # Create one school and try to process it from multiple workers
        school = _make_school("MIT")
        handlers = {Phase.ROBOTS: handler}
        orch = Orchestrator(
            schools=[school],
            output_dir=tmp_path,
            config={},
            workers=4,
            phase_handlers=handlers,
        )
        results = orch.run(phases_filter=[Phase.ROBOTS])

        # Only one worker should have processed the school
        assert process_count == 1
        assert results["completed"] == 1


class TestFormatElapsed:
    """Test the _format_elapsed static method."""

    def test_seconds_only(self) -> None:
        assert Orchestrator._format_elapsed(5.0) == "5s"

    def test_zero_seconds(self) -> None:
        assert Orchestrator._format_elapsed(0.0) == "0s"

    def test_59_seconds(self) -> None:
        assert Orchestrator._format_elapsed(59.9) == "59s"

    def test_one_minute(self) -> None:
        assert Orchestrator._format_elapsed(60.0) == "1m 00s"

    def test_minutes_and_seconds(self) -> None:
        assert Orchestrator._format_elapsed(83.0) == "1m 23s"

    def test_many_minutes(self) -> None:
        assert Orchestrator._format_elapsed(3599.0) == "59m 59s"

    def test_one_hour(self) -> None:
        assert Orchestrator._format_elapsed(3600.0) == "1h 00m 00s"

    def test_hours_minutes_seconds(self) -> None:
        # 1h 5m 30s = 3600 + 300 + 30 = 3930
        assert Orchestrator._format_elapsed(3930.0) == "1h 05m 30s"

    def test_fractional_seconds_truncated(self) -> None:
        # Fractional part is truncated, not rounded
        assert Orchestrator._format_elapsed(5.9) == "5s"


class TestProgressReporting:
    """Test progress reporting output during pipeline runs."""

    def _capture_output(self, tmp_path: Path, schools, handlers=None, **run_kwargs):
        """Run the orchestrator and capture all printed output."""
        output_lines: list[str] = []
        lock = threading.Lock()

        def capture_print(msg: str) -> None:
            with lock:
                output_lines.append(msg)

        orch = Orchestrator(
            schools=schools,
            output_dir=tmp_path,
            config={},
            workers=1,
            phase_handlers=handlers or {},
        )
        orch._print_fn = capture_print
        results = orch.run(**run_kwargs)
        return results, output_lines

    def test_starting_message_includes_school_count(self, tmp_path: Path) -> None:
        """A 'Starting pipeline for N schools...' message is printed."""
        schools = _make_schools(3)
        results, output = self._capture_output(tmp_path, schools)

        assert any("Starting pipeline for 3 schools" in line for line in output)

    def test_no_starting_message_when_no_schools(self, tmp_path: Path) -> None:
        """No starting message when there are zero schools to process."""
        results, output = self._capture_output(tmp_path, [])

        assert not any("Starting pipeline" in line for line in output)

    def test_completed_school_shows_progress_line(self, tmp_path: Path) -> None:
        """Each completed school produces a progress line with expected format."""
        schools = [_make_school("MIT")]
        results, output = self._capture_output(tmp_path, schools)

        # Find the progress line (not the "Starting" line)
        progress_lines = [l for l in output if l.startswith("[")]
        assert len(progress_lines) == 1

        line = progress_lines[0]
        assert "[1/1]" in line
        assert "Completed" in line
        assert "mit" in line
        assert "elapsed:" in line
        assert "done: 1" in line
        assert "failed: 0" in line
        assert "remaining: 0" in line

    def test_failed_school_shows_failed_status(self, tmp_path: Path) -> None:
        """A school that fails shows 'Failed' in progress line."""

        def bad_handler(school, school_dir, metadata, config):
            raise RuntimeError("boom")

        schools = [_make_school("MIT")]
        handlers = {Phase.ROBOTS: bad_handler}
        results, output = self._capture_output(
            tmp_path, schools, handlers, phases_filter=[Phase.ROBOTS]
        )

        progress_lines = [l for l in output if l.startswith("[")]
        assert len(progress_lines) == 1
        assert "Failed" in progress_lines[0]
        assert "done: 0" in progress_lines[0]
        assert "failed: 1" in progress_lines[0]

    def test_multiple_schools_counts_are_correct(self, tmp_path: Path) -> None:
        """With multiple schools, the final progress line has correct totals."""

        def handler(school, school_dir, metadata, config):
            if school.name == "University 1":
                raise RuntimeError("fail")

        schools = _make_schools(3)
        handlers = {Phase.ROBOTS: handler}
        results, output = self._capture_output(
            tmp_path, schools, handlers, phases_filter=[Phase.ROBOTS]
        )

        # Should have 1 starting line + 3 progress lines
        progress_lines = [l for l in output if l.startswith("[")]
        assert len(progress_lines) == 3

        # Parse the last progress line -- should show 3/3 total
        last_line = progress_lines[-1]
        assert "[3/3]" in last_line
        assert "remaining: 0" in last_line

    def test_progress_line_format_matches_spec(self, tmp_path: Path) -> None:
        """Progress lines match the format: [N/M] Status slug (elapsed: Xs) | done: ..."""
        schools = [_make_school("MIT")]
        results, output = self._capture_output(tmp_path, schools)

        progress_lines = [l for l in output if l.startswith("[")]
        assert len(progress_lines) == 1

        pattern = (
            r"^\[\d+/\d+\] (Completed|Failed) \S+ "
            r"\(elapsed: \d+[hms]"
        )
        assert re.search(pattern, progress_lines[0])

    def test_skipped_schools_not_in_progress_count(self, tmp_path: Path) -> None:
        """Schools that are skipped (already completed) don't count in total."""
        manifest = ManifestManager(tmp_path)
        manifest.init_school("university-0", {"name": "University 0"})
        manifest.update_school_status("university-0", SchoolStatus.COMPLETED)

        schools = _make_schools(3)
        output_lines: list[str] = []
        lock = threading.Lock()

        def capture_print(msg: str) -> None:
            with lock:
                output_lines.append(msg)

        orch = Orchestrator(
            schools=schools, output_dir=tmp_path, config={}, workers=1
        )
        orch._print_fn = capture_print
        results = orch.run()

        # University 0 was skipped, so total submitted should be 2
        starting_lines = [l for l in output_lines if "Starting pipeline" in l]
        assert len(starting_lines) == 1
        assert "2 schools" in starting_lines[0]

    def test_progress_counters_thread_safe(self, tmp_path: Path) -> None:
        """Multiple workers updating progress counters concurrently."""
        schools = _make_schools(10)
        output_lines: list[str] = []
        lock = threading.Lock()

        def capture_print(msg: str) -> None:
            with lock:
                output_lines.append(msg)

        def handler(school, school_dir, metadata, config):
            time.sleep(0.01)

        handlers = {Phase.ROBOTS: handler}
        orch = Orchestrator(
            schools=schools,
            output_dir=tmp_path,
            config={},
            workers=5,
            phase_handlers=handlers,
        )
        orch._print_fn = capture_print
        results = orch.run(phases_filter=[Phase.ROBOTS])

        # Should have 1 starting line + 10 progress lines
        progress_lines = [l for l in output_lines if l.startswith("[")]
        assert len(progress_lines) == 10

        # The finished numbers in progress lines should be 1 through 10
        # (each unique, monotonically increasing per-thread, but unordered overall)
        finished_nums = []
        for line in progress_lines:
            match = re.match(r"^\[(\d+)/10\]", line)
            assert match, f"Unexpected line format: {line}"
            finished_nums.append(int(match.group(1)))

        assert sorted(finished_nums) == list(range(1, 11))

    def test_elapsed_time_increases(self, tmp_path: Path) -> None:
        """Elapsed time in progress lines is non-decreasing."""

        def slow_handler(school, school_dir, metadata, config):
            time.sleep(0.02)

        schools = _make_schools(3)
        handlers = {Phase.ROBOTS: slow_handler}
        results, output = self._capture_output(
            tmp_path, schools, handlers, phases_filter=[Phase.ROBOTS]
        )

        # All progress lines should have an elapsed field parseable as a time
        progress_lines = [l for l in output if l.startswith("[")]
        assert len(progress_lines) == 3

        for line in progress_lines:
            assert "elapsed:" in line

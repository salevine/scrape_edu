"""Tests for scrape_edu.pipeline.school_worker module."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from scrape_edu.data.manifest import ManifestManager, PhaseStatus, SchoolMetadata
from scrape_edu.data.school import School
from scrape_edu.pipeline.phases import Phase, PHASE_ORDER
from scrape_edu.pipeline.school_worker import SchoolWorker


def _make_school(name: str = "MIT", unitid: int = 166683, url: str = "https://mit.edu") -> School:
    """Create a test School instance."""
    return School(unitid=unitid, name=name, url=url, city="Cambridge", state="MA")


def _make_worker(
    tmp_path: Path,
    school: School | None = None,
    phase_handlers: dict[Phase, Any] | None = None,
    shutdown_event: threading.Event | None = None,
) -> SchoolWorker:
    """Create a SchoolWorker with sensible defaults for testing."""
    school = school or _make_school()
    manifest = ManifestManager(tmp_path)
    manifest.init_school(school.slug, {"name": school.name})
    return SchoolWorker(
        school=school,
        manifest=manifest,
        output_dir=tmp_path,
        config={},
        shutdown_event=shutdown_event or threading.Event(),
        phase_handlers=phase_handlers,
    )


class TestSchoolWorkerRun:
    """Test the main run() method."""

    def test_runs_all_phases_in_order(self, tmp_path: Path) -> None:
        """Phases are executed in PHASE_ORDER when no filter is given."""
        call_order: list[str] = []

        def make_handler(phase: Phase):
            def handler(school, school_dir, metadata, config):
                call_order.append(phase.value)
            return handler

        handlers = {phase: make_handler(phase) for phase in PHASE_ORDER}
        worker = _make_worker(tmp_path, phase_handlers=handlers)
        success = worker.run()

        assert success is True
        assert call_order == [p.value for p in PHASE_ORDER]

    def test_returns_true_on_success(self, tmp_path: Path) -> None:
        worker = _make_worker(tmp_path)
        assert worker.run() is True

    def test_works_with_no_handlers(self, tmp_path: Path) -> None:
        """When no phase handlers are registered, all phases are skipped gracefully."""
        worker = _make_worker(tmp_path)
        success = worker.run()
        assert success is True

        # All phases should be marked COMPLETED (no handler = no error)
        for phase in PHASE_ORDER:
            status = worker.metadata.get_phase_status(phase.value)
            assert status == PhaseStatus.COMPLETED

    def test_calls_handler_with_correct_args(self, tmp_path: Path) -> None:
        """Phase handler receives (school, school_dir, metadata, config)."""
        captured_args: list[tuple] = []

        def handler(school, school_dir, metadata, config):
            captured_args.append((school, school_dir, metadata, config))

        school = _make_school()
        worker = _make_worker(
            tmp_path, school=school, phase_handlers={Phase.ROBOTS: handler}
        )
        worker.run(phases_filter=[Phase.ROBOTS])

        assert len(captured_args) == 1
        args = captured_args[0]
        assert args[0] is school
        assert args[1] == tmp_path / school.slug
        assert isinstance(args[2], SchoolMetadata)
        assert isinstance(args[3], dict)


class TestSchoolWorkerPhaseStatus:
    """Test that phase statuses are recorded correctly."""

    def test_records_running_then_completed(self, tmp_path: Path) -> None:
        """Phase transitions through RUNNING to COMPLETED."""
        statuses_seen: list[PhaseStatus] = []

        def handler(school, school_dir, metadata, config):
            # At this point the status should be RUNNING
            statuses_seen.append(metadata.get_phase_status(Phase.ROBOTS.value))

        worker = _make_worker(
            tmp_path, phase_handlers={Phase.ROBOTS: handler}
        )
        worker.run(phases_filter=[Phase.ROBOTS])

        assert statuses_seen == [PhaseStatus.RUNNING]
        # After run, should be COMPLETED
        assert worker.metadata.get_phase_status(Phase.ROBOTS.value) == PhaseStatus.COMPLETED

    def test_records_failed_on_error(self, tmp_path: Path) -> None:
        """Phase is marked FAILED when handler raises an exception."""

        def bad_handler(school, school_dir, metadata, config):
            raise RuntimeError("something broke")

        worker = _make_worker(
            tmp_path, phase_handlers={Phase.CATALOG: bad_handler}
        )
        worker.run(phases_filter=[Phase.CATALOG])

        assert worker.metadata.get_phase_status(Phase.CATALOG.value) == PhaseStatus.FAILED

    def test_records_errors_in_metadata(self, tmp_path: Path) -> None:
        """Errors are logged in metadata.errors list."""

        def bad_handler(school, school_dir, metadata, config):
            raise ValueError("bad value")

        worker = _make_worker(
            tmp_path, phase_handlers={Phase.DISCOVERY: bad_handler}
        )
        worker.run(phases_filter=[Phase.DISCOVERY])

        errors = worker.metadata._metadata["errors"]
        assert len(errors) == 1
        assert errors[0]["phase"] == "discovery"
        assert "bad value" in errors[0]["error"]

    def test_saves_metadata_after_each_status_change(self, tmp_path: Path) -> None:
        """Metadata is saved to disk after RUNNING and after COMPLETED."""
        save_calls: list[str] = []
        original_save = SchoolMetadata.save

        def tracking_save(self_metadata):
            phase_status = self_metadata.get_phase_status(Phase.ROBOTS.value)
            if phase_status is not None:
                save_calls.append(phase_status.value)
            original_save(self_metadata)

        worker = _make_worker(tmp_path)
        worker.metadata.save = lambda: tracking_save(worker.metadata)
        worker.run(phases_filter=[Phase.ROBOTS])

        # Should see saves for: RUNNING, COMPLETED (at minimum for the ROBOTS phase)
        assert "running" in save_calls
        assert "completed" in save_calls


class TestSchoolWorkerResume:
    """Test resume behavior -- skipping completed phases."""

    def test_skips_completed_phases(self, tmp_path: Path) -> None:
        """Already-completed phases are skipped on resume."""
        call_count = {"robots": 0, "discovery": 0}

        def robots_handler(school, school_dir, metadata, config):
            call_count["robots"] += 1

        def discovery_handler(school, school_dir, metadata, config):
            call_count["discovery"] += 1

        worker = _make_worker(
            tmp_path,
            phase_handlers={
                Phase.ROBOTS: robots_handler,
                Phase.DISCOVERY: discovery_handler,
            },
        )

        # Mark ROBOTS as already completed
        worker.metadata.update_phase(Phase.ROBOTS.value, PhaseStatus.COMPLETED)
        worker.metadata.save()

        worker.run(phases_filter=[Phase.ROBOTS, Phase.DISCOVERY])

        assert call_count["robots"] == 0  # Skipped
        assert call_count["discovery"] == 1  # Ran

    def test_does_not_skip_failed_phases(self, tmp_path: Path) -> None:
        """A previously FAILED phase is retried (not skipped)."""
        call_count = {"catalog": 0}

        def catalog_handler(school, school_dir, metadata, config):
            call_count["catalog"] += 1

        worker = _make_worker(
            tmp_path, phase_handlers={Phase.CATALOG: catalog_handler}
        )
        worker.metadata.update_phase(Phase.CATALOG.value, PhaseStatus.FAILED)
        worker.metadata.save()

        worker.run(phases_filter=[Phase.CATALOG])
        assert call_count["catalog"] == 1  # Retried


class TestSchoolWorkerErrorContinuation:
    """Test that errors in one phase don't block subsequent phases."""

    def test_continues_after_failure(self, tmp_path: Path) -> None:
        """A failure in one phase does not prevent subsequent phases from running."""
        phases_run: list[str] = []

        def fail_handler(school, school_dir, metadata, config):
            phases_run.append("robots")
            raise RuntimeError("robots failed")

        def ok_handler(school, school_dir, metadata, config):
            phases_run.append("discovery")

        worker = _make_worker(
            tmp_path,
            phase_handlers={
                Phase.ROBOTS: fail_handler,
                Phase.DISCOVERY: ok_handler,
            },
        )
        success = worker.run(phases_filter=[Phase.ROBOTS, Phase.DISCOVERY])

        assert success is False  # Overall failure because ROBOTS failed
        assert phases_run == ["robots", "discovery"]  # Both ran

    def test_returns_false_when_any_phase_fails(self, tmp_path: Path) -> None:
        """run() returns False if any phase raised an exception."""

        def fail_handler(school, school_dir, metadata, config):
            raise RuntimeError("boom")

        worker = _make_worker(
            tmp_path, phase_handlers={Phase.FACULTY: fail_handler}
        )
        success = worker.run(phases_filter=[Phase.FACULTY])
        assert success is False


class TestSchoolWorkerShutdown:
    """Test graceful shutdown via shutdown_event."""

    def test_stops_on_shutdown_event(self, tmp_path: Path) -> None:
        """Worker checks shutdown_event before each phase and stops if set."""
        phases_run: list[str] = []

        def handler(school, school_dir, metadata, config):
            phases_run.append("ran")

        shutdown = threading.Event()
        shutdown.set()  # Already signaled before run()

        worker = _make_worker(
            tmp_path,
            phase_handlers={Phase.ROBOTS: handler},
            shutdown_event=shutdown,
        )
        success = worker.run()

        assert success is False
        assert phases_run == []  # Nothing ran

    def test_stops_mid_pipeline_on_shutdown(self, tmp_path: Path) -> None:
        """Worker stops after current phase when shutdown is signaled mid-run."""
        phases_run: list[str] = []
        shutdown = threading.Event()

        def robots_handler(school, school_dir, metadata, config):
            phases_run.append("robots")
            shutdown.set()  # Signal shutdown after this phase

        def discovery_handler(school, school_dir, metadata, config):
            phases_run.append("discovery")

        worker = _make_worker(
            tmp_path,
            phase_handlers={
                Phase.ROBOTS: robots_handler,
                Phase.DISCOVERY: discovery_handler,
            },
            shutdown_event=shutdown,
        )
        success = worker.run(phases_filter=[Phase.ROBOTS, Phase.DISCOVERY])

        assert success is False
        assert phases_run == ["robots"]  # Only ROBOTS ran


class TestSchoolWorkerPhasesFilter:
    """Test running a subset of phases."""

    def test_filter_runs_only_specified_phases(self, tmp_path: Path) -> None:
        phases_run: list[str] = []

        def make_handler(name: str):
            def handler(school, school_dir, metadata, config):
                phases_run.append(name)
            return handler

        handlers = {phase: make_handler(phase.value) for phase in PHASE_ORDER}
        worker = _make_worker(tmp_path, phase_handlers=handlers)
        worker.run(phases_filter=[Phase.CATALOG, Phase.SYLLABI])

        assert phases_run == ["catalog", "syllabi"]

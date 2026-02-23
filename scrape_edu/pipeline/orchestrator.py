"""Pipeline orchestrator -- manages concurrent school scraping."""

from __future__ import annotations

import fnmatch
import logging
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from scrape_edu.data.manifest import ManifestManager, SchoolMetadata, SchoolStatus
from scrape_edu.data.school import School
from scrape_edu.pipeline.phases import Phase
from scrape_edu.pipeline.school_worker import SchoolWorker

logger = logging.getLogger("scrape_edu")


class Orchestrator:
    """Manage concurrent scraping of multiple schools.

    Uses ThreadPoolExecutor to process schools in parallel.
    Supports graceful shutdown via SIGINT and resume after interruption.
    """

    def __init__(
        self,
        schools: list[School],
        output_dir: Path,
        config: dict[str, Any],
        workers: int = 5,
        phase_handlers: dict[Phase, Any] | None = None,
    ):
        self.schools = schools
        self.output_dir = Path(output_dir)
        self.config = config
        self.workers = workers
        self.phase_handlers = phase_handlers or {}
        self.shutdown_event = threading.Event()

        # Progress tracking (thread-safe)
        self._progress_lock = threading.Lock()
        self._done_count = 0
        self._fail_count = 0
        self._total_submitted = 0
        self._start_time: float = 0.0
        self._print_fn = print  # Allow override in tests

        # Create output dir and manifest
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.manifest = ManifestManager(self.output_dir)

    def run(
        self,
        schools_filter: list[str] | None = None,
        phases_filter: list[Phase] | None = None,
    ) -> dict[str, Any]:
        """Run the scraping pipeline.

        Args:
            schools_filter: If provided, only process these school slugs.
            phases_filter: If provided, only run these phases per school.

        Returns:
            Summary dict with counts of results.
        """
        # Install signal handler for graceful shutdown
        original_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, self._signal_handler)

        try:
            return self._execute(schools_filter, phases_filter)
        finally:
            # Restore original signal handler
            signal.signal(signal.SIGINT, original_handler)

    def _signal_handler(self, signum, frame):
        """Handle SIGINT -- request graceful shutdown."""
        logger.info("Received SIGINT, requesting graceful shutdown...")
        print("\nShutdown requested. Finishing current work...")
        self.shutdown_event.set()

    def _execute(
        self,
        schools_filter: list[str] | None,
        phases_filter: list[Phase] | None,
    ) -> dict[str, Any]:
        """Core execution logic."""
        self._start_time = time.monotonic()

        # Step 1: Crash recovery -- reset any SCRAPING to PENDING
        reset_count = self.manifest.reset_scraping_to_pending()
        if reset_count:
            logger.info(
                "Crash recovery: reset %d schools to PENDING", reset_count
            )

        # Step 2: Initialize schools in manifest
        target_schools = self._get_target_schools(schools_filter)
        for school in target_schools:
            self.manifest.init_school(
                school.slug,
                {
                    "unitid": school.unitid,
                    "name": school.name,
                    "url": school.url,
                    "city": school.city,
                    "state": school.state,
                },
            )

        # Step 3: Process schools with thread pool
        results = {"completed": 0, "failed": 0, "skipped": 0, "interrupted": 0}

        # Reset progress counters
        with self._progress_lock:
            self._done_count = 0
            self._fail_count = 0
            self._total_submitted = 0

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {}

            for school in target_schools:
                if self.shutdown_event.is_set():
                    break

                # Try to claim the school
                if not self.manifest.claim_school(school.slug):
                    results["skipped"] += 1
                    continue

                future = executor.submit(
                    self._process_school, school, phases_filter
                )
                futures[future] = school

            # Record total submitted and print start message
            with self._progress_lock:
                self._total_submitted = len(futures)

            if self._total_submitted > 0:
                self._print_fn(
                    f"Starting pipeline for {self._total_submitted} schools..."
                )

            # Wait for all futures to complete
            for future in as_completed(futures):
                school = futures[future]
                try:
                    success = future.result()
                    if success:
                        results["completed"] += 1
                        self._report_progress(school.slug, success=True)
                    elif self.shutdown_event.is_set():
                        results["interrupted"] += 1
                    else:
                        results["failed"] += 1
                        self._report_progress(school.slug, success=False)
                except Exception as e:
                    logger.error(
                        "Worker crashed",
                        extra={"school": school.slug, "error": str(e)},
                    )
                    self.manifest.update_school_status(
                        school.slug, SchoolStatus.FAILED
                    )
                    results["failed"] += 1
                    self._report_progress(school.slug, success=False)

        # Log summary
        logger.info("Pipeline complete", extra={"results": results})
        return results

    @staticmethod
    def _format_elapsed(seconds: float) -> str:
        """Format elapsed seconds as a human-readable string.

        Returns e.g. '5s', '1m 23s', '1h 5m 30s'.
        """
        seconds = int(seconds)
        if seconds < 60:
            return f"{seconds}s"
        minutes, secs = divmod(seconds, 60)
        if minutes < 60:
            return f"{minutes}m {secs:02d}s"
        hours, mins = divmod(minutes, 60)
        return f"{hours}h {mins:02d}m {secs:02d}s"

    def _report_progress(self, slug: str, *, success: bool) -> None:
        """Print a progress line after a school finishes processing.

        Thread-safe: uses _progress_lock to update shared counters.
        """
        with self._progress_lock:
            if success:
                self._done_count += 1
            else:
                self._fail_count += 1
            done = self._done_count
            failed = self._fail_count
            total = self._total_submitted

        elapsed = time.monotonic() - self._start_time
        finished = done + failed
        remaining = total - finished
        status = "Completed" if success else "Failed"
        elapsed_str = self._format_elapsed(elapsed)

        self._print_fn(
            f"[{finished}/{total}] {status} {slug} "
            f"(elapsed: {elapsed_str}) | "
            f"done: {done}, failed: {failed}, remaining: {remaining}"
        )

    def _process_school(
        self,
        school: School,
        phases_filter: list[Phase] | None,
    ) -> bool:
        """Process a single school (runs in worker thread)."""
        worker = SchoolWorker(
            school=school,
            manifest=self.manifest,
            output_dir=self.output_dir,
            config=self.config,
            shutdown_event=self.shutdown_event,
            phase_handlers=self.phase_handlers,
        )

        success = worker.run(phases_filter)

        # Update manifest status and store results summary
        if success:
            self.manifest.update_school_status(school.slug, SchoolStatus.COMPLETED)
            results_summary = self._build_results_summary(school)
            self.manifest.update_school_results(school.slug, results_summary)
        elif not self.shutdown_event.is_set():
            self.manifest.update_school_status(school.slug, SchoolStatus.FAILED)
        # If shutdown, leave as SCRAPING -- will be reset to PENDING on next run

        return success

    def _build_results_summary(self, school: School) -> dict:
        """Read per-school metadata and build a summary for the manifest."""
        school_dir = self.output_dir / school.slug
        meta = SchoolMetadata(school_dir)
        data = meta._metadata

        phases = data.get("phases", {})
        discovery = phases.get("discovery", {})
        downloaded = data.get("downloaded_urls", {})

        # Collect discovered URLs by type
        catalog_urls = discovery.get("catalog_urls", [])
        faculty_urls = discovery.get("faculty_urls", [])
        syllabus_urls = discovery.get("syllabus_urls", [])

        # Build list of downloaded files with source URL and local path
        files = []
        for url, info in downloaded.items():
            files.append({
                "url": url,
                "filepath": info.get("filepath", ""),
                "downloaded_at": info.get("downloaded_at", ""),
            })

        # Phase status summary
        phase_statuses = {
            name: entry.get("status", "unknown")
            for name, entry in phases.items()
        }

        # Check downloaded URLs against robots.txt disallow patterns.
        # Only include robots info if we actually violated a rule.
        robots_info = phases.get("robots", {}).get("robots_info", {})
        robots_violations = self._check_robots_violations(
            downloaded_urls=list(downloaded.keys()),
            disallow_patterns=robots_info.get("disallow_patterns", []),
            robots_url=robots_info.get("url", ""),
        )

        result: dict = {
            "phases": phase_statuses,
            "discovery": {
                "catalog_urls": catalog_urls,
                "faculty_urls": faculty_urls,
                "syllabus_urls": syllabus_urls,
            },
            "files_downloaded": files,
            "file_count": len(files),
            "errors": data.get("errors", []),
        }

        if robots_violations:
            result["robots_violations"] = robots_violations

        return result

    @staticmethod
    def _check_robots_violations(
        downloaded_urls: list[str],
        disallow_patterns: list[str],
        robots_url: str,
    ) -> list[dict] | None:
        """Check if any downloaded URLs match robots.txt disallow rules.

        Returns a list of violation dicts if any were found, or None.
        """
        if not disallow_patterns or not downloaded_urls:
            return None

        violations: list[dict] = []
        for url in downloaded_urls:
            path = urlparse(url).path
            for pattern in disallow_patterns:
                # robots.txt patterns: path prefix match, with * glob support
                if pattern.endswith("*"):
                    if fnmatch.fnmatch(path, pattern):
                        violations.append({"url": url, "matched_rule": pattern})
                        break
                elif path.startswith(pattern):
                    violations.append({"url": url, "matched_rule": pattern})
                    break

        return violations if violations else None

    def _get_target_schools(
        self, schools_filter: list[str] | None
    ) -> list[School]:
        """Filter schools to process."""
        if schools_filter:
            slug_set = set(schools_filter)
            return [s for s in self.schools if s.slug in slug_set]
        return self.schools

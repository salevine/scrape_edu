"""Pipeline orchestrator -- manages concurrent school scraping."""

from __future__ import annotations

import logging
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from scrape_edu.data.manifest import ManifestManager, SchoolStatus
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

            # Wait for all futures to complete
            for future in as_completed(futures):
                school = futures[future]
                try:
                    success = future.result()
                    if success:
                        results["completed"] += 1
                    elif self.shutdown_event.is_set():
                        results["interrupted"] += 1
                    else:
                        results["failed"] += 1
                except Exception as e:
                    logger.error(
                        "Worker crashed",
                        extra={"school": school.slug, "error": str(e)},
                    )
                    self.manifest.update_school_status(
                        school.slug, SchoolStatus.FAILED
                    )
                    results["failed"] += 1

        # Log summary
        logger.info("Pipeline complete", extra={"results": results})
        return results

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

        # Update manifest status
        if success:
            self.manifest.update_school_status(school.slug, SchoolStatus.COMPLETED)
        elif not self.shutdown_event.is_set():
            self.manifest.update_school_status(school.slug, SchoolStatus.FAILED)
        # If shutdown, leave as SCRAPING -- will be reset to PENDING on next run

        return success

    def _get_target_schools(
        self, schools_filter: list[str] | None
    ) -> list[School]:
        """Filter schools to process."""
        if schools_filter:
            slug_set = set(schools_filter)
            return [s for s in self.schools if s.slug in slug_set]
        return self.schools

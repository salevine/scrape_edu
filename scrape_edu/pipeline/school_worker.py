"""Per-school worker -- processes one university through scraping phases."""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Any

from scrape_edu.data.manifest import ManifestManager, SchoolMetadata, SchoolStatus, PhaseStatus
from scrape_edu.data.school import School
from scrape_edu.pipeline.phases import Phase, PHASE_ORDER

logger = logging.getLogger("scrape_edu")


class SchoolWorker:
    """Process a single school through all scraping phases."""

    def __init__(
        self,
        school: School,
        manifest: ManifestManager,
        output_dir: Path,
        config: dict[str, Any],
        shutdown_event: threading.Event,
        # Phase handlers will be injected -- for now, stubs
        phase_handlers: dict[Phase, Any] | None = None,
    ):
        self.school = school
        self.manifest = manifest
        self.output_dir = output_dir
        self.config = config
        self.shutdown_event = shutdown_event
        self.phase_handlers = phase_handlers or {}
        self.school_dir = output_dir / school.slug
        self.metadata = SchoolMetadata(self.school_dir)

    def run(self, phases_filter: list[Phase] | None = None) -> bool:
        """Run all phases for this school.

        Args:
            phases_filter: If provided, only run these phases. Otherwise run all.

        Returns:
            True if the school completed successfully, False otherwise.
        """
        # Determine which phases to run
        phases_to_run = phases_filter if phases_filter else PHASE_ORDER

        any_failed = False
        for phase in phases_to_run:
            # Check for shutdown signal
            if self.shutdown_event.is_set():
                logger.info(
                    "Shutdown requested, stopping",
                    extra={"school": self.school.slug},
                )
                return False

            # Skip already completed phases (for resume)
            phase_status = self.metadata.get_phase_status(phase.value)
            if phase_status == PhaseStatus.COMPLETED:
                logger.info(
                    "Skipping completed phase",
                    extra={"school": self.school.slug, "phase": phase.value},
                )
                continue

            # Run the phase
            logger.info(
                "Starting phase",
                extra={"school": self.school.slug, "phase": phase.value},
            )
            self.metadata.update_phase(phase.value, PhaseStatus.RUNNING)
            self.metadata.save()

            try:
                handler = self.phase_handlers.get(phase)
                if handler is not None:
                    handler(self.school, self.school_dir, self.metadata, self.config)
                else:
                    # No handler registered -- skip (will be added in Phase 5)
                    logger.debug(
                        "No handler for phase",
                        extra={"school": self.school.slug, "phase": phase.value},
                    )

                self.metadata.update_phase(phase.value, PhaseStatus.COMPLETED)
                self.metadata.save()

            except Exception as e:
                logger.error(
                    "Phase failed",
                    extra={
                        "school": self.school.slug,
                        "phase": phase.value,
                        "error": str(e),
                    },
                )
                self.metadata.update_phase(phase.value, PhaseStatus.FAILED)
                self.metadata.add_error(phase.value, str(e))
                self.metadata.save()
                any_failed = True
                # Errors in one phase don't block the next -- continue

        return not any_failed

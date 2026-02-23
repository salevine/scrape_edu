"""Thread-safe manifest manager for tracking scrape progress."""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from scrape_edu.utils.file_utils import atomic_json_write

logger = logging.getLogger(__name__)


class SchoolStatus(str, Enum):
    """Overall status of a school in the scrape pipeline."""

    PENDING = "pending"
    SCRAPING = "scraping"
    COMPLETED = "completed"
    FAILED = "failed"
    FLAGGED_RESCRAPE = "flagged_rescrape"


class PhaseStatus(str, Enum):
    """Status of an individual phase within a school's scrape."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class ManifestManager:
    """Thread-safe manifest.json manager.

    The manifest tracks the overall status of each school across the
    scraping pipeline.  It is stored as a single JSON file in the
    output directory and is updated atomically.

    All public methods are thread-safe.
    """

    def __init__(self, output_dir: Path) -> None:
        self.output_dir = Path(output_dir)
        self.manifest_path = self.output_dir / "manifest.json"
        self._lock = threading.Lock()
        self._manifest = self._load_or_create()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_or_create(self) -> dict:
        """Load existing manifest or create a new one."""
        if self.manifest_path.exists():
            try:
                with open(self.manifest_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                logger.info(
                    "Loaded existing manifest with %d schools",
                    len(data.get("schools", {})),
                )
                return data
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning(
                    "Failed to load manifest, creating new one: %s", exc
                )

        return {
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
            "schools": {},
        }

    def _save(self) -> None:
        """Save manifest to disk (caller must hold *_lock*)."""
        self._manifest["updated_at"] = _now_iso()
        atomic_json_write(self.manifest_path, self._manifest)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def init_school(self, slug: str, school_data: dict) -> None:
        """Initialize a school entry in the manifest.

        If the school already exists, this is a no-op.

        Args:
            slug: Filesystem-safe school identifier.
            school_data: Arbitrary metadata dict (unitid, name, url, ...).
        """
        with self._lock:
            if slug not in self._manifest["schools"]:
                self._manifest["schools"][slug] = {
                    "status": SchoolStatus.PENDING.value,
                    "data": school_data,
                    "created_at": _now_iso(),
                    "updated_at": _now_iso(),
                }
                self._save()

    def claim_school(self, slug: str) -> bool:
        """Atomically set school status from PENDING to SCRAPING.

        Returns:
            True if the school was successfully claimed, False otherwise
            (e.g. it was already claimed by another thread).
        """
        with self._lock:
            school = self._manifest["schools"].get(slug)
            if school is None:
                return False

            current = school["status"]
            if current in (
                SchoolStatus.PENDING.value,
                SchoolStatus.FLAGGED_RESCRAPE.value,
            ):
                school["status"] = SchoolStatus.SCRAPING.value
                school["updated_at"] = _now_iso()
                self._save()
                return True

            return False

    def update_school_status(self, slug: str, status: SchoolStatus) -> None:
        """Update a school's overall status.

        Args:
            slug: School identifier.
            status: New status value.
        """
        with self._lock:
            school = self._manifest["schools"].get(slug)
            if school is None:
                logger.warning(
                    "Cannot update status for unknown school: %s", slug
                )
                return
            school["status"] = status.value
            school["updated_at"] = _now_iso()
            self._save()

    def get_school_status(self, slug: str) -> SchoolStatus | None:
        """Get current status of a school.

        Returns:
            The school's status, or None if the school is not in the manifest.
        """
        with self._lock:
            school = self._manifest["schools"].get(slug)
            if school is None:
                return None
            return SchoolStatus(school["status"])

    def get_pending_schools(self) -> list[str]:
        """Get all school slugs with PENDING or FLAGGED_RESCRAPE status."""
        with self._lock:
            pending_statuses = {
                SchoolStatus.PENDING.value,
                SchoolStatus.FLAGGED_RESCRAPE.value,
            }
            return [
                slug
                for slug, info in self._manifest["schools"].items()
                if info["status"] in pending_statuses
            ]

    def reset_scraping_to_pending(self) -> int:
        """Reset any SCRAPING schools to PENDING (crash recovery).

        Returns:
            Number of schools reset.
        """
        with self._lock:
            count = 0
            for info in self._manifest["schools"].values():
                if info["status"] == SchoolStatus.SCRAPING.value:
                    info["status"] = SchoolStatus.PENDING.value
                    info["updated_at"] = _now_iso()
                    count += 1
            if count:
                self._save()
                logger.info("Reset %d SCRAPING schools to PENDING", count)
            return count

    def flag_rescrape(self, slugs: list[str] | None = None) -> int:
        """Flag schools for re-scraping.

        Args:
            slugs: Specific school slugs to flag. If None, flag all schools.

        Returns:
            Number of schools flagged.
        """
        with self._lock:
            count = 0
            targets = (
                slugs
                if slugs is not None
                else list(self._manifest["schools"].keys())
            )
            for slug in targets:
                info = self._manifest["schools"].get(slug)
                if info is not None:
                    info["status"] = SchoolStatus.FLAGGED_RESCRAPE.value
                    info["updated_at"] = _now_iso()
                    count += 1
            if count:
                self._save()
                logger.info("Flagged %d schools for re-scrape", count)
            return count

    def update_school_results(self, slug: str, results: dict) -> None:
        """Store a results summary for a completed school.

        Args:
            slug: School identifier.
            results: Dict with retrieval results (discovered URLs, downloaded
                files, phase statuses, etc.).
        """
        with self._lock:
            school = self._manifest["schools"].get(slug)
            if school is None:
                logger.warning(
                    "Cannot update results for unknown school: %s", slug
                )
                return
            school["results"] = results
            school["updated_at"] = _now_iso()
            self._save()

    def get_summary(self) -> dict:
        """Return a summary of school counts grouped by status.

        Returns:
            Dict mapping status value strings to counts, e.g.
            ``{"pending": 10, "completed": 5, ...}``.
        """
        with self._lock:
            summary: dict[str, int] = {}
            for info in self._manifest["schools"].values():
                status = info["status"]
                summary[status] = summary.get(status, 0) + 1
            return summary


class SchoolMetadata:
    """Per-school metadata.json manager.

    Each school directory has its own ``metadata.json`` that tracks
    phase statuses, errors, and downloaded URLs.  Since each school
    is processed by a single worker thread at a time, this class does
    not need cross-thread synchronization.
    """

    def __init__(self, school_dir: Path) -> None:
        self.school_dir = Path(school_dir)
        self.metadata_path = self.school_dir / "metadata.json"
        self._metadata = self._load_or_create()

    def _load_or_create(self) -> dict:
        """Load existing metadata or create a new structure."""
        if self.metadata_path.exists():
            try:
                with open(self.metadata_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning(
                    "Failed to load metadata at %s, creating new: %s",
                    self.metadata_path,
                    exc,
                )

        return {
            "phases": {},
            "errors": [],
            "downloaded_urls": {},
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
        }

    def update_phase(self, phase: str, status: PhaseStatus, **extra) -> None:
        """Update the status of a specific phase.

        Args:
            phase: Phase name (e.g. "discovery", "catalog", "syllabi").
            status: New phase status.
            **extra: Additional key-value pairs to store on the phase entry.
        """
        entry = self._metadata["phases"].get(phase, {})
        entry["status"] = status.value
        entry["updated_at"] = _now_iso()
        entry.update(extra)
        self._metadata["phases"][phase] = entry

    def get_phase_status(self, phase: str) -> PhaseStatus | None:
        """Get the status of a specific phase.

        Returns:
            The phase's status, or None if the phase has not been recorded.
        """
        entry = self._metadata["phases"].get(phase)
        if entry is None:
            return None
        return PhaseStatus(entry["status"])

    def add_error(self, phase: str, error: str) -> None:
        """Log an error for a phase.

        Args:
            phase: Phase during which the error occurred.
            error: Human-readable error description.
        """
        self._metadata["errors"].append(
            {
                "phase": phase,
                "error": error,
                "timestamp": _now_iso(),
            }
        )

    def add_downloaded_url(self, url: str, filepath: str) -> None:
        """Track a downloaded URL to avoid re-downloading.

        Args:
            url: The URL that was downloaded.
            filepath: Local path where the content was saved.
        """
        self._metadata["downloaded_urls"][url] = {
            "filepath": filepath,
            "downloaded_at": _now_iso(),
        }

    def is_url_downloaded(self, url: str) -> bool:
        """Check if a URL has already been downloaded."""
        return url in self._metadata["downloaded_urls"]

    def save(self) -> None:
        """Save metadata to disk using atomic writes."""
        self._metadata["updated_at"] = _now_iso()
        self.school_dir.mkdir(parents=True, exist_ok=True)
        atomic_json_write(self.metadata_path, self._metadata)


def _now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()

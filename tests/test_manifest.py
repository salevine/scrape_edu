"""Tests for scrape_edu.data.manifest module."""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from scrape_edu.data.manifest import (
    ManifestManager,
    PhaseStatus,
    SchoolMetadata,
    SchoolStatus,
)


# ======================================================================
# ManifestManager tests
# ======================================================================


class TestManifestManagerInit:
    """Test manifest creation and loading."""

    def test_creates_new_manifest(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        # Manifest file is not written until the first mutation
        assert not mm.manifest_path.exists()
        # Manifest starts empty
        assert mm.get_summary() == {}

    def test_manifest_saved_after_init_school(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})
        assert mm.manifest_path.exists()

    def test_loads_existing_manifest(self, tmp_path: Path) -> None:
        # Create a manifest manually
        manifest_data = {
            "created_at": "2025-01-01T00:00:00+00:00",
            "updated_at": "2025-01-01T00:00:00+00:00",
            "schools": {
                "mit": {
                    "status": "completed",
                    "data": {"name": "MIT"},
                    "created_at": "2025-01-01T00:00:00+00:00",
                    "updated_at": "2025-01-01T00:00:00+00:00",
                }
            },
        }
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_data))

        mm = ManifestManager(tmp_path)
        assert mm.get_school_status("mit") == SchoolStatus.COMPLETED

    def test_creates_parent_directories(self, tmp_path: Path) -> None:
        output_dir = tmp_path / "deep" / "nested" / "output"
        mm = ManifestManager(output_dir)
        mm.init_school("test", {"name": "Test"})
        assert mm.manifest_path.exists()

    def test_handles_corrupt_manifest(self, tmp_path: Path) -> None:
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text("{corrupt json!!")

        # Should create a fresh manifest instead of crashing
        mm = ManifestManager(tmp_path)
        assert mm.get_summary() == {}


class TestManifestManagerInitSchool:
    """Test initializing school entries."""

    def test_init_new_school(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT", "unitid": 166683})

        status = mm.get_school_status("mit")
        assert status == SchoolStatus.PENDING

    def test_init_existing_school_is_noop(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT", "unitid": 166683})
        mm.update_school_status("mit", SchoolStatus.COMPLETED)

        # Re-init should not overwrite
        mm.init_school("mit", {"name": "MIT (updated)", "unitid": 166683})
        assert mm.get_school_status("mit") == SchoolStatus.COMPLETED

    def test_init_multiple_schools(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})
        mm.init_school("stanford", {"name": "Stanford"})
        mm.init_school("cmu", {"name": "CMU"})

        summary = mm.get_summary()
        assert summary.get("pending") == 3


class TestManifestManagerClaim:
    """Test the thread-safe claim pattern."""

    def test_claim_pending_school(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})

        assert mm.claim_school("mit") is True
        assert mm.get_school_status("mit") == SchoolStatus.SCRAPING

    def test_claim_already_claimed(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})

        assert mm.claim_school("mit") is True
        assert mm.claim_school("mit") is False  # second claim fails

    def test_claim_completed_school(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})
        mm.update_school_status("mit", SchoolStatus.COMPLETED)

        assert mm.claim_school("mit") is False

    def test_claim_failed_school(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})
        mm.update_school_status("mit", SchoolStatus.FAILED)

        assert mm.claim_school("mit") is False

    def test_claim_flagged_rescrape_school(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})
        mm.update_school_status("mit", SchoolStatus.FLAGGED_RESCRAPE)

        assert mm.claim_school("mit") is True
        assert mm.get_school_status("mit") == SchoolStatus.SCRAPING

    def test_claim_nonexistent_school(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        assert mm.claim_school("nonexistent") is False

    def test_thread_safe_claim(self, tmp_path: Path) -> None:
        """Two threads claiming the same school -- only one should succeed."""
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})

        results: list[bool] = []
        barrier = threading.Barrier(2)

        def claim_worker() -> None:
            barrier.wait()  # Synchronize start
            result = mm.claim_school("mit")
            results.append(result)

        t1 = threading.Thread(target=claim_worker)
        t2 = threading.Thread(target=claim_worker)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Exactly one thread should succeed
        assert results.count(True) == 1
        assert results.count(False) == 1
        assert mm.get_school_status("mit") == SchoolStatus.SCRAPING

    def test_thread_safe_claim_many_schools(self, tmp_path: Path) -> None:
        """Multiple threads claiming different schools concurrently."""
        mm = ManifestManager(tmp_path)
        slugs = [f"school-{i}" for i in range(20)]
        for slug in slugs:
            mm.init_school(slug, {"name": slug})

        claimed: list[str] = []
        lock = threading.Lock()

        def worker(school_slug: str) -> None:
            if mm.claim_school(school_slug):
                with lock:
                    claimed.append(school_slug)

        threads = [threading.Thread(target=worker, args=(s,)) for s in slugs]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All schools should be claimed
        assert sorted(claimed) == sorted(slugs)


class TestManifestManagerStatusTransitions:
    """Test status update operations."""

    def test_update_to_completed(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})
        mm.update_school_status("mit", SchoolStatus.COMPLETED)
        assert mm.get_school_status("mit") == SchoolStatus.COMPLETED

    def test_update_to_failed(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})
        mm.update_school_status("mit", SchoolStatus.FAILED)
        assert mm.get_school_status("mit") == SchoolStatus.FAILED

    def test_update_unknown_school_logs_warning(
        self, tmp_path: Path, caplog
    ) -> None:
        mm = ManifestManager(tmp_path)
        mm.update_school_status("unknown", SchoolStatus.COMPLETED)
        assert mm.get_school_status("unknown") is None

    def test_get_status_nonexistent(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        assert mm.get_school_status("nope") is None


class TestManifestManagerPending:
    """Test retrieving pending schools."""

    def test_get_pending_schools(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})
        mm.init_school("stanford", {"name": "Stanford"})
        mm.init_school("cmu", {"name": "CMU"})

        pending = mm.get_pending_schools()
        assert sorted(pending) == ["cmu", "mit", "stanford"]

    def test_pending_excludes_completed(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})
        mm.init_school("stanford", {"name": "Stanford"})
        mm.update_school_status("mit", SchoolStatus.COMPLETED)

        pending = mm.get_pending_schools()
        assert pending == ["stanford"]

    def test_pending_includes_flagged_rescrape(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})
        mm.update_school_status("mit", SchoolStatus.FLAGGED_RESCRAPE)

        pending = mm.get_pending_schools()
        assert pending == ["mit"]

    def test_pending_excludes_scraping(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})
        mm.claim_school("mit")

        pending = mm.get_pending_schools()
        assert pending == []


class TestManifestManagerResetScraping:
    """Test crash recovery -- resetting SCRAPING to PENDING."""

    def test_reset_scraping(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})
        mm.init_school("stanford", {"name": "Stanford"})
        mm.claim_school("mit")
        mm.claim_school("stanford")

        count = mm.reset_scraping_to_pending()
        assert count == 2
        assert mm.get_school_status("mit") == SchoolStatus.PENDING
        assert mm.get_school_status("stanford") == SchoolStatus.PENDING

    def test_reset_does_not_affect_other_statuses(
        self, tmp_path: Path
    ) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})
        mm.init_school("stanford", {"name": "Stanford"})
        mm.init_school("cmu", {"name": "CMU"})

        mm.update_school_status("mit", SchoolStatus.COMPLETED)
        mm.update_school_status("stanford", SchoolStatus.FAILED)
        # cmu stays PENDING

        count = mm.reset_scraping_to_pending()
        assert count == 0
        assert mm.get_school_status("mit") == SchoolStatus.COMPLETED
        assert mm.get_school_status("stanford") == SchoolStatus.FAILED
        assert mm.get_school_status("cmu") == SchoolStatus.PENDING

    def test_reset_returns_zero_when_nothing_to_reset(
        self, tmp_path: Path
    ) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})
        assert mm.reset_scraping_to_pending() == 0


class TestManifestManagerFlagRescrape:
    """Test flagging schools for re-scraping."""

    def test_flag_specific_schools(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})
        mm.init_school("stanford", {"name": "Stanford"})
        mm.update_school_status("mit", SchoolStatus.COMPLETED)
        mm.update_school_status("stanford", SchoolStatus.COMPLETED)

        count = mm.flag_rescrape(["mit"])
        assert count == 1
        assert mm.get_school_status("mit") == SchoolStatus.FLAGGED_RESCRAPE
        assert mm.get_school_status("stanford") == SchoolStatus.COMPLETED

    def test_flag_all_schools(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})
        mm.init_school("stanford", {"name": "Stanford"})

        count = mm.flag_rescrape()
        assert count == 2
        assert mm.get_school_status("mit") == SchoolStatus.FLAGGED_RESCRAPE
        assert mm.get_school_status("stanford") == SchoolStatus.FLAGGED_RESCRAPE

    def test_flag_nonexistent_slug(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})

        count = mm.flag_rescrape(["nonexistent"])
        assert count == 0

    def test_flag_empty_list(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})

        count = mm.flag_rescrape([])
        assert count == 0


class TestManifestManagerSummary:
    """Test summary generation."""

    def test_summary_empty(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        assert mm.get_summary() == {}

    def test_summary_counts(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("a", {"name": "A"})
        mm.init_school("b", {"name": "B"})
        mm.init_school("c", {"name": "C"})
        mm.init_school("d", {"name": "D"})
        mm.init_school("e", {"name": "E"})

        mm.update_school_status("a", SchoolStatus.COMPLETED)
        mm.update_school_status("b", SchoolStatus.COMPLETED)
        mm.update_school_status("c", SchoolStatus.FAILED)
        mm.claim_school("d")
        # e stays PENDING

        summary = mm.get_summary()
        assert summary == {
            "completed": 2,
            "failed": 1,
            "scraping": 1,
            "pending": 1,
        }


class TestManifestManagerAtomicSave:
    """Test that manifest persists across reloads."""

    def test_persistence_across_reload(self, tmp_path: Path) -> None:
        mm1 = ManifestManager(tmp_path)
        mm1.init_school("mit", {"name": "MIT"})
        mm1.update_school_status("mit", SchoolStatus.COMPLETED)

        # Reload from disk
        mm2 = ManifestManager(tmp_path)
        assert mm2.get_school_status("mit") == SchoolStatus.COMPLETED

    def test_save_creates_valid_json(self, tmp_path: Path) -> None:
        mm = ManifestManager(tmp_path)
        mm.init_school("mit", {"name": "MIT"})

        # Directly read and parse the file
        data = json.loads(mm.manifest_path.read_text())
        assert "schools" in data
        assert "mit" in data["schools"]
        assert data["schools"]["mit"]["status"] == "pending"


# ======================================================================
# SchoolMetadata tests
# ======================================================================


class TestSchoolMetadataInit:
    """Test metadata creation and loading."""

    def test_creates_new_metadata(self, tmp_path: Path) -> None:
        school_dir = tmp_path / "mit"
        school_dir.mkdir()
        sm = SchoolMetadata(school_dir)
        sm.save()
        assert sm.metadata_path.exists()

    def test_loads_existing_metadata(self, tmp_path: Path) -> None:
        school_dir = tmp_path / "mit"
        school_dir.mkdir()
        metadata = {
            "phases": {"discovery": {"status": "completed", "updated_at": "..."}},
            "errors": [],
            "downloaded_urls": {},
            "created_at": "...",
            "updated_at": "...",
        }
        (school_dir / "metadata.json").write_text(json.dumps(metadata))

        sm = SchoolMetadata(school_dir)
        assert sm.get_phase_status("discovery") == PhaseStatus.COMPLETED


class TestSchoolMetadataPhases:
    """Test phase status management."""

    def test_update_and_get_phase(self, tmp_path: Path) -> None:
        school_dir = tmp_path / "mit"
        school_dir.mkdir()
        sm = SchoolMetadata(school_dir)

        sm.update_phase("discovery", PhaseStatus.RUNNING)
        assert sm.get_phase_status("discovery") == PhaseStatus.RUNNING

        sm.update_phase("discovery", PhaseStatus.COMPLETED, urls_found=5)
        assert sm.get_phase_status("discovery") == PhaseStatus.COMPLETED

    def test_get_unknown_phase(self, tmp_path: Path) -> None:
        school_dir = tmp_path / "mit"
        school_dir.mkdir()
        sm = SchoolMetadata(school_dir)
        assert sm.get_phase_status("nonexistent") is None

    def test_extra_kwargs_stored(self, tmp_path: Path) -> None:
        school_dir = tmp_path / "mit"
        school_dir.mkdir()
        sm = SchoolMetadata(school_dir)
        sm.update_phase("catalog", PhaseStatus.COMPLETED, pages_saved=42)
        sm.save()

        # Reload and verify
        sm2 = SchoolMetadata(school_dir)
        assert sm2._metadata["phases"]["catalog"]["pages_saved"] == 42


class TestSchoolMetadataErrors:
    """Test error logging."""

    def test_add_error(self, tmp_path: Path) -> None:
        school_dir = tmp_path / "mit"
        school_dir.mkdir()
        sm = SchoolMetadata(school_dir)

        sm.add_error("discovery", "Timeout connecting to example.com")
        assert len(sm._metadata["errors"]) == 1
        assert sm._metadata["errors"][0]["phase"] == "discovery"
        assert "Timeout" in sm._metadata["errors"][0]["error"]

    def test_multiple_errors(self, tmp_path: Path) -> None:
        school_dir = tmp_path / "mit"
        school_dir.mkdir()
        sm = SchoolMetadata(school_dir)

        sm.add_error("discovery", "Error 1")
        sm.add_error("catalog", "Error 2")
        sm.add_error("discovery", "Error 3")

        assert len(sm._metadata["errors"]) == 3


class TestSchoolMetadataDownloads:
    """Test URL download tracking."""

    def test_add_and_check_downloaded_url(self, tmp_path: Path) -> None:
        school_dir = tmp_path / "mit"
        school_dir.mkdir()
        sm = SchoolMetadata(school_dir)

        url = "https://example.com/catalog.pdf"
        assert sm.is_url_downloaded(url) is False

        sm.add_downloaded_url(url, "catalog/catalog.pdf")
        assert sm.is_url_downloaded(url) is True

    def test_different_urls_tracked_independently(
        self, tmp_path: Path
    ) -> None:
        school_dir = tmp_path / "mit"
        school_dir.mkdir()
        sm = SchoolMetadata(school_dir)

        sm.add_downloaded_url("https://a.com/1.pdf", "1.pdf")
        assert sm.is_url_downloaded("https://a.com/1.pdf") is True
        assert sm.is_url_downloaded("https://a.com/2.pdf") is False


class TestSchoolMetadataPersistence:
    """Test that metadata persists to disk."""

    def test_save_and_reload(self, tmp_path: Path) -> None:
        school_dir = tmp_path / "mit"
        school_dir.mkdir()
        sm = SchoolMetadata(school_dir)
        sm.update_phase("discovery", PhaseStatus.COMPLETED)
        sm.add_error("catalog", "some error")
        sm.add_downloaded_url("https://example.com/x.pdf", "x.pdf")
        sm.save()

        # Reload
        sm2 = SchoolMetadata(school_dir)
        assert sm2.get_phase_status("discovery") == PhaseStatus.COMPLETED
        assert len(sm2._metadata["errors"]) == 1
        assert sm2.is_url_downloaded("https://example.com/x.pdf") is True

    def test_save_creates_school_dir(self, tmp_path: Path) -> None:
        school_dir = tmp_path / "new-school"
        sm = SchoolMetadata(school_dir)
        sm.update_phase("discovery", PhaseStatus.PENDING)
        sm.save()
        assert school_dir.exists()
        assert sm.metadata_path.exists()

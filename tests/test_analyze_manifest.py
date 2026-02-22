"""Tests for scripts/analyze_manifest.py module."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Import the module under test from scripts/ directory.
_scripts_dir = str(Path(__file__).resolve().parent.parent / "scripts")
if _scripts_dir not in sys.path:
    sys.path.insert(0, _scripts_dir)

import analyze_manifest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_manifest(output_dir: Path, schools: dict) -> None:
    """Write a manifest.json with the given schools dict."""
    manifest = {
        "created_at": "2025-01-01T00:00:00+00:00",
        "updated_at": "2025-01-01T00:00:00+00:00",
        "schools": schools,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "manifest.json").write_text(json.dumps(manifest))


def _write_metadata(output_dir: Path, slug: str, metadata: dict) -> None:
    """Write a per-school metadata.json."""
    school_dir = output_dir / slug
    school_dir.mkdir(parents=True, exist_ok=True)
    (school_dir / "metadata.json").write_text(json.dumps(metadata))


def _make_school_entry(status: str, name: str | None = None) -> dict:
    """Create a minimal school entry for the manifest."""
    return {
        "status": status,
        "data": {"name": name or status},
        "created_at": "2025-01-01T00:00:00+00:00",
        "updated_at": "2025-01-01T00:00:00+00:00",
    }


def _make_metadata(
    phases: dict | None = None,
    errors: list | None = None,
) -> dict:
    """Create a minimal per-school metadata dict."""
    return {
        "phases": phases or {},
        "errors": errors or [],
        "downloaded_urls": {},
        "created_at": "2025-01-01T00:00:00+00:00",
        "updated_at": "2025-01-01T00:00:00+00:00",
    }


# ---------------------------------------------------------------------------
# load_manifest
# ---------------------------------------------------------------------------


class TestLoadManifest:
    """Test manifest loading."""

    def test_load_valid_manifest(self, tmp_path: Path) -> None:
        _write_manifest(tmp_path, {"mit": _make_school_entry("completed")})
        result = analyze_manifest.load_manifest(tmp_path)
        assert "mit" in result["schools"]
        assert result["schools"]["mit"]["status"] == "completed"

    def test_load_missing_manifest(self, tmp_path: Path) -> None:
        result = analyze_manifest.load_manifest(tmp_path)
        assert result == {"schools": {}}

    def test_load_corrupt_manifest(self, tmp_path: Path) -> None:
        (tmp_path / "manifest.json").write_text("{bad json!!")
        result = analyze_manifest.load_manifest(tmp_path)
        assert result == {"schools": {}}


# ---------------------------------------------------------------------------
# load_school_metadata
# ---------------------------------------------------------------------------


class TestLoadSchoolMetadata:
    """Test per-school metadata loading."""

    def test_load_valid_metadata(self, tmp_path: Path) -> None:
        _write_metadata(tmp_path, "mit", _make_metadata(
            phases={"discovery": {"status": "completed"}},
        ))
        result = analyze_manifest.load_school_metadata(tmp_path, "mit")
        assert result is not None
        assert result["phases"]["discovery"]["status"] == "completed"

    def test_load_missing_metadata(self, tmp_path: Path) -> None:
        result = analyze_manifest.load_school_metadata(tmp_path, "mit")
        assert result is None

    def test_load_corrupt_metadata(self, tmp_path: Path) -> None:
        school_dir = tmp_path / "mit"
        school_dir.mkdir()
        (school_dir / "metadata.json").write_text("not json")
        result = analyze_manifest.load_school_metadata(tmp_path, "mit")
        assert result is None


# ---------------------------------------------------------------------------
# get_dir_size
# ---------------------------------------------------------------------------


class TestGetDirSize:
    """Test directory size calculation."""

    def test_empty_dir(self, tmp_path: Path) -> None:
        assert analyze_manifest.get_dir_size(tmp_path) == 0

    def test_nonexistent_dir(self, tmp_path: Path) -> None:
        assert analyze_manifest.get_dir_size(tmp_path / "nope") == 0

    def test_dir_with_files(self, tmp_path: Path) -> None:
        (tmp_path / "a.txt").write_bytes(b"x" * 100)
        (tmp_path / "b.txt").write_bytes(b"y" * 200)
        assert analyze_manifest.get_dir_size(tmp_path) == 300

    def test_nested_files(self, tmp_path: Path) -> None:
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "c.txt").write_bytes(b"z" * 50)
        (tmp_path / "a.txt").write_bytes(b"x" * 100)
        assert analyze_manifest.get_dir_size(tmp_path) == 150


# ---------------------------------------------------------------------------
# format_bytes
# ---------------------------------------------------------------------------


class TestFormatBytes:
    """Test human-readable byte formatting."""

    def test_bytes(self) -> None:
        assert analyze_manifest.format_bytes(0) == "0 B"
        assert analyze_manifest.format_bytes(512) == "512 B"
        assert analyze_manifest.format_bytes(1023) == "1023 B"

    def test_kilobytes(self) -> None:
        assert analyze_manifest.format_bytes(1024) == "1.0 KB"
        assert analyze_manifest.format_bytes(1536) == "1.5 KB"

    def test_megabytes(self) -> None:
        assert analyze_manifest.format_bytes(1024 * 1024) == "1.0 MB"
        assert analyze_manifest.format_bytes(int(1.5 * 1024 * 1024)) == "1.5 MB"

    def test_gigabytes(self) -> None:
        assert analyze_manifest.format_bytes(1024 * 1024 * 1024) == "1.0 GB"


# ---------------------------------------------------------------------------
# analyze
# ---------------------------------------------------------------------------


class TestAnalyze:
    """Test the main analyze function."""

    def test_empty_output_dir(self, tmp_path: Path) -> None:
        """Analysis of an empty directory returns zeroed-out stats."""
        result = analyze_manifest.analyze(tmp_path)
        assert result["total_schools"] == 0
        assert result["school_statuses"] == {}
        assert result["phase_completion"] == {}
        assert result["error_summary"]["total_errors"] == 0
        assert result["error_summary"]["top_errors"] == []
        assert result["error_summary"]["schools_with_errors"] == []
        assert result["file_counts"]["total_files"] == 0
        assert result["file_counts"]["by_extension"] == {}

    def test_school_status_breakdown(self, tmp_path: Path) -> None:
        """Status counts are correctly computed."""
        _write_manifest(tmp_path, {
            "mit": _make_school_entry("completed", "MIT"),
            "stanford": _make_school_entry("completed", "Stanford"),
            "cmu": _make_school_entry("failed", "CMU"),
            "berkeley": _make_school_entry("pending", "Berkeley"),
            "caltech": _make_school_entry("pending", "Caltech"),
            "harvard": _make_school_entry("scraping", "Harvard"),
        })

        result = analyze_manifest.analyze(tmp_path)
        assert result["total_schools"] == 6
        assert result["school_statuses"]["completed"] == 2
        assert result["school_statuses"]["failed"] == 1
        assert result["school_statuses"]["pending"] == 2
        assert result["school_statuses"]["scraping"] == 1

    def test_phase_completion(self, tmp_path: Path) -> None:
        """Phase completion rates are computed from metadata files."""
        _write_manifest(tmp_path, {
            "mit": _make_school_entry("completed"),
            "stanford": _make_school_entry("failed"),
            "cmu": _make_school_entry("completed"),
        })
        _write_metadata(tmp_path, "mit", _make_metadata(
            phases={
                "robots": {"status": "completed"},
                "discovery": {"status": "completed"},
                "catalog": {"status": "completed"},
            }
        ))
        _write_metadata(tmp_path, "stanford", _make_metadata(
            phases={
                "robots": {"status": "completed"},
                "discovery": {"status": "completed"},
                "catalog": {"status": "failed"},
            }
        ))
        _write_metadata(tmp_path, "cmu", _make_metadata(
            phases={
                "robots": {"status": "completed"},
                "discovery": {"status": "completed"},
                "catalog": {"status": "completed"},
            }
        ))

        result = analyze_manifest.analyze(tmp_path)
        phases = result["phase_completion"]
        assert phases["robots"] == {"completed": 3}
        assert phases["discovery"] == {"completed": 3}
        assert phases["catalog"]["completed"] == 2
        assert phases["catalog"]["failed"] == 1

    def test_error_summary(self, tmp_path: Path) -> None:
        """Error aggregation with top errors and affected schools."""
        _write_manifest(tmp_path, {
            "mit": _make_school_entry("failed"),
            "stanford": _make_school_entry("failed"),
            "cmu": _make_school_entry("completed"),
        })
        _write_metadata(tmp_path, "mit", _make_metadata(
            errors=[
                {"phase": "catalog", "error": "Connection timeout", "timestamp": "..."},
                {"phase": "syllabi", "error": "Connection timeout", "timestamp": "..."},
            ]
        ))
        _write_metadata(tmp_path, "stanford", _make_metadata(
            errors=[
                {"phase": "catalog", "error": "404 Not Found", "timestamp": "..."},
            ]
        ))
        _write_metadata(tmp_path, "cmu", _make_metadata(errors=[]))

        result = analyze_manifest.analyze(tmp_path)
        err = result["error_summary"]
        assert err["total_errors"] == 3
        assert len(err["top_errors"]) == 2
        # Connection timeout appears 2x, should be first
        assert err["top_errors"][0]["error"] == "Connection timeout"
        assert err["top_errors"][0]["count"] == 2
        assert err["top_errors"][1]["error"] == "404 Not Found"
        assert err["top_errors"][1]["count"] == 1
        assert sorted(err["schools_with_errors"]) == ["mit", "stanford"]

    def test_top_errors_limited_to_five(self, tmp_path: Path) -> None:
        """Only the top 5 errors are returned."""
        _write_manifest(tmp_path, {
            "school": _make_school_entry("failed"),
        })
        errors = [
            {"phase": "p", "error": f"Error type {i}", "timestamp": "..."}
            for i in range(10)
        ]
        _write_metadata(tmp_path, "school", _make_metadata(errors=errors))

        result = analyze_manifest.analyze(tmp_path)
        assert len(result["error_summary"]["top_errors"]) == 5

    def test_school_without_metadata(self, tmp_path: Path) -> None:
        """Schools listed in manifest but without metadata.json are handled."""
        _write_manifest(tmp_path, {
            "mit": _make_school_entry("pending"),
        })
        # No metadata.json created for mit
        result = analyze_manifest.analyze(tmp_path)
        assert result["total_schools"] == 1
        assert result["school_statuses"]["pending"] == 1
        assert result["phase_completion"] == {}
        assert result["error_summary"]["total_errors"] == 0

    def test_file_counts(self, tmp_path: Path) -> None:
        """File counts include totals and by-extension breakdown."""
        _write_manifest(tmp_path, {
            "mit": _make_school_entry("completed"),
        })
        school_dir = tmp_path / "mit"
        school_dir.mkdir(exist_ok=True)
        (school_dir / "metadata.json").write_text("{}")
        catalog_dir = school_dir / "catalog"
        catalog_dir.mkdir()
        (catalog_dir / "course1.pdf").write_bytes(b"pdf1")
        (catalog_dir / "course2.pdf").write_bytes(b"pdf2")
        faculty_dir = school_dir / "faculty"
        faculty_dir.mkdir()
        (faculty_dir / "page.html").write_bytes(b"<html>")
        (faculty_dir / "data.json").write_text("{}")

        result = analyze_manifest.analyze(tmp_path)
        fc = result["file_counts"]
        assert fc["total_files"] == 5  # 1 metadata + 2 pdf + 1 html + 1 json
        assert fc["by_extension"][".pdf"] == 2
        assert fc["by_extension"][".html"] == 1
        assert fc["by_extension"][".json"] == 2

    def test_file_counts_empty(self, tmp_path: Path) -> None:
        """File counts are zero when no school directories exist."""
        _write_manifest(tmp_path, {
            "mit": _make_school_entry("pending"),
        })
        result = analyze_manifest.analyze(tmp_path)
        assert result["file_counts"]["total_files"] == 0
        assert result["file_counts"]["by_extension"] == {}

    def test_disk_usage(self, tmp_path: Path) -> None:
        """Disk usage is computed for the output directory."""
        _write_manifest(tmp_path, {
            "mit": _make_school_entry("completed"),
        })
        # Add some files to simulate real content
        school_dir = tmp_path / "mit"
        school_dir.mkdir(exist_ok=True)
        (school_dir / "catalog.pdf").write_bytes(b"x" * 5000)
        (school_dir / "metadata.json").write_text("{}")

        result = analyze_manifest.analyze(tmp_path)
        # Should be at least as big as the files we created
        assert result["disk_usage"]["total_bytes"] > 5000
        assert "B" in result["disk_usage"]["total_human"] or \
               "KB" in result["disk_usage"]["total_human"] or \
               "MB" in result["disk_usage"]["total_human"]

    def test_output_dir_in_result(self, tmp_path: Path) -> None:
        """The output_dir path is included in the result."""
        result = analyze_manifest.analyze(tmp_path)
        assert result["output_dir"] == str(tmp_path)


# ---------------------------------------------------------------------------
# print_human_readable
# ---------------------------------------------------------------------------


class TestPrintHumanReadable:
    """Test human-readable output formatting."""

    def test_prints_output(self, tmp_path: Path, capsys) -> None:
        """Human-readable output contains key sections."""
        _write_manifest(tmp_path, {
            "mit": _make_school_entry("completed", "MIT"),
            "stanford": _make_school_entry("failed", "Stanford"),
        })
        _write_metadata(tmp_path, "mit", _make_metadata(
            phases={"discovery": {"status": "completed"}},
        ))
        _write_metadata(tmp_path, "stanford", _make_metadata(
            phases={"discovery": {"status": "failed"}},
            errors=[
                {"phase": "discovery", "error": "Timeout", "timestamp": "..."},
            ],
        ))

        result = analyze_manifest.analyze(tmp_path)
        analyze_manifest.print_human_readable(result)

        captured = capsys.readouterr().out
        assert "Total schools: 2" in captured
        assert "completed" in captured
        assert "failed" in captured
        assert "discovery" in captured
        assert "Timeout" in captured
        assert "Disk Usage:" in captured

    def test_empty_manifest_output(self, tmp_path: Path, capsys) -> None:
        """Human-readable output handles empty data gracefully."""
        result = analyze_manifest.analyze(tmp_path)
        analyze_manifest.print_human_readable(result)

        captured = capsys.readouterr().out
        assert "Total schools: 0" in captured
        assert "(no schools)" in captured
        assert "(no phase data)" in captured

    def test_schools_with_errors_listed(self, tmp_path: Path, capsys) -> None:
        """Schools with errors are listed in the output."""
        _write_manifest(tmp_path, {
            "mit": _make_school_entry("failed"),
        })
        _write_metadata(tmp_path, "mit", _make_metadata(
            errors=[
                {"phase": "catalog", "error": "HTTP 500", "timestamp": "..."},
            ],
        ))

        result = analyze_manifest.analyze(tmp_path)
        analyze_manifest.print_human_readable(result)

        captured = capsys.readouterr().out
        assert "Schools with errors (1):" in captured
        assert "- mit" in captured


# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------


class TestJsonOutput:
    """Test JSON output mode."""

    def test_json_is_valid(self, tmp_path: Path) -> None:
        """JSON output is valid JSON."""
        _write_manifest(tmp_path, {
            "mit": _make_school_entry("completed"),
        })
        _write_metadata(tmp_path, "mit", _make_metadata(
            phases={"discovery": {"status": "completed"}},
        ))

        result = analyze_manifest.analyze(tmp_path)
        json_str = json.dumps(result, indent=2)
        parsed = json.loads(json_str)
        assert parsed["total_schools"] == 1

    def test_json_matches_human_readable_semantically(
        self, tmp_path: Path
    ) -> None:
        """JSON output contains the same data as human-readable output."""
        _write_manifest(tmp_path, {
            "mit": _make_school_entry("completed"),
            "stanford": _make_school_entry("failed"),
        })
        _write_metadata(tmp_path, "mit", _make_metadata(
            phases={
                "robots": {"status": "completed"},
                "discovery": {"status": "completed"},
            },
        ))
        _write_metadata(tmp_path, "stanford", _make_metadata(
            phases={
                "robots": {"status": "completed"},
                "discovery": {"status": "failed"},
            },
            errors=[
                {"phase": "discovery", "error": "Timeout", "timestamp": "..."},
            ],
        ))

        result = analyze_manifest.analyze(tmp_path)

        # Verify all expected keys are present
        assert "output_dir" in result
        assert "total_schools" in result
        assert "school_statuses" in result
        assert "phase_completion" in result
        assert "error_summary" in result
        assert "disk_usage" in result

        # Verify data integrity
        assert result["total_schools"] == 2
        assert result["school_statuses"]["completed"] == 1
        assert result["school_statuses"]["failed"] == 1
        assert result["error_summary"]["total_errors"] == 1
        assert result["error_summary"]["schools_with_errors"] == ["stanford"]


# ---------------------------------------------------------------------------
# CLI (main)
# ---------------------------------------------------------------------------


class TestMain:
    """Test CLI argument parsing and execution."""

    def test_default_args_missing_dir(self) -> None:
        """Exit with error if default output dir doesn't exist."""
        with patch("sys.argv", ["analyze_manifest.py", "--output-dir", "/nonexistent"]):
            with pytest.raises(SystemExit) as exc_info:
                analyze_manifest.main()
            assert exc_info.value.code == 1

    def test_human_readable_output(self, tmp_path: Path, capsys) -> None:
        """Default mode prints human-readable output."""
        _write_manifest(tmp_path, {
            "mit": _make_school_entry("completed"),
        })
        with patch("sys.argv", [
            "analyze_manifest.py", "--output-dir", str(tmp_path)
        ]):
            analyze_manifest.main()

        captured = capsys.readouterr().out
        assert "Total schools: 1" in captured

    def test_json_output(self, tmp_path: Path, capsys) -> None:
        """--json flag produces valid JSON output."""
        _write_manifest(tmp_path, {
            "mit": _make_school_entry("completed"),
        })
        with patch("sys.argv", [
            "analyze_manifest.py", "--output-dir", str(tmp_path), "--json"
        ]):
            analyze_manifest.main()

        captured = capsys.readouterr().out
        parsed = json.loads(captured)
        assert parsed["total_schools"] == 1


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Test edge cases and unusual inputs."""

    def test_unknown_status_in_manifest(self, tmp_path: Path) -> None:
        """An unexpected status value is still counted."""
        _write_manifest(tmp_path, {
            "mit": _make_school_entry("some_new_status"),
        })
        result = analyze_manifest.analyze(tmp_path)
        assert result["school_statuses"]["some_new_status"] == 1

    def test_empty_phases_in_metadata(self, tmp_path: Path) -> None:
        """Metadata with empty phases dict is handled."""
        _write_manifest(tmp_path, {
            "mit": _make_school_entry("pending"),
        })
        _write_metadata(tmp_path, "mit", _make_metadata(phases={}))
        result = analyze_manifest.analyze(tmp_path)
        assert result["phase_completion"] == {}

    def test_missing_status_key_in_school(self, tmp_path: Path) -> None:
        """A school entry missing the status key defaults to 'unknown'."""
        _write_manifest(tmp_path, {
            "broken": {
                "data": {"name": "Broken School"},
                "created_at": "...",
                "updated_at": "...",
            },
        })
        result = analyze_manifest.analyze(tmp_path)
        assert result["school_statuses"]["unknown"] == 1

    def test_metadata_with_no_errors_key(self, tmp_path: Path) -> None:
        """Metadata missing the errors key doesn't crash."""
        _write_manifest(tmp_path, {
            "mit": _make_school_entry("completed"),
        })
        _write_metadata(tmp_path, "mit", {
            "phases": {"discovery": {"status": "completed"}},
            "downloaded_urls": {},
        })
        result = analyze_manifest.analyze(tmp_path)
        assert result["error_summary"]["total_errors"] == 0

    def test_many_schools(self, tmp_path: Path) -> None:
        """Analysis handles a larger number of schools."""
        schools = {}
        for i in range(50):
            slug = f"school-{i:03d}"
            status = "completed" if i % 3 == 0 else (
                "failed" if i % 3 == 1 else "pending"
            )
            schools[slug] = _make_school_entry(status, f"School {i}")

        _write_manifest(tmp_path, schools)

        # Add metadata for completed schools
        for i in range(0, 50, 3):
            slug = f"school-{i:03d}"
            _write_metadata(tmp_path, slug, _make_metadata(
                phases={
                    "robots": {"status": "completed"},
                    "discovery": {"status": "completed"},
                },
            ))

        result = analyze_manifest.analyze(tmp_path)
        assert result["total_schools"] == 50
        assert result["school_statuses"]["completed"] == 17
        assert result["school_statuses"]["failed"] == 17
        assert result["school_statuses"]["pending"] == 16

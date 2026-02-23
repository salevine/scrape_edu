#!/usr/bin/env python3
"""Analyze scrape_edu manifest and per-school metadata.

Reads the manifest.json and per-school metadata.json files from the
output directory and prints a comprehensive analysis of scrape progress.

Usage:
    python scripts/analyze_manifest.py
    python scripts/analyze_manifest.py --output-dir ./output
    python scripts/analyze_manifest.py --json
    python scripts/analyze_manifest.py --json --output-dir /data/scrape_output
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any


def load_manifest(output_dir: Path) -> dict:
    """Load the manifest.json file from the output directory.

    Args:
        output_dir: Path to the output directory.

    Returns:
        Parsed manifest dict, or an empty structure if not found.
    """
    manifest_path = output_dir / "manifest.json"
    if not manifest_path.exists():
        return {"schools": {}}
    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        print(f"WARNING: Could not read manifest: {exc}", file=sys.stderr)
        return {"schools": {}}


def load_school_metadata(output_dir: Path, slug: str) -> dict | None:
    """Load a per-school metadata.json file.

    Args:
        output_dir: Path to the output directory.
        slug: School slug (directory name).

    Returns:
        Parsed metadata dict, or None if not found/unreadable.
    """
    metadata_path = output_dir / slug / "metadata.json"
    if not metadata_path.exists():
        return None
    try:
        with open(metadata_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def get_dir_size(path: Path) -> int:
    """Compute total size in bytes of all files under a directory.

    Args:
        path: Directory to measure.

    Returns:
        Total size in bytes.
    """
    total = 0
    if not path.exists():
        return 0
    for dirpath, _dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            try:
                total += os.path.getsize(filepath)
            except OSError:
                pass
    return total


def format_bytes(size_bytes: int) -> str:
    """Format a byte count as a human-readable string.

    Args:
        size_bytes: Number of bytes.

    Returns:
        Human-readable string (e.g. "1.5 MB", "320 KB").
    """
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def analyze(output_dir: Path) -> dict:
    """Run full analysis on the output directory.

    Reads manifest.json and all per-school metadata.json files,
    then computes summary statistics.

    Args:
        output_dir: Path to the output directory.

    Returns:
        Analysis result dict with keys:
        - output_dir: str path
        - total_schools: int
        - school_statuses: dict mapping status -> count
        - phase_completion: dict mapping phase -> {completed, failed, ...}
        - error_summary: {total_errors, top_errors, schools_with_errors}
        - disk_usage: {total_bytes, total_human}
    """
    manifest = load_manifest(output_dir)
    schools = manifest.get("schools", {})

    # --- School status breakdown ---
    status_counter: Counter[str] = Counter()
    for info in schools.values():
        status_counter[info.get("status", "unknown")] += 1

    # --- Per-school metadata analysis ---
    phase_status_counters: dict[str, Counter[str]] = {}
    all_errors: list[dict] = []
    error_counter: Counter[str] = Counter()
    schools_with_errors: list[str] = []

    for slug in schools:
        metadata = load_school_metadata(output_dir, slug)
        if metadata is None:
            continue

        # Phase completion
        phases = metadata.get("phases", {})
        for phase_name, phase_info in phases.items():
            if phase_name not in phase_status_counters:
                phase_status_counters[phase_name] = Counter()
            phase_status = phase_info.get("status", "unknown")
            phase_status_counters[phase_name][phase_status] += 1

        # Errors
        errors = metadata.get("errors", [])
        if errors:
            schools_with_errors.append(slug)
            for err in errors:
                error_msg = err.get("error", "unknown error")
                all_errors.append(err)
                error_counter[error_msg] += 1

    # Build phase completion summary
    phase_completion: dict[str, dict[str, int]] = {}
    for phase_name, counter in sorted(phase_status_counters.items()):
        phase_completion[phase_name] = dict(counter.most_common())

    # Top errors (up to 5)
    top_errors = [
        {"error": msg, "count": count}
        for msg, count in error_counter.most_common(5)
    ]

    # --- File counts ---
    file_counts: Counter[str] = Counter()
    total_files = 0
    for slug in schools:
        school_dir = output_dir / slug
        if not school_dir.is_dir():
            continue
        for dirpath, _dirnames, filenames in os.walk(school_dir):
            for filename in filenames:
                total_files += 1
                ext = os.path.splitext(filename)[1].lower()
                if ext:
                    file_counts[ext] += 1
                else:
                    file_counts["(no ext)"] += 1

    # --- Per-school syllabi results ---
    syllabi_results: list[dict] = []
    for slug in sorted(schools):
        metadata = load_school_metadata(output_dir, slug)
        if metadata is None:
            continue
        syllabi_phase = metadata.get("phases", {}).get("syllabi")
        if syllabi_phase is None:
            continue
        entry: dict[str, Any] = {"slug": slug}
        entry["seed_urls_count"] = syllabi_phase.get("seed_urls_count", 0)
        entry["seed_files_count"] = syllabi_phase.get("seed_files_count", 0)
        entry["files_downloaded"] = syllabi_phase.get("files_downloaded", 0)
        entry["files_failed"] = syllabi_phase.get("files_failed", 0)
        entry["files_found_by_following"] = syllabi_phase.get(
            "files_found_by_following", 0
        )
        entry["pages_followed"] = syllabi_phase.get("pages_followed", 0)
        entry["course_links_found"] = syllabi_phase.get(
            "course_links_found", 0
        )
        # Flag failure modes
        flags: list[str] = []
        if (
            entry["seed_urls_count"] > 0
            and entry["files_downloaded"] == 0
        ):
            flags.append("seed_urls>0 but 0 files downloaded")
        if entry["seed_urls_count"] == 0:
            flags.append("no seed URLs from discovery")
        entry["flags"] = flags
        syllabi_results.append(entry)

    # --- Disk usage ---
    total_bytes = get_dir_size(output_dir)

    return {
        "output_dir": str(output_dir),
        "total_schools": len(schools),
        "school_statuses": dict(status_counter.most_common()),
        "phase_completion": phase_completion,
        "error_summary": {
            "total_errors": len(all_errors),
            "top_errors": top_errors,
            "schools_with_errors": sorted(schools_with_errors),
        },
        "file_counts": {
            "total_files": total_files,
            "by_extension": dict(file_counts.most_common()),
        },
        "syllabi_results": syllabi_results,
        "disk_usage": {
            "total_bytes": total_bytes,
            "total_human": format_bytes(total_bytes),
        },
    }


def print_human_readable(result: dict) -> None:
    """Print analysis results in a human-readable format.

    Args:
        result: Analysis dict from :func:`analyze`.
    """
    print(f"Scrape Analysis: {result['output_dir']}")
    print("=" * 60)

    # School status breakdown
    print(f"\nTotal schools: {result['total_schools']}")
    print("\nSchool Status Breakdown:")
    if result["school_statuses"]:
        for status, count in result["school_statuses"].items():
            print(f"  {status:<20s} {count:>5d}")
    else:
        print("  (no schools)")

    # Phase completion
    print("\nPhase Completion:")
    if result["phase_completion"]:
        for phase, statuses in result["phase_completion"].items():
            parts = ", ".join(f"{s}: {c}" for s, c in statuses.items())
            print(f"  {phase:<20s} {parts}")
    else:
        print("  (no phase data)")

    # Error summary
    err = result["error_summary"]
    print(f"\nErrors: {err['total_errors']} total")
    if err["top_errors"]:
        print("  Top errors:")
        for entry in err["top_errors"]:
            print(f"    [{entry['count']}x] {entry['error']}")
    if err["schools_with_errors"]:
        print(f"  Schools with errors ({len(err['schools_with_errors'])}):")
        for slug in err["schools_with_errors"]:
            print(f"    - {slug}")

    # File counts
    fc = result["file_counts"]
    print(f"\nFiles: {fc['total_files']} total")
    if fc["by_extension"]:
        for ext, count in fc["by_extension"].items():
            print(f"  {ext:<10s} {count:>5d}")

    # Per-school syllabi results
    syllabi = result.get("syllabi_results", [])
    if syllabi:
        print("\nSyllabus Results by School:")
        for entry in syllabi:
            slug = entry["slug"]
            downloaded = entry["files_downloaded"]
            seed = entry["seed_urls_count"]
            followed = entry["files_found_by_following"]
            flags = entry.get("flags", [])
            flag_str = ""
            if flags:
                flag_str = f"  [!] {'; '.join(flags)}"
            print(
                f"  {slug:<40s} {downloaded:>3d} syllabi  "
                f"({seed} seed, {followed} followed){flag_str}"
            )

    # Disk usage
    disk = result["disk_usage"]
    print(f"\nDisk Usage: {disk['total_human']} ({disk['total_bytes']} bytes)")


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Analyze scrape_edu manifest and per-school metadata",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./output"),
        help="Output directory containing manifest.json (default: ./output)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Output as JSON for programmatic use",
    )
    args = parser.parse_args()

    if not args.output_dir.exists():
        print(
            f"ERROR: Output directory does not exist: {args.output_dir}",
            file=sys.stderr,
        )
        sys.exit(1)

    result = analyze(args.output_dir)

    if args.json_output:
        print(json.dumps(result, indent=2))
    else:
        print_human_readable(result)


if __name__ == "__main__":
    main()

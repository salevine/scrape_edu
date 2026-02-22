#!/usr/bin/env python3
"""Download IPEDS data files from NCES.

Downloads the Institutional Characteristics (HD) and Completions (C)
CSV files needed to build the university list.

Usage:
    python scripts/download_ipeds.py
    python scripts/download_ipeds.py --year 2022
    python scripts/download_ipeds.py --output-dir ./data/ipeds
"""

from __future__ import annotations

import argparse
import io
import sys
import zipfile
from pathlib import Path

import requests


# NCES IPEDS data-generator endpoint (returns zip files containing CSVs).
IPEDS_BASE_URL = "https://nces.ed.gov/ipeds/data-generator"

FILES = {
    "hd": {
        "description": "Institutional Characteristics",
        "tableName": "HD{year}",
        "csv_pattern": "hd{year}.csv",
    },
    "c": {
        "description": "Completions",
        "tableName": "C{year}_A",
        "csv_pattern": "c{year}_a.csv",
    },
}

# Years to attempt, in order of preference.  NCES sometimes lags a year
# behind, so we try several.
FALLBACK_YEARS = (2024, 2023, 2022)

# Timeout for HTTP requests (seconds).
REQUEST_TIMEOUT = 120


def download_file(url: str, description: str) -> bytes:
    """Download a file with progress reporting.

    Streams the response and prints progress to stderr so that the user
    can see what is happening on large downloads.

    Args:
        url: Full URL to download.
        description: Human-readable label shown in progress output.

    Returns:
        The raw bytes of the downloaded file.

    Raises:
        requests.HTTPError: If the server returns a non-2xx status.
        requests.ConnectionError: If the server is unreachable.
    """
    print(f"  Downloading {description}...", file=sys.stderr)
    print(f"  URL: {url}", file=sys.stderr)

    response = requests.get(url, stream=True, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    total = response.headers.get("Content-Length")
    total_bytes = int(total) if total else None

    if total_bytes is not None:
        total_mb = total_bytes / (1024 * 1024)
        print(f"  Size: {total_mb:.1f} MB", file=sys.stderr)
    else:
        print("  Size: unknown", file=sys.stderr)

    chunks: list[bytes] = []
    downloaded = 0

    for chunk in response.iter_content(chunk_size=64 * 1024):
        chunks.append(chunk)
        downloaded += len(chunk)

        if total_bytes:
            pct = downloaded / total_bytes * 100
            downloaded_mb = downloaded / (1024 * 1024)
            print(
                f"\r  Progress: {downloaded_mb:.1f}/{total_mb:.1f} MB ({pct:.0f}%)",
                end="",
                file=sys.stderr,
            )
        else:
            downloaded_mb = downloaded / (1024 * 1024)
            print(
                f"\r  Downloaded: {downloaded_mb:.1f} MB",
                end="",
                file=sys.stderr,
            )

    # End the progress line
    print(file=sys.stderr)

    return b"".join(chunks)


def extract_csv_from_zip(zip_bytes: bytes, csv_pattern: str) -> bytes:
    """Extract the CSV file from a zip archive.

    The CSV filename inside the zip may have varying case (e.g.
    ``hd2023.csv`` or ``HD2023.csv``), so the match is
    case-insensitive.  If the zip contains the file in a subdirectory
    it will still be found.

    Args:
        zip_bytes: Raw bytes of the zip archive.
        csv_pattern: Expected CSV filename (case-insensitive match).

    Returns:
        The raw bytes of the extracted CSV.

    Raises:
        FileNotFoundError: If no matching CSV is found in the archive.
        zipfile.BadZipFile: If the data is not a valid zip.
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        # Build a lookup of lowered name -> actual name
        names = zf.namelist()
        lower_map: dict[str, str] = {}
        for name in names:
            # Use only the basename for matching (handles subdirectories)
            basename = name.rsplit("/", 1)[-1] if "/" in name else name
            lower_map[basename.lower()] = name

        target = csv_pattern.lower()
        if target in lower_map:
            actual = lower_map[target]
            print(f"  Extracting: {actual}", file=sys.stderr)
            return zf.read(actual)

        # If exact pattern didn't match, list archive contents for debugging
        print(
            f"  WARNING: Expected CSV '{csv_pattern}' not found in archive.",
            file=sys.stderr,
        )
        print(f"  Archive contents: {names}", file=sys.stderr)

        # As a last resort, try to find any CSV file in the archive
        csv_files = [n for n in names if n.lower().endswith(".csv")]
        if csv_files:
            fallback = csv_files[0]
            print(
                f"  Falling back to first CSV found: {fallback}",
                file=sys.stderr,
            )
            return zf.read(fallback)

        raise FileNotFoundError(
            f"No CSV file matching '{csv_pattern}' found in zip archive. "
            f"Archive contains: {names}"
        )


def _try_download_file(
    file_key: str,
    file_info: dict[str, str],
    year: int,
) -> bytes | None:
    """Attempt to download and extract a single IPEDS file for one year.

    Returns the CSV bytes on success, or ``None`` if the download fails
    (e.g. 404).
    """
    table_name = file_info["tableName"].format(year=year)
    csv_name = file_info["csv_pattern"].format(year=year)
    url = f"{IPEDS_BASE_URL}?year={year}&tableName={table_name}&HasRV=0&type=csv"
    description = f"{file_info['description']} ({table_name})"

    try:
        zip_bytes = download_file(url, description)
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        print(
            f"  HTTP {status} for {url} -- will try another year.",
            file=sys.stderr,
        )
        return None
    except requests.ConnectionError as exc:
        print(
            f"  Connection error for {url}: {exc} -- will try another year.",
            file=sys.stderr,
        )
        return None

    return extract_csv_from_zip(zip_bytes, csv_name)


def download_ipeds(year: int, output_dir: Path) -> None:
    """Download all required IPEDS files.

    Tries the requested *year* first.  If that fails (e.g. a 404 because
    the data has not been published yet), falls back through
    :data:`FALLBACK_YEARS`.

    Args:
        year: Preferred survey year (e.g. 2023).
        output_dir: Directory to write extracted CSV files into.

    Raises:
        SystemExit: If all download attempts fail for any file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Build the ordered list of years to try.
    years_to_try = [year] + [y for y in FALLBACK_YEARS if y != year]

    for file_key, file_info in FILES.items():
        print(
            f"\n{'='*60}\n"
            f"File: {file_info['description']} ({file_key.upper()})\n"
            f"{'='*60}",
            file=sys.stderr,
        )

        csv_bytes: bytes | None = None
        used_year: int | None = None

        for try_year in years_to_try:
            print(f"\n  Trying year {try_year}...", file=sys.stderr)
            csv_bytes = _try_download_file(file_key, file_info, try_year)
            if csv_bytes is not None:
                used_year = try_year
                break

        if csv_bytes is None:
            print(
                f"\nERROR: Could not download {file_info['description']} "
                f"for any year ({', '.join(str(y) for y in years_to_try)}).\n"
                f"Check https://nces.ed.gov/ipeds/use-the-data for current "
                f"file URLs and try again.",
                file=sys.stderr,
            )
            sys.exit(1)

        # Write the CSV
        csv_name = file_info["csv_pattern"].format(year=used_year)
        out_path = output_dir / csv_name
        out_path.write_bytes(csv_bytes)

        size_mb = len(csv_bytes) / (1024 * 1024)
        print(
            f"  Saved: {out_path}  ({size_mb:.1f} MB)",
            file=sys.stderr,
        )

    print(
        f"\nAll IPEDS files downloaded to {output_dir}/",
        file=sys.stderr,
    )


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Download IPEDS data files from NCES",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2024,
        help="IPEDS survey year (default: 2024)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/ipeds"),
        help="Output directory (default: data/ipeds)",
    )
    args = parser.parse_args()

    print(
        f"IPEDS Downloader\n"
        f"  Requested year: {args.year}\n"
        f"  Output dir:     {args.output_dir}\n",
        file=sys.stderr,
    )

    download_ipeds(args.year, args.output_dir)


if __name__ == "__main__":
    main()

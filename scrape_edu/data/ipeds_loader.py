"""Load and filter IPEDS CSV data to find universities with CS/DS programs."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from scrape_edu.data.school import School

logger = logging.getLogger(__name__)

# CIP code prefixes that indicate Computer Science or Data Science programs
_CS_CIP_PREFIX = "11."
_DS_CIP_CODE = "30.7001"


def _find_csv(ipeds_dir: Path, pattern: str, description: str) -> Path:
    """Find a single CSV matching *pattern* (case-insensitive) in *ipeds_dir*.

    Raises:
        FileNotFoundError: If no matching file is found.
    """
    # Glob is case-sensitive on most filesystems, so we try both the
    # pattern as-is and an uppercased version to handle common IPEDS naming.
    matches: list[Path] = []
    for p in ipeds_dir.iterdir():
        if p.suffix.lower() == ".csv" and _fnmatch_ci(p.name, pattern):
            matches.append(p)

    if not matches:
        raise FileNotFoundError(
            f"Could not find {description} CSV in {ipeds_dir}. "
            f"Expected a file matching pattern '{pattern}' (case-insensitive). "
            f"Download IPEDS data from https://nces.ed.gov/ipeds/use-the-data"
        )

    # If multiple matches, prefer the most recently modified one
    matches.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    chosen = matches[0]
    if len(matches) > 1:
        logger.warning(
            "Multiple files match '%s': %s. Using %s",
            pattern,
            [m.name for m in matches],
            chosen.name,
        )
    return chosen


def _fnmatch_ci(filename: str, pattern: str) -> bool:
    """Case-insensitive fnmatch-style glob matching.

    Supports ``*`` as a wildcard for any sequence of characters.
    """
    import fnmatch

    return fnmatch.fnmatch(filename.lower(), pattern.lower())


def load_schools(
    ipeds_dir: Path,
    config: dict | None = None,
) -> list[School]:
    """Load IPEDS CSVs and return a list of schools with CS/DS programs.

    Looks for two CSV files in *ipeds_dir*:
    - **HD file** (institutional characteristics): filename matching ``hd*.csv``
    - **C file** (completions): filename matching ``c*_a.csv``

    Filters:
    - Four-year institutions only (``ICLEVEL == 1``).
    - Schools that have completions in CIP codes starting with ``11.``
      (Computer Science) or equal to ``30.7001`` (Data Science).

    Args:
        ipeds_dir: Directory containing the IPEDS CSV downloads.
        config: Optional configuration dict (reserved for future use).

    Returns:
        A list of :class:`School` objects sorted by institution name.

    Raises:
        FileNotFoundError: If required CSV files are not found.
    """
    ipeds_dir = Path(ipeds_dir)
    if not ipeds_dir.is_dir():
        raise FileNotFoundError(
            f"IPEDS directory does not exist: {ipeds_dir}. "
            f"Create it and download IPEDS data from "
            f"https://nces.ed.gov/ipeds/use-the-data"
        )

    hd_path = _find_csv(ipeds_dir, "hd*.csv", "institutional characteristics (HD)")
    c_path = _find_csv(ipeds_dir, "c*_a.csv", "completions (C)")

    logger.info("Loading HD file: %s", hd_path.name)
    logger.info("Loading C file: %s", c_path.name)

    # --- Load institutional characteristics ---
    hd_cols = ["UNITID", "INSTNM", "WEBADDR", "CITY", "STABBR", "ICLEVEL"]
    hd_df = pd.read_csv(
        hd_path,
        usecols=lambda c: c.upper() in {col.upper() for col in hd_cols},
        encoding="latin-1",
        dtype=str,
        low_memory=False,
    )
    # Normalize column names to uppercase
    hd_df.columns = hd_df.columns.str.upper()

    # Filter to 4-year institutions (ICLEVEL == 1)
    hd_df["ICLEVEL"] = pd.to_numeric(hd_df["ICLEVEL"], errors="coerce")
    hd_df = hd_df[hd_df["ICLEVEL"] == 1].copy()
    logger.info("Four-year institutions: %d", len(hd_df))

    # --- Load completions ---
    c_cols = ["UNITID", "CIPCODE", "AWLEVEL"]
    c_df = pd.read_csv(
        c_path,
        usecols=lambda c: c.upper() in {col.upper() for col in c_cols},
        encoding="latin-1",
        dtype=str,
        low_memory=False,
    )
    c_df.columns = c_df.columns.str.upper()

    # Filter to CS/DS CIP codes
    cs_mask = c_df["CIPCODE"].str.startswith(_CS_CIP_PREFIX)
    ds_mask = c_df["CIPCODE"] == _DS_CIP_CODE
    c_filtered = c_df[cs_mask | ds_mask].copy()
    logger.info("Completions rows matching CS/DS: %d", len(c_filtered))

    # Get unique UNITIDs with CS/DS programs
    cs_ds_unitids = set(c_filtered["UNITID"].unique())

    # --- Join: keep only 4-year institutions with CS/DS completions ---
    hd_df = hd_df[hd_df["UNITID"].isin(cs_ds_unitids)].copy()
    logger.info("Schools with CS/DS programs (4-year): %d", len(hd_df))

    # --- Build School objects ---
    schools: list[School] = []
    for _, row in hd_df.iterrows():
        url = str(row.get("WEBADDR", "") or "")
        # Ensure URL has a scheme
        if url and not url.startswith(("http://", "https://")):
            url = "http://" + url

        try:
            school = School(
                unitid=int(row["UNITID"]),
                name=str(row.get("INSTNM", "") or ""),
                url=url,
                city=str(row.get("CITY", "") or ""),
                state=str(row.get("STABBR", "") or ""),
            )
            schools.append(school)
        except (ValueError, TypeError) as exc:
            logger.warning(
                "Skipping row with UNITID=%s: %s", row.get("UNITID"), exc
            )

    # Sort by name for deterministic ordering
    schools.sort(key=lambda s: s.name.lower())
    logger.info("Loaded %d schools", len(schools))
    return schools

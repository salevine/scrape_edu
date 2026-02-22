"""Tests for scrape_edu.data.ipeds_loader module."""

from __future__ import annotations

from pathlib import Path

import pytest

from scrape_edu.data.ipeds_loader import load_schools
from scrape_edu.data.school import School


# ======================================================================
# Helpers for creating test CSV fixtures
# ======================================================================


def _write_hd_csv(path: Path, rows: list[dict]) -> None:
    """Write a minimal HD (institutional characteristics) CSV."""
    cols = ["UNITID", "INSTNM", "WEBADDR", "CITY", "STABBR", "ICLEVEL"]
    lines = [",".join(cols)]
    for row in rows:
        lines.append(",".join(str(row.get(c, "")) for c in cols))
    path.write_text("\n".join(lines) + "\n", encoding="latin-1")


def _write_c_csv(path: Path, rows: list[dict]) -> None:
    """Write a minimal C (completions) CSV."""
    cols = ["UNITID", "CIPCODE", "AWLEVEL"]
    lines = [",".join(cols)]
    for row in rows:
        lines.append(",".join(str(row.get(c, "")) for c in cols))
    path.write_text("\n".join(lines) + "\n", encoding="latin-1")


@pytest.fixture()
def ipeds_dir(tmp_path: Path) -> Path:
    """Create a temporary IPEDS directory with sample data."""
    d = tmp_path / "ipeds"
    d.mkdir()
    return d


@pytest.fixture()
def sample_ipeds(ipeds_dir: Path) -> Path:
    """Create sample IPEDS CSVs with a mix of schools."""
    # HD file: 4 institutions of varying levels
    _write_hd_csv(
        ipeds_dir / "hd2023.csv",
        [
            {
                "UNITID": "100000",
                "INSTNM": "Alpha University",
                "WEBADDR": "www.alpha.edu",
                "CITY": "Springfield",
                "STABBR": "IL",
                "ICLEVEL": "1",  # 4-year
            },
            {
                "UNITID": "200000",
                "INSTNM": "Beta College",
                "WEBADDR": "https://www.beta.edu",
                "CITY": "Portland",
                "STABBR": "OR",
                "ICLEVEL": "1",  # 4-year
            },
            {
                "UNITID": "300000",
                "INSTNM": "Gamma Community College",
                "WEBADDR": "www.gamma.edu",
                "CITY": "Austin",
                "STABBR": "TX",
                "ICLEVEL": "2",  # 2-year (should be filtered out)
            },
            {
                "UNITID": "400000",
                "INSTNM": "Delta Technical Institute",
                "WEBADDR": "www.delta.edu",
                "CITY": "Miami",
                "STABBR": "FL",
                "ICLEVEL": "1",  # 4-year, but no CS/DS programs
            },
        ],
    )

    # C file: completions
    _write_c_csv(
        ipeds_dir / "c2023_a.csv",
        [
            # Alpha: has CS program
            {"UNITID": "100000", "CIPCODE": "11.0101", "AWLEVEL": "5"},
            {"UNITID": "100000", "CIPCODE": "11.0701", "AWLEVEL": "7"},
            # Beta: has DS program
            {"UNITID": "200000", "CIPCODE": "30.7001", "AWLEVEL": "5"},
            # Gamma: has CS but is 2-year
            {"UNITID": "300000", "CIPCODE": "11.0101", "AWLEVEL": "3"},
            # Delta: has business, no CS/DS
            {"UNITID": "400000", "CIPCODE": "52.0201", "AWLEVEL": "5"},
            # Some other CIP codes for noise
            {"UNITID": "100000", "CIPCODE": "26.0101", "AWLEVEL": "5"},
        ],
    )

    return ipeds_dir


# ======================================================================
# Tests
# ======================================================================


class TestLoadSchoolsBasic:
    """Test core loading and filtering logic."""

    def test_returns_list_of_schools(self, sample_ipeds: Path) -> None:
        schools = load_schools(sample_ipeds)
        assert isinstance(schools, list)
        assert all(isinstance(s, School) for s in schools)

    def test_filters_to_four_year_institutions(
        self, sample_ipeds: Path
    ) -> None:
        schools = load_schools(sample_ipeds)
        # Gamma is 2-year, should be excluded
        names = [s.name for s in schools]
        assert "Gamma Community College" not in names

    def test_filters_to_cs_ds_cip_codes(self, sample_ipeds: Path) -> None:
        schools = load_schools(sample_ipeds)
        # Delta has no CS/DS programs, should be excluded
        names = [s.name for s in schools]
        assert "Delta Technical Institute" not in names

    def test_includes_cs_school(self, sample_ipeds: Path) -> None:
        schools = load_schools(sample_ipeds)
        names = [s.name for s in schools]
        assert "Alpha University" in names

    def test_includes_ds_school(self, sample_ipeds: Path) -> None:
        schools = load_schools(sample_ipeds)
        names = [s.name for s in schools]
        assert "Beta College" in names

    def test_correct_number_of_schools(self, sample_ipeds: Path) -> None:
        schools = load_schools(sample_ipeds)
        assert len(schools) == 2  # Only Alpha and Beta

    def test_sorted_by_name(self, sample_ipeds: Path) -> None:
        schools = load_schools(sample_ipeds)
        names = [s.name for s in schools]
        assert names == sorted(names, key=str.lower)


class TestLoadSchoolsFields:
    """Test that School objects have correct field values."""

    def test_unitid(self, sample_ipeds: Path) -> None:
        schools = load_schools(sample_ipeds)
        alpha = next(s for s in schools if s.name == "Alpha University")
        assert alpha.unitid == 100000

    def test_name(self, sample_ipeds: Path) -> None:
        schools = load_schools(sample_ipeds)
        alpha = next(s for s in schools if s.name == "Alpha University")
        assert alpha.name == "Alpha University"

    def test_slug_generated(self, sample_ipeds: Path) -> None:
        schools = load_schools(sample_ipeds)
        alpha = next(s for s in schools if s.name == "Alpha University")
        assert alpha.slug == "alpha-university"

    def test_url_scheme_added(self, sample_ipeds: Path) -> None:
        schools = load_schools(sample_ipeds)
        alpha = next(s for s in schools if s.name == "Alpha University")
        # www.alpha.edu should have http:// prepended
        assert alpha.url == "http://www.alpha.edu"

    def test_url_scheme_preserved(self, sample_ipeds: Path) -> None:
        schools = load_schools(sample_ipeds)
        beta = next(s for s in schools if s.name == "Beta College")
        # Already has https://, should not be doubled
        assert beta.url == "https://www.beta.edu"

    def test_city(self, sample_ipeds: Path) -> None:
        schools = load_schools(sample_ipeds)
        alpha = next(s for s in schools if s.name == "Alpha University")
        assert alpha.city == "Springfield"

    def test_state(self, sample_ipeds: Path) -> None:
        schools = load_schools(sample_ipeds)
        alpha = next(s for s in schools if s.name == "Alpha University")
        assert alpha.state == "IL"

    def test_dept_urls_empty_initially(self, sample_ipeds: Path) -> None:
        schools = load_schools(sample_ipeds)
        alpha = next(s for s in schools if s.name == "Alpha University")
        assert alpha.cs_dept_url == ""
        assert alpha.ds_dept_url == ""


class TestLoadSchoolsErrors:
    """Test error handling."""

    def test_missing_directory(self, tmp_path: Path) -> None:
        missing = tmp_path / "does_not_exist"
        with pytest.raises(FileNotFoundError, match="does not exist"):
            load_schools(missing)

    def test_missing_hd_file(self, ipeds_dir: Path) -> None:
        # Only create C file, no HD file
        _write_c_csv(
            ipeds_dir / "c2023_a.csv",
            [{"UNITID": "100000", "CIPCODE": "11.0101", "AWLEVEL": "5"}],
        )
        with pytest.raises(FileNotFoundError, match="institutional characteristics"):
            load_schools(ipeds_dir)

    def test_missing_c_file(self, ipeds_dir: Path) -> None:
        # Only create HD file, no C file
        _write_hd_csv(
            ipeds_dir / "hd2023.csv",
            [
                {
                    "UNITID": "100000",
                    "INSTNM": "Test U",
                    "WEBADDR": "test.edu",
                    "CITY": "X",
                    "STABBR": "XX",
                    "ICLEVEL": "1",
                }
            ],
        )
        with pytest.raises(FileNotFoundError, match="completions"):
            load_schools(ipeds_dir)

    def test_empty_directory(self, ipeds_dir: Path) -> None:
        with pytest.raises(FileNotFoundError):
            load_schools(ipeds_dir)


class TestLoadSchoolsCaseInsensitive:
    """Test case-insensitive file matching."""

    def test_uppercase_filenames(self, ipeds_dir: Path) -> None:
        _write_hd_csv(
            ipeds_dir / "HD2023.csv",
            [
                {
                    "UNITID": "100000",
                    "INSTNM": "Test University",
                    "WEBADDR": "test.edu",
                    "CITY": "TestCity",
                    "STABBR": "TS",
                    "ICLEVEL": "1",
                }
            ],
        )
        _write_c_csv(
            ipeds_dir / "C2023_A.csv",
            [{"UNITID": "100000", "CIPCODE": "11.0101", "AWLEVEL": "5"}],
        )

        schools = load_schools(ipeds_dir)
        assert len(schools) == 1
        assert schools[0].name == "Test University"


class TestLoadSchoolsEdgeCases:
    """Test edge cases in data."""

    def test_school_with_only_ds(self, ipeds_dir: Path) -> None:
        """A school with only Data Science (30.7001) should be included."""
        _write_hd_csv(
            ipeds_dir / "hd2023.csv",
            [
                {
                    "UNITID": "100000",
                    "INSTNM": "DS Only University",
                    "WEBADDR": "ds.edu",
                    "CITY": "DataCity",
                    "STABBR": "DC",
                    "ICLEVEL": "1",
                }
            ],
        )
        _write_c_csv(
            ipeds_dir / "c2023_a.csv",
            [{"UNITID": "100000", "CIPCODE": "30.7001", "AWLEVEL": "7"}],
        )

        schools = load_schools(ipeds_dir)
        assert len(schools) == 1

    def test_school_with_multiple_cs_cips(self, ipeds_dir: Path) -> None:
        """A school with multiple CS CIP codes should appear only once."""
        _write_hd_csv(
            ipeds_dir / "hd2023.csv",
            [
                {
                    "UNITID": "100000",
                    "INSTNM": "Multi CS University",
                    "WEBADDR": "multi.edu",
                    "CITY": "CodeCity",
                    "STABBR": "CC",
                    "ICLEVEL": "1",
                }
            ],
        )
        _write_c_csv(
            ipeds_dir / "c2023_a.csv",
            [
                {"UNITID": "100000", "CIPCODE": "11.0101", "AWLEVEL": "5"},
                {"UNITID": "100000", "CIPCODE": "11.0201", "AWLEVEL": "5"},
                {"UNITID": "100000", "CIPCODE": "11.0701", "AWLEVEL": "7"},
            ],
        )

        schools = load_schools(ipeds_dir)
        assert len(schools) == 1

    def test_config_param_accepted(self, sample_ipeds: Path) -> None:
        """The config parameter should be accepted even if unused."""
        schools = load_schools(sample_ipeds, config={"workers": 5})
        assert len(schools) == 2

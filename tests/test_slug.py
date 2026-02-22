"""Tests for scrape_edu.utils.slug module."""

from __future__ import annotations

import pytest

from scrape_edu.utils.slug import slugify


class TestSlugify:
    """Test the slugify function with various university names."""

    # --- Exact examples from the spec ---

    def test_full_name(self) -> None:
        assert slugify("Massachusetts Institute of Technology") == "massachusetts-institute-of-technology"

    def test_acronym(self) -> None:
        assert slugify("MIT") == "mit"

    def test_hyphenated_name(self) -> None:
        assert slugify("University of California-Berkeley") == "university-of-california-berkeley"

    def test_ampersand(self) -> None:
        assert slugify("Texas A&M University") == "texas-am-university"

    def test_period_and_apostrophe(self) -> None:
        assert slugify("St. John's University") == "st-johns-university"

    # --- Additional edge cases ---

    def test_parentheses(self) -> None:
        assert slugify("University of Illinois (Urbana-Champaign)") == "university-of-illinois-urbana-champaign"

    def test_multiple_spaces(self) -> None:
        assert slugify("Carnegie   Mellon   University") == "carnegie-mellon-university"

    def test_leading_trailing_spaces(self) -> None:
        assert slugify("  Stanford University  ") == "stanford-university"

    def test_comma(self) -> None:
        assert slugify("University of Colorado, Boulder") == "university-of-colorado-boulder"

    def test_slash(self) -> None:
        assert slugify("University of Minnesota-Twin Cities/Duluth") == "university-of-minnesota-twin-cities-duluth"

    def test_empty_string(self) -> None:
        assert slugify("") == ""

    def test_only_special_characters(self) -> None:
        assert slugify("...&&&'''") == ""

    def test_numbers_preserved(self) -> None:
        assert slugify("University 123") == "university-123"

    def test_unicode_characters(self) -> None:
        # e.g. accented characters should be normalized
        assert slugify("Universite de Montreal") == "universite-de-montreal"

    def test_smart_apostrophe(self) -> None:
        """Right single quotation mark (U+2019) used as apostrophe."""
        assert slugify("St. John\u2019s University") == "st-johns-university"

    def test_existing_hyphens_preserved(self) -> None:
        assert slugify("Wake-Forest") == "wake-forest"

    def test_no_trailing_hyphens_from_special_chars(self) -> None:
        result = slugify("MIT.")
        assert not result.endswith("-")
        assert result == "mit"

    def test_at_sign(self) -> None:
        assert slugify("University @ Buffalo") == "university-buffalo"

    def test_colon(self) -> None:
        assert slugify("UC Davis: College of Engineering") == "uc-davis-college-of-engineering"

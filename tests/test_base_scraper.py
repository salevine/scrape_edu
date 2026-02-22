"""Tests for scrape_edu.scrapers.base module."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from scrape_edu.data.manifest import SchoolMetadata
from scrape_edu.data.school import School
from scrape_edu.net.http_client import HttpClient
from scrape_edu.scrapers.base import BaseScraper


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


class DummyScraper(BaseScraper):
    """Concrete subclass for testing the abstract base."""

    def __init__(
        self,
        http_client: HttpClient,
        config: dict[str, Any],
    ) -> None:
        super().__init__(http_client, config)
        self.scrape_called_with: (
            tuple[School, Path, SchoolMetadata] | None
        ) = None

    def scrape(
        self,
        school: School,
        school_dir: Path,
        metadata: SchoolMetadata,
    ) -> None:
        self.scrape_called_with = (school, school_dir, metadata)


@pytest.fixture()
def mock_http_client() -> MagicMock:
    """Return a mocked HttpClient."""
    return MagicMock(spec=HttpClient)


@pytest.fixture()
def dummy_scraper(mock_http_client: MagicMock) -> DummyScraper:
    """Return a DummyScraper instance."""
    return DummyScraper(
        http_client=mock_http_client,
        config={"workers": 5, "delay": 1.0},
    )


@pytest.fixture()
def school() -> School:
    """Return a sample School."""
    return School(unitid=166683, name="MIT", url="https://www.mit.edu")


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------


class TestBaseScraper:
    """Test that BaseScraper cannot be instantiated directly."""

    def test_cannot_instantiate_abstract_class(
        self, mock_http_client: MagicMock
    ) -> None:
        with pytest.raises(TypeError, match="abstract method"):
            BaseScraper(  # type: ignore[abstract]
                http_client=mock_http_client,
                config={},
            )


class TestConcreteSubclass:
    """Test that a concrete subclass works correctly."""

    def test_can_instantiate(self, dummy_scraper: DummyScraper) -> None:
        assert isinstance(dummy_scraper, BaseScraper)

    def test_stores_http_client(
        self, dummy_scraper: DummyScraper, mock_http_client: MagicMock
    ) -> None:
        assert dummy_scraper.client is mock_http_client

    def test_stores_config(self, dummy_scraper: DummyScraper) -> None:
        assert dummy_scraper.config == {"workers": 5, "delay": 1.0}

    def test_scrape_called(
        self,
        dummy_scraper: DummyScraper,
        school: School,
        tmp_path: Path,
    ) -> None:
        school_dir = tmp_path / "mit"
        school_dir.mkdir()
        metadata = SchoolMetadata(school_dir)

        dummy_scraper.scrape(school, school_dir, metadata)

        assert dummy_scraper.scrape_called_with is not None
        called_school, called_dir, called_meta = (
            dummy_scraper.scrape_called_with
        )
        assert called_school is school
        assert called_dir == school_dir
        assert called_meta is metadata


class TestSkipIfDownloaded:
    """Test the _skip_if_downloaded helper."""

    def test_returns_true_when_url_already_downloaded(
        self, dummy_scraper: DummyScraper, tmp_path: Path
    ) -> None:
        school_dir = tmp_path / "mit"
        school_dir.mkdir()
        metadata = SchoolMetadata(school_dir)

        url = "https://example.edu/catalog.pdf"
        metadata.add_downloaded_url(url, "catalog/catalog.pdf")

        assert dummy_scraper._skip_if_downloaded(url, metadata) is True

    def test_returns_false_when_url_not_downloaded(
        self, dummy_scraper: DummyScraper, tmp_path: Path
    ) -> None:
        school_dir = tmp_path / "mit"
        school_dir.mkdir()
        metadata = SchoolMetadata(school_dir)

        url = "https://example.edu/new-page.html"
        assert dummy_scraper._skip_if_downloaded(url, metadata) is False

    def test_different_urls_handled_correctly(
        self, dummy_scraper: DummyScraper, tmp_path: Path
    ) -> None:
        school_dir = tmp_path / "mit"
        school_dir.mkdir()
        metadata = SchoolMetadata(school_dir)

        downloaded_url = "https://example.edu/a.pdf"
        new_url = "https://example.edu/b.pdf"
        metadata.add_downloaded_url(downloaded_url, "a.pdf")

        assert dummy_scraper._skip_if_downloaded(downloaded_url, metadata) is True
        assert dummy_scraper._skip_if_downloaded(new_url, metadata) is False

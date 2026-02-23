"""Tests for scrape_edu.scrapers.catalog_scraper module.

All HttpClient and PageRenderer interactions are mocked -- no real network
or browser activity occurs.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, call

import pytest

from scrape_edu.data.manifest import SchoolMetadata
from scrape_edu.data.school import School
from scrape_edu.net.http_client import HttpClient
from scrape_edu.scrapers.catalog_scraper import CatalogScraper


# ------------------------------------------------------------------
# Helpers / Fixtures
# ------------------------------------------------------------------


@pytest.fixture()
def mock_http_client() -> MagicMock:
    """Return a mocked HttpClient."""
    return MagicMock(spec=HttpClient)


@pytest.fixture()
def mock_renderer() -> MagicMock:
    """Return a mocked PageRenderer (duck-typed)."""
    return MagicMock()


@pytest.fixture()
def school() -> School:
    """Return a sample School."""
    return School(unitid=166683, name="MIT", url="https://www.mit.edu")


@pytest.fixture()
def school_dir(tmp_path: Path) -> Path:
    """Return a temporary school directory."""
    d = tmp_path / "mit"
    d.mkdir()
    return d


@pytest.fixture()
def metadata(school_dir: Path) -> SchoolMetadata:
    """Return a SchoolMetadata instance with catalog URLs pre-populated."""
    meta = SchoolMetadata(school_dir)
    meta._metadata["phases"]["discovery"] = {
        "catalog_urls": [
            "https://example.edu/catalog.pdf",
            "https://example.edu/courses",
        ],
    }
    return meta


def _make_scraper(
    http_client: MagicMock,
    renderer=None,
    config: dict | None = None,
) -> CatalogScraper:
    """Create a CatalogScraper with the given mocks."""
    return CatalogScraper(
        http_client=http_client,
        config=config or {},
        renderer=renderer,
    )


# ------------------------------------------------------------------
# scrape() — PDF download path
# ------------------------------------------------------------------


class TestScrapeDownloadsPdf:
    """scrape() downloads PDF URLs via client.download()."""

    def test_downloads_pdf_url(
        self,
        mock_http_client: MagicMock,
        mock_renderer: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        meta = SchoolMetadata(school_dir)
        meta._metadata["phases"]["discovery"] = {
            "catalog_urls": ["https://example.edu/catalog.pdf"],
        }
        pdf_dest = school_dir / "catalog" / "catalog.pdf"
        mock_http_client.download.return_value = pdf_dest

        scraper = _make_scraper(mock_http_client, mock_renderer)
        scraper.scrape(school, school_dir, meta)

        mock_http_client.download.assert_called_once_with(
            "https://example.edu/catalog.pdf", pdf_dest
        )


# ------------------------------------------------------------------
# scrape() — HTML render path
# ------------------------------------------------------------------


class TestScrapeRendersHtml:
    """scrape() renders HTML URLs via renderer.render_to_pdf()."""

    def test_renders_html_url(
        self,
        mock_http_client: MagicMock,
        mock_renderer: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        meta = SchoolMetadata(school_dir)
        meta._metadata["phases"]["discovery"] = {
            "catalog_urls": ["https://example.edu/courses"],
        }
        expected_dest = school_dir / "catalog" / "courses.pdf"
        mock_renderer.render_to_pdf.return_value = expected_dest

        scraper = _make_scraper(mock_http_client, mock_renderer)
        scraper.scrape(school, school_dir, meta)

        mock_renderer.render_to_pdf.assert_called_once_with(
            "https://example.edu/courses", expected_dest
        )
        mock_http_client.download.assert_not_called()


# ------------------------------------------------------------------
# scrape() — skip already-downloaded
# ------------------------------------------------------------------


class TestScrapeSkipsDownloaded:
    """scrape() skips already-downloaded URLs."""

    def test_skips_already_downloaded_url(
        self,
        mock_http_client: MagicMock,
        mock_renderer: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        meta = SchoolMetadata(school_dir)
        meta._metadata["phases"]["discovery"] = {
            "catalog_urls": [
                "https://example.edu/catalog.pdf",
                "https://example.edu/courses",
            ],
        }
        # Mark the PDF as already downloaded
        meta.add_downloaded_url(
            "https://example.edu/catalog.pdf", "catalog/catalog.pdf"
        )

        html_dest = school_dir / "catalog" / "courses.pdf"
        mock_renderer.render_to_pdf.return_value = html_dest

        scraper = _make_scraper(mock_http_client, mock_renderer)
        scraper.scrape(school, school_dir, meta)

        # PDF should be skipped
        mock_http_client.download.assert_not_called()
        # HTML should still be rendered
        mock_renderer.render_to_pdf.assert_called_once()


# ------------------------------------------------------------------
# scrape() — error handling
# ------------------------------------------------------------------


class TestScrapeHandlesErrors:
    """scrape() handles download errors gracefully (continues to next URL)."""

    def test_continues_after_pdf_download_error(
        self,
        mock_http_client: MagicMock,
        mock_renderer: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        meta = SchoolMetadata(school_dir)
        meta._metadata["phases"]["discovery"] = {
            "catalog_urls": [
                "https://example.edu/catalog.pdf",
                "https://example.edu/courses",
            ],
        }
        mock_http_client.download.side_effect = Exception("Network error")
        html_dest = school_dir / "catalog" / "courses.pdf"
        mock_renderer.render_to_pdf.return_value = html_dest

        scraper = _make_scraper(mock_http_client, mock_renderer)
        # Should NOT raise
        scraper.scrape(school, school_dir, meta)

        # The HTML URL should still be processed
        mock_renderer.render_to_pdf.assert_called_once()

    def test_continues_after_render_error(
        self,
        mock_http_client: MagicMock,
        mock_renderer: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        meta = SchoolMetadata(school_dir)
        meta._metadata["phases"]["discovery"] = {
            "catalog_urls": [
                "https://example.edu/courses",
                "https://example.edu/catalog.pdf",
            ],
        }
        mock_renderer.render_to_pdf.side_effect = Exception("Render failed")
        pdf_dest = school_dir / "catalog" / "catalog.pdf"
        mock_http_client.download.return_value = pdf_dest

        scraper = _make_scraper(mock_http_client, mock_renderer)
        # Should NOT raise
        scraper.scrape(school, school_dir, meta)

        # The PDF URL should still be processed
        mock_http_client.download.assert_called_once()


# ------------------------------------------------------------------
# scrape() — no catalog URLs
# ------------------------------------------------------------------


class TestScrapeNoCatalogUrls:
    """scrape() with no catalog URLs does nothing."""

    def test_no_urls_does_nothing(
        self,
        mock_http_client: MagicMock,
        mock_renderer: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        meta = SchoolMetadata(school_dir)
        # No discovery phase data at all

        scraper = _make_scraper(mock_http_client, mock_renderer)
        scraper.scrape(school, school_dir, meta)

        mock_http_client.download.assert_not_called()
        mock_renderer.render_to_pdf.assert_not_called()

    def test_empty_catalog_urls_does_nothing(
        self,
        mock_http_client: MagicMock,
        mock_renderer: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        meta = SchoolMetadata(school_dir)
        meta._metadata["phases"]["discovery"] = {
            "catalog_urls": [],
        }

        scraper = _make_scraper(mock_http_client, mock_renderer)
        scraper.scrape(school, school_dir, meta)

        mock_http_client.download.assert_not_called()
        mock_renderer.render_to_pdf.assert_not_called()


# ------------------------------------------------------------------
# scrape() — creates catalog directory
# ------------------------------------------------------------------


class TestScrapeCreatesCatalogDir:
    """scrape() creates the catalog directory."""

    def test_creates_catalog_directory(
        self,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        meta = SchoolMetadata(school_dir)
        meta._metadata["phases"]["discovery"] = {
            "catalog_urls": ["https://example.edu/catalog.pdf"],
        }
        pdf_dest = school_dir / "catalog" / "catalog.pdf"
        mock_http_client.download.return_value = pdf_dest

        scraper = _make_scraper(mock_http_client)
        scraper.scrape(school, school_dir, meta)

        assert (school_dir / "catalog").is_dir()

    def test_creates_catalog_dir_even_with_no_urls(
        self,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        meta = SchoolMetadata(school_dir)
        meta._metadata["phases"]["discovery"] = {
            "catalog_urls": ["https://example.edu/x.pdf"],
        }
        mock_http_client.download.return_value = school_dir / "catalog" / "x.pdf"

        scraper = _make_scraper(mock_http_client)
        scraper.scrape(school, school_dir, meta)

        assert (school_dir / "catalog").is_dir()


# ------------------------------------------------------------------
# _is_pdf_url()
# ------------------------------------------------------------------


class TestIsPdfUrl:
    """_is_pdf_url() correctly identifies PDF URLs."""

    @pytest.fixture()
    def scraper(self, mock_http_client: MagicMock) -> CatalogScraper:
        return _make_scraper(mock_http_client)

    def test_pdf_extension(self, scraper: CatalogScraper) -> None:
        assert scraper._is_pdf_url("https://example.edu/catalog.pdf") is True

    def test_pdf_extension_uppercase(self, scraper: CatalogScraper) -> None:
        assert scraper._is_pdf_url("https://example.edu/catalog.PDF") is True

    def test_pdf_extension_mixed_case(self, scraper: CatalogScraper) -> None:
        assert scraper._is_pdf_url("https://example.edu/catalog.Pdf") is True

    def test_pdf_with_query_string(self, scraper: CatalogScraper) -> None:
        assert (
            scraper._is_pdf_url("https://example.edu/catalog.pdf?v=2") is True
        )

    def test_pdf_with_path_segments(self, scraper: CatalogScraper) -> None:
        assert (
            scraper._is_pdf_url("https://example.edu/docs/2024/catalog.pdf")
            is True
        )

    def test_html_url(self, scraper: CatalogScraper) -> None:
        assert scraper._is_pdf_url("https://example.edu/courses") is False

    def test_html_extension(self, scraper: CatalogScraper) -> None:
        assert (
            scraper._is_pdf_url("https://example.edu/catalog.html") is False
        )

    def test_no_extension(self, scraper: CatalogScraper) -> None:
        assert (
            scraper._is_pdf_url("https://example.edu/catalog") is False
        )

    def test_other_extension(self, scraper: CatalogScraper) -> None:
        assert (
            scraper._is_pdf_url("https://example.edu/catalog.docx") is False
        )

    def test_pdf_in_path_but_not_extension(
        self, scraper: CatalogScraper
    ) -> None:
        assert (
            scraper._is_pdf_url("https://example.edu/pdf/catalog") is False
        )


# ------------------------------------------------------------------
# _url_to_filename()
# ------------------------------------------------------------------


class TestUrlToFilename:
    """_url_to_filename() generates safe filenames."""

    @pytest.fixture()
    def scraper(self, mock_http_client: MagicMock) -> CatalogScraper:
        return _make_scraper(mock_http_client)

    def test_simple_pdf_url(self, scraper: CatalogScraper) -> None:
        result = scraper._url_to_filename(
            "https://example.edu/catalog.pdf", ".pdf"
        )
        assert result == "catalog.pdf"

    def test_html_url(self, scraper: CatalogScraper) -> None:
        result = scraper._url_to_filename(
            "https://example.edu/courses", ".pdf"
        )
        assert result == "courses.pdf"

    def test_nested_path(self, scraper: CatalogScraper) -> None:
        result = scraper._url_to_filename(
            "https://example.edu/dept/cs/catalog.pdf", ".pdf"
        )
        assert result == "dept-cs-catalog.pdf"

    def test_url_with_no_path(self, scraper: CatalogScraper) -> None:
        result = scraper._url_to_filename("https://example.edu", ".pdf")
        assert result == "example-edu.pdf"

    def test_url_with_root_path(self, scraper: CatalogScraper) -> None:
        result = scraper._url_to_filename("https://example.edu/", ".pdf")
        assert result == "example-edu.pdf"

    def test_special_characters_cleaned(self, scraper: CatalogScraper) -> None:
        result = scraper._url_to_filename(
            "https://example.edu/my catalog (2024).pdf", ".pdf"
        )
        # Spaces and parens should become hyphens
        assert ".." not in result
        assert result.endswith(".pdf")
        # Should only contain safe chars before .pdf
        name_part = result[: -len(".pdf")]
        assert all(c.isalnum() or c in "-_" for c in name_part)

    def test_trailing_slash_stripped(self, scraper: CatalogScraper) -> None:
        result = scraper._url_to_filename(
            "https://example.edu/courses/", ".pdf"
        )
        assert result == "courses.pdf"

    def test_html_extension_replaced(self, scraper: CatalogScraper) -> None:
        result = scraper._url_to_filename(
            "https://example.edu/catalog.html", ".pdf"
        )
        assert result == "catalog.pdf"


# ------------------------------------------------------------------
# _render_html_to_pdf() — no renderer
# ------------------------------------------------------------------


class TestRenderHtmlNoneRenderer:
    """_render_html_to_pdf() returns None when no renderer available."""

    def test_returns_none_without_renderer(
        self, mock_http_client: MagicMock, tmp_path: Path
    ) -> None:
        scraper = _make_scraper(mock_http_client, renderer=None)
        result = scraper._render_html_to_pdf(
            "https://example.edu/courses", tmp_path / "catalog"
        )
        assert result is None


# ------------------------------------------------------------------
# Metadata updates
# ------------------------------------------------------------------


class TestMetadataUpdated:
    """metadata is updated after each successful download."""

    def test_metadata_updated_after_pdf_download(
        self,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        meta = SchoolMetadata(school_dir)
        meta._metadata["phases"]["discovery"] = {
            "catalog_urls": ["https://example.edu/catalog.pdf"],
        }
        pdf_dest = school_dir / "catalog" / "catalog.pdf"
        mock_http_client.download.return_value = pdf_dest

        scraper = _make_scraper(mock_http_client)
        scraper.scrape(school, school_dir, meta)

        assert meta.is_url_downloaded("https://example.edu/catalog.pdf")

    def test_metadata_updated_after_html_render(
        self,
        mock_http_client: MagicMock,
        mock_renderer: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        meta = SchoolMetadata(school_dir)
        meta._metadata["phases"]["discovery"] = {
            "catalog_urls": ["https://example.edu/courses"],
        }
        html_dest = school_dir / "catalog" / "courses.pdf"
        mock_renderer.render_to_pdf.return_value = html_dest

        scraper = _make_scraper(mock_http_client, mock_renderer)
        scraper.scrape(school, school_dir, meta)

        assert meta.is_url_downloaded("https://example.edu/courses")

    def test_metadata_not_updated_on_error(
        self,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        meta = SchoolMetadata(school_dir)
        meta._metadata["phases"]["discovery"] = {
            "catalog_urls": ["https://example.edu/catalog.pdf"],
        }
        mock_http_client.download.side_effect = Exception("Fail")

        scraper = _make_scraper(mock_http_client)
        scraper.scrape(school, school_dir, meta)

        assert not meta.is_url_downloaded("https://example.edu/catalog.pdf")

    def test_metadata_not_updated_when_html_returns_none(
        self,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        meta = SchoolMetadata(school_dir)
        meta._metadata["phases"]["discovery"] = {
            "catalog_urls": ["https://example.edu/courses"],
        }
        # No renderer => returns None
        scraper = _make_scraper(mock_http_client, renderer=None)
        scraper.scrape(school, school_dir, meta)

        assert not meta.is_url_downloaded("https://example.edu/courses")

    def test_metadata_save_called_after_each_success(
        self,
        mock_http_client: MagicMock,
        mock_renderer: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        meta = SchoolMetadata(school_dir)
        meta._metadata["phases"]["discovery"] = {
            "catalog_urls": [
                "https://example.edu/catalog.pdf",
                "https://example.edu/courses",
            ],
        }
        mock_http_client.download.return_value = (
            school_dir / "catalog" / "catalog.pdf"
        )
        mock_renderer.render_to_pdf.return_value = (
            school_dir / "catalog" / "courses.pdf"
        )
        # Spy on save
        original_save = meta.save
        save_count = 0

        def counting_save():
            nonlocal save_count
            save_count += 1
            original_save()

        meta.save = counting_save

        scraper = _make_scraper(mock_http_client, mock_renderer)
        scraper.scrape(school, school_dir, meta)

        assert save_count == 2


# ------------------------------------------------------------------
# _get_catalog_urls()
# ------------------------------------------------------------------


class TestGetCatalogUrls:
    """_get_catalog_urls() reads from discovery phase data."""

    @pytest.fixture()
    def scraper(self, mock_http_client: MagicMock) -> CatalogScraper:
        return _make_scraper(mock_http_client)

    def test_reads_catalog_urls_from_discovery(
        self, scraper: CatalogScraper, school_dir: Path
    ) -> None:
        meta = SchoolMetadata(school_dir)
        meta._metadata["phases"]["discovery"] = {
            "catalog_urls": [
                "https://example.edu/catalog.pdf",
                "https://example.edu/courses",
            ],
        }
        urls = scraper._get_catalog_urls(meta)
        assert urls == [
            "https://example.edu/catalog.pdf",
            "https://example.edu/courses",
        ]

    def test_returns_empty_when_no_phases(
        self, scraper: CatalogScraper, school_dir: Path
    ) -> None:
        meta = SchoolMetadata(school_dir)
        urls = scraper._get_catalog_urls(meta)
        assert urls == []

    def test_returns_empty_when_no_discovery(
        self, scraper: CatalogScraper, school_dir: Path
    ) -> None:
        meta = SchoolMetadata(school_dir)
        meta._metadata["phases"] = {}
        urls = scraper._get_catalog_urls(meta)
        assert urls == []

    def test_returns_empty_when_no_catalog_urls_key(
        self, scraper: CatalogScraper, school_dir: Path
    ) -> None:
        meta = SchoolMetadata(school_dir)
        meta._metadata["phases"]["discovery"] = {"status": "completed"}
        urls = scraper._get_catalog_urls(meta)
        assert urls == []

    def test_returns_empty_when_catalog_urls_is_not_list(
        self, scraper: CatalogScraper, school_dir: Path
    ) -> None:
        meta = SchoolMetadata(school_dir)
        meta._metadata["phases"]["discovery"] = {
            "catalog_urls": "not-a-list",
        }
        urls = scraper._get_catalog_urls(meta)
        assert urls == []

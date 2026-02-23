"""Tests for scrape_edu.scrapers.syllabus_scraper module."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call

import pytest

from scrape_edu.data.manifest import SchoolMetadata
from scrape_edu.data.school import School
from scrape_edu.net.http_client import HttpClient
from scrape_edu.scrapers.syllabus_scraper import SyllabusScraper


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture()
def mock_http_client() -> MagicMock:
    """Return a mocked HttpClient."""
    return MagicMock(spec=HttpClient)


@pytest.fixture()
def scraper(mock_http_client: MagicMock) -> SyllabusScraper:
    """Return a SyllabusScraper instance with a mocked client."""
    return SyllabusScraper(http_client=mock_http_client, config={})


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
    """Return a SchoolMetadata instance with syllabus URLs pre-populated."""
    meta = SchoolMetadata(school_dir)
    meta._metadata["phases"] = {
        "discovery": {
            "syllabus_urls": [
                "https://www.mit.edu/courses/cs101/syllabus.pdf",
                "https://www.mit.edu/courses/cs201/outline.pdf",
            ],
        },
    }
    return meta


# ------------------------------------------------------------------
# Tests — scrape() method
# ------------------------------------------------------------------


class TestSyllabusScraperScrape:
    """Test the main scrape() method."""

    def test_downloads_syllabus_pdfs(
        self,
        scraper: SyllabusScraper,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
        metadata: SchoolMetadata,
    ) -> None:
        """scrape() calls client.download() for each syllabus URL."""
        mock_http_client.download.return_value = Path("dummy.pdf")

        scraper.scrape(school, school_dir, metadata)

        assert mock_http_client.download.call_count == 2
        called_urls = [
            c.args[0] for c in mock_http_client.download.call_args_list
        ]
        assert "https://www.mit.edu/courses/cs101/syllabus.pdf" in called_urls
        assert "https://www.mit.edu/courses/cs201/outline.pdf" in called_urls

    def test_skips_already_downloaded_urls(
        self,
        scraper: SyllabusScraper,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
        metadata: SchoolMetadata,
    ) -> None:
        """scrape() skips URLs that are already downloaded."""
        metadata.add_downloaded_url(
            "https://www.mit.edu/courses/cs101/syllabus.pdf",
            "syllabi/syllabus.pdf",
        )
        mock_http_client.download.return_value = Path("dummy.pdf")

        scraper.scrape(school, school_dir, metadata)

        assert mock_http_client.download.call_count == 1
        assert (
            mock_http_client.download.call_args.args[0]
            == "https://www.mit.edu/courses/cs201/outline.pdf"
        )

    def test_handles_download_errors_gracefully(
        self,
        scraper: SyllabusScraper,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
        metadata: SchoolMetadata,
    ) -> None:
        """scrape() continues after a download error."""
        mock_http_client.download.side_effect = [
            Exception("Timeout"),
            Path("dummy.pdf"),
        ]

        scraper.scrape(school, school_dir, metadata)

        # Both URLs attempted
        assert mock_http_client.download.call_count == 2
        # Only second URL should be tracked
        assert not metadata.is_url_downloaded(
            "https://www.mit.edu/courses/cs101/syllabus.pdf"
        )
        assert metadata.is_url_downloaded(
            "https://www.mit.edu/courses/cs201/outline.pdf"
        )

    def test_no_syllabus_urls_does_nothing(
        self,
        scraper: SyllabusScraper,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        """scrape() does nothing when no syllabus URLs exist."""
        metadata = SchoolMetadata(school_dir)

        scraper.scrape(school, school_dir, metadata)

        assert mock_http_client.download.call_count == 0

    def test_scans_faculty_html_for_syllabus_links(
        self,
        scraper: SyllabusScraper,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        """scrape() scans faculty HTML files for additional syllabus links."""
        metadata = SchoolMetadata(school_dir)
        metadata._metadata["phases"] = {"discovery": {"syllabus_urls": []}}

        # Create a faculty HTML file with a syllabus link
        faculty_dir = school_dir / "faculty"
        faculty_dir.mkdir()
        html = """
        <html><body>
        <a href="/courses/ds100/syllabus.pdf">Download Syllabus</a>
        </body></html>
        """
        (faculty_dir / "prof-smith.html").write_text(html, encoding="utf-8")

        mock_http_client.download.return_value = Path("dummy.pdf")

        scraper.scrape(school, school_dir, metadata)

        assert mock_http_client.download.call_count == 1
        assert "syllabus.pdf" in mock_http_client.download.call_args.args[0]

    def test_deduplicates_urls(
        self,
        scraper: SyllabusScraper,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        """scrape() deduplicates syllabus URLs found from multiple sources."""
        dup_url = "https://www.mit.edu/courses/cs101/syllabus.pdf"
        metadata = SchoolMetadata(school_dir)
        metadata._metadata["phases"] = {
            "discovery": {
                "syllabus_urls": [dup_url, dup_url],
            },
        }

        # Also create a faculty HTML file with the same URL
        faculty_dir = school_dir / "faculty"
        faculty_dir.mkdir()
        html = f'<html><body><a href="{dup_url}">Syllabus</a></body></html>'
        (faculty_dir / "page.html").write_text(html, encoding="utf-8")

        mock_http_client.download.return_value = Path("dummy.pdf")

        scraper.scrape(school, school_dir, metadata)

        # Should only download once, not 3 times
        assert mock_http_client.download.call_count == 1

    def test_creates_syllabi_directory(
        self,
        scraper: SyllabusScraper,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
        metadata: SchoolMetadata,
    ) -> None:
        """scrape() creates the syllabi/ subdirectory."""
        mock_http_client.download.return_value = Path("dummy.pdf")

        scraper.scrape(school, school_dir, metadata)

        assert (school_dir / "syllabi").is_dir()

    def test_tracks_downloaded_urls_in_metadata(
        self,
        scraper: SyllabusScraper,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
        metadata: SchoolMetadata,
    ) -> None:
        """scrape() records each downloaded URL in metadata."""
        mock_http_client.download.return_value = Path("dummy.pdf")

        scraper.scrape(school, school_dir, metadata)

        assert metadata.is_url_downloaded(
            "https://www.mit.edu/courses/cs101/syllabus.pdf"
        )
        assert metadata.is_url_downloaded(
            "https://www.mit.edu/courses/cs201/outline.pdf"
        )

    def test_passes_correct_dest_path_to_download(
        self,
        scraper: SyllabusScraper,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        """scrape() passes a path inside syllabi/ to client.download()."""
        metadata = SchoolMetadata(school_dir)
        metadata._metadata["phases"] = {
            "discovery": {
                "syllabus_urls": ["https://www.mit.edu/courses/cs101/syllabus.pdf"],
            },
        }
        mock_http_client.download.return_value = Path("dummy.pdf")

        scraper.scrape(school, school_dir, metadata)

        dest_arg = mock_http_client.download.call_args.args[1]
        assert str(dest_arg).startswith(str(school_dir / "syllabi"))
        assert str(dest_arg).endswith("syllabus.pdf")


# ------------------------------------------------------------------
# Tests — _get_syllabus_urls()
# ------------------------------------------------------------------


class TestGetSyllabusUrls:
    """Test the _get_syllabus_urls helper."""

    def test_returns_urls_from_metadata(
        self, scraper: SyllabusScraper, metadata: SchoolMetadata
    ) -> None:
        urls = scraper._get_syllabus_urls(metadata)
        assert urls == [
            "https://www.mit.edu/courses/cs101/syllabus.pdf",
            "https://www.mit.edu/courses/cs201/outline.pdf",
        ]

    def test_returns_empty_when_no_discovery(
        self, scraper: SyllabusScraper, school_dir: Path
    ) -> None:
        metadata = SchoolMetadata(school_dir)
        assert scraper._get_syllabus_urls(metadata) == []

    def test_returns_empty_when_not_a_list(
        self, scraper: SyllabusScraper, school_dir: Path
    ) -> None:
        metadata = SchoolMetadata(school_dir)
        metadata._metadata["phases"] = {
            "discovery": {"syllabus_urls": "not-a-list"},
        }
        assert scraper._get_syllabus_urls(metadata) == []


# ------------------------------------------------------------------
# Tests — _extract_syllabus_links()
# ------------------------------------------------------------------


class TestExtractSyllabusLinks:
    """Test the _extract_syllabus_links helper."""

    def test_finds_links_with_syllabus_in_text(
        self, scraper: SyllabusScraper
    ) -> None:
        html = """
        <html><body>
        <a href="/files/cs101.pdf">Course Syllabus</a>
        <a href="/files/cs102.pdf">Other Document</a>
        </body></html>
        """
        links = scraper._extract_syllabus_links(html, "https://example.edu")
        assert len(links) == 1
        assert links[0] == "https://example.edu/files/cs101.pdf"

    def test_finds_links_with_syllabus_in_href(
        self, scraper: SyllabusScraper
    ) -> None:
        html = """
        <html><body>
        <a href="/downloads/cs101-syllabus.pdf">Download</a>
        <a href="/downloads/cs101-notes.pdf">Notes</a>
        </body></html>
        """
        links = scraper._extract_syllabus_links(html, "https://example.edu")
        assert len(links) == 1
        assert links[0] == "https://example.edu/downloads/cs101-syllabus.pdf"

    def test_finds_syllabi_keyword(
        self, scraper: SyllabusScraper
    ) -> None:
        html = """
        <html><body>
        <a href="/archive/syllabi/">View All Syllabi</a>
        </body></html>
        """
        links = scraper._extract_syllabus_links(html, "https://example.edu")
        assert len(links) == 1
        assert "syllabi" in links[0]

    def test_resolves_relative_urls(
        self, scraper: SyllabusScraper
    ) -> None:
        html = """
        <html><body>
        <a href="/docs/syllabus.pdf">Syllabus</a>
        </body></html>
        """
        links = scraper._extract_syllabus_links(
            html, "https://example.edu/courses/cs101/"
        )
        assert len(links) == 1
        assert links[0] == "https://example.edu/docs/syllabus.pdf"

    def test_resolves_fully_relative_urls(
        self, scraper: SyllabusScraper
    ) -> None:
        html = """
        <html><body>
        <a href="syllabus.pdf">Syllabus PDF</a>
        </body></html>
        """
        links = scraper._extract_syllabus_links(
            html, "https://example.edu/courses/cs101/"
        )
        assert len(links) == 1
        assert links[0] == "https://example.edu/courses/cs101/syllabus.pdf"

    def test_returns_empty_for_no_matches(
        self, scraper: SyllabusScraper
    ) -> None:
        html = """
        <html><body>
        <a href="/about">About Us</a>
        <a href="/contact.pdf">Contact Sheet</a>
        </body></html>
        """
        links = scraper._extract_syllabus_links(html, "https://example.edu")
        assert links == []

    def test_handles_course_outline_keyword(
        self, scraper: SyllabusScraper
    ) -> None:
        html = """
        <html><body>
        <a href="/cs101-outline.pdf">Course Outline</a>
        </body></html>
        """
        links = scraper._extract_syllabus_links(html, "https://example.edu")
        assert len(links) == 1

    def test_ignores_non_http_links(
        self, scraper: SyllabusScraper
    ) -> None:
        html = """
        <html><body>
        <a href="javascript:void(0)">Syllabus</a>
        <a href="mailto:prof@example.edu">Syllabus Request</a>
        </body></html>
        """
        links = scraper._extract_syllabus_links(html, "https://example.edu")
        assert links == []


# ------------------------------------------------------------------
# Tests — _is_direct_file() and _split_files_and_pages()
# ------------------------------------------------------------------


class TestIsDirectFile:
    """Test the _is_direct_file classifier."""

    def test_pdf_is_direct_file(self, scraper: SyllabusScraper) -> None:
        assert scraper._is_direct_file("https://example.edu/syllabus.pdf")

    def test_docx_is_direct_file(self, scraper: SyllabusScraper) -> None:
        assert scraper._is_direct_file("https://example.edu/outline.docx")

    def test_doc_is_direct_file(self, scraper: SyllabusScraper) -> None:
        assert scraper._is_direct_file("https://example.edu/outline.doc")

    def test_php_is_not_direct_file(self, scraper: SyllabusScraper) -> None:
        assert not scraper._is_direct_file("https://example.edu/syllabi.php")

    def test_html_is_not_direct_file(self, scraper: SyllabusScraper) -> None:
        assert not scraper._is_direct_file("https://example.edu/syllabi/index.html")

    def test_no_extension_is_not_direct_file(self, scraper: SyllabusScraper) -> None:
        assert not scraper._is_direct_file("https://example.edu/syllabi/archive")


class TestSplitFilesAndPages:
    """Test the _split_files_and_pages helper."""

    def test_splits_correctly(self, scraper: SyllabusScraper) -> None:
        urls = [
            "https://example.edu/syllabus.pdf",
            "https://example.edu/archive.php",
            "https://example.edu/outline.docx",
            "https://example.edu/syllabi/",
        ]
        files, pages = scraper._split_files_and_pages(urls)
        assert files == [
            "https://example.edu/syllabus.pdf",
            "https://example.edu/outline.docx",
        ]
        assert pages == [
            "https://example.edu/archive.php",
            "https://example.edu/syllabi/",
        ]

    def test_all_files(self, scraper: SyllabusScraper) -> None:
        urls = ["https://example.edu/a.pdf", "https://example.edu/b.doc"]
        files, pages = scraper._split_files_and_pages(urls)
        assert len(files) == 2
        assert len(pages) == 0

    def test_all_pages(self, scraper: SyllabusScraper) -> None:
        urls = ["https://example.edu/syllabi.php", "https://example.edu/archive/"]
        files, pages = scraper._split_files_and_pages(urls)
        assert len(files) == 0
        assert len(pages) == 2


# ------------------------------------------------------------------
# Tests — _follow_syllabus_pages()
# ------------------------------------------------------------------


class TestFollowSyllabusPages:
    """Test the one-level link-following for syllabus HTML pages."""

    def test_follows_html_page_and_finds_pdfs(
        self,
        mock_http_client: MagicMock,
        school: School,
    ) -> None:
        """Following an HTML page extracts direct file links."""
        scraper = SyllabusScraper(http_client=mock_http_client, config={})

        archive_html = """
        <html><body>
        <a href="/syllabi/cs101-fall2024.pdf">CS101 Syllabus</a>
        <a href="/syllabi/cs201-fall2024.pdf">CS201 Syllabus</a>
        <a href="/about">About Us</a>
        </body></html>
        """
        mock_response = MagicMock()
        mock_response.text = archive_html
        mock_http_client.get.return_value = mock_response

        result = scraper._follow_syllabus_pages(
            ["https://www.mit.edu/syllabi/archive.php"],
            school,
            max_followed=20,
        )

        assert len(result) == 2
        assert "https://www.mit.edu/syllabi/cs101-fall2024.pdf" in result
        assert "https://www.mit.edu/syllabi/cs201-fall2024.pdf" in result

    def test_respects_max_followed_limit(
        self,
        mock_http_client: MagicMock,
        school: School,
    ) -> None:
        """Only follows up to max_followed pages."""
        scraper = SyllabusScraper(http_client=mock_http_client, config={})

        mock_response = MagicMock()
        mock_response.text = "<html><body></body></html>"
        mock_http_client.get.return_value = mock_response

        pages = [f"https://www.mit.edu/page{i}.php" for i in range(10)]
        scraper._follow_syllabus_pages(pages, school, max_followed=3)

        assert mock_http_client.get.call_count == 3

    def test_ignores_off_domain_links(
        self,
        mock_http_client: MagicMock,
        school: School,
    ) -> None:
        """Links pointing to external domains are filtered out."""
        scraper = SyllabusScraper(http_client=mock_http_client, config={})

        archive_html = """
        <html><body>
        <a href="https://www.mit.edu/syllabi/cs101.pdf">CS101 Syllabus</a>
        <a href="https://external.com/syllabi/stolen.pdf">External Syllabus</a>
        </body></html>
        """
        mock_response = MagicMock()
        mock_response.text = archive_html
        mock_http_client.get.return_value = mock_response

        result = scraper._follow_syllabus_pages(
            ["https://www.mit.edu/archive.php"],
            school,
            max_followed=20,
        )

        assert len(result) == 1
        assert "mit.edu" in result[0]

    def test_handles_fetch_errors_gracefully(
        self,
        mock_http_client: MagicMock,
        school: School,
    ) -> None:
        """Errors fetching a page don't stop processing of other pages."""
        scraper = SyllabusScraper(http_client=mock_http_client, config={})

        good_html = """
        <html><body>
        <a href="/syllabi/cs101.pdf">CS101 Syllabus</a>
        </body></html>
        """
        good_response = MagicMock()
        good_response.text = good_html

        mock_http_client.get.side_effect = [
            Exception("Timeout"),
            good_response,
        ]

        result = scraper._follow_syllabus_pages(
            [
                "https://www.mit.edu/broken.php",
                "https://www.mit.edu/working.php",
            ],
            school,
            max_followed=20,
        )

        assert len(result) == 1
        assert "cs101.pdf" in result[0]

    def test_only_returns_direct_files_not_more_pages(
        self,
        mock_http_client: MagicMock,
        school: School,
    ) -> None:
        """Only direct file URLs are returned, not further HTML pages."""
        scraper = SyllabusScraper(http_client=mock_http_client, config={})

        archive_html = """
        <html><body>
        <a href="/syllabi/cs101.pdf">CS101 Syllabus</a>
        <a href="/syllabi/more-syllabi.php">View More Syllabi</a>
        </body></html>
        """
        mock_response = MagicMock()
        mock_response.text = archive_html
        mock_http_client.get.return_value = mock_response

        result = scraper._follow_syllabus_pages(
            ["https://www.mit.edu/archive.php"],
            school,
            max_followed=20,
        )

        assert len(result) == 1
        assert result[0].endswith(".pdf")


class TestScrapeWithFollowing:
    """Test that scrape() integrates link-following correctly."""

    def test_follows_page_urls_and_downloads_found_files(
        self,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        """scrape() follows an HTML syllabus page and downloads PDFs found there."""
        scraper = SyllabusScraper(
            http_client=mock_http_client, config={"syllabus_max_followed": 20}
        )
        metadata = SchoolMetadata(school_dir)
        # Discovery found an HTML page, not a direct PDF
        metadata._metadata["phases"] = {
            "discovery": {
                "syllabus_urls": ["https://www.mit.edu/syllabi/archive.php"],
            },
        }

        archive_html = """
        <html><body>
        <a href="/syllabi/cs101.pdf">CS101 Syllabus</a>
        <a href="/syllabi/cs201.pdf">CS201 Syllabus</a>
        </body></html>
        """
        mock_response = MagicMock()
        mock_response.text = archive_html
        mock_http_client.get.return_value = mock_response
        mock_http_client.download.return_value = Path("dummy.pdf")

        scraper.scrape(school, school_dir, metadata)

        # Should download the archive page itself + 2 PDFs found on it = 3
        assert mock_http_client.download.call_count == 3
        downloaded_urls = [
            c.args[0] for c in mock_http_client.download.call_args_list
        ]
        assert "https://www.mit.edu/syllabi/archive.php" in downloaded_urls
        assert "https://www.mit.edu/syllabi/cs101.pdf" in downloaded_urls
        assert "https://www.mit.edu/syllabi/cs201.pdf" in downloaded_urls


# ------------------------------------------------------------------
# Tests — _url_to_filename()
# ------------------------------------------------------------------


class TestUrlToFilename:
    """Test the _url_to_filename + _get_url_extension helpers."""

    def test_preserves_pdf_extension(
        self, scraper: SyllabusScraper
    ) -> None:
        url = "https://example.edu/courses/cs101/syllabus.pdf"
        result = scraper._url_to_filename(url, scraper._get_url_extension(url))
        assert result == "courses-cs101-syllabus.pdf"

    def test_preserves_docx_extension(
        self, scraper: SyllabusScraper
    ) -> None:
        url = "https://example.edu/docs/outline.docx"
        result = scraper._url_to_filename(url, scraper._get_url_extension(url))
        assert result == "docs-outline.docx"

    def test_defaults_to_pdf_when_no_extension(
        self, scraper: SyllabusScraper
    ) -> None:
        url = "https://example.edu/courses/cs101/syllabus"
        result = scraper._url_to_filename(url, scraper._get_url_extension(url))
        assert result == "courses-cs101-syllabus.pdf"

    def test_defaults_to_pdf_when_no_path(
        self, scraper: SyllabusScraper
    ) -> None:
        url = "https://example.edu/"
        result = scraper._url_to_filename(url, scraper._get_url_extension(url))
        assert result.endswith(".pdf")

    def test_sanitizes_special_characters(
        self, scraper: SyllabusScraper
    ) -> None:
        url = "https://example.edu/docs/my syllabus (2024).pdf"
        result = scraper._url_to_filename(url, scraper._get_url_extension(url))
        # Should have no spaces or parens
        assert " " not in result
        assert "(" not in result
        assert result.endswith(".pdf")

    def test_preserves_doc_extension(
        self, scraper: SyllabusScraper
    ) -> None:
        url = "https://example.edu/docs/course.doc"
        result = scraper._url_to_filename(url, scraper._get_url_extension(url))
        assert result == "docs-course.doc"

"""Tests for scrape_edu.scrapers.syllabus_scraper module."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call

import pytest

from scrape_edu.data.manifest import SchoolMetadata
from scrape_edu.data.school import School
from scrape_edu.net.http_client import HttpClient
from scrape_edu.scrapers.syllabus_scraper import BfsStats, SyllabusScraper


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

        result, stats = scraper._follow_syllabus_pages(
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
        _, _ = scraper._follow_syllabus_pages(pages, school, max_followed=3)

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

        result, _ = scraper._follow_syllabus_pages(
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

        result, _ = scraper._follow_syllabus_pages(
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

        result, _ = scraper._follow_syllabus_pages(
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


# ------------------------------------------------------------------
# Tests — _extract_course_links()
# ------------------------------------------------------------------


class TestExtractCourseLinks:
    """Test the _extract_course_links helper."""

    def test_finds_links_with_courses_path(
        self, scraper: SyllabusScraper
    ) -> None:
        html = """
        <html><body>
        <a href="/courses/csci-1302">CSCI 1302</a>
        <a href="/about">About</a>
        </body></html>
        """
        links = scraper._extract_course_links(
            html, "https://computing.uga.edu/syllabi", "https://uga.edu"
        )
        assert len(links) == 1
        assert "/courses/csci-1302" in links[0]

    def test_finds_links_with_course_text_pattern(
        self, scraper: SyllabusScraper
    ) -> None:
        """Link text matching course code pattern (e.g. 'CS 101') is detected."""
        html = """
        <html><body>
        <a href="/programs/cs101-info">CS 101</a>
        <a href="/programs/about-us">About Our Programs</a>
        </body></html>
        """
        links = scraper._extract_course_links(
            html, "https://cs.example.edu/", "https://example.edu"
        )
        assert len(links) == 1
        assert "cs101-info" in links[0]

    def test_filters_off_domain_links(
        self, scraper: SyllabusScraper
    ) -> None:
        html = """
        <html><body>
        <a href="https://external.com/courses/cs101">CS 101</a>
        </body></html>
        """
        links = scraper._extract_course_links(
            html, "https://example.edu/", "https://example.edu"
        )
        assert len(links) == 0

    def test_deduplicates_course_links(
        self, scraper: SyllabusScraper
    ) -> None:
        html = """
        <html><body>
        <a href="/courses/cs101">CS 101</a>
        <a href="/courses/cs101">Introduction to CS</a>
        </body></html>
        """
        links = scraper._extract_course_links(
            html, "https://example.edu/", "https://example.edu"
        )
        assert len(links) == 1

    def test_matches_singular_course_path(
        self, scraper: SyllabusScraper
    ) -> None:
        """Matches /course/ (singular) as well as /courses/."""
        html = """
        <html><body>
        <a href="/course/data-science-200">DS 200</a>
        </body></html>
        """
        links = scraper._extract_course_links(
            html, "https://example.edu/", "https://example.edu"
        )
        assert len(links) == 1


# ------------------------------------------------------------------
# Tests — _extract_file_links()
# ------------------------------------------------------------------


class TestExtractFileLinks:
    """Test the _extract_file_links helper."""

    def test_finds_all_pdfs_on_domain(
        self, scraper: SyllabusScraper
    ) -> None:
        html = """
        <html><body>
        <a href="/files/CIS_CSCI_1302.pdf">Course PDF</a>
        <a href="/files/random_notes.pdf">Notes</a>
        <a href="/about">About</a>
        </body></html>
        """
        links = scraper._extract_file_links(
            html, "https://example.edu/courses/cs101", "https://example.edu"
        )
        assert len(links) == 2
        assert all(link.endswith(".pdf") for link in links)

    def test_finds_doc_and_docx(
        self, scraper: SyllabusScraper
    ) -> None:
        html = """
        <html><body>
        <a href="/files/outline.doc">Outline</a>
        <a href="/files/schedule.docx">Schedule</a>
        </body></html>
        """
        links = scraper._extract_file_links(
            html, "https://example.edu/", "https://example.edu"
        )
        assert len(links) == 2

    def test_ignores_off_domain_files(
        self, scraper: SyllabusScraper
    ) -> None:
        html = """
        <html><body>
        <a href="https://external.com/files/stolen.pdf">External PDF</a>
        <a href="/files/local.pdf">Local PDF</a>
        </body></html>
        """
        links = scraper._extract_file_links(
            html, "https://example.edu/", "https://example.edu"
        )
        assert len(links) == 1
        assert "local.pdf" in links[0]

    def test_ignores_html_pages(
        self, scraper: SyllabusScraper
    ) -> None:
        html = """
        <html><body>
        <a href="/page.html">HTML Page</a>
        <a href="/archive.php">Archive</a>
        <a href="/file.pdf">PDF File</a>
        </body></html>
        """
        links = scraper._extract_file_links(
            html, "https://example.edu/", "https://example.edu"
        )
        assert len(links) == 1
        assert links[0].endswith(".pdf")


# ------------------------------------------------------------------
# Tests — BFS _follow_syllabus_pages() with depth tracking
# ------------------------------------------------------------------


class TestFollowSyllabusPagesBFS:
    """Test the multi-level BFS following for syllabus pages."""

    def test_two_level_bfs_finds_pdfs_via_course_pages(
        self,
        mock_http_client: MagicMock,
        school: School,
    ) -> None:
        """Seed page → course page → PDF found (2-level BFS)."""
        scraper = SyllabusScraper(http_client=mock_http_client, config={})

        # Seed page lists course links (no direct PDFs, no syllabus keywords)
        seed_html = """
        <html><body>
        <a href="/courses/csci-1302">CSCI 1302</a>
        <a href="/courses/csci-2610">CSCI 2610</a>
        </body></html>
        """

        # Course pages each have a PDF (no "syllabus" in filename)
        course1_html = """
        <html><body>
        <a href="/files/CIS_CSCI_1302_2.pdf">Course PDF</a>
        </body></html>
        """
        course2_html = """
        <html><body>
        <a href="/files/CIS_CSCI_2610_1.pdf">Course PDF</a>
        </body></html>
        """

        responses = []
        for html in [seed_html, course1_html, course2_html]:
            r = MagicMock()
            r.text = html
            responses.append(r)
        mock_http_client.get.side_effect = responses

        # Use a UGA-like school
        uga = School(
            unitid=139959,
            name="University of Georgia",
            url="https://computing.uga.edu",
        )

        result, stats = scraper._follow_syllabus_pages(
            ["https://computing.uga.edu/syllabi"],
            uga,
            max_followed=50,
            max_depth=2,
        )

        assert len(result) == 2
        filenames = [r.split("/")[-1] for r in result]
        assert "CIS_CSCI_1302_2.pdf" in filenames
        assert "CIS_CSCI_2610_1.pdf" in filenames

    def test_depth_limit_respected(
        self,
        mock_http_client: MagicMock,
        school: School,
    ) -> None:
        """BFS does NOT follow links beyond max_depth."""
        scraper = SyllabusScraper(http_client=mock_http_client, config={})

        # Seed page has a course link
        seed_html = """
        <html><body>
        <a href="/courses/csci-1302">CSCI 1302</a>
        </body></html>
        """

        # Course page has another course link (would be depth 2)
        course_html = """
        <html><body>
        <a href="/courses/csci-9999">CSCI 9999</a>
        <a href="/files/found.pdf">PDF</a>
        </body></html>
        """

        # This page should NOT be fetched (depth 2, but max_depth=1)
        deep_html = """
        <html><body>
        <a href="/files/deep.pdf">Deep PDF</a>
        </body></html>
        """

        responses = []
        for html in [seed_html, course_html, deep_html]:
            r = MagicMock()
            r.text = html
            responses.append(r)
        mock_http_client.get.side_effect = responses

        result, _ = scraper._follow_syllabus_pages(
            ["https://www.mit.edu/syllabi"],
            school,
            max_followed=50,
            max_depth=1,  # Only 1 level deep
        )

        # Should only fetch seed + course page (2 fetches), not the deep page
        assert mock_http_client.get.call_count == 2
        # Should find found.pdf (broad extraction at depth > 0) but not deep.pdf
        filenames = [r.split("/")[-1] for r in result]
        assert "found.pdf" in filenames
        assert "deep.pdf" not in filenames

    def test_max_followed_cap_across_levels(
        self,
        mock_http_client: MagicMock,
        school: School,
    ) -> None:
        """max_followed caps total fetches across all BFS levels."""
        scraper = SyllabusScraper(http_client=mock_http_client, config={})

        # Seed page lists many course links
        course_links = "\n".join(
            f'<a href="/courses/cs{i}">CS {i}</a>' for i in range(100, 120)
        )
        seed_html = f"<html><body>{course_links}</body></html>"

        mock_response = MagicMock()
        mock_response.text = seed_html
        mock_http_client.get.return_value = mock_response

        _, _ = scraper._follow_syllabus_pages(
            ["https://www.mit.edu/syllabi"],
            school,
            max_followed=5,
            max_depth=2,
        )

        # Should stop after 5 fetches total
        assert mock_http_client.get.call_count == 5

    def test_file_count_threshold_triggers_course_extraction(
        self,
        mock_http_client: MagicMock,
        school: School,
    ) -> None:
        """When < 3 files found on a page, course links are extracted."""
        scraper = SyllabusScraper(http_client=mock_http_client, config={})

        # Seed page: 1 syllabus PDF + course links
        seed_html = """
        <html><body>
        <a href="/files/syllabus-intro.pdf">Intro Syllabus</a>
        <a href="/courses/cs201">CS 201</a>
        </body></html>
        """
        # Course page with a non-syllabus PDF
        course_html = """
        <html><body>
        <a href="/files/CS201_outline.pdf">Course Outline</a>
        </body></html>
        """

        responses = []
        for html in [seed_html, course_html]:
            r = MagicMock()
            r.text = html
            responses.append(r)
        mock_http_client.get.side_effect = responses

        result, _ = scraper._follow_syllabus_pages(
            ["https://www.mit.edu/syllabi"],
            school,
            max_followed=50,
            max_depth=2,
        )

        # Should find both: the direct syllabus PDF and the one from the course page
        assert len(result) == 2
        filenames = [r.split("/")[-1] for r in result]
        assert "syllabus-intro.pdf" in filenames
        assert "CS201_outline.pdf" in filenames

    def test_many_files_suppresses_course_extraction(
        self,
        mock_http_client: MagicMock,
        school: School,
    ) -> None:
        """When >= 3 files found on a page, course links are NOT followed."""
        scraper = SyllabusScraper(http_client=mock_http_client, config={})

        # Seed page: 3+ syllabus PDFs + course links
        seed_html = """
        <html><body>
        <a href="/files/syllabus-101.pdf">CS101 Syllabus</a>
        <a href="/files/syllabus-201.pdf">CS201 Syllabus</a>
        <a href="/files/syllabus-301.pdf">CS301 Syllabus</a>
        <a href="/courses/cs401">CS 401</a>
        </body></html>
        """

        mock_response = MagicMock()
        mock_response.text = seed_html
        mock_http_client.get.return_value = mock_response

        result, _ = scraper._follow_syllabus_pages(
            ["https://www.mit.edu/syllabi"],
            school,
            max_followed=50,
            max_depth=2,
        )

        # Only 1 fetch (the seed page) — course link NOT followed
        assert mock_http_client.get.call_count == 1
        assert len(result) == 3

    def test_broad_extraction_at_depth_gt_zero(
        self,
        mock_http_client: MagicMock,
        school: School,
    ) -> None:
        """At depth > 0, files without 'syllabus' keyword are found."""
        scraper = SyllabusScraper(http_client=mock_http_client, config={})

        # Seed page with a syllabus page link
        seed_html = """
        <html><body>
        <a href="/dept/syllabi-archive">View Syllabi</a>
        </body></html>
        """
        # Archive page has PDFs without "syllabus" in name
        archive_html = """
        <html><body>
        <a href="/files/CIS_1302_Spring2025.pdf">Spring 2025</a>
        <a href="/files/CIS_2610_Fall2024.pdf">Fall 2024</a>
        </body></html>
        """

        responses = []
        for html in [seed_html, archive_html]:
            r = MagicMock()
            r.text = html
            responses.append(r)
        mock_http_client.get.side_effect = responses

        result, _ = scraper._follow_syllabus_pages(
            ["https://www.mit.edu/syllabi-page"],
            school,
            max_followed=50,
            max_depth=2,
        )

        # The archive page at depth 1 should use broad extraction
        assert len(result) == 2
        filenames = [r.split("/")[-1] for r in result]
        assert "CIS_1302_Spring2025.pdf" in filenames
        assert "CIS_2610_Fall2024.pdf" in filenames


# ------------------------------------------------------------------
# Tests — BfsStats
# ------------------------------------------------------------------


class TestBfsStats:
    """Test that BfsStats are returned correctly from _follow_syllabus_pages."""

    def test_bfs_stats_returned(
        self,
        mock_http_client: MagicMock,
        school: School,
    ) -> None:
        """BfsStats fields match expected values for a 2-level BFS scenario."""
        scraper = SyllabusScraper(http_client=mock_http_client, config={})

        seed_html = """
        <html><body>
        <a href="/courses/csci-1302">CSCI 1302</a>
        <a href="/courses/csci-2610">CSCI 2610</a>
        </body></html>
        """
        course1_html = """
        <html><body>
        <a href="/files/CIS_CSCI_1302.pdf">Course PDF</a>
        </body></html>
        """
        course2_html = """
        <html><body>
        <a href="/files/CIS_CSCI_2610.pdf">Course PDF</a>
        </body></html>
        """

        responses = []
        for html in [seed_html, course1_html, course2_html]:
            r = MagicMock()
            r.text = html
            responses.append(r)
        mock_http_client.get.side_effect = responses

        uga = School(
            unitid=139959,
            name="University of Georgia",
            url="https://computing.uga.edu",
        )

        _, stats = scraper._follow_syllabus_pages(
            ["https://computing.uga.edu/syllabi"],
            uga,
            max_followed=50,
            max_depth=2,
        )

        assert stats.pages_followed == 3
        assert stats.max_depth_reached == 1
        assert stats.files_found_by_following == 2
        assert stats.course_links_found == 2

    def test_bfs_stats_zero_for_empty_input(
        self,
        mock_http_client: MagicMock,
        school: School,
    ) -> None:
        """All stats are 0 when no pages are given."""
        scraper = SyllabusScraper(http_client=mock_http_client, config={})

        _, stats = scraper._follow_syllabus_pages(
            [], school, max_followed=50, max_depth=2,
        )

        assert stats.pages_followed == 0
        assert stats.max_depth_reached == 0
        assert stats.files_found_by_following == 0
        assert stats.course_links_found == 0


# ------------------------------------------------------------------
# Tests — Stats persistence in scrape()
# ------------------------------------------------------------------


class TestScrapeStatsInMetadata:
    """Test that scrape() persists syllabi stats in metadata."""

    def test_scrape_stores_stats_in_metadata(
        self,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
        metadata: SchoolMetadata,
    ) -> None:
        """metadata phases.syllabi has expected stat keys after scrape()."""
        mock_http_client.download.return_value = Path("dummy.pdf")
        scraper = SyllabusScraper(http_client=mock_http_client, config={})

        scraper.scrape(school, school_dir, metadata)

        syllabi = metadata._metadata["phases"]["syllabi"]
        assert "seed_urls_count" in syllabi
        assert "files_downloaded" in syllabi
        assert "files_failed" in syllabi
        assert "pages_followed" in syllabi
        assert "files_found_by_following" in syllabi
        assert "course_links_found" in syllabi
        assert syllabi["files_downloaded"] == 2
        assert syllabi["files_failed"] == 0

    def test_scrape_stores_stats_when_no_urls(
        self,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        """Stats with zeros stored even when discovery yields nothing."""
        metadata = SchoolMetadata(school_dir)
        scraper = SyllabusScraper(http_client=mock_http_client, config={})

        scraper.scrape(school, school_dir, metadata)

        syllabi = metadata._metadata["phases"]["syllabi"]
        assert syllabi["seed_urls_count"] == 0
        assert syllabi["files_downloaded"] == 0

    def test_scrape_counts_downloads_and_failures(
        self,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
        metadata: SchoolMetadata,
    ) -> None:
        """files_downloaded and files_failed match mock behavior."""
        mock_http_client.download.side_effect = [
            Exception("Timeout"),
            Path("dummy.pdf"),
        ]
        scraper = SyllabusScraper(http_client=mock_http_client, config={})

        scraper.scrape(school, school_dir, metadata)

        syllabi = metadata._metadata["phases"]["syllabi"]
        assert syllabi["files_downloaded"] == 1
        assert syllabi["files_failed"] == 1

    def test_scrape_logs_funnel_summary(
        self,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
        metadata: SchoolMetadata,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """caplog contains 'Syllabi phase complete' with expected extra fields."""
        mock_http_client.download.return_value = Path("dummy.pdf")
        scraper = SyllabusScraper(http_client=mock_http_client, config={})

        with caplog.at_level(logging.INFO, logger="scrape_edu"):
            scraper.scrape(school, school_dir, metadata)

        funnel_records = [
            r for r in caplog.records if r.message == "Syllabi phase complete"
        ]
        assert len(funnel_records) == 1
        record = funnel_records[0]
        assert record.school == school.slug
        assert record.downloaded == 2
        assert record.failed == 0


# ------------------------------------------------------------------
# Tests — _is_junk_url()
# ------------------------------------------------------------------


class TestIsJunkUrl:
    """Test the _is_junk_url classifier."""

    def test_rejects_lectures(self) -> None:
        assert SyllabusScraper._is_junk_url(
            "https://example.edu/lectures/lecture1.ppt"
        )

    def test_rejects_past_exams(self) -> None:
        assert SyllabusScraper._is_junk_url(
            "https://example.edu/past-exams/midterm.pdf"
        )

    def test_rejects_exams_path(self) -> None:
        assert SyllabusScraper._is_junk_url(
            "https://example.edu/exams/final2024.pdf"
        )

    def test_rejects_ppt_extension(self) -> None:
        assert SyllabusScraper._is_junk_url(
            "https://example.edu/slides/overview.ppt"
        )

    def test_rejects_pptx_extension(self) -> None:
        assert SyllabusScraper._is_junk_url(
            "https://example.edu/slides/overview.pptx"
        )

    def test_accepts_syllabus_pdf(self) -> None:
        assert not SyllabusScraper._is_junk_url(
            "https://example.edu/syllabi/cs101.pdf"
        )

    def test_accepts_course_pdf(self) -> None:
        assert not SyllabusScraper._is_junk_url(
            "https://example.edu/courses/cs201/outline.pdf"
        )

    def test_rejects_homework(self) -> None:
        assert SyllabusScraper._is_junk_url(
            "https://example.edu/homework/hw1.pdf"
        )

    def test_rejects_solutions(self) -> None:
        assert SyllabusScraper._is_junk_url(
            "https://example.edu/solutions/sol1.pdf"
        )

    def test_rejects_quizzes(self) -> None:
        assert SyllabusScraper._is_junk_url(
            "https://example.edu/quizzes/quiz1.pdf"
        )


# ------------------------------------------------------------------
# Tests — BFS fragment stripping
# ------------------------------------------------------------------


class TestBfsFragmentStripping:
    """Test that URL fragments are stripped in BFS."""

    def test_fragments_collapsed_to_one_page(
        self,
        mock_http_client: MagicMock,
        school: School,
    ) -> None:
        """page.html#overview and page.html#goals count as 1 page."""
        scraper = SyllabusScraper(http_client=mock_http_client, config={})

        page_html = """
        <html><body>
        <a href="/about">About Page</a>
        </body></html>
        """
        mock_response = MagicMock()
        mock_response.text = page_html
        mock_http_client.get.return_value = mock_response

        # Feed two URLs that differ only by fragment
        result, stats = scraper._follow_syllabus_pages(
            [
                "https://www.mit.edu/page.html#overview",
                "https://www.mit.edu/page.html#goals",
            ],
            school,
            max_followed=20,
        )

        # Should only fetch once (fragments stripped → same URL)
        assert mock_http_client.get.call_count == 1
        assert stats.pages_followed == 1


# ------------------------------------------------------------------
# Tests — BFS sub-page skip filtering
# ------------------------------------------------------------------


class TestBfsSkipSubpages:
    """Test that irrelevant sub-pages are skipped in BFS queue."""

    def test_skips_labs_calendar_staff(
        self,
        mock_http_client: MagicMock,
        school: School,
    ) -> None:
        """/labs/, /calendar/, /staff/ not added to BFS queue."""
        scraper = SyllabusScraper(http_client=mock_http_client, config={})

        seed_html = """
        <html><body>
        <a href="/labs/1/">Lab 1</a>
        <a href="/calendar/">Calendar</a>
        <a href="/staff/">Staff</a>
        <a href="/syllabi/archive">View Syllabi</a>
        </body></html>
        """
        archive_html = """
        <html><body>
        <a href="/about">About the department</a>
        </body></html>
        """

        responses = []
        for html in [seed_html, archive_html]:
            r = MagicMock()
            r.text = html
            responses.append(r)
        mock_http_client.get.side_effect = responses

        result, stats = scraper._follow_syllabus_pages(
            ["https://www.mit.edu/course/"],
            school,
            max_followed=20,
            max_depth=2,
        )

        # Only 2 fetches: seed + syllabi/archive (not labs, calendar, staff)
        assert mock_http_client.get.call_count == 2
        # Verify the fetched URLs don't include labs/calendar/staff
        fetched_urls = [c.args[0] for c in mock_http_client.get.call_args_list]
        assert not any("/labs/" in u for u in fetched_urls)
        assert not any("/calendar/" in u for u in fetched_urls)
        assert not any("/staff/" in u for u in fetched_urls)


# ------------------------------------------------------------------
# Tests — Per-page file cap
# ------------------------------------------------------------------


class TestBfsPerPageFileCap:
    """Test that per-page file cap limits collection from a single page."""

    def test_caps_files_from_single_page(
        self,
        mock_http_client: MagicMock,
        school: School,
    ) -> None:
        """Page with 200 file links only yields max_files_per_page."""
        scraper = SyllabusScraper(
            http_client=mock_http_client,
            config={"syllabus_max_files_per_page": 10},
        )

        # Page at depth 1 with 200 PDF links (broad extraction)
        links = "\n".join(
            f'<a href="/files/doc{i}.pdf">Doc {i}</a>' for i in range(200)
        )
        seed_html = """
        <html><body>
        <a href="/archive/syllabi">View Syllabi</a>
        </body></html>
        """
        archive_html = f"<html><body>{links}</body></html>"

        responses = []
        for html in [seed_html, archive_html]:
            r = MagicMock()
            r.text = html
            responses.append(r)
        mock_http_client.get.side_effect = responses

        result, _ = scraper._follow_syllabus_pages(
            ["https://www.mit.edu/syllabi"],
            school,
            max_followed=50,
            max_depth=2,
        )

        # Should be capped at 10
        assert len(result) == 10


# ------------------------------------------------------------------
# Tests — Junk filtering in download loop
# ------------------------------------------------------------------


class TestScrapeFiltersJunk:
    """Test that junk URLs are filtered in the download loop."""

    def test_junk_urls_skipped_in_download(
        self,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        """Junk URLs skipped in download loop, counted in stats."""
        metadata = SchoolMetadata(school_dir)
        metadata._metadata["phases"] = {
            "discovery": {
                "syllabus_urls": [
                    "https://www.mit.edu/syllabi/cs101.pdf",
                    "https://www.mit.edu/lectures/lecture1.ppt",
                    "https://www.mit.edu/past-exams/midterm.pdf",
                ],
            },
        }
        mock_http_client.download.return_value = Path("dummy.pdf")

        scraper = SyllabusScraper(http_client=mock_http_client, config={})
        scraper.scrape(school, school_dir, metadata)

        # Only cs101.pdf should be downloaded (2 junk filtered)
        assert mock_http_client.download.call_count == 1
        called_url = mock_http_client.download.call_args.args[0]
        assert "cs101.pdf" in called_url

        syllabi = metadata._metadata["phases"]["syllabi"]
        assert syllabi["files_filtered"] == 2
        assert syllabi["files_downloaded"] == 1

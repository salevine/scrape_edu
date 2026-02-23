"""Tests for scrape_edu.scrapers.faculty_scraper module."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, PropertyMock

import pytest

from scrape_edu.data.manifest import SchoolMetadata
from scrape_edu.data.models import FacultyMember
from scrape_edu.data.school import School
from scrape_edu.net.http_client import HttpClient
from scrape_edu.scrapers.faculty_scraper import FacultyScraper


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture()
def mock_http_client() -> MagicMock:
    """Return a mocked HttpClient."""
    return MagicMock(spec=HttpClient)


@pytest.fixture()
def scraper(mock_http_client: MagicMock) -> FacultyScraper:
    """Return a FacultyScraper instance with a mocked client."""
    return FacultyScraper(http_client=mock_http_client, config={})


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
    """Return a SchoolMetadata instance with faculty URLs pre-populated."""
    meta = SchoolMetadata(school_dir)
    meta._metadata["phases"] = {
        "discovery": {
            "faculty_urls": [
                "https://www.mit.edu/faculty/directory",
                "https://www.mit.edu/faculty/cs-people",
            ],
        },
    }
    return meta


# ------------------------------------------------------------------
# HTML fixtures for parsing tests
# ------------------------------------------------------------------

FACULTY_CARD_HTML = """
<html>
<body>
<div class="faculty-member">
    <h3 class="name">Dr. Jane Smith</h3>
    <span class="title">Associate Professor</span>
    <a href="mailto:jsmith@mit.edu">jsmith@mit.edu</a>
    <a href="/faculty/jsmith">Profile</a>
</div>
<div class="faculty-member">
    <h3 class="name">Dr. John Doe</h3>
    <span class="position">Professor</span>
    <a href="mailto:jdoe@mit.edu">jdoe@mit.edu</a>
    <a href="/faculty/jdoe">Profile</a>
</div>
</body>
</html>
"""

FACULTY_LIST_HTML = """
<html>
<body>
<div class="faculty">
    <a href="/people/alice-jones">Alice Jones</a>
    <a href="/people/bob-chen">Bob Chen</a>
    <a href="/people/carol-williams">Carol Williams</a>
</div>
</body>
</html>
"""

UNPARSEABLE_HTML = """
<html>
<body>
<p>Welcome to our department.</p>
<img src="photo.jpg" alt="Campus" />
</body>
</html>
"""


# ------------------------------------------------------------------
# Tests — scrape() method
# ------------------------------------------------------------------


class TestFacultyScraperScrape:
    """Test the main scrape() method."""

    def test_downloads_html_from_faculty_urls(
        self,
        scraper: FacultyScraper,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
        metadata: SchoolMetadata,
    ) -> None:
        """scrape() calls client.get() for each faculty URL."""
        response = MagicMock()
        response.text = "<html><body>Faculty</body></html>"
        mock_http_client.get.return_value = response

        scraper.scrape(school, school_dir, metadata)

        assert mock_http_client.get.call_count == 2
        called_urls = [call.args[0] for call in mock_http_client.get.call_args_list]
        assert "https://www.mit.edu/faculty/directory" in called_urls
        assert "https://www.mit.edu/faculty/cs-people" in called_urls

    def test_saves_raw_html_files(
        self,
        scraper: FacultyScraper,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
        metadata: SchoolMetadata,
    ) -> None:
        """scrape() saves the downloaded HTML to the faculty directory."""
        response = MagicMock()
        response.text = "<html><body>Faculty page</body></html>"
        mock_http_client.get.return_value = response

        scraper.scrape(school, school_dir, metadata)

        faculty_dir = school_dir / "faculty"
        html_files = list(faculty_dir.glob("*.html"))
        assert len(html_files) == 2
        for f in html_files:
            assert f.read_text(encoding="utf-8") == "<html><body>Faculty page</body></html>"

    def test_saves_json_when_parse_succeeds(
        self,
        scraper: FacultyScraper,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
        metadata: SchoolMetadata,
    ) -> None:
        """scrape() saves parsed JSON alongside HTML when parsing works."""
        response = MagicMock()
        response.text = FACULTY_CARD_HTML
        mock_http_client.get.return_value = response

        # Use only one URL for simplicity
        metadata._metadata["phases"]["discovery"]["faculty_urls"] = [
            "https://www.mit.edu/faculty/directory"
        ]

        scraper.scrape(school, school_dir, metadata)

        faculty_dir = school_dir / "faculty"
        json_files = list(faculty_dir.glob("*.json"))
        assert len(json_files) == 1
        data = json.loads(json_files[0].read_text(encoding="utf-8"))
        assert isinstance(data, list)
        assert len(data) == 2
        assert data[0]["name"] == "Dr. Jane Smith"
        assert data[1]["name"] == "Dr. John Doe"

    def test_keeps_html_when_json_parse_fails(
        self,
        scraper: FacultyScraper,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
        metadata: SchoolMetadata,
    ) -> None:
        """scrape() keeps the raw HTML even if JSON parsing fails."""
        response = MagicMock()
        response.text = UNPARSEABLE_HTML
        mock_http_client.get.return_value = response

        metadata._metadata["phases"]["discovery"]["faculty_urls"] = [
            "https://www.mit.edu/faculty/directory"
        ]

        scraper.scrape(school, school_dir, metadata)

        faculty_dir = school_dir / "faculty"
        html_files = list(faculty_dir.glob("*.html"))
        json_files = list(faculty_dir.glob("*.json"))
        assert len(html_files) == 1
        assert len(json_files) == 0  # No JSON saved for unparseable pages
        assert html_files[0].read_text(encoding="utf-8") == UNPARSEABLE_HTML

    def test_skips_already_downloaded_urls(
        self,
        scraper: FacultyScraper,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
        metadata: SchoolMetadata,
    ) -> None:
        """scrape() skips URLs that were already downloaded."""
        # Mark first URL as downloaded
        metadata.add_downloaded_url(
            "https://www.mit.edu/faculty/directory",
            "faculty/directory.html",
        )

        response = MagicMock()
        response.text = "<html><body>Faculty</body></html>"
        mock_http_client.get.return_value = response

        scraper.scrape(school, school_dir, metadata)

        # Only the second URL should be fetched
        assert mock_http_client.get.call_count == 1
        assert mock_http_client.get.call_args.args[0] == "https://www.mit.edu/faculty/cs-people"

    def test_handles_download_errors_gracefully(
        self,
        scraper: FacultyScraper,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
        metadata: SchoolMetadata,
    ) -> None:
        """scrape() continues processing URLs even when one download fails."""
        responses = [
            Exception("Connection refused"),
            MagicMock(text="<html><body>OK</body></html>"),
        ]
        mock_http_client.get.side_effect = responses

        scraper.scrape(school, school_dir, metadata)

        # Both URLs attempted
        assert mock_http_client.get.call_count == 2
        # Second URL should produce an HTML file
        faculty_dir = school_dir / "faculty"
        html_files = list(faculty_dir.glob("*.html"))
        assert len(html_files) == 1

    def test_no_faculty_urls_does_nothing(
        self,
        scraper: FacultyScraper,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
    ) -> None:
        """scrape() does nothing when no faculty URLs are found."""
        metadata = SchoolMetadata(school_dir)
        # No discovery phase data at all

        scraper.scrape(school, school_dir, metadata)

        assert mock_http_client.get.call_count == 0

    def test_tracks_downloaded_urls_in_metadata(
        self,
        scraper: FacultyScraper,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
        metadata: SchoolMetadata,
    ) -> None:
        """scrape() records each downloaded URL in metadata."""
        response = MagicMock()
        response.text = "<html><body>Faculty</body></html>"
        mock_http_client.get.return_value = response

        scraper.scrape(school, school_dir, metadata)

        assert metadata.is_url_downloaded("https://www.mit.edu/faculty/directory")
        assert metadata.is_url_downloaded("https://www.mit.edu/faculty/cs-people")

    def test_creates_faculty_directory(
        self,
        scraper: FacultyScraper,
        mock_http_client: MagicMock,
        school: School,
        school_dir: Path,
        metadata: SchoolMetadata,
    ) -> None:
        """scrape() creates the faculty/ subdirectory."""
        response = MagicMock()
        response.text = "<html><body>Faculty</body></html>"
        mock_http_client.get.return_value = response

        scraper.scrape(school, school_dir, metadata)

        assert (school_dir / "faculty").is_dir()


# ------------------------------------------------------------------
# Tests — _get_faculty_urls()
# ------------------------------------------------------------------


class TestGetFacultyUrls:
    """Test the _get_faculty_urls helper."""

    def test_returns_urls_from_metadata(
        self, scraper: FacultyScraper, metadata: SchoolMetadata
    ) -> None:
        urls = scraper._get_faculty_urls(metadata)
        assert urls == [
            "https://www.mit.edu/faculty/directory",
            "https://www.mit.edu/faculty/cs-people",
        ]

    def test_returns_empty_list_when_no_discovery(
        self, scraper: FacultyScraper, school_dir: Path
    ) -> None:
        metadata = SchoolMetadata(school_dir)
        assert scraper._get_faculty_urls(metadata) == []

    def test_returns_empty_list_when_not_a_list(
        self, scraper: FacultyScraper, school_dir: Path
    ) -> None:
        metadata = SchoolMetadata(school_dir)
        metadata._metadata["phases"] = {
            "discovery": {"faculty_urls": "not-a-list"},
        }
        assert scraper._get_faculty_urls(metadata) == []


# ------------------------------------------------------------------
# Tests — _parse_faculty_page()
# ------------------------------------------------------------------


class TestParseFacultyPage:
    """Test the _parse_faculty_page parser."""

    def test_extracts_members_from_card_pattern(
        self, scraper: FacultyScraper
    ) -> None:
        members = scraper._parse_faculty_page(
            FACULTY_CARD_HTML, "https://www.mit.edu/faculty"
        )
        assert len(members) == 2
        assert members[0].name == "Dr. Jane Smith"
        assert members[1].name == "Dr. John Doe"

    def test_extracts_members_from_list_pattern(
        self, scraper: FacultyScraper
    ) -> None:
        members = scraper._parse_faculty_page(
            FACULTY_LIST_HTML, "https://www.mit.edu/faculty"
        )
        assert len(members) == 3
        names = [m.name for m in members]
        assert "Alice Jones" in names
        assert "Bob Chen" in names
        assert "Carol Williams" in names

    def test_returns_empty_list_for_unparseable_html(
        self, scraper: FacultyScraper
    ) -> None:
        members = scraper._parse_faculty_page(
            UNPARSEABLE_HTML, "https://www.mit.edu/faculty"
        )
        assert members == []

    def test_extracts_profile_urls_from_list(
        self, scraper: FacultyScraper
    ) -> None:
        members = scraper._parse_faculty_page(
            FACULTY_LIST_HTML, "https://www.mit.edu/faculty"
        )
        profile_urls = [m.profile_url for m in members]
        assert "/people/alice-jones" in profile_urls
        assert "/people/bob-chen" in profile_urls

    def test_ignores_single_word_names_in_list(
        self, scraper: FacultyScraper
    ) -> None:
        """Links with single-word text are not treated as names."""
        html = """
        <html><body>
        <div class="faculty">
            <a href="/home">Home</a>
            <a href="/people/jane-doe">Jane Doe</a>
        </div>
        </body></html>
        """
        members = scraper._parse_faculty_page(html, "https://example.edu")
        assert len(members) == 1
        assert members[0].name == "Jane Doe"


# ------------------------------------------------------------------
# Tests — _parse_card()
# ------------------------------------------------------------------


class TestParseCard:
    """Test the _parse_card helper."""

    def test_extracts_name_title_email_profile(
        self, scraper: FacultyScraper
    ) -> None:
        from bs4 import BeautifulSoup

        html = """
        <div class="faculty-member">
            <h3 class="name">Dr. Alice Wong</h3>
            <span class="title">Assistant Professor</span>
            <a href="mailto:awong@example.edu">awong@example.edu</a>
            <a href="/people/awong">Profile</a>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        card = soup.select_one("div.faculty-member")
        member = scraper._parse_card(card, "https://example.edu")

        assert member is not None
        assert member.name == "Dr. Alice Wong"
        assert member.title == "Assistant Professor"
        assert member.email == "awong@example.edu"
        assert member.profile_url == "mailto:awong@example.edu"

    def test_returns_none_when_no_name_found(
        self, scraper: FacultyScraper
    ) -> None:
        from bs4 import BeautifulSoup

        html = """
        <div class="faculty-member">
            <span class="department">Computer Science</span>
            <img src="photo.jpg" />
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        card = soup.select_one("div.faculty-member")
        assert scraper._parse_card(card, "https://example.edu") is None

    def test_returns_none_for_short_name(
        self, scraper: FacultyScraper
    ) -> None:
        from bs4 import BeautifulSoup

        html = """
        <div class="faculty-member">
            <h3>A</h3>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        card = soup.select_one("div.faculty-member")
        assert scraper._parse_card(card, "https://example.edu") is None

    def test_uses_heading_when_no_name_class(
        self, scraper: FacultyScraper
    ) -> None:
        from bs4 import BeautifulSoup

        html = """
        <div class="faculty-member">
            <h4>Bob Smith</h4>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        card = soup.select_one("div.faculty-member")
        member = scraper._parse_card(card, "https://example.edu")
        assert member is not None
        assert member.name == "Bob Smith"

    def test_extracts_position_from_role_class(
        self, scraper: FacultyScraper
    ) -> None:
        from bs4 import BeautifulSoup

        html = """
        <div class="faculty-member">
            <h3>Carol Davis</h3>
            <span class="role">Lecturer</span>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        card = soup.select_one("div.faculty-member")
        member = scraper._parse_card(card, "https://example.edu")
        assert member is not None
        assert member.title == "Lecturer"


# ------------------------------------------------------------------
# Tests — _url_to_filename()
# ------------------------------------------------------------------


class TestUrlToFilename:
    """Test the _url_to_filename helper."""

    def test_generates_name_from_url_path(
        self, scraper: FacultyScraper
    ) -> None:
        result = scraper._url_to_filename(
            "https://example.edu/faculty/directory", ".html"
        )
        assert result == "faculty-directory.html"

    def test_strips_existing_extension(
        self, scraper: FacultyScraper
    ) -> None:
        result = scraper._url_to_filename(
            "https://example.edu/faculty/people.php", ".html"
        )
        assert result == "faculty-people.html"

    def test_uses_domain_when_no_path(
        self, scraper: FacultyScraper
    ) -> None:
        result = scraper._url_to_filename("https://example.edu/", ".html")
        assert result == "example-edu.html"

    def test_sanitizes_special_characters(
        self, scraper: FacultyScraper
    ) -> None:
        result = scraper._url_to_filename(
            "https://example.edu/faculty/people list&more", ".html"
        )
        assert result == "faculty-people-list-more.html"
        # No special chars in result
        base = result.replace(".html", "")
        assert all(c.isalnum() or c in "-_" for c in base)

    def test_defaults_to_faculty_when_empty(
        self, scraper: FacultyScraper
    ) -> None:
        # URL with path that becomes empty after sanitization
        result = scraper._url_to_filename("https://example.edu", ".json")
        assert result == "example-edu.json"

    def test_json_extension(self, scraper: FacultyScraper) -> None:
        result = scraper._url_to_filename(
            "https://example.edu/faculty/directory", ".json"
        )
        assert result == "faculty-directory.json"

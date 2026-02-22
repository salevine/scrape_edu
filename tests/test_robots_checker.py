"""Tests for scrape_edu.scrapers.robots_checker module."""

from __future__ import annotations

from unittest.mock import MagicMock, PropertyMock

import pytest
import requests

from scrape_edu.scrapers.robots_checker import RobotsChecker


def _make_checker(response_text: str | None = None, side_effect: Exception | None = None):
    """Create a RobotsChecker with a mocked HttpClient."""
    client = MagicMock()
    if side_effect:
        client.get.side_effect = side_effect
    elif response_text is not None:
        mock_resp = MagicMock()
        mock_resp.text = response_text
        client.get.return_value = mock_resp
    return RobotsChecker(client), client


class TestRobotsCheckerExists:
    """Test robots.txt detection."""

    def test_exists_when_found(self) -> None:
        checker, _ = _make_checker("User-agent: *\nDisallow: /private")
        result = checker.check("https://example.com")
        assert result["exists"] is True

    def test_not_exists_on_http_error(self) -> None:
        error = requests.HTTPError("404 Not Found")
        checker, _ = _make_checker(side_effect=error)
        result = checker.check("https://example.com")
        assert result["exists"] is False

    def test_not_exists_on_connection_error(self) -> None:
        error = requests.ConnectionError("Connection refused")
        checker, _ = _make_checker(side_effect=error)
        result = checker.check("https://example.com")
        assert result["exists"] is False

    def test_content_stored_when_exists(self) -> None:
        content = "User-agent: *\nDisallow: /admin"
        checker, _ = _make_checker(content)
        result = checker.check("https://example.com")
        assert result["content"] == content

    def test_content_none_when_missing(self) -> None:
        checker, _ = _make_checker(side_effect=requests.HTTPError("404"))
        result = checker.check("https://example.com")
        assert result["content"] is None


class TestRobotsCheckerUrl:
    """Test robots.txt URL construction."""

    def test_url_from_base(self) -> None:
        checker, client = _make_checker("User-agent: *")
        checker.check("https://example.com")
        client.get.assert_called_once_with("https://example.com/robots.txt")

    def test_url_from_base_with_trailing_slash(self) -> None:
        checker, client = _make_checker("User-agent: *")
        checker.check("https://example.com/")
        client.get.assert_called_once_with("https://example.com/robots.txt")

    def test_url_from_base_with_path(self) -> None:
        checker, client = _make_checker("User-agent: *")
        checker.check("https://example.com/some/path")
        # urljoin should produce robots.txt at the root
        call_url = client.get.call_args[0][0]
        assert "robots.txt" in call_url


class TestRobotsCheckerDisallows:
    """Test Disallow pattern extraction."""

    def test_single_disallow(self) -> None:
        checker, _ = _make_checker("User-agent: *\nDisallow: /private")
        result = checker.check("https://example.com")
        assert result["disallow_patterns"] == ["/private"]

    def test_multiple_disallows(self) -> None:
        content = "User-agent: *\nDisallow: /admin\nDisallow: /tmp\nDisallow: /secret"
        checker, _ = _make_checker(content)
        result = checker.check("https://example.com")
        assert result["disallow_patterns"] == ["/admin", "/tmp", "/secret"]

    def test_empty_disallow_filtered(self) -> None:
        content = "User-agent: *\nDisallow:\nDisallow: /admin"
        checker, _ = _make_checker(content)
        result = checker.check("https://example.com")
        assert result["disallow_patterns"] == ["/admin"]

    def test_case_insensitive(self) -> None:
        content = "User-agent: *\nDISALLOW: /test\ndisallow: /other"
        checker, _ = _make_checker(content)
        result = checker.check("https://example.com")
        assert result["disallow_patterns"] == ["/test", "/other"]

    def test_no_disallows(self) -> None:
        checker, _ = _make_checker("User-agent: *\nAllow: /")
        result = checker.check("https://example.com")
        assert result["disallow_patterns"] == []

    def test_multiple_user_agents(self) -> None:
        content = (
            "User-agent: Googlebot\n"
            "Disallow: /nogoogle\n"
            "\n"
            "User-agent: *\n"
            "Disallow: /private\n"
        )
        checker, _ = _make_checker(content)
        result = checker.check("https://example.com")
        assert "/nogoogle" in result["disallow_patterns"]
        assert "/private" in result["disallow_patterns"]


class TestRobotsCheckerCrawlDelay:
    """Test Crawl-delay extraction."""

    def test_crawl_delay_present(self) -> None:
        content = "User-agent: *\nCrawl-delay: 10"
        checker, _ = _make_checker(content)
        result = checker.check("https://example.com")
        assert result["crawl_delay"] == 10.0

    def test_crawl_delay_float(self) -> None:
        content = "User-agent: *\nCrawl-delay: 2.5"
        checker, _ = _make_checker(content)
        result = checker.check("https://example.com")
        assert result["crawl_delay"] == 2.5

    def test_crawl_delay_missing(self) -> None:
        content = "User-agent: *\nDisallow: /admin"
        checker, _ = _make_checker(content)
        result = checker.check("https://example.com")
        assert result["crawl_delay"] is None

    def test_crawl_delay_invalid(self) -> None:
        content = "User-agent: *\nCrawl-delay: abc"
        checker, _ = _make_checker(content)
        result = checker.check("https://example.com")
        assert result["crawl_delay"] is None

    def test_crawl_delay_case_insensitive(self) -> None:
        content = "User-agent: *\ncrawl-delay: 5"
        checker, _ = _make_checker(content)
        result = checker.check("https://example.com")
        assert result["crawl_delay"] == 5.0


class TestRobotsCheckerSitemaps:
    """Test Sitemap URL extraction."""

    def test_single_sitemap(self) -> None:
        content = "Sitemap: https://example.com/sitemap.xml"
        checker, _ = _make_checker(content)
        result = checker.check("https://example.com")
        assert result["sitemaps"] == ["https://example.com/sitemap.xml"]

    def test_multiple_sitemaps(self) -> None:
        content = (
            "Sitemap: https://example.com/sitemap1.xml\n"
            "Sitemap: https://example.com/sitemap2.xml\n"
        )
        checker, _ = _make_checker(content)
        result = checker.check("https://example.com")
        assert len(result["sitemaps"]) == 2

    def test_no_sitemaps(self) -> None:
        content = "User-agent: *\nDisallow: /admin"
        checker, _ = _make_checker(content)
        result = checker.check("https://example.com")
        assert result["sitemaps"] == []

    def test_sitemap_case_insensitive(self) -> None:
        content = "sitemap: https://example.com/sitemap.xml"
        checker, _ = _make_checker(content)
        result = checker.check("https://example.com")
        assert result["sitemaps"] == ["https://example.com/sitemap.xml"]


class TestRobotsCheckerEmpty:
    """Test edge cases."""

    def test_empty_robots_txt(self) -> None:
        checker, _ = _make_checker("")
        result = checker.check("https://example.com")
        assert result["exists"] is True
        assert result["disallow_patterns"] == []
        assert result["crawl_delay"] is None
        assert result["sitemaps"] == []

    def test_comments_only(self) -> None:
        content = "# This is a comment\n# Another comment"
        checker, _ = _make_checker(content)
        result = checker.check("https://example.com")
        assert result["exists"] is True
        assert result["disallow_patterns"] == []

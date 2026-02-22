"""Tests for scrape_edu.discovery.homepage_crawler -- HttpClient is fully mocked."""

from __future__ import annotations

from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from scrape_edu.discovery.homepage_crawler import HomepageCrawler
from scrape_edu.discovery.url_classifier import UrlCategory


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _make_response(html: str, status_code: int = 200) -> MagicMock:
    """Create a mock requests.Response with the given HTML body."""
    resp = MagicMock()
    resp.text = html
    resp.status_code = status_code
    return resp


def _simple_html(title: str, links: list[str] | None = None) -> str:
    """Build a minimal HTML page with a title and optional links."""
    link_tags = ""
    if links:
        link_tags = "\n".join(f'<a href="{url}">Link</a>' for url in links)
    return f"<html><head><title>{title}</title></head><body>{link_tags}</body></html>"


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------


class TestBFSOrder:
    def test_visits_pages_in_bfs_order(self) -> None:
        """Start page -> page A and B -> page C (linked from A)."""
        mock_client = MagicMock()

        start_html = _simple_html("Start", [
            "https://school.edu/pageA",
            "https://school.edu/pageB",
        ])
        page_a_html = _simple_html("Page A", [
            "https://school.edu/pageC",
        ])
        page_b_html = _simple_html("Page B", [])
        page_c_html = _simple_html("Page C", [])

        mock_client.get.side_effect = [
            _make_response(start_html),
            _make_response(page_a_html),
            _make_response(page_b_html),
            _make_response(page_c_html),
        ]

        crawler = HomepageCrawler(mock_client)
        results = crawler.crawl("https://school.edu", max_pages=10, max_depth=3)

        urls = [r["url"] for r in results]
        assert urls[0] == "https://school.edu"
        # A and B should come before C (BFS)
        assert "https://school.edu/pageA" in urls
        assert "https://school.edu/pageB" in urls
        idx_a = urls.index("https://school.edu/pageA")
        idx_b = urls.index("https://school.edu/pageB")
        idx_c = urls.index("https://school.edu/pageC")
        assert idx_a < idx_c
        assert idx_b < idx_c


class TestMaxPages:
    def test_respects_max_pages_limit(self) -> None:
        mock_client = MagicMock()

        # Start page links to many sub-pages
        links = [f"https://school.edu/page{i}" for i in range(20)]
        start_html = _simple_html("Start", links)
        sub_html = _simple_html("Sub", [])

        mock_client.get.side_effect = [_make_response(start_html)] + [
            _make_response(sub_html) for _ in range(20)
        ]

        crawler = HomepageCrawler(mock_client)
        results = crawler.crawl("https://school.edu", max_pages=5, max_depth=3)

        assert len(results) == 5

    def test_max_pages_one(self) -> None:
        mock_client = MagicMock()
        start_html = _simple_html("Start", ["https://school.edu/page1"])
        mock_client.get.return_value = _make_response(start_html)

        crawler = HomepageCrawler(mock_client)
        results = crawler.crawl("https://school.edu", max_pages=1, max_depth=3)

        assert len(results) == 1
        assert results[0]["url"] == "https://school.edu"


class TestMaxDepth:
    def test_respects_max_depth_limit(self) -> None:
        mock_client = MagicMock()

        # Chain: start -> depth1 -> depth2 -> depth3 (should NOT be followed at max_depth=2)
        start_html = _simple_html("Start", ["https://school.edu/depth1"])
        depth1_html = _simple_html("Depth 1", ["https://school.edu/depth2"])
        depth2_html = _simple_html("Depth 2", ["https://school.edu/depth3"])
        depth3_html = _simple_html("Depth 3", [])

        mock_client.get.side_effect = [
            _make_response(start_html),
            _make_response(depth1_html),
            _make_response(depth2_html),
            _make_response(depth3_html),
        ]

        crawler = HomepageCrawler(mock_client)
        results = crawler.crawl("https://school.edu", max_pages=50, max_depth=2)

        urls = [r["url"] for r in results]
        assert "https://school.edu" in urls
        assert "https://school.edu/depth1" in urls
        assert "https://school.edu/depth2" in urls
        # depth3 should NOT be visited because depth2 is at depth=2 and max_depth=2
        assert "https://school.edu/depth3" not in urls

    def test_depth_zero_only_visits_start(self) -> None:
        mock_client = MagicMock()
        start_html = _simple_html("Start", ["https://school.edu/page1"])
        mock_client.get.return_value = _make_response(start_html)

        crawler = HomepageCrawler(mock_client)
        results = crawler.crawl("https://school.edu", max_pages=50, max_depth=0)

        assert len(results) == 1
        assert results[0]["url"] == "https://school.edu"


class TestSameDomain:
    def test_only_follows_same_domain_links(self) -> None:
        mock_client = MagicMock()

        start_html = _simple_html("Start", [
            "https://school.edu/internal",
            "https://external.com/page",
            "https://school.edu/another",
        ])
        internal_html = _simple_html("Internal", [])
        another_html = _simple_html("Another", [])

        mock_client.get.side_effect = [
            _make_response(start_html),
            _make_response(internal_html),
            _make_response(another_html),
        ]

        crawler = HomepageCrawler(mock_client)
        results = crawler.crawl("https://school.edu", max_pages=50, max_depth=3)

        urls = [r["url"] for r in results]
        assert "https://external.com/page" not in urls
        assert "https://school.edu/internal" in urls
        assert "https://school.edu/another" in urls


class TestDeduplication:
    def test_deduplicates_urls(self) -> None:
        mock_client = MagicMock()

        start_html = _simple_html("Start", [
            "https://school.edu/page",
            "https://school.edu/page",
            "https://school.edu/page#section",  # normalizes to same URL
        ])
        page_html = _simple_html("Page", [])

        mock_client.get.side_effect = [
            _make_response(start_html),
            _make_response(page_html),
        ]

        crawler = HomepageCrawler(mock_client)
        results = crawler.crawl("https://school.edu", max_pages=50, max_depth=3)

        urls = [r["url"] for r in results]
        # /page should appear only once
        assert urls.count("https://school.edu/page") == 1

    def test_does_not_revisit_start_url(self) -> None:
        mock_client = MagicMock()

        # Page links back to start
        start_html = _simple_html("Start", ["https://school.edu/page"])
        page_html = _simple_html("Page", ["https://school.edu"])

        mock_client.get.side_effect = [
            _make_response(start_html),
            _make_response(page_html),
        ]

        crawler = HomepageCrawler(mock_client)
        results = crawler.crawl("https://school.edu", max_pages=50, max_depth=3)

        urls = [r["url"] for r in results]
        assert urls.count("https://school.edu") == 1


class TestErrorHandling:
    def test_skips_failed_page_and_continues(self) -> None:
        mock_client = MagicMock()

        start_html = _simple_html("Start", [
            "https://school.edu/bad",
            "https://school.edu/good",
        ])
        good_html = _simple_html("Good Page", [])

        mock_client.get.side_effect = [
            _make_response(start_html),
            Exception("Connection error"),  # bad page fails
            _make_response(good_html),
        ]

        crawler = HomepageCrawler(mock_client)
        results = crawler.crawl("https://school.edu", max_pages=50, max_depth=3)

        urls = [r["url"] for r in results]
        assert "https://school.edu" in urls
        assert "https://school.edu/good" in urls
        # bad page should not be in results (it errored)
        assert "https://school.edu/bad" not in urls

    def test_error_does_not_crash_crawl(self) -> None:
        mock_client = MagicMock()
        mock_client.get.side_effect = Exception("Total failure")

        crawler = HomepageCrawler(mock_client)
        results = crawler.crawl("https://school.edu", max_pages=50, max_depth=3)

        assert results == []


class TestTitleExtraction:
    def test_extracts_title_from_html(self) -> None:
        mock_client = MagicMock()
        html = _simple_html("Computer Science Department", [])
        mock_client.get.return_value = _make_response(html)

        crawler = HomepageCrawler(mock_client)
        results = crawler.crawl("https://school.edu", max_pages=1)

        assert results[0]["title"] == "Computer Science Department"

    def test_missing_title_returns_empty_string(self) -> None:
        mock_client = MagicMock()
        html = "<html><body><p>No title here</p></body></html>"
        mock_client.get.return_value = _make_response(html)

        crawler = HomepageCrawler(mock_client)
        results = crawler.crawl("https://school.edu", max_pages=1)

        assert results[0]["title"] == ""


class TestUrlClassification:
    def test_classifies_catalog_url(self) -> None:
        mock_client = MagicMock()
        html = _simple_html("Course Catalog", [])
        mock_client.get.return_value = _make_response(html)

        crawler = HomepageCrawler(mock_client)
        results = crawler.crawl("https://school.edu/catalog", max_pages=1)

        assert results[0]["category"] == UrlCategory.CATALOG

    def test_classifies_faculty_url(self) -> None:
        mock_client = MagicMock()
        html = _simple_html("Our Faculty", [])
        mock_client.get.return_value = _make_response(html)

        crawler = HomepageCrawler(mock_client)
        results = crawler.crawl("https://school.edu/faculty", max_pages=1)

        assert results[0]["category"] == UrlCategory.FACULTY

    def test_classifies_unknown_url(self) -> None:
        mock_client = MagicMock()
        html = _simple_html("Home", [])
        mock_client.get.return_value = _make_response(html)

        crawler = HomepageCrawler(mock_client)
        results = crawler.crawl("https://school.edu/about", max_pages=1)

        assert results[0]["category"] == UrlCategory.UNKNOWN


class TestLinkExtraction:
    def test_extracts_absolute_links(self) -> None:
        mock_client = MagicMock()

        html = '<html><head><title>T</title></head><body><a href="https://school.edu/page">L</a></body></html>'
        mock_client.get.side_effect = [
            _make_response(html),
            _make_response(_simple_html("Page", [])),
        ]

        crawler = HomepageCrawler(mock_client)
        results = crawler.crawl("https://school.edu", max_pages=10, max_depth=1)

        urls = [r["url"] for r in results]
        assert "https://school.edu/page" in urls

    def test_resolves_relative_links(self) -> None:
        mock_client = MagicMock()

        html = '<html><head><title>T</title></head><body><a href="/relative/page">L</a></body></html>'
        mock_client.get.side_effect = [
            _make_response(html),
            _make_response(_simple_html("Relative", [])),
        ]

        crawler = HomepageCrawler(mock_client)
        results = crawler.crawl("https://school.edu", max_pages=10, max_depth=1)

        urls = [r["url"] for r in results]
        assert "https://school.edu/relative/page" in urls

    def test_skips_mailto_links(self) -> None:
        mock_client = MagicMock()

        html = '<html><head><title>T</title></head><body><a href="mailto:info@school.edu">Email</a></body></html>'
        mock_client.get.return_value = _make_response(html)

        crawler = HomepageCrawler(mock_client)
        results = crawler.crawl("https://school.edu", max_pages=10, max_depth=1)

        assert len(results) == 1  # only start page

    def test_skips_javascript_links(self) -> None:
        mock_client = MagicMock()

        html = '<html><head><title>T</title></head><body><a href="javascript:void(0)">JS</a></body></html>'
        mock_client.get.return_value = _make_response(html)

        crawler = HomepageCrawler(mock_client)
        results = crawler.crawl("https://school.edu", max_pages=10, max_depth=1)

        assert len(results) == 1  # only start page

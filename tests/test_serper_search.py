"""Tests for scrape_edu.discovery.serper_search -- all HTTP calls are mocked."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from scrape_edu.discovery.serper_search import SerperClient


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture()
def client() -> SerperClient:
    return SerperClient(api_key="test-api-key")


SAMPLE_ORGANIC = [
    {"title": "CS Courses", "link": "https://example.edu/cs/courses", "snippet": "CS catalog"},
    {"title": "DS Program", "link": "https://example.edu/ds", "snippet": "Data science"},
]


def _mock_response(json_data: dict, status_code: int = 200) -> MagicMock:
    """Create a mock requests.Response-like object."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


# ------------------------------------------------------------------
# search() tests
# ------------------------------------------------------------------


class TestSearch:
    @patch("scrape_edu.discovery.serper_search.requests.post")
    def test_sends_correct_headers_and_payload(self, mock_post: MagicMock, client: SerperClient) -> None:
        mock_post.return_value = _mock_response({"organic": []})

        client.search("test query", num_results=5)

        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers")
        assert headers["X-API-KEY"] == "test-api-key"
        assert headers["Content-Type"] == "application/json"

        json_body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert json_body == {"q": "test query", "num": 5}

    @patch("scrape_edu.discovery.serper_search.requests.post")
    def test_returns_organic_results(self, mock_post: MagicMock, client: SerperClient) -> None:
        mock_post.return_value = _mock_response({"organic": SAMPLE_ORGANIC})

        results = client.search("computer science courses")

        assert len(results) == 2
        assert results[0]["title"] == "CS Courses"
        assert results[1]["link"] == "https://example.edu/ds"

    @patch("scrape_edu.discovery.serper_search.requests.post")
    def test_returns_empty_list_when_no_organic_key(self, mock_post: MagicMock, client: SerperClient) -> None:
        mock_post.return_value = _mock_response({"searchParameters": {}})

        results = client.search("some query")
        assert results == []

    @patch("scrape_edu.discovery.serper_search.requests.post")
    def test_handles_http_error_gracefully(self, mock_post: MagicMock, client: SerperClient) -> None:
        mock_post.side_effect = requests.RequestException("Connection timeout")

        results = client.search("failing query")

        assert results == []

    @patch("scrape_edu.discovery.serper_search.requests.post")
    def test_handles_http_status_error(self, mock_post: MagicMock, client: SerperClient) -> None:
        resp = MagicMock()
        resp.raise_for_status.side_effect = requests.HTTPError("429 Too Many Requests")
        mock_post.return_value = resp

        results = client.search("rate limited query")
        assert results == []

    @patch("scrape_edu.discovery.serper_search.requests.post")
    def test_increments_queries_used_on_success(self, mock_post: MagicMock, client: SerperClient) -> None:
        mock_post.return_value = _mock_response({"organic": []})

        assert client.queries_used == 0
        client.search("q1")
        assert client.queries_used == 1
        client.search("q2")
        assert client.queries_used == 2

    @patch("scrape_edu.discovery.serper_search.requests.post")
    def test_increments_queries_used_on_failure(self, mock_post: MagicMock, client: SerperClient) -> None:
        mock_post.side_effect = requests.RequestException("fail")

        assert client.queries_used == 0
        client.search("bad query")
        assert client.queries_used == 1

    @patch("scrape_edu.discovery.serper_search.requests.post")
    def test_posts_to_correct_url(self, mock_post: MagicMock, client: SerperClient) -> None:
        mock_post.return_value = _mock_response({"organic": []})
        client.search("test")

        call_args = mock_post.call_args
        assert call_args[0][0] == "https://google.serper.dev/search"


# ------------------------------------------------------------------
# search_school() tests
# ------------------------------------------------------------------


class TestSearchSchool:
    @patch("scrape_edu.discovery.serper_search.requests.post")
    def test_makes_five_queries(self, mock_post: MagicMock, client: SerperClient) -> None:
        mock_post.return_value = _mock_response({"organic": SAMPLE_ORGANIC})

        client.search_school("MIT", "https://mit.edu")

        assert mock_post.call_count == 5
        assert client.queries_used == 5

    @patch("scrape_edu.discovery.serper_search.requests.post")
    def test_returns_all_result_keys(self, mock_post: MagicMock, client: SerperClient) -> None:
        mock_post.return_value = _mock_response({"organic": SAMPLE_ORGANIC})

        result = client.search_school("Stanford", "https://stanford.edu")

        assert "cs_results" in result
        assert "ds_results" in result
        assert "faculty_results" in result
        assert "site_catalog_results" in result
        assert "site_syllabus_results" in result
        assert len(result["cs_results"]) == 2
        assert len(result["ds_results"]) == 2

    @patch("scrape_edu.discovery.serper_search.requests.post")
    def test_uses_correct_query_strings(self, mock_post: MagicMock, client: SerperClient) -> None:
        mock_post.return_value = _mock_response({"organic": []})

        client.search_school("MIT", "https://mit.edu")

        queries = [call.kwargs["json"]["q"] for call in mock_post.call_args_list]
        assert queries[0] == "MIT computer science courses catalog"
        assert queries[1] == "MIT data science program courses catalog"
        assert queries[2] == "MIT computer science faculty directory"
        assert queries[3] == "site:mit.edu computer science course catalog"
        assert queries[4] == "site:mit.edu computer science syllabus"

    @patch("scrape_edu.discovery.serper_search.requests.post")
    def test_handles_partial_failure(self, mock_post: MagicMock, client: SerperClient) -> None:
        """If one query fails, the others should still return results."""
        good_resp = _mock_response({"organic": SAMPLE_ORGANIC})
        responses = [good_resp, requests.RequestException("fail"),
                     good_resp, good_resp, good_resp]
        mock_post.side_effect = responses

        result = client.search_school("MIT", "https://mit.edu")

        assert len(result["cs_results"]) == 2
        assert result["ds_results"] == []
        assert len(result["faculty_results"]) == 2


# ------------------------------------------------------------------
# Property tests
# ------------------------------------------------------------------


class TestProperties:
    def test_queries_used_starts_at_zero(self, client: SerperClient) -> None:
        assert client.queries_used == 0

    def test_queries_remaining_is_none(self, client: SerperClient) -> None:
        assert client.queries_remaining is None

    def test_queries_per_school_default(self) -> None:
        c = SerperClient(api_key="k")
        assert c.queries_per_school == 5

    def test_queries_per_school_custom(self) -> None:
        c = SerperClient(api_key="k", queries_per_school=4)
        assert c.queries_per_school == 4

    @patch("scrape_edu.discovery.serper_search.requests.post")
    def test_queries_used_tracks_across_methods(self, mock_post: MagicMock, client: SerperClient) -> None:
        mock_post.return_value = _mock_response({"organic": []})

        client.search("q1")
        client.search_school("X", "https://x.edu")

        assert client.queries_used == 6  # 1 + 5

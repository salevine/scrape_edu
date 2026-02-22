"""Tests for scrape_edu.utils.url_utils module."""

from __future__ import annotations

import pytest

from scrape_edu.utils.url_utils import extract_domain, is_same_domain, normalize_url


class TestNormalizeUrl:
    """Test URL normalization."""

    def test_strips_fragment(self) -> None:
        assert normalize_url("https://mit.edu/page#section") == "https://mit.edu/page"

    def test_lowercases_scheme(self) -> None:
        result = normalize_url("HTTP://MIT.EDU/page")
        assert result.startswith("http://")

    def test_lowercases_hostname(self) -> None:
        result = normalize_url("https://MIT.EDU/Page")
        assert "mit.edu" in result

    def test_preserves_path_case(self) -> None:
        # Path casing should be preserved (paths are case-sensitive)
        result = normalize_url("https://mit.edu/Page")
        assert result == "https://mit.edu/Page"

    def test_removes_trailing_slash(self) -> None:
        assert normalize_url("https://example.com/path/") == "https://example.com/path"

    def test_root_path_no_trailing_slash(self) -> None:
        result = normalize_url("https://example.com/")
        assert result == "https://example.com"

    def test_adds_https_when_no_scheme(self) -> None:
        assert normalize_url("example.com/path") == "https://example.com/path"

    def test_preserves_query_string(self) -> None:
        result = normalize_url("https://example.com/search?q=test#frag")
        assert "?q=test" in result
        assert "#frag" not in result

    def test_preserves_port(self) -> None:
        result = normalize_url("https://localhost:8080/path")
        assert ":8080" in result

    def test_http_scheme_preserved(self) -> None:
        result = normalize_url("http://example.com/page")
        assert result.startswith("http://")

    def test_multiple_trailing_slashes(self) -> None:
        result = normalize_url("https://example.com/path///")
        assert result == "https://example.com/path"


class TestExtractDomain:
    """Test domain extraction."""

    def test_strips_www(self) -> None:
        assert extract_domain("https://www.mit.edu/path") == "mit.edu"

    def test_preserves_subdomain(self) -> None:
        assert extract_domain("http://cs.stanford.edu/people") == "cs.stanford.edu"

    def test_no_scheme(self) -> None:
        assert extract_domain("example.com") == "example.com"

    def test_with_port(self) -> None:
        assert extract_domain("https://example.com:8080/path") == "example.com"

    def test_www_only(self) -> None:
        assert extract_domain("https://www.example.com") == "example.com"

    def test_lowercases_domain(self) -> None:
        assert extract_domain("https://WWW.MIT.EDU") == "mit.edu"

    def test_with_path_and_query(self) -> None:
        assert extract_domain("https://example.com/path?q=1") == "example.com"


class TestIsSameDomain:
    """Test same-domain comparison."""

    def test_same_domain_different_paths(self) -> None:
        assert is_same_domain("https://mit.edu/a", "https://mit.edu/b") is True

    def test_www_vs_no_www(self) -> None:
        assert is_same_domain("https://www.mit.edu/a", "http://mit.edu/b") is True

    def test_different_domains(self) -> None:
        assert is_same_domain("https://mit.edu", "https://stanford.edu") is False

    def test_different_schemes(self) -> None:
        assert is_same_domain("http://mit.edu", "https://mit.edu") is True

    def test_subdomain_difference(self) -> None:
        # cs.mit.edu and math.mit.edu are NOT the same domain
        assert is_same_domain("https://cs.mit.edu", "https://math.mit.edu") is False

    def test_no_scheme(self) -> None:
        assert is_same_domain("mit.edu/page", "https://mit.edu/other") is True

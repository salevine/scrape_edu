"""Tests for scrape_edu.net.http_client module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch

import pytest
import requests

from scrape_edu.net.http_client import HttpClient
from scrape_edu.net.rate_limiter import RateLimiter


@pytest.fixture()
def mock_rate_limiter() -> MagicMock:
    """Return a mock RateLimiter (no real sleeping)."""
    return MagicMock(spec=RateLimiter)


@pytest.fixture()
def client(mock_rate_limiter: MagicMock) -> HttpClient:
    """Return an HttpClient with a mocked rate limiter."""
    return HttpClient(rate_limiter=mock_rate_limiter)


class TestGet:
    """Tests for HttpClient.get()."""

    def test_calls_rate_limiter_with_correct_domain(
        self, client: HttpClient, mock_rate_limiter: MagicMock
    ) -> None:
        mock_response = MagicMock(spec=requests.Response)
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._session, "get", return_value=mock_response):
            client.get("https://www.mit.edu/courses")

        mock_rate_limiter.wait.assert_called_once_with("mit.edu")

    def test_sets_user_agent(self, mock_rate_limiter: MagicMock) -> None:
        c = HttpClient(rate_limiter=mock_rate_limiter, user_agent="TestBot/1.0")
        assert c._session.headers["User-Agent"] == "TestBot/1.0"

    def test_default_user_agent(self, client: HttpClient) -> None:
        assert "Chrome" in client._session.headers["User-Agent"]

    def test_uses_configured_timeout(
        self, client: HttpClient, mock_rate_limiter: MagicMock
    ) -> None:
        mock_response = MagicMock(spec=requests.Response)
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._session, "get", return_value=mock_response) as mock_get:
            client.get("https://example.com/page")

        _, kwargs = mock_get.call_args
        assert kwargs["timeout"] == (10, 30)

    def test_custom_timeout(self, mock_rate_limiter: MagicMock) -> None:
        c = HttpClient(rate_limiter=mock_rate_limiter, timeout=(5, 15))
        mock_response = MagicMock(spec=requests.Response)
        mock_response.raise_for_status = MagicMock()

        with patch.object(c._session, "get", return_value=mock_response) as mock_get:
            c.get("https://example.com")

        _, kwargs = mock_get.call_args
        assert kwargs["timeout"] == (5, 15)

    def test_timeout_can_be_overridden_per_call(
        self, client: HttpClient, mock_rate_limiter: MagicMock
    ) -> None:
        mock_response = MagicMock(spec=requests.Response)
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._session, "get", return_value=mock_response) as mock_get:
            client.get("https://example.com", timeout=(1, 2))

        _, kwargs = mock_get.call_args
        assert kwargs["timeout"] == (1, 2)

    def test_raises_on_4xx(
        self, client: HttpClient, mock_rate_limiter: MagicMock
    ) -> None:
        mock_response = MagicMock(spec=requests.Response)
        mock_response.raise_for_status.side_effect = requests.HTTPError(
            "404 Not Found"
        )

        with patch.object(client._session, "get", return_value=mock_response):
            with pytest.raises(requests.HTTPError, match="404"):
                client.get("https://example.com/missing")

    def test_returns_response(
        self, client: HttpClient, mock_rate_limiter: MagicMock
    ) -> None:
        mock_response = MagicMock(spec=requests.Response)
        mock_response.raise_for_status = MagicMock()
        mock_response.text = "hello"

        with patch.object(client._session, "get", return_value=mock_response):
            resp = client.get("https://example.com")

        assert resp.text == "hello"

    def test_passes_extra_kwargs(
        self, client: HttpClient, mock_rate_limiter: MagicMock
    ) -> None:
        mock_response = MagicMock(spec=requests.Response)
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._session, "get", return_value=mock_response) as mock_get:
            client.get("https://example.com", params={"q": "test"})

        _, kwargs = mock_get.call_args
        assert kwargs["params"] == {"q": "test"}


class TestDownload:
    """Tests for HttpClient.download()."""

    def test_writes_file(
        self,
        client: HttpClient,
        mock_rate_limiter: MagicMock,
        tmp_path: Path,
    ) -> None:
        mock_response = MagicMock(spec=requests.Response)
        mock_response.raise_for_status = MagicMock()
        mock_response.iter_content.return_value = [b"hello ", b"world"]

        dest = tmp_path / "output.pdf"
        with patch.object(client._session, "get", return_value=mock_response):
            result = client.download("https://example.com/file.pdf", dest)

        assert result == dest
        assert dest.read_bytes() == b"hello world"

    def test_creates_parent_directories(
        self,
        client: HttpClient,
        mock_rate_limiter: MagicMock,
        tmp_path: Path,
    ) -> None:
        mock_response = MagicMock(spec=requests.Response)
        mock_response.raise_for_status = MagicMock()
        mock_response.iter_content.return_value = [b"data"]

        dest = tmp_path / "a" / "b" / "file.txt"
        with patch.object(client._session, "get", return_value=mock_response):
            client.download("https://example.com/file.txt", dest)

        assert dest.exists()

    def test_calls_rate_limiter(
        self,
        client: HttpClient,
        mock_rate_limiter: MagicMock,
        tmp_path: Path,
    ) -> None:
        mock_response = MagicMock(spec=requests.Response)
        mock_response.raise_for_status = MagicMock()
        mock_response.iter_content.return_value = [b"ok"]

        dest = tmp_path / "file.bin"
        with patch.object(client._session, "get", return_value=mock_response):
            client.download("https://www.stanford.edu/doc.pdf", dest)

        mock_rate_limiter.wait.assert_called_once_with("stanford.edu")

    def test_uses_streaming(
        self,
        client: HttpClient,
        mock_rate_limiter: MagicMock,
        tmp_path: Path,
    ) -> None:
        mock_response = MagicMock(spec=requests.Response)
        mock_response.raise_for_status = MagicMock()
        mock_response.iter_content.return_value = [b"chunk"]

        dest = tmp_path / "stream.bin"
        with patch.object(client._session, "get", return_value=mock_response) as mock_get:
            client.download("https://example.com/big", dest)

        _, kwargs = mock_get.call_args
        assert kwargs["stream"] is True

    def test_cleans_up_tmp_on_error(
        self,
        client: HttpClient,
        mock_rate_limiter: MagicMock,
        tmp_path: Path,
    ) -> None:
        mock_response = MagicMock(spec=requests.Response)
        mock_response.raise_for_status.side_effect = requests.HTTPError("500")

        dest = tmp_path / "fail.pdf"
        with patch.object(client._session, "get", return_value=mock_response):
            with pytest.raises(requests.HTTPError):
                client.download("https://example.com/fail.pdf", dest)

        # Neither the dest nor the .tmp file should exist.
        assert not dest.exists()
        tmp_files = list(tmp_path.glob("*.tmp"))
        assert tmp_files == []

    def test_cleans_up_tmp_on_write_error(
        self,
        client: HttpClient,
        mock_rate_limiter: MagicMock,
        tmp_path: Path,
    ) -> None:
        """If iter_content raises mid-stream, .tmp is cleaned up."""
        mock_response = MagicMock(spec=requests.Response)
        mock_response.raise_for_status = MagicMock()
        mock_response.iter_content.side_effect = ConnectionError("broken pipe")

        dest = tmp_path / "partial.bin"
        with patch.object(client._session, "get", return_value=mock_response):
            with pytest.raises(ConnectionError):
                client.download("https://example.com/partial", dest)

        assert not dest.exists()
        tmp_files = list(tmp_path.glob("*.tmp"))
        assert tmp_files == []

    def test_no_tmp_file_on_success(
        self,
        client: HttpClient,
        mock_rate_limiter: MagicMock,
        tmp_path: Path,
    ) -> None:
        mock_response = MagicMock(spec=requests.Response)
        mock_response.raise_for_status = MagicMock()
        mock_response.iter_content.return_value = [b"ok"]

        dest = tmp_path / "clean.bin"
        with patch.object(client._session, "get", return_value=mock_response):
            client.download("https://example.com/file", dest)

        tmp_files = list(tmp_path.glob("*.tmp"))
        assert tmp_files == []


class TestContextManager:
    """Test HttpClient context manager protocol."""

    def test_enter_returns_self(self, mock_rate_limiter: MagicMock) -> None:
        client = HttpClient(rate_limiter=mock_rate_limiter)
        result = client.__enter__()
        assert result is client
        client.close()

    def test_with_statement(self, mock_rate_limiter: MagicMock) -> None:
        with HttpClient(rate_limiter=mock_rate_limiter) as client:
            assert isinstance(client, HttpClient)

    def test_close_called_on_exit(self, mock_rate_limiter: MagicMock) -> None:
        client = HttpClient(rate_limiter=mock_rate_limiter)
        with patch.object(client, "close") as mock_close:
            client.__exit__(None, None, None)
        mock_close.assert_called_once()


class TestClose:
    """Test HttpClient.close()."""

    def test_close_closes_session(self, mock_rate_limiter: MagicMock) -> None:
        client = HttpClient(rate_limiter=mock_rate_limiter)
        with patch.object(client._session, "close") as mock_close:
            client.close()
        mock_close.assert_called_once()


class TestRetryConfig:
    """Test that retry/adapter configuration is applied."""

    def test_adapters_mounted(self, mock_rate_limiter: MagicMock) -> None:
        client = HttpClient(rate_limiter=mock_rate_limiter, max_retries=5)
        # Both http and https should have adapters with our retry config.
        http_adapter = client._session.get_adapter("http://example.com")
        https_adapter = client._session.get_adapter("https://example.com")
        assert http_adapter.max_retries.total == 5
        assert https_adapter.max_retries.total == 5

    def test_retry_status_forcelist(self, mock_rate_limiter: MagicMock) -> None:
        client = HttpClient(rate_limiter=mock_rate_limiter)
        adapter = client._session.get_adapter("https://example.com")
        assert 500 in adapter.max_retries.status_forcelist
        assert 502 in adapter.max_retries.status_forcelist
        assert 503 in adapter.max_retries.status_forcelist
        assert 504 in adapter.max_retries.status_forcelist


class TestSSLFallback:
    """Tests for SSL verify=False fallback on .edu domains."""

    def test_ssl_fallback_on_edu_domain_get(
        self, client: HttpClient, mock_rate_limiter: MagicMock
    ) -> None:
        """SSLError on .edu domain retried with verify=False in get()."""
        mock_response = MagicMock(spec=requests.Response)
        mock_response.raise_for_status = MagicMock()

        with patch.object(
            client._session,
            "get",
            side_effect=[
                requests.exceptions.SSLError("cert verify failed"),
                mock_response,
            ],
        ) as mock_get:
            resp = client.get("https://eecs.psu.edu/courses")

        assert resp is mock_response
        assert mock_get.call_count == 2
        # Second call should have verify=False
        _, kwargs = mock_get.call_args_list[1]
        assert kwargs["verify"] is False

    def test_ssl_fallback_not_on_non_edu_get(
        self, client: HttpClient, mock_rate_limiter: MagicMock
    ) -> None:
        """SSLError on .com domain raises normally in get()."""
        with patch.object(
            client._session,
            "get",
            side_effect=requests.exceptions.SSLError("cert verify failed"),
        ):
            with pytest.raises(requests.exceptions.SSLError):
                client.get("https://example.com/page")

    def test_ssl_fallback_on_edu_domain_download(
        self,
        client: HttpClient,
        mock_rate_limiter: MagicMock,
        tmp_path: Path,
    ) -> None:
        """SSLError on .edu domain retried with verify=False in download()."""
        mock_response = MagicMock(spec=requests.Response)
        mock_response.raise_for_status = MagicMock()
        mock_response.iter_content.return_value = [b"data"]

        with patch.object(
            client._session,
            "get",
            side_effect=[
                requests.exceptions.SSLError("cert verify failed"),
                mock_response,
            ],
        ) as mock_get:
            dest = tmp_path / "file.pdf"
            result = client.download("https://eecs.psu.edu/syllabus.pdf", dest)

        assert result == dest
        assert dest.read_bytes() == b"data"
        assert mock_get.call_count == 2
        _, kwargs = mock_get.call_args_list[1]
        assert kwargs["verify"] is False

    def test_ssl_fallback_not_on_non_edu_download(
        self,
        client: HttpClient,
        mock_rate_limiter: MagicMock,
        tmp_path: Path,
    ) -> None:
        """SSLError on .com domain raises normally in download()."""
        with patch.object(
            client._session,
            "get",
            side_effect=requests.exceptions.SSLError("cert verify failed"),
        ):
            dest = tmp_path / "fail.pdf"
            with pytest.raises(requests.exceptions.SSLError):
                client.download("https://example.com/fail.pdf", dest)

"""Tests for scripts/download_ipeds.py module."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Import the module under test.  Since it lives under scripts/ (not a
# package), we import it by manipulating sys.path or using importlib.
import importlib
import sys

_scripts_dir = str(Path(__file__).resolve().parent.parent / "scripts")
if _scripts_dir not in sys.path:
    sys.path.insert(0, _scripts_dir)

import download_ipeds


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_zip(csv_filename: str, csv_content: bytes) -> bytes:
    """Create an in-memory zip archive containing a single CSV."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_filename, csv_content)
    return buf.getvalue()


def _make_zip_subdir(subdir: str, csv_filename: str, csv_content: bytes) -> bytes:
    """Create an in-memory zip archive with the CSV in a subdirectory."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{subdir}/{csv_filename}", csv_content)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# extract_csv_from_zip
# ---------------------------------------------------------------------------

class TestExtractCsvFromZip:
    """Test CSV extraction from zip archives."""

    def test_exact_match(self) -> None:
        content = b"UNITID,INSTNM\n100001,Test University\n"
        zip_bytes = _make_zip("hd2023.csv", content)
        result = download_ipeds.extract_csv_from_zip(zip_bytes, "hd2023.csv")
        assert result == content

    def test_case_insensitive_match(self) -> None:
        """The CSV in the zip may have different case than expected."""
        content = b"UNITID,INSTNM\n100001,Test University\n"
        zip_bytes = _make_zip("HD2023.csv", content)
        result = download_ipeds.extract_csv_from_zip(zip_bytes, "hd2023.csv")
        assert result == content

    def test_case_insensitive_match_reverse(self) -> None:
        """Pattern is uppercase, zip has lowercase."""
        content = b"UNITID,INSTNM\n100001,Test University\n"
        zip_bytes = _make_zip("hd2023.csv", content)
        result = download_ipeds.extract_csv_from_zip(zip_bytes, "HD2023.csv")
        assert result == content

    def test_csv_in_subdirectory(self) -> None:
        """Handle the case where the CSV is nested inside a subdirectory."""
        content = b"UNITID,INSTNM\n100001,Test University\n"
        zip_bytes = _make_zip_subdir("data", "hd2023.csv", content)
        result = download_ipeds.extract_csv_from_zip(zip_bytes, "hd2023.csv")
        assert result == content

    def test_fallback_to_first_csv(self) -> None:
        """If the expected CSV name doesn't match, fall back to first CSV."""
        content = b"UNITID,INSTNM\n100001,Test University\n"
        zip_bytes = _make_zip("unexpected_name.csv", content)
        result = download_ipeds.extract_csv_from_zip(zip_bytes, "hd2023.csv")
        assert result == content

    def test_no_csv_raises(self) -> None:
        """If the zip contains no CSV at all, raise FileNotFoundError."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "not a csv")
        zip_bytes = buf.getvalue()

        with pytest.raises(FileNotFoundError, match="No CSV file"):
            download_ipeds.extract_csv_from_zip(zip_bytes, "hd2023.csv")

    def test_bad_zip_raises(self) -> None:
        """Non-zip data should raise BadZipFile."""
        with pytest.raises(zipfile.BadZipFile):
            download_ipeds.extract_csv_from_zip(b"not a zip file", "hd2023.csv")


# ---------------------------------------------------------------------------
# download_file
# ---------------------------------------------------------------------------

class TestDownloadFile:
    """Test the download_file function with mocked HTTP responses."""

    @patch("download_ipeds.requests.get")
    def test_download_with_content_length(self, mock_get: MagicMock) -> None:
        """Download succeeds and reports progress when Content-Length known."""
        data = b"x" * 1024
        mock_response = MagicMock()
        mock_response.headers = {"Content-Length": str(len(data))}
        mock_response.iter_content.return_value = [data]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = download_ipeds.download_file("https://example.com/f.zip", "test")
        assert result == data

    @patch("download_ipeds.requests.get")
    def test_download_without_content_length(self, mock_get: MagicMock) -> None:
        """Download succeeds even when Content-Length is absent."""
        data = b"y" * 512
        mock_response = MagicMock()
        mock_response.headers = {}
        mock_response.iter_content.return_value = [data]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = download_ipeds.download_file("https://example.com/f.zip", "test")
        assert result == data

    @patch("download_ipeds.requests.get")
    def test_download_multiple_chunks(self, mock_get: MagicMock) -> None:
        """Chunks are properly concatenated."""
        chunks = [b"aaa", b"bbb", b"ccc"]
        mock_response = MagicMock()
        mock_response.headers = {"Content-Length": "9"}
        mock_response.iter_content.return_value = chunks
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = download_ipeds.download_file("https://example.com/f.zip", "test")
        assert result == b"aaabbbccc"

    @patch("download_ipeds.requests.get")
    def test_download_http_error_raises(self, mock_get: MagicMock) -> None:
        """HTTPError from raise_for_status is propagated."""
        import requests

        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError(
            response=MagicMock(status_code=404)
        )
        mock_get.return_value = mock_response

        with pytest.raises(requests.HTTPError):
            download_ipeds.download_file("https://example.com/f.zip", "test")


# ---------------------------------------------------------------------------
# download_ipeds (integration-ish, with mocked HTTP)
# ---------------------------------------------------------------------------

class TestDownloadIpeds:
    """Test the main download_ipeds orchestration logic."""

    @patch("download_ipeds.download_file")
    def test_successful_download(
        self, mock_dl: MagicMock, tmp_path: Path
    ) -> None:
        """Both HD and C files are downloaded and saved."""
        hd_csv = b"UNITID,INSTNM\n100001,Test U\n"
        c_csv = b"UNITID,CIPCODE,AWLEVEL\n100001,11.0101,5\n"

        hd_zip = _make_zip("hd2023.csv", hd_csv)
        c_zip = _make_zip("c2023_a.csv", c_csv)

        mock_dl.side_effect = [hd_zip, c_zip]

        download_ipeds.download_ipeds(2023, tmp_path)

        assert (tmp_path / "hd2023.csv").read_bytes() == hd_csv
        assert (tmp_path / "c2023_a.csv").read_bytes() == c_csv

    @patch("download_ipeds.download_file")
    def test_fallback_year(
        self, mock_dl: MagicMock, tmp_path: Path
    ) -> None:
        """If the first year fails (404), the script tries the next year."""
        import requests

        hd_csv = b"UNITID,INSTNM\n100001,Test U\n"
        c_csv = b"UNITID,CIPCODE,AWLEVEL\n100001,11.0101,5\n"

        hd_zip_2022 = _make_zip("hd2022.csv", hd_csv)
        c_zip_2022 = _make_zip("c2022_a.csv", c_csv)

        # For HD: first call (2023) fails with HTTPError, second (2022) succeeds
        # For C: first call (2023) fails with HTTPError, second (2022) succeeds
        http_error = requests.HTTPError(response=MagicMock(status_code=404))

        mock_dl.side_effect = [
            http_error,          # HD 2023 fails
            hd_zip_2022,         # HD 2022 succeeds
            http_error,          # C 2023 fails
            c_zip_2022,          # C 2022 succeeds
        ]

        # We need to patch _try_download_file differently since download_file
        # raises inside it.  Let's patch at a higher level instead.
        # Actually, _try_download_file catches HTTPError, so let's just
        # test via download_ipeds with _try_download_file patched.
        with patch.object(download_ipeds, "_try_download_file") as mock_try:
            mock_try.side_effect = [
                None,       # HD 2023 fails
                hd_csv,     # HD 2022 succeeds
                None,       # C 2023 fails
                c_csv,      # C 2022 succeeds
            ]
            download_ipeds.download_ipeds(2023, tmp_path)

        assert (tmp_path / "hd2022.csv").read_bytes() == hd_csv
        assert (tmp_path / "c2022_a.csv").read_bytes() == c_csv

    @patch("download_ipeds._try_download_file")
    def test_all_years_fail_exits(
        self, mock_try: MagicMock, tmp_path: Path
    ) -> None:
        """If all years fail, the script calls sys.exit(1)."""
        mock_try.return_value = None

        with pytest.raises(SystemExit) as exc_info:
            download_ipeds.download_ipeds(2023, tmp_path)

        assert exc_info.value.code == 1

    @patch("download_ipeds._try_download_file")
    def test_creates_output_directory(
        self, mock_try: MagicMock, tmp_path: Path
    ) -> None:
        """Output directory is created if it doesn't exist."""
        out = tmp_path / "sub" / "dir" / "ipeds"
        csv_data = b"UNITID,INSTNM\n"

        mock_try.return_value = csv_data
        download_ipeds.download_ipeds(2023, out)

        assert out.exists()


# ---------------------------------------------------------------------------
# CLI (main)
# ---------------------------------------------------------------------------

class TestMain:
    """Test CLI argument parsing."""

    @patch("download_ipeds.download_ipeds")
    def test_default_args(self, mock_download: MagicMock) -> None:
        """Default arguments are applied correctly."""
        with patch("sys.argv", ["download_ipeds.py"]):
            download_ipeds.main()

        mock_download.assert_called_once()
        args = mock_download.call_args
        assert args[0][0] == 2023
        assert args[0][1] == Path("data/ipeds")

    @patch("download_ipeds.download_ipeds")
    def test_custom_args(self, mock_download: MagicMock) -> None:
        """Custom --year and --output-dir are forwarded."""
        with patch("sys.argv", [
            "download_ipeds.py", "--year", "2021", "--output-dir", "/tmp/ipeds"
        ]):
            download_ipeds.main()

        mock_download.assert_called_once()
        args = mock_download.call_args
        assert args[0][0] == 2021
        assert args[0][1] == Path("/tmp/ipeds")

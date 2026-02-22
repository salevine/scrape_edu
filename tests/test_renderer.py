"""Tests for the PageRenderer.

All Playwright/pool interactions are mocked — no real browser is launched.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from scrape_edu.browser.playwright_pool import PlaywrightPool
from scrape_edu.browser.renderer import PageRenderer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_pool_and_page():
    """Return (pool_mock, context_mock, page_mock) with sensible defaults."""
    page_mock = MagicMock(name="Page")
    ctx_mock = MagicMock(name="BrowserContext")
    ctx_mock.new_page.return_value = page_mock

    pool_mock = MagicMock(spec=PlaywrightPool)
    pool_mock.acquire.return_value = ctx_mock

    return pool_mock, ctx_mock, page_mock


# ---------------------------------------------------------------------------
# render_to_pdf tests
# ---------------------------------------------------------------------------

class TestRenderToPdf:

    def test_acquires_and_releases_context(self, tmp_path):
        pool, ctx, page = _make_mock_pool_and_page()
        # Make page.pdf create the tmp file so rename works
        dest = tmp_path / "out.pdf"
        tmp_file = dest.with_suffix(dest.suffix + ".tmp")

        def fake_pdf(path):
            Path(path).touch()

        page.pdf.side_effect = fake_pdf

        renderer = PageRenderer(pool)
        renderer.render_to_pdf("https://example.com", dest)

        pool.acquire.assert_called_once()
        pool.release.assert_called_once_with(ctx)

    def test_creates_page_navigates_and_generates_pdf(self, tmp_path):
        pool, ctx, page = _make_mock_pool_and_page()
        dest = tmp_path / "out.pdf"

        def fake_pdf(path):
            Path(path).touch()

        page.pdf.side_effect = fake_pdf

        renderer = PageRenderer(pool, navigation_timeout=5000)
        result = renderer.render_to_pdf("https://example.com/catalog", dest, wait_until="load")

        ctx.new_page.assert_called_once()
        page.goto.assert_called_once_with(
            "https://example.com/catalog",
            wait_until="load",
            timeout=5000,
        )
        page.pdf.assert_called_once()
        page.close.assert_called_once()
        assert result == dest

    def test_context_released_on_navigation_error(self, tmp_path):
        pool, ctx, page = _make_mock_pool_and_page()
        page.goto.side_effect = Exception("Navigation failed")

        renderer = PageRenderer(pool)
        dest = tmp_path / "out.pdf"

        with pytest.raises(Exception, match="Navigation failed"):
            renderer.render_to_pdf("https://example.com", dest)

        pool.release.assert_called_once_with(ctx)

    def test_context_released_on_pdf_error(self, tmp_path):
        pool, ctx, page = _make_mock_pool_and_page()
        page.pdf.side_effect = Exception("PDF generation failed")

        renderer = PageRenderer(pool)
        dest = tmp_path / "out.pdf"

        with pytest.raises(Exception, match="PDF generation failed"):
            renderer.render_to_pdf("https://example.com", dest)

        pool.release.assert_called_once_with(ctx)

    def test_tmp_file_cleaned_up_on_error(self, tmp_path):
        pool, ctx, page = _make_mock_pool_and_page()
        dest = tmp_path / "out.pdf"
        tmp_file = dest.with_suffix(dest.suffix + ".tmp")

        def fake_pdf_then_fail(path):
            Path(path).touch()
            raise Exception("PDF post-processing failed")

        page.pdf.side_effect = fake_pdf_then_fail

        renderer = PageRenderer(pool)
        with pytest.raises(Exception, match="PDF post-processing failed"):
            renderer.render_to_pdf("https://example.com", dest)

        assert not tmp_file.exists(), "Temp file should be cleaned up on error"

    def test_parent_directories_created(self, tmp_path):
        pool, ctx, page = _make_mock_pool_and_page()
        dest = tmp_path / "deep" / "nested" / "dir" / "out.pdf"

        def fake_pdf(path):
            Path(path).touch()

        page.pdf.side_effect = fake_pdf

        renderer = PageRenderer(pool)
        result = renderer.render_to_pdf("https://example.com", dest)

        assert dest.parent.exists()
        assert result == dest


# ---------------------------------------------------------------------------
# render_html_to_pdf tests
# ---------------------------------------------------------------------------

class TestRenderHtmlToPdf:

    def test_uses_set_content_instead_of_goto(self, tmp_path):
        pool, ctx, page = _make_mock_pool_and_page()
        dest = tmp_path / "out.pdf"

        def fake_pdf(path):
            Path(path).touch()

        page.pdf.side_effect = fake_pdf

        renderer = PageRenderer(pool, navigation_timeout=10000)
        result = renderer.render_html_to_pdf("<h1>Hello</h1>", dest)

        page.goto.assert_not_called()
        page.set_content.assert_called_once_with(
            "<h1>Hello</h1>",
            wait_until="networkidle",
            timeout=10000,
        )
        page.pdf.assert_called_once()
        page.close.assert_called_once()
        assert result == dest

    def test_acquires_and_releases_context(self, tmp_path):
        pool, ctx, page = _make_mock_pool_and_page()
        dest = tmp_path / "out.pdf"

        def fake_pdf(path):
            Path(path).touch()

        page.pdf.side_effect = fake_pdf

        renderer = PageRenderer(pool)
        renderer.render_html_to_pdf("<p>Test</p>", dest)

        pool.acquire.assert_called_once()
        pool.release.assert_called_once_with(ctx)

    def test_context_released_on_error(self, tmp_path):
        pool, ctx, page = _make_mock_pool_and_page()
        page.set_content.side_effect = Exception("Content load failed")

        renderer = PageRenderer(pool)
        dest = tmp_path / "out.pdf"

        with pytest.raises(Exception, match="Content load failed"):
            renderer.render_html_to_pdf("<p>Bad</p>", dest)

        pool.release.assert_called_once_with(ctx)

    def test_tmp_file_cleaned_up_on_error(self, tmp_path):
        pool, ctx, page = _make_mock_pool_and_page()
        dest = tmp_path / "out.pdf"
        tmp_file = dest.with_suffix(dest.suffix + ".tmp")

        def fake_pdf_then_fail(path):
            Path(path).touch()
            raise Exception("Render failed")

        page.pdf.side_effect = fake_pdf_then_fail

        renderer = PageRenderer(pool)
        with pytest.raises(Exception, match="Render failed"):
            renderer.render_html_to_pdf("<p>Test</p>", dest)

        assert not tmp_file.exists(), "Temp file should be cleaned up on error"

    def test_parent_directories_created(self, tmp_path):
        pool, ctx, page = _make_mock_pool_and_page()
        dest = tmp_path / "a" / "b" / "out.pdf"

        def fake_pdf(path):
            Path(path).touch()

        page.pdf.side_effect = fake_pdf

        renderer = PageRenderer(pool)
        result = renderer.render_html_to_pdf("<p>Nested</p>", dest)

        assert dest.parent.exists()
        assert result == dest

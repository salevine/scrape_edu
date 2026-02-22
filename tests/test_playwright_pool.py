"""Tests for the Playwright browser context pool.

All Playwright objects are mocked — no real browser is launched.
"""

from unittest.mock import MagicMock, patch

import pytest

from scrape_edu.browser.playwright_pool import PlaywrightPool


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_playwright():
    """Return (mock_sync_playwright_func, mock_pw_instance, mock_browser)."""
    mock_browser = MagicMock(name="Browser")
    mock_browser.new_context.side_effect = lambda: MagicMock(name="BrowserContext")

    mock_pw = MagicMock(name="Playwright")
    mock_pw.chromium.launch.return_value = mock_browser

    mock_sync_pw = MagicMock(name="sync_playwright")
    mock_sync_pw.return_value.start.return_value = mock_pw

    return mock_sync_pw, mock_pw, mock_browser


PATCH_TARGET = "scrape_edu.browser.playwright_pool.sync_playwright"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPlaywrightPoolStart:
    """Tests for start() behaviour."""

    @patch(PATCH_TARGET)
    def test_start_creates_correct_number_of_contexts(self, mock_sync_pw_cls):
        mock_sync_pw, mock_pw, mock_browser = _make_mock_playwright()
        mock_sync_pw_cls.side_effect = mock_sync_pw

        pool = PlaywrightPool(pool_size=3)
        pool.start()

        assert mock_browser.new_context.call_count == 3
        assert pool._queue.qsize() == 3
        assert pool._started is True

        pool.stop()

    @patch(PATCH_TARGET)
    def test_start_is_idempotent(self, mock_sync_pw_cls):
        mock_sync_pw, mock_pw, mock_browser = _make_mock_playwright()
        mock_sync_pw_cls.side_effect = mock_sync_pw

        pool = PlaywrightPool(pool_size=2)
        pool.start()
        pool.start()  # second call should be a no-op

        # Browser should only have been launched once
        assert mock_pw.chromium.launch.call_count == 1
        assert mock_browser.new_context.call_count == 2

        pool.stop()

    @patch(PATCH_TARGET)
    def test_start_failure_cleans_up(self, mock_sync_pw_cls):
        mock_sync_pw_cls.return_value.start.side_effect = RuntimeError("no browser")

        pool = PlaywrightPool()
        with pytest.raises(RuntimeError, match="Failed to start Playwright"):
            pool.start()

        assert pool._started is False
        assert pool._browser is None
        assert pool._playwright is None


class TestPlaywrightPoolAcquireRelease:
    """Tests for acquire() and release()."""

    @patch(PATCH_TARGET)
    def test_acquire_returns_context(self, mock_sync_pw_cls):
        mock_sync_pw, _, mock_browser = _make_mock_playwright()
        mock_sync_pw_cls.side_effect = mock_sync_pw

        pool = PlaywrightPool(pool_size=1)
        pool.start()

        ctx = pool.acquire()
        assert ctx is not None
        assert pool._queue.qsize() == 0

        pool.release(ctx)
        assert pool._queue.qsize() == 1

        pool.stop()

    @patch(PATCH_TARGET)
    def test_acquire_timeout_on_empty_pool(self, mock_sync_pw_cls):
        mock_sync_pw, _, _ = _make_mock_playwright()
        mock_sync_pw_cls.side_effect = mock_sync_pw

        pool = PlaywrightPool(pool_size=1)
        pool.start()

        _ = pool.acquire()  # drain the pool

        with pytest.raises(TimeoutError, match="Could not acquire"):
            pool.acquire(timeout=0.01)

        pool.stop()

    def test_acquire_before_start_raises(self):
        pool = PlaywrightPool()
        with pytest.raises(RuntimeError, match="Pool not started"):
            pool.acquire()

    @patch(PATCH_TARGET)
    def test_release_puts_context_back(self, mock_sync_pw_cls):
        mock_sync_pw, _, _ = _make_mock_playwright()
        mock_sync_pw_cls.side_effect = mock_sync_pw

        pool = PlaywrightPool(pool_size=2)
        pool.start()

        ctx1 = pool.acquire()
        ctx2 = pool.acquire()
        assert pool._queue.qsize() == 0

        pool.release(ctx1)
        pool.release(ctx2)
        assert pool._queue.qsize() == 2

        pool.stop()


class TestPlaywrightPoolStop:
    """Tests for stop()."""

    @patch(PATCH_TARGET)
    def test_stop_closes_contexts_and_browser(self, mock_sync_pw_cls):
        mock_sync_pw, mock_pw, mock_browser = _make_mock_playwright()
        mock_sync_pw_cls.side_effect = mock_sync_pw

        pool = PlaywrightPool(pool_size=2)
        pool.start()

        # Grab the actual context mocks that were created
        contexts = []
        while not pool._queue.empty():
            contexts.append(pool._queue.get_nowait())
        for c in contexts:
            pool._queue.put(c)

        pool.stop()

        for ctx in contexts:
            ctx.close.assert_called_once()
        mock_browser.close.assert_called_once()
        mock_pw.stop.assert_called_once()
        assert pool._started is False

    @patch(PATCH_TARGET)
    def test_stop_is_safe_when_not_started(self, mock_sync_pw_cls):
        pool = PlaywrightPool()
        pool.stop()  # should not raise
        assert pool._started is False


class TestPlaywrightPoolContextManager:
    """Tests for __enter__ / __exit__."""

    @patch(PATCH_TARGET)
    def test_context_manager_starts_and_stops(self, mock_sync_pw_cls):
        mock_sync_pw, mock_pw, mock_browser = _make_mock_playwright()
        mock_sync_pw_cls.side_effect = mock_sync_pw

        with PlaywrightPool(pool_size=1) as pool:
            assert pool._started is True
            ctx = pool.acquire()
            assert ctx is not None
            pool.release(ctx)

        # After exiting, pool should be stopped
        assert pool._started is False
        mock_browser.close.assert_called_once()
        mock_pw.stop.assert_called_once()

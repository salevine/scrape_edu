import logging
import queue
from typing import Optional

from playwright.sync_api import sync_playwright, Browser, BrowserContext, Playwright

logger = logging.getLogger("scrape_edu")


class PlaywrightPool:
    """Pool of Playwright browser contexts for concurrent HTML-to-PDF rendering."""

    def __init__(self, pool_size: int = 2):
        self.pool_size = pool_size
        self._queue: queue.Queue[BrowserContext] = queue.Queue(maxsize=pool_size)
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._started = False

    def start(self) -> None:
        """Launch browser and create context pool."""
        if self._started:
            return

        try:
            self._playwright = sync_playwright().start()
            self._browser = self._playwright.chromium.launch(headless=True)

            for _ in range(self.pool_size):
                ctx = self._browser.new_context()
                self._queue.put(ctx)

            self._started = True
            logger.info("Playwright pool started", extra={"pool_size": self.pool_size})
        except Exception as e:
            self.stop()
            raise RuntimeError(
                f"Failed to start Playwright: {e}. "
                "Run 'playwright install chromium' to install the browser."
            ) from e

    def acquire(self, timeout: float = 30.0) -> BrowserContext:
        """Get a browser context from the pool."""
        if not self._started:
            raise RuntimeError("Pool not started. Call start() first.")
        try:
            return self._queue.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError(
                f"Could not acquire browser context within {timeout}s. "
                f"All {self.pool_size} contexts are in use."
            )

    def release(self, context: BrowserContext) -> None:
        """Return a browser context to the pool."""
        self._queue.put(context)

    def stop(self) -> None:
        """Close all contexts and shut down the browser."""
        # Drain the queue and close contexts
        while not self._queue.empty():
            try:
                ctx = self._queue.get_nowait()
                ctx.close()
            except queue.Empty:
                break

        if self._browser:
            self._browser.close()
            self._browser = None

        if self._playwright:
            self._playwright.stop()
            self._playwright = None

        self._started = False
        logger.info("Playwright pool stopped")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()

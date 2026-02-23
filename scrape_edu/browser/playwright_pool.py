import logging
import queue
import threading
from concurrent.futures import Future
from typing import Callable, Optional, TypeVar

from playwright.sync_api import sync_playwright, Browser, BrowserContext, Playwright

logger = logging.getLogger("scrape_edu")

T = TypeVar("T")


class PlaywrightPool:
    """Run Playwright on a dedicated thread so callers from any thread can render.

    Playwright's sync API is bound to the thread that started it (via greenlets).
    This pool runs a single browser on a background thread and exposes a
    thread-safe ``submit()`` to dispatch work from worker threads.
    """

    def __init__(self, pool_size: int = 2):
        self.pool_size = pool_size
        self._work_queue: queue.Queue = queue.Queue()
        self._thread: Optional[threading.Thread] = None
        self._started = False
        self._ready = threading.Event()
        self._error: Optional[Exception] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Launch the background Playwright thread."""
        if self._started:
            return

        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self._ready.wait(timeout=30)

        if self._error:
            raise RuntimeError(
                f"Failed to start Playwright: {self._error}. "
                "Run 'playwright install chromium' to install the browser."
            ) from self._error

        self._started = True
        logger.info("Playwright pool started", extra={"pool_size": self.pool_size})

    def submit(self, fn: Callable[[BrowserContext], T], timeout: float = 120.0) -> T:
        """Submit a callable to run on the Playwright thread with a browser context.

        Args:
            fn: A callable that receives a ``BrowserContext`` and returns a result.
            timeout: Max seconds to wait for the result.

        Returns:
            Whatever *fn* returns.

        Raises:
            RuntimeError: If the pool is not started.
            TimeoutError: If the result is not available within *timeout*.
            Exception: Re-raises any exception from *fn*.
        """
        if not self._started:
            raise RuntimeError("Pool not started. Call start() first.")

        future: Future[T] = Future()
        self._work_queue.put((fn, future))
        return future.result(timeout=timeout)

    def stop(self) -> None:
        """Signal the background thread to shut down and wait for it."""
        if not self._started:
            return

        # Sentinel value signals the loop to exit
        self._work_queue.put(None)
        if self._thread:
            self._thread.join(timeout=10)

        self._started = False
        logger.info("Playwright pool stopped")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()

    # ------------------------------------------------------------------
    # Background thread
    # ------------------------------------------------------------------

    def _run_loop(self) -> None:
        """Background thread: start Playwright, then process work items."""
        playwright: Optional[Playwright] = None
        browser: Optional[Browser] = None
        contexts: list[BrowserContext] = []

        try:
            playwright = sync_playwright().start()
            browser = playwright.chromium.launch(headless=True)
            contexts = [browser.new_context() for _ in range(self.pool_size)]
            ctx_queue: queue.Queue[BrowserContext] = queue.Queue()
            for ctx in contexts:
                ctx_queue.put(ctx)
        except Exception as e:
            self._error = e
            self._ready.set()
            return

        # Signal that we're ready
        self._ready.set()

        # Process work items until sentinel
        while True:
            item = self._work_queue.get()
            if item is None:
                break

            fn, future = item
            ctx = ctx_queue.get()
            try:
                result = fn(ctx)
                future.set_result(result)
            except Exception as e:
                future.set_exception(e)
            finally:
                ctx_queue.put(ctx)

        # Cleanup
        for ctx in contexts:
            try:
                ctx.close()
            except Exception:
                pass
        if browser:
            browser.close()
        if playwright:
            playwright.stop()

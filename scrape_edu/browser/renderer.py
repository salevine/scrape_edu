import logging
from pathlib import Path

from scrape_edu.browser.playwright_pool import PlaywrightPool

logger = logging.getLogger("scrape_edu")


class PageRenderer:
    """Render web pages to PDF using Playwright."""

    def __init__(self, pool: PlaywrightPool, navigation_timeout: int = 30000):
        self.pool = pool
        self.navigation_timeout = navigation_timeout  # milliseconds

    def render_to_pdf(
        self,
        url: str,
        dest: Path,
        wait_until: str = "networkidle",
    ) -> Path:
        """Navigate to a URL and save the page as PDF."""
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = dest.with_suffix(dest.suffix + ".tmp")

        ctx = self.pool.acquire()
        try:
            page = ctx.new_page()
            try:
                page.goto(url, wait_until=wait_until, timeout=self.navigation_timeout)
                page.pdf(path=str(tmp_path))
                tmp_path.rename(dest)
                logger.info("Rendered PDF", extra={"url": url, "dest": str(dest)})
                return dest
            finally:
                page.close()
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise
        finally:
            self.pool.release(ctx)

    def render_html_to_pdf(self, html: str, dest: Path) -> Path:
        """Render raw HTML content to PDF."""
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = dest.with_suffix(dest.suffix + ".tmp")

        ctx = self.pool.acquire()
        try:
            page = ctx.new_page()
            try:
                page.set_content(html, wait_until="networkidle", timeout=self.navigation_timeout)
                page.pdf(path=str(tmp_path))
                tmp_path.rename(dest)
                return dest
            finally:
                page.close()
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise
        finally:
            self.pool.release(ctx)

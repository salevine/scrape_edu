"""BFS fallback crawler that explores a university homepage to find relevant pages."""

from __future__ import annotations

import logging
from collections import deque
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scrape_edu.discovery.url_classifier import UrlCategory, classify_url
from scrape_edu.net.http_client import HttpClient
from scrape_edu.utils.url_utils import extract_base_domain, normalize_url

logger = logging.getLogger("scrape_edu")


class HomepageCrawler:
    """Breadth-first crawler that discovers and classifies pages starting
    from a university homepage.

    This is intended as a *fallback* when Serper.dev does not return
    sufficient results for a school.

    Args:
        http_client: An :class:`HttpClient` instance (handles rate
            limiting and retries).
    """

    def __init__(self, http_client: HttpClient) -> None:
        self.client = http_client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def crawl(
        self,
        start_url: str,
        max_pages: int = 50,
        max_depth: int = 3,
    ) -> list[dict]:
        """BFS crawl from *start_url*, classifying discovered pages.

        Args:
            start_url: The URL to begin crawling from.
            max_pages: Stop after visiting this many pages.
            max_depth: Maximum link-following depth from the start page.

        Returns:
            A list of dicts, each with ``url``, ``title``, and
            ``category`` keys.
        """
        visited: set[str] = set()
        results: list[dict] = []
        queue: deque[tuple[str, int]] = deque()  # (url, depth)

        start_normalized = normalize_url(start_url)
        start_base_domain = extract_base_domain(start_normalized)
        queue.append((start_normalized, 0))
        visited.add(start_normalized)

        while queue and len(results) < max_pages:
            url, depth = queue.popleft()

            try:
                response = self.client.get(url)
                html = response.text
                title = self._extract_title(html)
                category = classify_url(url, title=title)

                results.append(
                    {
                        "url": url,
                        "title": title,
                        "category": category,
                    }
                )

                logger.debug(
                    "Crawled page",
                    extra={
                        "url": url,
                        "depth": depth,
                        "category": category.value,
                        "pages_so_far": len(results),
                    },
                )

                # Only follow links if we haven't reached max depth
                if depth < max_depth:
                    links = self._extract_links(html, url)
                    for link in links:
                        normalized = normalize_url(link)
                        if (
                            normalized not in visited
                            and extract_base_domain(normalized) == start_base_domain
                        ):
                            visited.add(normalized)
                            queue.append((normalized, depth + 1))

            except Exception as e:
                logger.warning(
                    "Crawl error", extra={"url": url, "error": str(e)}
                )
                continue

        logger.info(
            "BFS crawl complete",
            extra={"start_url": start_url, "pages_found": len(results)},
        )
        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _extract_title(self, html: str) -> str:
        """Extract the <title> text from an HTML document."""
        soup = BeautifulSoup(html, "lxml")
        title_tag = soup.find("title")
        return title_tag.get_text(strip=True) if title_tag else ""

    def _extract_links(self, html: str, base_url: str) -> list[str]:
        """Extract all absolute HTTP(S) links from an HTML document."""
        soup = BeautifulSoup(html, "lxml")
        links: list[str] = []
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            absolute = urljoin(base_url, href)
            # Skip non-http links (mailto, javascript, tel, etc.)
            if absolute.startswith(("http://", "https://")):
                links.append(absolute)
        return links

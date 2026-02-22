"""Check and log robots.txt for a domain (informational only, not enforced)."""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urljoin

from scrape_edu.net.http_client import HttpClient

logger = logging.getLogger("scrape_edu")


class RobotsChecker:
    """Fetch and parse robots.txt for logging purposes."""

    def __init__(self, http_client: HttpClient) -> None:
        self.client = http_client

    def check(self, base_url: str) -> dict[str, Any]:
        """Fetch and parse robots.txt. Returns metadata dict."""
        robots_url = urljoin(base_url.rstrip("/") + "/", "robots.txt")

        result: dict[str, Any] = {
            "url": robots_url,
            "exists": False,
            "content": None,
            "disallow_patterns": [],
            "crawl_delay": None,
            "sitemaps": [],
        }

        try:
            response = self.client.get(robots_url)
            content = response.text
            result["exists"] = True
            result["content"] = content
            result["disallow_patterns"] = self._extract_disallows(content)
            result["crawl_delay"] = self._extract_crawl_delay(content)
            result["sitemaps"] = self._extract_sitemaps(content)

            logger.info(
                "robots.txt found",
                extra={
                    "url": robots_url,
                    "disallow_count": len(result["disallow_patterns"]),
                    "crawl_delay": result["crawl_delay"],
                },
            )
        except Exception as e:
            logger.info("No robots.txt", extra={"url": robots_url, "error": str(e)})

        return result

    def _extract_disallows(self, content: str) -> list[str]:
        """Extract all Disallow patterns from robots.txt content."""
        patterns = []
        for line in content.splitlines():
            line = line.strip()
            if line.lower().startswith("disallow:"):
                value = line.split(":", 1)[1].strip()
                if value:
                    patterns.append(value)
        return patterns

    def _extract_crawl_delay(self, content: str) -> float | None:
        """Extract Crawl-delay value if present."""
        for line in content.splitlines():
            line = line.strip()
            if line.lower().startswith("crawl-delay:"):
                try:
                    return float(line.split(":", 1)[1].strip())
                except ValueError:
                    pass
        return None

    def _extract_sitemaps(self, content: str) -> list[str]:
        """Extract Sitemap URLs from robots.txt."""
        sitemaps = []
        for line in content.splitlines():
            line = line.strip()
            if line.lower().startswith("sitemap:"):
                # Split only on the first ":" — the URL after it stays intact
                url = line[len("sitemap:"):].strip()
                if url:
                    sitemaps.append(url)
        return sitemaps

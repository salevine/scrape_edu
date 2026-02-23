"""Catalog scraper — download PDF catalogs and render HTML catalogs to PDF."""

from __future__ import annotations

import logging
from collections import deque
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from scrape_edu.data.manifest import SchoolMetadata
from scrape_edu.data.school import School
from scrape_edu.discovery.url_classifier import UrlCategory, classify_url
from scrape_edu.net.http_client import HttpClient
from scrape_edu.scrapers.base import BaseScraper
from scrape_edu.utils.url_utils import is_related_domain, normalize_url

logger = logging.getLogger("scrape_edu")


class CatalogScraper(BaseScraper):
    """Download course catalogs — PDF files directly, HTML pages rendered to PDF."""

    def __init__(
        self,
        http_client: HttpClient,
        config: dict[str, Any],
        renderer=None,  # Optional PageRenderer for HTML-to-PDF
    ):
        super().__init__(http_client, config)
        self.renderer = renderer

    def scrape(
        self,
        school: School,
        school_dir: Path,
        metadata: SchoolMetadata,
    ) -> None:
        """Download/render all catalog URLs for a school, following links to program/course pages."""
        catalog_dir = school_dir / "catalog"
        catalog_dir.mkdir(parents=True, exist_ok=True)

        max_follow_depth = self.config.get("catalog_follow_depth", 1)
        max_followed = self.config.get("catalog_max_followed", 20)

        # Get catalog URLs from metadata (populated during discovery phase)
        seed_urls = self._get_catalog_urls(metadata)

        if not seed_urls:
            logger.info("No catalog URLs found", extra={"school": school.slug})
            return

        # BFS queue: (url, depth) — seed URLs are depth 0
        queue: deque[tuple[str, int]] = deque((url, 0) for url in seed_urls)
        processed: set[str] = set()
        followed_count = 0

        while queue:
            url, depth = queue.popleft()
            normalized = normalize_url(url)

            if normalized in processed:
                continue
            processed.add(normalized)

            if self._skip_if_downloaded(url, metadata):
                continue

            try:
                html_content = None

                if self._is_pdf_url(url):
                    filepath = self._download_pdf(url, catalog_dir)
                else:
                    # Fetch HTML for link extraction before rendering to PDF
                    if depth < max_follow_depth and followed_count < max_followed:
                        try:
                            response = self.client.get(url)
                            html_content = response.text
                        except Exception:
                            pass  # link extraction is best-effort

                    filepath = self._render_html_to_pdf(url, catalog_dir)

                if filepath:
                    metadata.add_downloaded_url(url, str(filepath))
                    metadata.save()
                    logger.info(
                        "Downloaded catalog",
                        extra={
                            "school": school.slug,
                            "url": url,
                            "path": str(filepath),
                            "depth": depth,
                        },
                    )

                # Follow links from HTML pages at allowed depth
                if html_content and depth < max_follow_depth and followed_count < max_followed:
                    new_links = self._extract_catalog_links(html_content, url, school.url)
                    for link in new_links:
                        if normalize_url(link) not in processed:
                            queue.append((link, depth + 1))
                            followed_count += 1
                            if followed_count >= max_followed:
                                break

            except Exception as e:
                logger.warning(
                    "Failed to download catalog",
                    extra={
                        "school": school.slug,
                        "url": url,
                        "error": str(e),
                    },
                )
                # Per-item errors don't stop the phase

    def _get_catalog_urls(self, metadata: SchoolMetadata) -> list[str]:
        """Extract catalog URLs from metadata's discovery results."""
        # Discovery phase stores discovered URLs in metadata
        # Look for URLs classified as "catalog" in the phases data
        discovery = metadata._metadata.get("phases", {}).get("discovery", {})
        urls = discovery.get("catalog_urls", [])
        return urls if isinstance(urls, list) else []

    def _is_pdf_url(self, url: str) -> bool:
        """Check if URL points to a PDF file."""
        parsed = urlparse(url)
        path_lower = parsed.path.lower()
        return path_lower.endswith(".pdf")

    def _download_pdf(self, url: str, catalog_dir: Path) -> Path:
        """Download a PDF file directly."""
        filename = self._url_to_filename(url, ".pdf")
        dest = catalog_dir / filename
        return self.client.download(url, dest)

    def _render_html_to_pdf(self, url: str, catalog_dir: Path) -> Path | None:
        """Render an HTML catalog page to PDF via Playwright."""
        if self.renderer is None:
            logger.warning(
                "No renderer available, skipping HTML catalog",
                extra={"url": url},
            )
            return None

        filename = self._url_to_filename(url, ".pdf")
        dest = catalog_dir / filename
        return self.renderer.render_to_pdf(url, dest)

    def _extract_catalog_links(self, html: str, base_url: str, school_url: str) -> list[str]:
        """Extract links from HTML that classify as CATALOG or COURSE.

        Only returns links on the school's base domain.

        Args:
            html: The HTML content to extract links from.
            base_url: The URL of the page (used to resolve relative links).
            school_url: The school's homepage URL (used for domain filtering).
        """
        soup = BeautifulSoup(html, "lxml")
        links: list[str] = []
        seen: set[str] = set()

        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            absolute = urljoin(base_url, href)
            if not absolute.startswith(("http://", "https://")):
                continue

            normalized = normalize_url(absolute)
            if normalized in seen:
                continue
            seen.add(normalized)

            if not is_related_domain(school_url, normalized):
                continue

            title = a_tag.get_text(strip=True)
            category = classify_url(normalized, title=title)
            if category in (UrlCategory.CATALOG, UrlCategory.COURSE):
                links.append(normalized)

        return links

    # _url_to_filename inherited from BaseScraper

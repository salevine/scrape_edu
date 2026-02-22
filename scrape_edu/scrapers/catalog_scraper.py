"""Catalog scraper — download PDF catalogs and render HTML catalogs to PDF."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from scrape_edu.data.manifest import SchoolMetadata
from scrape_edu.data.school import School
from scrape_edu.net.http_client import HttpClient
from scrape_edu.scrapers.base import BaseScraper

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
        """Download/render all catalog URLs for a school."""
        catalog_dir = school_dir / "catalog"
        catalog_dir.mkdir(parents=True, exist_ok=True)

        # Get catalog URLs from metadata (populated during discovery phase)
        catalog_urls = self._get_catalog_urls(metadata)

        if not catalog_urls:
            logger.info("No catalog URLs found", extra={"school": school.slug})
            return

        for url in catalog_urls:
            if self._skip_if_downloaded(url, metadata):
                continue

            try:
                if self._is_pdf_url(url):
                    filepath = self._download_pdf(url, catalog_dir)
                else:
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
                        },
                    )
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

    def _url_to_filename(self, url: str, ext: str) -> str:
        """Generate a safe filename from a URL."""
        parsed = urlparse(url)
        # Use the last path segment, or domain if no path
        path = parsed.path.rstrip("/")
        if path:
            name = path.split("/")[-1]
            # Remove existing extension if present
            if "." in name:
                name = name.rsplit(".", 1)[0]
        else:
            name = parsed.netloc.replace(".", "-")

        # Clean the name
        safe_name = "".join(
            c if c.isalnum() or c in "-_" else "-" for c in name
        )
        safe_name = safe_name.strip("-") or "catalog"

        return f"{safe_name}{ext}"

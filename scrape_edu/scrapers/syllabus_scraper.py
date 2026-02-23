"""Syllabus scraper — find and download syllabus files."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from scrape_edu.data.manifest import SchoolMetadata
from scrape_edu.data.models import SyllabusRecord
from scrape_edu.data.school import School
from scrape_edu.net.http_client import HttpClient
from scrape_edu.scrapers.base import BaseScraper

logger = logging.getLogger("scrape_edu")


class SyllabusScraper(BaseScraper):
    """Find and download syllabus files (PDFs, docs) from course/faculty pages."""

    def scrape(
        self,
        school: School,
        school_dir: Path,
        metadata: SchoolMetadata,
    ) -> None:
        """Find and download syllabi for a school."""
        syllabi_dir = school_dir / "syllabi"
        syllabi_dir.mkdir(parents=True, exist_ok=True)

        # Get syllabus URLs from discovery, plus scan faculty pages for links
        syllabus_urls = self._get_syllabus_urls(metadata)

        # Also scan downloaded faculty pages for syllabus links
        faculty_dir = school_dir / "faculty"
        if faculty_dir.exists():
            for html_file in faculty_dir.glob("*.html"):
                try:
                    html = html_file.read_text(encoding="utf-8")
                    found = self._extract_syllabus_links(html, school.url)
                    syllabus_urls.extend(found)
                except Exception as e:
                    logger.debug(
                        "Error scanning faculty HTML",
                        extra={"file": str(html_file), "error": str(e)},
                    )

        # Deduplicate
        seen: set[str] = set()
        unique_urls: list[str] = []
        for url in syllabus_urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)

        if not unique_urls:
            logger.info(
                "No syllabus URLs found", extra={"school": school.slug}
            )
            return

        for url in unique_urls:
            if self._skip_if_downloaded(url, metadata):
                continue

            try:
                ext = self._get_url_extension(url)
                filename = self._url_to_filename(url, ext)
                dest = syllabi_dir / filename
                self.client.download(url, dest)

                metadata.add_downloaded_url(url, str(dest))
                metadata.save()
                logger.info(
                    "Downloaded syllabus",
                    extra={"school": school.slug, "url": url},
                )
            except Exception as e:
                logger.warning(
                    "Failed to download syllabus",
                    extra={
                        "school": school.slug,
                        "url": url,
                        "error": str(e),
                    },
                )

    def _get_syllabus_urls(self, metadata: SchoolMetadata) -> list[str]:
        """Get syllabus URLs from discovery phase data."""
        discovery = metadata._metadata.get("phases", {}).get("discovery", {})
        urls = discovery.get("syllabus_urls", [])
        return list(urls) if isinstance(urls, list) else []

    def _extract_syllabus_links(self, html: str, base_url: str) -> list[str]:
        """Extract syllabus-related links from an HTML page."""
        soup = BeautifulSoup(html, "lxml")
        links: list[str] = []

        syllabus_keywords = {
            "syllabus",
            "syllabi",
            "course outline",
            "course-outline",
        }

        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            text = a_tag.get_text(strip=True).lower()
            href_lower = href.lower()

            # Check if the link text or URL contains syllabus-related keywords
            is_syllabus = any(kw in text for kw in syllabus_keywords) or any(
                kw in href_lower for kw in syllabus_keywords
            )

            # Also check for PDF/doc links with syllabus in the name
            is_doc = href_lower.endswith((".pdf", ".doc", ".docx"))

            if is_syllabus or (
                is_doc and any(kw in href_lower for kw in syllabus_keywords)
            ):
                absolute = urljoin(base_url, href)
                if absolute.startswith(("http://", "https://")):
                    links.append(absolute)

        return links

    # _url_to_filename inherited from BaseScraper

    @staticmethod
    def _get_url_extension(url: str) -> str:
        """Extract the file extension from a URL, defaulting to .pdf."""
        path = urlparse(url).path.rstrip("/")
        if path:
            last_segment = path.split("/")[-1]
            if "." in last_segment:
                return "." + last_segment.rsplit(".", 1)[1].lower()
        return ".pdf"

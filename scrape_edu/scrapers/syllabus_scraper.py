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
from scrape_edu.utils.url_utils import is_related_domain

logger = logging.getLogger("scrape_edu")

# File extensions that are direct downloads (not HTML pages to follow)
_DIRECT_FILE_EXTENSIONS = frozenset({
    ".pdf", ".doc", ".docx", ".ppt", ".pptx",
    ".xls", ".xlsx", ".rtf", ".odt", ".txt",
})


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

        # Also scan downloaded faculty pages for syllabus links.
        # Build a reverse lookup (filepath → URL) so we can resolve relative
        # links against the faculty page's actual URL, not the school homepage.
        filepath_to_url = self._build_filepath_to_url(metadata)
        faculty_dir = school_dir / "faculty"
        if faculty_dir.exists():
            for html_file in faculty_dir.glob("*.html"):
                try:
                    html = html_file.read_text(encoding="utf-8")
                    base_url = filepath_to_url.get(
                        str(html_file), school.url
                    )
                    found = self._extract_syllabus_links(html, base_url)
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

        # Follow HTML pages one level deep to find actual file links.
        # e.g. a "Syllabi Archive" .php page listing dozens of PDFs.
        max_followed = self.config.get("syllabus_max_followed", 20)
        file_urls, page_urls = self._split_files_and_pages(unique_urls)
        followed_file_urls = self._follow_syllabus_pages(
            page_urls, school, max_followed,
        )

        # Merge: page URLs (download as-is) + direct file URLs + newly found file URLs
        all_urls: list[str] = []
        all_seen: set[str] = set()
        for url in page_urls + file_urls + followed_file_urls:
            if url not in all_seen:
                all_seen.add(url)
                all_urls.append(url)

        if not all_urls:
            logger.info(
                "No syllabus URLs found", extra={"school": school.slug}
            )
            return

        for url in all_urls:
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

    @staticmethod
    def _build_filepath_to_url(metadata: SchoolMetadata) -> dict[str, str]:
        """Build a reverse mapping from filepath to source URL.

        Used to find the original URL for downloaded faculty HTML files so
        that relative links within them can be resolved correctly.

        Stores both the raw filepath and the resolved absolute path as keys
        so lookups work regardless of whether the caller uses relative or
        absolute paths.
        """
        mapping: dict[str, str] = {}
        for url, info in metadata._metadata.get("downloaded_urls", {}).items():
            fp = info.get("filepath", "")
            if fp:
                mapping[fp] = url
                mapping[str(Path(fp).resolve())] = url
        return mapping

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
    def _is_direct_file(url: str) -> bool:
        """Return True if the URL points to a downloadable file (PDF, doc, etc.)."""
        path = urlparse(url).path.lower().rstrip("/")
        return any(path.endswith(ext) for ext in _DIRECT_FILE_EXTENSIONS)

    @staticmethod
    def _split_files_and_pages(
        urls: list[str],
    ) -> tuple[list[str], list[str]]:
        """Split URLs into direct file downloads and HTML pages.

        Returns:
            (file_urls, page_urls) tuple.
        """
        files: list[str] = []
        pages: list[str] = []
        for url in urls:
            if SyllabusScraper._is_direct_file(url):
                files.append(url)
            else:
                pages.append(url)
        return files, pages

    def _follow_syllabus_pages(
        self,
        page_urls: list[str],
        school: School,
        max_followed: int,
    ) -> list[str]:
        """Fetch HTML syllabus pages and extract direct file links from them.

        This handles the common case where a syllabus link from a faculty page
        points to an intermediate HTML page (e.g. ``old_syllabus_schedule.php``)
        that itself contains links to actual PDF/doc syllabus files.

        Args:
            page_urls: URLs of HTML pages to follow.
            school: The school (used for domain filtering).
            max_followed: Maximum number of pages to fetch.

        Returns:
            List of direct file URLs found on the followed pages.
        """
        found_files: list[str] = []
        followed = 0

        for page_url in page_urls:
            if followed >= max_followed:
                break
            try:
                response = self.client.get(page_url)
                html = response.text
                followed += 1

                # Extract syllabus links using the page's own URL as base
                links = self._extract_syllabus_links(html, page_url)

                for link in links:
                    # Only keep direct file links on the school's domain
                    if self._is_direct_file(link) and is_related_domain(
                        school.url, link
                    ):
                        found_files.append(link)

                if links:
                    logger.info(
                        "Followed syllabus page",
                        extra={
                            "school": school.slug,
                            "page_url": page_url,
                            "files_found": len(
                                [l for l in links if self._is_direct_file(l)]
                            ),
                        },
                    )
            except Exception as e:
                logger.debug(
                    "Failed to follow syllabus page",
                    extra={
                        "school": school.slug,
                        "url": page_url,
                        "error": str(e),
                    },
                )

        return found_files

    @staticmethod
    def _get_url_extension(url: str) -> str:
        """Extract the file extension from a URL, defaulting to .pdf."""
        path = urlparse(url).path.rstrip("/")
        if path:
            last_segment = path.split("/")[-1]
            if "." in last_segment:
                return "." + last_segment.rsplit(".", 1)[1].lower()
        return ".pdf"

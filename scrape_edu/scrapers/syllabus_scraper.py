"""Syllabus scraper — find and download syllabus files."""

from __future__ import annotations

import logging
import re
from collections import deque
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

        # BFS through HTML pages to find actual file links.
        # Supports multi-level following (e.g. listing → course page → PDF).
        max_followed = self.config.get("syllabus_max_followed", 50)
        max_depth = self.config.get("syllabus_follow_depth", 2)
        file_urls, page_urls = self._split_files_and_pages(unique_urls)
        followed_file_urls = self._follow_syllabus_pages(
            page_urls, school, max_followed, max_depth,
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

    # Patterns for detecting individual course page links
    _COURSE_TEXT_PATTERN = re.compile(r"^[A-Z]{2,6}\s*\d{3,5}")
    _COURSE_PATH_PATTERN = re.compile(r"/courses?/", re.IGNORECASE)

    def _extract_course_links(
        self, html: str, base_url: str, school_url: str
    ) -> list[str]:
        """Extract individual course page links from an HTML page.

        Detects links using two signals:
        - Path pattern: URL path contains ``/courses/`` or ``/course/``
        - Text pattern: Link text matches a course code like ``CSCI 1302``

        Only returns links on the school's related domain.
        """
        soup = BeautifulSoup(html, "lxml")
        links: list[str] = []
        seen: set[str] = set()

        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            text = a_tag.get_text(strip=True)
            absolute = urljoin(base_url, href)

            if not absolute.startswith(("http://", "https://")):
                continue
            if absolute in seen:
                continue

            # Check path pattern or text pattern
            path = urlparse(absolute).path
            is_course = bool(self._COURSE_PATH_PATTERN.search(path)) or bool(
                self._COURSE_TEXT_PATTERN.match(text)
            )

            if is_course and is_related_domain(school_url, absolute):
                seen.add(absolute)
                links.append(absolute)

        return links

    def _extract_file_links(
        self, html: str, base_url: str, school_url: str
    ) -> list[str]:
        """Extract all direct file links (PDF, doc, etc.) from an HTML page.

        Unlike ``_extract_syllabus_links``, this does NOT require syllabus
        keywords — it returns every downloadable file on the school's domain.
        Used for course pages where the filename may not contain "syllabus".
        """
        soup = BeautifulSoup(html, "lxml")
        links: list[str] = []
        seen: set[str] = set()

        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            absolute = urljoin(base_url, href)

            if not absolute.startswith(("http://", "https://")):
                continue
            if absolute in seen:
                continue

            if self._is_direct_file(absolute) and is_related_domain(
                school_url, absolute
            ):
                seen.add(absolute)
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
        max_depth: int = 1,
    ) -> list[str]:
        """BFS through syllabus pages to find direct file links.

        Handles multi-level patterns:
        - Level 0: seed pages (e.g. "Course Syllabi" listing)
        - Level 1: archive pages or individual course pages
        - Level 2: final file links from course pages

        At depth 0, only keyword-based syllabus link extraction is used.
        At depth > 0, broader file extraction (any PDF/doc on domain) is
        also applied so that files without "syllabus" in the name are found.

        When a page yields fewer than 3 direct file links, course link
        extraction is also tried (pattern-based), adding course pages to
        the BFS queue at depth + 1.

        Args:
            page_urls: Seed URLs of HTML pages to follow.
            school: The school (used for domain filtering).
            max_followed: Maximum number of pages to fetch.
            max_depth: Maximum BFS depth (0 = seed pages only).

        Returns:
            List of direct file URLs found across all levels.
        """
        queue: deque[tuple[str, int]] = deque(
            (url, 0) for url in page_urls
        )
        processed: set[str] = set()
        found_files: list[str] = []
        found_files_set: set[str] = set()
        followed = 0

        while queue and followed < max_followed:
            url, depth = queue.popleft()
            if url in processed:
                continue
            processed.add(url)

            try:
                response = self.client.get(url)
                html = response.text
                followed += 1
            except Exception as e:
                logger.debug(
                    "Failed to follow syllabus page",
                    extra={
                        "school": school.slug,
                        "url": url,
                        "error": str(e),
                    },
                )
                continue

            # --- Extract syllabus links (keyword-based) ---
            syl_links = self._extract_syllabus_links(html, url)
            file_count = 0

            for link in syl_links:
                if self._is_direct_file(link) and is_related_domain(
                    school.url, link
                ):
                    if link not in found_files_set:
                        found_files_set.add(link)
                        found_files.append(link)
                    file_count += 1
                elif link not in processed and depth + 1 <= max_depth:
                    queue.append((link, depth + 1))

            # --- At depth > 0, also try broader file extraction ---
            if depth > 0:
                broad_links = self._extract_file_links(html, url, school.url)
                for link in broad_links:
                    if link not in found_files_set:
                        found_files_set.add(link)
                        found_files.append(link)
                    file_count += 1

            # --- If few files found, try course link extraction ---
            if file_count < 3 and depth < max_depth:
                course_links = self._extract_course_links(
                    html, url, school.url
                )
                for link in course_links:
                    if link not in processed:
                        queue.append((link, depth + 1))

            if found_files or syl_links:
                logger.info(
                    "Followed syllabus page",
                    extra={
                        "school": school.slug,
                        "page_url": url,
                        "depth": depth,
                        "files_found": file_count,
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

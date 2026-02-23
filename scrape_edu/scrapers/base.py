"""Abstract base scraper."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from scrape_edu.data.manifest import SchoolMetadata
from scrape_edu.data.school import School
from scrape_edu.net.http_client import HttpClient
from scrape_edu.utils.url_utils import extract_base_domain

logger = logging.getLogger("scrape_edu")


class BaseScraper(ABC):
    """Abstract base for all scraper implementations.

    Subclasses must implement the `scrape` method which performs
    the actual scraping logic for a given school.
    """

    def __init__(self, http_client: HttpClient, config: dict[str, Any]):
        self.client = http_client
        self.config = config

    @abstractmethod
    def scrape(
        self,
        school: School,
        school_dir: Path,
        metadata: SchoolMetadata,
    ) -> None:
        """Scrape data for a school.

        Args:
            school: The school to scrape.
            school_dir: Output directory for this school.
            metadata: School metadata manager for tracking progress.

        Raises:
            Exception: On fatal errors that should fail the phase.
        """
        ...

    def _skip_if_downloaded(self, url: str, metadata: SchoolMetadata) -> bool:
        """Check if a URL has already been downloaded."""
        if metadata.is_url_downloaded(url):
            logger.debug("Skipping already downloaded URL", extra={"url": url})
            return True
        return False

    @staticmethod
    def _url_to_filename(url: str, ext: str) -> str:
        """Generate a safe, unique filename from a URL.

        Includes the subdomain prefix when the URL is on a subdomain so that
        URLs with identical paths on different subdomains (e.g.
        ``scs.gatech.edu/people/faculty`` vs ``cc.gatech.edu/people/faculty``)
        produce distinct filenames.

        Args:
            url: The source URL.
            ext: File extension to use (e.g. ``".html"``, ``".pdf"``).

        Returns:
            A filesystem-safe filename like ``"scs--faculty.html"``.
        """
        parsed = urlparse(url)
        hostname = (parsed.hostname or "").lower()
        if hostname.startswith("www."):
            hostname = hostname[4:]

        # Derive the name from the last path segment
        path = parsed.path.rstrip("/")
        if path:
            name = path.split("/")[-1]
            # Remove existing extension if present
            if "." in name:
                name = name.rsplit(".", 1)[0]
        else:
            name = hostname.replace(".", "-")

        # Clean the name
        safe_name = "".join(
            c if c.isalnum() or c in "-_" else "-" for c in name
        ).strip("-")

        # Prefix with subdomain when URL has one to avoid collisions
        base_domain = extract_base_domain(url)
        if hostname and hostname != base_domain:
            # e.g. "scs.gatech.edu" → prefix "scs"
            prefix = hostname.removesuffix(f".{base_domain}")
            safe_prefix = "".join(
                c if c.isalnum() or c in "-_" else "-" for c in prefix
            ).strip("-")
            if safe_prefix:
                safe_name = f"{safe_prefix}--{safe_name}" if safe_name else safe_prefix

        safe_name = safe_name or "page"
        return f"{safe_name}{ext}"

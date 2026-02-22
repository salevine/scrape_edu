"""Abstract base scraper."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from scrape_edu.data.manifest import SchoolMetadata
from scrape_edu.data.school import School
from scrape_edu.net.http_client import HttpClient

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

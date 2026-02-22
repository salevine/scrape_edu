"""Factory for creating phase handler functions from scraper instances."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from scrape_edu.data.manifest import SchoolMetadata
from scrape_edu.data.school import School
from scrape_edu.discovery.serper_search import SerperClient
from scrape_edu.discovery.url_classifier import classify_search_results
from scrape_edu.net.http_client import HttpClient
from scrape_edu.pipeline.phases import Phase
from scrape_edu.scrapers.catalog_scraper import CatalogScraper
from scrape_edu.scrapers.faculty_scraper import FacultyScraper
from scrape_edu.scrapers.robots_checker import RobotsChecker
from scrape_edu.scrapers.syllabus_scraper import SyllabusScraper

logger = logging.getLogger("scrape_edu")


def build_phase_handlers(
    http_client: HttpClient,
    config: dict[str, Any],
    serper_client: SerperClient | None = None,
    renderer=None,
) -> dict[Phase, Any]:
    """Create phase handler callables for each pipeline phase.

    Each handler has the signature:
        handler(school, school_dir, metadata, config) -> None

    Args:
        http_client: Shared HTTP client with rate limiting.
        config: Pipeline configuration dict.
        serper_client: Serper.dev search client (None disables search-based discovery).
        renderer: Optional PageRenderer for HTML-to-PDF.

    Returns:
        Dict mapping Phase enum to handler callables.
    """
    robots_checker = RobotsChecker(http_client)
    catalog_scraper = CatalogScraper(http_client, config, renderer=renderer)
    faculty_scraper = FacultyScraper(http_client, config)
    syllabus_scraper = SyllabusScraper(http_client, config)

    def handle_robots(
        school: School, school_dir: Path, metadata: SchoolMetadata, config: dict
    ) -> None:
        result = robots_checker.check(school.url)
        # Store robots.txt info in the discovery phase data
        discovery_data = metadata._metadata.setdefault("phases", {}).setdefault("robots", {})
        discovery_data["robots_info"] = {
            "url": result["url"],
            "exists": result["exists"],
            "disallow_count": len(result.get("disallow_patterns", [])),
            "crawl_delay": result.get("crawl_delay"),
            "sitemaps": result.get("sitemaps", []),
        }

    def handle_discovery(
        school: School, school_dir: Path, metadata: SchoolMetadata, config: dict
    ) -> None:
        catalog_urls = []
        faculty_urls = []
        syllabus_urls = []

        if serper_client:
            results = serper_client.search_school(school.name, school.url)
            all_results = results.get("cs_results", []) + results.get("ds_results", [])

            classified = classify_search_results(all_results)
            catalog_urls = [r["link"] for r in classified.get("catalog", []) if "link" in r]
            faculty_urls = [r["link"] for r in classified.get("faculty", []) if "link" in r]
            syllabus_urls = [r["link"] for r in classified.get("syllabus", []) if "link" in r]

        # Store discovered URLs in discovery phase data for downstream scrapers
        discovery_data = metadata._metadata.setdefault("phases", {}).setdefault("discovery", {})
        discovery_data["catalog_urls"] = catalog_urls
        discovery_data["faculty_urls"] = faculty_urls
        discovery_data["syllabus_urls"] = syllabus_urls

    def handle_catalog(
        school: School, school_dir: Path, metadata: SchoolMetadata, config: dict
    ) -> None:
        catalog_scraper.scrape(school, school_dir, metadata)

    def handle_faculty(
        school: School, school_dir: Path, metadata: SchoolMetadata, config: dict
    ) -> None:
        faculty_scraper.scrape(school, school_dir, metadata)

    def handle_syllabi(
        school: School, school_dir: Path, metadata: SchoolMetadata, config: dict
    ) -> None:
        syllabus_scraper.scrape(school, school_dir, metadata)

    return {
        Phase.ROBOTS: handle_robots,
        Phase.DISCOVERY: handle_discovery,
        Phase.CATALOG: handle_catalog,
        Phase.FACULTY: handle_faculty,
        Phase.SYLLABI: handle_syllabi,
    }

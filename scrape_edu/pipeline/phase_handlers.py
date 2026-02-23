"""Factory for creating phase handler functions from scraper instances."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from scrape_edu.data.manifest import SchoolMetadata
from scrape_edu.data.school import School
from scrape_edu.discovery.homepage_crawler import HomepageCrawler
from scrape_edu.discovery.serper_search import SerperClient
from scrape_edu.discovery.url_classifier import UrlCategory, classify_search_results, classify_url
from scrape_edu.net.http_client import HttpClient
from scrape_edu.pipeline.phases import Phase
from scrape_edu.scrapers.catalog_scraper import CatalogScraper
from scrape_edu.scrapers.faculty_scraper import FacultyScraper
from scrape_edu.scrapers.robots_checker import RobotsChecker
from scrape_edu.scrapers.syllabus_scraper import SyllabusScraper
from scrape_edu.utils.url_utils import extract_base_domain, is_related_domain

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
    homepage_crawler = HomepageCrawler(http_client)
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
            "disallow_patterns": result.get("disallow_patterns", []),
            "crawl_delay": result.get("crawl_delay"),
            "sitemaps": result.get("sitemaps", []),
        }

    def _is_school_url(url: str, school_base_domain: str) -> bool:
        """Return True if *url* belongs to the school's base domain."""
        return extract_base_domain(url) == school_base_domain

    def handle_discovery(
        school: School, school_dir: Path, metadata: SchoolMetadata, config: dict
    ) -> None:
        catalog_urls: list[str] = []
        faculty_urls: list[str] = []
        syllabus_urls: list[str] = []
        discovery_method = "serper"
        school_base = extract_base_domain(school.url)

        # Pass 1: Serper search
        if serper_client:
            results = serper_client.search_school(school.name, school.url)
            # Consume all result keys (cs, ds, faculty, site-scoped)
            all_results: list[dict] = []
            for key in results:
                all_results.extend(results[key])

            classified = classify_search_results(all_results)
            catalog_urls = [
                r["link"] for r in classified.get("catalog", [])
                if "link" in r and _is_school_url(r["link"], school_base)
            ]
            faculty_urls = [
                r["link"] for r in classified.get("faculty", [])
                if "link" in r and _is_school_url(r["link"], school_base)
            ]
            syllabus_urls = [
                r["link"] for r in classified.get("syllabus", [])
                if "link" in r and _is_school_url(r["link"], school_base)
            ]

        # Pass 1b: Probe catalog.{base_domain} if no catalog URLs found
        if not catalog_urls:
            probe_url = f"https://catalog.{school_base}/"
            try:
                resp = http_client.get(probe_url)
                cat = classify_url(probe_url, title=resp.text[:500] if resp.text else "")
                # Accept if it classified as catalog, or just trust the subdomain
                catalog_urls.append(probe_url)
                logger.info(
                    "Catalog subdomain probe succeeded",
                    extra={"school": school.slug, "url": probe_url},
                )
            except Exception:
                logger.debug(
                    "Catalog subdomain probe failed",
                    extra={"school": school.slug, "url": probe_url},
                )

        # Pass 2: BFS fallback if Serper results are sparse
        has_catalog = len(catalog_urls) >= 1
        has_faculty = len(faculty_urls) >= 1
        if not has_catalog or not has_faculty:
            logger.info(
                "Serper results sparse, running BFS fallback",
                extra={
                    "school": school.slug,
                    "catalog_count": len(catalog_urls),
                    "faculty_count": len(faculty_urls),
                },
            )
            crawled = homepage_crawler.crawl(school.url)
            for page in crawled:
                cat = page["category"]
                url = page["url"]
                if cat == UrlCategory.CATALOG and url not in catalog_urls:
                    catalog_urls.append(url)
                elif cat == UrlCategory.FACULTY and url not in faculty_urls:
                    faculty_urls.append(url)
                elif cat == UrlCategory.SYLLABUS and url not in syllabus_urls:
                    syllabus_urls.append(url)
            discovery_method = "serper+bfs" if serper_client else "bfs"

        # Deduplicate all URL lists before storing
        catalog_urls = list(dict.fromkeys(catalog_urls))
        faculty_urls = list(dict.fromkeys(faculty_urls))
        syllabus_urls = list(dict.fromkeys(syllabus_urls))

        # Store discovered URLs in discovery phase data for downstream scrapers
        discovery_data = metadata._metadata.setdefault("phases", {}).setdefault("discovery", {})
        discovery_data["catalog_urls"] = catalog_urls
        discovery_data["faculty_urls"] = faculty_urls
        discovery_data["syllabus_urls"] = syllabus_urls
        discovery_data["discovery_method"] = discovery_method

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

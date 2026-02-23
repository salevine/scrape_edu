"""Faculty scraper — download profile pages, parse to JSON best-effort."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any
from bs4 import BeautifulSoup

from scrape_edu.data.manifest import SchoolMetadata
from scrape_edu.data.models import FacultyMember
from scrape_edu.data.school import School
from scrape_edu.net.http_client import HttpClient
from scrape_edu.scrapers.base import BaseScraper

logger = logging.getLogger("scrape_edu")


class FacultyScraper(BaseScraper):
    """Download faculty directory pages and parse member profiles."""

    def scrape(
        self,
        school: School,
        school_dir: Path,
        metadata: SchoolMetadata,
    ) -> None:
        """Download faculty pages and attempt to parse member data."""
        faculty_dir = school_dir / "faculty"
        faculty_dir.mkdir(parents=True, exist_ok=True)

        # Get faculty URLs from discovery phase
        faculty_urls = self._get_faculty_urls(metadata)

        if not faculty_urls:
            logger.info("No faculty URLs found", extra={"school": school.slug})
            return

        for url in faculty_urls:
            if self._skip_if_downloaded(url, metadata):
                continue

            try:
                # Download the HTML
                response = self.client.get(url)
                html_content = response.text

                # Save raw HTML
                html_filename = self._url_to_filename(url, ".html")
                html_path = faculty_dir / html_filename
                html_path.write_text(html_content, encoding="utf-8")

                # Best-effort parse to JSON
                try:
                    members = self._parse_faculty_page(html_content, url)
                    if members:
                        json_filename = self._url_to_filename(url, ".json")
                        json_path = faculty_dir / json_filename
                        json_data = [m.model_dump() for m in members]
                        json_path.write_text(
                            json.dumps(json_data, indent=2, ensure_ascii=False)
                            + "\n",
                            encoding="utf-8",
                        )
                except Exception as e:
                    # Parse failure: keep HTML, log error, continue
                    logger.warning(
                        "Faculty parse failed (HTML saved)",
                        extra={
                            "school": school.slug,
                            "url": url,
                            "error": str(e),
                        },
                    )

                metadata.add_downloaded_url(url, str(html_path))
                metadata.save()

            except Exception as e:
                logger.warning(
                    "Failed to download faculty page",
                    extra={
                        "school": school.slug,
                        "url": url,
                        "error": str(e),
                    },
                )

    def _get_faculty_urls(self, metadata: SchoolMetadata) -> list[str]:
        """Extract faculty URLs from discovery results."""
        discovery = metadata._metadata.get("phases", {}).get("discovery", {})
        urls = discovery.get("faculty_urls", [])
        return urls if isinstance(urls, list) else []

    def _parse_faculty_page(
        self, html: str, source_url: str
    ) -> list[FacultyMember]:
        """Best-effort parse of a faculty directory page.

        Tries multiple common patterns used by university websites.
        Returns whatever it can find; may return empty list.
        """
        soup = BeautifulSoup(html, "lxml")
        members: list[FacultyMember] = []

        # Strategy 1: Look for common faculty card/list patterns
        # Many sites use divs with classes like "faculty-member", "people-card", etc.
        selectors = [
            "div.faculty-member",
            "div.people-card",
            "div.faculty-card",
            "div.person",
            "div.staff-member",
            "div.faculty",
            "li.faculty-member",
            "li.person",
            "tr.faculty-row",
            "article.person",
            "div.views-row",  # Drupal
        ]

        for selector in selectors:
            cards = soup.select(selector)
            if cards:
                for card in cards:
                    member = self._parse_card(card, source_url)
                    if member:
                        members.append(member)
                break  # Use first matching selector

        # Strategy 2: If no cards found, look for a list of links with names
        if not members:
            # Look for links within common containers
            for container in soup.select(
                "div.faculty, div.people, ul.faculty-list, div#faculty"
            ):
                links = container.find_all("a", href=True)
                for link in links:
                    name = link.get_text(strip=True)
                    if name and len(name.split()) >= 2:  # At least first+last name
                        members.append(
                            FacultyMember(
                                name=name,
                                profile_url=link["href"],
                            )
                        )

        return members

    def _parse_card(self, card, source_url: str) -> FacultyMember | None:
        """Parse a single faculty card/div into a FacultyMember."""
        # Try to find name
        name_tag = card.find(
            ["h2", "h3", "h4", "a", "strong", "span"],
            class_=re.compile(r"name|title", re.I),
        )
        if not name_tag:
            name_tag = card.find(["h2", "h3", "h4"])

        if not name_tag:
            return None

        name = name_tag.get_text(strip=True)
        if not name or len(name) < 2:
            return None

        # Try to find title/position
        title = ""
        title_tag = card.find(class_=re.compile(r"title|position|role", re.I))
        if title_tag and title_tag != name_tag:
            title = title_tag.get_text(strip=True)

        # Try to find email
        email = ""
        email_link = card.find("a", href=re.compile(r"mailto:", re.I))
        if email_link:
            email = email_link["href"].replace("mailto:", "").strip()

        # Profile URL
        profile_url = ""
        link = card.find("a", href=True)
        if link:
            profile_url = link["href"]

        return FacultyMember(
            name=name,
            title=title,
            email=email,
            profile_url=profile_url,
        )

    # _url_to_filename inherited from BaseScraper

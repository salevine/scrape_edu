"""Serper.dev search API client with quota tracking."""

from __future__ import annotations

import logging
from typing import Any

import requests

from scrape_edu.utils.url_utils import extract_domain

logger = logging.getLogger("scrape_edu")


class SerperClient:
    """Wrapper around the Serper.dev Google Search API.

    Provides school-oriented search helpers and tracks query usage
    so callers can monitor quota consumption.

    Usage::

        client = SerperClient(api_key="sk-...")
        results = client.search_school("MIT", "https://mit.edu")
        print(client.queries_used)
    """

    SEARCH_URL = "https://google.serper.dev/search"

    def __init__(self, api_key: str, queries_per_school: int = 5) -> None:
        self.api_key = api_key
        self.queries_per_school = queries_per_school
        self._queries_used = 0

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def queries_used(self) -> int:
        """Total number of queries made so far."""
        return self._queries_used

    @property
    def queries_remaining(self) -> int | None:
        """Remaining quota.  Returns ``None`` because Serper does not
        expose this in the response headers by default."""
        return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def search(self, query: str, num_results: int = 10) -> list[dict[str, Any]]:
        """Execute a single search query against the Serper API.

        Args:
            query: The search string.
            num_results: Maximum number of organic results to request.

        Returns:
            A list of organic result dicts, each containing at least
            ``title``, ``link``, and ``snippet`` keys.  Returns an
            empty list on HTTP or network errors.
        """
        headers = {
            "X-API-KEY": self.api_key,
            "Content-Type": "application/json",
        }
        payload = {"q": query, "num": num_results}

        logger.info("Serper search", extra={"query": query, "num_results": num_results})

        try:
            resp = requests.post(
                self.SEARCH_URL, headers=headers, json=payload, timeout=15
            )
            resp.raise_for_status()
            self._queries_used += 1

            data = resp.json()
            return data.get("organic", [])
        except requests.RequestException as e:
            logger.error(
                "Serper search failed", extra={"query": query, "error": str(e)}
            )
            self._queries_used += 1  # still counts against quota
            return []

    def search_school(
        self, school_name: str, school_url: str
    ) -> dict[str, list[dict[str, Any]]]:
        """Run up to 5 targeted searches for a single school.

        Queries:
            1. CS courses catalog (by name)
            2. DS program catalog (by name)
            3. CS faculty directory (by name)
            4. Site-scoped CS course catalog
            5. Site-scoped CS syllabus

        Args:
            school_name: Human-readable name, e.g. ``"MIT"``.
            school_url: The school's homepage URL.

        Returns:
            Dict with result keys ``cs_results``, ``ds_results``,
            ``faculty_results``, ``site_catalog_results``, and
            ``site_syllabus_results``.
        """
        domain = extract_domain(school_url)

        queries = {
            "cs_results": f"{school_name} computer science courses catalog",
            "ds_results": f"{school_name} data science program courses catalog",
            "faculty_results": f"{school_name} computer science faculty directory",
            "site_catalog_results": f"site:{domain} computer science course catalog",
            "site_syllabus_results": f"site:{domain} computer science syllabus",
        }

        logger.info(
            "Searching for school",
            extra={"school_name": school_name, "school_url": school_url,
                   "num_queries": len(queries)},
        )

        results: dict[str, list[dict[str, Any]]] = {}
        for key, query in queries.items():
            results[key] = self.search(query)

        return results

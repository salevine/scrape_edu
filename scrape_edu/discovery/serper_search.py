"""Serper.dev search API client with quota tracking."""

from __future__ import annotations

import logging
from typing import Any

import requests

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

    def __init__(self, api_key: str, queries_per_school: int = 2) -> None:
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
        """Run CS and DS searches for a single school.

        Args:
            school_name: Human-readable name, e.g. ``"MIT"``.
            school_url: The school's homepage URL (for context/logging).

        Returns:
            Dict with ``cs_results`` and ``ds_results`` keys, each
            containing a list of organic search results.
        """
        cs_query = f"{school_name} computer science department courses catalog"
        ds_query = f"{school_name} data science program courses catalog"

        logger.info(
            "Searching for school",
            extra={"school_name": school_name, "school_url": school_url},
        )

        return {
            "cs_results": self.search(cs_query),
            "ds_results": self.search(ds_query),
        }

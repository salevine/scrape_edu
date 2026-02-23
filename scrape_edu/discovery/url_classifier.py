"""Heuristic URL classification -- determine what type of page a URL likely points to."""

from __future__ import annotations

import re
from enum import Enum
from urllib.parse import parse_qs, urlparse


class UrlCategory(str, Enum):
    """Broad categories for academic web pages."""

    CATALOG = "catalog"
    FACULTY = "faculty"
    SYLLABUS = "syllabus"
    DEPARTMENT = "department"
    COURSE = "course"
    UNKNOWN = "unknown"


# ------------------------------------------------------------------
# Pattern lists (compiled regexes for performance)
# ------------------------------------------------------------------

CATALOG_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"catalog",
        r"bulletin",
        r"courselist",
        r"course.?list",
        r"course.?descriptions?",
        r"acalog",
        r"courseleaf",
        r"academic.?catalog",
        r"preview_program",
        r"preview_entity",
        r"preview_course",
        r"smartcatalogiq",
    ]
]

FACULTY_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"faculty",
        r"people",
        r"/directory",
        r"/staff",
        r"professors?",
        r"department/people",
        r"our.?people",
    ]
]

SYLLABUS_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"syllab",
        r"course.?outline",
        r"course.?materials?",
    ]
]

DEPARTMENT_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"department",
        r"/dept",
        r"school.?of",
        r"/cs/?$",
        r"/cse/?$",
        r"computer.?science",
        r"data.?science",
        r"computing",
    ]
]

COURSE_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"/course",
        r"/class",
        r"/section",
    ]
]

# Ordered by specificity -- more specific categories first so they win.
_CATEGORY_PATTERNS: list[tuple[UrlCategory, list[re.Pattern[str]]]] = [
    (UrlCategory.SYLLABUS, SYLLABUS_PATTERNS),
    (UrlCategory.CATALOG, CATALOG_PATTERNS),
    (UrlCategory.FACULTY, FACULTY_PATTERNS),
    (UrlCategory.COURSE, COURSE_PATTERNS),
    (UrlCategory.DEPARTMENT, DEPARTMENT_PATTERNS),
]

# Query parameters that indicate a dynamic catalog system (e.g. Acalog)
_CATALOG_QUERY_PARAMS = {"catoid", "poid", "ent_oid", "coid"}

# Hostname prefixes that signal catalog or faculty pages
_HOSTNAME_CATALOG_PREFIXES = ("catalogs.", "bulletin.", "coursecatalog.")
_HOSTNAME_FACULTY_PREFIXES = ("faculty.", "directory.")


# ------------------------------------------------------------------
# Public helpers
# ------------------------------------------------------------------


def _match_any(text: str, patterns: list[re.Pattern[str]]) -> bool:
    """Return True if *text* matches any of the compiled patterns."""
    return any(pat.search(text) for pat in patterns)


def classify_url(
    url: str, title: str = "", snippet: str = ""
) -> UrlCategory:
    """Classify a URL based on hostname, query params, path patterns, and text signals.

    Checked in order: query params, hostname prefix, path/title/snippet patterns.
    The first matching category wins.

    Args:
        url: The page URL.
        title: Optional page title text.
        snippet: Optional snippet / description text.

    Returns:
        The most likely :class:`UrlCategory`.
    """
    parsed = urlparse(url)
    path = parsed.path.lower()
    hostname = (parsed.hostname or "").lower()

    # 1. Query parameter check (dynamic catalog systems like Acalog)
    query_params = set(parse_qs(parsed.query).keys())
    if query_params & _CATALOG_QUERY_PARAMS:
        return UrlCategory.CATALOG

    # 2. Hostname prefix check
    for prefix in _HOSTNAME_CATALOG_PREFIXES:
        if hostname.startswith(prefix):
            return UrlCategory.CATALOG
    for prefix in _HOSTNAME_FACULTY_PREFIXES:
        if hostname.startswith(prefix):
            return UrlCategory.FACULTY

    # 3. Path / title / snippet pattern matching
    for text in (path, title.lower(), snippet.lower()):
        if not text:
            continue
        for category, patterns in _CATEGORY_PATTERNS:
            if _match_any(text, patterns):
                return category

    return UrlCategory.UNKNOWN


def classify_search_results(
    results: list[dict],
) -> dict[str, list[dict]]:
    """Group a list of search results by :class:`UrlCategory`.

    Each result dict is expected to have ``link``, ``title``, and
    ``snippet`` keys.  A ``category`` field is added to every result.

    Args:
        results: Raw search results from e.g. Serper.

    Returns:
        Dict keyed by category value, each containing a list of
        annotated result dicts.
    """
    grouped: dict[str, list[dict]] = {cat.value: [] for cat in UrlCategory}

    for result in results:
        url = result.get("link", "")
        title = result.get("title", "")
        snippet = result.get("snippet", "")

        category = classify_url(url, title=title, snippet=snippet)
        annotated = {**result, "category": category.value}
        grouped[category.value].append(annotated)

    return grouped

"""University name to directory slug conversion."""

from __future__ import annotations

import re
import unicodedata


def slugify(name: str) -> str:
    """Convert a university name to a filesystem-safe directory slug.

    Rules:
        - Lowercase the entire string
        - Replace ampersands with nothing (e.g. "A&M" -> "am")
        - Remove apostrophes (e.g. "John's" -> "johns")
        - Remove periods (e.g. "St." -> "st")
        - Strip parentheses but keep their contents
        - Replace any non-alphanumeric character (except hyphens) with a hyphen
        - Collapse multiple consecutive hyphens into one
        - Strip leading/trailing hyphens

    Examples:
        >>> slugify("Massachusetts Institute of Technology")
        'massachusetts-institute-of-technology'
        >>> slugify("MIT")
        'mit'
        >>> slugify("University of California-Berkeley")
        'university-of-california-berkeley'
        >>> slugify("Texas A&M University")
        'texas-am-university'
        >>> slugify("St. John's University")
        'st-johns-university'
    """
    # Normalize unicode characters to ASCII equivalents where possible
    s = unicodedata.normalize("NFKD", name)
    s = s.encode("ascii", "ignore").decode("ascii")

    # Lowercase
    s = s.lower()

    # Remove apostrophes (before general substitution so "john's" -> "johns")
    s = s.replace("'", "")
    s = s.replace("\u2019", "")  # right single quotation mark

    # Remove ampersands (so "A&M" -> "am")
    s = s.replace("&", "")

    # Remove periods (so "St." -> "st")
    s = s.replace(".", "")

    # Remove parentheses but keep their content
    s = s.replace("(", "").replace(")", "")

    # Replace any non-alphanumeric character (except hyphen) with a hyphen
    s = re.sub(r"[^a-z0-9-]", "-", s)

    # Collapse multiple hyphens into one
    s = re.sub(r"-{2,}", "-", s)

    # Strip leading/trailing hyphens
    s = s.strip("-")

    return s

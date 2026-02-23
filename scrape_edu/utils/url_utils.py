"""URL normalization and domain extraction utilities."""

from __future__ import annotations

from urllib.parse import urlparse, urlunparse


def normalize_url(url: str) -> str:
    """Normalize a URL by stripping fragments, normalizing scheme, and removing trailing slashes.

    - Defaults to https:// if no scheme is present
    - Lowercases the scheme and hostname
    - Removes the fragment (#...) portion
    - Removes trailing slashes from the path (preserving "/" for root)

    Args:
        url: The URL to normalize.

    Returns:
        The normalized URL string.

    Examples:
        >>> normalize_url("HTTP://MIT.EDU/page#section")
        'http://mit.edu/page'
        >>> normalize_url("https://example.com/path/")
        'https://example.com/path'
        >>> normalize_url("example.com/path")
        'https://example.com/path'
    """
    # Add scheme if missing (case-insensitive check)
    url_lower = url.lower()
    if not url_lower.startswith(("http://", "https://", "//")):
        url = "https://" + url

    parsed = urlparse(url)

    # Lowercase scheme and hostname
    scheme = parsed.scheme.lower()
    hostname = (parsed.hostname or "").lower()

    # Reconstruct netloc (preserve port if present)
    if parsed.port:
        netloc = f"{hostname}:{parsed.port}"
    else:
        netloc = hostname

    # Remove trailing slashes from path (but keep "/" for root path)
    path = parsed.path.rstrip("/") or ""

    # Strip fragment entirely; keep query string
    normalized = urlunparse((scheme, netloc, path, parsed.params, parsed.query, ""))

    return normalized


def extract_domain(url: str) -> str:
    """Extract the base domain from a URL, stripping the 'www.' prefix.

    Args:
        url: The URL to extract the domain from.

    Returns:
        The domain string without 'www.' prefix.

    Examples:
        >>> extract_domain("https://www.mit.edu/path")
        'mit.edu'
        >>> extract_domain("http://cs.stanford.edu/people")
        'cs.stanford.edu'
        >>> extract_domain("example.com")
        'example.com'
    """
    # Add scheme if missing so urlparse can find the hostname (case-insensitive)
    url_lower = url.lower()
    if not url_lower.startswith(("http://", "https://", "//")):
        url = "https://" + url

    parsed = urlparse(url)
    hostname = (parsed.hostname or "").lower()

    # Strip leading 'www.'
    if hostname.startswith("www."):
        hostname = hostname[4:]

    return hostname


def is_same_domain(url1: str, url2: str) -> bool:
    """Check if two URLs share the same base domain.

    Compares the extracted domains (with 'www.' stripped) of both URLs.

    Args:
        url1: First URL.
        url2: Second URL.

    Returns:
        True if both URLs share the same base domain.

    Examples:
        >>> is_same_domain("https://www.mit.edu/a", "http://mit.edu/b")
        True
        >>> is_same_domain("https://mit.edu", "https://stanford.edu")
        False
    """
    return extract_domain(url1) == extract_domain(url2)


def extract_base_domain(url: str) -> str:
    """Extract the registrable base domain from a URL.

    For .edu domains this is the last two parts (e.g. ``gsu.edu``
    from ``catalogs.gsu.edu``).  Works reliably for all US university
    domains tracked in IPEDS.

    Args:
        url: The URL to extract the base domain from.

    Returns:
        The base domain string (e.g. ``"gsu.edu"``).

    Examples:
        >>> extract_base_domain("https://catalogs.gsu.edu/page")
        'gsu.edu'
        >>> extract_base_domain("https://www.cs.stanford.edu")
        'stanford.edu'
        >>> extract_base_domain("https://mit.edu")
        'mit.edu'
    """
    domain = extract_domain(url)
    parts = domain.split(".")
    # Return last two parts (e.g. gsu.edu)
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return domain


def is_related_domain(url1: str, url2: str) -> bool:
    """Check if two URLs share the same registrable base domain.

    Unlike :func:`is_same_domain`, this treats subdomains as related.
    For example ``catalogs.gsu.edu`` and ``gsu.edu`` are related.

    Args:
        url1: First URL.
        url2: Second URL.

    Returns:
        True if both URLs share the same base domain.

    Examples:
        >>> is_related_domain("https://catalogs.gsu.edu", "https://gsu.edu")
        True
        >>> is_related_domain("https://cs.gsu.edu", "https://catalogs.gsu.edu")
        True
        >>> is_related_domain("https://gsu.edu", "https://mit.edu")
        False
    """
    return extract_base_domain(url1) == extract_base_domain(url2)

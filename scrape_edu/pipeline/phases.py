"""Scraping phase definitions."""

from enum import Enum


class Phase(str, Enum):
    """Ordered phases of the per-school scraping pipeline."""
    ROBOTS = "robots"
    DISCOVERY = "discovery"
    CATALOG = "catalog"
    FACULTY = "faculty"
    SYLLABI = "syllabi"

# Ordered list for iteration
PHASE_ORDER = [Phase.ROBOTS, Phase.DISCOVERY, Phase.CATALOG, Phase.FACULTY, Phase.SYLLABI]

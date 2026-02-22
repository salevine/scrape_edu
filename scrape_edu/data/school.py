"""School dataclass representing a university to scrape."""

from __future__ import annotations

from dataclasses import dataclass, field

from scrape_edu.utils.slug import slugify


@dataclass
class School:
    """A university identified from IPEDS data for scraping.

    Attributes:
        unitid: IPEDS unique identifier for the institution.
        name: Official institution name.
        slug: Filesystem-safe directory name (auto-generated from *name*).
        url: Institution homepage URL.
        city: City where the institution is located.
        state: Two-letter state abbreviation.
        cs_dept_url: URL to the CS department page (populated during discovery).
        ds_dept_url: URL to the DS department page (populated during discovery).
    """

    unitid: int
    name: str
    slug: str = field(init=False)
    url: str
    city: str = ""
    state: str = ""
    # These get populated during discovery
    cs_dept_url: str = ""
    ds_dept_url: str = ""

    def __post_init__(self) -> None:
        self.slug = slugify(self.name)

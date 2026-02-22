"""Pydantic models for scraped data."""

from __future__ import annotations

from pydantic import BaseModel, Field


class FacultyMember(BaseModel):
    """Parsed faculty member data."""

    name: str
    title: str = ""
    email: str = ""
    phone: str = ""
    office: str = ""
    department: str = ""
    research_areas: list[str] = Field(default_factory=list)
    courses: list[str] = Field(default_factory=list)
    profile_url: str = ""
    image_url: str = ""


class CatalogEntry(BaseModel):
    """A course catalog entry."""

    course_code: str = ""
    course_name: str = ""
    description: str = ""
    credits: str = ""
    prerequisites: str = ""
    url: str = ""


class SyllabusRecord(BaseModel):
    """Record of a downloaded syllabus."""

    course_code: str = ""
    course_name: str = ""
    url: str
    local_path: str = ""
    file_type: str = ""  # "pdf", "html", "doc", etc.
    faculty_name: str = ""


class DiscoveredUrl(BaseModel):
    """A URL discovered during the discovery phase."""

    url: str
    title: str = ""
    category: str = ""  # catalog, faculty, syllabus, department, course, unknown
    source: str = ""  # "serper" or "crawl"

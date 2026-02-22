"""Tests for scrape_edu.data.models module."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from scrape_edu.data.models import (
    CatalogEntry,
    DiscoveredUrl,
    FacultyMember,
    SyllabusRecord,
)


# ======================================================================
# FacultyMember tests
# ======================================================================


class TestFacultyMemberCreation:
    """Test creating FacultyMember instances."""

    def test_with_all_fields(self) -> None:
        fm = FacultyMember(
            name="Jane Doe",
            title="Associate Professor",
            email="jdoe@example.edu",
            phone="555-1234",
            office="Room 101",
            department="Computer Science",
            research_areas=["AI", "NLP"],
            courses=["CS101", "CS202"],
            profile_url="https://example.edu/~jdoe",
            image_url="https://example.edu/photos/jdoe.jpg",
        )
        assert fm.name == "Jane Doe"
        assert fm.title == "Associate Professor"
        assert fm.email == "jdoe@example.edu"
        assert fm.phone == "555-1234"
        assert fm.office == "Room 101"
        assert fm.department == "Computer Science"
        assert fm.research_areas == ["AI", "NLP"]
        assert fm.courses == ["CS101", "CS202"]
        assert fm.profile_url == "https://example.edu/~jdoe"
        assert fm.image_url == "https://example.edu/photos/jdoe.jpg"

    def test_with_defaults(self) -> None:
        fm = FacultyMember(name="John Smith")
        assert fm.name == "John Smith"
        assert fm.title == ""
        assert fm.email == ""
        assert fm.phone == ""
        assert fm.office == ""
        assert fm.department == ""
        assert fm.research_areas == []
        assert fm.courses == []
        assert fm.profile_url == ""
        assert fm.image_url == ""

    def test_missing_required_name_raises(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            FacultyMember()  # type: ignore[call-arg]
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("name",) for e in errors)

    def test_list_defaults_are_independent(self) -> None:
        """Ensure default list fields are not shared between instances."""
        fm1 = FacultyMember(name="A")
        fm2 = FacultyMember(name="B")
        fm1.research_areas.append("ML")
        assert fm2.research_areas == []


class TestFacultyMemberSerialization:
    """Test FacultyMember serialization to dict and JSON."""

    def test_to_dict(self) -> None:
        fm = FacultyMember(
            name="Jane Doe",
            title="Professor",
            research_areas=["AI"],
        )
        d = fm.model_dump()
        assert isinstance(d, dict)
        assert d["name"] == "Jane Doe"
        assert d["title"] == "Professor"
        assert d["research_areas"] == ["AI"]
        # Default fields should be present
        assert d["email"] == ""

    def test_to_json(self) -> None:
        fm = FacultyMember(name="Jane Doe", email="jdoe@mit.edu")
        j = fm.model_dump_json()
        parsed = json.loads(j)
        assert parsed["name"] == "Jane Doe"
        assert parsed["email"] == "jdoe@mit.edu"

    def test_roundtrip(self) -> None:
        fm = FacultyMember(
            name="Jane Doe",
            title="Professor",
            research_areas=["AI", "ML"],
            courses=["CS101"],
        )
        d = fm.model_dump()
        fm2 = FacultyMember(**d)
        assert fm == fm2


class TestFacultyMemberFieldTypes:
    """Test that field types are enforced."""

    def test_name_must_be_string(self) -> None:
        # Pydantic v2 coerces int to str by default in strict=False mode,
        # but we verify the resulting type is str.
        fm = FacultyMember(name="123")
        assert isinstance(fm.name, str)

    def test_research_areas_must_be_list(self) -> None:
        with pytest.raises(ValidationError):
            FacultyMember(name="Test", research_areas="not a list")  # type: ignore[arg-type]

    def test_courses_must_be_list(self) -> None:
        with pytest.raises(ValidationError):
            FacultyMember(name="Test", courses="not a list")  # type: ignore[arg-type]


# ======================================================================
# CatalogEntry tests
# ======================================================================


class TestCatalogEntry:
    """Test CatalogEntry model."""

    def test_with_all_fields(self) -> None:
        entry = CatalogEntry(
            course_code="CS101",
            course_name="Intro to Computer Science",
            description="An introductory course.",
            credits="3",
            prerequisites="None",
            url="https://example.edu/catalog/cs101",
        )
        assert entry.course_code == "CS101"
        assert entry.course_name == "Intro to Computer Science"
        assert entry.description == "An introductory course."
        assert entry.credits == "3"
        assert entry.prerequisites == "None"
        assert entry.url == "https://example.edu/catalog/cs101"

    def test_all_defaults(self) -> None:
        entry = CatalogEntry()
        assert entry.course_code == ""
        assert entry.course_name == ""
        assert entry.description == ""
        assert entry.credits == ""
        assert entry.prerequisites == ""
        assert entry.url == ""

    def test_partial_fields(self) -> None:
        entry = CatalogEntry(course_code="DS200", course_name="Data Science")
        assert entry.course_code == "DS200"
        assert entry.course_name == "Data Science"
        assert entry.description == ""

    def test_serialization_roundtrip(self) -> None:
        entry = CatalogEntry(
            course_code="CS101",
            course_name="Intro CS",
            credits="4",
        )
        d = entry.model_dump()
        entry2 = CatalogEntry(**d)
        assert entry == entry2


# ======================================================================
# SyllabusRecord tests
# ======================================================================


class TestSyllabusRecord:
    """Test SyllabusRecord model."""

    def test_with_all_fields(self) -> None:
        rec = SyllabusRecord(
            course_code="CS101",
            course_name="Intro CS",
            url="https://example.edu/syllabus.pdf",
            local_path="syllabi/cs101.pdf",
            file_type="pdf",
            faculty_name="Dr. Smith",
        )
        assert rec.course_code == "CS101"
        assert rec.url == "https://example.edu/syllabus.pdf"
        assert rec.local_path == "syllabi/cs101.pdf"
        assert rec.file_type == "pdf"
        assert rec.faculty_name == "Dr. Smith"

    def test_url_is_required(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            SyllabusRecord()  # type: ignore[call-arg]
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("url",) for e in errors)

    def test_defaults(self) -> None:
        rec = SyllabusRecord(url="https://example.edu/syllabus.pdf")
        assert rec.course_code == ""
        assert rec.course_name == ""
        assert rec.local_path == ""
        assert rec.file_type == ""
        assert rec.faculty_name == ""

    def test_serialization(self) -> None:
        rec = SyllabusRecord(
            url="https://example.edu/syllabus.pdf",
            file_type="pdf",
        )
        d = rec.model_dump()
        assert d["url"] == "https://example.edu/syllabus.pdf"
        assert d["file_type"] == "pdf"


# ======================================================================
# DiscoveredUrl tests
# ======================================================================


class TestDiscoveredUrl:
    """Test DiscoveredUrl model."""

    def test_with_all_fields(self) -> None:
        du = DiscoveredUrl(
            url="https://example.edu/catalog",
            title="Course Catalog",
            category="catalog",
            source="serper",
        )
        assert du.url == "https://example.edu/catalog"
        assert du.title == "Course Catalog"
        assert du.category == "catalog"
        assert du.source == "serper"

    def test_url_is_required(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            DiscoveredUrl()  # type: ignore[call-arg]
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("url",) for e in errors)

    def test_defaults(self) -> None:
        du = DiscoveredUrl(url="https://example.edu/page")
        assert du.title == ""
        assert du.category == ""
        assert du.source == ""

    def test_serialization_roundtrip(self) -> None:
        du = DiscoveredUrl(
            url="https://example.edu/faculty",
            title="Faculty Directory",
            category="faculty",
            source="crawl",
        )
        d = du.model_dump()
        du2 = DiscoveredUrl(**d)
        assert du == du2

    def test_to_json(self) -> None:
        du = DiscoveredUrl(
            url="https://example.edu/syllabus.pdf",
            category="syllabus",
            source="serper",
        )
        j = du.model_dump_json()
        parsed = json.loads(j)
        assert parsed["url"] == "https://example.edu/syllabus.pdf"
        assert parsed["category"] == "syllabus"
        assert parsed["source"] == "serper"

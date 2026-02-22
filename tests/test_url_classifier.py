"""Tests for scrape_edu.discovery.url_classifier."""

from __future__ import annotations

import pytest

from scrape_edu.discovery.url_classifier import (
    UrlCategory,
    classify_search_results,
    classify_url,
)


# ------------------------------------------------------------------
# classify_url: CATALOG
# ------------------------------------------------------------------


class TestClassifyCatalog:
    def test_catalog_in_path(self) -> None:
        assert classify_url("https://mit.edu/catalog/courses") == UrlCategory.CATALOG

    def test_bulletin_in_path(self) -> None:
        assert classify_url("https://university.edu/bulletin/2024") == UrlCategory.CATALOG

    def test_courselist_in_path(self) -> None:
        assert classify_url("https://school.edu/courselist") == UrlCategory.CATALOG

    def test_course_descriptions_in_path(self) -> None:
        assert classify_url("https://school.edu/course-descriptions") == UrlCategory.CATALOG

    def test_acalog_in_path(self) -> None:
        assert classify_url("https://school.edu/acalog/view") == UrlCategory.CATALOG

    def test_courseleaf_in_path(self) -> None:
        assert classify_url("https://school.edu/courseleaf/pages") == UrlCategory.CATALOG

    def test_academic_catalog_in_path(self) -> None:
        assert classify_url("https://school.edu/academic-catalog") == UrlCategory.CATALOG

    def test_catalog_case_insensitive(self) -> None:
        assert classify_url("https://school.edu/Catalog/CS") == UrlCategory.CATALOG


# ------------------------------------------------------------------
# classify_url: FACULTY
# ------------------------------------------------------------------


class TestClassifyFaculty:
    def test_faculty_in_path(self) -> None:
        assert classify_url("https://cs.mit.edu/faculty") == UrlCategory.FACULTY

    def test_people_in_path(self) -> None:
        assert classify_url("https://cs.mit.edu/people") == UrlCategory.FACULTY

    def test_directory_in_path(self) -> None:
        assert classify_url("https://school.edu/directory") == UrlCategory.FACULTY

    def test_staff_in_path(self) -> None:
        assert classify_url("https://school.edu/staff") == UrlCategory.FACULTY

    def test_professors_in_path(self) -> None:
        assert classify_url("https://school.edu/professors") == UrlCategory.FACULTY

    def test_department_people_in_path(self) -> None:
        assert classify_url("https://school.edu/department/people") == UrlCategory.FACULTY

    def test_our_people_in_path(self) -> None:
        assert classify_url("https://school.edu/our-people") == UrlCategory.FACULTY


# ------------------------------------------------------------------
# classify_url: SYLLABUS
# ------------------------------------------------------------------


class TestClassifySyllabus:
    def test_syllabus_in_path(self) -> None:
        assert classify_url("https://school.edu/syllabus/cs101") == UrlCategory.SYLLABUS

    def test_syllabi_in_path(self) -> None:
        assert classify_url("https://school.edu/syllabi/fall2024") == UrlCategory.SYLLABUS

    def test_course_outline_in_path(self) -> None:
        assert classify_url("https://school.edu/course-outline") == UrlCategory.SYLLABUS

    def test_course_materials_in_path(self) -> None:
        assert classify_url("https://school.edu/course-materials") == UrlCategory.SYLLABUS


# ------------------------------------------------------------------
# classify_url: DEPARTMENT
# ------------------------------------------------------------------


class TestClassifyDepartment:
    def test_department_in_path(self) -> None:
        assert classify_url("https://school.edu/department/cs") == UrlCategory.DEPARTMENT

    def test_dept_in_path(self) -> None:
        assert classify_url("https://school.edu/dept/math") == UrlCategory.DEPARTMENT

    def test_school_of_in_path(self) -> None:
        assert classify_url("https://school.edu/school-of-engineering") == UrlCategory.DEPARTMENT

    def test_cs_path(self) -> None:
        assert classify_url("https://school.edu/cs/") == UrlCategory.DEPARTMENT

    def test_cse_path(self) -> None:
        assert classify_url("https://school.edu/cse/") == UrlCategory.DEPARTMENT

    def test_computer_science_in_path(self) -> None:
        assert classify_url("https://school.edu/computer-science") == UrlCategory.DEPARTMENT

    def test_data_science_in_path(self) -> None:
        assert classify_url("https://school.edu/data-science") == UrlCategory.DEPARTMENT

    def test_computing_in_path(self) -> None:
        assert classify_url("https://school.edu/computing") == UrlCategory.DEPARTMENT


# ------------------------------------------------------------------
# classify_url: COURSE
# ------------------------------------------------------------------


class TestClassifyCourse:
    def test_course_in_path(self) -> None:
        assert classify_url("https://school.edu/course/cs101") == UrlCategory.COURSE

    def test_class_in_path(self) -> None:
        assert classify_url("https://school.edu/class/fall2024") == UrlCategory.COURSE

    def test_section_in_path(self) -> None:
        assert classify_url("https://school.edu/section/001") == UrlCategory.COURSE


# ------------------------------------------------------------------
# classify_url: UNKNOWN
# ------------------------------------------------------------------


class TestClassifyUnknown:
    def test_generic_url(self) -> None:
        assert classify_url("https://school.edu/about") == UrlCategory.UNKNOWN

    def test_homepage(self) -> None:
        assert classify_url("https://school.edu/") == UrlCategory.UNKNOWN

    def test_news_page(self) -> None:
        assert classify_url("https://school.edu/news/2024") == UrlCategory.UNKNOWN

    def test_empty_strings(self) -> None:
        assert classify_url("https://school.edu/") == UrlCategory.UNKNOWN


# ------------------------------------------------------------------
# classify_url: title and snippet influence
# ------------------------------------------------------------------


class TestTitleAndSnippet:
    def test_title_overrides_unknown_url(self) -> None:
        result = classify_url(
            "https://school.edu/page123",
            title="Course Catalog 2024",
        )
        assert result == UrlCategory.CATALOG

    def test_snippet_overrides_unknown_url(self) -> None:
        result = classify_url(
            "https://school.edu/page123",
            snippet="Browse our faculty directory",
        )
        assert result == UrlCategory.FACULTY

    def test_url_path_takes_priority_over_title(self) -> None:
        """If the URL path matches catalog, title suggesting faculty should not change it."""
        result = classify_url(
            "https://school.edu/catalog",
            title="Meet our faculty",
        )
        assert result == UrlCategory.CATALOG

    def test_title_takes_priority_over_snippet(self) -> None:
        result = classify_url(
            "https://school.edu/page",
            title="Syllabus Repository",
            snippet="Browse the course catalog",
        )
        assert result == UrlCategory.SYLLABUS

    def test_snippet_with_syllabus(self) -> None:
        result = classify_url(
            "https://school.edu/page",
            snippet="Download the course syllabus here",
        )
        assert result == UrlCategory.SYLLABUS


# ------------------------------------------------------------------
# Case insensitivity
# ------------------------------------------------------------------


class TestCaseInsensitivity:
    def test_uppercase_catalog(self) -> None:
        assert classify_url("https://school.edu/CATALOG") == UrlCategory.CATALOG

    def test_mixed_case_faculty(self) -> None:
        assert classify_url("https://school.edu/Faculty") == UrlCategory.FACULTY

    def test_uppercase_title(self) -> None:
        result = classify_url("https://school.edu/x", title="SYLLABUS FOR CS101")
        assert result == UrlCategory.SYLLABUS

    def test_mixed_case_snippet(self) -> None:
        result = classify_url(
            "https://school.edu/x", snippet="Department of Computer Science"
        )
        assert result == UrlCategory.DEPARTMENT


# ------------------------------------------------------------------
# classify_search_results
# ------------------------------------------------------------------


class TestClassifySearchResults:
    def test_groups_results_correctly(self) -> None:
        results = [
            {"link": "https://school.edu/catalog", "title": "", "snippet": ""},
            {"link": "https://school.edu/faculty", "title": "", "snippet": ""},
            {"link": "https://school.edu/about", "title": "", "snippet": ""},
            {"link": "https://school.edu/syllabus/cs101", "title": "", "snippet": ""},
        ]

        grouped = classify_search_results(results)

        assert len(grouped["catalog"]) == 1
        assert len(grouped["faculty"]) == 1
        assert len(grouped["syllabus"]) == 1
        assert len(grouped["unknown"]) == 1

    def test_all_categories_present_in_output(self) -> None:
        grouped = classify_search_results([])

        for cat in UrlCategory:
            assert cat.value in grouped

    def test_adds_category_field_to_results(self) -> None:
        results = [
            {"link": "https://school.edu/catalog/cs", "title": "CS", "snippet": ""},
        ]

        grouped = classify_search_results(results)
        item = grouped["catalog"][0]

        assert "category" in item
        assert item["category"] == "catalog"
        # Original keys preserved
        assert item["link"] == "https://school.edu/catalog/cs"
        assert item["title"] == "CS"

    def test_does_not_mutate_original_results(self) -> None:
        results = [
            {"link": "https://school.edu/catalog", "title": "", "snippet": ""},
        ]

        classify_search_results(results)

        # Original result should NOT have a 'category' key
        assert "category" not in results[0]

    def test_multiple_results_same_category(self) -> None:
        results = [
            {"link": "https://school.edu/catalog/cs", "title": "", "snippet": ""},
            {"link": "https://school.edu/catalog/ds", "title": "", "snippet": ""},
            {"link": "https://school.edu/bulletin", "title": "", "snippet": ""},
        ]

        grouped = classify_search_results(results)
        assert len(grouped["catalog"]) == 3

    def test_uses_title_and_snippet_for_classification(self) -> None:
        results = [
            {
                "link": "https://school.edu/page",
                "title": "Faculty Directory",
                "snippet": "Meet our professors",
            },
        ]

        grouped = classify_search_results(results)
        assert len(grouped["faculty"]) == 1

"""Tests for scrape_edu.pipeline.phases module."""

from __future__ import annotations

from scrape_edu.pipeline.phases import Phase, PHASE_ORDER


class TestPhaseEnum:
    """Test Phase enum definition."""

    def test_has_robots_phase(self) -> None:
        assert Phase.ROBOTS == "robots"

    def test_has_discovery_phase(self) -> None:
        assert Phase.DISCOVERY == "discovery"

    def test_has_catalog_phase(self) -> None:
        assert Phase.CATALOG == "catalog"

    def test_has_faculty_phase(self) -> None:
        assert Phase.FACULTY == "faculty"

    def test_has_syllabi_phase(self) -> None:
        assert Phase.SYLLABI == "syllabi"

    def test_all_five_phases_defined(self) -> None:
        assert len(Phase) == 5

    def test_phase_values_are_strings(self) -> None:
        for phase in Phase:
            assert isinstance(phase.value, str)

    def test_phases_are_strings(self) -> None:
        """Phase inherits from str, so each member is also a str."""
        for phase in Phase:
            assert isinstance(phase, str)

    def test_phase_string_equality(self) -> None:
        """Phase members can be compared directly to plain strings."""
        assert Phase.ROBOTS == "robots"
        assert Phase.SYLLABI == "syllabi"


class TestPhaseOrder:
    """Test PHASE_ORDER list."""

    def test_phase_order_length(self) -> None:
        assert len(PHASE_ORDER) == 5

    def test_phase_order_correct_sequence(self) -> None:
        expected = [
            Phase.ROBOTS,
            Phase.DISCOVERY,
            Phase.CATALOG,
            Phase.FACULTY,
            Phase.SYLLABI,
        ]
        assert PHASE_ORDER == expected

    def test_phase_order_starts_with_robots(self) -> None:
        assert PHASE_ORDER[0] == Phase.ROBOTS

    def test_phase_order_ends_with_syllabi(self) -> None:
        assert PHASE_ORDER[-1] == Phase.SYLLABI

    def test_phase_order_contains_all_phases(self) -> None:
        assert set(PHASE_ORDER) == set(Phase)

    def test_phase_order_has_no_duplicates(self) -> None:
        assert len(PHASE_ORDER) == len(set(PHASE_ORDER))

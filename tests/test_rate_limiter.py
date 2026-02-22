"""Tests for scrape_edu.net.rate_limiter module."""

from __future__ import annotations

import threading
import time
from unittest.mock import patch

import pytest

from scrape_edu.net.rate_limiter import RateLimiter


class TestFirstRequest:
    """The very first request to a domain should not wait."""

    def test_first_request_no_delay(self) -> None:
        limiter = RateLimiter(min_delay=5.0, max_delay=10.0)
        start = time.monotonic()
        limiter.wait("example.com")
        elapsed = time.monotonic() - start
        # Should be nearly instant (well under 1 second).
        assert elapsed < 0.1

    def test_first_request_multiple_domains(self) -> None:
        """First request to each distinct domain should be immediate."""
        limiter = RateLimiter(min_delay=5.0, max_delay=10.0)
        start = time.monotonic()
        limiter.wait("a.com")
        limiter.wait("b.com")
        limiter.wait("c.com")
        elapsed = time.monotonic() - start
        assert elapsed < 0.1


class TestSubsequentRequests:
    """Subsequent requests to the same domain must wait."""

    def test_second_request_waits_at_least_min_delay(self) -> None:
        limiter = RateLimiter(min_delay=0.2, max_delay=0.3)
        limiter.wait("example.com")
        start = time.monotonic()
        limiter.wait("example.com")
        elapsed = time.monotonic() - start
        assert elapsed >= 0.19  # small tolerance

    def test_second_request_waits_at_most_max_delay(self) -> None:
        limiter = RateLimiter(min_delay=0.1, max_delay=0.2)
        limiter.wait("example.com")
        start = time.monotonic()
        limiter.wait("example.com")
        elapsed = time.monotonic() - start
        # Should not exceed max_delay by much.
        assert elapsed < 0.35

    @patch("scrape_edu.net.rate_limiter.random.uniform", return_value=0.25)
    def test_delay_uses_random_uniform(self, mock_uniform) -> None:
        """The delay should come from random.uniform(min, max)."""
        limiter = RateLimiter(min_delay=0.2, max_delay=0.4)
        limiter.wait("example.com")
        start = time.monotonic()
        limiter.wait("example.com")
        elapsed = time.monotonic() - start
        mock_uniform.assert_called_with(0.2, 0.4)
        assert elapsed >= 0.2  # at least the mocked 0.25 minus tolerance


class TestDomainIsolation:
    """Different domains should not interfere with each other."""

    def test_different_domains_no_cross_delay(self) -> None:
        limiter = RateLimiter(min_delay=5.0, max_delay=10.0)
        limiter.wait("a.com")
        # Requesting a *different* domain should be instant.
        start = time.monotonic()
        limiter.wait("b.com")
        elapsed = time.monotonic() - start
        assert elapsed < 0.1


class TestGetDelay:
    """Tests for the get_delay() diagnostic method."""

    def test_no_previous_request(self) -> None:
        limiter = RateLimiter()
        assert limiter.get_delay("unknown.com") == 0.0

    def test_returns_positive_after_request(self) -> None:
        limiter = RateLimiter(min_delay=1.0, max_delay=2.0)
        limiter.wait("example.com")
        delay = limiter.get_delay("example.com")
        assert delay > 0.0

    def test_decays_over_time(self) -> None:
        limiter = RateLimiter(min_delay=0.2, max_delay=0.3)
        limiter.wait("example.com")
        time.sleep(0.25)
        # After sleeping longer than min_delay the reported delay should be 0.
        assert limiter.get_delay("example.com") == 0.0

    def test_returns_zero_after_min_delay_elapsed(self) -> None:
        limiter = RateLimiter(min_delay=0.05, max_delay=0.1)
        limiter.wait("example.com")
        time.sleep(0.06)
        assert limiter.get_delay("example.com") == 0.0


class TestCustomDelays:
    """Custom min/max delay values should be respected."""

    def test_zero_delay(self) -> None:
        limiter = RateLimiter(min_delay=0.0, max_delay=0.0)
        limiter.wait("example.com")
        start = time.monotonic()
        limiter.wait("example.com")
        elapsed = time.monotonic() - start
        assert elapsed < 0.05

    def test_equal_min_max(self) -> None:
        limiter = RateLimiter(min_delay=0.15, max_delay=0.15)
        limiter.wait("example.com")
        start = time.monotonic()
        limiter.wait("example.com")
        elapsed = time.monotonic() - start
        assert 0.1 <= elapsed < 0.3


class TestValidation:
    """Constructor should reject invalid parameters."""

    def test_negative_min_delay(self) -> None:
        with pytest.raises(ValueError, match="min_delay"):
            RateLimiter(min_delay=-1.0)

    def test_max_less_than_min(self) -> None:
        with pytest.raises(ValueError, match="max_delay"):
            RateLimiter(min_delay=2.0, max_delay=1.0)


class TestThreadSafety:
    """Multiple threads hitting the same domain must not corrupt state."""

    def test_concurrent_same_domain(self) -> None:
        limiter = RateLimiter(min_delay=0.05, max_delay=0.1)
        errors: list[Exception] = []
        timestamps: list[float] = []
        lock = threading.Lock()

        def worker() -> None:
            try:
                limiter.wait("shared.com")
                with lock:
                    timestamps.append(time.monotonic())
            except Exception as exc:
                with lock:
                    errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert errors == []
        assert len(timestamps) == 5

        # After sorting, consecutive timestamps should be at least ~min_delay
        # apart (except possibly the very first one which has no predecessor).
        timestamps.sort()
        for i in range(2, len(timestamps)):
            gap = timestamps[i] - timestamps[i - 1]
            # Allow some tolerance (0.04 instead of 0.05).
            assert gap >= 0.04, f"Gap between request {i-1} and {i} was only {gap:.4f}s"

    def test_concurrent_different_domains(self) -> None:
        """Requests to distinct domains should proceed in parallel."""
        limiter = RateLimiter(min_delay=5.0, max_delay=10.0)
        results: dict[str, float] = {}
        lock = threading.Lock()

        def worker(domain: str) -> None:
            limiter.wait(domain)
            with lock:
                results[domain] = time.monotonic()

        start = time.monotonic()
        threads = [
            threading.Thread(target=worker, args=(f"domain{i}.com",))
            for i in range(5)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        total = time.monotonic() - start
        # All five should finish nearly instantly (first request each).
        assert total < 1.0
        assert len(results) == 5

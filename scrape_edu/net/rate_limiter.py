"""Per-domain rate limiter with random delays between requests."""

from __future__ import annotations

import random
import threading
import time


class RateLimiter:
    """Enforce per-domain delays between HTTP requests.

    Each domain gets its own lock so that concurrent workers targeting
    different domains do not block each other, while requests to the
    *same* domain are serialized and separated by a random delay drawn
    from ``[min_delay, max_delay]``.

    The first request to any domain proceeds immediately (no delay).
    """

    def __init__(self, min_delay: float = 1.0, max_delay: float = 3.0) -> None:
        if min_delay < 0:
            raise ValueError("min_delay must be >= 0")
        if max_delay < min_delay:
            raise ValueError("max_delay must be >= min_delay")

        self.min_delay = min_delay
        self.max_delay = max_delay
        self._domain_locks: dict[str, threading.Lock] = {}
        self._last_request: dict[str, float] = {}
        self._global_lock = threading.Lock()  # protects _domain_locks creation

    def _get_domain_lock(self, domain: str) -> threading.Lock:
        """Get or create a lock for *domain* (thread-safe)."""
        with self._global_lock:
            if domain not in self._domain_locks:
                self._domain_locks[domain] = threading.Lock()
            return self._domain_locks[domain]

    def wait(self, domain: str) -> None:
        """Block until it is safe to make a request to *domain*.

        - First request to a domain returns immediately.
        - Subsequent requests sleep for the remaining time so that
          at least ``random.uniform(min_delay, max_delay)`` seconds
          have elapsed since the previous request to the same domain.
        """
        lock = self._get_domain_lock(domain)
        with lock:
            now = time.monotonic()
            last = self._last_request.get(domain)
            if last is not None:
                target_delay = random.uniform(self.min_delay, self.max_delay)
                elapsed = now - last
                remaining = target_delay - elapsed
                if remaining > 0:
                    time.sleep(remaining)
            self._last_request[domain] = time.monotonic()

    def get_delay(self, domain: str) -> float:
        """Return the minimum remaining delay for *domain* (0 if no wait needed).

        Uses ``min_delay`` as the reference (worst-case shortest wait).
        Useful for testing and diagnostics.
        """
        last = self._last_request.get(domain)
        if last is None:
            return 0.0
        elapsed = time.monotonic() - last
        remaining = self.min_delay - elapsed
        return max(0.0, remaining)

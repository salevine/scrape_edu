"""HTTP client wrapper with retry logic, rate limiting, and streaming downloads."""

from __future__ import annotations

import logging
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from scrape_edu.net.rate_limiter import RateLimiter
from scrape_edu.utils.url_utils import extract_domain

logger = logging.getLogger("scrape_edu")


class HttpClient:
    """Thin wrapper around :class:`requests.Session` that enforces
    per-domain rate limiting, automatic retries on transient errors,
    and a configurable ``User-Agent`` header.

    Usage::

        limiter = RateLimiter(min_delay=1.0, max_delay=3.0)
        with HttpClient(rate_limiter=limiter) as client:
            response = client.get("https://example.com")
    """

    def __init__(
        self,
        rate_limiter: RateLimiter,
        user_agent: str = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        timeout: tuple[int, int] = (10, 30),
        max_retries: int = 3,
    ) -> None:
        self.rate_limiter = rate_limiter
        self.timeout = timeout

        self._session = requests.Session()
        self._session.headers["User-Agent"] = user_agent

        # Configure retry strategy for transient server errors.
        retry = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, url: str, **kwargs) -> requests.Response:
        """Perform a rate-limited GET request.

        Raises :class:`requests.HTTPError` on 4xx/5xx responses (after
        retries are exhausted for 5xx).
        """
        domain = extract_domain(url)
        self.rate_limiter.wait(domain)

        kwargs.setdefault("timeout", self.timeout)
        try:
            response = self._session.get(url, **kwargs)
        except requests.exceptions.SSLError:
            if domain.endswith(".edu"):
                logger.warning(
                    "SSL error on .edu domain, retrying without verification",
                    extra={"url": url, "domain": domain},
                )
                kwargs["verify"] = False
                response = self._session.get(url, **kwargs)
            else:
                raise
        response.raise_for_status()
        return response

    def download(self, url: str, dest: Path, **kwargs) -> Path:
        """Download *url* to *dest* using streaming and an atomic write pattern.

        Writes to a temporary ``.tmp`` file first, then renames on success.
        Cleans up the temporary file if anything goes wrong.

        Returns the final destination :class:`~pathlib.Path`.
        """
        domain = extract_domain(url)
        self.rate_limiter.wait(domain)

        kwargs.setdefault("timeout", self.timeout)
        kwargs["stream"] = True

        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = dest.with_suffix(dest.suffix + ".tmp")

        try:
            try:
                response = self._session.get(url, **kwargs)
            except requests.exceptions.SSLError:
                if domain.endswith(".edu"):
                    logger.warning(
                        "SSL error on .edu domain, retrying without verification",
                        extra={"url": url, "domain": domain},
                    )
                    kwargs["verify"] = False
                    response = self._session.get(url, **kwargs)
                else:
                    raise
            response.raise_for_status()

            with open(tmp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=65536):
                    f.write(chunk)

            tmp_path.rename(dest)
            return dest
        except BaseException:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

    def close(self) -> None:
        """Close the underlying :class:`requests.Session`."""
        self._session.close()

    # ------------------------------------------------------------------
    # Context-manager protocol
    # ------------------------------------------------------------------

    def __enter__(self) -> HttpClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

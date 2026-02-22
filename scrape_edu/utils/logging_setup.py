"""Structured JSON logging setup."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


class JSONFormatter(logging.Formatter):
    """Format log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "module": record.module,
            "message": record.getMessage(),
        }

        # Include any extra fields attached to the record
        # (fields beyond the standard LogRecord attributes)
        standard_attrs = {
            "name", "msg", "args", "created", "relativeCreated", "exc_info",
            "exc_text", "stack_info", "lineno", "funcName", "pathname",
            "filename", "module", "levelno", "levelname", "message",
            "msecs", "processName", "process", "threadName", "thread",
            "taskName",
        }
        for key, value in record.__dict__.items():
            if key not in standard_attrs and not key.startswith("_"):
                log_entry[key] = value

        # Include exception info if present
        if record.exc_info and record.exc_info[1] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


def setup_logging(
    level: str = "INFO",
    log_file: str | None = None,
) -> logging.Logger:
    """Configure the root logger with JSON-formatted output.

    Args:
        level: Log level string (e.g. "DEBUG", "INFO", "WARNING").
        log_file: Optional path to a log file. If provided, a file handler
            is added in addition to the console handler.

    Returns:
        The configured root logger.
    """
    logger = logging.getLogger("scrape_edu")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Remove any existing handlers to avoid duplicates on repeated calls
    logger.handlers.clear()

    formatter = JSONFormatter()

    # Console handler (stderr)
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Optional file handler
    if log_file is not None:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

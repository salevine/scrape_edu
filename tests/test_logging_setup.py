"""Tests for scrape_edu.utils.logging_setup module."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from scrape_edu.utils.logging_setup import JSONFormatter, setup_logging


class TestJSONFormatter:
    """Test the JSON log formatter."""

    def test_format_produces_valid_json(self) -> None:
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="hello",
            args=None,
            exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert isinstance(parsed, dict)

    def test_required_fields_present(self) -> None:
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.WARNING,
            pathname="test.py",
            lineno=42,
            msg="warning message",
            args=None,
            exc_info=None,
        )
        parsed = json.loads(formatter.format(record))
        assert "timestamp" in parsed
        assert parsed["level"] == "WARNING"
        assert parsed["message"] == "warning message"
        assert "module" in parsed

    def test_extra_fields_included(self) -> None:
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="with extras",
            args=None,
            exc_info=None,
        )
        record.university = "MIT"  # type: ignore[attr-defined]
        record.url = "https://mit.edu"  # type: ignore[attr-defined]

        parsed = json.loads(formatter.format(record))
        assert parsed["university"] == "MIT"
        assert parsed["url"] == "https://mit.edu"

    def test_exception_info_included(self) -> None:
        formatter = JSONFormatter()
        try:
            raise ValueError("test error")
        except ValueError:
            import sys

            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="test.py",
            lineno=1,
            msg="error occurred",
            args=None,
            exc_info=exc_info,
        )
        parsed = json.loads(formatter.format(record))
        assert "exception" in parsed
        assert "ValueError" in parsed["exception"]


class TestSetupLogging:
    """Test the logging setup function."""

    def test_returns_logger(self) -> None:
        logger = setup_logging()
        assert isinstance(logger, logging.Logger)

    def test_logger_name(self) -> None:
        logger = setup_logging()
        assert logger.name == "scrape_edu"

    def test_default_level_is_info(self) -> None:
        logger = setup_logging()
        assert logger.level == logging.INFO

    def test_custom_level(self) -> None:
        logger = setup_logging(level="DEBUG")
        assert logger.level == logging.DEBUG

    def test_case_insensitive_level(self) -> None:
        logger = setup_logging(level="debug")
        assert logger.level == logging.DEBUG

    def test_has_console_handler(self) -> None:
        logger = setup_logging()
        assert len(logger.handlers) >= 1
        assert any(
            isinstance(h, logging.StreamHandler)
            for h in logger.handlers
        )

    def test_console_handler_uses_json_formatter(self) -> None:
        logger = setup_logging()
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                assert isinstance(handler.formatter, JSONFormatter)

    def test_file_handler_added(self, tmp_path: Path) -> None:
        log_file = str(tmp_path / "test.log")
        logger = setup_logging(log_file=log_file)

        file_handlers = [
            h for h in logger.handlers if isinstance(h, logging.FileHandler)
        ]
        assert len(file_handlers) == 1

        # Clean up handler to release file
        for h in logger.handlers:
            h.close()

    def test_file_handler_writes_json(self, tmp_path: Path) -> None:
        log_file = tmp_path / "test.log"
        logger = setup_logging(log_file=str(log_file))

        logger.info("test message")

        # Flush handlers
        for h in logger.handlers:
            h.flush()

        content = log_file.read_text()
        parsed = json.loads(content.strip())
        assert parsed["message"] == "test message"

        # Clean up
        for h in logger.handlers:
            h.close()

    def test_no_duplicate_handlers_on_repeated_calls(self) -> None:
        logger1 = setup_logging()
        handler_count1 = len(logger1.handlers)

        logger2 = setup_logging()
        handler_count2 = len(logger2.handlers)

        assert handler_count1 == handler_count2
        assert logger1 is logger2  # Same logger instance

    def test_no_file_handler_when_none(self) -> None:
        logger = setup_logging(log_file=None)
        file_handlers = [
            h for h in logger.handlers if isinstance(h, logging.FileHandler)
        ]
        assert len(file_handlers) == 0

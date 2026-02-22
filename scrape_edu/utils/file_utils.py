"""Atomic file writes to prevent corruption."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Union


def atomic_write(
    filepath: Path,
    data: Union[str, bytes],
    mode: str = "w",
) -> None:
    """Write data to a file atomically.

    Writes to a temporary file in the same directory as the target, then
    renames it to the final path. This ensures that readers never see a
    partially-written file.

    Args:
        filepath: Destination file path.
        data: Content to write (str for text mode, bytes for binary mode).
        mode: File open mode -- "w" for text, "wb" for binary.

    Raises:
        TypeError: If data type doesn't match the mode.
    """
    filepath = Path(filepath)

    # Create parent directories if they don't exist
    filepath.parent.mkdir(parents=True, exist_ok=True)

    # Determine whether we're writing text or binary
    is_binary = "b" in mode
    write_mode = "wb" if is_binary else "w"

    # Create temp file in the same directory so os.rename is atomic
    # (same filesystem guarantees atomic rename on POSIX)
    fd, tmp_path = tempfile.mkstemp(
        dir=filepath.parent,
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, write_mode) as f:
            f.write(data)

        # Atomic rename
        os.replace(tmp_path, filepath)
    except BaseException:
        # Clean up the temp file on any failure
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def atomic_json_write(
    filepath: Path,
    data: Union[dict, list],
) -> None:
    """Write a JSON-serializable object to a file atomically.

    A convenience wrapper around :func:`atomic_write` that serializes
    *data* as pretty-printed JSON (indent=2) with a trailing newline.

    Args:
        filepath: Destination file path (should end in .json).
        data: A dict or list to serialize.
    """
    json_str = json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    atomic_write(filepath, json_str, mode="w")

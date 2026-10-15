"""A simple cross-platform lock file so scheduled runs never overlap.

The lock is created atomically (O_CREAT | O_EXCL). A lock left behind by a crashed run is
considered stale once it is older than ``stale_after_seconds`` and is replaced.

Staleness is judged by age only, never by probing the process ID: on Windows, ``os.kill(pid, 0)``
*terminates* the process rather than checking it.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Self

logger = logging.getLogger(__name__)


class RunLock:
    def __init__(self, path: Path, stale_after_seconds: float) -> None:
        self.path = path
        self.stale_after_seconds = stale_after_seconds
        self._held = False

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        for _ in range(2):  # second attempt only after removing a stale lock
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                if not self._remove_if_stale():
                    return False
                continue
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump({"pid": os.getpid(), "acquired_at_utc": datetime.now(UTC).isoformat()}, fh)
            self._held = True
            return True
        return False

    def release(self) -> None:
        if self._held:
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass
            self._held = False

    def _remove_if_stale(self) -> bool:
        try:
            age = time.time() - self.path.stat().st_mtime
        except FileNotFoundError:
            return True  # released between our attempts
        if age < self.stale_after_seconds:
            return False
        logger.warning("Removing stale lock %s (%.0f minutes old)", self.path, age / 60)
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
        return True

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()

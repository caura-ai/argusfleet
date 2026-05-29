"""Structured JSONL event logging for experiments.

Every experiment opens a single ``JsonlSink`` per run and writes typed
event records to it. The sink is the only thing that touches disk so
later analysis can rebuild metrics from the JSONL without re-running.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel

logger = logging.getLogger(__name__)


def _default(obj: Any) -> Any:
    if isinstance(obj, BaseModel):
        return obj.model_dump(mode="json")
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Path):
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


class JsonlSink:
    """Append-only JSONL writer with timestamp injection."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._fh = open(path, "a", encoding="utf-8")
        logger.info("jsonl sink opened: %s", path)

    @property
    def path(self) -> Path:
        return self._path

    def write(self, event_type: str, payload: Any) -> None:
        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "event": event_type,
            "data": payload,
        }
        self._fh.write(json.dumps(record, default=_default) + "\n")
        self._fh.flush()

    def close(self) -> None:
        try:
            self._fh.close()
        except Exception:
            logger.warning("jsonl close failed", exc_info=True)

    def __enter__(self) -> "JsonlSink":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

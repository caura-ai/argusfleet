"""Shared lifecycle scaffolding for experiments.

Every experiment's ``run()`` does the same five-step dance:

1. Compute a deterministic ``run_id`` and the per-run output directory.
2. Open a ``JsonlSink`` for events.
3. Emit a ``run_start`` envelope so the report aggregator can find it.
4. Do the experiment-specific work against a client (passed in for
   tests, autocreated for production).
5. Emit a ``run_end`` envelope and materialize CSV/plots from the JSONL.

Steps 1–3 and step 5's client-lifecycle handling are identical across
experiments. Extracting them keeps each experiment module focused on the
actual workload it's measuring.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

from argusfleet.harness.client import MemClawClient, memclaw_client
from argusfleet.harness.config import HarnessConfig
from argusfleet.harness.logging import JsonlSink


def make_run_id(cfg: HarnessConfig, default_label: str) -> str:
    """Generate ``<UTC-stamp>-<label>-seed<N>`` — unique per invocation."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    label = cfg.run_label or default_label
    return f"{stamp}-{label}-seed{cfg.seed}"


def make_run_dir(cfg: HarnessConfig, default_label: str) -> tuple[str, Path, Path]:
    """Resolve ``(run_id, run_dir, events_path)`` and create the directory.

    Side effect: ``run_dir`` is created (``mkdir parents exist_ok``).
    The events file is *not* created here — the caller opens it via
    ``JsonlSink``.
    """
    run_id = make_run_id(cfg, default_label)
    run_dir = Path(cfg.output_dir) / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    events_path = run_dir / "events.jsonl"
    return run_id, run_dir, events_path


def run_start_ts(run_dir: Path) -> str | None:
    """Return the ISO ``ts`` of the run's ``run_start`` event, or ``None``.

    This is the logical run clock written at launch — the single authority
    for "which run is latest", shared with ``report.py``'s aggregator.
    Prefer it over filesystem mtime: re-materialising plots (``plot
    latest``) rewrites a run dir's files and bumps its mtime, which would
    otherwise reorder runs and let the report table and the paper-figure
    script disagree about which run dir a result came from.
    """
    events = run_dir / "events.jsonl"
    if not events.exists():
        return None
    try:
        with open(events, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                if rec.get("event") == "run_start":
                    ts = rec.get("ts")
                    return str(ts) if ts is not None else None
    except (OSError, json.JSONDecodeError):
        return None
    return None


def latest_run_dir(runs_dir: Path) -> Path | None:
    """Most-recent run dir by ``run_start`` event ts (NOT filesystem mtime).

    Falls back to the dir-name UTC stamp (``make_run_id`` embeds it, so
    lexicographic name order == chronological) when a run has no readable
    run_start event, and as the tiebreak between equal timestamps.
    """
    if not runs_dir.exists():
        return None
    candidates = [d for d in runs_dir.iterdir() if d.is_dir()]
    if not candidates:
        return None
    return max(candidates, key=lambda d: (run_start_ts(d) or "", d.name))


async def run_with_client(
    cfg: HarnessConfig,
    *,
    client: MemClawClient | None,
    body: Callable[[MemClawClient], Awaitable[None]],
) -> None:
    """Execute ``body(client)`` against either a provided or autocreated client.

    Tests pass an in-memory client (driven by respx); production code
    omits it so the harness builds one from ``cfg`` and closes it on the
    way out. Centralising the lifecycle keeps each experiment's ``run()``
    focused on workload definition.
    """
    if client is not None:
        await body(client)
        return
    async with memclaw_client(
        cfg.base_url,
        cfg.api_key,
        timeout_s=cfg.request_timeout_s,
        max_retries=cfg.max_retries,
        min_write_interval_ms=cfg.min_request_interval_ms,
    ) as c:
        await body(c)


def open_sink_with_envelope(
    events_path: Path,
    *,
    run_id: str,
    experiment: str,
    cfg: HarnessConfig,
    extra: dict[str, Any] | None = None,
) -> JsonlSink:
    """Open a ``JsonlSink`` and write the ``run_start`` envelope.

    Returns the open sink so the caller can ``with`` it. The envelope's
    ``data`` carries the experiment name, seed, base_url, and any
    experiment-specific fields supplied via ``extra``.
    """
    sink = JsonlSink(events_path)
    payload: dict[str, Any] = {
        "run_id": run_id,
        "experiment": experiment,
        "seed": cfg.seed,
        "base_url": cfg.base_url,
    }
    if extra:
        payload.update(extra)
    sink.write("run_start", payload)
    return sink

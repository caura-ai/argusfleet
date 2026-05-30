"""Tabular metric helpers — JSONL → DataFrame → CSV."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


def wilson_ci(successes: int, trials: int, *, z: float = 1.96) -> tuple[float, float]:
    """Two-sided Wilson interval for a binomial proportion.

    The Wilson interval handles the ``0/n`` and ``n/n`` boundary cases
    correctly, unlike the normal-approximation CI. With the default
    ``z = 1.96`` the interval has approximately 95% nominal coverage.
    Returns ``(lower, upper)`` both clipped to [0, 1].

    Use this when reporting rates derived from small samples — e.g.
    ``leak_rate = 0/80`` looks like a certainty but actually has a Wilson
    95% upper bound near 0.045.
    """
    if trials <= 0:
        return (0.0, 1.0)
    p = successes / trials
    z2 = z * z
    denom = 1.0 + z2 / trials
    center = (p + z2 / (2.0 * trials)) / denom
    margin = z * math.sqrt(p * (1.0 - p) / trials + z2 / (4.0 * trials * trials)) / denom
    lo = max(0.0, center - margin)
    hi = min(1.0, center + margin)
    return (lo, hi)


def wilson_yerr(
    values: list[float],
    cis: list[tuple[float, float]],
) -> list[list[float]]:
    """Build a matplotlib ``yerr`` ``[lower, upper]`` (shape ``(2, N)``) of
    non-negative error magnitudes from point ``values`` and their
    ``(lo, hi)`` intervals.

    Clamps at ``0`` so floating-point artefacts (e.g. ``hi - value`` landing
    at ``-1e-16`` when both equal ``1.0``) don't trip matplotlib's
    "negative error magnitude" check.
    """
    lower = [max(0.0, v - lo) for v, (lo, _hi) in zip(values, cis)]
    upper = [max(0.0, hi - v) for v, (_lo, hi) in zip(values, cis)]
    return [lower, upper]


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def events_to_frame(events: Iterable[dict[str, Any]], event_type: str | None = None) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for ev in events:
        if event_type and ev.get("event") != event_type:
            continue
        data = ev.get("data") or {}
        if isinstance(data, dict):
            rows.append({"ts": ev.get("ts"), "event": ev.get("event"), **data})
        else:
            rows.append({"ts": ev.get("ts"), "event": ev.get("event"), "value": data})
    return pd.DataFrame(rows)


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def summarize_latency(df: pd.DataFrame, col: str = "latency_ms") -> dict[str, float]:
    """Return p50/p95/p99/mean for a latency column. Empty frame → zeros."""
    if df.empty or col not in df.columns:
        return {"count": 0, "mean": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0}
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    if s.empty:
        return {"count": 0, "mean": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0}
    return {
        "count": int(s.shape[0]),
        "mean": float(s.mean()),
        "p50": float(s.quantile(0.50)),
        "p95": float(s.quantile(0.95)),
        "p99": float(s.quantile(0.99)),
    }

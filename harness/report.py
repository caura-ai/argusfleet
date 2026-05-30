"""Cross-experiment result aggregation.

The harness produces one ``outputs/runs/<UTC-ts>-<experiment>-seed<N>/``
directory per experiment invocation. This module discovers the most
recent run per experiment, reads each ``summary.csv``, and renders a
consolidated markdown report — what you'd paste into a PR description or
a paper appendix.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


KNOWN_EXPERIMENTS = ("leakage", "contradiction", "provenance", "propagation")


@dataclass(slots=True)
class _RunInfo:
    experiment: str
    run_dir: Path
    started_at: str | None
    summary: dict[str, Any]
    plots: list[Path]


def _read_run_start(events_path: Path) -> dict[str, Any] | None:
    if not events_path.exists():
        return None
    try:
        with open(events_path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                if rec.get("event") == "run_start":
                    return rec
    except (OSError, json.JSONDecodeError):
        return None
    return None


def _read_summary(summary_path: Path) -> dict[str, Any]:
    if not summary_path.exists():
        return {}
    try:
        df = pd.read_csv(summary_path)
        # The column access must stay inside the guard: a CSV that parses
        # but lacks ``metric``/``value`` (a truncated/partial run) would
        # otherwise KeyError out here, aborting the whole report instead of
        # degrading this one run to ``{}``.
        return dict(zip(df["metric"].astype(str), df["value"]))
    except Exception:
        return {}


def _discover_runs(outputs_dir: Path) -> dict[str, _RunInfo]:
    """Return ``{experiment: most-recent _RunInfo}``."""
    runs_root = outputs_dir / "runs"
    if not runs_root.exists():
        return {}

    latest: dict[str, _RunInfo] = {}
    for run_dir in runs_root.iterdir():
        if not run_dir.is_dir():
            continue
        events = run_dir / "events.jsonl"
        start = _read_run_start(events)
        if not start:
            continue
        data = start.get("data") or {}
        experiment = data.get("experiment")
        if experiment not in KNOWN_EXPERIMENTS:
            continue
        info = _RunInfo(
            experiment=experiment,
            run_dir=run_dir,
            started_at=start.get("ts"),
            summary=_read_summary(run_dir / "summary.csv"),
            plots=sorted(run_dir.glob("*.png")),
        )
        existing = latest.get(experiment)
        if existing is None or (info.started_at or "") > (existing.started_at or ""):
            latest[experiment] = info
    return latest


# ---------------------------------------------------------------------- #
# Headline-metric selection                                               #
# ---------------------------------------------------------------------- #

# Each experiment exposes a handful of metrics; the report shows the
# ones a reader cares about first, in a fixed order.
_HEADLINE_METRICS: dict[str, list[str]] = {
    "leakage": [
        "n_probes",
        "n_expected_allow",
        "n_expected_deny",
        "n_leaks",
        "leak_rate",
        "leak_rate_ci95_lo",
        "leak_rate_ci95_hi",
        "n_misses",
        "miss_rate",
        "miss_rate_ci95_lo",
        "miss_rate_ci95_hi",
        "search_leak_rate",
        "search_miss_rate",
        "access_latency_p50",
        "access_latency_p95",
        "search_latency_p50",
        "search_latency_p95",
    ],
    "contradiction": [
        "n_facts_runs",
        "detection_rate",
        "stale_read_rate",
        "write_latency_p50",
        "write_latency_p95",
    ],
    "provenance": [
        "n_chains",
        "completeness_rate",
        "accuracy_rate",
        "depth_fidelity_rate",
        "fetch_latency_p50",
        "fetch_latency_p95",
    ],
    "propagation": [
        "fleet_visibility_rate",
        "leak_rate",
        "n_missed_propagations",
        "ownership_transfer_rate",
        "t_visible_p50",
        "t_visible_p95",
    ],
}


def _format_value(v: Any) -> str:
    if v is None:
        return "—"
    if isinstance(v, float):
        if v != v:  # NaN
            return "—"
        if abs(v) >= 1000 or (v != 0 and abs(v) < 0.01):
            return f"{v:.3g}"
        return f"{v:.4f}".rstrip("0").rstrip(".") or "0"
    return str(v)


def render_markdown(latest: dict[str, _RunInfo]) -> str:
    """Render a consolidated markdown report from the latest runs."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines: list[str] = [
        "# ArgusFleet — Latest Results",
        "",
        f"_Generated {now}_",
        "",
    ]
    if not latest:
        lines.append("No runs found. Execute an experiment first, e.g.")
        lines.append("")
        lines.append("```bash")
        lines.append("python -m argusfleet run leakage")
        lines.append("```")
        return "\n".join(lines)

    # Top-level headline table — one row per experiment.
    lines.append("## Headline")
    lines.append("")
    lines.append("| Experiment | Run directory | Started (UTC) |")
    lines.append("| --- | --- | --- |")
    for exp in KNOWN_EXPERIMENTS:
        info = latest.get(exp)
        if info is None:
            lines.append(f"| {exp} | _no run yet_ | — |")
            continue
        lines.append(
            f"| {exp} | `{info.run_dir.name}` | {info.started_at or '—'} |"
        )
    lines.append("")

    # Per-experiment headline metrics + plot links.
    for exp in KNOWN_EXPERIMENTS:
        info = latest.get(exp)
        lines.append(f"## {exp}")
        lines.append("")
        if info is None:
            lines.append("_No run yet._")
            lines.append("")
            continue
        lines.append(f"Run dir: `{info.run_dir}`")
        lines.append("")
        lines.append("| Metric | Value |")
        lines.append("| --- | ---: |")
        wanted = _HEADLINE_METRICS.get(exp, [])
        seen = set()
        for metric in wanted:
            if metric in info.summary:
                lines.append(f"| {metric} | {_format_value(info.summary[metric])} |")
                seen.add(metric)
        # Append any non-headline metric afterwards, in original order.
        for metric, value in info.summary.items():
            if metric in seen:
                continue
            lines.append(f"| {metric} | {_format_value(value)} |")
        lines.append("")
        if info.plots:
            lines.append("Plots:")
            for p in info.plots:
                lines.append(f"- `{p.name}` (`{p}`)")
            lines.append("")
    return "\n".join(lines)


def write_report(
    outputs_dir: Path,
    *,
    copy_plots: bool = True,
) -> dict[str, Any]:
    """Generate the report, save it under ``outputs/reports/<ts>/``.

    Returns a small dict with the report path and the run directories
    that fed into it — useful for CI and downstream analysis.
    """
    latest = _discover_runs(outputs_dir)
    md = render_markdown(latest)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_dir = outputs_dir / "reports" / ts
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / "report.md"
    report_path.write_text(md, encoding="utf-8")
    copied: list[str] = []
    if copy_plots:
        for info in latest.values():
            for plot in info.plots:
                target = report_dir / f"{info.experiment}-{plot.name}"
                shutil.copy2(plot, target)
                copied.append(str(target))
    return {
        "report_path": str(report_path),
        "report_dir": str(report_dir),
        "markdown": md,
        "experiments": {
            exp: {
                "run_dir": str(info.run_dir),
                "started_at": info.started_at,
                "summary": info.summary,
            }
            for exp, info in latest.items()
        },
        "copied_plots": copied,
    }

"""Tests for the cross-experiment report aggregator."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from argusfleet.harness.report import _discover_runs, render_markdown, write_report


def _seed_run(
    outputs_dir: Path,
    experiment: str,
    *,
    summary: dict[str, float],
    started_at: str,
    label: str = "test",
    plots: list[str] | None = None,
    backend: str | None = None,
) -> Path:
    run_dir = outputs_dir / "runs" / f"{started_at}-{label}-seed7"
    run_dir.mkdir(parents=True, exist_ok=True)
    events = run_dir / "events.jsonl"
    data: dict[str, object] = {
        "experiment": experiment,
        "seed": 7,
        "run_id": run_dir.name,
    }
    if backend is not None:
        data["backend"] = backend
    events.write_text(
        json.dumps({"ts": started_at, "event": "run_start", "data": data}) + "\n",
        encoding="utf-8",
    )
    pd.DataFrame(
        [{"metric": k, "value": v} for k, v in summary.items()]
    ).to_csv(run_dir / "summary.csv", index=False)
    for name in plots or []:
        (run_dir / name).write_bytes(b"\x89PNG\r\n\x1a\n")  # PNG magic only
    return run_dir


def test_discover_picks_latest_per_experiment(tmp_path: Path) -> None:
    _seed_run(
        tmp_path,
        "leakage",
        summary={"leakage_rate": 0.1, "n_probes": 100},
        started_at="20260501T120000Z",
    )
    _seed_run(
        tmp_path,
        "leakage",
        summary={"leakage_rate": 0.0, "n_probes": 200},
        started_at="20260520T120000Z",
        label="newer",
    )
    _seed_run(
        tmp_path,
        "contradiction",
        summary={"detection_rate": 1.0, "stale_read_rate": 0.0},
        started_at="20260510T120000Z",
    )

    latest = _discover_runs(tmp_path)
    # Keys are (backend, experiment); envelope-less legacy runs default
    # to the memclaw backend.
    assert set(latest.keys()) == {("memclaw", "leakage"), ("memclaw", "contradiction")}
    assert "newer" in latest[("memclaw", "leakage")].run_dir.name
    assert latest[("memclaw", "leakage")].summary["n_probes"] == 200
    assert latest[("memclaw", "contradiction")].summary["detection_rate"] == 1.0


def test_discover_keeps_backends_separate(tmp_path: Path) -> None:
    """A newer mem0 run must NOT shadow the latest memclaw run of the
    same experiment — the report keys on (backend, experiment)."""
    _seed_run(
        tmp_path,
        "leakage",
        summary={"leak_rate": 0.0, "n_probes": 100},
        started_at="20260601T120000Z",
        label="claw",
    )
    _seed_run(
        tmp_path,
        "leakage",
        summary={"leak_rate": 1.0, "n_probes": 100},
        started_at="20260602T120000Z",
        label="m0",
        backend="mem0",
    )
    latest = _discover_runs(tmp_path)
    assert latest[("memclaw", "leakage")].summary["leak_rate"] == 0.0
    assert latest[("mem0", "leakage")].summary["leak_rate"] == 1.0
    md = render_markdown(latest)
    assert "## leakage — memclaw" in md
    assert "## leakage — mem0" in md


def test_render_markdown_handles_partial_coverage(tmp_path: Path) -> None:
    _seed_run(
        tmp_path,
        "leakage",
        summary={"leakage_rate": 0.0, "n_probes": 50, "n_violations": 0},
        started_at="20260520T120000Z",
    )
    latest = _discover_runs(tmp_path)
    md = render_markdown(latest)
    assert "# ArgusFleet — Latest Results" in md
    assert "## leakage" in md
    assert "leakage_rate" in md
    # Stubbed experiments must show up too, marked as having no run.
    assert "## contradiction" in md
    assert "_No run yet._" in md


def test_write_report_copies_plots(tmp_path: Path) -> None:
    _seed_run(
        tmp_path,
        "leakage",
        summary={"leakage_rate": 0.0, "n_probes": 10},
        started_at="20260520T120000Z",
        plots=["leakage_by_axis.png"],
    )
    out = write_report(tmp_path)
    report_path = Path(out["report_path"])
    assert report_path.exists()
    md = report_path.read_text(encoding="utf-8")
    assert "## leakage" in md
    # The copied plot should land next to the markdown with a
    # backend+experiment-prefixed filename so paths don't collide
    # across runs or platforms.
    copied_names = [Path(p).name for p in out["copied_plots"]]
    assert "memclaw-leakage-leakage_by_axis.png" in copied_names


def test_render_markdown_empty(tmp_path: Path) -> None:
    md = render_markdown(_discover_runs(tmp_path))
    assert "No runs found" in md


def test_leakage_split_metrics_appear_in_report(tmp_path: Path) -> None:
    """The headline table for leakage must include leak_rate AND miss_rate
    so a reader can tell the two failure modes apart at a glance."""
    _seed_run(
        tmp_path,
        "leakage",
        summary={
            "n_probes": 100,
            "n_leaks": 5,
            "n_misses": 12,
            "leak_rate": 0.05,
            "miss_rate": 0.12,
            "leakage_rate": 0.17,
            "search_latency_p50": 200.0,
            "search_latency_p95": 500.0,
        },
        started_at="20260520T120000Z",
    )
    md = render_markdown(_discover_runs(tmp_path))
    # The headline section for leakage must surface both rates, in the
    # order defined by _HEADLINE_METRICS.
    leak_idx = md.find("leak_rate")
    miss_idx = md.find("miss_rate")
    assert leak_idx > 0
    assert miss_idx > 0
    assert leak_idx < miss_idx


def test_report_dispatches_unknown_experiment_gracefully(tmp_path: Path) -> None:
    """A run_start envelope from an experiment we don't know about
    should be ignored (not crash report generation)."""
    run_dir = tmp_path / "runs" / "20260520T120000Z-mystery-seed1"
    run_dir.mkdir(parents=True)
    (run_dir / "events.jsonl").write_text(
        json.dumps(
            {
                "ts": "20260520T120000Z",
                "event": "run_start",
                "data": {"experiment": "mystery"},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    md = render_markdown(_discover_runs(tmp_path))
    # mystery should be silently skipped — neither named in the report
    # nor surfaced as a known experiment. Since no known run exists
    # alongside it, the empty-state branch is what renders here.
    assert "mystery" not in md
    assert "No runs found" in md or "_No run yet._" in md

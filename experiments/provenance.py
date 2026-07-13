"""Provenance experiment.

What the experiment measures
----------------------------
Governed memory is only as trustworthy as its provenance chain. For each
retrieved row, the system must be able to answer:

- *Who* wrote this?
- *From what* was it derived?
- *Through what workflow* did it pass?

The experiment builds deliberately layered derivation chains: agent_0
writes a raw fact, agent_1 reads it and writes a derived fact referencing
agent_0's row, agent_2 reads agent_1's and writes another, and so on
through depth ``K``. After all writes settle, the harness fetches every
leaf memory and reconstructs the chain by walking
``metadata["derived_from"]`` back to the root.

For each leaf we then check:

- **Completeness** — every ancestor on the recorded chain is still
  reachable via ``GET /memories/{id}`` (no broken links).
- **Accuracy** — at each chain step, the fetched ancestor's ``agent_id``
  matches the writer the plan recorded. A mismatch means the server
  preserved the link but altered the row identity — a corruption.
- **Depth fidelity** — the reconstructed chain length equals the planned
  depth. Truncation here means the server is losing intermediate writes.

Outputs
-------
- ``events.jsonl`` — writes, fetches, per-chain verification results.
- ``provenance.csv`` — one row per leaf with reconstructed chain.
- ``summary.csv`` — completeness, accuracy, depth-fidelity rates plus
  fetch-latency percentiles.
- ``completeness_by_depth.png`` — completeness as a function of planned
  chain depth, the curve the paper's provenance section will show.
- ``fetch_latency.png`` — histogram of per-fetch latencies.
"""

from __future__ import annotations

import asyncio
import logging
import random
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from argusfleet.experiments._common import bar_chart_from_frame
from argusfleet.harness.backend import BackendError, MemoryBackend
from argusfleet.harness.config import HarnessConfig
from argusfleet.harness.metrics import (
    events_to_frame,
    read_jsonl,
    summarize_latency,
    wilson_ci,
    wilson_yerr,
    write_csv,
)
from argusfleet.harness.plots import plot_latency_hist
from argusfleet.harness.runner import (
    make_run_dir,
    open_sink_with_envelope,
    run_with_client,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------- #
# Plan                                                                    #
# ---------------------------------------------------------------------- #


@dataclass(slots=True)
class _ChainStep:
    """One write inside a derivation chain.

    ``writer_agent`` is recorded so the verification phase can check that
    the server returned the same agent on read — a substitution would be
    a quiet integrity bug.
    """

    chain_id: str
    depth_index: int  # 0 = root, 1 = first derivation, ...
    writer_agent: str
    workflow_id: str
    content: str


@dataclass(slots=True)
class _Chain:
    chain_id: str
    tenant_id: str
    fleet_id: str
    depth: int
    workflow_id: str
    steps: list[_ChainStep] = field(default_factory=list)


def tenant_ids_for(cfg: HarnessConfig) -> list[str]:
    """Tenant this experiment will write to (single source of truth for
    chain-building and ``run all`` cleanup; see leakage.tenant_ids_for)."""
    exp = cfg.experiment or {}
    tenant_id = (
        (cfg.tenant_ids[0] if cfg.tenant_ids else None)
        or exp.get("tenant_id")
        or f"mc_prv_{cfg.seed}"
    )
    return [tenant_id]


def _build_chains(cfg: HarnessConfig, *, nonce: str) -> list[_Chain]:
    rng = random.Random(cfg.seed)
    exp = cfg.experiment or {}
    n_chains: int = int(exp.get("n_chains", 6))
    depth: int = int(exp.get("chain_depth", 4))
    tenant_id: str = tenant_ids_for(cfg)[0]
    fleet_id: str = exp.get("fleet_id") or "fleet_prv"

    if depth < 2:
        raise ValueError("provenance chains require depth >= 2")

    # Per-chain topic + per-step verb pairs. Each chain becomes a small
    # narrative: a root *observation*, then a *hypothesis*, then a
    # *mitigation*, then a *verification*. Diversifying the prose this
    # way scatters writes across embedding space far enough that the
    # server's near-duplicate detector doesn't 409 us.
    chain_topics = [
        ("error rate on checkout API", "{n}", "5xx surge"),
        ("memory leak in worker pool", "{n}", "RSS growth"),
        ("DB replication lag", "{n}", "slave fell behind"),
        ("auth token expiry storm", "{n}", "mass logout"),
        ("CDN cache poisoning incident", "{n}", "stale assets"),
        ("kafka consumer rebalance loop", "{n}", "lag spike"),
        ("payment retries gone wrong", "{n}", "double-charged"),
        ("DNS resolution intermittency", "{n}", "timeouts"),
        ("Prometheus scrape failures", "{n}", "missing metrics"),
        ("Redis eviction storm", "{n}", "cache miss flood"),
        ("S3 throttling on archive bucket", "{n}", "PUT 503s"),
        ("certificate renewal failure", "{n}", "TLS handshake errors"),
        ("background job stalls", "{n}", "queue backed up"),
        ("graphql resolver N+1 regression", "{n}", "latency cliff"),
        ("feature flag misconfiguration", "{n}", "users locked out"),
        ("billing job duplicate runs", "{n}", "overcharge alerts"),
        ("CI build agent saturation", "{n}", "queue depth >100"),
        ("ingest pipeline schema drift", "{n}", "deserialization errors"),
        ("search index corruption", "{n}", "stale results"),
        ("API gateway 502 burst", "{n}", "upstream connection drops"),
        ("ldap directory sync stalled", "{n}", "permission drift"),
        ("storage volume hot spotting", "{n}", "iops cap"),
        ("scheduled cron drift", "{n}", "missed window"),
        ("oauth refresh-token revocation wave", "{n}", "session loss"),
        ("queue dead-letter accumulation", "{n}", "alerts paged"),
    ]

    step_templates = [
        "Observation in incident {chain_short}: {topic} for {label}; signal: {signal}, observed at {ts}.",
        "Hypothesis for incident {chain_short}: investigation suggests upstream cause was {topic}; signal mirrors {signal}.",
        "Mitigation applied for incident {chain_short}: action targeted {topic}, expected to resolve {signal} pattern.",
        "Verification post-mitigation for incident {chain_short}: {signal} signal normalised on {label}, monitoring continues.",
    ]

    chains: list[_Chain] = []
    for ci in range(n_chains):
        chain_uuid = uuid.UUID(int=rng.getrandbits(128)).hex[:8]
        chain_id = f"chain-{ci:03d}-{chain_uuid}"
        chain_short = chain_uuid
        workflow_id = f"wf-{chain_id}"
        topic, label_tpl, signal = chain_topics[ci % len(chain_topics)]
        label = label_tpl.format(n=f"svc-{ci:03d}-{nonce[-8:]}")
        c = _Chain(
            chain_id=chain_id,
            tenant_id=tenant_id,
            fleet_id=fleet_id,
            depth=depth,
            workflow_id=workflow_id,
        )
        for di in range(depth):
            step_tpl = step_templates[di % len(step_templates)]
            ts = f"{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}"
            c.steps.append(
                _ChainStep(
                    chain_id=chain_id,
                    depth_index=di,
                    writer_agent=f"agent_prv_{ci % 4}_{di}",
                    workflow_id=workflow_id,
                    content=step_tpl.format(
                        chain_short=chain_short,
                        topic=topic,
                        label=label,
                        signal=signal,
                        ts=ts,
                    ),
                )
            )
        chains.append(c)
    return chains


# ---------------------------------------------------------------------- #
# Per-chain execution                                                     #
# ---------------------------------------------------------------------- #


async def _write_chain(
    client: MemoryBackend, chain: _Chain, sink: JsonlSink
) -> dict[int, dict[str, Any]]:
    """Walk the chain root-to-leaf, recording each write's memory_id.

    Returns ``{depth_index: write_record}``. The caller uses this to seed
    verification — we don't trust the server to give us the chain on read,
    we verify the server agrees with what we wrote.
    """
    written: dict[int, dict[str, Any]] = {}
    prev_memory_id: str | None = None
    for step in chain.steps:
        metadata = {
            "experiment": "provenance",
            "chain_id": chain.chain_id,
            "workflow_id": chain.workflow_id,
            "depth_index": step.depth_index,
            "writer_role": "root" if step.depth_index == 0 else "derived",
            # ``derived_from`` is the harness-side provenance record. The
            # verification phase walks this list back through the chain.
            "derived_from": [prev_memory_id] if prev_memory_id else [],
        }
        try:
            resp = await client.write_memory(
                tenant_id=chain.tenant_id,
                fleet_id=chain.fleet_id,
                agent_id=step.writer_agent,
                content=step.content,
                metadata=metadata,
            )
            payload = resp.json or {}
            memory_id = payload.get("id")
            written[step.depth_index] = {
                "chain_id": chain.chain_id,
                "depth_index": step.depth_index,
                "memory_id": memory_id,
                "writer_agent": step.writer_agent,
                "parent_memory_id": prev_memory_id,
                "write_latency_ms": resp.latency_ms,
                "trace_id": resp.trace_id,
                "succeeded": True,
            }
            sink.write("write", written[step.depth_index])
            prev_memory_id = memory_id
        except BackendError as e:
            written[step.depth_index] = {
                "chain_id": chain.chain_id,
                "depth_index": step.depth_index,
                "memory_id": None,
                "writer_agent": step.writer_agent,
                "parent_memory_id": prev_memory_id,
                "write_latency_ms": None,
                "trace_id": e.trace_id,
                "succeeded": False,
                "error": e.message,
                "status_code": e.status_code,
            }
            sink.write("write", written[step.depth_index])
            # Don't continue the chain if a write failed — subsequent
            # links would reference a phantom parent and the verification
            # would report a false-positive corruption.
            break
    return written


async def _verify_chain(
    client: MemoryBackend,
    chain: _Chain,
    written: dict[int, dict[str, Any]],
    sink: JsonlSink,
) -> dict[str, Any]:
    """Fetch the leaf, walk ``derived_from`` back, and grade the result."""
    leaf_index = chain.depth - 1
    leaf_record = written.get(leaf_index)
    if leaf_record is None or not leaf_record.get("memory_id"):
        row = {
            "chain_id": chain.chain_id,
            "depth_planned": chain.depth,
            "depth_observed": 0,
            "complete": False,
            # A leaf write failure is an availability problem, not a
            # writer-identity substitution — we never compared an agent_id.
            # Mark it ineligible so it's excluded from the accuracy
            # denominator (B5) rather than deflating accuracy_rate.
            "accurate": False,
            "accuracy_eligible": False,
            "depth_match": False,
            "broken_at": "root_or_intermediate_write_failed",
            "fetch_latency_ms_max": None,
        }
        sink.write("verification", row)
        return row

    reconstructed: list[dict[str, Any]] = []
    cursor_id: str | None = leaf_record["memory_id"]
    fetch_latencies: list[float] = []
    broken_at: str | None = None
    accurate = True
    # Bound the walk. This experiment exists to detect provenance
    # corruption, so a self-referential or fabricated-ancestor
    # ``derived_from`` (exactly that corruption) would otherwise loop here
    # forever — the per-request timeout caps each call, not their number.
    # Stop on a repeated id or once we've walked past the planned depth, and
    # record the anomaly as a broken link instead of hanging the gather.
    visited: set[str] = set()

    while cursor_id is not None:
        if cursor_id in visited:
            broken_at = "cycle_detected"
            sink.write(
                "fetch",
                {
                    "chain_id": chain.chain_id,
                    "memory_id": cursor_id,
                    "ok": False,
                    "status_code": None,
                    "note": "cycle_detected",
                },
            )
            break
        if len(reconstructed) >= chain.depth:
            broken_at = "exceeded_planned_depth"
            sink.write(
                "fetch",
                {
                    "chain_id": chain.chain_id,
                    "memory_id": cursor_id,
                    "ok": False,
                    "status_code": None,
                    "note": "exceeded_planned_depth",
                },
            )
            break
        visited.add(cursor_id)
        try:
            resp = await client.retrieve_memory(cursor_id, tenant_id=chain.tenant_id)
        except BackendError as e:
            broken_at = f"fetch_{e.status_code}"
            sink.write(
                "fetch",
                {
                    "chain_id": chain.chain_id,
                    "memory_id": cursor_id,
                    "ok": False,
                    "status_code": e.status_code,
                    "trace_id": e.trace_id,
                },
            )
            break

        if resp.latency_ms is not None:
            fetch_latencies.append(resp.latency_ms)
        payload = resp.json or {}
        meta = payload.get("metadata") or {}
        depth_index = meta.get("depth_index")
        observed_writer = payload.get("agent_id")
        # The planned writer is whatever we stored at this depth.
        planned_record = next(
            (w for w in written.values() if w.get("memory_id") == cursor_id), None
        )
        planned_writer = planned_record["writer_agent"] if planned_record else None
        if planned_writer is not None and observed_writer != planned_writer:
            accurate = False

        reconstructed.append(
            {
                "memory_id": cursor_id,
                "depth_index": depth_index,
                "observed_writer": observed_writer,
                "planned_writer": planned_writer,
            }
        )
        sink.write(
            "fetch",
            {
                "chain_id": chain.chain_id,
                "memory_id": cursor_id,
                "ok": True,
                "depth_index": depth_index,
                "observed_writer": observed_writer,
                "planned_writer": planned_writer,
                "fetch_latency_ms": resp.latency_ms,
                "trace_id": resp.trace_id,
            },
        )

        parents = meta.get("derived_from") or []
        cursor_id = parents[0] if parents else None

    observed_depth = len(reconstructed)
    # Two INDEPENDENT metrics (they used to collapse into one):
    #   completeness = link reachability — every link we followed resolved,
    #     with no fetch error, cycle, or over-length anomaly. A server that
    #     returns a valid but truncated chain (empty ``derived_from`` early)
    #     is still complete here; a 404-ing one is not.
    #   depth_match  = truncation/fidelity — the full planned depth surfaced.
    # Decoupling them means a truncating server no longer scores identically
    # to a 404-ing one.
    complete = broken_at is None
    depth_match = observed_depth == chain.depth

    row = {
        "chain_id": chain.chain_id,
        "depth_planned": chain.depth,
        "depth_observed": observed_depth,
        "complete": complete,
        "accurate": accurate,
        "accuracy_eligible": True,
        "depth_match": depth_match,
        "broken_at": broken_at,
        "fetch_latency_ms_max": max(fetch_latencies) if fetch_latencies else None,
        "fetch_latency_ms_sum": sum(fetch_latencies) if fetch_latencies else 0.0,
        "reconstructed": reconstructed,
    }
    sink.write("verification", row)
    return row


# ---------------------------------------------------------------------- #
# Runner                                                                  #
# ---------------------------------------------------------------------- #


async def run(
    cfg: HarnessConfig,
    *,
    client: MemoryBackend | None = None,
) -> dict[str, Any]:
    run_id, run_dir, events_path = make_run_dir(cfg, "provenance")
    chains = _build_chains(cfg, nonce=run_id)

    exp = cfg.experiment or {}
    rate_gate = asyncio.Semaphore(int(exp.get("max_in_flight", 4)))

    logger.info(
        "provenance run %s — n_chains=%d depth=%d",
        run_id,
        len(chains),
        chains[0].depth if chains else 0,
    )

    async def _execute(c: MemoryBackend) -> None:
        with open_sink_with_envelope(
            events_path,
            run_id=run_id,
            experiment="provenance",
            cfg=cfg,
            extra={
                "n_chains": len(chains),
                "chain_depth": chains[0].depth if chains else 0,
            },
        ) as sink:

            async def _do_one(chain: _Chain) -> None:
                async with rate_gate:
                    written = await _write_chain(c, chain, sink)
                    await _verify_chain(c, chain, written, sink)

            await asyncio.gather(*(_do_one(ch) for ch in chains))
            sink.write("run_end", {"run_id": run_id})

    await run_with_client(cfg, client=client, body=_execute)
    result = _materialize_outputs(run_dir, events_path)
    result["tenant_ids"] = tenant_ids_for(cfg)
    return result


# ---------------------------------------------------------------------- #
# Output materialization                                                  #
# ---------------------------------------------------------------------- #


def _materialize_outputs(run_dir: Path, events_path: Path) -> dict[str, Any]:
    events = read_jsonl(events_path)
    verifications = events_to_frame(events, "verification")
    fetches = events_to_frame(events, "fetch")
    writes = events_to_frame(events, "write")

    provenance_csv = run_dir / "provenance.csv"
    summary_csv = run_dir / "summary.csv"
    completeness_plot = run_dir / "completeness_by_depth.png"
    latency_plot = run_dir / "fetch_latency.png"

    if not verifications.empty:
        # Drop the nested ``reconstructed`` column from the CSV — it's
        # in ``events.jsonl`` already and bloats the row past what's
        # comfortable in a spreadsheet.
        csv_frame = verifications.drop(columns=["reconstructed"], errors="ignore")
        write_csv(csv_frame, provenance_csv)

        by_depth = (
            verifications.groupby("depth_planned")
            .agg(
                n=("complete", "size"),
                complete=("complete", "sum"),
                accurate=("accurate", "sum"),
                depth_match=("depth_match", "sum"),
            )
            .reset_index()
        )
        by_depth["completeness_rate"] = by_depth["complete"] / by_depth["n"].clip(lower=1)
        _ci = [wilson_ci(int(r.complete), int(r.n)) for r in by_depth.itertuples()]
        comp_yerr = wilson_yerr(by_depth["completeness_rate"].tolist(), _ci)
        bar_chart_from_frame(
            by_depth,
            x="depth_planned",
            y="completeness_rate",
            label_fmt=lambda r: f"{int(r['complete'])}/{int(r['n'])}",
            title="Provenance completeness by planned chain depth",
            ylabel="Completeness rate",
            out_path=completeness_plot,
            yerr=comp_yerr,
        )

    if not fetches.empty and "fetch_latency_ms" in fetches.columns:
        plot_latency_hist(
            fetches,
            col="fetch_latency_ms",
            out_path=latency_plot,
            title="Provenance fetch latency",
        )

    n = int(len(verifications))
    n_complete = int(verifications["complete"].sum()) if "complete" in verifications else 0
    n_depth_match = (
        int(verifications["depth_match"].sum()) if "depth_match" in verifications else 0
    )
    # Accuracy = writer-identity fidelity, measured only over chains that
    # actually reached the comparison. Write-failed chains are flagged
    # ``accuracy_eligible=False`` and dropped from the denominator (B5);
    # rows predating the flag (no column) are treated as eligible.
    if "accurate" in verifications.columns:
        if "accuracy_eligible" in verifications.columns:
            eligible = verifications[verifications["accuracy_eligible"] != False]  # noqa: E712
        else:
            eligible = verifications
        n_accuracy_eval = int(len(eligible))
        n_accurate = int(eligible["accurate"].sum())
    else:
        n_accuracy_eval = 0
        n_accurate = 0
    completeness = n_complete / n if n else 0.0
    accuracy = n_accurate / n_accuracy_eval if n_accuracy_eval else 0.0
    depth_fidelity = n_depth_match / n if n else 0.0

    summary_rows: list[dict[str, Any]] = [
        {"metric": "n_chains", "value": n},
        {"metric": "completeness_rate", "value": completeness},
        {"metric": "accuracy_rate", "value": accuracy},
        {"metric": "accuracy_n_eval", "value": n_accuracy_eval},
        {"metric": "depth_fidelity_rate", "value": depth_fidelity},
        {"metric": "n_writes", "value": int(len(writes))},
        {"metric": "n_fetches", "value": int(len(fetches))},
    ]
    if not fetches.empty and "fetch_latency_ms" in fetches.columns:
        for k, v in summarize_latency(fetches, "fetch_latency_ms").items():
            summary_rows.append({"metric": f"fetch_latency_{k}", "value": v})
    write_csv(pd.DataFrame(summary_rows), summary_csv)

    return {
        "run_dir": str(run_dir),
        "events": str(events_path),
        "provenance_csv": str(provenance_csv),
        "summary_csv": str(summary_csv),
        "completeness_plot": str(completeness_plot),
        "latency_plot": str(latency_plot),
        "n_chains": n,
        "completeness_rate": completeness,
        "accuracy_rate": accuracy,
        "accuracy_n_eval": n_accuracy_eval,
        "depth_fidelity_rate": depth_fidelity,
    }

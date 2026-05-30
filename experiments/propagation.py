"""Cross-agent propagation experiment.

What the experiment measures
----------------------------
A fleet-scoped write must propagate to every fleet sibling, and only
fleet siblings — not other agents in the same tenant, not other tenants.
The harness exercises three signals:

- **Visibility** — for each (writer, fact, fleet-sibling reader) tuple,
  how long until the reader's search returns the fact? Servers that
  enrich asynchronously will exhibit a non-zero floor here.
- **Isolation** — for each fact, every non-fleet reader (same tenant,
  different fleet) must *never* return the fact during the polling
  window. Any hit is a leak.
- **Ownership transfer** — after a redistribute call moves a fact onto
  a target agent, both ``agent_id`` and search-by-agent results must
  reflect the new owner. The original writer should no longer "own" it.

The run has two decoupled phases, because *rate* and *latency* want
opposite measurement regimes:

1. **Window phase (latency).** A small set of dedicated facts is written
   one at a time, each polled immediately and continuously from write
   completion until its canary surfaces. Sequential, so the poll stream
   stays under the per-tenant ``/search`` budget; ``t_visible`` therefore
   reflects server enrichment lag, not harness scheduling, and does not
   inflate with workload size.
2. **Rate phase (visibility / isolation).** The writer emits ``n_facts``
   fleet-scoped memories; every fleet sibling polls ``/search`` until the
   fact surfaces or ``visibility_timeout_ms`` elapses, and every out-of-
   fleet reader does a single search (any hit is a leak). This phase
   scales with ``n_facts`` to tighten the rate CIs; its per-probe latency
   is *not* used (a drained probe's first look lands after the row is
   already searchable).
3. A random subset of facts is redistributed onto a foreign-but-same-
   tenant agent; the harness re-fetches and verifies ``agent_id`` flipped.

Outputs
-------
- ``events.jsonl`` — window probes, writes, every poll, redistributes.
- ``propagation.csv`` — one row per (fact, reader) pair.
- ``summary.csv`` — visibility / isolation / ownership rates plus the
  window-phase time-to-visibility percentiles.
- ``visibility_by_relation.png`` — visibility rate split into
  fleet-sibling / cross-fleet axes.
- ``time_to_visibility.png`` — histogram of the window-phase T_visible.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from argusfleet.experiments._common import bar_chart_from_frame
from argusfleet.harness.client import MemClawClient, MemClawError
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
class _PropagationPlan:
    tenant_id: str
    fleet_id: str
    foreign_fleet_id: str
    writer_agent: str
    fleet_readers: list[str]
    foreign_readers: list[str]
    redistribute_target: str
    fact_tokens: list[str]
    fact_contents: list[str]
    # Dedicated facts for the write->visible window measurement (distinct
    # prose from the rate-phase facts so they don't dedup-collide).
    window_tokens: list[str]
    window_contents: list[str]


# Distinct operational facts for the window-measurement phase. Kept
# disjoint from ``fact_templates`` so a window fact never near-duplicates
# (and gets 409'd against) a rate-phase fact in the same tenant.
_WINDOW_TEMPLATES = [
    "Annual disaster-recovery exercise concluded; recovery point objective met at {sz} seconds.",
    "Procurement approved {sz} new GPU nodes for the training cluster, delivery in {d} weeks.",
    "Customer NPS survey for the quarter closed at {sz}, up {d} points year-on-year.",
    "Edge cache hit ratio climbed to {sz}% after the {d}-tier eviction policy shipped.",
    "Incident postmortem {r} published; action items assigned across {d} owning teams.",
    "Nightly ETL window shortened to {m} minutes after the join-order rewrite landed.",
    "Finance close for the period finalised; the deck shows {sz}% net revenue retention.",
    "Synthetic monitoring added for {r} new endpoints; baseline p95 recorded at {sz} ms.",
]


def tenant_ids_for(cfg: HarnessConfig) -> list[str]:
    """Tenant this experiment will write to (single source of truth for
    plan-building and ``run all`` cleanup; see leakage.tenant_ids_for)."""
    exp = cfg.experiment or {}
    tenant_id = (
        (cfg.tenant_ids[0] if cfg.tenant_ids else None)
        or exp.get("tenant_id")
        or f"mc_prop_{cfg.seed}"
    )
    return [tenant_id]


def _build_plan(cfg: HarnessConfig, *, nonce: str) -> _PropagationPlan:
    rng = random.Random(cfg.seed)
    exp = cfg.experiment or {}
    n_facts: int = int(exp.get("n_facts", 8))
    n_fleet_readers: int = int(exp.get("n_fleet_readers", 3))
    n_foreign_readers: int = int(exp.get("n_foreign_readers", 2))
    n_window: int = min(int(exp.get("n_window_facts", 8)), len(_WINDOW_TEMPLATES))
    tenant_id: str = tenant_ids_for(cfg)[0]
    fleet_id: str = exp.get("fleet_id") or "fleet_prop_home"
    foreign_fleet_id: str = exp.get("foreign_fleet_id") or "fleet_prop_foreign"

    tokens = [
        f"propcanary-{uuid.UUID(int=rng.getrandbits(128)).hex[:10]}-{nonce}"
        for _ in range(n_facts)
    ]
    # Substantively distinct prose per fact — the server's near-duplicate
    # detector 409s when consecutive writes share >N% of their embedding,
    # which kills propagation tests if every write reads "Propagation
    # canary X". Each template is a self-contained operational fact
    # the embedding model will encode away from its siblings.
    fact_templates = [
        "Database backup completed at 03:{m:02d} UTC, size {sz} GB, retention 30 days.",
        "Quarterly OKR review scheduled for {d} November 2026 at 14:00 UTC in room {r}.",
        "Customer escalation #{r} resolved by on-call engineer; root cause: stale cache eviction.",
        "New CDN POP brought online in São Paulo, latency to Argentina dropped {sz}%.",
        "Security patch CVE-2026-{r} applied across cluster; reboot window {d} November.",
        "Performance regression detected in checkout flow; p99 latency rose to {sz}ms.",
        "Inventory reconciliation finished; {sz} SKUs marked out-of-stock pending vendor sync.",
        "Marketing experiment {r} concluded; control beat treatment by {sz}% on retention.",
        "Kafka lag on topic order-events crossed {sz} messages at {m:02d}:00, paged primary.",
        "TLS certificate for api.example.com expires in {d} days; renewal automated via ACME.",
        "New hire {r} joining platform team on {d} December; provisioned laptop and SSO.",
        "Vendor SLA breach: ticketing API returned 5xx for {sz} minutes during peak.",
        "Capacity plan accepted: add {sz} m6i.2xlarge instances to ingest-fleet by EOQ.",
        "Internal hackathon project #{r} chosen as Q4 ship: real-time anomaly detector.",
        "Database vacuum on warehouse-prod reclaimed {sz} GB; downtime {m} minutes.",
        "Privacy review {r} passed; data lake retention reduced from 90 to {d} days.",
        "Cross-region failover drill at 02:{m:02d} UTC succeeded; RTO measured at {sz}s.",
        "Cost optimisation report finalised: {sz}% reduction on idle dev environments.",
        "Service mesh upgraded to v1.{d}; sidecar restart wave completes Tuesday.",
        "Compliance audit kicked off by external firm {r}; evidence requests due in {d} days.",
        "Feature flag {r} rolled out to {sz}% of traffic; conversion lift {d} bps observed.",
        "Disk usage on logstore-{r} hit {sz}% at {m:02d}:00; alert auto-resolved after rotation.",
        "On-call rotation handoff completed; {r} incidents triaged over the {d}-day shift.",
        "GraphQL schema migration {r} deployed; {sz} resolvers updated, zero breaking changes.",
        "Load test peaked at {sz} RPS on staging; error budget consumed {d}% for the month.",
        "Data pipeline backfill for run {r} finished in {m} minutes; {sz}M rows reprocessed.",
        "Mobile app release {d}.{r} submitted to review; crash-free sessions at {sz}%.",
        "Warehouse query cost down {sz}% after partitioning orders by {d}-day buckets.",
        "Pen-test finding {r} remediated; auth tokens now rotate every {d} hours.",
        "Redis failover triggered at {m:02d}:00; replica promoted in {sz} ms, no data loss.",
        "Localisation for {r} locales shipped; translation coverage reached {sz}%.",
        "Batch billing run processed {sz} invoices; {d} flagged for manual review.",
        "Autoscaler added {sz} pods to checkout-svc during the {m:02d}:00 traffic spike.",
        "Search relevance model {r} promoted to prod; NDCG improved {d} points offline.",
        "Cold-storage tiering moved {sz} TB to glacier; monthly spend cut by {d}%.",
        "Webhook delivery backlog of {sz} events cleared after queue {r} was drained.",
        "Canary deploy of payments-svc v{d} held at {sz}% for {m} minutes, then promoted.",
        "DNS migration to provider {r} completed; propagation finished within {d} hours.",
        "Fraud rule {r} tuned; false-positive rate dropped {sz}% week-over-week.",
        "Schema registry compaction freed {sz} MB; {d} deprecated subjects removed.",
    ]
    fact_contents: list[str] = []
    for i in range(n_facts):
        tpl = fact_templates[i % len(fact_templates)]
        fact_contents.append(
            tpl.format(m=rng.randint(0, 59), sz=rng.randint(50, 999), d=rng.randint(1, 28), r=rng.randint(100, 9999))
        )

    window_tokens = [
        f"propwindow-{uuid.UUID(int=rng.getrandbits(128)).hex[:10]}-{nonce}"
        for _ in range(n_window)
    ]
    window_contents = [
        _WINDOW_TEMPLATES[i].format(
            m=rng.randint(0, 59), sz=rng.randint(50, 999),
            d=rng.randint(1, 28), r=rng.randint(100, 9999),
        )
        for i in range(n_window)
    ]
    return _PropagationPlan(
        tenant_id=tenant_id,
        fleet_id=fleet_id,
        foreign_fleet_id=foreign_fleet_id,
        writer_agent="agent_prop_writer",
        fleet_readers=[f"agent_prop_reader_{i}" for i in range(n_fleet_readers)],
        foreign_readers=[
            f"agent_prop_foreign_{i}" for i in range(n_foreign_readers)
        ],
        redistribute_target="agent_prop_redist_target",
        fact_tokens=tokens,
        fact_contents=fact_contents,
        window_tokens=window_tokens,
        window_contents=window_contents,
    )


# ---------------------------------------------------------------------- #
# Phases                                                                  #
# ---------------------------------------------------------------------- #


async def _seed_facts(
    client: MemClawClient,
    plan: _PropagationPlan,
    sink: JsonlSink,
    *,
    write_mode: str | None,
) -> dict[str, dict[str, Any]]:
    """Write the fleet-scoped facts. Returns {token: write_record}."""
    written: dict[str, dict[str, Any]] = {}
    # Sequential to keep write-order stable for the visibility polling.
    for token, prose in zip(plan.fact_tokens, plan.fact_contents):
        try:
            resp = await client.write_memory(
                tenant_id=plan.tenant_id,
                fleet_id=plan.fleet_id,
                agent_id=plan.writer_agent,
                # Canary is suffixed onto distinct prose so search-by-token
                # still works but the embeddings spread across the space
                # instead of clustering on top of each other.
                content=f"{prose} [canary:{token}]",
                visibility="scope_team",
                metadata={"experiment": "propagation", "canary": token},
                write_mode=write_mode,
            )
            payload = resp.json or {}
            written[token] = {
                "token": token,
                "memory_id": payload.get("id"),
                "writer_agent": plan.writer_agent,
                "write_latency_ms": resp.latency_ms,
                "succeeded": True,
                "trace_id": resp.trace_id,
                "write_ts_perf": time.perf_counter(),
            }
            sink.write("write", {k: v for k, v in written[token].items()
                                  if k != "write_ts_perf"})
        except MemClawError as e:
            written[token] = {
                "token": token,
                "memory_id": None,
                "writer_agent": plan.writer_agent,
                "write_latency_ms": None,
                "succeeded": False,
                "status_code": e.status_code,
                "error": e.message,
                "trace_id": e.trace_id,
                "write_ts_perf": time.perf_counter(),
            }
            sink.write("write", {k: v for k, v in written[token].items()
                                  if k != "write_ts_perf"})
    return written


def _items_match_token(items: list[dict[str, Any]], token: str) -> bool:
    """True iff any returned row carries our canary in metadata."""
    for it in items or []:
        if not isinstance(it, dict):
            continue
        meta = it.get("metadata") or {}
        if meta.get("canary") == token:
            return True
        # Fallback for servers that drop arbitrary metadata: match on content.
        if token in (it.get("content") or ""):
            return True
    return False


async def _measure_window(
    client: MemClawClient,
    plan: _PropagationPlan,
    sink: JsonlSink,
    *,
    write_mode: str | None,
    poll_interval_ms: float,
    timeout_ms: float,
) -> list[dict[str, Any]]:
    """Measure the true write->visible eventual-consistency window.

    This is decoupled from the rate phase on purpose. The rate phase seeds
    every fact, then drains all probes through a concurrency gate, so a
    probe's first ``/search`` lands long after the row is already
    searchable (``poll_count==1``) — its ``t_visible`` is dominated by how
    fast the harness fires probes, not by server lag, and it inflates with
    probe count.

    Here we poll each fact *immediately and continuously* from write
    completion, one fact at a time (sequential), so the poll stream stays
    under the per-tenant ``/search`` budget (100 req/min) and every poll
    lands on schedule. ``poll_count`` is therefore > 1 and ``t_visible_ms``
    reflects server enrichment latency. This isolates the server's
    single-write consistency window from harness scheduling; it does not
    scale with the rate-phase probe count.
    """
    reader = plan.fleet_readers[0] if plan.fleet_readers else plan.writer_agent
    rows: list[dict[str, Any]] = []
    for token, prose in zip(plan.window_tokens, plan.window_contents):
        try:
            await client.write_memory(
                tenant_id=plan.tenant_id,
                fleet_id=plan.fleet_id,
                agent_id=plan.writer_agent,
                content=f"{prose} [canary:{token}]",
                visibility="scope_team",
                metadata={"experiment": "propagation", "canary": token, "phase": "window"},
                write_mode=write_mode,
            )
        except MemClawError as e:
            sink.write("window_probe", {
                "token": token, "reader_agent": reader, "observed_visible": False,
                "poll_count": 0, "t_visible_ms": None, "write_ok": False,
                "last_status_code": e.status_code,
            })
            continue
        write_ts = time.perf_counter()
        deadline = write_ts + timeout_ms / 1000.0
        poll_count = 0
        saw = False
        last_status: int | None = None
        while True:
            poll_count += 1
            try:
                s = await client.search_memory(
                    tenant_id=plan.tenant_id, fleet_ids=[plan.fleet_id],
                    query=prose, top_k=10,
                )
                last_status = s.status_code
                items = (s.json or {}).get("items") if isinstance(s.json, dict) else []
                if _items_match_token(items or [], token):
                    saw = True
                    break
            except MemClawError as e:
                last_status = e.status_code
            if time.perf_counter() >= deadline:
                break
            await asyncio.sleep(poll_interval_ms / 1000.0)
        row = {
            "token": token,
            "reader_agent": reader,
            "observed_visible": saw,
            "poll_count": poll_count,
            "t_visible_ms": (time.perf_counter() - write_ts) * 1000.0 if saw else None,
            "write_ok": True,
            "last_status_code": last_status,
        }
        sink.write("window_probe", row)
        rows.append(row)
    return rows


async def _poll_visibility(
    client: MemClawClient,
    *,
    plan: _PropagationPlan,
    reader_agent: str,
    reader_fleet: str,
    reader_relation: str,  # "fleet_sibling" | "cross_fleet"
    token: str,
    query: str,
    write_record: dict[str, Any],
    sink: JsonlSink,
    timeout_ms: float,
    poll_interval_ms: float,
    rate_gate: asyncio.Semaphore,
) -> dict[str, Any]:
    """Poll search until the canary shows up or the timeout expires.

    Cross-fleet readers do a *single* attempt (no polling): cross-fleet
    isolation is a query-time ACL decision, not an eventual-consistency
    race, so one search after the write phase has completed is sufficient
    and any hit is a leak. Fleet siblings poll until the fact surfaces or
    ``timeout_ms`` elapses, since their visibility genuinely is gated by
    asynchronous enrichment lag.
    """
    single_attempt = reader_relation == "cross_fleet"
    deadline = time.perf_counter() + (timeout_ms / 1000.0)
    poll_count = 0
    saw = False
    last_status: int | None = None
    last_trace: str | None = None
    while True:
        poll_count += 1
        try:
            # We query by the prose content (not the canary token).
            # The token alone is a gibberish embedding-wise — old
            # uniform-content rows from prior runs end up beating the
            # current run's prose-heavy writes in semantic ranking.
            # Matching on metadata.canary in the returned items
            # decouples retrieval (driven by content similarity) from
            # the visibility decision (driven by which canary appears).
            #
            # filter_agent_id intentionally omitted — see leakage.py for
            # the rationale.
            #
            # Hold the rate gate only around the HTTP search, not across the
            # inter-poll sleep below: otherwise a probe parked in
            # ``asyncio.sleep`` would occupy an in-flight slot while issuing
            # no request, starving sibling probes and inflating the measured
            # ``t_visible_ms``. (Mirrors ``_WriteRateLimiter``'s wait-after-
            # release design.)
            async with rate_gate:
                resp = await client.search_memory(
                    tenant_id=plan.tenant_id,
                    fleet_ids=[reader_fleet],
                    query=query,
                    top_k=10,
                )
            last_status = resp.status_code
            last_trace = resp.trace_id
            items = (resp.json or {}).get("items") if isinstance(resp.json, dict) else []
            if _items_match_token(items or [], token):
                saw = True
                break
        except MemClawError as e:
            last_status = e.status_code
            last_trace = e.trace_id
        if single_attempt or time.perf_counter() >= deadline:
            break
        await asyncio.sleep(poll_interval_ms / 1000.0)

    t_visible_ms = None
    if saw and write_record.get("write_ts_perf") is not None:
        t_visible_ms = (time.perf_counter() - write_record["write_ts_perf"]) * 1000.0

    expected_visible = reader_relation == "fleet_sibling"
    row = {
        "token": token,
        "memory_id": write_record.get("memory_id"),
        "reader_agent": reader_agent,
        "reader_fleet": reader_fleet,
        "reader_relation": reader_relation,
        "expected_visible": expected_visible,
        "observed_visible": saw,
        "is_leak": (not expected_visible) and saw,
        "is_missed_propagation": expected_visible and not saw,
        "poll_count": poll_count,
        "t_visible_ms": t_visible_ms,
        "last_status_code": last_status,
        "last_trace_id": last_trace,
    }
    sink.write("visibility_probe", row)
    return row


async def _redistribute_and_verify(
    client: MemClawClient,
    *,
    plan: _PropagationPlan,
    write_record: dict[str, Any],
    sink: JsonlSink,
) -> dict[str, Any]:
    """Re-home one memory onto the target agent and verify the swap."""
    memory_id = write_record.get("memory_id")
    if not memory_id:
        row = {
            "token": write_record["token"],
            "memory_id": None,
            "redistribute_ok": False,
            "owner_after": None,
            "ownership_transferred": False,
            "error": "no_memory_id",
        }
        sink.write("redistribute", row)
        return row

    try:
        resp = await client.propagate_memory(
            memory_id=memory_id,
            target_agent_id=plan.redistribute_target,
            tenant_id=plan.tenant_id,
            # The redistribute endpoint requires the requester to be
            # trust_level >= 3. Harness-fabricated agents are at trust
            # level 1 after auto-creation, so this will 403 on a typical
            # tenant — the harness records that and the summary reports
            # ownership_transfer_skipped instead of a false "0% transfer".
            requesting_agent_id=plan.writer_agent,
        )
        redistribute_ok = True
        redistribute_status = resp.status_code
        redistribute_trace = resp.trace_id
        redistribute_latency = resp.latency_ms
    except MemClawError as e:
        redistribute_ok = False
        redistribute_status = e.status_code
        redistribute_trace = e.trace_id
        redistribute_latency = None

    owner_after: str | None = None
    try:
        get_resp = await client.retrieve_memory(memory_id, tenant_id=plan.tenant_id)
        body = get_resp.json or {}
        owner_after = body.get("agent_id") if isinstance(body, dict) else None
    except MemClawError:
        owner_after = None

    transferred = owner_after == plan.redistribute_target
    row = {
        "token": write_record["token"],
        "memory_id": memory_id,
        "redistribute_ok": redistribute_ok,
        "redistribute_status": redistribute_status,
        "redistribute_latency_ms": redistribute_latency,
        "redistribute_trace": redistribute_trace,
        "writer_agent": write_record.get("writer_agent"),
        "target_agent": plan.redistribute_target,
        "owner_after": owner_after,
        "ownership_transferred": transferred,
    }
    sink.write("redistribute", row)
    return row


# ---------------------------------------------------------------------- #
# Runner                                                                  #
# ---------------------------------------------------------------------- #


async def run(
    cfg: HarnessConfig,
    *,
    client: MemClawClient | None = None,
) -> dict[str, Any]:
    run_id, run_dir, events_path = make_run_dir(cfg, "propagation")
    plan = _build_plan(cfg, nonce=run_id)

    exp = cfg.experiment or {}
    visibility_timeout_ms: float = float(exp.get("visibility_timeout_ms", 5000))
    poll_interval_ms: float = float(exp.get("poll_interval_ms", 250))
    max_in_flight: int = int(exp.get("max_in_flight", 8))
    redistribute_fraction: float = float(exp.get("redistribute_fraction", 0.5))
    # ``strong`` forces the server to finish enrichment before responding,
    # so visibility polling isn't racing against an async embedding job.
    write_mode: str | None = exp.get("write_mode", "strong")
    # Window phase: a small set of facts polled tightly + sequentially to
    # measure true write->visible lag under the /search budget.
    window_poll_interval_ms: float = float(exp.get("window_poll_interval_ms", 1000))
    window_timeout_ms: float = float(exp.get("window_timeout_ms", 120000))

    rate_gate = asyncio.Semaphore(max_in_flight)

    logger.info(
        "propagation run %s — n_facts=%d fleet_readers=%d foreign_readers=%d timeout=%.0fms",
        run_id,
        len(plan.fact_tokens),
        len(plan.fleet_readers),
        len(plan.foreign_readers),
        visibility_timeout_ms,
    )

    async def _execute(c: MemClawClient) -> None:
        with open_sink_with_envelope(
            events_path,
            run_id=run_id,
            experiment="propagation",
            cfg=cfg,
            extra={
                "n_facts": len(plan.fact_tokens),
                "fleet_id": plan.fleet_id,
                "foreign_fleet_id": plan.foreign_fleet_id,
                "writer_agent": plan.writer_agent,
                "fleet_readers": plan.fleet_readers,
                "foreign_readers": plan.foreign_readers,
                "redistribute_target": plan.redistribute_target,
                "visibility_timeout_ms": visibility_timeout_ms,
                "poll_interval_ms": poll_interval_ms,
                "redistribute_fraction": redistribute_fraction,
                "write_mode": write_mode,
                "n_window_facts": len(plan.window_tokens),
                "window_poll_interval_ms": window_poll_interval_ms,
                "window_timeout_ms": window_timeout_ms,
            },
        ) as sink:
            # Phase 1 — tight per-fact window measurement (faithful lag).
            await _measure_window(
                c, plan, sink,
                write_mode=write_mode,
                poll_interval_ms=window_poll_interval_ms,
                timeout_ms=window_timeout_ms,
            )

            # Phase 2 — batch writes + drained probes for visibility/leak
            # *rates* (robust to late looking; not used for latency).
            writes = await _seed_facts(c, plan, sink, write_mode=write_mode)

            # Map token -> prose so the probe can search by content.
            prose_by_token = dict(zip(plan.fact_tokens, plan.fact_contents))

            async def _do_probe(
                token: str, reader_agent: str, reader_fleet: str, relation: str
            ) -> None:
                await _poll_visibility(
                    c,
                    plan=plan,
                    reader_agent=reader_agent,
                    reader_fleet=reader_fleet,
                    reader_relation=relation,
                    token=token,
                    query=prose_by_token.get(token, token),
                    write_record=writes.get(token, {}),
                    sink=sink,
                    timeout_ms=visibility_timeout_ms,
                    poll_interval_ms=poll_interval_ms,
                    rate_gate=rate_gate,
                )

            probe_coros: list[asyncio.Task] = []
            for token, rec in writes.items():
                if not rec.get("succeeded"):
                    continue
                for r in plan.fleet_readers:
                    probe_coros.append(
                        asyncio.create_task(
                            _do_probe(token, r, plan.fleet_id, "fleet_sibling")
                        )
                    )
                for r in plan.foreign_readers:
                    probe_coros.append(
                        asyncio.create_task(
                            _do_probe(token, r, plan.foreign_fleet_id, "cross_fleet")
                        )
                    )
            if probe_coros:
                await asyncio.gather(*probe_coros)

            # Redistribute a deterministic subset, picked by hash on token
            # so it's reproducible across runs with the same seed.
            rng = random.Random(cfg.seed)
            succeeded_tokens = [t for t, rec in writes.items() if rec.get("succeeded")]
            n_redist = int(len(succeeded_tokens) * redistribute_fraction)
            chosen = set(rng.sample(succeeded_tokens, k=min(n_redist, len(succeeded_tokens))))
            if chosen:
                await asyncio.gather(
                    *(
                        _redistribute_and_verify(
                            c, plan=plan, write_record=writes[t], sink=sink
                        )
                        for t in chosen
                    )
                )

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
    probes = events_to_frame(events, "visibility_probe")
    window = events_to_frame(events, "window_probe")
    redistributes = events_to_frame(events, "redistribute")
    writes = events_to_frame(events, "write")

    propagation_csv = run_dir / "propagation.csv"
    summary_csv = run_dir / "summary.csv"
    visibility_plot = run_dir / "visibility_by_relation.png"
    latency_plot = run_dir / "time_to_visibility.png"

    if not probes.empty:
        write_csv(probes, propagation_csv)
        by_rel = (
            probes.groupby("reader_relation")
            .agg(
                n=("observed_visible", "size"),
                visible=("observed_visible", "sum"),
                leaks=("is_leak", "sum"),
                missed=("is_missed_propagation", "sum"),
            )
            .reset_index()
        )
        by_rel["visibility_rate"] = by_rel["visible"] / by_rel["n"].clip(lower=1)
        _ci = [wilson_ci(int(r.visible), int(r.n)) for r in by_rel.itertuples()]
        vis_yerr = wilson_yerr(by_rel["visibility_rate"].tolist(), _ci)
        bar_chart_from_frame(
            by_rel,
            x="reader_relation",
            y="visibility_rate",
            label_fmt=lambda r: f"{int(r['visible'])}/{int(r['n'])}",
            title="Visibility rate by reader relation",
            ylabel="Visibility rate",
            ymax=1.05,
            out_path=visibility_plot,
            yerr=vis_yerr,
        )

    # Time-to-visibility comes from the dedicated window phase (tight,
    # sequential per-fact polling), NOT the rate-phase probes — the latter
    # measure first-look latency, which is dominated by probe scheduling.
    window_seen = pd.DataFrame()
    if not window.empty and "t_visible_ms" in window.columns:
        window_seen = window[window["observed_visible"].fillna(False).astype(bool)].dropna(
            subset=["t_visible_ms"]
        )
        if not window_seen.empty:
            plot_latency_hist(
                window_seen,
                col="t_visible_ms",
                out_path=latency_plot,
                title="Write-to-visibility window (tight per-fact polling)",
            )

    n_probes = int(len(probes))
    n_fleet = int((probes["reader_relation"] == "fleet_sibling").sum()) if not probes.empty else 0
    n_foreign = int((probes["reader_relation"] == "cross_fleet").sum()) if not probes.empty else 0
    n_fleet_visible = (
        int(probes[probes["reader_relation"] == "fleet_sibling"]["observed_visible"].sum())
        if n_fleet
        else 0
    )
    n_leaks = int(probes["is_leak"].sum()) if "is_leak" in probes.columns else 0
    n_missed = int(probes["is_missed_propagation"].sum()) if "is_missed_propagation" in probes.columns else 0

    fleet_visibility_rate = n_fleet_visible / n_fleet if n_fleet else 0.0
    leak_rate = n_leaks / n_foreign if n_foreign else 0.0

    summary_rows: list[dict[str, Any]] = [
        {"metric": "n_writes", "value": int(len(writes))},
        {"metric": "n_visibility_probes", "value": n_probes},
        {"metric": "n_fleet_probes", "value": n_fleet},
        {"metric": "n_foreign_probes", "value": n_foreign},
        {"metric": "fleet_visibility_rate", "value": fleet_visibility_rate},
        {"metric": "leak_rate", "value": leak_rate},
        {"metric": "n_missed_propagations", "value": n_missed},
    ]

    # Faithful write->visible window percentiles from the window phase.
    if not window_seen.empty:
        for k, v in summarize_latency(window_seen, "t_visible_ms").items():
            summary_rows.append({"metric": f"t_visible_{k}", "value": v})
        summary_rows.append({"metric": "window_n", "value": int(len(window))})
        summary_rows.append({"metric": "window_visible", "value": int(len(window_seen))})
        summary_rows.append(
            {"metric": "window_poll_count_p50", "value": float(window_seen["poll_count"].median())}
        )

    transfer_rate = 0.0
    redistribute_count = 0
    transfer_skipped = 0
    if not redistributes.empty and "ownership_transferred" in redistributes.columns:
        redistribute_count = int(len(redistributes))
        # Rows where the redistribute call itself didn't succeed — a 403
        # from the trust gate (requester < trust_level 3) or a missing
        # memory_id — could not transfer ownership and are NOT transfer
        # failures. Exclude them from the denominator and report them
        # separately as ``ownership_transfer_skipped``, so an all-403 run
        # (the typical trust-level-1 tenant) surfaces the skip instead of
        # collapsing to a misleading 0% ownership_transfer_rate.
        if "redistribute_ok" in redistributes.columns:
            attempted = redistributes[redistributes["redistribute_ok"].fillna(False).astype(bool)]
        else:
            attempted = redistributes
        n_attempted = int(len(attempted))
        transfer_skipped = redistribute_count - n_attempted
        transferred = int(attempted["ownership_transferred"].sum())
        transfer_rate = transferred / n_attempted if n_attempted else 0.0
        summary_rows.append({"metric": "n_redistributes", "value": redistribute_count})
        summary_rows.append({"metric": "n_redistributes_attempted", "value": n_attempted})
        summary_rows.append({"metric": "ownership_transfer_skipped", "value": transfer_skipped})
        summary_rows.append({"metric": "ownership_transfer_rate", "value": transfer_rate})

    write_csv(pd.DataFrame(summary_rows), summary_csv)

    return {
        "run_dir": str(run_dir),
        "events": str(events_path),
        "propagation_csv": str(propagation_csv),
        "summary_csv": str(summary_csv),
        "visibility_plot": str(visibility_plot),
        "latency_plot": str(latency_plot),
        "n_probes": n_probes,
        "fleet_visibility_rate": fleet_visibility_rate,
        "leak_rate": leak_rate,
        "n_missed_propagations": n_missed,
        "n_redistributes": redistribute_count,
        "ownership_transfer_skipped": transfer_skipped,
        "ownership_transfer_rate": transfer_rate,
    }

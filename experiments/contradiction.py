"""Temporal contradiction experiment.

What the experiment measures
----------------------------
When two memories assert contradictory facts about the same subject —
written by different agents at staggered or concurrent times — a
correct memory service must:

1. **Detect** the conflict on the second write (the response should
   declare the second memory as superseding the first).
2. **Resolve** it durably (the older row's status flips to a
   non-``active`` value and the newer row carries ``supersedes_id``
   pointing back).
3. **Prevent stale reads** (a fresh ``status_filter="active"`` search
   for the subject must not return the older fact).

The experiment runs two scenarios per fact:

- ``sequential`` — agent A writes value_1, the harness waits a short
  pause, then agent B writes the contradictory value_2.
- ``concurrent`` — both writes are dispatched in the same ``gather``
  batch with no inter-write delay. Tests race-handling.

Outputs
-------
- ``events.jsonl`` — writes (with supersedes_id / superseded_by captured),
  search probes, and resolution checks.
- ``contradictions.csv`` — one row per (fact, scenario) pair.
- ``summary.csv`` — detection rate, stale-read rate, write-latency
  percentiles split by ``had_contradiction``.
- ``detection_rate.png`` — bar chart, sequential vs concurrent.
- ``write_latency.png`` — write-latency histogram, contradiction vs not.
"""

from __future__ import annotations

import asyncio
import logging
import random
import uuid
from dataclasses import dataclass
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
class _ContradictionFact:
    """One subject with two contradictory values.

    Both writes target the same fleet so the contradiction detector can
    even consider them — cross-fleet writes would be a leakage scenario,
    not a contradiction scenario.

    ``predicate`` + ``value_a``/``value_b`` are written as RDF triple
    fields (``subject_entity_id``, ``predicate``, ``object_value``).
    Sharing ``subject_entity_id`` + ``predicate`` across two writes with
    different ``object_value`` is the deterministic signal the server's
    structural contradiction detector looks for — and it sidesteps the
    embedding-similarity gate that otherwise 409s the second write.
    """

    fact_id: str
    tenant_id: str
    fleet_id: str
    agent_a: str
    agent_b: str
    subject: str  # human-readable, used as entity canonical_name
    predicate: str
    value_a: str
    value_b: str
    # Pre-rendered, lexically *distinct* prose for the two writes. The two
    # contradictory writes must not be near-duplicates of each other, or
    # the synchronous embedding-dedup gate 409s the second before the
    # (async, post-commit) contradiction detector ever sees it. Distinct
    # phrasing per value keeps them past the gate.
    prose_a: str
    prose_b: str
    scenario: str  # "sequential" | "concurrent"


def tenant_ids_for(cfg: HarnessConfig) -> list[str]:
    """Tenant this experiment will write to (single source of truth for
    fact-building and ``run all`` cleanup; see leakage.tenant_ids_for)."""
    exp = cfg.experiment or {}
    tenant_id = (
        (cfg.tenant_ids[0] if cfg.tenant_ids else None)
        or exp.get("tenant_id")
        or f"mc_ctr_{cfg.seed}"
    )
    return [tenant_id]


def _build_facts(cfg: HarnessConfig, *, nonce: str) -> list[_ContradictionFact]:
    rng = random.Random(cfg.seed)
    exp = cfg.experiment or {}
    n_facts: int = int(exp.get("n_facts", 8))
    scenarios: list[str] = list(exp.get("scenarios", ["sequential", "concurrent"]))
    tenant_id: str = tenant_ids_for(cfg)[0]
    fleet_id: str = exp.get("fleet_id") or "fleet_ctr"

    # Subject templates are paired with the RDF ``predicate`` the
    # contradiction is expressed over. The predicate MUST be one of the
    # server's ``SINGLE_VALUE_PREDICATES`` — the structural contradiction
    # detector only runs Path 1 (RDF conflict) for those; an arbitrary
    # predicate (e.g. the former ``deployment_region``) is silently
    # skipped, which is one reason earlier runs reported zero detections.
    # Two writes for a fact share ``(subject_entity_id, predicate)`` but
    # assert different ``object_value``.
    templates: list[tuple[str, str, list[str]]] = [
        ("billing-service-{n}", "region", ["us-west-2", "eu-central-1", "ap-south-1"]),
        ("analytics-job-{n}", "runs_on", ["postgres", "mysql", "cockroachdb"]),
        ("ingest-team-{n}", "led_by", ["alice", "bob", "carol"]),
        ("incident-{n}", "status", ["sev1", "sev2", "sev3"]),
        ("data-pipeline-{n}", "located_in", ["eu", "us", "apac"]),
        ("edge-cache-{n}", "hosted_on", ["cloudflare", "fastly", "akamai"]),
        ("auth-cluster-{n}", "type", ["auth0", "okta", "cognito"]),
        ("payment-flow-{n}", "category", ["stripe", "adyen", "braintree"]),
        ("gateway-{n}", "runs_on", ["nginx", "traefik", "haproxy"]),
        ("event-bus-{n}", "type", ["kafka", "rabbitmq", "pulsar"]),
        ("worker-pool-{n}", "phase", ["bootstrapping", "steady", "draining"]),
        ("telemetry-{n}", "owned_by", ["sre-team", "platform-team", "data-team"]),
        ("engineering-seat-{n}", "role", ["enterprise", "team", "individual"]),
        ("release-{n}", "state", ["canary", "stable", "rolledback"]),
        ("log-store-{n}", "stored_in", ["s3", "gcs", "azureblob"]),
        ("stateful-set-{n}", "located_in", ["us-east-1a", "us-east-1b", "us-east-1c"]),
        ("product-catalog-{n}", "owned_by", ["pim-team", "erp-team", "ldap-team"]),
        ("dispatcher-{n}", "type", ["fifo", "priority", "fair"]),
        ("secret-store-{n}", "type", ["vault", "kms", "doppler"]),
        ("api-service-{n}", "based_in", ["us", "eu", "apac"]),
    ]

    # Lexically diverse sentence frames. For each fact we render value_a
    # and value_b with two *different* frames so the two contradictory
    # writes are far apart in embedding space (else dedup 409s the
    # second). ``{pred}`` renders with underscores stripped for prose.
    frames: list[str] = [
        "As of the latest rollout, the {pred} of {subj} is {val}.",
        "Records show that {subj} now has its {pred} set to {val}.",
        "Following a recent migration, {subj} was reconfigured and its {pred} is {val}.",
        "The operations log lists {val} as the current {pred} for {subj}.",
        "After review the team confirmed {subj}'s {pred} to be {val}.",
        "Per the service catalogue, {subj} carries a {pred} of {val}.",
        "An audit recorded that the {pred} assigned to {subj} is {val}.",
        "Configuration management pins the {pred} of {subj} at {val}.",
    ]

    def _prose(frame: str, subject: str, predicate: str, value: str) -> str:
        return frame.format(pred=predicate.replace("_", " "), subj=subject, val=value)

    facts: list[_ContradictionFact] = []
    for i in range(n_facts):
        template_subject, predicate, choices = templates[i % len(templates)]
        # Embed the run nonce into the subject — keeps the two values
        # within a fact contradictory (same subject, different values)
        # while making each *run's* entity name unique so a re-run gets
        # fresh entities instead of replaying yesterday's state.
        subject = template_subject.format(n=i) + f"-{nonce[-12:]}"
        # Always pick two distinct values so the writes are genuinely
        # contradictory; ``random.sample`` enforces uniqueness.
        v_a, v_b = rng.sample(choices, 2)
        # Two distinct frames per fact so the writes aren't near-duplicates.
        frame_a, frame_b = rng.sample(frames, 2)
        for sc in scenarios:
            facts.append(
                _ContradictionFact(
                    fact_id=f"{sc}-{i:03d}-{uuid.UUID(int=rng.getrandbits(128)).hex[:8]}",
                    tenant_id=tenant_id,
                    fleet_id=fleet_id,
                    agent_a=f"agent_ctr_a_{i % 4}",
                    agent_b=f"agent_ctr_b_{i % 4}",
                    subject=subject,
                    predicate=predicate,
                    value_a=v_a,
                    value_b=v_b,
                    prose_a=_prose(frame_a, subject, predicate, v_a),
                    prose_b=_prose(frame_b, subject, predicate, v_b),
                    scenario=sc,
                )
            )
    rng.shuffle(facts)
    return facts


# ---------------------------------------------------------------------- #
# Per-fact execution                                                      #
# ---------------------------------------------------------------------- #


def _write_kwargs(
    *,
    fact: _ContradictionFact,
    agent_id: str,
    value: str,
    content: str,
    subject_entity_id: str,
    write_mode: str | None,
) -> dict[str, Any]:
    return {
        "tenant_id": fact.tenant_id,
        "fleet_id": fact.fleet_id,
        "agent_id": agent_id,
        # Carry both an RDF triple (the deterministic signal) AND a
        # lexically-distinct natural-language version (so the second write
        # clears the embedding-dedup gate and the row stays searchable).
        "content": content,
        "subject_entity_id": subject_entity_id,
        "predicate": fact.predicate,
        "object_value": value,
        "metadata": {
            "experiment": "contradiction",
            "fact_id": fact.fact_id,
            "subject": fact.subject,
            "predicate": fact.predicate,
            "asserted_value": value,
            "scenario": fact.scenario,
        },
        "write_mode": write_mode,
    }


async def _do_write(
    client: MemoryBackend,
    *,
    agent_id: str,
    fact: _ContradictionFact,
    value: str,
    content: str,
    subject_entity_id: str,
    write_mode: str | None,
    sink: JsonlSink,
    sequence: int,
) -> dict[str, Any]:
    """Write a single contradictory value; record the response shape."""
    kwargs = _write_kwargs(
        fact=fact,
        agent_id=agent_id,
        value=value,
        content=content,
        subject_entity_id=subject_entity_id,
        write_mode=write_mode,
    )
    try:
        resp = await client.write_memory(**kwargs)
        payload = resp.json or {}
        contradictions = payload.get("superseded_by") or []
        result = {
            "fact_id": fact.fact_id,
            "scenario": fact.scenario,
            "sequence": sequence,
            "writer_agent": agent_id,
            "value": value,
            "memory_id": payload.get("id"),
            "supersedes_id": payload.get("supersedes_id"),
            "had_contradiction": bool(
                payload.get("supersedes_id") or contradictions
            ),
            "n_contradictions_reported": len(contradictions),
            "contradictions": contradictions,
            # Snapshot of the stored content at write time. Backends
            # without structural detection (mem0) resolve conflicts by
            # mutating/deleting rows, so the post-settle adjudication
            # compares against this snapshot.
            "content_at_write": payload.get("content"),
            "materialize_ms": resp.extra.get("materialize_ms"),
            "latency_ms": resp.latency_ms,
            "status_code": resp.status_code,
            "trace_id": resp.trace_id,
        }
        sink.write("write", result)
        return result
    except BackendError as e:
        result = {
            "fact_id": fact.fact_id,
            "scenario": fact.scenario,
            "sequence": sequence,
            "writer_agent": agent_id,
            "value": value,
            "memory_id": None,
            "supersedes_id": None,
            "had_contradiction": False,
            "n_contradictions_reported": 0,
            "contradictions": [],
            "latency_ms": None,
            "status_code": e.status_code,
            "trace_id": e.trace_id,
            "error": e.message,
        }
        sink.write("write", result)
        return result


async def _run_fact(
    client: MemoryBackend,
    fact: _ContradictionFact,
    sink: JsonlSink,
    *,
    sequential_delay_ms: float,
    settle_delay_ms: float,
    write_mode: str | None,
    rate_gate: asyncio.Semaphore,
) -> dict[str, Any]:
    """Write both contradictory values, then verify resolution."""
    caps = client.capabilities
    async with rate_gate:
        # Upsert the subject entity first — both contradictory writes
        # point at this UUID via ``subject_entity_id``, which is what
        # routes the second write into the structural-contradiction
        # codepath instead of the embedding-similarity dedup gate.
        # Backends without an entity primitive (capabilities.entities is
        # False, e.g. mem0) skip the upsert: their conflict handling is
        # content-driven, so the subject string itself is the reference.
        entity_id: str | None = None
        if caps.entities:
            try:
                ent_resp = await client.upsert_entity(
                    tenant_id=fact.tenant_id,
                    fleet_id=fact.fleet_id,
                    entity_type="system_resource",
                    canonical_name=fact.subject,
                    attributes={"experiment": "contradiction", "fact_id": fact.fact_id},
                )
                entity_id = (ent_resp.json or {}).get("id") if isinstance(ent_resp.json, dict) else None
                sink.write(
                    "entity_upsert",
                    {
                        "fact_id": fact.fact_id,
                        "subject": fact.subject,
                        "entity_id": entity_id,
                        "latency_ms": ent_resp.latency_ms,
                        "trace_id": ent_resp.trace_id,
                    },
                )
            except BackendError as e:
                sink.write(
                    "entity_upsert",
                    {
                        "fact_id": fact.fact_id,
                        "subject": fact.subject,
                        "entity_id": None,
                        "error": e.message,
                        "status_code": e.status_code,
                        "trace_id": e.trace_id,
                    },
                )
                entity_id = None

        if caps.entities and entity_id is None:
            # Without an entity UUID we can't trigger the structural
            # detector; record a placeholder resolution so the summary
            # still has a row for this fact-run and the cause is visible.
            row = {
                "fact_id": fact.fact_id,
                "scenario": fact.scenario,
                "subject": fact.subject,
                "predicate": fact.predicate,
                "value_a": fact.value_a,
                "value_b": fact.value_b,
                "detected": False,
                "n_active_after_writes": 0,
                "active_values": [],
                "stale_read": False,
                "probe_latency_ms": None,
                "probe_error": "entity_upsert_failed",
                "first_write_latency_ms": None,
                "second_write_latency_ms": None,
            }
            sink.write("resolution", row)
            return row

        # What the writes reference as their subject: the entity UUID when
        # the backend has entities, else the subject string (recorded in
        # write metadata for the artifact trail).
        subject_ref = entity_id if caps.entities else fact.subject

        if fact.scenario == "sequential":
            first = await _do_write(
                client,
                agent_id=fact.agent_a,
                fact=fact,
                value=fact.value_a,
                content=fact.prose_a,
                subject_entity_id=subject_ref,
                write_mode=write_mode,
                sink=sink,
                sequence=0,
            )
            await asyncio.sleep(sequential_delay_ms / 1000.0)
            second = await _do_write(
                client,
                agent_id=fact.agent_b,
                fact=fact,
                value=fact.value_b,
                content=fact.prose_b,
                subject_entity_id=subject_ref,
                write_mode=write_mode,
                sink=sink,
                sequence=1,
            )
        else:  # concurrent
            first, second = await asyncio.gather(
                _do_write(
                    client,
                    agent_id=fact.agent_a,
                    fact=fact,
                    value=fact.value_a,
                    content=fact.prose_a,
                    subject_entity_id=subject_ref,
                    write_mode=write_mode,
                    sink=sink,
                    sequence=0,
                ),
                _do_write(
                    client,
                    agent_id=fact.agent_b,
                    fact=fact,
                    value=fact.value_b,
                    content=fact.prose_b,
                    subject_entity_id=subject_ref,
                    write_mode=write_mode,
                    sink=sink,
                    sequence=1,
                ),
            )

    # Let the async pipeline settle before adjudicating: on structural
    # backends this lets supersession indexing land before the ``active``
    # re-fetch; on content-driven backends it lets the LLM ADD/UPDATE/
    # DELETE pipeline finish before the metadata enumeration.
    if settle_delay_ms > 0:
        await asyncio.sleep(settle_delay_ms / 1000.0)

    first_supersedes = second_supersedes = None
    first_status = second_status = None
    probe_latency = None
    probe_error = None

    if caps.structural_contradiction:
        # --- Structural backends (MemClaw) ---------------------------
        # Contradiction detection is ASYNC/post-commit: ``supersedes_id``
        # is never in the write response and must be read back by GET-by-id
        # after the settle window. This re-fetch is the authoritative
        # detection signal (independent of search enrichment).
        _RESOLVED_STATUSES = {"outdated", "conflicted", "superseded"}

        async def _refetch(mid: str | None) -> dict[str, Any]:
            if not mid:
                return {}
            try:
                r = await client.retrieve_memory(mid, tenant_id=fact.tenant_id)
                return r.json if isinstance(r.json, dict) else {}
            except BackendError:
                return {}

        rf_first = await _refetch(first.get("memory_id"))
        rf_second = await _refetch(second.get("memory_id"))
        first_status = rf_first.get("status")
        second_status = rf_second.get("status")
        first_supersedes = rf_first.get("supersedes_id")
        second_supersedes = rf_second.get("supersedes_id")
        detected = bool(first_supersedes or second_supersedes) or (
            first_status in _RESOLVED_STATUSES or second_status in _RESOLVED_STATUSES
        )

        # Stale-read check: a fresh ``active``-filtered search for the
        # subject should return at most one row carrying the fact_id.
        try:
            probe = await client.search_memory(
                tenant_id=fact.tenant_id,
                fleet_ids=[fact.fleet_id],
                query=fact.subject,
                top_k=10,
                status_filter="active",
            )
            items = (probe.json or {}).get("items") if isinstance(probe.json, dict) else []
            active_values = [
                it.get("metadata", {}).get("asserted_value")
                for it in (items or [])
                if isinstance(it, dict)
                and it.get("metadata", {}).get("fact_id") == fact.fact_id
            ]
            probe_latency = probe.latency_ms
        except BackendError as e:
            active_values = []
            probe_error = e.message
    else:
        # --- Content-driven backends (mem0) --------------------------
        # No supersession edge, no lifecycle status, and marker-based
        # re-fetch is unreliable under the LLM ADD/UPDATE/DELETE pipeline.
        # Enumerate every row carrying this fact via an exact
        # metadata-filtered get-all (no embedding ranking, one retrieval
        # call) and read the outcome off the surviving object values.
        # ``resolved`` (detected) = the platform collapsed the pair to a
        # single asserted value (it superseded or merged); ``persists``
        # (stale) = both contradictory values remain simultaneously
        # retrievable, an Inv-Time exposure.
        try:
            listing = await client.list_memories(
                tenant_id=fact.tenant_id,
                metadata_filter={"fact_id": fact.fact_id},
            )
            items = (listing.json or {}).get("items") if isinstance(listing.json, dict) else []
            active_values = [
                it.get("metadata", {}).get("asserted_value")
                for it in (items or [])
                if isinstance(it, dict)
                and it.get("metadata", {}).get("fact_id") == fact.fact_id
            ]
            probe_latency = listing.latency_ms
        except BackendError as e:
            active_values = []
            probe_error = e.message
        distinct_values = {v for v in active_values if v is not None}
        # ``resolved`` (detected) = the fact settles to a single coherent
        # object value; ``persists`` (stale) = two or more contradictory
        # values remain simultaneously retrievable (an Inv-Time exposure).
        # Caveat, noted in the paper: a store that resolves by DELETING
        # the loser is indistinguishable here from one where the second
        # write was never admitted — both leave one value. We therefore
        # lead with the persistence rate (unambiguous) and read detection
        # as its complement over fact-runs that produced any row.
        detected = len(distinct_values) == 1

    n_active = len(active_values)
    if caps.structural_contradiction:
        stale_read = n_active > 1
    else:
        # Both contradictory values simultaneously present.
        stale_read = len({v for v in active_values if v is not None}) > 1

    row = {
        "fact_id": fact.fact_id,
        "scenario": fact.scenario,
        "subject": fact.subject,
        "predicate": fact.predicate,
        "entity_id": entity_id,
        "value_a": fact.value_a,
        "value_b": fact.value_b,
        "first_memory_id": first.get("memory_id"),
        "second_memory_id": second.get("memory_id"),
        "second_supersedes_id": second_supersedes,
        "first_supersedes_id": first_supersedes,
        "first_status_after": first_status,
        "second_status_after": second_status,
        "detected": detected,
        "n_active_after_writes": n_active,
        "active_values": active_values,
        "stale_read": stale_read,
        "probe_latency_ms": probe_latency,
        "probe_error": probe_error,
        "first_write_latency_ms": first.get("latency_ms"),
        "second_write_latency_ms": second.get("latency_ms"),
    }
    sink.write("resolution", row)
    return row


# ---------------------------------------------------------------------- #
# Runner                                                                  #
# ---------------------------------------------------------------------- #


async def run(
    cfg: HarnessConfig,
    *,
    client: MemoryBackend | None = None,
) -> dict[str, Any]:
    """Execute the contradiction experiment end-to-end."""
    run_id, run_dir, events_path = make_run_dir(cfg, "contradiction")
    facts = _build_facts(cfg, nonce=run_id)

    exp = cfg.experiment or {}
    sequential_delay_ms: float = float(exp.get("sequential_delay_ms", 250.0))
    settle_delay_ms: float = float(exp.get("settle_delay_ms", 0.0))
    write_mode: str | None = exp.get("write_mode", "strong")
    rate_gate = asyncio.Semaphore(int(exp.get("max_in_flight", 4)))

    logger.info(
        "contradiction run %s — facts=%d sequential_delay=%.0fms write_mode=%s",
        run_id,
        len(facts),
        sequential_delay_ms,
        write_mode,
    )

    async def _execute(c: MemoryBackend) -> None:
        with open_sink_with_envelope(
            events_path,
            run_id=run_id,
            experiment="contradiction",
            cfg=cfg,
            extra={
                "n_facts": len(facts),
                "sequential_delay_ms": sequential_delay_ms,
                "settle_delay_ms": settle_delay_ms,
                "write_mode": write_mode,
            },
        ) as sink:
            await asyncio.gather(
                *(
                    _run_fact(
                        c,
                        f,
                        sink,
                        sequential_delay_ms=sequential_delay_ms,
                        settle_delay_ms=settle_delay_ms,
                        write_mode=write_mode,
                        rate_gate=rate_gate,
                    )
                    for f in facts
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
    resolutions = events_to_frame(events, "resolution")
    writes = events_to_frame(events, "write")

    contradictions_csv = run_dir / "contradictions.csv"
    summary_csv = run_dir / "summary.csv"
    detection_plot = run_dir / "detection_rate.png"
    latency_plot = run_dir / "write_latency.png"

    if not resolutions.empty:
        write_csv(resolutions, contradictions_csv)
        # Per-scenario detection rate as a bar chart.
        grouped = (
            resolutions.groupby("scenario")
            .agg(
                n=("detected", "size"),
                detected=("detected", "sum"),
                stale=("stale_read", "sum"),
            )
            .reset_index()
        )
        grouped["detection_rate"] = grouped["detected"] / grouped["n"].clip(lower=1)
        grouped["stale_read_rate"] = grouped["stale"] / grouped["n"].clip(lower=1)
        bar_chart_from_frame(
            grouped,
            x="scenario",
            y="detection_rate",
            label_fmt=lambda r: f"{int(r['detected'])}/{int(r['n'])}",
            title="Contradiction detection rate",
            ylabel="Detection rate",
            out_path=detection_plot,
        )

    if not writes.empty:
        plot_latency_hist(
            writes,
            col="latency_ms",
            out_path=latency_plot,
            title="Write latency (contradiction experiment)",
        )

    n_resolutions = int(len(resolutions))
    n_detected = int(resolutions["detected"].sum()) if "detected" in resolutions else 0
    n_stale = int(resolutions["stale_read"].sum()) if "stale_read" in resolutions else 0
    detection_rate = n_detected / n_resolutions if n_resolutions else 0.0
    stale_read_rate = n_stale / n_resolutions if n_resolutions else 0.0

    summary_rows: list[dict[str, Any]] = [
        {"metric": "n_facts_runs", "value": n_resolutions},
        {"metric": "n_detected", "value": n_detected},
        {"metric": "detection_rate", "value": detection_rate},
        {"metric": "n_stale_reads", "value": n_stale},
        {"metric": "stale_read_rate", "value": stale_read_rate},
        {"metric": "n_writes", "value": int(len(writes))},
    ]
    if not writes.empty:
        for k, v in summarize_latency(writes, "latency_ms").items():
            summary_rows.append({"metric": f"write_latency_{k}", "value": v})
        if "had_contradiction" in writes.columns:
            had = writes[writes["had_contradiction"] == True]  # noqa: E712
            no = writes[writes["had_contradiction"] == False]  # noqa: E712
            for label, frame in (("with_contradiction", had), ("no_contradiction", no)):
                for k, v in summarize_latency(frame, "latency_ms").items():
                    summary_rows.append({"metric": f"write_latency_{label}_{k}", "value": v})

    write_csv(pd.DataFrame(summary_rows), summary_csv)

    return {
        "run_dir": str(run_dir),
        "events": str(events_path),
        "contradictions_csv": str(contradictions_csv),
        "summary_csv": str(summary_csv),
        "detection_plot": str(detection_plot),
        "latency_plot": str(latency_plot),
        "n_facts_runs": n_resolutions,
        "n_detected": n_detected,
        "detection_rate": detection_rate,
        "n_stale_reads": n_stale,
        "stale_read_rate": stale_read_rate,
    }

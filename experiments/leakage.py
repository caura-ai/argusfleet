"""Leakage experiment.

What the experiment claims to measure
-------------------------------------
A multi-tenant, multi-fleet memory service is *correct* only if a
retrieval issued by agent ``A`` in tenant ``T1`` *never* returns a row
that was written into a scope ``A`` is not entitled to read.

We populate the server with secret-bearing memories across a deliberately
small set of governance boundaries, then have each agent fire scripted
queries — some against their own scope (control), some across boundaries
they shouldn't see (probe).

Outputs
-------
- ``outputs/runs/<run_id>/events.jsonl`` — every write / search / decision
- ``outputs/runs/<run_id>/leakage.csv`` — one row per probe attempt
- ``outputs/runs/<run_id>/summary.csv`` — aggregated rates
- ``outputs/runs/<run_id>/leakage_by_axis.png`` — bar chart of leakage rate
- ``outputs/runs/<run_id>/search_latency.png`` — search latency histogram

Determinism
-----------
The synthetic workload is fully seeded. Probe schedule, secret content,
and query phrasing are derived from ``config.seed`` so two runs with the
same seed and same server state produce identical traffic patterns.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from argusfleet.harness.client import MemClawClient, MemClawError
from argusfleet.harness.config import HarnessConfig
from argusfleet.harness.metrics import (
    events_to_frame,
    read_jsonl,
    summarize_latency,
    wilson_ci,
    write_csv,
)
from argusfleet.harness.models import MemoryScope, PolicyDecision
from argusfleet.harness.plots import plot_latency_hist, plot_leakage_by_axis
from argusfleet.harness.runner import (
    make_run_dir,
    open_sink_with_envelope,
    run_with_client,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------- #
# Synthetic workload                                                      #
# ---------------------------------------------------------------------- #


@dataclass(slots=True)
class _SyntheticAgent:
    tenant_id: str
    fleet_id: str
    agent_id: str
    role: str  # "owner" or "probe"


@dataclass(slots=True)
class _ScenarioPlan:
    agents: list[_SyntheticAgent]
    secrets: list[dict[str, Any]]
    probes: list[dict[str, Any]]
    tenant_ids: list[str] = field(default_factory=list)


def tenant_ids_for(cfg: HarnessConfig) -> list[str]:
    """Tenants this experiment will write to.

    Single source of truth shared by plan-building and by ``run all``'s
    cleanup, so the purge targets the tenants actually written — including
    the seed-synthesised IDs used in the default (no ``MEMCLAW_TENANT_ID``)
    mode, which would otherwise never be registered for cleanup.
    """
    exp = cfg.experiment or {}
    override = list(cfg.tenant_ids) or list(exp.get("tenant_ids") or [])
    if override:
        return override
    n_tenants = int(exp.get("n_tenants", 2))
    return [f"mc_lkg_t{ti}_{cfg.seed}" for ti in range(n_tenants)]


def _build_plan(cfg: HarnessConfig, *, nonce: str) -> _ScenarioPlan:
    """Generate a deterministic plan from the seed.

    Two tenants × two fleets × two agents per fleet. Each owner writes
    one ``fleet``-scoped secret and one ``agent``-scoped secret. Every
    other agent then probes for every secret with the most direct query
    we'd expect to surface it.
    """

    rng = random.Random(cfg.seed)
    exp = cfg.experiment or {}
    n_fleets_per_tenant: int = int(exp.get("n_fleets_per_tenant", 2))
    n_agents_per_fleet: int = int(exp.get("n_agents_per_fleet", 2))

    # Resolve the tenants to write to (real IDs from MEMCLAW_TENANT_ID /
    # YAML if supplied, else seed-synthesised — see tenant_ids_for).
    # Synthesising IDs only works for admin keys that bypass
    # enforce_tenant; single-tenant mode still exercises cross-fleet
    # leakage even when cross-tenant isn't testable with the caller's key.
    resolved_tenants = tenant_ids_for(cfg)
    n_tenants = len(resolved_tenants)

    agents: list[_SyntheticAgent] = []
    tenant_ids: list[str] = []
    for ti in range(n_tenants):
        tenant_id = resolved_tenants[ti]
        tenant_ids.append(tenant_id)
        for fi in range(n_fleets_per_tenant):
            fleet_id = f"fleet_{ti}_{fi}"
            for ai in range(n_agents_per_fleet):
                agents.append(
                    _SyntheticAgent(
                        tenant_id=tenant_id,
                        fleet_id=fleet_id,
                        agent_id=f"agent_{ti}_{fi}_{ai}",
                        role="owner" if ai == 0 else "probe",
                    )
                )

    # Each secret carries (a) a distinctive prose body so the embedding
    # scatters away from sibling secrets and the server's near-duplicate
    # detector doesn't 409 our writes, and (b) a unique canary token in
    # metadata that the probe matches on after the search returns.
    #
    # Templates have multiple ``{...}`` placeholders that get randomised
    # per write (dates, amounts, names, team IDs). Per-run variance is
    # what actually moves the embedding — the nonce alone is "noise"
    # tokens the embedder largely ignores.
    secret_templates = [
        "Internal credential rotation for {n}: new shared secret valid until 20{yr:02d}-{mo:02d}-{d:02d}, distributed by team-{t}, expires after {h} hours.",
        "Acquisition target {n} term sheet: revenue multiple {mult}x, close by 20{yr:02d}-Q{q}, advisor commission ${amt}K.",
        "Payroll discrepancy on cost center {n}: missing bonus accrual for Q{q}, ~${amt}K affecting {t} headcount, owner team-{h}.",
        "Vendor contract {n} renegotiated terms: SLA {pct}.{q}% with ${amt}K monthly credit cap, renewal {yr:02d}-{mo:02d}.",
        "Customer support escalation #{t}{h}: account named on legal hold list, opened 20{yr:02d}-{mo:02d}-{d:02d}, assigned to team-{q}.",
        "Layoff plan version {n} draft: {pct}% reduction in region-{t}, communicated 20{yr:02d}-{mo:02d}-{d:02d}, severance ${amt}K cap.",
        "Security incident {t}-{h} root cause: SAML assertion replay window left at {q} minutes, exploited 20{yr:02d}-{mo:02d}-{d:02d}, scope {amt} accounts.",
        "M&A diligence finding {n}: target company {t}{h} has undisclosed litigation valued ${amt}K, surfaced {yr:02d}-{mo:02d}-{d:02d}.",
    ]
    secrets: list[dict[str, Any]] = []
    owner_counter = 0
    for a in agents:
        if a.role != "owner":
            continue
        token = uuid.UUID(int=rng.getrandbits(128), version=4).hex[:12]
        for scope in (MemoryScope.FLEET, MemoryScope.AGENT):
            tpl = secret_templates[owner_counter % len(secret_templates)]
            # Randomise the meaningful values so each write embeds into
            # a different region of vector space. The nonce-suffixed
            # label ``n`` keeps cross-run uniqueness; the other fields
            # supply within-run variance.
            prose = tpl.format(
                n=f"{owner_counter}-{scope.value}-{nonce[-12:]}",
                yr=rng.randint(26, 35),
                mo=rng.randint(1, 12),
                d=rng.randint(1, 28),
                q=rng.randint(1, 4),
                t=rng.randint(10, 99),
                h=rng.randint(100, 999),
                pct=rng.randint(60, 99),
                mult=round(rng.uniform(2.0, 12.5), 1),
                amt=rng.randint(50, 9999),
            )
            owner_counter += 1
            secrets.append(
                {
                    "tenant_id": a.tenant_id,
                    "fleet_id": a.fleet_id,
                    "owner_agent": a.agent_id,
                    "scope": scope,
                    "token": f"canary-{scope.value}-{token}-{nonce}",
                    # Prose drives search recall; the canary in metadata
                    # is what the probe's match logic asserts on.
                    "content": prose,
                    "prose": prose,
                }
            )

    probes: list[dict[str, Any]] = []
    for probe in agents:
        for secret in secrets:
            same_tenant = probe.tenant_id == secret["tenant_id"]
            same_fleet = same_tenant and probe.fleet_id == secret["fleet_id"]
            same_agent = same_fleet and probe.agent_id == secret["owner_agent"]
            scope = secret["scope"]
            if scope == MemoryScope.AGENT:
                allowed = same_agent
            elif scope == MemoryScope.FLEET:
                allowed = same_fleet
            else:
                allowed = same_tenant
            probes.append(
                {
                    "probe_agent": probe.agent_id,
                    "probe_tenant": probe.tenant_id,
                    "probe_fleet": probe.fleet_id,
                    "secret_token": secret["token"],
                    "secret_tenant": secret["tenant_id"],
                    "secret_fleet": secret["fleet_id"],
                    "secret_owner": secret["owner_agent"],
                    "scope": scope,
                    "expected_allow": allowed,
                    # Query by the secret's prose, not the canary token —
                    # the server's search is embedding-driven and a
                    # gibberish token returns nothing useful.
                    "query": secret["prose"],
                }
            )

    # Probe order shouldn't bias outcomes; shuffle deterministically.
    rng.shuffle(probes)
    return _ScenarioPlan(
        agents=agents,
        secrets=secrets,
        probes=probes,
        tenant_ids=tenant_ids,
    )


# ---------------------------------------------------------------------- #
# Runner                                                                  #
# ---------------------------------------------------------------------- #


def _scope_to_visibility(scope: MemoryScope) -> str:
    return {
        MemoryScope.AGENT: "scope_agent",
        MemoryScope.FLEET: "scope_team",
        MemoryScope.TENANT: "scope_tenant",
        MemoryScope.PUBLIC: "scope_public",
    }[scope]


async def _seed_secrets(
    client: MemClawClient, plan: _ScenarioPlan, sink: JsonlSink
) -> dict[str, str]:
    """Write every secret. Returns {token -> memory_id} for verification."""
    written: dict[str, str] = {}
    for secret in plan.secrets:
        idem_key = hashlib.sha256(secret["token"].encode()).hexdigest()[:32]
        t0 = datetime.now(timezone.utc)
        try:
            resp = await client.write_memory(
                tenant_id=secret["tenant_id"],
                fleet_id=secret["fleet_id"],
                agent_id=secret["owner_agent"],
                content=secret["content"],
                visibility=_scope_to_visibility(secret["scope"]),
                metadata={"experiment": "leakage", "canary": secret["token"]},
                idempotency_key=idem_key,
            )
            mem_id = (resp.json or {}).get("id") if isinstance(resp.json, dict) else None
            if mem_id:
                written[secret["token"]] = mem_id
            sink.write(
                "write",
                {
                    "writer_agent": secret["owner_agent"],
                    "tenant_id": secret["tenant_id"],
                    "fleet_id": secret["fleet_id"],
                    "scope": secret["scope"].value,
                    "memory_id": mem_id,
                    "succeeded": True,
                    "latency_ms": resp.latency_ms,
                    "trace_id": resp.trace_id,
                    "token": secret["token"],
                },
            )
        except MemClawError as e:
            sink.write(
                "write",
                {
                    "writer_agent": secret["owner_agent"],
                    "tenant_id": secret["tenant_id"],
                    "fleet_id": secret["fleet_id"],
                    "scope": secret["scope"].value,
                    "succeeded": False,
                    "status_code": e.status_code,
                    "error": e.message,
                    "trace_id": e.trace_id,
                    "token": secret["token"],
                    "elapsed_ms": (datetime.now(timezone.utc) - t0).total_seconds() * 1000,
                },
            )
    return written


async def _probe_one(
    client: MemClawClient,
    probe: dict[str, Any],
    sink: JsonlSink,
    *,
    memory_id: str | None,
    rate_gate: asyncio.Semaphore,
) -> dict[str, Any]:
    """Issue two orthogonal probes per (probe_agent, secret) pair:

    1. **Access probe** (authoritative): ``GET /memories/{id}?tenant_id=...``
       against the probe's own tenant. The server returns 200 iff the
       row is genuinely visible to the probe's tenant. 4xx is a deny.
       This is the *security envelope* — it isolates scope enforcement
       from embedding-ranking artefacts. The ``leak_rate`` and
       ``miss_rate`` summary metrics derive from this signal.

    2. **Discovery probe** (secondary): the same prose search the
       experiment used previously. This captures whether the row is
       *findable* via embedding-similarity search, separate from
       whether it's *accessible*. It is reported as
       ``search_leak_rate`` and ``search_miss_rate`` and never
       conflated with the security signal.
    """
    async with rate_gate:
        expected_allow = probe["expected_allow"]
        expected = "allow" if expected_allow else "deny"

        # --- Access probe (GET) --------------------------------------
        access_decision = "error"
        access_status: int | None = None
        access_trace: str | None = None
        access_latency: float | None = None
        access_error: str | None = None
        if memory_id:
            try:
                get_resp = await client.retrieve_memory(
                    memory_id, tenant_id=probe["probe_tenant"]
                )
                access_decision = "allow"
                access_status = get_resp.status_code
                access_trace = get_resp.trace_id
                access_latency = get_resp.latency_ms
            except MemClawError as e:
                # 403/404 are the correct deny outcomes; everything else
                # (5xx, transport) is recorded as an error rather than a
                # silent deny to keep the metric honest.
                access_status = e.status_code
                access_trace = e.trace_id
                access_error = e.message
                if e.status_code in (401, 403, 404):
                    access_decision = "deny"
                else:
                    access_decision = "error"

        is_access_leak = access_decision == "allow" and not expected_allow
        is_access_miss = access_decision == "deny" and expected_allow

        # --- Discovery probe (search) --------------------------------
        search_decision = "error"
        search_status: int | None = None
        search_trace: str | None = None
        search_latency: float | None = None
        returned_count = 0
        try:
            # filter_agent_id intentionally NOT set — see governance
            # discussion in earlier comment.
            resp = await client.search_memory(
                tenant_id=probe["probe_tenant"],
                query=probe["query"],
                top_k=5,
            )
            items = (resp.json or {}).get("items") if isinstance(resp.json, dict) else None
            items = items or []
            returned_count = len(items)
            matched_tokens = [
                it.get("metadata", {}).get("canary")
                for it in items
                if isinstance(it, dict)
            ]
            saw_secret = probe["secret_token"] in matched_tokens
            search_decision = "allow" if saw_secret else "deny"
            search_status = resp.status_code
            search_trace = resp.trace_id
            search_latency = resp.latency_ms
        except MemClawError as e:
            search_status = e.status_code
            search_trace = e.trace_id

        is_search_leak = search_decision == "allow" and not expected_allow
        is_search_miss = search_decision == "deny" and expected_allow

        row = {
            "probe_agent": probe["probe_agent"],
            "probe_tenant": probe["probe_tenant"],
            "probe_fleet": probe["probe_fleet"],
            "secret_token": probe["secret_token"],
            "secret_tenant": probe["secret_tenant"],
            "secret_fleet": probe["secret_fleet"],
            "secret_owner": probe["secret_owner"],
            "memory_id": memory_id,
            "scope": probe["scope"].value,
            "expected_allow": expected_allow,
            "expected_decision": expected,
            # Authoritative access signals.
            "access_decision": access_decision,
            "access_status_code": access_status,
            "access_latency_ms": access_latency,
            "access_trace_id": access_trace,
            "access_error": access_error,
            "is_leak": is_access_leak,
            "is_miss": is_access_miss,
            # Secondary discovery signals.
            "search_decision": search_decision,
            "search_status_code": search_status,
            "search_latency_ms": search_latency,
            "search_trace_id": search_trace,
            "returned_count": returned_count,
            "is_search_leak": is_search_leak,
            "is_search_miss": is_search_miss,
        }
        sink.write("probe", row)
        sink.write(
            "policy_decision",
            PolicyDecision(
                actor_agent=probe["probe_agent"],
                actor_tenant=probe["probe_tenant"],
                actor_fleet=probe["probe_fleet"],
                target_scope=probe["scope"],
                target_tenant=probe["secret_tenant"],
                target_fleet=probe["secret_fleet"],
                action="read",
                decision=access_decision,
                expected_decision=expected,
                reason=(
                    f"GET returned {access_status}" if memory_id
                    else "no memory_id (write failed)"
                ),
            ).model_dump(mode="json"),
        )
        return row


async def run(
    cfg: HarnessConfig,
    *,
    client: MemClawClient | None = None,
) -> dict[str, Any]:
    """Execute the leakage experiment end-to-end.

    The function returns a small dict of result paths + summary stats so a
    caller (CLI, notebook, pytest) can assert on it directly.
    """
    run_id, run_dir, events_path = make_run_dir(cfg, "leakage")
    plan = _build_plan(cfg, nonce=run_id)
    logger.info(
        "leakage run %s — agents=%d secrets=%d probes=%d",
        run_id,
        len(plan.agents),
        len(plan.secrets),
        len(plan.probes),
    )

    exp = cfg.experiment or {}
    rate_gate = asyncio.Semaphore(int(exp.get("max_in_flight", 8)))
    settle_delay_ms = float(exp.get("settle_delay_ms", 0.0))

    async def _execute(c: MemClawClient) -> None:
        with open_sink_with_envelope(
            events_path,
            run_id=run_id,
            experiment="leakage",
            cfg=cfg,
            extra={
                "agents": [asdict(a) for a in plan.agents],
                "tenant_ids": plan.tenant_ids,
                "n_secrets": len(plan.secrets),
                "n_probes": len(plan.probes),
            },
        ) as sink:
            tokens_to_ids = await _seed_secrets(c, plan, sink)
            if settle_delay_ms > 0:
                logger.info("settling %.0fms before probes", settle_delay_ms)
                sink.write("settle", {"delay_ms": settle_delay_ms, "phase": "post_write"})
                await asyncio.sleep(settle_delay_ms / 1000.0)
            await asyncio.gather(
                *(
                    _probe_one(
                        c,
                        p,
                        sink,
                        memory_id=tokens_to_ids.get(p["secret_token"]),
                        rate_gate=rate_gate,
                    )
                    for p in plan.probes
                )
            )
            sink.write("run_end", {"run_id": run_id})

    await run_with_client(cfg, client=client, body=_execute)
    result = _materialize_outputs(run_dir, events_path)
    result["tenant_ids"] = tenant_ids_for(cfg)
    return result


def _materialize_outputs(run_dir: Path, events_path: Path) -> dict[str, Any]:
    """Read the JSONL back and emit CSVs + plots. Returns summary dict."""
    events = read_jsonl(events_path)
    probes = events_to_frame(events, "probe")
    writes = events_to_frame(events, "write")

    leakage_csv = run_dir / "leakage.csv"
    summary_csv = run_dir / "summary.csv"
    leakage_plot = run_dir / "leakage_by_axis.png"
    latency_plot = run_dir / "search_latency.png"

    if not probes.empty:
        write_csv(probes, leakage_csv)
        plot_leakage_by_axis(probes, out_path=leakage_plot)
        plot_latency_hist(probes, col="search_latency_ms", out_path=latency_plot,
                          title="Search latency (leakage probes)")

    total = int(len(probes))
    # ``is_violation`` = leak ∪ miss; computed on the fly rather than stored.
    if total and "is_leak" in probes.columns and "is_miss" in probes.columns:
        violations = int((probes["is_leak"] | probes["is_miss"]).sum())
    else:
        violations = 0
    # ``leak_rate`` and ``miss_rate`` are derived from the authoritative
    # GET access probe, not from semantic search. Denominators differ:
    # leak_rate is over expected-deny probes (the security envelope);
    # miss_rate is over expected-allow probes (the availability envelope).
    n_leaks = int(probes["is_leak"].sum()) if "is_leak" in probes else 0
    n_misses = int(probes["is_miss"].sum()) if "is_miss" in probes else 0
    n_expected_deny = (
        int((probes["expected_allow"] == False).sum())  # noqa: E712
        if "expected_allow" in probes
        else 0
    )
    n_expected_allow = (
        int((probes["expected_allow"] == True).sum())  # noqa: E712
        if "expected_allow" in probes
        else 0
    )
    leak_rate = (n_leaks / n_expected_deny) if n_expected_deny else 0.0
    miss_rate = (n_misses / n_expected_allow) if n_expected_allow else 0.0
    leak_ci = wilson_ci(n_leaks, n_expected_deny)
    miss_ci = wilson_ci(n_misses, n_expected_allow)

    # Secondary discovery-axis signals (search-based). Reported but
    # never folded into the headline leak/miss rates.
    n_search_leaks = int(probes["is_search_leak"].sum()) if "is_search_leak" in probes else 0
    n_search_misses = int(probes["is_search_miss"].sum()) if "is_search_miss" in probes else 0
    search_leak_rate = (n_search_leaks / n_expected_deny) if n_expected_deny else 0.0
    search_miss_rate = (n_search_misses / n_expected_allow) if n_expected_allow else 0.0

    combined_violation_rate = (violations / total) if total else 0.0
    search_latency_summary = (
        summarize_latency(probes, "search_latency_ms")
        if total and "search_latency_ms" in probes.columns
        else {}
    )
    access_latency_summary = (
        summarize_latency(probes, "access_latency_ms")
        if total and "access_latency_ms" in probes.columns
        else {}
    )

    summary_rows = [
        {"metric": "n_probes", "value": total},
        {"metric": "n_expected_allow", "value": n_expected_allow},
        {"metric": "n_expected_deny", "value": n_expected_deny},
        # Authoritative access signal (GET-by-id).
        {"metric": "n_leaks", "value": n_leaks},
        {"metric": "n_misses", "value": n_misses},
        {"metric": "leak_rate", "value": leak_rate},
        {"metric": "leak_rate_ci95_lo", "value": leak_ci[0]},
        {"metric": "leak_rate_ci95_hi", "value": leak_ci[1]},
        {"metric": "miss_rate", "value": miss_rate},
        {"metric": "miss_rate_ci95_lo", "value": miss_ci[0]},
        {"metric": "miss_rate_ci95_hi", "value": miss_ci[1]},
        # Secondary discovery signal (search).
        {"metric": "n_search_leaks", "value": n_search_leaks},
        {"metric": "n_search_misses", "value": n_search_misses},
        {"metric": "search_leak_rate", "value": search_leak_rate},
        {"metric": "search_miss_rate", "value": search_miss_rate},
        {"metric": "n_violations", "value": violations},
        {"metric": "n_writes", "value": int(len(writes))},
    ]
    for k, v in search_latency_summary.items():
        summary_rows.append({"metric": f"search_latency_{k}", "value": v})
    for k, v in access_latency_summary.items():
        summary_rows.append({"metric": f"access_latency_{k}", "value": v})
    write_csv(pd.DataFrame(summary_rows), summary_csv)

    return {
        "run_dir": str(run_dir),
        "events": str(events_path),
        "leakage_csv": str(leakage_csv),
        "summary_csv": str(summary_csv),
        "leakage_plot": str(leakage_plot),
        "latency_plot": str(latency_plot),
        "n_probes": total,
        "n_violations": violations,
        "n_leaks": n_leaks,
        "n_misses": n_misses,
        "leak_rate": leak_rate,
        "leak_rate_ci95": list(leak_ci),
        "miss_rate": miss_rate,
        "miss_rate_ci95": list(miss_ci),
        "search_leak_rate": search_leak_rate,
        "search_miss_rate": search_miss_rate,
        "search_latency": search_latency_summary,
        "access_latency": access_latency_summary,
    }

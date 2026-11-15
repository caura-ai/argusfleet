"""Derived-leakage experiment.

What the experiment claims to measure
-------------------------------------
Invariant (*derivation-closed scope*): a memory ``M``'s retrievability by
reader ``R`` must imply ``R`` is authorized for **every** source in ``M``'s
transitive derivation — not just ``M``'s own entry-level scope.

The ``leakage`` experiment tests entry-level scope: can ``R`` read a row
whose *own* visibility excludes ``R``? This experiment tests the
derivation channel: agent ``A`` writes an agent-scoped secret ``S``, then
writes a team-scoped summary ``M`` with ``metadata.derived_from = [S]``.
``M``'s entry-level scope admits every agent in the fleet, but its content
is derived from a row only ``A`` may read. A same-fleet peer ``C`` that can
retrieve ``M`` has read (a projection of) ``S`` without being authorized
for ``S``.

Scenario (v1)
-------------
1. With the tenant key, provision two agent keys — ``A`` and ``C``, both
   trust-1, both homed in ``home_fleet`` (default ``platform``). Identity
   is resolved by the gateway from each key; the tenant key carries none.
2. Per chain, ``A`` (with its own key) writes:
   - ``S``  — ``scope_agent``, secret prose, canary in metadata;
   - ``M1 .. Md`` — ``scope_team``, distinct summary prose,
     ``derived_from = [previous id]`` (``d = chain_depth``, max 4);
   - ``D``  — ``scope_team``, clean prose, **no** ``derived_from``. Same
     writer and same visibility as the leaf ``Md``: the only difference
     between ``D`` and ``Md`` is ancestry, which isolates the variable.
3. Settle (async embedding), then verify each chain by walking
   ``metadata.derived_from`` from the leaf back to ``S`` with the tenant
   key. The walk is **bounded** (visited set + hop cap = ``chain_depth``
   + 1) — see docs/code-review.md B3; a cyclic or truncated chain is
   recorded as ``chain_broken`` (a data-quality signal, *not* a leak) and
   its derived probe is excluded from grading.
4. Probe with each reader's own agent key, two signals per probe (same as
   ``leakage._probe_one``): access = ``GET /memories/{id}?tenant_id=``
   (authoritative; 200 allow, 401/403/404 deny, else error); discovery =
   search by the target's prose, ``top_k=5``, match ``metadata.canary``.

Probe kinds (ground truth):

==================  ======  ======  ==============  ===========================
kind                reader  target  expected_allow  role
==================  ======  ======  ==============  ===========================
``derived``         C       Md      False           the probe (headline)
``control_clean``   C       D       True            availability envelope
``control_owner``   A       Md      True            availability envelope
``control_source``  C       S       False           run-validity control
==================  ======  ======  ==============  ===========================

``control_source`` validates that the agent keys actually distinguish
readers. If C can GET S, the run cannot say anything about derivation and
is marked ``run_valid = false``; derived-leak metrics are then left blank
rather than graded.

The probe does not adjudicate whether any write was legitimate; it tests
only whether C's retrieval of M respects S's scope.

Preregistered expected outcome
------------------------------
On the current MemClaw server this probe is **expected to FAIL**:
``M`` is team-visible and nothing in the retrieval path consults
``derived_from`` (it is harness-written metadata the server stores
verbatim). We predict ``derived_leak_rate`` near 1.0 with the run valid
(C denied on S) and the clean / owner controls allowed. A fail is the
predicted result, not a bug report — predict, measure, remediate. If it
passes, it stands as a regression guard. This module measures only; it
does not change the server.

Out of scope for v1 (follow-ups)
--------------------------------
- Cross-agent derivation: ``B`` derives ``M`` from ``A``'s ``M'`` (the
  same writer ``A`` produces every link in v1).
- Cross-fleet readers and trust-2 readers (trust ladder interactions).
- Multi-parent ``derived_from`` (fan-in) and deletion/revocation of the
  source after derivation.

Outputs
-------
- ``events.jsonl`` — every provision (ids/trust/fleet only, never keys),
  write, verification and probe
- ``derived_leakage.csv`` — one row per probe
- ``chains.csv`` — one row per chain verification walk
- ``summary.csv`` — aggregated rates with 95% Wilson intervals
- ``allow_rate_by_kind.png`` — GET allow rate per probe kind
- ``access_latency.png`` — access-probe latency histogram

``fetch_latency_*`` in the summary is the access-probe GET latency (the
headline signal); ``verify_latency_*`` is the chain-walk GET latency.

Determinism
-----------
The plan (prose, tokens, probe order) is seeded from ``cfg.seed``; the
per-run nonce salts canary tokens, content and agent ids so re-runs
against a stateful tenant stay distinct. Writes carry idempotency keys
derived from their tokens.
"""

from __future__ import annotations

import asyncio
import hashlib
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
from argusfleet.harness.logging import JsonlSink
from argusfleet.harness.metrics import (
    events_to_frame,
    read_jsonl,
    summarize_latency,
    wilson_ci,
    wilson_yerr,
    write_csv,
)
from argusfleet.harness.models import MemoryScope, PolicyDecision
from argusfleet.harness.plots import plot_latency_hist
from argusfleet.harness.runner import (
    make_run_dir,
    open_sink_with_envelope,
    run_with_client,
)

logger = logging.getLogger(__name__)

EXPERIMENT = "derived-leakage"
MAX_CHAIN_DEPTH = 4
PROBE_KINDS = ("derived", "control_clean", "control_owner", "control_source")


class AgentKeyProvisioningError(RuntimeError):
    """Agent keys could not be provisioned with the configured key.

    Raised instead of falling back to tenant-keyed probes: a tenant key
    carries no agent identity, so such a run would measure tenant-wide
    access, not derivation-aware scope.
    """


# ---------------------------------------------------------------------- #
# Plan                                                                    #
# ---------------------------------------------------------------------- #


@dataclass(slots=True)
class _Agent:
    role: str  # "writer" (A) or "reader" (C)
    agent_id: str
    fleet_id: str
    trust: int


@dataclass(slots=True)
class _Node:
    """One planned write. ``parent`` is the token of the node it derives
    from (``None`` for S and D)."""

    kind: str  # "source" | "derived" | "clean"
    token: str
    prose: str
    visibility: str
    scope: MemoryScope
    hop: int  # 0 for S/D, 1..d for M1..Md
    parent: str | None = None


@dataclass(slots=True)
class _Chain:
    chain_id: str
    source: _Node
    derived: list[_Node]  # M1..Md, in write order
    clean: _Node

    @property
    def leaf(self) -> _Node:
        return self.derived[-1]


@dataclass(slots=True)
class _Plan:
    tenant_id: str
    writer: _Agent
    reader: _Agent
    chains: list[_Chain]
    probes: list[dict[str, Any]] = field(default_factory=list)


def tenant_ids_for(cfg: HarnessConfig) -> list[str]:
    """Tenants this experiment writes to (exactly one).

    Same resolution order as ``leakage.tenant_ids_for``; only the first
    tenant is used — agent keys are provisioned into the tenant key's
    tenant — so only that one is returned for ``run all`` cleanup.
    """
    exp = cfg.experiment or {}
    override = list(cfg.tenant_ids) or list(exp.get("tenant_ids") or [])
    if not override and exp.get("tenant_id"):
        override = [str(exp["tenant_id"])]
    if override:
        return override[:1]
    return [f"mc_dlk_t0_{cfg.seed}"]


def _chain_depth(cfg: HarnessConfig) -> int:
    depth = int((cfg.experiment or {}).get("chain_depth", 1))
    if not 1 <= depth <= MAX_CHAIN_DEPTH:
        raise ValueError(f"chain_depth must be in [1, {MAX_CHAIN_DEPTH}], got {depth}")
    return depth


_SECRET_TEMPLATES = [
    "Private negotiation note {n}: walk-away price ${amt}K for supplier {t}, deadline 20{yr:02d}-{mo:02d}-{d:02d}, do not disclose to procurement.",
    "Personal medical accommodation {n}: employee badge {t}{h} on reduced hours through 20{yr:02d}-{mo:02d}, approved by HR case {q}.",
    "Unreleased earnings figure {n}: Q{q} operating margin {pct}.{d}% versus guidance, embargoed until 20{yr:02d}-{mo:02d}-{d:02d}.",
    "Credential escrow record {n}: break-glass root password for cluster {t}-{h} rotated 20{yr:02d}-{mo:02d}-{d:02d}, held by on-call only.",
    "Litigation strategy memo {n}: settle claim {t}{h} below ${amt}K, counsel advises against discovery before Q{q} 20{yr:02d}.",
    "Whistleblower intake {n}: report {t}-{h} names a director in region {q}, filed 20{yr:02d}-{mo:02d}-{d:02d}, identity protected.",
]

_SUMMARY_TEMPLATES = [
    "Status digest {n}: the team should plan the vendor review around a {pct}-day window and a budget ceiling near ${amt}K; follow-up owner rotates weekly.",
    "Weekly roll-up {n}: staffing in group {t} adjusts for a while, so schedule handoffs carefully until month {mo}; nothing else blocking.",
    "Planning recap {n}: finance expects quarter {q} margins to come in off-plan; keep spend discretionary until the announcement lands.",
    "Ops briefing {n}: access to cluster {t} is temporarily restricted to on-call; route all admin requests through the incident channel for now.",
    "Legal sync {n}: prefer an early resolution on matter {t}; avoid creating new written records on that topic before quarter {q}.",
    "Governance note {n}: a sensitive report from region {q} is under review; keep discussion of leadership changes off shared channels.",
]

_CLEAN_TEMPLATES = [
    "Team logistics {n}: the shared standup moves to {h} UTC on Tuesdays and the wiki page for room bookings now lives under ops/rooms.",
    "Release calendar {n}: build train {t} freezes on day {d} of month {mo}; changelog drafts are due one day earlier.",
    "Onboarding tip {n}: new members should request read access to dashboard {t}{h} and join the fleet channel before their first rotation.",
    "Tooling update {n}: the linter config was bumped to ruleset {q}.{d}; run the formatter before opening a pull request.",
    "Facilities note {n}: badge readers on floor {q} will be serviced during week {d}; use the east entrance meanwhile.",
    "Docs reminder {n}: runbooks for service {t} were reorganised; search by alert name rather than by owner team.",
]


def _fill(tpl: str, rng: random.Random, n: str) -> str:
    return tpl.format(
        n=n,
        yr=rng.randint(26, 35),
        mo=rng.randint(1, 12),
        d=rng.randint(1, 28),
        q=rng.randint(1, 4),
        t=rng.randint(10, 99),
        h=rng.randint(100, 999),
        pct=rng.randint(10, 99),
        amt=rng.randint(50, 9999),
    )


def _short_nonce(nonce: str) -> str:
    return hashlib.sha256(nonce.encode()).hexdigest()[:8]


def _build_plan(cfg: HarnessConfig, *, nonce: str) -> _Plan:
    """Deterministic plan from the seed; ``nonce`` salts tokens/prose/ids."""
    rng = random.Random(cfg.seed)
    exp = cfg.experiment or {}
    n_chains = int(exp.get("n_chains", 20))
    depth = _chain_depth(cfg)
    fleet = str(exp.get("home_fleet", "platform"))
    short = _short_nonce(nonce)

    writer = _Agent(role="writer", agent_id=f"af-dlk-a-{short}", fleet_id=fleet, trust=1)
    reader = _Agent(role="reader", agent_id=f"af-dlk-c-{short}", fleet_id=fleet, trust=1)

    chains: list[_Chain] = []
    for ci in range(n_chains):
        chain_id = f"ch{ci:03d}"
        tok = uuid.UUID(int=rng.getrandbits(128), version=4).hex[:12]
        label = f"{chain_id}-{nonce[-12:]}"
        tpl_i = ci % len(_SECRET_TEMPLATES)
        source = _Node(
            kind="source",
            token=f"canary-dlk-s-{tok}-{nonce}",
            prose=_fill(_SECRET_TEMPLATES[tpl_i], rng, label),
            visibility="scope_agent",
            scope=MemoryScope.AGENT,
            hop=0,
        )
        derived: list[_Node] = []
        parent = source.token
        for hop in range(1, depth + 1):
            node = _Node(
                kind="derived",
                token=f"canary-dlk-m{hop}-{tok}-{nonce}",
                prose=_fill(
                    _SUMMARY_TEMPLATES[(tpl_i + hop - 1) % len(_SUMMARY_TEMPLATES)],
                    rng,
                    f"{label}-h{hop}",
                ),
                visibility="scope_team",
                scope=MemoryScope.FLEET,
                hop=hop,
                parent=parent,
            )
            derived.append(node)
            parent = node.token
        clean = _Node(
            kind="clean",
            token=f"canary-dlk-d-{tok}-{nonce}",
            prose=_fill(_CLEAN_TEMPLATES[tpl_i], rng, label),
            visibility="scope_team",
            scope=MemoryScope.FLEET,
            hop=0,
        )
        chains.append(_Chain(chain_id=chain_id, source=source, derived=derived, clean=clean))

    probes: list[dict[str, Any]] = []
    for ch in chains:
        for kind, who, target, expected in (
            ("derived", reader, ch.leaf, False),
            ("control_clean", reader, ch.clean, True),
            ("control_owner", writer, ch.leaf, True),
            ("control_source", reader, ch.source, False),
        ):
            probes.append(
                {
                    "chain_id": ch.chain_id,
                    "kind": kind,
                    "reader_role": who.role,
                    "reader_agent": who.agent_id,
                    "reader_fleet": who.fleet_id,
                    "target_token": target.token,
                    "target_kind": target.kind,
                    "target_scope": target.scope,
                    "target_hop": target.hop,
                    "expected_allow": expected,
                    "query": target.prose,
                }
            )
    rng.shuffle(probes)
    return _Plan(
        tenant_id=tenant_ids_for(cfg)[0],
        writer=writer,
        reader=reader,
        chains=chains,
        probes=probes,
    )


# ---------------------------------------------------------------------- #
# Provisioning                                                            #
# ---------------------------------------------------------------------- #


async def _provision(
    client: MemoryBackend, agent: _Agent, sink: JsonlSink
) -> MemoryBackend:
    """Provision ``agent``'s key and return a client bound to it.

    Fails fast — never falls back to the tenant key. Only the agent id,
    fleet and trust are logged; the raw key stays in memory on the
    returned client instance.
    """
    provision = getattr(client, "provision_agent_key", None)
    for_agent_key = getattr(client, "for_agent_key", None)
    if provision is None or for_agent_key is None:
        raise AgentKeyProvisioningError(
            f"{EXPERIMENT} requires agent-scoped credentials; backend "
            f"{type(client).__name__} has no agent-key support (MemClaw only)."
        )
    try:
        resp = await provision(
            agent_id=agent.agent_id,
            fleet_id=agent.fleet_id,
            trust=agent.trust,
            label=f"argusfleet {EXPERIMENT} {agent.role}",
        )
    except BackendError as e:
        sink.write(
            "agent_provisioned",
            {
                "role": agent.role,
                "agent_id": agent.agent_id,
                "fleet_id": agent.fleet_id,
                "trust": agent.trust,
                "succeeded": False,
                "status_code": e.status_code,
            },
        )
        hint = (
            "the configured MEMCLAW_API_KEY lacks the admin capability required "
            "by POST /api/v1/admin/agent-keys/provision"
            if e.status_code in (401, 403)
            else "the provision endpoint returned an unexpected error"
        )
        raise AgentKeyProvisioningError(
            f"provisioning agent key for {agent.agent_id} failed (HTTP {e.status_code}): "
            f"{hint}. Refusing to fall back to tenant-keyed probes, which would "
            f"measure tenant-wide access rather than derivation-aware scope."
        ) from e
    raw = type(client).raw_agent_key(resp)  # type: ignore[attr-defined]
    if not raw:
        fields = sorted((resp.json or {}).keys()) if isinstance(resp.json, dict) else []
        raise AgentKeyProvisioningError(
            f"provision response for {agent.agent_id} carried no raw_key/key/api_key "
            f"(fields: {fields})"
        )
    sink.write(
        "agent_provisioned",
        {
            "role": agent.role,
            "agent_id": agent.agent_id,
            "fleet_id": agent.fleet_id,
            "trust": agent.trust,
            "succeeded": True,
            "trace_id": resp.trace_id,
        },
    )
    return for_agent_key(raw)


# ---------------------------------------------------------------------- #
# Writes                                                                  #
# ---------------------------------------------------------------------- #


async def _write_node(
    client: MemoryBackend,
    plan: _Plan,
    chain: _Chain,
    node: _Node,
    parent_id: str | None,
    sink: JsonlSink,
) -> str | None:
    metadata: dict[str, Any] = {"experiment": EXPERIMENT, "canary": node.token}
    if parent_id is not None:
        metadata["derived_from"] = [parent_id]
    idem = hashlib.sha256(node.token.encode()).hexdigest()[:32]
    base = {
        "chain_id": chain.chain_id,
        "node_kind": node.kind,
        "hop": node.hop,
        "writer_agent": plan.writer.agent_id,
        "tenant_id": plan.tenant_id,
        "fleet_id": plan.writer.fleet_id,
        "visibility": node.visibility,
        "token": node.token,
        "parent_memory_id": parent_id,
    }
    try:
        resp = await client.write_memory(
            tenant_id=plan.tenant_id,
            agent_id=plan.writer.agent_id,
            fleet_id=plan.writer.fleet_id,
            content=node.prose,
            visibility=node.visibility,
            metadata=metadata,
            idempotency_key=idem,
        )
    except BackendError as e:
        sink.write(
            "write",
            {**base, "succeeded": False, "status_code": e.status_code,
             "error": e.message, "trace_id": e.trace_id},
        )
        return None
    mem_id = (resp.json or {}).get("id") if isinstance(resp.json, dict) else None
    sink.write(
        "write",
        {**base, "succeeded": bool(mem_id), "memory_id": mem_id,
         "latency_ms": resp.latency_ms, "trace_id": resp.trace_id},
    )
    return mem_id


async def _seed_chain(
    writer: MemoryBackend, plan: _Plan, chain: _Chain, sink: JsonlSink
) -> dict[str, str]:
    """Write S, M1..Md, D for one chain with A's key. Returns {token: id}.

    A failed link stops the derived chain (later links would reference a
    phantom parent); D is written regardless.
    """
    ids: dict[str, str] = {}
    s_id = await _write_node(writer, plan, chain, chain.source, None, sink)
    if s_id:
        ids[chain.source.token] = s_id
        parent_id = s_id
        for node in chain.derived:
            m_id = await _write_node(writer, plan, chain, node, parent_id, sink)
            if not m_id:
                break
            ids[node.token] = m_id
            parent_id = m_id
    d_id = await _write_node(writer, plan, chain, chain.clean, None, sink)
    if d_id:
        ids[chain.clean.token] = d_id
    return ids


# ---------------------------------------------------------------------- #
# Chain verification (bounded walk)                                       #
# ---------------------------------------------------------------------- #


async def _verify_chain(
    client: MemoryBackend,
    plan: _Plan,
    chain: _Chain,
    ids: dict[str, str],
    sink: JsonlSink,
) -> dict[str, Any]:
    """Walk ``metadata.derived_from`` from the leaf back to S.

    Bounded per docs/code-review.md B3: a visited set stops cycles and a
    hop cap (``chain_depth + 1`` fetches: every M plus S) stops runaway or
    fabricated ancestry. Anything short of reaching S is ``chain_broken``.
    """
    depth = len(chain.derived)
    s_id = ids.get(chain.source.token)
    leaf_id = ids.get(chain.leaf.token)
    row: dict[str, Any] = {
        "chain_id": chain.chain_id,
        "chain_depth": depth,
        "source_memory_id": s_id,
        "leaf_memory_id": leaf_id,
        "hops_walked": 0,
        "reached_source": False,
        "chain_broken": True,
        "broken_reason": None,
        "verify_latency_ms_max": None,
    }
    if not s_id or not leaf_id:
        row["broken_reason"] = "write_failed"
        sink.write("verification", row)
        return row

    max_fetches = depth + 1
    visited: set[str] = set()
    latencies: list[float] = []
    cursor: str | None = leaf_id
    reason: str | None = None
    while True:
        if cursor is None:
            reason = "derived_from_missing"
            break
        if cursor in visited:
            reason = "cycle_detected"
            break
        if len(visited) >= max_fetches:
            reason = "exceeded_depth_cap"
            break
        visited.add(cursor)
        try:
            resp = await client.retrieve_memory(cursor, tenant_id=plan.tenant_id)
        except BackendError as e:
            reason = f"fetch_{e.status_code}"
            break
        if resp.latency_ms is not None:
            latencies.append(resp.latency_ms)
        if cursor == s_id:
            row["reached_source"] = True
            break
        row["hops_walked"] += 1
        payload = resp.json if isinstance(resp.json, dict) else {}
        parents = (payload.get("metadata") or {}).get("derived_from") or []
        cursor = str(parents[0]) if isinstance(parents, list) and parents else None

    row["chain_broken"] = not row["reached_source"]
    row["broken_reason"] = reason if row["chain_broken"] else None
    row["n_fetches"] = len(visited)
    row["verify_latency_ms_max"] = max(latencies) if latencies else None
    row["verify_latencies_ms"] = latencies
    sink.write("verification", row)
    return row


# ---------------------------------------------------------------------- #
# Probes                                                                  #
# ---------------------------------------------------------------------- #


async def _probe_one(
    client: MemoryBackend,
    plan: _Plan,
    probe: dict[str, Any],
    sink: JsonlSink,
    *,
    memory_id: str | None,
    chain_broken: bool,
    rate_gate: asyncio.Semaphore,
) -> dict[str, Any]:
    """Two signals per probe, issued with the reader's own agent key —
    the same access/discovery split as ``leakage._probe_one``."""
    async with rate_gate:
        expected_allow = probe["expected_allow"]
        expected = "allow" if expected_allow else "deny"

        access_decision = "error"
        access_status: int | None = None
        access_trace: str | None = None
        access_latency: float | None = None
        access_error: str | None = None
        if memory_id:
            try:
                resp = await client.retrieve_memory(memory_id, tenant_id=plan.tenant_id)
                access_decision = "allow"
                access_status = resp.status_code
                access_trace = resp.trace_id
                access_latency = resp.latency_ms
            except BackendError as e:
                access_status = e.status_code
                access_trace = e.trace_id
                access_error = e.message
                access_decision = "deny" if e.status_code in (401, 403, 404) else "error"

        search_decision = "error"
        search_status: int | None = None
        search_trace: str | None = None
        search_latency: float | None = None
        returned_count = 0
        try:
            resp = await client.search_memory(
                tenant_id=plan.tenant_id, query=probe["query"], top_k=5
            )
            items = (resp.json or {}).get("items") if isinstance(resp.json, dict) else None
            items = items or []
            returned_count = len(items)
            canaries = [
                (it.get("metadata") or {}).get("canary") for it in items if isinstance(it, dict)
            ]
            search_decision = "allow" if probe["target_token"] in canaries else "deny"
            search_status = resp.status_code
            search_trace = resp.trace_id
            search_latency = resp.latency_ms
        except BackendError as e:
            search_status = e.status_code
            search_trace = e.trace_id

        # Derived probes on a broken chain are not graded: the server-side
        # ancestry isn't established, so an allow there says nothing about
        # derivation-aware scope.
        graded = not (probe["kind"] == "derived" and chain_broken)
        row = {
            "chain_id": probe["chain_id"],
            "kind": probe["kind"],
            "reader_role": probe["reader_role"],
            "reader_agent": probe["reader_agent"],
            "reader_fleet": probe["reader_fleet"],
            "tenant_id": plan.tenant_id,
            "target_token": probe["target_token"],
            "target_kind": probe["target_kind"],
            "target_scope": probe["target_scope"].value,
            "target_hop": probe["target_hop"],
            "memory_id": memory_id,
            "chain_broken": chain_broken,
            "graded": graded,
            "expected_allow": expected_allow,
            "expected_decision": expected,
            "access_decision": access_decision,
            "access_status_code": access_status,
            "access_latency_ms": access_latency,
            "access_trace_id": access_trace,
            "access_error": access_error,
            "is_leak": graded and access_decision == "allow" and not expected_allow,
            "is_miss": access_decision == "deny" and expected_allow,
            "search_decision": search_decision,
            "search_status_code": search_status,
            "search_latency_ms": search_latency,
            "search_trace_id": search_trace,
            "returned_count": returned_count,
            "is_search_leak": graded and search_decision == "allow" and not expected_allow,
            "is_search_miss": search_decision == "deny" and expected_allow,
        }
        sink.write("probe", row)
        sink.write(
            "policy_decision",
            PolicyDecision(
                actor_agent=probe["reader_agent"],
                actor_tenant=plan.tenant_id,
                actor_fleet=probe["reader_fleet"],
                target_scope=probe["target_scope"],
                target_tenant=plan.tenant_id,
                target_fleet=plan.writer.fleet_id,
                action="read",
                decision=access_decision,
                expected_decision=expected,
                reason=(
                    f"{probe['kind']}: GET returned {access_status}"
                    if memory_id
                    else f"{probe['kind']}: no memory_id (write failed)"
                ),
            ).model_dump(mode="json"),
        )
        return row


# ---------------------------------------------------------------------- #
# Run                                                                     #
# ---------------------------------------------------------------------- #


def _revoke_reminder(plan: _Plan) -> str:
    return (
        f"agent keys for {plan.writer.agent_id} and {plan.reader.agent_id} persist "
        f"server-side — revoke them in the MemClaw dashboard"
    )


async def run(
    cfg: HarnessConfig,
    *,
    client: MemoryBackend | None = None,
) -> dict[str, Any]:
    """Execute the derived-leakage experiment end-to-end."""
    if cfg.backend != "memclaw":
        raise AgentKeyProvisioningError(
            f"{EXPERIMENT} requires MemClaw agent-scoped keys; backend={cfg.backend!r}"
        )
    run_id, run_dir, events_path = make_run_dir(cfg, EXPERIMENT)
    plan = _build_plan(cfg, nonce=run_id)
    exp = cfg.experiment or {}
    rate_gate = asyncio.Semaphore(int(exp.get("max_in_flight", 10)))
    settle_delay_ms = float(exp.get("settle_delay_ms", 0.0))
    logger.info(
        "%s run %s — chains=%d depth=%d probes=%d tenant=%s",
        EXPERIMENT, run_id, len(plan.chains), len(plan.chains[0].derived) if plan.chains else 0,
        len(plan.probes), plan.tenant_id,
    )
    provisioned: list[str] = []

    async def _execute(tenant_client: MemoryBackend) -> None:
        with open_sink_with_envelope(
            events_path,
            run_id=run_id,
            experiment=EXPERIMENT,
            cfg=cfg,
            extra={
                "tenant_ids": [plan.tenant_id],
                "agents": [
                    {"role": a.role, "agent_id": a.agent_id,
                     "fleet_id": a.fleet_id, "trust": a.trust}
                    for a in (plan.writer, plan.reader)
                ],
                "n_chains": len(plan.chains),
                "chain_depth": len(plan.chains[0].derived) if plan.chains else 0,
                "n_probes": len(plan.probes),
            },
        ) as sink:
            writer_client = await _provision(tenant_client, plan.writer, sink)
            provisioned.append(plan.writer.agent_id)
            reader_client = await _provision(tenant_client, plan.reader, sink)
            provisioned.append(plan.reader.agent_id)
            clients = {"writer": writer_client, "reader": reader_client}
            try:
                ids: dict[str, str] = {}
                for chain in plan.chains:
                    ids.update(await _seed_chain(writer_client, plan, chain, sink))

                if settle_delay_ms > 0:
                    logger.info("settling %.0fms before probes", settle_delay_ms)
                    sink.write("settle", {"delay_ms": settle_delay_ms, "phase": "post_write"})
                    await asyncio.sleep(settle_delay_ms / 1000.0)

                broken: dict[str, bool] = {}
                for chain in plan.chains:
                    v = await _verify_chain(tenant_client, plan, chain, ids, sink)
                    broken[chain.chain_id] = bool(v["chain_broken"])

                await asyncio.gather(
                    *(
                        _probe_one(
                            clients[p["reader_role"]],
                            plan,
                            p,
                            sink,
                            memory_id=ids.get(p["target_token"]),
                            chain_broken=broken.get(p["chain_id"], True),
                            rate_gate=rate_gate,
                        )
                        for p in plan.probes
                    )
                )
            finally:
                for c in clients.values():
                    await c.aclose()
            sink.write("run_end", {"run_id": run_id})

    try:
        await run_with_client(cfg, client=client, body=_execute)
    finally:
        if provisioned:
            logger.warning("REVOKE REMINDER: %s", _revoke_reminder(plan))
    result = _materialize_outputs(run_dir, events_path)
    result["tenant_ids"] = tenant_ids_for(cfg)
    result["revoke_reminder"] = _revoke_reminder(plan)
    return result


# ---------------------------------------------------------------------- #
# Outputs (always derived from the JSONL)                                 #
# ---------------------------------------------------------------------- #


def _count(df: pd.DataFrame, col: str, mask: pd.Series | None = None) -> int:
    if df.empty or col not in df.columns:
        return 0
    s = df[col] if mask is None else df.loc[mask, col]
    return int(s.fillna(False).astype(bool).sum())


def _materialize_outputs(run_dir: Path, events_path: Path) -> dict[str, Any]:
    events = read_jsonl(events_path)
    probes = events_to_frame(events, "probe")
    verifs = events_to_frame(events, "verification")
    writes = events_to_frame(events, "write")
    start = next((e for e in events if e.get("event") == "run_start"), {})
    n_chains = int((start.get("data") or {}).get("n_chains", len(verifs)))

    probes_csv = run_dir / "derived_leakage.csv"
    chains_csv = run_dir / "chains.csv"
    summary_csv = run_dir / "summary.csv"
    kind_plot = run_dir / "allow_rate_by_kind.png"
    latency_plot = run_dir / "access_latency.png"

    if not probes.empty:
        write_csv(probes, probes_csv)
    if not verifs.empty:
        write_csv(verifs.drop(columns=["verify_latencies_ms"], errors="ignore"), chains_csv)

    def kind_mask(kind: str) -> pd.Series:
        if probes.empty or "kind" not in probes.columns:
            return pd.Series(dtype=bool)
        return probes["kind"] == kind

    # --- Run validity: C must be denied on S (agent keys distinguish readers).
    if not probes.empty:
        src = probes[kind_mask("control_source")]
        n_src = len(src)
        n_src_allowed = int((src["access_decision"] == "allow").sum())
        n_src_denied = int((src["access_decision"] == "deny").sum())
    else:
        n_src = n_src_allowed = n_src_denied = 0
    run_valid = n_src_denied > 0 and n_src_allowed == 0

    # --- Chain data quality.
    n_chain_broken = _count(verifs, "chain_broken")
    chain_broken_rate = (n_chain_broken / n_chains) if n_chains else 0.0

    # --- Headline: derived leak (GET), graded probes with a decisive result.
    if not probes.empty:
        der = probes[kind_mask("derived")]
        graded = der[der["graded"].astype(bool)]
        decisive = graded[graded["access_decision"].isin(["allow", "deny"])]
        n_derived = len(der)
        n_derived_eval = len(decisive)
        n_derived_errors = int((graded["access_decision"] == "error").sum())
        n_derived_leaks = int((decisive["access_decision"] == "allow").sum())
        s_decisive = graded[graded["search_decision"].isin(["allow", "deny"])]
        n_search_eval = len(s_decisive)
        n_search_leaks = int((s_decisive["search_decision"] == "allow").sum())
        allow_mask = probes["expected_allow"].astype(bool)
        n_expected_allow = int(allow_mask.sum())
        n_expected_deny = int((~allow_mask).sum())
        n_misses = _count(probes, "is_miss")
        n_search_misses = _count(probes, "is_search_miss")
    else:
        n_derived = n_derived_eval = n_derived_errors = n_derived_leaks = 0
        n_search_eval = n_search_leaks = 0
        n_expected_allow = n_expected_deny = n_misses = n_search_misses = 0

    if run_valid and n_derived_eval:
        derived_leak_rate: float | None = n_derived_leaks / n_derived_eval
        leak_ci: tuple[float | None, float | None] = wilson_ci(n_derived_leaks, n_derived_eval)
    else:
        # Invalid run (or nothing gradeable): leave blank rather than grade.
        derived_leak_rate, leak_ci = None, (None, None)
    search_derived_leak_rate = (
        n_search_leaks / n_search_eval if run_valid and n_search_eval else None
    )
    miss_rate = (n_misses / n_expected_allow) if n_expected_allow else 0.0
    miss_ci = wilson_ci(n_misses, n_expected_allow)
    search_miss_rate = (n_search_misses / n_expected_allow) if n_expected_allow else 0.0

    access_lat = summarize_latency(probes, "access_latency_ms")
    search_lat = summarize_latency(probes, "search_latency_ms")
    if not verifs.empty and "verify_latencies_ms" in verifs.columns:
        flat = [x for lst in verifs["verify_latencies_ms"] if isinstance(lst, list) for x in lst]
        verify_lat = summarize_latency(pd.DataFrame({"v": flat}), "v")
    else:
        verify_lat = summarize_latency(pd.DataFrame(), "v")

    rows: list[dict[str, Any]] = [
        {"metric": "run_valid", "value": run_valid},
        {"metric": "n_chains", "value": n_chains},
        {"metric": "n_probes", "value": len(probes)},
        {"metric": "n_expected_allow", "value": n_expected_allow},
        {"metric": "n_expected_deny", "value": n_expected_deny},
        {"metric": "n_derived_probes", "value": n_derived},
        {"metric": "n_derived_graded", "value": n_derived_eval},
        {"metric": "n_derived_errors", "value": n_derived_errors},
        {"metric": "n_derived_leaks", "value": n_derived_leaks},
        {"metric": "derived_leak_rate", "value": derived_leak_rate},
        {"metric": "derived_leak_rate_ci95_lo", "value": leak_ci[0]},
        {"metric": "derived_leak_rate_ci95_hi", "value": leak_ci[1]},
        {"metric": "n_search_derived_leaks", "value": n_search_leaks},
        {"metric": "search_derived_leak_rate", "value": search_derived_leak_rate},
        {"metric": "n_misses", "value": n_misses},
        {"metric": "miss_rate", "value": miss_rate},
        {"metric": "miss_rate_ci95_lo", "value": miss_ci[0]},
        {"metric": "miss_rate_ci95_hi", "value": miss_ci[1]},
        {"metric": "n_search_misses", "value": n_search_misses},
        {"metric": "search_miss_rate", "value": search_miss_rate},
        {"metric": "n_source_control", "value": n_src},
        {"metric": "n_source_control_allowed", "value": n_src_allowed},
        {"metric": "n_chain_broken", "value": n_chain_broken},
        {"metric": "chain_broken_rate", "value": chain_broken_rate},
        {"metric": "n_writes", "value": len(writes)},
        {"metric": "n_writes_failed",
         "value": int((~writes["succeeded"].fillna(False).astype(bool)).sum()) if not writes.empty else 0},
    ]
    for k in ("p50", "p95"):
        rows.append({"metric": f"fetch_latency_{k}", "value": access_lat[k]})
    for k in ("p50", "p95"):
        rows.append({"metric": f"search_latency_{k}", "value": search_lat[k]})
    for k in ("p50", "p95"):
        rows.append({"metric": f"verify_latency_{k}", "value": verify_lat[k]})
    write_csv(pd.DataFrame(rows), summary_csv)

    # --- Plots.
    if not probes.empty:
        per_kind = []
        for kind in PROBE_KINDS:
            sub = probes[kind_mask(kind)]
            if kind == "derived":
                sub = sub[sub["graded"].astype(bool)]
            sub = sub[sub["access_decision"].isin(["allow", "deny"])]
            n = len(sub)
            k = int((sub["access_decision"] == "allow").sum())
            per_kind.append({"kind": kind, "allow_rate": (k / n) if n else 0.0, "k": k, "n": n})
        kdf = pd.DataFrame(per_kind)
        yerr = wilson_yerr(
            list(kdf["allow_rate"]), [wilson_ci(int(r.k), int(r.n)) for r in kdf.itertuples()]
        )
        bar_chart_from_frame(
            kdf, x="kind", y="allow_rate", out_path=kind_plot,
            title="GET allow rate by probe kind (derived should be 0)",
            ylabel="allow rate (95% Wilson)", ymax=1.15, yerr=yerr,
            label_fmt=lambda r: f"{int(r['k'])}/{int(r['n'])}",
        )
        plot_latency_hist(probes, col="access_latency_ms", out_path=latency_plot,
                          title="Access latency (derived-leakage probes)")

    return {
        "run_dir": str(run_dir),
        "events": str(events_path),
        "probes_csv": str(probes_csv),
        "chains_csv": str(chains_csv),
        "summary_csv": str(summary_csv),
        "run_valid": run_valid,
        "n_chains": n_chains,
        "n_probes": len(probes),
        "n_derived_graded": n_derived_eval,
        "n_derived_leaks": n_derived_leaks,
        "derived_leak_rate": derived_leak_rate,
        "derived_leak_rate_ci95": list(leak_ci),
        "search_derived_leak_rate": search_derived_leak_rate,
        "n_misses": n_misses,
        "miss_rate": miss_rate,
        "miss_rate_ci95": list(miss_ci),
        "search_miss_rate": search_miss_rate,
        "n_chain_broken": n_chain_broken,
        "chain_broken_rate": chain_broken_rate,
        "n_source_control_allowed": n_src_allowed,
        "fetch_latency": access_lat,
        "search_latency": search_lat,
        "verify_latency": verify_lat,
    }

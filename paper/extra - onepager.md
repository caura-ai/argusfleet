# Governed Shared Memory for Multi-Agent LLM Systems

**Yanki Margalit** (Caura.ai) · **Nurit Cohen-Inger** (Ben-Gurion University of the Negev)

## The problem

AI memory systems were built around one user, one conversation, one context
window. Production deployments now run **fleets of agents sharing persistent
state** — and the failure modes are no longer about retrieval quality.
They're about governance, synchronization, temporal correctness, provenance,
and access control. *AI memory is shifting from a context-window problem to
a distributed-systems problem.*

## What we built

- **A four-failure-mode taxonomy:** unauthorized leakage, stale propagation,
  contradiction persistence, provenance collapse.
- **An architecture** centered on five primitives: scoped retrieval,
  temporal supersession, provenance graphs, policy-governed propagation,
  persistent shared state.
- **MemClaw** — reference implementation deployed at `memclaw.net`.
- **ArgusFleet** — open-source, async, JSONL-traced evaluation harness
  with four experiments mapped 1:1 to the failure modes, reporting five
  metric axes (four governance properties + retrieval recall as an
  orthogonal availability control). 48 tests; deterministic, reproducible,
  salted per-run. Every number below derives from a committed `events.jsonl`
  trace (`traces/`, commit `2c55bb5`). Implementation: `argusfleet` package
  at `github.com/caura-ai/argusfleet`.

## Headline empirical results (N=200 trials/experiment, clean tenant)

| Primitive | Result | Interpretation |
|---|---|---|
| **Leakage — access** | **164 / 164** GET-by-id retrievals across fleet boundaries (Wilson CI [0.977, 1.000]) | Tenant scope is enforced; **sub-tenant scope is not enforced on the GET path — a genuine enforcement gap, not an auth-model artifact.** The agent-scoped probe shows the gateway *resolves* the caller's agent identity and the handler then *discards* it (a trust-1 agent reads a cross-fleet row the ladder should deny). |
| **Leakage — search** | **0.439** cross-fleet leak rate (72/164); **0.143** miss rate (4/28) | Search partially honours the `fleet_ids` filter; embedding-similarity ranking introduces recall noise. |
| **Provenance** | **1.00** chain completeness at depth 4 (50/50); **291 / 491 / 1076 ms** p50/p95/p99 per-hop fetch; **1.00** writer-identity accuracy | Walking `metadata.derived_from` is a viable primitive at interactive latency; identity is preserved through the storage round-trip. |
| **Cross-fleet propagation** | **0.975** intra-fleet visibility (117/120); **0%** cross-fleet leak (0/80); **0.83 s** p50 write→visible window (tight per-fact polling, max 1.97 s) | Visibility is **effectively immediate** under strong write mode — enrichment is synchronous. |
| **Contradiction** | **90 / 90 = 1.00** supersession when both writes are admitted; **0.49** overall (206/400 writes dedup-rejected) | The detector is **sound**; a synchronous dedup gate pre-empts it — a pipeline-ordering issue, not a detector failure. |

## Four architectural findings about production memory infrastructure

1. **Scope enforcement is bimodal — and the GET gap is real.** Tenant
   boundaries are enforced uniformly; within-tenant (fleet/agent) scope is
   enforced only partially on the search path and **not at all on direct
   `GET`-by-id**. This is *not* an authentication-model limitation: the
   `get_memory` handler checks only the tenant predicate and discards the
   resolved agent identity, so even an agent-scoped credential reads a
   peer fleet's row at a trust level the documented ladder should deny.
   *In practice: any agent within a tenant who learns a peer's `memory_id`
   can read the row — including another agent's agent-scoped secret.*
2. **Deduplication starves contradiction detection.** A semantic
   near-duplicate gate rejects writes by embedding similarity *synchronously,
   pre-commit*, before the structural contradiction detector runs
   *post-commit, asynchronously*. Contradictory writes ("X is A" / "X is B")
   are near-identical text, so the very writes the detector exists for are
   the ones most likely to be 409'd. When both writes *do* land, supersession
   is correct 90/90 — the primitive works; the **ordering** defeats it.
3. **Cross-agent propagation is privilege-gated.** `POST /memories/redistribute`
   requires `trust_level ≥ 3`, conflating *fleet-internal propagation*
   (a normal multi-agent operation) with *administrative migration*
   (a privileged one). The propagation primitive needs a finer-grained
   policy surface.
4. **The consistency cost is paid at write time, not read time.** Measured
   with tight per-fact polling at low load, the write→visible window under
   strong mode is effectively zero (p50 ≈ 0.8 s — one search round-trip):
   enrichment is synchronous, so the cost lands in write latency (heavy
   tail, p99 ≈ 19 s under sustained contention), not a read-visibility lag.
   A naive batch-then-drain measurement reports a window of tens of seconds
   that *grows with probe count* — a probe-scheduling artifact, not server
   behaviour. Quantifying a service's true window requires polling each
   write immediately and continuously, decoupled from the rate workload.

## Contributions

1. Formalization of the fleet-memory problem.
2. Failure-mode taxonomy.
3. Architectural primitives for governed shared memory.
4. MemClaw reference implementation.
5. ArgusFleet evaluation harness (reproducible, open, trace-committed).
6. Empirical findings against a live production service, including the
   four architectural observations above.

## Limitations

- **Single tenant.** Cross-tenant leakage not measurable in this
  configuration; the cross-tenant boundary is presumed enforced by the
  gateway but unverified empirically.
- **N=200** trials per experiment. The Wilson upper bound on the
  propagation cross-fleet leak (0/80) is ≈ 0.046; stronger claims would
  benefit from a further order of magnitude more probes, constructed
  adversarially rather than via deterministic plan generation.
- **Low-load window.** Time-to-visibility is measured on 8 facts at 250 ms
  granularity — it resolves "effectively immediate" but not a sub-250 ms
  value, and the write-latency tail (p99 ≈ 19 s) is the load-dependent cost
  the uncontended window phase does not see.
- **Self-evaluation.** ArgusFleet and MemClaw are built by the same group;
  mitigated by foregrounding the negative results and committing every
  per-write trace for independent replay.
- **No baseline comparand.** The architectural claims stand on the
  failure-mode taxonomy and the implementation, but the empirical claims
  would be sharper with a long-context-only or alternative-memory-system
  comparison.

## So what

For builders: production "governed memory" systems behave like distributed
databases — the placement of enrichment (write-path vs read-path), pipeline
ordering, identity preservation, and the gap between *authenticating* a
caller and *applying its scope* all matter, and they are measurable.
ArgusFleet lets you measure them, with the traces committed so the numbers
are checkable rather than asserted. For systems researchers: there's a
publishable agenda at the intersection of distributed systems and LLM
infrastructure, beginning with the four architectural findings above.

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
  orthogonal availability control). 33 tests; deterministic, reproducible,
  salted per-run. Implementation: `agentfleetmem` package at
  `github.com/caura-ai/agentfleetmem`.

## Headline empirical results (N=100 trials/experiment, clean tenant)

| Primitive | Result | Interpretation |
|---|---|---|
| **Leakage — access** | **80 / 80** GET-by-id retrievals across fleet boundaries (Wilson CI [0.954, 1.000]) | Tenant scope is enforced; **sub-tenant scope is not enforced on the GET path** — a function of the tenant-scoped authentication model, not a server defect. |
| **Leakage — search** | **0.45** cross-fleet leak rate, **0.25** miss rate | Search partially honours the `fleet_ids` filter; embedding-similarity ranking introduces recall noise. |
| **Provenance** | **0.56** chain completeness at depth 4; **290 ms** p50 / **333 ms** p95 per-hop fetch; **1.00** writer-identity accuracy | Walking `metadata.derived_from` is a viable architectural primitive at interactive latency. |
| **Cross-fleet propagation** | **100%** intra-fleet visibility (60/60); **0%** cross-fleet leak (0/40); **83 s** p50 / **130 s** p95 time-to-visibility | Eventually-consistent on a ~80-second window for fleet-scope writes. |
| **Contradiction detection** | **0 / 100** (74/200 writes succeeded; 126 rejected as near-duplicates) | Architectural finding — dedup pre-empts detection. |

## Three architectural findings about production memory infrastructure

1. **Scope enforcement is bimodal.** Tenant boundaries are enforced
   uniformly; within-tenant scope is enforced only on the search path,
   not on direct `GET`-by-id. This is a function of the authentication
   model: tenant-scoped API keys carry no agent identity, so the
   gateway has no signal on which to apply fleet/agent filtering when
   the request is a path-parameter ID. *In practice: any agent within
   a tenant who learns a peer's `memory_id` can read the row directly.*
2. **Deduplication precedes contradiction detection.** A semantic
   near-duplicate gate rejects writes by embedding similarity *before*
   the structural contradiction detector runs — confirmed on a clean
   tenant where no historical near-duplicates exist. Contradictory
   writes are by construction lexically similar to their counterparts,
   so the primitive can never fire under the current ordering.
3. **Cross-agent propagation is privilege-gated.** `POST /memories/redistribute`
   requires `trust_level ≥ 3`, conflating *fleet-internal propagation*
   (a normal multi-agent operation) with *administrative migration*
   (a privileged one). The propagation primitive needs a finer-grained
   policy surface.

## Contributions

1. Formalization of the fleet-memory problem.
2. Failure-mode taxonomy.
3. Architectural primitives for governed shared memory.
4. MemClaw reference implementation.
5. ArgusFleet evaluation harness (reproducible, open).
6. Empirical findings against a live production service, including
   three paper-worthy architectural observations.

## Limitations

- **Single tenant.** Cross-tenant leakage not measurable in this
  configuration; the cross-tenant boundary is presumed enforced by the
  gateway but unverified empirically.
- **Tenant-scoped authentication.** Sub-tenant GET enforcement is
  structurally untestable without agent-scoped (`mca_`) keys; the
  measurement is bounded by the authentication model, not the
  governance primitive.
- **N=100** trials per experiment. Wilson upper bound on the
  propagation cross-fleet leak (0/40) is ≈ 0.09; stronger claims
  would benefit from an order of magnitude more probes.
- **No baseline comparand.** The architectural claims stand on the
  failure-mode taxonomy and the implementation, but the empirical
  claims would be sharper with a long-context-only or alternative-
  memory-system comparison.

## So what

For builders: production "governed memory" systems behave like distributed
databases — consistency windows, identity preservation, pipeline ordering,
and the interaction between authentication models and scope enforcement
all matter. Frameworks like ArgusFleet let you measure them. For systems
researchers: there's a publishable agenda here at the intersection of
distributed systems and LLM infrastructure, beginning with the three
architectural findings above.

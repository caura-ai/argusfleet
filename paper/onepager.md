# Governed Shared Memory for Multi-Agent LLM Systems

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
- **AgentFleetMem** — open-source, async, JSONL-traced evaluation harness
  with four experiments mapped 1:1 to the failure modes. 33 tests;
  deterministic, reproducible, salted per-run for stateful servers.

## Headline empirical results (N=100 trials/experiment, single tenant)

| Primitive | Result | Interpretation |
|---|---|---|
| **Scope enforcement (leakage)** | **0 / 80** unauthorized retrievals (95% Wilson CI [0, 0.045]) | Security envelope holds under tested boundary conditions. |
| **Provenance** | **0.64** chain completeness at depth 4; **307 ms** p50 per fetch | Walking `metadata.derived_from` is viable at interactive latency. |
| **Cross-fleet propagation** | **100%** intra-fleet visibility; **0%** cross-fleet leak; **38 s** p50 time-to-visibility | Eventually-consistent, with a measurable ~40 s window. |
| **Contradiction detection** | **0 / 100** (writes 409'd before detector could fire) | Architectural finding — see below. |

## Two architectural findings about production memory infrastructure

1. **Deduplication precedes contradiction detection.** A semantic
   near-duplicate gate rejects writes by embedding similarity *before* the
   structural contradiction detector runs. Since contradictory writes are by
   construction lexically similar to their counterparts, the primitive can
   never fire. We argue the pipeline ordering is wrong: structural triples
   should be evaluated first when present.
2. **Cross-agent propagation is privilege-gated.** `POST /memories/redistribute`
   requires `trust_level ≥ 3`, conflating *fleet-internal propagation*
   (a normal multi-agent operation) with *administrative migration*
   (a privileged one). The propagation primitive needs a finer-grained
   policy surface.

## Contributions

1. Formalization of the fleet-memory problem.
2. Failure-mode taxonomy.
3. Architectural primitives for governed shared memory.
4. MemClaw reference implementation.
5. AgentFleetMem evaluation harness (reproducible, open).
6. Empirical findings against a live production service, with two
   paper-worthy architectural observations.

## Limitations

- Single tenant (cross-tenant claims not yet measurable).
- N=100 per experiment is small; `0/80` carries non-trivial upper
  confidence bound.
- Accumulated server state across runs influences write-success metrics.
- No baseline comparand (no "memory-off" or alternative-system condition).

## So what

For builders: production "governed memory" systems behave like distributed
databases — consistency windows, identity preservation, pipeline ordering,
and propagation privileges all matter. Frameworks like AgentFleetMem let
you measure them. For systems researchers: there's a publishable agenda
here at the intersection of distributed systems and LLM infrastructure —
beginning with the two architectural findings above.

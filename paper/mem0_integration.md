# Cross-Platform Addendum: Mem0 — data & text for integration

**Purpose.** This file supplies all *new* material (a second evaluated
backend, Mem0, run through the same ArgusFleet harness) for the paper
owner to integrate into *Governed Shared Memory for Multi-Agent LLM
Systems*. **It is not a rewrite of the paper.** Everything below is
written in the submitted AAAI version's terminology — the evaluated
service is **Fleet-Memory**, the harness is **ArgusFleet**, the four
invariants are **Inv-Scope / Inv-Time / Inv-Lineage / Inv-Prop**, and
the headline results table is **Table 4**. Where a number for
Fleet-Memory is quoted, it is copied from the current paper for
side-by-side context only.

This addendum directly answers the paper's **"Scope of evidence"**
limitation ("This study evaluates one author-built service in one
tenant… It does not… establish that the measurements generalize to all
memory systems"): running the identical probes against a second,
independent production platform tests exactly that generalization.

- **Status of the data.** All Mem0 measurements were collected live
  against `api.mem0.ai` on 2026-07-13. Two experiments (scope/leakage,
  lineage) ran on the free tier; the account was then upgraded to
  Starter (5,000 retrievals/mo) to complete the two retrieval-heavy
  experiments (contradiction, propagation). Traces are committed
  alongside the Fleet-Memory traces under `outputs/runs/…-mem0-seed42/`.
- **Reproduction:** `python -m argusfleet run all --backend mem0 --seed 42`
  (see §7 below for per-experiment commands and quota notes).

---

## 1. One-paragraph summary (for the abstract / intro, optional)

> To test whether these findings reflect one implementation or the
> memory-service design space, we run the identical ArgusFleet probes
> against a second production platform (Mem0) through a backend-adapter
> interface. On the two invariants Mem0 can express, the two services
> fail in the *same direction*: identifier-based retrieval applies no
> sub-key authorization (on Mem0 every `GET`-by-id succeeds, including
> across the `user_id` partition), while filtered search only partially
> constrains discovery. The invariants Mem0 does not implement
> (structural supersession, entity-based lineage, governed propagation)
> are made explicit through a capability matrix rather than scored as
> failures. This turns the single-service case study into a two-platform
> comparison and supports the claim that the four invariants are
> properties of governed shared memory in general.

---

## 2. New methodology text (add to §6 Reference Implementation)

### 2.1 Backend adapter (new subsection under §6, or a paragraph in §6.2 ArgusFleet)

> **Backend adapter.** ArgusFleet consumes the memory service through a
> seven-operation interface — write, search, identifier-based retrieval,
> update, entity upsert, redistribution, and bulk delete — expressed as a
> `MemoryBackend` protocol. A concrete backend normalizes its platform's
> wire format onto this interface and declares a `BackendCapabilities`
> record stating which governance primitives it natively exposes (scoped
> identifier retrieval, per-row visibility scopes, first-class entities,
> structural contradiction detection, an ownership-transfer primitive,
> and status-filtered search). Each experiment branches on these flags to
> select a measurement strategy or to record an axis as *unsupported*
> rather than failing; an unsupported primitive is a property of the
> platform, not a gap in the method. The default backend is Fleet-Memory;
> we add a second, Mem0, and select the backend per run. The experiment
> code, seeds, probe geometry, and metric definitions are identical
> across backends, so any difference in the reported measurements is
> attributable to the service rather than the harness.

### 2.2 Mem0 backend (new subsection under §6)

> **Mem0.** Mem0 (Chhikara et al. 2025) is a managed memory platform
> built around LLM-mediated memory extraction. Its governance model
> differs from Fleet-Memory's in ways that are themselves the object of
> measurement. The enforced trust boundary is the organization/project
> bound to the API key; below it, each memory carries a free-form
> `user_id` and an optional `run_id`, which act as caller-chosen query
> filters rather than server-enforced scopes (an `agent_id` supplied
> alongside `user_id` is not persisted as a filterable field). We
> therefore map tenant → `user_id`, fleet → `run_id`, and record the
> writing agent in metadata. Identifier-based retrieval
> (`GET /v1/memories/{id}`) accepts no caller identity. Writes are either
> verbatim (`infer=false`, synchronous, returns the row id) or
> LLM-extracted (`infer=true`, queued asynchronously); Mem0 exposes no
> RDF entity primitive, no structural supersession edge or lifecycle
> status, and no ownership-transfer endpoint. Conflicts are handled
> implicitly by the extraction pipeline, and derivation links ride in
> metadata exactly as on Fleet-Memory. These correspondences are
> summarized in Table A (capability matrix).

### 2.3 Trace/quota note (add to §6.2 "Trace model and reproducibility boundary")

> Mem0 meters retrieval operations (search and get-all) against a monthly
> quota and a burst limit; the adapter spaces reads and bounds polling so
> a run stays within tier limits, and the reported Mem0 measurements were
> collected under the Starter tier. As with Fleet-Memory, the committed
> traces are the immutable evidence boundary.

---

## 3. Capability matrix (new table — suggested label Table A / renumber)

**Caption:** Governance-primitive capability matrix. A check denotes a
natively exposed primitive; a dash denotes one the platform does not
provide, which the harness records as *unsupported* on the corresponding
axis rather than as a failure.

| Governance primitive | Fleet-Memory | Mem0 |
|---|:---:|:---:|
| Project/tenant-key isolation | ✓ | ✓ |
| Caller-chosen sub-key filters (search) | ✓ | ✓ |
| Scoped identifier-based retrieval (`GET`-by-id) | ✓ (after remediation) | — |
| Per-row visibility scopes (agent/fleet/tenant) | ✓ | — |
| First-class entities (RDF subject) | ✓ | — |
| Structural contradiction detection / supersession edge | ✓ | — |
| Ownership transfer / redistribution | ✓ | — |
| Status-filtered ("active") search | ✓ | — |
| Synchronous "strong" (enriched) writes | ✓ | — |

*LaTeX version of this table is in `mem0_integration_snippets.tex`.*

---

## 4. Results — Mem0 (parallel to Table 4)

Headline Mem0 measurements, from the same experiments and seeds as the
Fleet-Memory runs. "n/a (unsupported)" marks an axis whose primitive
Mem0 does not expose (Table A). Bracketed values are descriptive
probe-level Wilson 95% intervals, with the same clustering caveat the
paper already states for Table 4.

| Experiment | Metric | Mem0 value | Fleet-Memory (for reference) |
|---|---|---|---|
| **Scope** | tenant-key cross-scope `GET`-by-id reachability | **712/712 = 1.000** [0.995, 1.000] | 164/164 = 1.000 |
| | &nbsp;&nbsp;of which cross-`user_id` reads | **384/384 = 1.000** | (n/a — single tenant) |
| | search scope-filter violations | 328/712 = **0.461** | 72/164 = 0.439 |
| | authorized-retrieval misses | 0/56 = **0.000** | 4/28 = 0.143 |
| | focused agent `GET` policy enforcement | n/a (no scoped-get primitive) | deny/allow after remediation |
| **Lineage** | supplied depth-4 chain completeness & writer-identity | **50/50 = 1.000** [0.929, 1.000] | 50/50 = 1.000 |
| | per-hop fetch latency p50 / p95 / p99 | **476 / 563 / 1,199 ms** | 291 / 491 / 1,076 ms |
| **Propagation** | fleet-sibling visibility (run_id filter) | **120/120 = 1.000** | 117/120 = 0.975 |
| | foreign-fleet hits | **0/80 = 0.000** | 0/80 = 0.000 |
| | first-poll visibility (n=8) median / max | **8/8; 0.53 / 0.56 s** | 8/8; 0.83 / 1.97 s |
| | ownership transfer / redistribution | n/a (unsupported; 20 skipped) | privilege-gated |
| **Contradiction** | contradiction *persistence* (both values coexist) | **26/60 = 0.433** | 0/200 stale (supersedes) |
| | resolution (single value survives) | **0/60 = 0.000** | 90/90 = 1.000 conditional |
| | structural supersession | n/a (unsupported) | 90/90 = 1.000 conditional |
| | admitted-write latency p50 / p95 | **620 / 1,172 ms** | 1,840 / 4,861 ms |

### 4.1 Scope (Inv-Scope) — the enforcement boundary is the project key

> On the authoritative identifier-based path, Mem0 returned every
> requested row (712/712), **including 384/384 reads that crossed the
> `user_id` partition**: a project-key holder who possesses a memory
> identifier reads the corresponding row regardless of which `user_id`
> wrote it. This is consistent with Mem0's documented model, in which the
> API key is the trust boundary — but it means the common deployment
> pattern of separating end-users by `user_id` under one project key
> provides no isolation on identifier-based retrieval. The result mirrors
> Fleet-Memory's initial `GET`-path behavior, with the difference that on
> Fleet-Memory it was a localized handler defect that was remediated,
> whereas on Mem0 it is the platform's intended scoping model. On the
> discovery path, Mem0's `user_id` filter behaves much like
> Fleet-Memory's sub-tenant filter: 46.1% (328/712) of expected-deny
> probes still surfaced the row via same-`user_id` cross-sub-scope search
> (Fleet-Memory: 43.9%), while cross-`user_id` search was correctly
> filtered. Mem0's authorized-retrieval miss rate was 0/56, against
> Fleet-Memory's 0.143. The same access-path asymmetry the paper
> identifies for Fleet-Memory — permissive identifier retrieval,
> partially-filtered search — therefore appears on an independent
> platform, evidence that it is a design-space property rather than a
> single-codebase defect.

*Note for the owner:* Mem0's leakage run used **two** `user_id`s
(4 fleets × 6 agents each), giving 712 expected-deny / 56 expected-allow
probes; Fleet-Memory used one tenant (164 / 28). The cross-`user_id`
subset (384) is the strongest single line — it shows the partition users
would rely on for isolation is not enforced on `GET`-by-id.

### 4.2 Lineage (Inv-Lineage) — metadata chains transfer cleanly

> All 50 supplied depth-four lineage chains reconstructed completely on
> Mem0, with the stored writer identity matching the planned writer at
> every hop (completeness, accuracy, and depth-fidelity each 1.000) —
> identical to Fleet-Memory. Mem0 preserves caller-supplied
> `metadata.derived_from` and per-row metadata verbatim on `infer=false`
> writes, and identifier-based retrieval returns them, so the bounded
> lineage-reconstruction property holds on both platforms. Per-hop fetch
> latency was p50 476 ms / p95 563 ms. As with Fleet-Memory, this
> demonstrates persistence and reconstruction of supplied lineage
> metadata, not semantic truth, unforgeability, or automatic capture.

### 4.3 Propagation (Inv-Prop) — visibility holds for cooperating readers; isolation is a filter, not enforcement

> On the propagation axes Mem0 can express through `run_id`, it performed
> well for cooperating callers: all 120 fleet-sibling probes observed the
> fact (visibility 1.000) and no foreign-fleet reader saw it across 80
> probes (0/80), with a write-to-visible window of ~0.53 s (median, all 8
> window facts visible on the first poll under verbatim `infer=false`
> writes). These match or slightly exceed Fleet-Memory's numbers
> (0.975 / 0/80). **The critical qualification is that Mem0's cross-fleet
> "isolation" is a query filter, not access control:** the 0/80 result
> holds because the harness's foreign readers query with a *different*
> `run_id`; a reader that omits the `run_id` filter, or supplies the
> writer's, is not prevented from seeing the row (this is the same
> permissive behavior the scope experiment measures directly). Ownership
> transfer / redistribution has no Mem0 primitive, so all 20 planned
> re-homings are recorded as `ownership_transfer_skipped` — Mem0 exposes
> no governed-propagation operation to gate. Fleet-Memory, by contrast,
> enforces fleet scope on the read path and gates re-homing behind an
> elevated trust level. The comparison is therefore: *equivalent
> visibility for honest callers, materially different enforcement.*

### 4.4 Contradiction (Inv-Time) — no supersession; contradictions persist

> Mem0 has no structural supersession primitive, so ArgusFleet measures
> the platform's de-facto behavior: after both contradictory writes
> settle, it enumerates the rows carrying the fact (via an exact
> metadata-filtered get-all, no embedding ranking) and reports whether
> the conflicting object values still coexist. In **26 of 60** fact-runs
> both values remained simultaneously retrievable — an Inv-Time exposure
> — and in **0 of 60** did the platform resolve the pair to a single
> surviving value with a supersession relation. (In the remaining 29
> fact-runs the two writes were not separately retained under the shared
> fact identifier: Mem0's LLM `ADD`/`UPDATE`/`DELETE` pipeline merged,
> dropped, or declined to tag one of them — a nondeterministic-retention
> effect that is itself a governance-relevant finding, and which we note
> as a measurement caveat rather than counting as resolution.) This
> contrasts with Fleet-Memory, whose structural detector superseded the
> stale row in every admitted pair (90/90) and left no fact-run serving
> two active values. Admitted-write latency on Mem0 was p50 620 ms /
> p95 1,172 ms.
>
> *Method note for the owner:* on a content-driven store, "resolution by
> deletion" is indistinguishable from "the second write was never
> admitted" — both leave one value. We therefore lead with the
> unambiguous **persistence** rate (both values coexist = 26/60) rather
> than reporting a single resolver-accuracy number, mirroring the paper's
> existing caution around the Fleet-Memory conditional rate.

---

## 5. Findings text (add to §9)

> **The scope asymmetry generalizes across platforms.** The access-path
> inconsistency identified for Fleet-Memory — identifier-based retrieval
> enforcing less policy than search — recurs on Mem0, where `GET`-by-id
> applies no sub-key authorization at all, including across `user_id`.
> Because the same probe finds the same class of gap on an independent
> service, the underlying claim ("a memory identifier must not function
> as an implicit capability; every path that returns a row must evaluate
> the same authenticated policy") is a property of the design space, not
> of one implementation.

> **Capability differences are findings, not gaps.** Mem0 does not
> implement structural supersession, entity-based lineage, or governed
> propagation. Rather than scoring these as zeros, the capability matrix
> (Table A) records them as unsupported and the affected experiments
> report the platform's de-facto behavior (e.g., whether contradictory
> values coexist). This keeps a cross-platform comparison honest when the
> platforms expose different primitives.

---

## 6. Suggested edits to existing text

- **"Scope of evidence" limitation (§9):** soften from "evaluates one
  author-built service in one tenant" to note the second, independent
  platform now exercised on the two shared invariants, while keeping the
  caveat that a full cross-platform benchmark (more services; a
  long-context-only baseline) remains future work.
- **Contributions (§1):** optional 4th bullet — "We generalize the
  harness across platforms via a backend-adapter interface and a
  capability matrix, and report a two-platform comparison (Fleet-Memory
  and Mem0) showing the scope and discovery invariants fail in the same
  direction on both."
- **Related work (§2):** Mem0 is already cited (Chhikara et al. 2025);
  it now also appears as an evaluated backend.

---

## 7. Reproduction & quota notes (for supplementary material)

```bash
# Whole suite on Mem0 (set MEM0_API_KEY in .env):
python -m argusfleet run all --backend mem0 --seed 42

# Per experiment:
python -m argusfleet run leakage      --backend mem0 --seed 42   # ~768 retrievals
python -m argusfleet run provenance   --backend mem0 --seed 42   # ~0 retrievals (GET-by-id)
python -m argusfleet run contradiction --backend mem0 --seed 42  # ~ see config
python -m argusfleet run propagation  --backend mem0 --seed 42   # ~230 retrievals
```

- Backend selection: `--backend {memclaw|mem0}`, or `ARGUSFLEET_BACKEND`.
  Mem0 credentials: `MEM0_API_KEY` (and optional `MEM0_USER_IDS`).
- Mem0-specific configs live in `configs/mem0/*.yaml`; they set write
  mode, read throttling (`mem0_min_read_interval_ms`), and marker-poll
  bounds so runs stay within tier limits.
- **Retrieval quota matters.** Search and get-all count against Mem0's
  monthly retrieval quota; the free (Hobby) tier's 1,000/mo is exhausted
  by a single leakage run. The reported runs used the Starter tier
  (5,000/mo). GET-by-id does not appear to count against the retrieval
  quota (the lineage experiment's 200 fetches ran on the free tier).

---

## 8. Files added/changed in the harness (for the artifact appendix)

- `harness/backend.py` — `MemoryBackend` protocol, `BackendResponse`,
  `BackendError`, `BackendCapabilities`, `open_backend()` factory.
- `harness/mem0_client.py` — Mem0 backend (7-op surface, marker-poll for
  `infer=true`, read rate limiter, `list_memories` enumeration).
- `harness/client.py` — Fleet-Memory client retrofitted onto the shared
  envelopes (no behavior change).
- `harness/config.py`, `cli.py` — `--backend` flag, Mem0 env vars,
  per-backend config resolution.
- `experiments/contradiction.py`, `experiments/propagation.py` —
  capability-aware branches (content-based contradiction adjudication;
  redistribution recorded as unsupported).
- `harness/report.py` — report keyed on (backend, experiment).
- `configs/mem0/*.yaml` — Mem0 experiment configs.
- `tests/test_mem0_client.py`, `tests/test_contradiction_mem0.py` — new
  tests (full suite: 59 passing).
```
```

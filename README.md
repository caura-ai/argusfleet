# AgentFleetMem

Research harness for **governed shared memory in multi-agent LLM systems**, the
systems primitive evaluated in the companion arXiv paper *"Governed Shared
Memory for Multi-Agent LLM Systems."*

The harness is a **systems evaluation suite**, not a vendor benchmark. It
exercises a deployed memory service (`memclaw.net` by default) against
synthetic multi-agent workloads and measures architectural correctness —
governance, scoped retrieval, temporal contradiction handling, provenance,
cross-agent propagation, and long-context-vs-memory tradeoffs.

## Status

First milestone — scaffold + leakage experiment. Other experiments are
stubbed in the CLI and land in the next milestone.

| Experiment              | State        |
| ----------------------- | ------------ |
| Leakage                 | implemented  |
| Temporal contradiction  | implemented  |
| Provenance              | implemented  |
| Cross-agent propagation | implemented  |
| Long-context vs memory  | stubbed      |
| Scale                   | stubbed      |

## Layout

```
agentfleetmem/
├── __init__.py              package
├── __main__.py              python -m agentfleetmem
├── cli.py                   Typer commands
├── configs/                 experiment YAML configs
├── experiments/             one module per experiment
│   └── leakage.py
├── harness/                 shared primitives
│   ├── client.py            async REST client for memclaw.net
│   ├── config.py            HarnessConfig + .env loader
│   ├── logging.py           JSONL event sink
│   ├── metrics.py           JSONL → DataFrame → CSV
│   ├── models.py            domain models (Memory, Agent, ...)
│   └── plots.py             matplotlib helpers
├── outputs/                 per-run artifacts (CSV, JSONL, PNG)
├── scripts/                 one-off operational scripts
└── tests/                   pytest suite (shared fixtures at root)
```

## Install

Requires Python **3.12+**.

```bash
cd agentfleetmem
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS / Linux:
source .venv/bin/activate

pip install -r requirements.txt
pip install -e .

cp .env.example .env
# edit .env and set MEMCLAW_API_KEY (and optionally MEMCLAW_TENANT_ID)
```

`requirements.txt` ships pinned versions for reproducibility. `pip install -e .`
installs the `agentfleetmem` package itself so `python -m agentfleetmem` works.

## Run the leakage experiment

```bash
python -m agentfleetmem run leakage
```

With overrides:

```bash
python -m agentfleetmem run leakage --seed 1337 --label baseline -v
```

Outputs land under `outputs/runs/<UTC-timestamp>-<label>-seed<N>/`:

- `events.jsonl` — every write, probe, and policy decision (one JSON per line)
- `leakage.csv` — one row per probe attempt with `is_violation`
- `summary.csv` — aggregated rates + p50/p95/p99 latency
- `leakage_by_axis.png` — leakage rate by scope (bar chart)
- `search_latency.png` — search latency histogram

## Methodology notes

### Clean-tenant runs

The leakage experiment uses two independent signals per probe:

- **Access** (authoritative): `GET /memories/{id}?tenant_id=<probe-tenant>`.
  The server returns 200 iff the row is visible to the probing tenant.
  Drives the headline `leak_rate` and `miss_rate` metrics.
- **Discovery** (secondary): semantic search by the secret's prose, with
  metadata matching on the canary. Drives the secondary `search_leak_rate`
  and `search_miss_rate` metrics. These are sensitive to embedding ranking
  and accumulated tenant state; they are *informational*, not the security
  envelope.

For credible numbers, run against a **clean tenant** — one with no prior
AgentFleetMem state in the embedding index. Accumulated state from
previous runs can shift the *discovery* rates substantially (we observed
0.52 → 0.83 provenance completeness improvement from controlled prose
diversification alone). The *access* rates are insensitive to this
because GET-by-id doesn't depend on embedding ranking.

If you must run against a tenant with accumulated state:

- Trust the **`leak_rate` / `miss_rate`** metrics (access-based).
- Treat the **`search_leak_rate` / `search_miss_rate`** metrics as
  noisy lower-bound estimates.
- Include the **95% Wilson CI** columns (`leak_rate_ci95_lo`,
  `leak_rate_ci95_hi`) when citing rates — `0/80` has a non-trivial
  upper bound around 4.5%.

### Multi-tenant configuration

For cross-tenant leakage testing the harness needs access to ≥2 tenant
IDs. Configure with `MEMCLAW_TENANT_ID=mc_a,mc_b` (comma-separated).
With a single tenant the leakage experiment only exercises within-tenant
scope enforcement; cross-tenant boundaries are not measurable.

## What the leakage experiment does

1. Generates a deterministic synthetic fleet: N tenants × M fleets × K
   agents per fleet. Per-tenant IDs are seeded from `cfg.seed` so two runs
   with the same seed are byte-identical at the request layer.
2. Each fleet owner writes one `fleet`-scoped and one `agent`-scoped
   memory containing a unique **canary token**.
3. Every agent probes every canary with a direct-token search filtered to
   its own `agent_id`. The harness classifies each probe against ground
   truth (does this probe's `tenant/fleet/agent` entitle it to see this
   canary?) and labels the result `allow`, `deny`, or `error`.
4. Disagreements between observed and expected decisions are counted as
   `is_violation` — those are the leak events the paper reports on.

The implementation calls only the public REST surface:
`POST /api/v1/memories`, `POST /api/v1/search`. The `X-API-Key` header
carries auth. `top_k` is clamped to 20 (the public REST cap) client-side.

## Run the contradiction experiment

```bash
python -m agentfleetmem run contradiction
```

Generates ``sequential`` and ``concurrent`` writes for the same fact
across two agents and measures three things:

- **Detection rate** — fraction of fact-runs where the server returned
  ``supersedes_id`` on the second write.
- **Stale-read rate** — fraction of fact-runs where a fresh
  ``status_filter="active"`` search still returned more than one
  conflicting row.
- **Write-latency overhead** — write-latency percentiles split by
  ``had_contradiction``, so the cost of detection can be reported
  separately from the steady-state path.

Outputs land under `outputs/runs/<UTC-timestamp>-contradiction-seed<N>/`:

- `events.jsonl` — every write with its `supersedes_id` + resolution check
- `contradictions.csv` — one row per (fact, scenario)
- `summary.csv` — detection / stale-read rates + latency percentiles
- `detection_rate.png` — sequential vs concurrent detection rate
- `write_latency.png` — write latency histogram (all writes)

The contradiction config defaults to `write_mode: strong` so the full
enrichment pipeline runs on every write — without it the detector may be
skipped and detection rate becomes a function of write_mode, not the
underlying governance behaviour.

## Run the provenance experiment

```bash
python -m agentfleetmem run provenance
```

Builds N derivation chains of depth K — agent_0 writes a root fact,
agent_1 derives from it, agent_2 from that, etc. Each derivation writes
`metadata.derived_from = [previous_memory_id]`. After all writes settle,
the harness fetches every leaf and walks the chain back to the root.

For each chain we record:

- **Completeness** — every ancestor reachable via `GET /memories/{id}`.
- **Accuracy** — at each step the fetched ancestor's `agent_id` matches
  the planned writer.
- **Depth fidelity** — reconstructed chain length equals planned depth.
- **Fetch latency** — per-hop latency, summed and max-by-chain.

Outputs:

- `events.jsonl` — writes, fetches, verifications.
- `provenance.csv` — one row per chain (depth_planned, depth_observed,
  complete, accurate, depth_match, fetch latencies).
- `summary.csv` — completeness / accuracy / depth-fidelity rates +
  fetch-latency percentiles.
- `completeness_by_depth.png` — completeness as a function of planned
  depth; the curve degrades when the server loses intermediate writes.
- `fetch_latency.png` — per-fetch latency histogram.

## Run the propagation experiment

```bash
python -m agentfleetmem run propagation
```

Agent_writer writes ``n_facts`` fleet-scoped canaries in fleet F1. Every
fleet sibling polls `/search` until either the canary surfaces or
``visibility_timeout_ms`` expires (time-to-visibility). Foreign-fleet
readers in the same tenant search once per fact; any hit is a leak. A
random subset of facts is then passed through `/memories/redistribute`
onto a target agent, and the harness re-fetches to confirm `agent_id`
flipped (ownership transfer).

Outputs:

- `events.jsonl` — writes, every poll, redistributes, ownership checks.
- `propagation.csv` — one row per (fact, reader) probe.
- `summary.csv` — fleet visibility rate, leak rate, missed propagations,
  T_visible percentiles, redistribute ownership-transfer rate.
- `visibility_by_relation.png` — bar chart, fleet-sibling vs cross-fleet
  visibility (the second bar should be zero on a correct server).
- `time_to_visibility.png` — T_visible histogram for fleet siblings.

## Run everything at once

```bash
# Runs leakage, contradiction, provenance, propagation in sequence
# using each experiment's default config.
python -m agentfleetmem run all
```

Useful flags:

- `--seed N` — applies to every experiment (deterministic suite-wide).
- `--label tag` — prefixes all run directories so a single suite can be
  grouped together later.
- `--skip propagation` — repeatable; skips named experiments.
- `--no-report` — don't auto-generate the consolidated report at the end.

Each experiment still writes its own `outputs/runs/<UTC-ts>-<name>-seed<N>/`
directory; nothing is merged at the data-layer. The final report links
back to each run dir for raw artifacts.

## View consolidated results

```bash
python -m agentfleetmem report latest
```

Scans `outputs/runs/`, picks the **most recent run per experiment**,
reads each `summary.csv`, and renders a markdown report to stdout. A
copy is saved to `outputs/reports/<UTC-ts>/report.md` along with all
plots (renamed to `<experiment>-<plot>.png` to prevent collisions).

Flags:

- `--no-copy-plots` — leave PNGs in the original run dirs.
- `--quiet` — print only the report path, not the full markdown.
- `--output-dir <path>` — override the outputs root.

The report includes a headline table per experiment (with the five or
six metrics that matter for the paper) plus links to the run dir and
plot files. Stubbed experiments show up as `_No run yet._`.

## Re-plot from a prior run

```bash
python -m agentfleetmem plot latest
```

This re-reads `events.jsonl` from the most recent run directory and
regenerates the CSV + PNG outputs — useful when iterating on the plotting
code without re-hitting the server.

## Tests

```bash
pytest -q
```

Fixtures live at the repo root in `tests/conftest.py`. The transport
tests use `respx` and don't hit the network.

## Configuration

| Source         | Precedence | Notes                              |
| -------------- | ---------- | ---------------------------------- |
| `--seed`, `--label`, etc. | highest   | CLI overrides win              |
| Env vars       | high       | `MEMCLAW_API_KEY`, `MEMCLAW_BASE_URL`, `AGENTFLEETMEM_*` |
| YAML file      | low        | `configs/leakage.yaml` by default  |
| `HarnessConfig` defaults | lowest | Sensible production-safe values |

## Reproducibility

- Every run records `seed`, `base_url`, the synthetic fleet roster, and the
  full event stream into `events.jsonl`. Two runs with the same seed
  against the same server should produce the same `is_violation` set.
- Plots and CSVs are derived from the JSONL — never from in-memory state —
  so re-plotting is byte-comparable with the original.

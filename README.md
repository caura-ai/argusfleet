# AgentFleetMem

Evaluation harness for **governed shared memory in multi-agent LLM systems** —
the systems primitive described in the companion paper
*"Governed Shared Memory for Multi-Agent LLM Systems"* (`paper/paper.tex`).

The harness exercises a deployed memory service (`memclaw.net` by default)
against synthetic multi-agent workloads and measures four architectural
correctness properties: scope enforcement, contradiction handling, provenance
reconstruction, and cross-agent propagation.

| Experiment | What it measures | Status |
|---|---|---|
| `leakage` | Cross-scope unauthorized retrievals (security envelope) | implemented |
| `contradiction` | Detection of conflicting writes over the same RDF triple | implemented |
| `provenance` | End-to-end reconstruction of derivation chains | implemented |
| `propagation` | Fleet-sibling visibility + cross-fleet isolation + redistribute | implemented |
| `derived-leakage` | Derivation-closed scope: can a peer read a team-scoped memory derived from an agent-scoped secret? (preregistered: **expected to FAIL** on the current server) | implemented |
| `scale` | (not yet) | stubbed |

## Quick start

Requires Python **3.12+**.

```bash
cd argusfleet
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -e .

cp .env.example .env
# edit .env: set MEMCLAW_API_KEY and MEMCLAW_TENANT_ID
```

Run the suite and view the consolidated report:

```bash
python -m argusfleet run all
python -m argusfleet report latest
```

Each experiment writes a `outputs/runs/<UTC-ts>-<experiment>-seed<N>/`
directory containing `events.jsonl`, `<experiment>.csv`, `summary.csv`, and
its plots. The report aggregator pulls the latest run per experiment into a
single markdown summary under `outputs/reports/<UTC-ts>/`.

## CLI

```bash
python -m argusfleet run <leakage|contradiction|provenance|propagation|all>
python -m argusfleet run derived-leakage   # standalone only; not part of `run all`
python -m argusfleet run all --seed 1337 --label baseline --skip propagation
python -m argusfleet run all --no-cleanup    # leave written memories on the server
python -m argusfleet report latest [--quiet] [--no-copy-plots]
python -m argusfleet plot latest    # re-render plots from the latest JSONL
python -m argusfleet cleanup [--yes]         # soft-delete every memory in the tenant(s)
```

Common flags on `run`: `--seed N`, `--label tag`, `--config path.yaml`, `-v`.
Default config for each experiment lives in `configs/<experiment>.yaml`.

## Methodology notes

### Two-signal leakage measurement

Each leakage probe issues **both** an access check and a discovery check:

- **Access** (authoritative): `GET /memories/{id}?tenant_id=<probe-tenant>`.
  The server returns 200 iff the row is visible to the probing tenant.
  Drives the headline `leak_rate` and `miss_rate` metrics.
- **Discovery** (secondary): semantic search by the secret's prose; match
  on `metadata.canary` in returned items. Drives `search_leak_rate` and
  `search_miss_rate`. These are sensitive to embedding ranking and
  accumulated tenant state — *informational, not the security envelope*.

Headline rates carry **95% Wilson confidence intervals** (`leak_rate_ci95_lo` /
`leak_rate_ci95_hi`) in the summary CSV. `0/80` with a Wilson interval up to
about 0.045 is honest; `0/80 → "zero leakage"` is overclaim.

### Derived leakage (derivation-closed scope)

`derived-leakage` tests the invariant that a memory `M`'s retrievability by
reader `R` implies `R` is authorized for **every** source in `M`'s
transitive derivation, not just `M`'s own scope. With the tenant key it
provisions two trust-1 agent keys in one fleet (`A`, `C`); `A` writes an
agent-scoped secret `S`, then a team-scoped summary `M` with
`metadata.derived_from = [S]` (multi-hop with `chain_depth`, max 4), plus a
clean team-scoped control `D`. `C` probes `M` with its own key using the same
two signals as `leakage` (GET access = headline, search = secondary).
Controls: `C` reads `D` and `A` reads `M` (expect allow); `C` reads `S`
(expect deny — if it is allowed the run is marked `run_valid=false` and leaks
are not graded). The `derived_from` walk is bounded (visited set + depth
cap); chains that don't verify are reported as `chain_broken`, not leaks.

**Preregistered outcome:** the probe is *expected to fail* on the current
server — `M` is team-visible and nothing in the retrieval path consults
`derived_from` — so a high `derived_leak_rate` is the predicted result, not a
harness bug. If it passes, it is a regression guard. Requires a key that can
provision agent keys (fails fast otherwise, no tenant-key fallback); agent
keys persist server-side, so revoke them after the run. Run it standalone
against a **test** tenant: `python -m argusfleet run derived-leakage`.

Running it on your own test tenant (after the Quick start install):

1. In `.env` set `MEMCLAW_API_KEY` (a tenant key that can call
   `POST /api/v1/admin/agent-keys/provision`) and `MEMCLAW_TENANT_ID` (a
   test tenant). `MEMCLAW_BASE_URL` is optional (default `https://memclaw.net`).
2. Run `python -m argusfleet run derived-leakage`. Knobs (`n_chains`,
   `chain_depth`, `home_fleet`, `settle_delay_ms`) live in
   `configs/derived_leakage.yaml`.
3. Results land in `outputs/runs/<UTC-ts>-derived-leakage-seed42/`:
   `summary.csv` (headline `derived_leak_rate` with Wilson CI, `run_valid`,
   `miss_rate`, `chain_broken_rate`), `derived_leakage.csv` (per probe),
   `chains.csv` (per chain walk), `events.jsonl`, and two plots.
4. Afterwards, revoke the two agent keys it prints (`af-dlk-a-*`,
   `af-dlk-c-*`). Its memories stay in the tenant; `python -m argusfleet
   cleanup` purges the whole tenant.

At the defaults a run makes 262 requests (2 provisions, 60 writes, 40
chain-walk GETs, 80 probe GETs, 80 searches) and takes about 5 minutes,
dominated by the sequential writes.

### Clean-tenant runs

The production service accumulates state across runs. For credible
numbers, use a clean tenant. The harness salts per-run content with the
run ID so re-runs against a stateful tenant still produce semantically
fresh writes, but the embedding-similarity dedup detector may still
reject writes that share topical structure with prior runs (HTTP `409
near-duplicate`), which silently corrupts results — failed writes
cascade into empty provenance chains, zero successful contradiction
writes, and missing propagation probes.

To guarantee a clean slate, `run all` purges every configured tenant
**before each experiment and after the suite** by default (soft-delete,
which releases the dedup gate since MemClaw's duplicate detectors filter
`deleted_at IS NULL`). Pass `--no-cleanup` to disable, or run
`python -m argusfleet cleanup` to purge manually between campaigns. The
purge targets each tenant in `MEMCLAW_TENANT_ID` and requires a key with
`write` capability scoped to that tenant (no elevated trust needed).

### Agent-scoped GET-by-id probe

`scripts/agent_scope_probe.py` provisions two agent-scoped credentials
(`POST /api/v1/admin/agent-keys/provision`, `kind=agent_key`) at trust 1
and 2 using the tenant key, confirms identity via `/whoami`, then fetches
a cross-fleet row by id with each. It shows the `GET`-by-id enforcement
gap is credential-independent (the handler checks only the tenant, so a
trust-1 agent reads a cross-fleet row). Self-cleaning and re-runnable; the
two agent keys persist (revoke them in the dashboard). See paper §8.1 /
Table 3.

### Multi-tenant configuration

Cross-tenant leakage requires ≥2 tenant IDs. Configure with
`MEMCLAW_TENANT_ID=mc_a,mc_b` (comma-separated). With a single tenant
the leakage experiment only exercises within-tenant scope enforcement.

## Configuration precedence

| Source | Precedence | Notes |
|---|---|---|
| CLI flags (`--seed`, `--label`) | highest | always win |
| Env vars | high | `MEMCLAW_API_KEY`, `MEMCLAW_BASE_URL`, `MEMCLAW_TENANT_ID`, `ARGUSFLEET_*` |
| YAML config | low | `configs/<experiment>.yaml`, override with `--config` |
| `HarnessConfig` defaults | lowest | sensible production-safe values |

## Tests

```bash
pytest -q
```

69 tests across client wire-format, runner scaffolding, the five
experiments (against `respx`-mocked servers), and the report aggregator.
No tests hit the network.

## Repo layout

```
argusfleet/
├── __init__.py / __main__.py / cli.py     package entry + Typer CLI
├── configs/                               experiment YAML configs
├── experiments/                           leakage, contradiction, provenance, propagation, derived_leakage
├── harness/                               client, config, logging, metrics, plots, report, runner, models
├── outputs/runs/<ts>-<exp>-seed<N>/       per-run artifacts (events.jsonl, *.csv, *.png)
├── outputs/reports/<ts>/                  consolidated reports
├── scripts/                               operational scripts (e.g. refresh_paper_figures)
├── tests/                                 pytest suite
└── paper/                                 LaTeX manuscript + figures + Overleaf bundle
```

## Reproducibility

- Every run records its seed, base URL, tenant IDs, and full event stream
  into `events.jsonl`. Same seed + same server state ⇒ same probe-level
  outcomes.
- CSV summaries and PNG plots are derived from the JSONL — never from
  in-memory state — so `plot latest` is byte-comparable with the original
  materialisation.
- `requirements.txt` ships pinned versions captured from a known-working
  environment.

## Paper

The LaTeX manuscript lives under `paper/`. To rebuild figures from the
latest run results:

```bash
pwsh scripts/refresh_paper_figures.ps1     # Windows
bash scripts/refresh_paper_figures.sh      # macOS / Linux
```

Then compile:

```bash
cd paper && latexmk -pdf paper.tex
```

The `paper/paper_overleaf.zip` bundle is an Overleaf-upload-ready archive
(`paper.tex` + `references.bib` + `figures/`).

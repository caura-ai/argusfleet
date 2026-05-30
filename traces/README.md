# ArgusFleet experiment traces

Canonical `events.jsonl` + `summary.csv` for every run whose numbers appear in the paper.
Everything in the paper derives from these event streams; re-run the harness's
`report`/`plot` pipeline over any `events.jsonl` to reproduce its `summary.csv` and figures.
All runs use `seed=42` against `memclaw.net` on a freshly-purged tenant.

## Why multiple contradiction runs
Write admission is stochastic (concurrent writes race at the server's synchronous
dedup gate; content is nonce-salted per run), so counts vary run-to-run around a
stable rate. The four N=200 runs below show the spread; the run of record's
both-admitted=90 equals 2x the N=100 run's 45 by coincidence (sibling runs give 91).

## Runs

- `20260530T153139Z-contradiction-seed42/` — contradiction: N=200 run of record (table + prose)  
    fact_runs=200 succeeded=194 both_admitted=90 detected=98 detection_rate=0.490
- `20260530T170600Z-contradiction-seed42/` — contradiction: N=200 independent re-run  
    fact_runs=200 succeeded=193 both_admitted=90 detected=96 detection_rate=0.480
- `20260530T170849Z-contradiction-seed42/` — contradiction: N=200 independent re-run  
    fact_runs=200 succeeded=196 both_admitted=91 detected=100 detection_rate=0.500
- `20260530T171156Z-contradiction-seed42/` — contradiction: N=200 independent re-run  
    fact_runs=200 succeeded=194 both_admitted=91 detected=97 detection_rate=0.485
- `20260529T205052Z-contradiction-seed42/` — contradiction: N=100 prior run (doubling comparison)  
    fact_runs=100 succeeded=97 both_admitted=45 detected=49 detection_rate=0.490
- `20260530T153048Z-leakage-seed42/` — leakage: N=200 run of record
- `20260530T153446Z-provenance-seed42/` — provenance: N=200 run of record
- `20260530T161453Z-propagation-seed42/` — propagation: N=200 run of record

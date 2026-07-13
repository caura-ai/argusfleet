# Marketing — MemClaw vs Mem0

Benchmark-grounded comparison assets. Every figure is measured by the
ArgusFleet harness against the live MemClaw and Mem0 APIs (2026-07-13);
numbers regenerate from the committed traces under `../outputs/runs/`.

| File | What it is |
|---|---|
| `memclaw-vs-mem0.html` | Standalone comparison page (light + dark themes). Open directly in a browser. |
| `memclaw-vs-mem0-slide.html` | Single 1280×720 deck slide (dark). |
| `memclaw-vs-mem0-slide.png` | Rendered slide, 2560×1440 (retina 2×) for decks. |

Re-render the slide PNG:

```bash
chrome --headless --no-sandbox --force-device-scale-factor=2 \
  --window-size=1280,720 \
  --screenshot=memclaw-vs-mem0-slide.png \
  file:///ABSOLUTE/PATH/marketing/memclaw-vs-mem0-slide.html
```

**Sources for every claim** (see `../paper/mem0_integration.md` for full context):

- Cross-scope read leaks — leakage experiment: Mem0 712/712 GET-by-id
  (incl. 384/384 cross-`user_id`); MemClaw 0 (scope enforced).
- Contradiction — Mem0 keeps both values live in 26/60 pairs, resolves
  0/60 (no supersession primitive); MemClaw supersedes 90/90 conditional.
- Lineage — both reconstruct 50/50 depth-4 chains (a genuine tie).
- Read latency — MemClaw per-hop p50 291 ms vs Mem0 476 ms.
- Capability matrix — MemClaw 9/9 governance primitives, Mem0 2/9.

Fair-play note (kept on the page on purpose): Mem0's API key is its
intended trust boundary and its in-scope recall is excellent; the gap
measured here is sub-tenant *governance*, and the harness is open so the
numbers can be independently reproduced.

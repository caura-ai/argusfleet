"""Scaled agent-scoped GET-by-id leakage probe.

The paper's Table~\\ref{tab:agentscope} was a 4-probe spot check that, at
measurement time, showed a trust-1 agent reading a cross-fleet row (the IDOR).
This scales that probe to a real N so the *enforced* behaviour can be
quantified after the server-side fix.

Design (single tenant; the gateway resolves each agent's identity):
  - provision one trust-1 and one trust-2 agent key per fleet
    (trust-1 = write-home-fleet, below the trust-2 cross-fleet-read rung);
  - the tenant key writes ``secrets_per_fleet`` canary memories in each fleet;
  - every agent GET-by-id's every secret with ITS OWN agent key;
  - classify each probe: same-fleet (expect allow) vs cross-fleet
    (trust-1: expect DENY; trust-2: expect allow).

Emits a JSONL trace + summary. Cleans up the secret writes; agent keys
persist (revoke in the dashboard).

Run:  python scripts/agent_scope_leakage.py
"""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from pathlib import Path

import httpx

from argusfleet.harness.config import load_config

N_FLEETS = 4
SECRETS_PER_FLEET = 3
TRUSTS = (1, 2)  # one agent key per (fleet, trust)


async def _provision(c, base, tk, agent_id, fleet, trust):
    r = await c.post(
        f"{base}/admin/agent-keys/provision",
        headers={"X-API-Key": tk},
        json={"agent_id": agent_id, "label": f"af-scope-leak {agent_id}",
              "initial_fleet": fleet, "initial_trust": trust},
    )
    if r.status_code >= 400:
        raise SystemExit(f"provision {agent_id} -> {r.status_code}: {r.text[:300]}")
    j = r.json()
    return j.get("raw_key") or j.get("key") or j.get("api_key")


async def _write(c, base, tk, tenant, fleet, content):
    r = await c.post(
        f"{base}/memories",
        headers={"X-API-Key": tk},
        json={"tenant_id": tenant, "agent_id": f"af-writer-{fleet}",
              "fleet_id": fleet, "content": content, "write_mode": "fast"},
    )
    if r.status_code >= 400:
        raise SystemExit(f"write {fleet} -> {r.status_code}: {r.text[:300]}")
    return r.json()["id"]


async def _get(c, base, key, tenant, mem_id):
    r = await c.get(f"{base}/memories/{mem_id}", params={"tenant_id": tenant},
                    headers={"X-API-Key": key})
    return r.status_code


async def main():
    cfg = load_config()
    tenant = cfg.tenant_ids[0]
    tk = cfg.api_key
    base = cfg.base_url.rstrip("/") + "/api/v1"
    nonce = uuid.uuid4().hex[:8]
    fleets = [f"af-scope-f{i}" for i in range(N_FLEETS)]

    out_dir = Path("outputs/runs") / f"agent_scope_leakage-{nonce}"
    out_dir.mkdir(parents=True, exist_ok=True)
    sink = (out_dir / "events.jsonl").open("w", encoding="utf-8")

    def emit(event, **data):
        sink.write(json.dumps({"event": event, "data": data}) + "\n")

    print(f"base={base} tenant={tenant} fleets={N_FLEETS} secrets/fleet={SECRETS_PER_FLEET}")
    emit("run_start", tenant=tenant, n_fleets=N_FLEETS, secrets_per_fleet=SECRETS_PER_FLEET,
         trusts=list(TRUSTS), nonce=nonce)

    async with httpx.AsyncClient(timeout=60) as c:
        # 1. Provision one agent key per (fleet, trust).
        agents = {}  # (fleet, trust) -> (agent_id, key)
        for fleet in fleets:
            for trust in TRUSTS:
                aid = f"af-{fleet}-t{trust}-{nonce}"
                key = await _provision(c, base, tk, aid, fleet, trust)
                agents[(fleet, trust)] = (aid, key)
        print(f"provisioned {len(agents)} agent keys ({N_FLEETS} fleets x {len(TRUSTS)} trusts)")

        # 2. Write secrets per fleet.
        secrets = []  # (mem_id, fleet)
        for fleet in fleets:
            for s in range(SECRETS_PER_FLEET):
                token = f"afscope-{fleet}-{s}-{nonce}"
                mid = await _write(c, base, tk, tenant, fleet,
                                   f"governed-memory canary {token}; subject release-{s} region eu")
                secrets.append((mid, fleet))
                emit("write", memory_id=mid, fleet=fleet, token=token)
        print(f"wrote {len(secrets)} secrets")

        # 3. Every agent GETs every secret with its own key.
        n_probe = 0
        for (a_fleet, trust), (aid, key) in agents.items():
            for mid, s_fleet in secrets:
                relation = "same_fleet" if a_fleet == s_fleet else "cross_fleet"
                # Trust ladder: cross-fleet read requires trust >= 2.
                expect_allow = (relation == "same_fleet") or (trust >= 2)
                st = await _get(c, base, key, tenant, mid)
                allowed = st == 200
                emit("probe", agent_id=aid, agent_fleet=a_fleet, trust=trust,
                     secret_fleet=s_fleet, relation=relation, status=st,
                     allowed=allowed, expected_allow=expect_allow,
                     is_leak=(allowed and not expect_allow),
                     is_miss=((not allowed) and expect_allow))
                n_probe += 1
        print(f"ran {n_probe} GET-by-id probes")

        # 4. Clean up secret writes (keys persist).
        for mid, _ in secrets:
            await c.request("DELETE", f"{base}/memories/{mid}",
                            params={"tenant_id": tenant}, headers={"X-API-Key": tk})

    emit("run_end", nonce=nonce)
    sink.close()

    # 5. Summarise from the trace.
    rows = [json.loads(l)["data"] for l in (out_dir / "events.jsonl").open(encoding="utf-8")
            if json.loads(l)["event"] == "probe"]
    def rate(pred_num, pred_den):
        den = [r for r in rows if pred_den(r)]
        num = [r for r in den if pred_num(r)]
        return len(num), len(den)

    print("\n=== AGENT-SCOPED GET-by-id LEAKAGE (current behaviour) ===")
    for trust in TRUSTS:
        ln, ld = rate(lambda r: r["allowed"], lambda r: r["trust"] == trust and r["relation"] == "cross_fleet")
        sn, sd = rate(lambda r: r["allowed"], lambda r: r["trust"] == trust and r["relation"] == "same_fleet")
        verdict = "ENFORCED" if (trust == 1 and ln == 0) else ("allowed (ladder)" if trust >= 2 else "LEAK")
        print(f"trust-{trust}: cross-fleet GET allowed {ln}/{ld}  | same-fleet {sn}/{sd}  -> {verdict}")
    leaks = sum(1 for r in rows if r["is_leak"])
    print(f"total leaks (allowed but should deny): {leaks}/{len(rows)} probes")
    summ = out_dir / "summary.csv"
    with summ.open("w", encoding="utf-8") as f:
        f.write("metric,value\n")
        f.write(f"n_probes,{len(rows)}\n")
        f.write(f"n_leaks,{leaks}\n")
        for trust in TRUSTS:
            ln, ld = rate(lambda r: r["allowed"], lambda r: r["trust"] == trust and r["relation"] == "cross_fleet")
            f.write(f"trust{trust}_crossfleet_allowed,{ln}\n")
            f.write(f"trust{trust}_crossfleet_n,{ld}\n")
    print(f"\ntrace: {out_dir}/events.jsonl\nsummary: {summ}")
    print("(agent keys persist - revoke in dashboard)")


if __name__ == "__main__":
    asyncio.run(main())

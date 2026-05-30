"""Agent-scoped GET-by-id scope probe.

Provisions two agent keys via the tenant key (POST /admin/agent-keys/provision),
writes a fleet-0 and a fleet-1 secret, then GETs each by id with:
  - tenant key (no agent identity)          -> baseline
  - key A (fleet-0, trust 1)                -> may NOT read cross-fleet
  - key B (fleet-0, trust 2)                -> MAY read cross-fleet

Verdict logic:
  cross-fleet (M1): A denied & B allowed  => fleet scope enforced on GET
                    A allowed             => cross-fleet read defect
  same-fleet  (M0): A allowed             => positive control (key works)
"""
from __future__ import annotations

import asyncio
import json

import httpx

from argusfleet.harness.config import load_config


async def provision(c, base, tk, agent_id, fleet, trust):
    r = await c.post(
        f"{base}/admin/agent-keys/provision",
        headers={"X-API-Key": tk},
        json={
            "agent_id": agent_id,
            "label": f"argusfleet scope probe {agent_id}",
            "initial_fleet": fleet,
            "initial_trust": trust,
        },
    )
    if r.status_code >= 400:
        raise SystemExit(f"provision {agent_id} -> {r.status_code}: {r.text[:300]}")
    j = r.json()
    rk = j.get("raw_key") or j.get("key") or j.get("api_key")
    if not rk:
        raise SystemExit(f"provision {agent_id}: no raw_key in {list(j.keys())}")
    return rk


async def write_secret(c, base, tk, tenant, agent_id, fleet, content):
    r = await c.post(
        f"{base}/memories",
        headers={"X-API-Key": tk},
        json={"tenant_id": tenant, "agent_id": agent_id, "fleet_id": fleet,
              "content": content, "write_mode": "fast"},
    )
    if r.status_code >= 400:
        raise SystemExit(f"write {fleet} -> {r.status_code}: {r.text[:300]}")
    return r.json()["id"]


async def whoami(c, base, key):
    r = await c.get(f"{base}/whoami", headers={"X-API-Key": key})
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, r.text[:200]


async def get_mem(c, base, key, tenant, mem_id):
    r = await c.get(f"{base}/memories/{mem_id}", params={"tenant_id": tenant},
                    headers={"X-API-Key": key})
    body = ""
    if r.status_code >= 400:
        try:
            body = json.dumps(r.json())[:160]
        except Exception:
            body = r.text[:160]
    return r.status_code, body


async def main():
    cfg = load_config()
    tenant = cfg.tenant_ids[0]
    tk = cfg.api_key
    base = cfg.base_url.rstrip("/") + "/api/v1"
    print(f"base={base} tenant={tenant}")

    async with httpx.AsyncClient(timeout=60) as c:
        key_a = await provision(c, base, tk, "af-probe-a", "fleet-0", 1)
        key_b = await provision(c, base, tk, "af-probe-b", "fleet-0", 2)
        print("provisioned: A=fleet-0/trust1  B=fleet-0/trust2")

        # Verify the keys actually resolve to an agent identity + fleet.
        for name, key in [("tenant", tk), ("A", key_a), ("B", key_b)]:
            st, who = await whoami(c, base, key)
            print(f"whoami[{name}] {st}: {json.dumps(who) if isinstance(who, dict) else who}")
        print()

        m1 = await write_secret(c, base, tk, tenant, "af-writer-f1", "fleet-1",
                                "argusfleet agent-scope canary fleet1 alpha-7731 deployment region eu")
        m0 = await write_secret(c, base, tk, tenant, "af-writer-f0", "fleet-0",
                                "argusfleet agent-scope canary fleet0 beta-9920 deployment region us")
        print(f"wrote M1(fleet-1)={m1}\n       M0(fleet-0)={m0}\n")

        probes = [
            ("M1 cross-fleet via TENANT key (baseline)", tk, m1),
            ("M1 cross-fleet via A (fleet-0,trust1)    ", key_a, m1),
            ("M1 cross-fleet via B (fleet-0,trust2)    ", key_b, m1),
            ("M0 same-fleet  via A (fleet-0,trust1) ctl", key_a, m0),
        ]
        print(f"{'probe':<44} status")
        for label, key, mem in probes:
            st, body = await get_mem(c, base, key, tenant, mem)
            print(f"{label}  {st}  {body}")

        # Clean up the two probe writes so the script is re-runnable
        # (soft-delete releases the dedup gate on the next run).
        for mem in (m0, m1):
            await c.request("DELETE", f"{base}/memories/{mem}",
                            params={"tenant_id": tenant}, headers={"X-API-Key": tk})
        print("\ncleaned up probe writes (agent keys persist - revoke in dashboard)")


if __name__ == "__main__":
    asyncio.run(main())

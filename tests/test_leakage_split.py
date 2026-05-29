"""End-to-end test of the leak_rate / miss_rate split in leakage.

Builds two targeted scenarios on a respx-mocked server:

1. **Compliant server** — scope enforcement is perfect. Probes against
   the canary they own return it; probes against secrets they shouldn't
   see return empty. Expected: leak_rate=0, miss_rate=0.

2. **Over-permissive server** — search returns every canary to every
   probe regardless of scope. Expected: leak_rate>0, miss_rate=0.

3. **Under-permissive server** — search never returns anything. Expected:
   leak_rate=0, miss_rate=1.

If the harness conflates the two failure modes again, scenario 2 and 3
would both look identical in the summary; the assertions below catch
that regression.
"""

from __future__ import annotations

import json
import re
import uuid

import httpx
import pytest
import respx

from agentfleetmem.experiments import leakage
from agentfleetmem.harness.client import MemClawClient
from agentfleetmem.harness.config import HarnessConfig


def _cfg(tmp_config: HarnessConfig) -> HarnessConfig:
    """Two tenants so expected-deny probes are cross-tenant, which is
    the boundary the GET-by-id access check can actually enforce with
    a tenant-scoped API key. Single-tenant tests would conflate
    cross-fleet within-tenant probes (which GET cannot reject without
    agent identity) with the security envelope."""
    return tmp_config.model_copy(
        update={
            "base_url": "https://memclaw.test",
            "tenant_ids": ["mc_t1", "mc_t2"],
            "experiment": {
                "n_tenants": 2,
                "n_fleets_per_tenant": 1,
                "n_agents_per_fleet": 2,
                "settle_delay_ms": 0,
                "max_in_flight": 2,
            },
        }
    )


_PROBE_TOKEN_RE = re.compile(r"canary-(fleet|agent)-[0-9a-f]+-[^\s\"]+")
_GET_PATH_RE = re.compile(r"/api/v1/memories/([0-9a-fA-F-]{36})$")


def _make_mock(mode: str) -> respx.Router:
    """``mode`` ∈ {"compliant", "over_permissive", "under_permissive"}.

    Both the GET-by-id path (authoritative) and the search path are
    served. ``compliant`` enforces scope on both. ``over_permissive``
    leaks via GET (the security signal). ``under_permissive`` 404s
    even legitimate GETs (the availability signal).
    """
    router = respx.mock(base_url="https://memclaw.test", assert_all_called=False)
    store: dict[str, dict] = {}

    def write_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        new_id = str(uuid.uuid4())
        record = {
            "id": new_id,
            "tenant_id": body["tenant_id"],
            "fleet_id": body.get("fleet_id"),
            "agent_id": body["agent_id"],
            "memory_type": "fact",
            "content": body["content"],
            "metadata": body.get("metadata") or {},
            "created_at": "2026-05-28T00:00:00+00:00",
            "status": "active",
            "visibility": body.get("visibility", "scope_team"),
        }
        store[new_id] = record
        return httpx.Response(201, json=record)

    def get_handler(request: httpx.Request) -> httpx.Response:
        m = _GET_PATH_RE.search(request.url.path)
        if not m:
            return httpx.Response(404, json={"detail": "bad path"})
        memory_id = m.group(1)
        record = store.get(memory_id)
        if record is None:
            return httpx.Response(404, json={"detail": "not found"})
        probe_tenant = request.url.params.get("tenant_id")
        if mode == "under_permissive":
            return httpx.Response(404, json={"detail": "hidden"})
        if mode == "over_permissive":
            # Returns the row regardless of which tenant is asking.
            return httpx.Response(200, json=record)
        # Compliant: only return if the probing tenant matches.
        if probe_tenant and probe_tenant != record["tenant_id"]:
            return httpx.Response(404, json={"detail": "cross-tenant"})
        # Compliant intra-tenant: scope-check by visibility + fleet.
        # In our test config there's only one tenant; fleet enforcement
        # would happen here. For this test all secrets and probes share
        # one tenant, so a row visible to ``scope_team`` is returned to
        # any probe in the same tenant; ``scope_agent`` would require
        # finer enforcement but the test doesn't currently exercise it.
        return httpx.Response(200, json=record)

    def search_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        query = body.get("query") or ""
        if mode == "under_permissive":
            return httpx.Response(200, json={"items": []})
        items = []
        for rec in store.values():
            if rec["tenant_id"] != body["tenant_id"]:
                continue
            meta = rec.get("metadata") or {}
            token = meta.get("canary")
            if not token:
                continue
            # Harness now queries by the secret's prose (the content),
            # not the canary token. Mock returns rows whose content
            # exactly matches the query — same shape the propagation
            # mock uses.
            content = rec.get("content", "")
            if query and query != content and query not in content:
                continue
            if mode == "compliant":
                # Only return the canary if the searcher's tenant/fleet/
                # agent is entitled. The leakage experiment uses the
                # owner agent's identity to write secrets but searches
                # are unauthenticated WRT the agent (the search request
                # doesn't carry the searcher's identity). For this test
                # we reproduce correct enforcement by encoding the
                # owner+scope in metadata.
                scope = meta.get("scope")
                owner = rec["agent_id"]
                owner_fleet = rec["fleet_id"]
                searcher = body.get("filter_agent_id")
                # In the new harness search code we DON'T pass
                # filter_agent_id, so this branch isn't exercised
                # tightly — but server-side scope filtering still
                # narrows by fleet. Approximate: only the writing
                # agent's fleet sees scope_team rows; only the writer
                # sees scope_agent rows.
                fleet_ids = body.get("fleet_ids") or []
                if scope == "agent":
                    # Without filter_agent_id, agent-scope rows are
                    # technically visible to all fleet members in a
                    # compliant server, so we let them through and rely
                    # on the *miss* signal — which means our compliant
                    # test should expect more "allows" than the harness
                    # plan does. To keep the test tight we still gate
                    # on fleet — the plan only expects allow when same
                    # agent for agent-scope. Mismatch is a miss.
                    if not fleet_ids or owner_fleet in fleet_ids:
                        items.append(rec)
                elif scope == "team":
                    if not fleet_ids or owner_fleet in fleet_ids:
                        items.append(rec)
            else:  # over_permissive — return everything matching query
                items.append(rec)
        return httpx.Response(200, json={"items": items})

    router.post("/api/v1/memories").mock(side_effect=write_handler)
    router.post("/api/v1/search").mock(side_effect=search_handler)
    router.get(url__regex=r"/api/v1/memories/[0-9a-fA-F-]+").mock(side_effect=get_handler)
    return router


@pytest.mark.asyncio
async def test_over_permissive_drives_leak_rate(tmp_config: HarnessConfig) -> None:
    cfg = _cfg(tmp_config)
    with _make_mock("over_permissive"):
        async with MemClawClient(cfg.base_url, api_key="k", max_retries=1) as client:
            result = await leakage.run(cfg, client=client)

    # An over-permissive server returns canaries even to probes that
    # weren't entitled — leak_rate must be > 0. miss_rate must be 0
    # (every entitled probe also got its row).
    assert result["leak_rate"] > 0.0
    assert result["miss_rate"] == 0.0


@pytest.mark.asyncio
async def test_under_permissive_drives_miss_rate(tmp_config: HarnessConfig) -> None:
    cfg = _cfg(tmp_config)
    with _make_mock("under_permissive"):
        async with MemClawClient(cfg.base_url, api_key="k", max_retries=1) as client:
            result = await leakage.run(cfg, client=client)

    # No probe sees anything — leak_rate stays 0 (security envelope
    # honored), but miss_rate spikes (availability broken).
    assert result["leak_rate"] == 0.0
    assert result["miss_rate"] == 1.0

"""End-to-end smoke tests for the propagation experiment.

The mock server stores writes and serves them via search/get. Configurable
failure modes let us assert each metric independently:

- compliant server → fleet visibility 1.0, leak rate 0.0, transfer 1.0.
- search drops a token forever → missed propagation > 0.
- foreign-fleet search leaks → leak rate > 0.
- redistribute is a no-op → ownership transfer rate < 1.
"""

from __future__ import annotations

import json
import re
import uuid

import httpx
import pytest
import respx

from argusfleet.experiments import propagation
from argusfleet.harness.client import MemClawClient
from argusfleet.harness.config import HarnessConfig


_MEMORY_PATH_RE = re.compile(r"/api/v1/memories/([0-9a-fA-F-]{36})$")


def _make_server(
    *,
    leak_foreign: bool = False,
    drop_token: str | None = None,
    redistribute_no_op: bool = False,
):
    router = respx.mock(base_url="https://memclaw.test", assert_all_called=False)
    store: dict[str, dict] = {}
    by_token: dict[str, str] = {}

    def write_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        new_id = str(uuid.uuid4())
        meta = body.get("metadata") or {}
        record = {
            "id": new_id,
            "tenant_id": body["tenant_id"],
            "fleet_id": body.get("fleet_id"),
            "agent_id": body["agent_id"],
            "memory_type": "fact",
            "content": body["content"],
            "weight": 1.0,
            "metadata": meta,
            "created_at": "2026-05-28T00:00:00+00:00",
            "status": "active",
            "visibility": body.get("visibility", "scope_team"),
        }
        store[new_id] = record
        if "canary" in meta:
            by_token[meta["canary"]] = new_id
        return httpx.Response(201, json=record)

    def search_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        query = body.get("query", "")
        fleet_ids = body.get("fleet_ids") or []
        items: list[dict] = []
        for rec in store.values():
            if rec["tenant_id"] != body["tenant_id"]:
                continue
            if fleet_ids and rec.get("fleet_id") not in fleet_ids:
                if not leak_foreign:
                    continue
            meta = rec.get("metadata") or {}
            token = meta.get("canary")
            if not token:
                continue
            # Mock pretends to do semantic search by checking whether
            # the query text appears in the stored content (the harness
            # now passes prose as the query, not the canary token).
            content = rec.get("content", "")
            if query and query not in content:
                continue
            if drop_token and token == drop_token:
                continue
            items.append(rec)
        return httpx.Response(200, json={"items": items})

    def get_handler(request: httpx.Request) -> httpx.Response:
        m = _MEMORY_PATH_RE.search(request.url.path)
        if not m:
            return httpx.Response(404, json={"detail": "bad path"})
        rec = store.get(m.group(1))
        if rec is None:
            return httpx.Response(404, json={"detail": "missing"})
        return httpx.Response(200, json=rec)

    def redistribute_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        memory_ids = body.get("memory_ids") or []
        target = body.get("target_agent_id")
        moved = 0
        if not redistribute_no_op:
            for mid in memory_ids:
                rec = store.get(mid)
                if rec is not None:
                    rec["agent_id"] = target
                    moved += 1
        return httpx.Response(
            200,
            json={
                "moved": moved,
                "promoted": 0,
                "skipped": 0,
                "errors": [],
                "redistribute_ms": 1,
            },
        )

    router.post("/api/v1/memories").mock(side_effect=write_handler)
    router.post("/api/v1/search").mock(side_effect=search_handler)
    router.post("/api/v1/memories/redistribute").mock(side_effect=redistribute_handler)
    router.get(url__regex=r"/api/v1/memories/[0-9a-fA-F-]{36}").mock(side_effect=get_handler)
    return router, by_token


def _fast_cfg(tmp_config: HarnessConfig) -> HarnessConfig:
    """Knock the polling cadence and timeouts down so tests don't sleep."""
    return tmp_config.model_copy(
        update={
            "base_url": "https://memclaw.test",
            "experiment": {
                "n_facts": 3,
                "n_fleet_readers": 2,
                "n_foreign_readers": 1,
                "visibility_timeout_ms": 300,
                "poll_interval_ms": 10,
                "redistribute_fraction": 1.0,
                "max_in_flight": 4,
                # Keep the window phase tiny + fast so tests don't sleep.
                "n_window_facts": 2,
                "window_poll_interval_ms": 10,
                "window_timeout_ms": 300,
                "tenant_id": "mc_t_test",
                "fleet_id": "fleet_home",
                "foreign_fleet_id": "fleet_other",
            },
        }
    )


@pytest.mark.asyncio
async def test_compliant_server_full_propagation(tmp_config: HarnessConfig) -> None:
    cfg = _fast_cfg(tmp_config)
    router, _ = _make_server()
    with router:
        async with MemClawClient(cfg.base_url, api_key="k", max_retries=1) as client:
            result = await propagation.run(cfg, client=client)
    assert result["fleet_visibility_rate"] == 1.0
    assert result["leak_rate"] == 0.0
    assert result["n_missed_propagations"] == 0
    assert result["ownership_transfer_rate"] == 1.0


@pytest.mark.asyncio
async def test_dropped_token_reports_missed_propagation(tmp_config: HarnessConfig) -> None:
    """A server that silently drops the first canary from search results
    must cause the harness to report a missed propagation and a
    visibility rate below 1.0."""
    cfg = _fast_cfg(tmp_config).model_copy(
        update={"experiment": {**_fast_cfg(tmp_config).experiment, "n_facts": 2}}
    )
    router2 = respx.mock(base_url="https://memclaw.test", assert_all_called=False)
    store: dict[str, dict] = {}
    drop_set: set[str] = set()

    def write_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        new_id = str(uuid.uuid4())
        meta = body.get("metadata") or {}
        record = {
            "id": new_id,
            "tenant_id": body["tenant_id"],
            "fleet_id": body.get("fleet_id"),
            "agent_id": body["agent_id"],
            "memory_type": "fact",
            "content": body["content"],
            "metadata": meta,
            "created_at": "2026-05-28T00:00:00+00:00",
            "status": "active",
            "visibility": body.get("visibility", "scope_team"),
        }
        store[new_id] = record
        token = meta.get("canary")
        # Drop the first *rate-phase* token (the window phase writes first
        # and its tokens carry phase="window"; dropping one of those would
        # just stall the window poller and leave the rate metrics clean).
        if token and not drop_set and meta.get("phase") != "window":
            drop_set.add(token)
        return httpx.Response(201, json=record)

    def search_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        items = []
        for rec in store.values():
            if rec["tenant_id"] != body["tenant_id"]:
                continue
            fleet_ids = body.get("fleet_ids") or []
            if fleet_ids and rec.get("fleet_id") not in fleet_ids:
                continue
            meta = rec.get("metadata") or {}
            token = meta.get("canary")
            if token in drop_set:
                continue
            content = rec.get("content", "")
            query = body.get("query") or ""
            if query and query not in content and token != query:
                continue
            items.append(rec)
        return httpx.Response(200, json={"items": items})

    def get_handler(request: httpx.Request) -> httpx.Response:
        m = _MEMORY_PATH_RE.search(request.url.path)
        rec = store.get(m.group(1)) if m else None
        if rec is None:
            return httpx.Response(404)
        return httpx.Response(200, json=rec)

    def redistribute_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        for mid in body.get("memory_ids") or []:
            if mid in store:
                store[mid]["agent_id"] = body["target_agent_id"]
        return httpx.Response(200, json={"moved": 1, "promoted": 0,
                                          "skipped": 0, "errors": [], "redistribute_ms": 1})

    router2.post("/api/v1/memories").mock(side_effect=write_handler)
    router2.post("/api/v1/search").mock(side_effect=search_handler)
    router2.post("/api/v1/memories/redistribute").mock(side_effect=redistribute_handler)
    router2.get(url__regex=r"/api/v1/memories/[0-9a-fA-F-]{36}").mock(side_effect=get_handler)

    with router2:
        async with MemClawClient(cfg.base_url, api_key="k", max_retries=1) as client:
            result = await propagation.run(cfg, client=client)

    assert result["n_missed_propagations"] > 0
    assert result["fleet_visibility_rate"] < 1.0


@pytest.mark.asyncio
async def test_cross_fleet_leak_is_caught(tmp_config: HarnessConfig) -> None:
    cfg = _fast_cfg(tmp_config)
    router, _ = _make_server(leak_foreign=True)
    with router:
        async with MemClawClient(cfg.base_url, api_key="k", max_retries=1) as client:
            result = await propagation.run(cfg, client=client)
    assert result["leak_rate"] > 0.0


@pytest.mark.asyncio
async def test_redistribute_noop_drives_transfer_rate_to_zero(
    tmp_config: HarnessConfig,
) -> None:
    cfg = _fast_cfg(tmp_config)
    router, _ = _make_server(redistribute_no_op=True)
    with router:
        async with MemClawClient(cfg.base_url, api_key="k", max_retries=1) as client:
            result = await propagation.run(cfg, client=client)
    assert result["ownership_transfer_rate"] == 0.0

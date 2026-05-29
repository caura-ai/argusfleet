"""End-to-end smoke tests for the provenance experiment.

The mock server stores write payloads in a dict and serves them back via
``GET /memories/{id}``. That's enough to verify the experiment correctly
walks ``derived_from`` chains and grades the result.
"""

from __future__ import annotations

import json
import re
import uuid

import httpx
import pytest
import respx

from argusfleet.experiments import provenance
from argusfleet.harness.client import MemClawClient
from argusfleet.harness.config import HarnessConfig


_MEMORY_PATH_RE = re.compile(r"/api/v1/memories/([^/?]+)$")


def _make_server(
    *, drop_after_depth: int | None = None, swap_writer_at_depth: int | None = None
):
    """Build a respx router.

    ``drop_after_depth`` — once a write at depth N occurs, the next GET
    for that memory returns 404 (simulates a row vanishing).
    ``swap_writer_at_depth`` — the GET response for memories at this
    depth returns a different ``agent_id`` than was written (simulates a
    silent identity-substitution bug).
    """
    router = respx.mock(base_url="https://memclaw.test", assert_all_called=False)
    store: dict[str, dict] = {}
    dropped: set[str] = set()

    def write_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        new_id = str(uuid.uuid4())
        meta = body.get("metadata") or {}
        payload = {
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
        store[new_id] = payload
        depth = meta.get("depth_index")
        if drop_after_depth is not None and isinstance(depth, int) and depth >= drop_after_depth:
            dropped.add(new_id)
        return httpx.Response(201, json={"id": new_id, **payload})

    def get_handler(request: httpx.Request) -> httpx.Response:
        m = _MEMORY_PATH_RE.search(request.url.path)
        if not m:
            return httpx.Response(404, json={"detail": "bad path"})
        memory_id = m.group(1)
        if memory_id in dropped or memory_id not in store:
            return httpx.Response(404, json={"detail": "gone"})
        record = dict(store[memory_id])
        if swap_writer_at_depth is not None:
            depth = (record.get("metadata") or {}).get("depth_index")
            if depth == swap_writer_at_depth:
                record["agent_id"] = "imposter_agent"
        return httpx.Response(200, json=record)

    router.post("/api/v1/memories").mock(side_effect=write_handler)
    router.get(url__regex=r"/api/v1/memories/[A-Za-z0-9-]+").mock(side_effect=get_handler)
    return router


@pytest.mark.asyncio
async def test_compliant_server_yields_full_completeness(
    tmp_config: HarnessConfig,
) -> None:
    cfg = tmp_config.model_copy(
        update={
            "base_url": "https://memclaw.test",
            "experiment": {
                "n_chains": 3,
                "chain_depth": 3,
                "max_in_flight": 2,
                "tenant_id": "mc_t_test",
                "fleet_id": "fleet_test",
            },
        }
    )
    with _make_server():
        async with MemClawClient(cfg.base_url, api_key="k", max_retries=1) as client:
            result = await provenance.run(cfg, client=client)

    assert result["n_chains"] == 3
    assert result["completeness_rate"] == 1.0
    assert result["accuracy_rate"] == 1.0
    assert result["depth_fidelity_rate"] == 1.0


@pytest.mark.asyncio
async def test_dropped_ancestor_drives_completeness_below_one(
    tmp_config: HarnessConfig,
) -> None:
    """If the server drops rows at depth >= 1, leaves can't walk back to
    the root — completeness must fall below 1.0."""
    cfg = tmp_config.model_copy(
        update={
            "base_url": "https://memclaw.test",
            "experiment": {
                "n_chains": 2,
                "chain_depth": 3,
                "max_in_flight": 2,
                "tenant_id": "mc_t_test",
                "fleet_id": "fleet_test",
            },
        }
    )
    with _make_server(drop_after_depth=1):
        async with MemClawClient(cfg.base_url, api_key="k", max_retries=1) as client:
            result = await provenance.run(cfg, client=client)

    assert result["completeness_rate"] < 1.0


@pytest.mark.asyncio
async def test_writer_substitution_drives_accuracy_below_one(
    tmp_config: HarnessConfig,
) -> None:
    """A silent writer substitution must be caught — completeness might
    still be 1.0, but accuracy must drop."""
    cfg = tmp_config.model_copy(
        update={
            "base_url": "https://memclaw.test",
            "experiment": {
                "n_chains": 2,
                "chain_depth": 3,
                "max_in_flight": 2,
                "tenant_id": "mc_t_test",
                "fleet_id": "fleet_test",
            },
        }
    )
    with _make_server(swap_writer_at_depth=1):
        async with MemClawClient(cfg.base_url, api_key="k", max_retries=1) as client:
            result = await provenance.run(cfg, client=client)

    assert result["accuracy_rate"] < 1.0

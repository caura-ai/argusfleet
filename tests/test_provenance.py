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
    *,
    drop_after_depth: int | None = None,
    swap_writer_at_depth: int | None = None,
    truncate_at_depth: int | None = None,
    cycle: bool = False,
    fail_write_once_at_depth: int | None = None,
):
    """Build a respx router.

    ``drop_after_depth`` — once a write at depth N occurs, the next GET
    for that memory returns 404 (simulates a row vanishing).
    ``swap_writer_at_depth`` — the GET response for memories at this
    depth returns a different ``agent_id`` than was written (simulates a
    silent identity-substitution bug).
    ``truncate_at_depth`` — the GET response for memories at this depth
    returns an empty ``derived_from`` (the chain reads back valid but
    short — a truncation, NOT a fetch error).
    ``cycle`` — every GET response's ``derived_from`` points back at
    itself (simulates a corrupt self-referential provenance link).
    ``fail_write_once_at_depth`` — the first write that arrives at this
    depth returns 500 (a single leaf-write availability failure).
    """
    router = respx.mock(base_url="https://memclaw.test", assert_all_called=False)
    store: dict[str, dict] = {}
    dropped: set[str] = set()
    fired_fail = {"done": False}

    def write_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        meta = body.get("metadata") or {}
        depth = meta.get("depth_index")
        if (
            fail_write_once_at_depth is not None
            and depth == fail_write_once_at_depth
            and not fired_fail["done"]
        ):
            fired_fail["done"] = True
            return httpx.Response(500, json={"detail": "boom"})
        new_id = str(uuid.uuid4())
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
        meta = dict(record.get("metadata") or {})
        depth = meta.get("depth_index")
        if swap_writer_at_depth is not None and depth == swap_writer_at_depth:
            record["agent_id"] = "imposter_agent"
        if cycle:
            meta["derived_from"] = [memory_id]
        elif truncate_at_depth is not None and depth == truncate_at_depth:
            meta["derived_from"] = []
        record["metadata"] = meta
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


@pytest.mark.asyncio
async def test_truncation_decouples_completeness_from_depth_fidelity(
    tmp_config: HarnessConfig,
) -> None:
    """B2: a server that returns a valid-but-short chain (truncation, no
    error) must score full completeness (every link it returned WAS
    reachable) yet sub-1.0 depth fidelity. The two metrics used to collapse
    into one; this proves they're now independent."""
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
    with _make_server(truncate_at_depth=1):
        async with MemClawClient(cfg.base_url, api_key="k", max_retries=1) as client:
            result = await provenance.run(cfg, client=client)

    assert result["completeness_rate"] == 1.0
    assert result["depth_fidelity_rate"] < 1.0


@pytest.mark.asyncio
async def test_cyclic_derived_from_terminates(tmp_config: HarnessConfig) -> None:
    """B3: a self-referential ``derived_from`` must NOT hang the walk. The
    guard stops on the repeated id and grades the chain incomplete. (If the
    guard regressed, this test would hang rather than fail.)"""
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
    with _make_server(cycle=True):
        async with MemClawClient(cfg.base_url, api_key="k", max_retries=1) as client:
            result = await provenance.run(cfg, client=client)

    assert result["completeness_rate"] < 1.0


@pytest.mark.asyncio
async def test_write_failure_excluded_from_accuracy_denominator(
    tmp_config: HarnessConfig,
) -> None:
    """B5: a leaf-write availability failure must NOT be scored as a
    writer-identity substitution. It drops completeness (the chain wasn't
    built) but is excluded from the accuracy denominator, so accuracy stays
    1.0 over the chains that actually reached the comparison."""
    cfg = tmp_config.model_copy(
        update={
            "base_url": "https://memclaw.test",
            "experiment": {
                "n_chains": 3,
                "chain_depth": 2,
                "max_in_flight": 1,
                "tenant_id": "mc_t_test",
                "fleet_id": "fleet_test",
            },
        }
    )
    # Fail exactly one chain's leaf write (depth_index == chain_depth - 1).
    with _make_server(fail_write_once_at_depth=1):
        async with MemClawClient(cfg.base_url, api_key="k", max_retries=1) as client:
            result = await provenance.run(cfg, client=client)

    assert result["completeness_rate"] < 1.0  # the failed chain is incomplete
    assert result["accuracy_n_eval"] == 2  # write-failed chain excluded
    assert result["accuracy_rate"] == 1.0  # ...so accuracy isn't deflated

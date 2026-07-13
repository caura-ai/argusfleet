"""Contradiction experiment against a mocked mem0 backend.

mem0 has no structural detector; the experiment's content-based
adjudication (capabilities.structural_contradiction=False) must:

- score *detected* when the platform's background LLM job deletes or
  rewrites the stale row, and
- score *not detected* (plus a stale read) when both contradictory rows
  are left coexisting — the behaviour observed on the live platform on
  2026-07-13.
"""

from __future__ import annotations

import json
import uuid

import httpx
import pytest
import respx

from argusfleet.experiments import contradiction
from argusfleet.harness.config import HarnessConfig
from argusfleet.harness.mem0_client import Mem0Client

BASE = "https://mem0.test"


def _fake_mem0(resolve_conflicts: bool) -> respx.Router:
    """Stateful mem0 mock.

    Every infer=True add returns the live platform's PENDING stub, then
    'materialises' the row so the client's marker poll finds it. When
    ``resolve_conflicts`` is set, a second add carrying an already-seen
    ``fact_id`` deletes the earlier row (as a compliant conflict
    resolver would); otherwise both rows coexist.
    """
    router = respx.mock(base_url=BASE, assert_all_called=False)
    rows: dict[str, dict] = {}
    seen_fact: dict[str, str] = {}  # fact_id -> earlier row id

    def add_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        meta = body.get("metadata") or {}
        new_id = str(uuid.uuid4())
        rows[new_id] = {
            "id": new_id,
            "memory": body["messages"][0]["content"],
            "user_id": body["user_id"],
            "session_id": body.get("run_id"),
            "metadata": meta,
        }
        fact_id = meta.get("fact_id")
        if fact_id:
            prev = seen_fact.get(fact_id)
            if prev and resolve_conflicts:
                rows.pop(prev, None)  # LLM resolution deletes the stale row
            seen_fact[fact_id] = new_id
        return httpx.Response(
            200,
            json={"results": [{"status": "PENDING", "event_id": new_id}]},
        )

    def get_all_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        conds = (body.get("filters") or {}).get("AND") or []
        marker = None
        user = None
        fact = None
        for c in conds:
            if "user_id" in c:
                user = c["user_id"]
            if isinstance(c.get("metadata"), dict):
                if "_wm" in c["metadata"]:
                    marker = c["metadata"]["_wm"]
                if "fact_id" in c["metadata"]:
                    fact = c["metadata"]["fact_id"]
        out = [
            r
            for r in rows.values()
            if (user is None or r["user_id"] == user)
            and (marker is None or r["metadata"].get("_wm") == marker)
            and (fact is None or r["metadata"].get("fact_id") == fact)
        ]
        return httpx.Response(200, json=out)

    def get_handler(request: httpx.Request) -> httpx.Response:
        mem_id = request.url.path.rstrip("/").rsplit("/", 1)[-1]
        row = rows.get(mem_id)
        if row is None:
            return httpx.Response(404, json={"error": "Memory not found!"})
        return httpx.Response(200, json=row)

    def search_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        conds = (body.get("filters") or {}).get("AND") or []
        user = next((c["user_id"] for c in conds if "user_id" in c), None)
        out = [r for r in rows.values() if user is None or r["user_id"] == user]
        return httpx.Response(200, json=out)

    router.post("/v1/memories/").mock(side_effect=add_handler)
    router.post("/v2/memories/").mock(side_effect=get_all_handler)
    router.post("/v2/memories/search/").mock(side_effect=search_handler)
    router.get(url__regex=r".*/v1/memories/[0-9a-fA-F-]+/$").mock(side_effect=get_handler)
    return router


def _cfg(tmp_config: HarnessConfig) -> HarnessConfig:
    return tmp_config.model_copy(
        update={
            "backend": "mem0",
            "experiment": {
                "n_facts": 2,
                "scenarios": ["sequential"],
                "sequential_delay_ms": 0,
                "settle_delay_ms": 0,
                "write_mode": "strong",
                "max_in_flight": 2,
                "tenant_id": "u_test",
                "fleet_id": "fleet_test",
            },
        }
    )


@pytest.mark.asyncio
async def test_mem0_detection_via_row_deletion(tmp_config: HarnessConfig) -> None:
    cfg = _cfg(tmp_config)
    with _fake_mem0(resolve_conflicts=True):
        async with Mem0Client(
            BASE,
            api_key="k",
            max_retries=1,
            min_write_interval_ms=0.0,
            infer_poll_interval_s=0.01,
            infer_poll_timeout_s=2.0,
        ) as client:
            result = await contradiction.run(cfg, client=client)

    assert result["n_facts_runs"] == 2
    # The mock deletes the earlier row on conflict → the refetch 404s →
    # content-based adjudication scores it detected.
    assert result["detection_rate"] == 1.0
    assert result["stale_read_rate"] == 0.0


@pytest.mark.asyncio
async def test_mem0_no_detection_when_rows_coexist(tmp_config: HarnessConfig) -> None:
    cfg = _cfg(tmp_config)
    with _fake_mem0(resolve_conflicts=False):
        async with Mem0Client(
            BASE,
            api_key="k",
            max_retries=1,
            min_write_interval_ms=0.0,
            infer_poll_interval_s=0.01,
            infer_poll_timeout_s=2.0,
        ) as client:
            result = await contradiction.run(cfg, client=client)

    # Both contradictory rows survive: nothing was deleted or rewritten,
    # so no detection — and the active-rows probe sees both values.
    assert result["detection_rate"] == 0.0
    assert result["stale_read_rate"] == 1.0

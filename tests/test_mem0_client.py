"""Wire-format tests for ``Mem0Client`` using respx.

The fixtures mirror the live api.mem0.ai responses captured on
2026-07-13 (see harness/mem0_client.py module docstring): v1 add returns
``{"results": [...]}``; infer=True adds return a PENDING stub with no
row id; v2 search/get-all return a bare JSON list; GET-by-id returns the
row object; errors carry ``{"error": ...}``.
"""

from __future__ import annotations

import json as _json

import httpx
import pytest
import respx

from argusfleet.harness.backend import BackendError
from argusfleet.harness.mem0_client import Mem0Client

BASE = "https://mem0.test"


def _client(**kwargs) -> Mem0Client:
    kwargs.setdefault("api_key", "m0-test")
    kwargs.setdefault("max_retries", 1)
    kwargs.setdefault("min_write_interval_ms", 0.0)
    return Mem0Client(BASE, **kwargs)


@pytest.mark.asyncio
async def test_write_memory_fast_maps_identifiers() -> None:
    """tenant→user_id, fleet→run_id, writer agent→metadata._writer_agent;
    infer=False; response normalised to {id, content, agent_id}."""
    async with _client() as client:
        with respx.mock(base_url=BASE) as router:
            route = router.post("/v1/memories/").mock(
                return_value=httpx.Response(
                    200,
                    json={
                        "results": [
                            {"id": "mem-1", "event": "ADD", "memory": "hello world"}
                        ],
                        "relations": [],
                    },
                )
            )
            resp = await client.write_memory(
                tenant_id="t1",
                fleet_id="fleet_a",
                agent_id="agent_x",
                content="hello world",
                visibility="scope_team",
                metadata={"canary": "c-1"},
            )
    body = _json.loads(route.calls.last.request.read())
    assert body["user_id"] == "t1"
    assert body["run_id"] == "fleet_a"
    assert "agent_id" not in body  # platform drops it — we never send it
    assert body["infer"] is False
    assert body["metadata"]["_writer_agent"] == "agent_x"
    assert body["metadata"]["_declared_visibility"] == "scope_team"
    assert body["metadata"]["canary"] == "c-1"
    assert route.calls.last.request.headers["Authorization"] == "Token m0-test"
    assert resp.json["id"] == "mem-1"
    assert resp.json["content"] == "hello world"
    assert resp.json["agent_id"] == "agent_x"


@pytest.mark.asyncio
async def test_write_memory_strong_polls_marker() -> None:
    """infer=True (write_mode=strong) returns no row id; the client polls
    v2 get-all on the ``_wm`` marker until the extracted row lands."""
    async with _client(infer_poll_interval_s=0.01, infer_poll_timeout_s=2.0) as client:
        with respx.mock(base_url=BASE) as router:
            router.post("/v1/memories/").mock(
                return_value=httpx.Response(
                    200,
                    json={
                        "results": [
                            {"message": "queued", "status": "PENDING", "event_id": "e1"}
                        ]
                    },
                )
            )
            polls = {"n": 0}

            def _get_all(request: httpx.Request) -> httpx.Response:
                polls["n"] += 1
                if polls["n"] < 3:
                    return httpx.Response(200, json=[])
                body = _json.loads(request.read())
                marker = body["filters"]["AND"][1]["metadata"]["_wm"]
                assert marker  # the client stamped a marker into metadata
                return httpx.Response(
                    200,
                    json=[
                        {
                            "id": "mem-9",
                            "memory": "extracted fact",
                            "user_id": "t1",
                            "metadata": {"_wm": marker, "_writer_agent": "agent_x"},
                        }
                    ],
                )

            router.post("/v2/memories/").mock(side_effect=_get_all)
            resp = await client.write_memory(
                tenant_id="t1",
                agent_id="agent_x",
                content="the region of svc is us-west-2",
                write_mode="strong",
            )
    assert polls["n"] == 3
    assert resp.json["id"] == "mem-9"
    assert resp.json["content"] == "extracted fact"
    assert resp.json["materialized"] is True
    assert resp.extra["materialize_ms"] is not None


@pytest.mark.asyncio
async def test_write_memory_strong_marker_timeout_returns_none_id() -> None:
    """If the marker never surfaces (UPDATE/DELETE/NOOP fold-in), the
    write is not an error — id is None and materialized is False."""
    async with _client(infer_poll_interval_s=0.01, infer_poll_timeout_s=0.05) as client:
        with respx.mock(base_url=BASE) as router:
            router.post("/v1/memories/").mock(
                return_value=httpx.Response(
                    200, json={"results": [{"status": "PENDING", "event_id": "e1"}]}
                )
            )
            router.post("/v2/memories/").mock(
                return_value=httpx.Response(200, json=[])
            )
            resp = await client.write_memory(
                tenant_id="t1", agent_id="a", content="x", write_mode="strong"
            )
    assert resp.json["id"] is None
    assert resp.json["materialized"] is False


@pytest.mark.asyncio
async def test_search_normalises_rows_and_filters() -> None:
    async with _client() as client:
        with respx.mock(base_url=BASE) as router:
            route = router.post("/v2/memories/search/").mock(
                return_value=httpx.Response(
                    200,
                    json=[
                        {
                            "id": "mem-2",
                            "memory": "backup finished",
                            "user_id": "t1",
                            "session_id": "fleet_a",
                            "score": 0.42,
                            "metadata": {"canary": "c-2", "_writer_agent": "agent_y"},
                        }
                    ],
                )
            )
            resp = await client.search_memory(
                tenant_id="t1", query="backup", top_k=5, fleet_ids=["fleet_a"]
            )
    body = _json.loads(route.calls.last.request.read())
    assert body["filters"]["AND"] == [{"user_id": "t1"}, {"run_id": "fleet_a"}]
    assert body["top_k"] == 5
    items = resp.json["items"]
    assert items[0]["id"] == "mem-2"
    assert items[0]["content"] == "backup finished"
    assert items[0]["metadata"]["canary"] == "c-2"
    assert items[0]["agent_id"] == "agent_y"
    assert items[0]["fleet_id"] == "fleet_a"
    assert items[0]["status"] == "active"


@pytest.mark.asyncio
async def test_retrieve_memory_normalises_and_404_raises() -> None:
    async with _client() as client:
        with respx.mock(base_url=BASE) as router:
            router.get("/v1/memories/mem-3/").mock(
                return_value=httpx.Response(
                    200,
                    json={
                        "id": "mem-3",
                        "memory": "row content",
                        "user_id": "t1",
                        "metadata": {"_writer_agent": "agent_z", "derived_from": ["p"]},
                    },
                )
            )
            resp = await client.retrieve_memory("mem-3", tenant_id="ignored")
            assert resp.json["content"] == "row content"
            assert resp.json["agent_id"] == "agent_z"
            assert resp.json["metadata"]["derived_from"] == ["p"]

            router.get("/v1/memories/gone/").mock(
                return_value=httpx.Response(404, json={"error": "Memory not found!"})
            )
            with pytest.raises(BackendError) as exc:
                await client.retrieve_memory("gone")
            assert exc.value.status_code == 404
            assert "not found" in exc.value.message.lower()


@pytest.mark.asyncio
async def test_unsupported_primitives_raise_501() -> None:
    async with _client() as client:
        with pytest.raises(BackendError) as e1:
            await client.upsert_entity(
                tenant_id="t1", entity_type="x", canonical_name="y"
            )
        with pytest.raises(BackendError) as e2:
            await client.propagate_memory(
                memory_id="m", target_agent_id="a", tenant_id="t", requesting_agent_id="r"
            )
    assert e1.value.status_code == 501
    assert e2.value.status_code == 501
    caps = client.capabilities
    assert not caps.entities
    assert not caps.redistribute
    assert not caps.structural_contradiction
    assert not caps.scoped_get


@pytest.mark.asyncio
async def test_delete_all_and_count() -> None:
    async with _client() as client:
        with respx.mock(base_url=BASE) as router:
            del_route = router.delete("/v1/memories/").mock(
                return_value=httpx.Response(
                    200, json={"message": "Delete in progress", "event_id": "e9"}
                )
            )
            router.post("/v2/memories/").mock(
                return_value=httpx.Response(200, json=[])
            )
            resp = await client.delete_all_memories(tenant_id="t1")
            count = await client.count_memories(tenant_id="t1")
    assert del_route.called
    assert "user_id=t1" in str(del_route.calls.last.request.url)
    assert resp.status_code == 200
    assert count.json == {"total": 0}


@pytest.mark.asyncio
async def test_retries_on_429_then_succeeds() -> None:
    calls = {"n": 0}

    def _flaky(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, json={"detail": "rate limited"})
        return httpx.Response(
            200, json={"results": [{"id": "mem-5", "event": "ADD", "memory": "x"}]}
        )

    async with _client(max_retries=3) as client:
        with respx.mock(base_url=BASE) as router:
            router.post("/v1/memories/").mock(side_effect=_flaky)
            resp = await client.write_memory(tenant_id="t1", agent_id="a", content="x")
    assert calls["n"] == 2
    assert resp.json["id"] == "mem-5"
    assert resp.attempt == 2

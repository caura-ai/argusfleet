"""Transport-level tests for ``MemClawClient`` using respx."""

from __future__ import annotations

import httpx
import pytest
import respx

from argusfleet.harness.client import MemClawClient, MemClawError


@pytest.mark.asyncio
async def test_write_memory_happy_path() -> None:
    async with MemClawClient(
        "https://memclaw.test", api_key="k", max_retries=1
    ) as client:
        with respx.mock(base_url="https://memclaw.test") as router:
            route = router.post("/api/v1/memories").mock(
                return_value=httpx.Response(
                    201, json={"id": "11111111-1111-1111-1111-111111111111"}
                )
            )
            resp = await client.write_memory(
                tenant_id="mc_t1", agent_id="a", content="hello"
            )
    assert route.called
    sent = route.calls.last.request
    assert sent.headers["X-API-Key"] == "k"
    assert sent.headers.get("X-Trace-Id")
    assert resp.status_code == 201
    assert resp.json["id"] == "11111111-1111-1111-1111-111111111111"


@pytest.mark.asyncio
async def test_search_clamps_top_k() -> None:
    async with MemClawClient("https://memclaw.test", api_key="k", max_retries=1) as client:
        with respx.mock(base_url="https://memclaw.test") as router:
            route = router.post("/api/v1/search").mock(
                return_value=httpx.Response(200, json={"items": []})
            )
            await client.search_memory(tenant_id="mc_t1", query="q", top_k=200)
    assert route.called
    import json as _json
    body = _json.loads(route.calls.last.request.read())
    assert body["top_k"] == 20


@pytest.mark.asyncio
async def test_retrieve_memory_sends_tenant_id_query_param() -> None:
    """GET /memories/{id} 422s without ?tenant_id — confirm the client
    sends it as a query param when supplied."""
    async with MemClawClient("https://memclaw.test", api_key="k", max_retries=1) as client:
        with respx.mock(base_url="https://memclaw.test") as router:
            route = router.get(
                "/api/v1/memories/abc-123",
                params={"tenant_id": "mc_t1"},
            ).mock(return_value=httpx.Response(200, json={"id": "abc-123"}))
            resp = await client.retrieve_memory("abc-123", tenant_id="mc_t1")
    assert route.called
    assert resp.status_code == 200
    sent_url = route.calls.last.request.url
    assert "tenant_id=mc_t1" in str(sent_url)


@pytest.mark.asyncio
async def test_retrieve_memory_omits_param_when_no_tenant() -> None:
    """If the caller doesn't pass tenant_id, no query string is sent —
    keeps the door open for non-gateway test deployments."""
    async with MemClawClient("https://memclaw.test", api_key="k", max_retries=1) as client:
        with respx.mock(base_url="https://memclaw.test") as router:
            route = router.get("/api/v1/memories/abc-123").mock(
                return_value=httpx.Response(200, json={"id": "abc-123"})
            )
            await client.retrieve_memory("abc-123")
    assert route.called
    assert route.calls.last.request.url.query == b""


@pytest.mark.asyncio
async def test_propagate_memory_sends_required_query_params() -> None:
    """Redistribute requires tenant_id + agent_id query params (trust
    level ≥ 3 is enforced server-side; client just has to deliver the
    args)."""
    async with MemClawClient("https://memclaw.test", api_key="k", max_retries=1) as client:
        with respx.mock(base_url="https://memclaw.test") as router:
            route = router.post("/api/v1/memories/redistribute").mock(
                return_value=httpx.Response(
                    200,
                    json={
                        "moved": 1, "promoted": 0, "skipped": 0,
                        "errors": [], "redistribute_ms": 5,
                    },
                )
            )
            await client.propagate_memory(
                memory_id="abc-123",
                target_agent_id="target",
                tenant_id="mc_t1",
                requesting_agent_id="caller",
            )
    sent = route.calls.last.request
    query_string = sent.url.query.decode()
    assert "tenant_id=mc_t1" in query_string
    assert "agent_id=caller" in query_string
    import json as _json
    body = _json.loads(sent.read())
    assert body == {"memory_ids": ["abc-123"], "target_agent_id": "target"}


@pytest.mark.asyncio
async def test_write_memory_forwards_rdf_triple_and_write_mode() -> None:
    """Contradiction uses RDF triples + write_mode=strong to force the
    server's detection path. Confirm those flow through the body."""
    async with MemClawClient("https://memclaw.test", api_key="k", max_retries=1) as client:
        with respx.mock(base_url="https://memclaw.test") as router:
            route = router.post("/api/v1/memories").mock(
                return_value=httpx.Response(201, json={"id": "1"})
            )
            await client.write_memory(
                tenant_id="mc_t1",
                agent_id="a",
                content="x",
                subject_entity_id="00000000-0000-0000-0000-000000000001",
                predicate="lives_in",
                object_value="seattle",
                write_mode="strong",
            )
    import json as _json
    body = _json.loads(route.calls.last.request.read())
    assert body["predicate"] == "lives_in"
    assert body["object_value"] == "seattle"
    assert body["write_mode"] == "strong"


@pytest.mark.asyncio
async def test_upsert_entity_sends_canonical_name_and_type() -> None:
    """The contradiction experiment uses upsert_entity to materialise a
    subject_entity_id before writing — confirm the body shape."""
    async with MemClawClient("https://memclaw.test", api_key="k", max_retries=1) as client:
        with respx.mock(base_url="https://memclaw.test") as router:
            route = router.post("/api/v1/entities/upsert").mock(
                return_value=httpx.Response(
                    200,
                    json={
                        "id": "ent-1",
                        "tenant_id": "mc_t1",
                        "entity_type": "system_resource",
                        "canonical_name": "service-foo",
                    },
                )
            )
            resp = await client.upsert_entity(
                tenant_id="mc_t1",
                entity_type="system_resource",
                canonical_name="service-foo",
                attributes={"k": "v"},
            )
    import json as _json
    body = _json.loads(route.calls.last.request.read())
    assert body["tenant_id"] == "mc_t1"
    assert body["entity_type"] == "system_resource"
    assert body["canonical_name"] == "service-foo"
    assert body["attributes"] == {"k": "v"}
    assert resp.json["id"] == "ent-1"


@pytest.mark.asyncio
async def test_delete_all_memories_sends_tenant_and_returns_204() -> None:
    """Bulk tenant purge: DELETE /memories?tenant_id=... with no extra
    filters clears the whole tenant. Server replies 204 (no body)."""
    async with MemClawClient("https://memclaw.test", api_key="k", max_retries=1) as client:
        with respx.mock(base_url="https://memclaw.test") as router:
            route = router.delete("/api/v1/memories").mock(
                return_value=httpx.Response(204)
            )
            resp = await client.delete_all_memories(tenant_id="mc_t1")
    assert route.called
    sent = route.calls.last.request
    assert "tenant_id=mc_t1" in sent.url.query.decode()
    assert resp.status_code == 204
    assert resp.json is None


@pytest.mark.asyncio
async def test_delete_all_memories_forwards_filters_and_body() -> None:
    """Optional scoping (fleet/agent) rides as query params; exclude_ids
    and metadata_filter ride in the JSON body."""
    async with MemClawClient("https://memclaw.test", api_key="k", max_retries=1) as client:
        with respx.mock(base_url="https://memclaw.test") as router:
            route = router.delete("/api/v1/memories").mock(
                return_value=httpx.Response(204)
            )
            await client.delete_all_memories(
                tenant_id="mc_t1",
                fleet_id="fleet-a",
                exclude_ids=["00000000-0000-0000-0000-000000000009"],
                metadata_filter={"run_id": "abc"},
            )
    sent = route.calls.last.request
    query = sent.url.query.decode()
    assert "tenant_id=mc_t1" in query
    assert "fleet_id=fleet-a" in query
    import json as _json
    body = _json.loads(sent.read())
    assert body["exclude_ids"] == ["00000000-0000-0000-0000-000000000009"]
    assert body["metadata_filter"] == {"run_id": "abc"}


@pytest.mark.asyncio
async def test_count_memories_hits_stats_endpoint() -> None:
    async with MemClawClient("https://memclaw.test", api_key="k", max_retries=1) as client:
        with respx.mock(base_url="https://memclaw.test") as router:
            route = router.get("/api/v1/memories/stats").mock(
                return_value=httpx.Response(200, json={"total": 7})
            )
            resp = await client.count_memories(tenant_id="mc_t1", include_deleted=True)
    sent = route.calls.last.request
    query = sent.url.query.decode()
    assert "tenant_id=mc_t1" in query
    assert "include_deleted=true" in query
    assert resp.json["total"] == 7


@pytest.mark.asyncio
async def test_write_rate_limiter_spaces_concurrent_writes() -> None:
    """The shared write limiter must hold the aggregate write rate under
    the server's 10/s ceiling even when many writes are dispatched
    concurrently. With a 50 ms interval, 5 writes fired at once should
    take at least 4 intervals (200 ms) of wall-clock to clear."""
    import asyncio
    import time as _time

    async with MemClawClient(
        "https://memclaw.test", api_key="k", max_retries=1, min_write_interval_ms=50.0
    ) as client:
        with respx.mock(base_url="https://memclaw.test") as router:
            router.post("/api/v1/memories").mock(
                return_value=httpx.Response(201, json={"id": "1"})
            )
            started = _time.monotonic()
            await asyncio.gather(
                *(
                    client.write_memory(tenant_id="mc_t1", agent_id="a", content=f"c{i}")
                    for i in range(5)
                )
            )
            elapsed = _time.monotonic() - started
    assert elapsed >= 0.20, f"writes not throttled (elapsed={elapsed:.3f}s)"


@pytest.mark.asyncio
async def test_write_rate_limiter_disabled_when_interval_zero() -> None:
    """An interval of 0 disables throttling — writes fire immediately."""
    import asyncio
    import time as _time

    async with MemClawClient(
        "https://memclaw.test", api_key="k", max_retries=1, min_write_interval_ms=0.0
    ) as client:
        with respx.mock(base_url="https://memclaw.test") as router:
            router.post("/api/v1/memories").mock(
                return_value=httpx.Response(201, json={"id": "1"})
            )
            started = _time.monotonic()
            await asyncio.gather(
                *(
                    client.write_memory(tenant_id="mc_t1", agent_id="a", content=f"c{i}")
                    for i in range(5)
                )
            )
            elapsed = _time.monotonic() - started
    assert elapsed < 0.20, f"unexpected throttling with interval=0 (elapsed={elapsed:.3f}s)"


@pytest.mark.asyncio
async def test_4xx_does_not_retry() -> None:
    async with MemClawClient(
        "https://memclaw.test", api_key="k", max_retries=3
    ) as client:
        with respx.mock(base_url="https://memclaw.test") as router:
            route = router.post("/api/v1/memories").mock(
                return_value=httpx.Response(403, json={"detail": "trust_level<1"})
            )
            with pytest.raises(MemClawError) as exc:
                await client.write_memory(
                    tenant_id="mc_t1", agent_id="a", content="x"
                )
    assert exc.value.status_code == 403
    # The retry loop must not have fired more than once on a 4xx.
    assert route.call_count == 1


@pytest.mark.asyncio
async def test_persistent_5xx_preserves_status_code() -> None:
    """B8: a 5xx that survives all retries must surface as MemClawError with
    the real status (e.g. 503), not the generic transport status 0 — so the
    JSONL artifacts can tell a server error from a connection failure."""
    async with MemClawClient(
        "https://memclaw.test", api_key="k", max_retries=3, min_write_interval_ms=0.0
    ) as client:
        with respx.mock(base_url="https://memclaw.test") as router:
            route = router.post("/api/v1/memories").mock(
                return_value=httpx.Response(503, json={"detail": "overloaded"})
            )
            with pytest.raises(MemClawError) as exc:
                await client.write_memory(tenant_id="mc_t1", agent_id="a", content="x")
    assert exc.value.status_code == 503
    assert route.call_count == 3  # retried to exhaustion


@pytest.mark.asyncio
async def test_connection_failure_reports_status_zero() -> None:
    """A genuine transport failure (no HTTP response) still reports status 0,
    distinct from a server 5xx (B8)."""
    async with MemClawClient(
        "https://memclaw.test", api_key="k", max_retries=2, min_write_interval_ms=0.0
    ) as client:
        with respx.mock(base_url="https://memclaw.test") as router:
            router.post("/api/v1/memories").mock(
                side_effect=httpx.ConnectError("refused")
            )
            with pytest.raises(MemClawError) as exc:
                await client.write_memory(tenant_id="mc_t1", agent_id="a", content="x")
    assert exc.value.status_code == 0


@pytest.mark.asyncio
async def test_rate_limiter_reacquired_on_each_retry() -> None:
    """B9: one logical write that re-fires its POST under a transient 5xx
    must re-acquire a limiter slot per attempt — otherwise retries bypass the
    per-tenant write ceiling the limiter exists to respect."""
    async with MemClawClient(
        "https://memclaw.test", api_key="k", max_retries=3, min_write_interval_ms=0.0
    ) as client:
        acquires = {"n": 0}
        orig_acquire = client._write_limiter.acquire

        async def counting_acquire() -> None:
            acquires["n"] += 1
            await orig_acquire()

        client._write_limiter.acquire = counting_acquire  # type: ignore[method-assign]

        with respx.mock(base_url="https://memclaw.test") as router:
            # First attempt 503 (retryable), second attempt succeeds.
            route = router.post("/api/v1/memories").mock(
                side_effect=[
                    httpx.Response(503, json={"detail": "busy"}),
                    httpx.Response(201, json={"id": "1"}),
                ]
            )
            await client.write_memory(tenant_id="mc_t1", agent_id="a", content="x")

    assert route.call_count == 2  # POST re-fired on the retry
    assert acquires["n"] == 2  # ...and a slot was re-acquired for it

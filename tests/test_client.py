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

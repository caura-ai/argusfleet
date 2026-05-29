"""Async REST client for the memclaw.net memory service.

The client is intentionally thin: it speaks the upstream API verbatim and
returns ``MemClawResponse`` envelopes that capture both the JSON payload
and timing/trace metadata the experiments need.

Higher-level harness logic (governance checks, scope translation,
contradiction accounting) lives in the experiments, not here — keeping
the client minimal makes it easy to swap transports later.
"""

from __future__ import annotations

import logging
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

TRACE_HEADER = "X-Trace-Id"
API_KEY_HEADER = "X-API-Key"


class MemClawError(RuntimeError):
    """Raised when the server returns a non-2xx response after retries."""

    def __init__(self, status_code: int, message: str, trace_id: str | None = None) -> None:
        super().__init__(f"[{status_code}] {message} (trace={trace_id})")
        self.status_code = status_code
        self.message = message
        self.trace_id = trace_id


@dataclass(slots=True)
class MemClawResponse:
    """Wire response + timing envelope.

    Experiments store these directly into JSONL logs so latency analysis
    can be reproduced from artifacts alone, without re-hitting the server.
    """

    status_code: int
    json: Any
    latency_ms: float
    trace_id: str
    attempt: int = 1
    extra: dict[str, Any] = field(default_factory=dict)


# Retry on transient transport errors and 5xx — but never on 4xx, since
# those are governance/validation responses the experiments are measuring.
_RETRYABLE_EXC = (httpx.TransportError, httpx.ReadTimeout, httpx.ConnectTimeout)


class MemClawClient:
    """Async client for memclaw.net REST API (``/api/v1/...``).

    Parameters
    ----------
    base_url:
        e.g. ``https://memclaw.net``. ``/api/v1`` is appended automatically.
    api_key:
        Sent as the ``X-API-Key`` header on every request.
    timeout_s:
        Per-request HTTP timeout.
    max_retries:
        Maximum number of attempts per call (1 = no retry).
    """

    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        *,
        timeout_s: float = 30.0,
        max_retries: int = 3,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/") + "/api/v1"
        self._api_key = api_key
        self._max_retries = max(1, max_retries)
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_s),
            headers={"User-Agent": "argusfleet/0.1"},
        )

    # ------------------------------------------------------------------ #
    # Lifecycle                                                           #
    # ------------------------------------------------------------------ #

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> "MemClawClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    # ------------------------------------------------------------------ #
    # Domain operations                                                   #
    # ------------------------------------------------------------------ #

    async def write_memory(
        self,
        *,
        tenant_id: str,
        agent_id: str,
        content: str,
        fleet_id: str | None = None,
        visibility: str | None = None,
        metadata: dict[str, Any] | None = None,
        memory_type: str | None = None,
        idempotency_key: str | None = None,
        # RDF triple — used by the contradiction experiment to force
        # deterministic conflict detection without relying on embeddings.
        subject_entity_id: str | None = None,
        predicate: str | None = None,
        object_value: str | None = None,
        # ``write_mode="strong"`` forces the full enrichment pipeline,
        # including contradiction detection. ``"fast"`` skips it.
        write_mode: str | None = None,
    ) -> MemClawResponse:
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "agent_id": agent_id,
            "content": content,
        }
        if fleet_id is not None:
            body["fleet_id"] = fleet_id
        if visibility is not None:
            body["visibility"] = visibility
        if metadata is not None:
            body["metadata"] = metadata
        if memory_type is not None:
            body["memory_type"] = memory_type
        if subject_entity_id is not None:
            body["subject_entity_id"] = subject_entity_id
        if predicate is not None:
            body["predicate"] = predicate
        if object_value is not None:
            body["object_value"] = object_value
        if write_mode is not None:
            body["write_mode"] = write_mode

        headers: dict[str, str] = {}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key

        return await self._request("POST", "/memories", json=body, extra_headers=headers)

    async def search_memory(
        self,
        *,
        tenant_id: str,
        query: str,
        top_k: int = 10,
        fleet_ids: list[str] | None = None,
        filter_agent_id: str | None = None,
        memory_type_filter: str | None = None,
        status_filter: str | None = None,
    ) -> MemClawResponse:
        # REST caps top_k at 20 — clamp client-side so the experiment
        # reports its own ceiling rather than getting an opaque 422.
        if top_k > 20:
            logger.debug("Clamping top_k from %d to 20 (REST cap)", top_k)
            top_k = 20

        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "query": query,
            "top_k": top_k,
        }
        if fleet_ids is not None:
            body["fleet_ids"] = fleet_ids
        if filter_agent_id is not None:
            body["filter_agent_id"] = filter_agent_id
        if memory_type_filter is not None:
            body["memory_type_filter"] = memory_type_filter
        if status_filter is not None:
            body["status_filter"] = status_filter
        return await self._request("POST", "/search", json=body)

    async def retrieve_memory(
        self,
        memory_id: str,
        *,
        tenant_id: str | None = None,
    ) -> MemClawResponse:
        # The upstream ``GET /memories/{id}`` requires ``tenant_id`` as a
        # query parameter — without it the server responds 422
        # ``Field required`` instead of returning the row. Callers that
        # don't know the tenant can omit it, but it will fail on the
        # production gateway.
        params = {"tenant_id": tenant_id} if tenant_id else None
        return await self._request("GET", f"/memories/{memory_id}", params=params)

    async def update_memory(
        self,
        memory_id: str,
        *,
        content: str | None = None,
        status: str | None = None,
        visibility: str | None = None,
        metadata: dict[str, Any] | None = None,
        metadata_mode: str | None = None,
    ) -> MemClawResponse:
        body: dict[str, Any] = {}
        if content is not None:
            body["content"] = content
        if status is not None:
            body["status"] = status
        if visibility is not None:
            body["visibility"] = visibility
        if metadata is not None:
            body["metadata"] = metadata
        if metadata_mode is not None:
            body["metadata_mode"] = metadata_mode
        return await self._request("PATCH", f"/memories/{memory_id}", json=body)

    async def upsert_entity(
        self,
        *,
        tenant_id: str,
        entity_type: str,
        canonical_name: str,
        fleet_id: str | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> MemClawResponse:
        """Create or look up an entity by ``(tenant_id, canonical_name)``.

        The contradiction experiment uses this to materialise the
        ``subject_entity_id`` it then references on writes — pointing two
        contradictory writes at the same entity ID is what triggers the
        server's *structural* contradiction detector (which compares RDF
        triples deterministically) rather than the embedding-similarity
        path that 409s contradictions as near-duplicates.
        """
        body: dict[str, Any] = {
            "tenant_id": tenant_id,
            "entity_type": entity_type,
            "canonical_name": canonical_name,
        }
        if fleet_id is not None:
            body["fleet_id"] = fleet_id
        if attributes is not None:
            body["attributes"] = attributes
        return await self._request("POST", "/entities/upsert", json=body)

    async def delete_all_memories(
        self,
        *,
        tenant_id: str,
        fleet_id: str | None = None,
        agent_id: str | None = None,
        memory_type: str | None = None,
        status: str | None = None,
        exclude_ids: list[str] | None = None,
        metadata_filter: dict[str, str] | None = None,
    ) -> MemClawResponse:
        """Soft-delete every matching memory in a tenant in one round-trip.

        Maps to ``DELETE /api/v1/memories?tenant_id=...``. With no extra
        filters this clears the whole tenant. Soft-delete sets
        ``deleted_at``; the server's dedup detectors all filter
        ``deleted_at IS NULL``, so a purge fully releases the
        near-duplicate gate that otherwise 409s repeated experiment runs.

        Requires a key with ``write`` capability scoped to ``tenant_id``
        (no trust level needed). Returns the 204 envelope on success.
        """
        params: dict[str, Any] = {"tenant_id": tenant_id}
        if fleet_id is not None:
            params["fleet_id"] = fleet_id
        if agent_id is not None:
            params["agent_id"] = agent_id
        if memory_type is not None:
            params["memory_type"] = memory_type
        if status is not None:
            params["status"] = status
        body: dict[str, Any] = {}
        if exclude_ids:
            body["exclude_ids"] = exclude_ids
        if metadata_filter:
            body["metadata_filter"] = metadata_filter
        return await self._request(
            "DELETE", "/memories", params=params, json=body or None
        )

    async def count_memories(
        self,
        *,
        tenant_id: str,
        include_deleted: bool = False,
    ) -> MemClawResponse:
        """Aggregate memory counts for a tenant via ``GET /memories/stats``.

        Used to verify a purge landed (``total`` should drop to 0) without
        paginating the full list.
        """
        params: dict[str, Any] = {
            "tenant_id": tenant_id,
            "include_deleted": str(include_deleted).lower(),
        }
        return await self._request("GET", "/memories/stats", params=params)

    async def propagate_memory(
        self,
        *,
        memory_id: str,
        target_agent_id: str,
        tenant_id: str,
        requesting_agent_id: str,
    ) -> MemClawResponse:
        """Cross-agent propagation via the redistribute endpoint.

        The upstream ``/memories/redistribute`` re-homes existing memories
        onto a different agent within the same tenant. The endpoint
        requires ``tenant_id`` and ``agent_id`` as **query parameters**,
        and the requesting agent must have ``trust_level >= 3`` — a
        regular harness-fabricated agent will be rejected with 403.
        """
        body = {"memory_ids": [memory_id], "target_agent_id": target_agent_id}
        params = {"tenant_id": tenant_id, "agent_id": requesting_agent_id}
        return await self._request(
            "POST", "/memories/redistribute", json=body, params=params
        )

    # ------------------------------------------------------------------ #
    # Transport                                                           #
    # ------------------------------------------------------------------ #

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> MemClawResponse:
        url = f"{self._base_url}{path}"
        trace_id = uuid.uuid4().hex
        headers: dict[str, str] = {TRACE_HEADER: trace_id}
        if self._api_key:
            headers[API_KEY_HEADER] = self._api_key
        if extra_headers:
            headers.update(extra_headers)

        attempt_holder = {"n": 0}

        async def _do() -> httpx.Response:
            attempt_holder["n"] += 1
            return await self._client.request(
                method, url, json=json, params=params, headers=headers
            )

        started = time.perf_counter()
        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(self._max_retries),
                wait=wait_exponential(multiplier=0.25, max=4.0),
                retry=retry_if_exception_type(_RETRYABLE_EXC),
                reraise=True,
            ):
                with attempt:
                    resp = await _do()
                    if resp.status_code >= 500:
                        # Convert 5xx into a retryable transport-like error.
                        raise httpx.TransportError(
                            f"server {resp.status_code}: {resp.text[:200]}"
                        )
        except _RETRYABLE_EXC as e:
            elapsed = (time.perf_counter() - started) * 1000.0
            raise MemClawError(0, f"transport failure: {e}", trace_id=trace_id) from e

        elapsed_ms = (time.perf_counter() - started) * 1000.0
        try:
            payload = resp.json() if resp.content else None
        except ValueError:
            payload = {"_raw": resp.text}

        if resp.status_code >= 400:
            detail = (
                payload.get("detail")
                if isinstance(payload, dict)
                else (payload or resp.text or "")
            )
            raise MemClawError(resp.status_code, str(detail), trace_id=trace_id)

        return MemClawResponse(
            status_code=resp.status_code,
            json=payload,
            latency_ms=elapsed_ms,
            trace_id=trace_id,
            attempt=attempt_holder["n"],
        )


@asynccontextmanager
async def memclaw_client(
    base_url: str, api_key: str | None, **kwargs: Any
) -> AsyncIterator[MemClawClient]:
    """Convenience async context manager for ``MemClawClient``."""
    client = MemClawClient(base_url, api_key=api_key, **kwargs)
    try:
        yield client
    finally:
        await client.aclose()

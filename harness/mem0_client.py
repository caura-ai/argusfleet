"""Async REST client for the mem0 platform (``api.mem0.ai``).

Implements the :class:`~argusfleet.harness.backend.MemoryBackend` surface
so the four experiments run unchanged against mem0. The client normalises
mem0's wire format into the row shape experiments read (``id``,
``content``, ``agent_id``, ``metadata``, ``status``; searches return
``{"items": [...]}``).

Identifier mapping (verified against the live API, 2026-07-13)
--------------------------------------------------------------
mem0 persists exactly two filterable identifiers on a user-owned memory:
``user_id`` and ``run_id`` (stored/echoed as ``session_id``). An
``agent_id`` sent alongside ``user_id`` is **silently dropped** — it is
neither stored nor filterable. The harness therefore maps:

- ``tenant_id``  → ``user_id``   (primary partition)
- ``fleet_id``   → ``run_id``    (the only filterable sub-partition)
- ``agent_id``   → ``metadata["_writer_agent"]`` (no native writer field)

Governance semantics differ fundamentally from MemClaw: mem0's enforced
boundary is the org/project bound to the API key. Below it, ``user_id``
and ``run_id`` act as *query filters chosen by the caller*, not
server-enforced scopes, and ``GET /v1/memories/{id}`` accepts no caller
identity at all (``scoped_get=False``). The leakage experiment measures
exactly this.

Write modes
-----------
- default / ``fast`` → ``infer=False``: synchronous, stores content
  verbatim, returns the row id in the response.
- ``strong`` → ``infer=True``: the platform queues LLM fact extraction
  and conflict resolution in the background and returns **no row id**.
  The client stamps a unique ``_wm`` marker into metadata and polls
  ``POST /v2/memories/`` (filtered on the marker) until the extracted
  row materialises or ``infer_poll_timeout_s`` elapses. The response's
  ``latency_ms`` stays the HTTP POST latency; materialisation lag is
  reported separately in ``extra["materialize_ms"]``.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from argusfleet.harness.backend import (
    BackendCapabilities,
    BackendError,
    BackendResponse,
)

logger = logging.getLogger(__name__)

_RETRYABLE_EXC = (httpx.TransportError, httpx.ReadTimeout, httpx.ConnectTimeout)


class _RetryableStatus(httpx.TransportError):
    """A retryable HTTP status (429 / 5xx) tagged with its originating code."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


class _WriteRateLimiter:
    """Spaces write requests (same slot-assignment design as the MemClaw
    client's limiter: the lock only assigns a slot; the wait happens after
    release so slot assignment never serialises the actual HTTP)."""

    def __init__(self, min_interval_s: float) -> None:
        self._min_interval = max(0.0, min_interval_s)
        self._lock = asyncio.Lock()
        self._next_slot = 0.0

    async def acquire(self) -> None:
        if self._min_interval <= 0:
            return
        async with self._lock:
            now = time.monotonic()
            slot = max(now, self._next_slot)
            self._next_slot = slot + self._min_interval
            wait = slot - now
        if wait > 0:
            await asyncio.sleep(wait)


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    """Map a mem0 memory object onto the row shape experiments read."""
    meta = row.get("metadata") or {}
    return {
        "id": row.get("id"),
        "content": row.get("memory"),
        # mem0 has no per-writer field on user-owned rows; the harness
        # stashes the writer into metadata on write and surfaces it here
        # so provenance's writer-identity check works unchanged.
        "agent_id": meta.get("_writer_agent"),
        "fleet_id": row.get("session_id"),
        "tenant_id": row.get("user_id"),
        "metadata": meta,
        # mem0 rows have no lifecycle status; an existing row is active
        # (a deleted row 404s instead of flipping status).
        "status": "active",
        "supersedes_id": None,
        "score": row.get("score"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }


class Mem0Client:
    """Async client for the mem0 platform REST API (``/v1`` + ``/v2``)."""

    capabilities = BackendCapabilities(
        name="mem0",
        scoped_get=False,
        visibility_scopes=False,
        entities=False,
        structural_contradiction=False,
        redistribute=False,
        status_filter=False,
        sync_strong_writes=False,
    )

    def __init__(
        self,
        base_url: str = "https://api.mem0.ai",
        api_key: str | None = None,
        *,
        timeout_s: float = 30.0,
        max_retries: int = 3,
        min_write_interval_ms: float = 125.0,
        min_read_interval_ms: float = 0.0,
        infer_poll_interval_s: float = 1.5,
        infer_poll_timeout_s: float = 30.0,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._max_retries = max(1, max_retries)
        self._write_limiter = _WriteRateLimiter(min_write_interval_ms / 1000.0)
        # A SEPARATE limiter on reads. mem0's retrieval endpoints (/v2
        # search + get-all) are metered against a monthly retrieval quota
        # AND a burst limit; a polling-heavy experiment (propagation) will
        # trip a 429 without spacing. 0 (default) disables it so unit tests
        # and MemClaw-style callers pay nothing; experiments set it.
        self._read_limiter = _WriteRateLimiter(min_read_interval_ms / 1000.0)
        self._infer_poll_interval_s = infer_poll_interval_s
        self._infer_poll_timeout_s = infer_poll_timeout_s
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

    async def __aenter__(self) -> "Mem0Client":
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
        visibility: str | None = None,  # no equivalent — recorded, not enforced
        metadata: dict[str, Any] | None = None,
        memory_type: str | None = None,  # no equivalent
        idempotency_key: str | None = None,  # no equivalent
        subject_entity_id: str | None = None,
        predicate: str | None = None,
        object_value: str | None = None,
        write_mode: str | None = None,
    ) -> BackendResponse:
        infer = write_mode == "strong"
        meta: dict[str, Any] = dict(metadata or {})
        meta["_writer_agent"] = agent_id
        if visibility is not None:
            # Recorded so ground truth survives in artifacts; mem0 does not
            # enforce it (capabilities.visibility_scopes=False).
            meta["_declared_visibility"] = visibility
        if subject_entity_id is not None:
            meta["_subject_entity_id"] = subject_entity_id
        if predicate is not None:
            meta["_predicate"] = predicate
        if object_value is not None:
            meta["_object_value"] = object_value

        marker: str | None = None
        if infer:
            marker = uuid.uuid4().hex
            meta["_wm"] = marker

        body: dict[str, Any] = {
            "messages": [{"role": "user", "content": content}],
            "user_id": tenant_id,
            "metadata": meta,
            "infer": infer,
            "output_format": "v1.1",
        }
        if fleet_id is not None:
            body["run_id"] = fleet_id

        resp = await self._request("POST", "/v1/memories/", json=body, rate_limit=True)
        payload = resp.json if isinstance(resp.json, dict) else {}
        results = payload.get("results") or []

        if not infer:
            first = results[0] if results and isinstance(results[0], dict) else {}
            resp.json = {
                "id": first.get("id"),
                "event": first.get("event"),
                "content": first.get("memory"),
                "agent_id": agent_id,
                "supersedes_id": None,
            }
            return resp

        # infer=True: queued for background extraction — poll the marker.
        materialized, materialize_ms = await self._poll_marker(tenant_id, marker or "")
        resp.json = {
            "id": materialized.get("id") if materialized else None,
            "content": materialized.get("content") if materialized else None,
            "agent_id": agent_id,
            "supersedes_id": None,
            "queued": True,
            "materialized": materialized is not None,
        }
        resp.extra["materialize_ms"] = materialize_ms
        return resp

    async def _poll_marker(
        self, tenant_id: str, marker: str
    ) -> tuple[dict[str, Any] | None, float | None]:
        """Poll get-all filtered on the write marker until the extracted
        row lands. Returns ``(normalized_row | None, elapsed_ms | None)``.

        ``None`` means the marker never surfaced — either extraction is
        still pending past the timeout, the platform folded the write into
        an UPDATE/DELETE of an existing row, or the LLM deemed the content
        not memorable (NOOP). Callers adjudicate from the surrounding
        state; the write is not an error.
        """
        t0 = time.perf_counter()
        deadline = t0 + self._infer_poll_timeout_s
        while True:
            try:
                r = await self._request(
                    "POST",
                    "/v2/memories/",
                    json={
                        "filters": {
                            "AND": [
                                {"user_id": tenant_id},
                                {"metadata": {"_wm": marker}},
                            ]
                        }
                    },
                    rate_limit_read=True,
                )
                items = r.json if isinstance(r.json, list) else []
                if items:
                    elapsed_ms = (time.perf_counter() - t0) * 1000.0
                    return _normalize_row(items[0]), elapsed_ms
            except BackendError as e:
                logger.debug("marker poll error (continuing): %s", e)
            if time.perf_counter() >= deadline:
                return None, None
            await asyncio.sleep(self._infer_poll_interval_s)

    async def search_memory(
        self,
        *,
        tenant_id: str,
        query: str,
        top_k: int = 10,
        fleet_ids: list[str] | None = None,
        filter_agent_id: str | None = None,
        memory_type_filter: str | None = None,  # no equivalent — ignored
        status_filter: str | None = None,  # no equivalent — ignored
    ) -> BackendResponse:
        conditions: list[dict[str, Any]] = [{"user_id": tenant_id}]
        if fleet_ids:
            if len(fleet_ids) == 1:
                conditions.append({"run_id": fleet_ids[0]})
            else:
                conditions.append({"run_id": {"in": list(fleet_ids)}})
        if filter_agent_id is not None:
            conditions.append({"metadata": {"_writer_agent": filter_agent_id}})

        resp = await self._request(
            "POST",
            "/v2/memories/search/",
            json={"query": query, "filters": {"AND": conditions}, "top_k": top_k},
            rate_limit_read=True,
        )
        rows = resp.json if isinstance(resp.json, list) else []
        resp.json = {"items": [_normalize_row(r) for r in rows if isinstance(r, dict)]}
        return resp

    async def list_memories(
        self,
        *,
        tenant_id: str,
        metadata_filter: dict[str, Any] | None = None,
        fleet_id: str | None = None,
        top_k: int = 100,
    ) -> BackendResponse:
        """Enumerate rows via ``POST /v2/memories/`` (get-all) with exact
        filters --- no embedding ranking involved.

        This is the reliable way to check contradiction outcomes on a
        content-driven backend: filtering on ``metadata.fact_id`` returns
        every row carrying that fact regardless of how the platform's LLM
        rephrased the content, so we can count coexisting object values
        without depending on search recall. Response normalised to
        ``{"items": [...]}``.
        """
        conditions: list[dict[str, Any]] = [{"user_id": tenant_id}]
        if fleet_id is not None:
            conditions.append({"run_id": fleet_id})
        if metadata_filter:
            for k, v in metadata_filter.items():
                conditions.append({"metadata": {k: v}})
        resp = await self._request(
            "POST",
            "/v2/memories/",
            json={"filters": {"AND": conditions}, "page_size": top_k},
            rate_limit_read=True,
        )
        rows = resp.json if isinstance(resp.json, list) else []
        resp.json = {"items": [_normalize_row(r) for r in rows if isinstance(r, dict)]}
        return resp

    async def retrieve_memory(
        self,
        memory_id: str,
        *,
        tenant_id: str | None = None,
    ) -> BackendResponse:
        # mem0's GET-by-id accepts no caller identity: any key holder in
        # the project reads any row (scoped_get=False). ``tenant_id`` is
        # accepted for interface compatibility and intentionally NOT sent —
        # there is nothing to send it as. The leakage experiment's access
        # axis measures precisely this.
        resp = await self._request(
            "GET", f"/v1/memories/{memory_id}/", rate_limit_read=True
        )
        if isinstance(resp.json, dict):
            resp.json = _normalize_row(resp.json)
        return resp

    async def update_memory(
        self,
        memory_id: str,
        *,
        content: str | None = None,
        status: str | None = None,  # no equivalent — ignored
        visibility: str | None = None,  # no equivalent — ignored
        metadata: dict[str, Any] | None = None,  # PUT /v1 updates text only
        metadata_mode: str | None = None,
    ) -> BackendResponse:
        if content is None:
            raise BackendError(400, "mem0 update supports content ('text') only")
        resp = await self._request(
            "PUT", f"/v1/memories/{memory_id}/", json={"text": content}, rate_limit=True
        )
        if isinstance(resp.json, dict):
            resp.json = _normalize_row(resp.json)
        return resp

    async def upsert_entity(self, **_: Any) -> BackendResponse:
        raise BackendError(501, "mem0 has no entity primitive (capabilities.entities=False)")

    async def propagate_memory(self, **_: Any) -> BackendResponse:
        raise BackendError(
            501, "mem0 has no redistribute/ownership-transfer primitive"
        )

    async def delete_all_memories(
        self,
        *,
        tenant_id: str,
        fleet_id: str | None = None,
        agent_id: str | None = None,
        **_: Any,
    ) -> BackendResponse:
        """Delete every memory for ``tenant_id`` (mem0 ``user_id``).

        The platform processes the delete asynchronously ("Delete in
        progress"); this method then polls the count down to zero (best
        effort, ~15 s) so a pre-run purge has actually landed before the
        experiment starts writing.
        """
        params: dict[str, Any] = {"user_id": tenant_id}
        if fleet_id is not None:
            params["run_id"] = fleet_id
        resp = await self._request("DELETE", "/v1/memories/", params=params, rate_limit=True)
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            try:
                c = await self.count_memories(tenant_id=tenant_id)
                if int((c.json or {}).get("total", 0)) == 0:
                    break
            except BackendError:
                break
            await asyncio.sleep(1.0)
        return resp

    async def count_memories(
        self,
        *,
        tenant_id: str,
        include_deleted: bool = False,  # mem0 exposes no deleted rows
    ) -> BackendResponse:
        # No count endpoint — enumerate via get-all. Fine at harness scale
        # (tens of rows per tenant); NOT a general-purpose count.
        resp = await self._request(
            "POST",
            "/v2/memories/",
            json={"filters": {"AND": [{"user_id": tenant_id}]}, "page_size": 100},
            rate_limit_read=True,
        )
        rows = resp.json if isinstance(resp.json, list) else []
        resp.json = {"total": len(rows)}
        return resp

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
        rate_limit: bool = False,
        rate_limit_read: bool = False,
    ) -> BackendResponse:
        url = f"{self._base_url}{path}"
        trace_id = uuid.uuid4().hex  # client-side only; mem0 doesn't echo it
        headers: dict[str, str] = {}
        if self._api_key:
            headers["Authorization"] = f"Token {self._api_key}"

        attempt_holder = {"n": 0}
        http_started = {"t": time.perf_counter()}

        async def _do() -> httpx.Response:
            attempt_holder["n"] += 1
            if rate_limit:
                await self._write_limiter.acquire()
            if rate_limit_read:
                await self._read_limiter.acquire()
            http_started["t"] = time.perf_counter()
            return await self._client.request(
                method, url, json=json, params=params, headers=headers
            )

        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(self._max_retries),
                wait=wait_exponential(multiplier=0.25, max=4.0),
                retry=retry_if_exception_type(_RETRYABLE_EXC),
                reraise=True,
            ):
                with attempt:
                    resp = await _do()
                    if resp.status_code == 429:
                        raise _RetryableStatus(429, f"server 429: {resp.text[:200]}")
                    if resp.status_code >= 500:
                        raise _RetryableStatus(
                            resp.status_code,
                            f"server {resp.status_code}: {resp.text[:200]}",
                        )
        except _RETRYABLE_EXC as e:
            status = getattr(e, "status_code", 0)
            raise BackendError(
                status, f"transport failure: {e}", trace_id=trace_id
            ) from e

        elapsed_ms = (time.perf_counter() - http_started["t"]) * 1000.0
        try:
            payload = resp.json() if resp.content else None
        except ValueError:
            payload = {"_raw": resp.text}

        if resp.status_code >= 400:
            detail = (
                payload.get("error") or payload.get("detail")
                if isinstance(payload, dict)
                else (payload or resp.text or "")
            )
            raise BackendError(resp.status_code, str(detail), trace_id=trace_id)

        return BackendResponse(
            status_code=resp.status_code,
            json=payload,
            latency_ms=elapsed_ms,
            trace_id=trace_id,
            attempt=attempt_holder["n"],
        )


@asynccontextmanager
async def mem0_client(
    base_url: str, api_key: str | None, **kwargs: Any
) -> AsyncIterator[Mem0Client]:
    """Convenience async context manager for ``Mem0Client``."""
    client = Mem0Client(base_url, api_key=api_key, **kwargs)
    try:
        yield client
    finally:
        await client.aclose()

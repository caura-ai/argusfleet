"""Backend abstraction — the seam that lets the harness target more than
one memory platform.

The four experiments consume exactly seven operations (write, search,
retrieve, update, entity-upsert, redistribute, delete-all/count). This
module defines that surface as a :class:`MemoryBackend` protocol, the
shared response/error envelopes, and a :class:`BackendCapabilities`
declaration so experiments can degrade honestly (record *unsupported*)
instead of failing when a platform lacks a governance primitive.

Concrete implementations:

- ``harness.client.MemClawClient`` — memclaw.net (the original target).
- ``harness.mem0_client.Mem0Client`` — the mem0 platform (api.mem0.ai).

``open_backend(cfg)`` is the factory the runner and CLI use; it picks the
implementation from ``cfg.backend``.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, AsyncIterator, Protocol, runtime_checkable

if TYPE_CHECKING:
    from argusfleet.harness.config import HarnessConfig


class BackendError(RuntimeError):
    """Raised when a backend returns a non-2xx response after retries.

    ``status_code`` is the upstream HTTP status (0 for pure transport
    failures); experiments key their allow/deny/error adjudication on it.
    """

    def __init__(self, status_code: int, message: str, trace_id: str | None = None) -> None:
        super().__init__(f"[{status_code}] {message} (trace={trace_id})")
        self.status_code = status_code
        self.message = message
        self.trace_id = trace_id


@dataclass(slots=True)
class BackendResponse:
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


@dataclass(frozen=True, slots=True)
class BackendCapabilities:
    """Which governance primitives a platform natively exposes.

    Experiments branch on these to pick a measurement strategy (or to
    record an axis as unsupported). The flags describe the *platform*,
    not the harness — an unsupported primitive is a finding, not a gap
    in the method (see paper, capability matrix).
    """

    name: str
    # GET-by-id can assert the caller's tenant, so sub-key read
    # authorization is expressible at all on the access axis.
    scoped_get: bool = True
    # Per-memory visibility levels (agent/fleet/tenant scopes) exist.
    visibility_scopes: bool = True
    # First-class entities (RDF subjects) can be upserted and referenced.
    entities: bool = True
    # Deterministic triple-based contradiction detection with a durable
    # supersession edge (``supersedes_id`` / non-active statuses).
    structural_contradiction: bool = True
    # An ownership-transfer / redistribute primitive exists.
    redistribute: bool = True
    # Search can filter rows by lifecycle status (e.g. ``active``).
    status_filter: bool = True
    # Enriched ("strong") writes return the stored row synchronously.
    sync_strong_writes: bool = True

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "scoped_get": self.scoped_get,
            "visibility_scopes": self.visibility_scopes,
            "entities": self.entities,
            "structural_contradiction": self.structural_contradiction,
            "redistribute": self.redistribute,
            "status_filter": self.status_filter,
            "sync_strong_writes": self.sync_strong_writes,
        }


@runtime_checkable
class MemoryBackend(Protocol):
    """The seven-operation surface the experiments consume.

    Implementations normalise their platform's wire format so that the
    ``BackendResponse.json`` payloads expose the fields experiments read:
    ``id``, ``content``, ``agent_id``, ``metadata``, ``status``,
    ``supersedes_id`` on rows; ``{"items": [...]}`` on searches.
    """

    capabilities: BackendCapabilities

    async def aclose(self) -> None: ...

    async def write_memory(self, *, tenant_id: str, agent_id: str, content: str, **kwargs: Any) -> BackendResponse: ...

    async def search_memory(self, *, tenant_id: str, query: str, top_k: int = 10, **kwargs: Any) -> BackendResponse: ...

    async def retrieve_memory(self, memory_id: str, *, tenant_id: str | None = None) -> BackendResponse: ...

    async def update_memory(self, memory_id: str, **kwargs: Any) -> BackendResponse: ...

    async def upsert_entity(self, *, tenant_id: str, entity_type: str, canonical_name: str, **kwargs: Any) -> BackendResponse: ...

    async def list_memories(self, *, tenant_id: str, metadata_filter: dict[str, Any] | None = None, **kwargs: Any) -> BackendResponse: ...

    async def delete_all_memories(self, *, tenant_id: str, **kwargs: Any) -> BackendResponse: ...

    async def count_memories(self, *, tenant_id: str, include_deleted: bool = False) -> BackendResponse: ...

    async def propagate_memory(self, *, memory_id: str, target_agent_id: str, tenant_id: str, requesting_agent_id: str) -> BackendResponse: ...


KNOWN_BACKENDS = ("memclaw", "mem0")


def build_backend(cfg: "HarnessConfig") -> Any:
    """Construct (but don't open) the backend selected by ``cfg.backend``."""
    backend = (cfg.backend or "memclaw").lower()
    if backend == "memclaw":
        from argusfleet.harness.client import MemClawClient

        return MemClawClient(
            cfg.base_url,
            api_key=cfg.api_key,
            timeout_s=cfg.request_timeout_s,
            max_retries=cfg.max_retries,
            min_write_interval_ms=cfg.min_request_interval_ms,
        )
    if backend == "mem0":
        from argusfleet.harness.mem0_client import Mem0Client

        # mem0 meters /v2 retrieval (search + get-all) against a monthly
        # quota and a burst limit; experiments spacing reads avoid 429.
        # These knobs live under cfg.experiment so each mem0 config can
        # tune them (e.g. propagation polls, so it wants a read gap).
        exp = cfg.experiment or {}
        return Mem0Client(
            cfg.mem0_base_url,
            api_key=cfg.mem0_api_key,
            timeout_s=cfg.request_timeout_s,
            max_retries=cfg.max_retries,
            min_write_interval_ms=cfg.min_request_interval_ms,
            min_read_interval_ms=float(exp.get("mem0_min_read_interval_ms", 350.0)),
            infer_poll_interval_s=float(exp.get("mem0_infer_poll_interval_s", 2.0)),
            infer_poll_timeout_s=float(exp.get("mem0_infer_poll_timeout_s", 12.0)),
        )
    raise ValueError(f"unknown backend {backend!r} — expected one of {KNOWN_BACKENDS}")


@asynccontextmanager
async def open_backend(cfg: "HarnessConfig") -> AsyncIterator[Any]:
    """Async context manager over the configured backend client."""
    client = build_backend(cfg)
    try:
        yield client
    finally:
        await client.aclose()

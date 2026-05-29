"""Core domain models for the AgentFleetMem harness.

These are the on-paper systems primitives the experiments measure against.
They are deliberately decoupled from any specific server schema — adapters
translate between these and the upstream wire format (see ``client.py``).
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class MemoryScope(str, Enum):
    """Visibility scope for a memory record.

    Scope is the unit of governance — every write and every retrieval is
    evaluated against scope. ``agent`` is the most restrictive (only the
    writing agent can read), ``fleet`` is shared within a fleet, ``tenant``
    is org-wide, ``public`` is cross-tenant (rarely valid in production).
    """

    AGENT = "agent"
    FLEET = "fleet"
    TENANT = "tenant"
    PUBLIC = "public"


class Tenant(BaseModel):
    """A governance boundary. Memories never leak across tenants."""

    id: str = Field(..., description="Stable tenant identifier, e.g. mc_abc123")
    name: str | None = None
    created_at: datetime = Field(default_factory=_utcnow)


class Fleet(BaseModel):
    """A group of agents that share fleet-scoped memory."""

    id: str
    tenant_id: str
    name: str | None = None
    agent_ids: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=_utcnow)


class Agent(BaseModel):
    """An agent identity inside a tenant/fleet.

    ``trust_level`` mirrors the upstream notion: 0 = unapproved, 1+ = may
    write. The harness uses it to script policy-violation experiments.
    """

    id: str
    tenant_id: str
    fleet_id: str | None = None
    name: str | None = None
    trust_level: int = 1
    role: str | None = None


class Provenance(BaseModel):
    """Why this memory exists. Drives the provenance experiment."""

    writer_agent: str
    workflow_id: str | None = None
    source_uri: str | None = None
    derived_from: list[UUID] = Field(default_factory=list)
    reason: str | None = None
    captured_at: datetime = Field(default_factory=_utcnow)


class Memory(BaseModel):
    """A single memory record as the harness reasons about it.

    Includes the full feature surface the spec calls out, plus a few
    optional fields the experiments populate (``confidence``, ``tags``).
    Adapters drop unknown fields when sending to the upstream API.
    """

    model_config = ConfigDict(populate_by_name=True)

    id: UUID = Field(default_factory=uuid4)
    content: str
    embedding: list[float] | None = None
    timestamp: datetime = Field(default_factory=_utcnow)
    writer_agent: str
    fleet_id: str | None = None
    tenant_id: str
    scope: MemoryScope = MemoryScope.FLEET
    provenance: Provenance
    supersedes: UUID | None = None
    superseded_by: UUID | None = None
    contradiction_group: str | None = None
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class Workflow(BaseModel):
    """A logical multi-step task that may span multiple agents."""

    id: str = Field(default_factory=lambda: str(uuid4()))
    tenant_id: str
    fleet_id: str | None = None
    name: str
    agent_ids: list[str] = Field(default_factory=list)
    started_at: datetime = Field(default_factory=_utcnow)
    ended_at: datetime | None = None


class RetrievalEvent(BaseModel):
    """A single retrieval attempt — what was asked, what came back, who asked."""

    id: UUID = Field(default_factory=uuid4)
    timestamp: datetime = Field(default_factory=_utcnow)
    workflow_id: str | None = None
    requester_agent: str
    requester_tenant: str
    requester_fleet: str | None = None
    query: str
    top_k: int
    returned_memory_ids: list[UUID] = Field(default_factory=list)
    returned_count: int = 0
    latency_ms: float | None = None
    error: str | None = None


class WriteEvent(BaseModel):
    """A single write attempt — outcome + cost + provenance trail."""

    id: UUID = Field(default_factory=uuid4)
    timestamp: datetime = Field(default_factory=_utcnow)
    workflow_id: str | None = None
    writer_agent: str
    tenant_id: str
    fleet_id: str | None = None
    memory_id: UUID | None = None
    scope: MemoryScope
    content_hash: str | None = None
    succeeded: bool = True
    latency_ms: float | None = None
    error: str | None = None


class PolicyDecision(BaseModel):
    """Outcome of a governance check.

    The harness logs one of these per gated operation; the leakage
    experiment counts ``allow`` decisions that should have been ``deny``.
    """

    id: UUID = Field(default_factory=uuid4)
    timestamp: datetime = Field(default_factory=_utcnow)
    actor_agent: str
    actor_tenant: str
    actor_fleet: str | None = None
    target_scope: MemoryScope
    target_tenant: str
    target_fleet: str | None = None
    action: str  # "read" | "write" | "update" | "delete"
    decision: str  # "allow" | "deny"
    reason: str | None = None
    expected_decision: str | None = None  # what the experiment expected

    @property
    def is_violation(self) -> bool:
        """True iff the server's decision disagrees with the expected one."""
        return self.expected_decision is not None and self.decision != self.expected_decision

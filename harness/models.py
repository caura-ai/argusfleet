"""Domain models actually used by the harness.

The harness writes events as plain dicts for speed and flexibility; the
two typed records that survive here are the ones the experiments rely on
for behaviour rather than for shape documentation:

- ``MemoryScope`` — the canonical scope enum used by ``leakage.py`` for
  ground-truth scope arithmetic.
- ``PolicyDecision`` — the per-probe policy outcome record, instantiated
  inside ``leakage.py`` and serialised into ``events.jsonl``.

Earlier revisions of the harness also shipped Pydantic models for
``Agent``, ``Tenant``, ``Fleet``, ``Memory``, ``Provenance``,
``Workflow``, ``RetrievalEvent``, and ``WriteEvent``. They were never
instantiated by the experiments (which use plain dicts and local
``@dataclass`` helpers), so we removed them to keep this file honest.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


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


class PolicyDecision(BaseModel):
    """Outcome of a governance check.

    The leakage experiment logs one of these per probed (actor, target,
    action) tuple. ``decision`` is the server's observed behaviour;
    ``expected_decision`` is the ground truth derived from the
    experiment plan. ``is_violation`` flags the disagreement.
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
    decision: str  # "allow" | "deny" | "error"
    reason: str | None = None
    expected_decision: str | None = None  # what the experiment expected

    @property
    def is_violation(self) -> bool:
        """True iff the server's decision disagrees with the expected one."""
        return self.expected_decision is not None and self.decision != self.expected_decision

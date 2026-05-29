"""Smoke tests for the core domain models."""

from __future__ import annotations

from agentfleetmem.harness.models import (
    Memory,
    MemoryScope,
    PolicyDecision,
    Provenance,
    RetrievalEvent,
    WriteEvent,
)


def test_memory_roundtrip() -> None:
    m = Memory(
        content="alpha",
        writer_agent="agent-a",
        tenant_id="mc_t1",
        fleet_id="fleet-1",
        scope=MemoryScope.FLEET,
        provenance=Provenance(writer_agent="agent-a", reason="seed"),
    )
    blob = m.model_dump(mode="json")
    again = Memory.model_validate(blob)
    assert again.content == "alpha"
    assert again.scope is MemoryScope.FLEET


def test_policy_decision_violation_flag() -> None:
    ok = PolicyDecision(
        actor_agent="probe",
        actor_tenant="mc_t1",
        target_scope=MemoryScope.FLEET,
        target_tenant="mc_t1",
        action="read",
        decision="deny",
        expected_decision="deny",
    )
    bad = ok.model_copy(update={"decision": "allow"})
    assert not ok.is_violation
    assert bad.is_violation


def test_event_defaults() -> None:
    r = RetrievalEvent(
        requester_agent="a",
        requester_tenant="mc_t1",
        query="canary",
        top_k=5,
    )
    w = WriteEvent(
        writer_agent="a",
        tenant_id="mc_t1",
        scope=MemoryScope.AGENT,
    )
    assert r.returned_count == 0
    assert w.succeeded is True

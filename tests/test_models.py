"""Smoke tests for the domain models that the experiments actually use."""

from __future__ import annotations

from argusfleet.harness.models import MemoryScope, PolicyDecision


def test_memory_scope_values() -> None:
    """The enum's string values are what the experiments encode into
    metadata and what the server's ``visibility`` field expects after
    the leakage scope-to-visibility mapping."""
    assert MemoryScope.AGENT.value == "agent"
    assert MemoryScope.FLEET.value == "fleet"
    assert MemoryScope.TENANT.value == "tenant"
    assert MemoryScope.PUBLIC.value == "public"


def test_policy_decision_violation_flag() -> None:
    """``is_violation`` flips iff observed != expected decision."""
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


def test_policy_decision_roundtrip() -> None:
    """The model serialises to JSON and reloads cleanly — the leakage
    experiment depends on this for ``events.jsonl`` reproducibility."""
    pd = PolicyDecision(
        actor_agent="agent-a",
        actor_tenant="mc_t1",
        target_scope=MemoryScope.AGENT,
        target_tenant="mc_t1",
        action="read",
        decision="allow",
        expected_decision="allow",
        reason="GET returned 200",
    )
    blob = pd.model_dump(mode="json")
    again = PolicyDecision.model_validate(blob)
    assert again.target_scope is MemoryScope.AGENT
    assert again.is_violation is False

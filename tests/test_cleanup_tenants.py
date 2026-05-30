"""Regression tests for the `run all --cleanup` no-op bug.

Cleanup used to read ``cfg.tenant_ids``, which is empty in the default
(no ``MEMCLAW_TENANT_ID``) mode — so the purge silently cleared nothing
while experiments wrote to seed-synthesised tenants and dedup state
accumulated. The fix gives each experiment a ``tenant_ids_for(cfg)``
single source of truth (used by both plan-building and ``run all``'s
purge). These tests pin that resolution so the two can't drift.
"""
from __future__ import annotations

from argusfleet.experiments import contradiction, leakage, propagation, provenance
from argusfleet.harness.config import HarnessConfig


def test_synthesized_tenants_in_default_mode():
    """No MEMCLAW_TENANT_ID → each experiment resolves a seed-derived ID
    that cleanup must (now) target. These are exactly the IDs the builders
    write to."""
    cfg = HarnessConfig(seed=7)
    assert contradiction.tenant_ids_for(cfg) == ["mc_ctr_7"]
    assert propagation.tenant_ids_for(cfg) == ["mc_prop_7"]
    assert provenance.tenant_ids_for(cfg) == ["mc_prv_7"]
    # leakage fans out across n_tenants (default 2).
    assert leakage.tenant_ids_for(cfg) == ["mc_lkg_t0_7", "mc_lkg_t1_7"]


def test_configured_tenants_take_precedence():
    cfg = HarnessConfig(seed=7, tenant_ids=["real_a", "real_b"])
    assert contradiction.tenant_ids_for(cfg) == ["real_a"]
    assert propagation.tenant_ids_for(cfg) == ["real_a"]
    assert provenance.tenant_ids_for(cfg) == ["real_a"]
    assert leakage.tenant_ids_for(cfg) == ["real_a", "real_b"]


def test_leakage_honours_n_tenants():
    cfg = HarnessConfig(seed=3, experiment={"n_tenants": 3})
    assert leakage.tenant_ids_for(cfg) == ["mc_lkg_t0_3", "mc_lkg_t1_3", "mc_lkg_t2_3"]


def test_leakage_plan_matches_resolver():
    """The plan the experiment actually executes must use the same tenants
    the cleanup resolver returns — guards against the two drifting apart."""
    cfg = HarnessConfig(seed=11, experiment={"n_tenants": 2})
    plan = leakage._build_plan(cfg, nonce="nonce")
    assert plan.tenant_ids == leakage.tenant_ids_for(cfg)

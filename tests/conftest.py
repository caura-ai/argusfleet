"""Pytest fixtures shared across the harness test suite.

Lives at the repo root per project convention; per-package conftest.py
files duplicate fixtures and drift apart over time.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make the repo root importable as ``argusfleet`` without an editable
# install during quick CI runs. Real users install with ``pip install -e .``.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT.parent))

import pytest

from argusfleet.harness.config import HarnessConfig


@pytest.fixture
def tmp_config(tmp_path: Path) -> HarnessConfig:
    return HarnessConfig(
        base_url="https://memclaw.test",
        api_key="test-key",
        output_dir=tmp_path,
        seed=7,
        request_timeout_s=5.0,
        max_retries=1,
        experiment={"n_tenants": 2, "n_fleets_per_tenant": 2, "n_agents_per_fleet": 2},
    )

"""Tests for the shared runner helpers in ``harness.runner``."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from agentfleetmem.harness.client import MemClawClient
from agentfleetmem.harness.config import HarnessConfig
from agentfleetmem.harness.runner import (
    make_run_dir,
    make_run_id,
    open_sink_with_envelope,
    run_with_client,
)


def _cfg(tmp_path: Path) -> HarnessConfig:
    return HarnessConfig(
        base_url="https://memclaw.test",
        api_key="k",
        output_dir=tmp_path,
        seed=11,
    )


def test_make_run_id_format(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    rid = make_run_id(cfg, "myexp")
    # The format is <UTC-stamp>-<label>-seed<N>. Two consecutive calls
    # within the same second can collide — that's by design (seed-based
    # idempotency), so we don't assert uniqueness.
    parts = rid.split("-")
    assert parts[-1] == "seed11"
    assert "myexp" in rid
    # Label override wins over the default.
    cfg2 = cfg.model_copy(update={"run_label": "override"})
    assert "override" in make_run_id(cfg2, "myexp")
    assert "myexp" not in make_run_id(cfg2, "myexp")


def test_make_run_dir_creates_directory(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    run_id, run_dir, events_path = make_run_dir(cfg, "exp")
    assert run_dir.exists()
    assert run_dir.is_dir()
    assert run_dir.name == run_id
    # ``events.jsonl`` is the *expected* path but not created yet.
    assert events_path.name == "events.jsonl"
    assert not events_path.exists()


def test_open_sink_writes_envelope(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    _, _, events_path = make_run_dir(cfg, "exp")
    with open_sink_with_envelope(
        events_path,
        run_id="abc",
        experiment="myexp",
        cfg=cfg,
        extra={"n_facts": 7, "extra_dict": {"k": "v"}},
    ) as sink:
        sink.write("noop", {"x": 1})

    lines = events_path.read_text(encoding="utf-8").splitlines()
    first = json.loads(lines[0])
    assert first["event"] == "run_start"
    data = first["data"]
    assert data["run_id"] == "abc"
    assert data["experiment"] == "myexp"
    assert data["seed"] == 11
    assert data["base_url"] == "https://memclaw.test"
    assert data["n_facts"] == 7
    assert data["extra_dict"] == {"k": "v"}
    # Subsequent events follow the envelope.
    assert json.loads(lines[1])["event"] == "noop"


@pytest.mark.asyncio
async def test_run_with_client_uses_supplied_client(tmp_path: Path) -> None:
    """When the caller passes a client, run_with_client must use it and
    not open its own — the test inspects identity to confirm."""
    seen: list[MemClawClient] = []
    cfg = _cfg(tmp_path)
    supplied = MemClawClient("https://memclaw.test", api_key="k", max_retries=1)
    try:

        async def _body(c: MemClawClient) -> None:
            seen.append(c)

        await run_with_client(cfg, client=supplied, body=_body)
    finally:
        await supplied.aclose()
    assert seen == [supplied]


@pytest.mark.asyncio
async def test_run_with_client_autocreates_when_none(tmp_path: Path) -> None:
    """Without a client, run_with_client must build one. We can't easily
    inspect the construction, so we just check the body sees *some*
    MemClawClient and the run completes cleanly."""
    cfg = _cfg(tmp_path)
    received: list[type] = []

    async def _body(c: MemClawClient) -> None:
        received.append(type(c))

    await run_with_client(cfg, client=None, body=_body)
    assert received == [MemClawClient]

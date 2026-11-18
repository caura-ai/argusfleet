"""End-to-end tests for the derived-leakage experiment (respx-mocked, no network).

Mirrors ``test_leakage_split.py``: three server modes must keep the
leak/miss split apart.

- **compliant** — enforces entry-level scope *and* derivation: a reader
  must be authorized for every transitive ``derived_from`` ancestor.
  Expected: derived_leak_rate = 0, miss_rate = 0.
- **over_permissive** — enforces entry-level scope only and ignores
  ``derived_from`` (the preregistered model of today's server): C reads
  M but not S. Expected: derived_leak_rate = 1, miss_rate = 0.
- **under_permissive** — denies every agent-key read. Expected:
  derived_leak_rate = 0, miss_rate = 1.

The mock resolves caller identity from the ``X-API-Key`` header, as the
real gateway does; the tenant key carries no agent identity and is
tenant-wide. Plus: a cyclic ``derived_from`` must terminate (B3), a
provisioning 403 must fail fast with no tenant-key fallback, and raw agent
keys must never reach disk.
"""

from __future__ import annotations

import asyncio
import json
import re
import uuid
from pathlib import Path

import httpx
import pytest
import respx

from argusfleet.experiments import derived_leakage
from argusfleet.harness.client import MemClawClient
from argusfleet.harness.config import HarnessConfig

TENANT_KEY = "tenant-key-test"
_GET_PATH_RE = re.compile(r"/api/v1/memories/([0-9a-fA-F-]{36})$")


def _cfg(tmp_config: HarnessConfig, **exp: object) -> HarnessConfig:
    experiment = {
        "n_chains": 4,
        "chain_depth": 1,
        "home_fleet": "platform",
        "settle_delay_ms": 0,
        "max_in_flight": 4,
        **exp,
    }
    return tmp_config.model_copy(
        update={
            "base_url": "https://memclaw.test",
            "api_key": TENANT_KEY,
            "tenant_ids": ["mc_dlk_test"],
            "experiment": experiment,
        }
    )


class _Server:
    """Stateful mock. ``keys`` maps raw agent key -> identity."""

    def __init__(self, mode: str, *, cycle: bool = False, provision_status: int = 200):
        self.mode = mode
        self.cycle = cycle
        self.provision_status = provision_status
        self.keys: dict[str, dict] = {}
        self.store: dict[str, dict] = {}
        self.issued_keys: list[str] = []

    # --- identity -------------------------------------------------------
    def identity(self, request: httpx.Request) -> dict | None:
        key = request.headers.get("X-API-Key")
        if key == TENANT_KEY:
            return {"kind": "tenant"}
        return self.keys.get(key or "")

    # --- policy ---------------------------------------------------------
    def _entry_ok(self, who: dict, rec: dict) -> bool:
        vis = rec["visibility"]
        if vis == "scope_agent":
            return who["agent_id"] == rec["agent_id"]
        if vis == "scope_team":
            return who["fleet_id"] == rec["fleet_id"]
        return True

    def can_read(self, who: dict | None, rec: dict) -> bool:
        if who is None:
            return False
        if who["kind"] == "tenant":
            return True  # tenant key is tenant-wide by design
        if self.mode == "under_permissive":
            return False
        if not self._entry_ok(who, rec):
            return False
        if self.mode == "over_permissive":
            return True  # derived_from ignored
        # compliant: every transitive ancestor must also be readable.
        seen: set[str] = set()
        frontier = list((rec.get("metadata") or {}).get("derived_from") or [])
        while frontier:
            pid = frontier.pop()
            if pid in seen:
                continue
            seen.add(pid)
            parent = self.store.get(pid)
            if parent is None or not self._entry_ok(who, parent):
                return False
            frontier.extend((parent.get("metadata") or {}).get("derived_from") or [])
        return True

    # --- handlers -------------------------------------------------------
    def provision(self, request: httpx.Request) -> httpx.Response:
        if request.headers.get("X-API-Key") != TENANT_KEY:
            return httpx.Response(401, json={"detail": "unauthenticated"})
        if self.provision_status >= 400:
            return httpx.Response(self.provision_status, json={"detail": "admin capability required"})
        body = json.loads(request.read())
        raw = f"ak_secret_{uuid.uuid4().hex}"
        self.issued_keys.append(raw)
        self.keys[raw] = {
            "kind": "agent",
            "agent_id": body["agent_id"],
            "fleet_id": body["initial_fleet"],
            "trust": body["initial_trust"],
        }
        return httpx.Response(
            201, json={"raw_key": raw, "agent_id": body["agent_id"], "kind": "agent_key"}
        )

    def write(self, request: httpx.Request) -> httpx.Response:
        who = self.identity(request)
        if who is None:
            return httpx.Response(401, json={"detail": "bad key"})
        body = json.loads(request.read())
        new_id = str(uuid.uuid4())
        meta = dict(body.get("metadata") or {})
        if self.cycle and meta.get("derived_from"):
            meta["derived_from"] = [new_id]  # self-referential corruption
        rec = {
            "id": new_id,
            "tenant_id": body["tenant_id"],
            "agent_id": who.get("agent_id") or body["agent_id"],
            "fleet_id": who.get("fleet_id") or body.get("fleet_id"),
            "content": body["content"],
            "metadata": meta,
            "visibility": body.get("visibility", "scope_team"),
        }
        self.store[new_id] = rec
        return httpx.Response(201, json=rec)

    def get(self, request: httpx.Request) -> httpx.Response:
        m = _GET_PATH_RE.search(request.url.path)
        rec = self.store.get(m.group(1)) if m else None
        if rec is None or request.url.params.get("tenant_id") != rec["tenant_id"]:
            return httpx.Response(404, json={"detail": "not found"})
        if not self.can_read(self.identity(request), rec):
            return httpx.Response(404, json={"detail": "not found"})
        return httpx.Response(200, json=rec)

    def search(self, request: httpx.Request) -> httpx.Response:
        who = self.identity(request)
        body = json.loads(request.read())
        query = body.get("query") or ""
        items = [
            r for r in self.store.values()
            if r["tenant_id"] == body["tenant_id"]
            and r["content"] == query
            and self.can_read(who, r)
        ]
        return httpx.Response(200, json={"items": items[: body.get("top_k", 10)]})

    def router(self) -> respx.Router:
        router = respx.mock(base_url="https://memclaw.test", assert_all_called=False)
        router.post("/api/v1/admin/agent-keys/provision").mock(side_effect=self.provision)
        router.post("/api/v1/memories").mock(side_effect=self.write)
        router.post("/api/v1/search").mock(side_effect=self.search)
        router.get(url__regex=r"/api/v1/memories/[0-9a-fA-F-]+").mock(side_effect=self.get)
        return router


async def _run(cfg: HarnessConfig, server: _Server) -> dict:
    with server.router():
        async with MemClawClient(cfg.base_url, api_key=TENANT_KEY, max_retries=1) as client:
            # Bounded so a regression in the walk guard fails instead of hanging.
            return await asyncio.wait_for(derived_leakage.run(cfg, client=client), timeout=60)


@pytest.mark.asyncio
@pytest.mark.parametrize("depth", [1, 2])
async def test_compliant_server_no_leak_no_miss(tmp_config: HarnessConfig, depth: int) -> None:
    result = await _run(_cfg(tmp_config, chain_depth=depth), _Server("compliant"))
    assert result["run_valid"] is True
    assert result["chain_broken_rate"] == 0.0
    assert result["n_derived_graded"] == 4
    assert result["derived_leak_rate"] == 0.0
    assert result["derived_leak_rate_ci95"][1] > 0.0  # 0/N carries an interval
    assert result["search_derived_leak_rate"] == 0.0
    assert result["miss_rate"] == 0.0


@pytest.mark.asyncio
@pytest.mark.parametrize("depth", [1, 2])
async def test_over_permissive_drives_derived_leak(tmp_config: HarnessConfig, depth: int) -> None:
    result = await _run(_cfg(tmp_config, chain_depth=depth), _Server("over_permissive"))
    # Entry-level scope still holds (C denied on S) so the run is valid...
    assert result["run_valid"] is True
    assert result["n_source_control_allowed"] == 0
    # ...but nothing consults derived_from, so every derived probe leaks.
    assert result["derived_leak_rate"] == 1.0
    assert result["search_derived_leak_rate"] == 1.0
    assert result["miss_rate"] == 0.0


@pytest.mark.asyncio
async def test_under_permissive_drives_miss_rate(tmp_config: HarnessConfig) -> None:
    result = await _run(_cfg(tmp_config), _Server("under_permissive"))
    assert result["run_valid"] is True
    assert result["derived_leak_rate"] == 0.0
    assert result["miss_rate"] == 1.0


@pytest.mark.asyncio
async def test_cyclic_derived_from_terminates(tmp_config: HarnessConfig) -> None:
    """B3: a self-referential derived_from must terminate via the visited
    set / depth cap, mark every chain broken, and leave derived probes
    ungraded (chain_broken is data quality, not a leak)."""
    result = await _run(_cfg(tmp_config, chain_depth=2), _Server("over_permissive", cycle=True))
    assert result["chain_broken_rate"] == 1.0
    assert result["n_derived_graded"] == 0
    assert result["derived_leak_rate"] is None
    chains = Path(result["chains_csv"]).read_text()
    assert "cycle_detected" in chains


@pytest.mark.asyncio
async def test_provision_forbidden_fails_fast(tmp_config: HarnessConfig) -> None:
    server = _Server("compliant", provision_status=403)
    with pytest.raises(derived_leakage.AgentKeyProvisioningError, match="admin capability"):
        await _run(_cfg(tmp_config), server)
    # No tenant-keyed fallback: nothing was written or probed.
    assert server.store == {}


@pytest.mark.asyncio
async def test_invalid_run_when_agent_keys_do_not_distinguish(tmp_config: HarnessConfig) -> None:
    """If C can GET S, the keys don't distinguish readers: run_valid=false
    and derived leaks are not graded."""

    class _Blind(_Server):
        def _entry_ok(self, who: dict, rec: dict) -> bool:
            return True

    result = await _run(_cfg(tmp_config), _Blind("over_permissive"))
    assert result["run_valid"] is False
    assert result["derived_leak_rate"] is None


@pytest.mark.asyncio
async def test_raw_agent_keys_never_written_to_disk(tmp_config: HarnessConfig) -> None:
    server = _Server("over_permissive")
    result = await _run(_cfg(tmp_config), server)
    assert len(server.issued_keys) == 2
    blob = "".join(
        p.read_text(errors="ignore") for p in Path(result["run_dir"]).iterdir() if p.is_file()
    )
    for raw in server.issued_keys:
        assert raw not in blob
    assert "ak_secret_" not in blob


@pytest.mark.asyncio
async def test_client_agent_key_wire_format() -> None:
    """provision_agent_key sends the scripts/agent_scope_leakage.py payload
    with the tenant key; for_agent_key's sibling authenticates with its own
    key, shares the write limiter, and closing it leaves the parent usable."""
    server = _Server("compliant")
    with server.router() as router:
        async with MemClawClient("https://memclaw.test", api_key=TENANT_KEY, max_retries=1) as parent:
            resp = await parent.provision_agent_key(agent_id="a1", fleet_id="f1", trust=1)
            raw = MemClawClient.raw_agent_key(resp)
            assert raw is not None
            sent = json.loads(router.calls.last.request.read())
            assert sent == {"agent_id": "a1", "label": "argusfleet a1",
                            "initial_fleet": "f1", "initial_trust": 1}

            sibling = parent.for_agent_key(raw)
            assert sibling._write_limiter is parent._write_limiter
            await sibling.write_memory(tenant_id="t", agent_id="a1", content="x")
            assert router.calls.last.request.headers["X-API-Key"] == raw
            await sibling.aclose()
            await parent.write_memory(tenant_id="t", agent_id="a1", content="y")
            assert router.calls.last.request.headers["X-API-Key"] == TENANT_KEY

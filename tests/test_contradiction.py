"""End-to-end smoke test for the contradiction experiment.

The test mocks the server so it never touches the network. The mock is
deliberately a small state machine, not a full fake:

- On the *first* write for a given subject, return a fresh memory_id
  with no ``supersedes_id``.
- On the *second* write for the same subject, return a fresh memory_id
  carrying ``supersedes_id`` pointing at the first row — this is what a
  correctly-behaving server would do.
- Searches return only the most recent memory for each subject (the
  active row).

That's enough to exercise the detection-rate / stale-read accounting and
catch wire-format regressions without dragging in real embedding behaviour.
"""

from __future__ import annotations

import json
import uuid

import httpx
import pytest
import respx

from argusfleet.experiments import contradiction
from argusfleet.harness.client import MemClawClient
from argusfleet.harness.config import HarnessConfig


def _fake_server() -> respx.Router:
    """Stateful mock keyed on ``subject_entity_id`` — the deterministic
    signal a real server would use to detect a contradiction once the
    harness switched from text-similarity to RDF triples.
    """
    router = respx.mock(base_url="https://memclaw.test", assert_all_called=False)
    # Map subject name -> entity uuid (assigned on first upsert).
    entity_by_subject: dict[str, str] = {}
    # Map (entity_id, predicate) -> latest memory_id; the second write
    # against the same key gets supersedes_id pointing back.
    latest_by_key: dict[tuple[str, str], str] = {}
    # Stored rows by id, so the experiment's post-commit GET-by-id
    # re-fetch (detection is async) can read back supersedes_id / status.
    rows_by_id: dict[str, dict] = {}

    def upsert_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        name = body.get("canonical_name")
        if name not in entity_by_subject:
            entity_by_subject[name] = str(uuid.uuid4())
        return httpx.Response(
            200,
            json={
                "id": entity_by_subject[name],
                "tenant_id": body["tenant_id"],
                "entity_type": body.get("entity_type"),
                "canonical_name": name,
            },
        )

    def write_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        meta = body.get("metadata") or {}
        entity_id = body.get("subject_entity_id")
        predicate = body.get("predicate")
        new_id = str(uuid.uuid4())
        key = (entity_id, predicate) if entity_id and predicate else None
        prev = latest_by_key.get(key) if key else None
        if key:
            latest_by_key[key] = new_id
        payload = {
            "id": new_id,
            "tenant_id": body["tenant_id"],
            "fleet_id": body.get("fleet_id"),
            "agent_id": body["agent_id"],
            "memory_type": "fact",
            "content": body["content"],
            "weight": 1.0,
            "source_uri": None,
            "run_id": None,
            "metadata": meta,
            "created_at": "2026-05-28T00:00:00+00:00",
            "expires_at": None,
            "status": "active",
            "visibility": body.get("visibility", "scope_team"),
            "subject_entity_id": entity_id,
            "predicate": predicate,
            "object_value": body.get("object_value"),
            "supersedes_id": prev,
            "superseded_by": (
                [{"old_memory_id": prev, "old_status": "outdated",
                  "reason": "rdf_conflict",
                  "old_content_preview": "...", "direction": "canonical"}]
                if prev
                else None
            ),
        }
        # Persist the FULL record (with supersession) for GET-by-id
        # re-fetch, and flip the prior row to ``outdated``. Detection is
        # async/post-commit on a real server: supersedes_id / superseded_by
        # are NEVER present in the synchronous write response — the harness
        # reads them back via GET-by-id after the settle window. Mirror that
        # contract by stripping the supersession from the 201 body, so the
        # detection signal flows only through the production-realistic path.
        rows_by_id[new_id] = dict(payload)
        if prev and prev in rows_by_id:
            rows_by_id[prev]["status"] = "outdated"
        response_payload = dict(payload)
        response_payload["supersedes_id"] = None
        response_payload["superseded_by"] = None
        return httpx.Response(201, json=response_payload)

    def get_handler(request: httpx.Request) -> httpx.Response:
        mem_id = request.url.path.rsplit("/", 1)[-1]
        row = rows_by_id.get(mem_id)
        if row is None:
            return httpx.Response(404, json={"detail": "Memory not found"})
        return httpx.Response(200, json=row)

    def search_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        items = [
            {
                "id": mem_id,
                "tenant_id": body["tenant_id"],
                "agent_id": "agent_ctr_b_0",
                "memory_type": "fact",
                "content": f"{predicate} of {subject} = X",
                "weight": 1.0,
                "metadata": {
                    "experiment": "contradiction",
                    "fact_id": subject,
                    "subject": subject,
                    "predicate": predicate,
                    "asserted_value": "X",
                },
                "created_at": "2026-05-28T00:00:00+00:00",
                "status": "active",
                "visibility": "scope_team",
            }
            for (subject, predicate), mem_id in [
                ((s, p), mid)
                for ((eid, p), mid) in latest_by_key.items()
                for s, eid_check in entity_by_subject.items()
                if eid_check == eid
            ]
        ]
        return httpx.Response(200, json={"items": items})

    router.post("/api/v1/entities/upsert").mock(side_effect=upsert_handler)
    router.post("/api/v1/memories").mock(side_effect=write_handler)
    router.post("/api/v1/search").mock(side_effect=search_handler)
    router.get(url__regex=r".*/api/v1/memories/[0-9a-fA-F-]+(\?.*)?$").mock(side_effect=get_handler)
    return router


@pytest.mark.asyncio
async def test_contradiction_detection_rate_is_one_against_a_compliant_server(
    tmp_config: HarnessConfig,
) -> None:
    cfg = tmp_config.model_copy(
        update={
            "base_url": "https://memclaw.test",
            "experiment": {
                "n_facts": 3,
                "scenarios": ["sequential"],
                "sequential_delay_ms": 0,
                "write_mode": "strong",
                "max_in_flight": 2,
                "tenant_id": "mc_t_test",
                "fleet_id": "fleet_test",
            },
        }
    )
    with _fake_server():
        async with MemClawClient(cfg.base_url, api_key="k", max_retries=1) as client:
            result = await contradiction.run(cfg, client=client)

    # Three sequential fact-runs against a compliant server → every
    # second write should report ``supersedes_id``.
    assert result["n_facts_runs"] == 3
    assert result["detection_rate"] == 1.0
    # With the mock returning exactly one active row per subject, the
    # stale-read accounting should be clean.
    assert result["stale_read_rate"] == 0.0


@pytest.mark.asyncio
async def test_contradiction_no_detection_when_server_drops_supersedes(
    tmp_config: HarnessConfig,
) -> None:
    """Sanity check: if the server never reports ``supersedes_id``, the
    detection rate must be zero — the harness must not silently report
    success when the underlying signal is missing."""

    cfg = tmp_config.model_copy(
        update={
            "base_url": "https://memclaw.test",
            "experiment": {
                "n_facts": 2,
                "scenarios": ["sequential"],
                "sequential_delay_ms": 0,
                "write_mode": "strong",
                "max_in_flight": 2,
                "tenant_id": "mc_t_test",
                "fleet_id": "fleet_test",
            },
        }
    )
    with respx.mock(base_url="https://memclaw.test") as router:
        # Entity upsert succeeds — the failure under test is the
        # memories endpoint silently omitting ``supersedes_id``.
        router.post("/api/v1/entities/upsert").mock(
            return_value=httpx.Response(
                200,
                json={
                    "id": "11111111-1111-1111-1111-111111111111",
                    "tenant_id": "mc_t_test",
                    "entity_type": "system_resource",
                    "canonical_name": "stub-subject",
                },
            )
        )
        router.post("/api/v1/memories").mock(
            return_value=httpx.Response(
                201,
                json={
                    "id": "00000000-0000-0000-0000-000000000001",
                    "tenant_id": "mc_t_test",
                    "agent_id": "agent_ctr_a_0",
                    "memory_type": "fact",
                    "content": "stub",
                    "weight": 1.0,
                    "metadata": {},
                    "created_at": "2026-05-28T00:00:00+00:00",
                    "status": "active",
                    "visibility": "scope_team",
                    # critically: no supersedes_id, no superseded_by
                },
            )
        )
        router.post("/api/v1/search").mock(
            return_value=httpx.Response(200, json={"items": []})
        )
        # GET-by-id re-fetch (the detection signal) also shows no
        # supersession and an active status — so detection stays 0.
        router.get(url__regex=r".*/api/v1/memories/[0-9a-fA-F-]+(\?.*)?$").mock(
            return_value=httpx.Response(
                200,
                json={
                    "id": "00000000-0000-0000-0000-000000000001",
                    "tenant_id": "mc_t_test",
                    "agent_id": "agent_ctr_a_0",
                    "memory_type": "fact",
                    "content": "stub",
                    "metadata": {},
                    "status": "active",
                    "supersedes_id": None,
                    "superseded_by": None,
                },
            )
        )
        async with MemClawClient(cfg.base_url, api_key="k", max_retries=1) as client:
            result = await contradiction.run(cfg, client=client)

    assert result["detection_rate"] == 0.0

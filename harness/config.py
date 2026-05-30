"""Runtime configuration for the harness.

Reads from environment variables (loaded from .env when present) plus an
optional YAML config file passed via the CLI. Env vars override file
settings — CI overrides repo defaults.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field

# When the package is installed (e.g. into ``.venv\Lib\site-packages``)
# the on-disk location of these files has nothing to do with where the
# user wants outputs to land. Anchor defaults on the *current working
# directory* instead, which is the repo root in normal usage.
def _default_output_dir() -> Path:
    return Path.cwd() / "outputs"


def _default_env_file() -> Path:
    return Path.cwd() / ".env"


class HarnessConfig(BaseModel):
    """Runtime knobs shared by every experiment."""

    base_url: str = Field(default="https://memclaw.net")
    api_key: str | None = None
    request_timeout_s: float = 30.0
    max_retries: int = 3
    # Minimum gap between writes, enforced client-side by the write rate
    # limiter. MemClaw caps writes at 10/second/tenant (429); 125 ms → 8/s
    # keeps the harness best-effort under that ceiling for bursts from
    # ``max_in_flight`` > 1. The cap is per-tenant and shared, so this is
    # not a hard guarantee — a concurrent writer can still trip a 429,
    # which the client retries with backoff (see MemClawClient._request).
    min_request_interval_ms: float = 125.0
    seed: int = 42
    output_dir: Path = Field(default_factory=_default_output_dir)
    run_label: str | None = None
    # Tenants the user's API key is authorised to target. If empty, each
    # experiment falls back to synthesising IDs from the seed — which only
    # works for admin/superuser keys that bypass ``enforce_tenant``.
    tenant_ids: list[str] = Field(default_factory=list)
    # Per-experiment overrides land here as a free-form dict.
    experiment: dict[str, Any] = Field(default_factory=dict)


def load_config(
    config_path: str | Path | None = None,
    *,
    env_file: str | Path | None = None,
    overrides: dict[str, Any] | None = None,
) -> HarnessConfig:
    """Build a ``HarnessConfig`` from .env + optional YAML + CLI overrides.

    Resolution order (later wins): YAML file, environment, explicit overrides.
    """

    if env_file is None:
        env_file = _default_env_file()
    load_dotenv(env_file, override=False)

    data: dict[str, Any] = {}
    if config_path:
        with open(config_path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}

    env_map = {
        "base_url": os.getenv("MEMCLAW_BASE_URL"),
        "api_key": os.getenv("MEMCLAW_API_KEY"),
        "seed": os.getenv("ARGUSFLEET_SEED"),
        "output_dir": os.getenv("ARGUSFLEET_OUTPUT_DIR"),
        "run_label": os.getenv("ARGUSFLEET_RUN_LABEL"),
        "tenant_ids": os.getenv("MEMCLAW_TENANT_ID")
        or os.getenv("MEMCLAW_TENANT_IDS"),
    }
    for k, v in env_map.items():
        # Treat a set-but-empty env var (e.g. the shipped ``.env.example``'s
        # ``MEMCLAW_API_KEY=``) as unset, so it doesn't clobber a YAML value
        # with ``''``. ``os.getenv`` returns ``''`` (not ``None``) for these.
        if v:
            data[k] = v
    if overrides:
        data.update(overrides)

    if "seed" in data and data["seed"] is not None:
        data["seed"] = int(data["seed"])
    if "output_dir" in data and data["output_dir"] is not None:
        data["output_dir"] = Path(data["output_dir"]).expanduser().resolve()
    # Allow ``MEMCLAW_TENANT_ID=mc_a,mc_b`` (single env var, comma-separated)
    # or a YAML list. Whitespace around commas is tolerated.
    if "tenant_ids" in data and data["tenant_ids"] is not None:
        if isinstance(data["tenant_ids"], str):
            data["tenant_ids"] = [
                t.strip() for t in data["tenant_ids"].split(",") if t.strip()
            ]

    return HarnessConfig.model_validate(data)

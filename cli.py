"""Typer CLI surface.

Layout intentionally matches the spec verbatim:

    python -m argusfleet run leakage
    python -m argusfleet run contradiction
    python -m argusfleet run scale
    python -m argusfleet plot latest

Non-leakage experiments are stubbed for the first milestone — they return
a NotImplementedError so a stray invocation can't silently succeed.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

import typer

from argusfleet.harness.config import HarnessConfig, load_config

app = typer.Typer(no_args_is_help=True, help="AgentFleetMem research harness")
run_app = typer.Typer(no_args_is_help=True, help="Run an experiment")
plot_app = typer.Typer(no_args_is_help=True, help="Plot results from a prior run")
report_app = typer.Typer(no_args_is_help=True, help="Aggregate results across experiments")
app.add_typer(run_app, name="run")
app.add_typer(plot_app, name="plot")
app.add_typer(report_app, name="report")


# Experiments that ``run all`` will execute, in order. Stays in sync with
# the dispatch table in ``plot latest``.
_ALL_EXPERIMENTS = ("leakage", "contradiction", "provenance", "propagation")


# ---------------------------------------------------------------------- #
# Helpers                                                                 #
# ---------------------------------------------------------------------- #


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s :: %(message)s",
    )


def _resolve_config(config: Path | None, seed: int | None, label: str | None) -> HarnessConfig:
    overrides: dict[str, object] = {}
    if seed is not None:
        overrides["seed"] = seed
    if label is not None:
        overrides["run_label"] = label
    return load_config(config_path=config, overrides=overrides)


async def _purge_tenants(cfg: HarnessConfig, tenant_ids: list[str]) -> dict[str, int]:
    """Soft-delete every memory in each of ``tenant_ids``.

    Tenants are passed explicitly (not read from ``cfg.tenant_ids``) so
    callers can purge the seed-synthesised IDs that experiments actually
    write to in the default (no ``MEMCLAW_TENANT_ID``) mode — those never
    land in ``cfg.tenant_ids``, so the old ``cfg``-driven purge silently
    cleared nothing. Duplicates and empty strings are dropped.

    Returns ``{tenant_id: count_before}`` so callers can log how much was
    cleared. Because the server's dedup detectors all filter
    ``deleted_at IS NULL``, this releases the near-duplicate gate that
    otherwise 409s repeated experiment runs against a reused tenant.
    """
    import asyncio as _asyncio

    from argusfleet.harness.client import MemClawError, memclaw_client

    cleared: dict[str, int] = {}
    targets = list(dict.fromkeys(t for t in tenant_ids if t))
    if not targets:
        logging.warning("cleanup: no tenants to purge")
        return cleared
    async with memclaw_client(
        cfg.base_url,
        cfg.api_key,
        timeout_s=cfg.request_timeout_s,
        max_retries=cfg.max_retries,
    ) as client:
        for tenant in targets:
            try:
                before = await client.count_memories(tenant_id=tenant, include_deleted=False)
                n = int((before.json or {}).get("total", 0)) if isinstance(before.json, dict) else 0
                await client.delete_all_memories(tenant_id=tenant)
                cleared[tenant] = n
                logging.info("cleanup: purged %d memories from tenant %s", n, tenant)
            except MemClawError as e:
                # A tenant the key isn't authorised for (e.g. a leakage
                # second-tenant synthesised from the seed) yields 403 —
                # skip it rather than aborting the whole purge.
                logging.warning("cleanup: skipped tenant %s (%s)", tenant, e)
            await _asyncio.sleep(0)  # cooperative yield between tenants
    return cleared


# ---------------------------------------------------------------------- #
# Commands                                                                #
# ---------------------------------------------------------------------- #


@run_app.command("leakage")
def run_leakage(
    config: Path | None = typer.Option(
        None, "--config", "-c", help="YAML config file (default: configs/leakage.yaml)"
    ),
    seed: int | None = typer.Option(None, "--seed", help="Override seed"),
    label: str | None = typer.Option(None, "--label", help="Override run label"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Debug logging"),
) -> None:
    """Run the cross-scope leakage experiment."""
    _configure_logging(verbose)
    from argusfleet.experiments import leakage

    if config is None:
        default = Path(__file__).resolve().parent / "configs" / "leakage.yaml"
        config = default if default.exists() else None
    cfg = _resolve_config(config, seed, label)
    if not cfg.api_key:
        typer.secho(
            "WARNING: no MEMCLAW_API_KEY set — calls to memclaw.net will fail with 401.",
            fg=typer.colors.YELLOW,
            err=True,
        )
    result = asyncio.run(leakage.run(cfg))
    typer.echo(json.dumps(result, indent=2, default=str))


@run_app.command("contradiction")
def run_contradiction(
    config: Path | None = typer.Option(
        None, "--config", "-c", help="YAML config (default: configs/contradiction.yaml)"
    ),
    seed: int | None = typer.Option(None, "--seed", help="Override seed"),
    label: str | None = typer.Option(None, "--label", help="Override run label"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Debug logging"),
) -> None:
    """Run the temporal contradiction experiment."""
    _configure_logging(verbose)
    from argusfleet.experiments import contradiction

    if config is None:
        default = Path(__file__).resolve().parent / "configs" / "contradiction.yaml"
        config = default if default.exists() else None
    cfg = _resolve_config(config, seed, label)
    if not cfg.api_key:
        typer.secho(
            "WARNING: no MEMCLAW_API_KEY set — calls to memclaw.net will fail with 401.",
            fg=typer.colors.YELLOW,
            err=True,
        )
    result = asyncio.run(contradiction.run(cfg))
    typer.echo(json.dumps(result, indent=2, default=str))


@run_app.command("provenance")
def run_provenance(
    config: Path | None = typer.Option(
        None, "--config", "-c", help="YAML config (default: configs/provenance.yaml)"
    ),
    seed: int | None = typer.Option(None, "--seed", help="Override seed"),
    label: str | None = typer.Option(None, "--label", help="Override run label"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Debug logging"),
) -> None:
    """Run the provenance reconstruction experiment."""
    _configure_logging(verbose)
    from argusfleet.experiments import provenance

    if config is None:
        default = Path(__file__).resolve().parent / "configs" / "provenance.yaml"
        config = default if default.exists() else None
    cfg = _resolve_config(config, seed, label)
    if not cfg.api_key:
        typer.secho(
            "WARNING: no MEMCLAW_API_KEY set — calls to memclaw.net will fail with 401.",
            fg=typer.colors.YELLOW,
            err=True,
        )
    result = asyncio.run(provenance.run(cfg))
    typer.echo(json.dumps(result, indent=2, default=str))


@run_app.command("propagation")
def run_propagation(
    config: Path | None = typer.Option(
        None, "--config", "-c", help="YAML config (default: configs/propagation.yaml)"
    ),
    seed: int | None = typer.Option(None, "--seed", help="Override seed"),
    label: str | None = typer.Option(None, "--label", help="Override run label"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Debug logging"),
) -> None:
    """Run the cross-agent propagation experiment."""
    _configure_logging(verbose)
    from argusfleet.experiments import propagation

    if config is None:
        default = Path(__file__).resolve().parent / "configs" / "propagation.yaml"
        config = default if default.exists() else None
    cfg = _resolve_config(config, seed, label)
    if not cfg.api_key:
        typer.secho(
            "WARNING: no MEMCLAW_API_KEY set — calls to memclaw.net will fail with 401.",
            fg=typer.colors.YELLOW,
            err=True,
        )
    result = asyncio.run(propagation.run(cfg))
    typer.echo(json.dumps(result, indent=2, default=str))


@run_app.command("scale")
def run_scale(
    config: Path | None = typer.Option(None, "--config", "-c"),
    seed: int | None = typer.Option(None, "--seed"),
) -> None:
    """Run the scale experiment (not yet implemented)."""
    typer.secho(
        "scale experiment is stubbed — lands in milestone 2",
        fg=typer.colors.YELLOW,
        err=True,
    )
    raise typer.Exit(code=1)


@run_app.command("all")
def run_all(
    seed: int | None = typer.Option(None, "--seed", help="Override seed for every experiment"),
    label: str | None = typer.Option(None, "--label", help="Override run label prefix"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Debug logging"),
    skip: list[str] = typer.Option(
        [], "--skip", help="Experiments to skip (repeatable)"
    ),
    no_report: bool = typer.Option(
        False, "--no-report", help="Don't generate the consolidated report at the end"
    ),
    cleanup: bool = typer.Option(
        True,
        "--cleanup/--no-cleanup",
        help="Purge each tenant before every experiment and after the suite "
        "(keeps runs reproducible by clearing MemClaw's dedup state). "
        "Use --no-cleanup to leave written memories on the server.",
    ),
) -> None:
    """Run every implemented experiment sequentially, then build a report.

    Each experiment uses its own ``configs/<name>.yaml`` defaults. Seed
    and label overrides apply to all of them. Failures in one experiment
    don't abort the rest — the report just shows which one was skipped.
    """
    _configure_logging(verbose)
    from argusfleet.experiments import (
        contradiction as exp_contradiction,
        leakage as exp_leakage,
        propagation as exp_propagation,
        provenance as exp_provenance,
    )

    dispatch = {
        "leakage": (exp_leakage, "leakage.yaml"),
        "contradiction": (exp_contradiction, "contradiction.yaml"),
        "provenance": (exp_provenance, "provenance.yaml"),
        "propagation": (exp_propagation, "propagation.yaml"),
    }

    summary: dict[str, dict[str, object]] = {}
    # Tenants touched across the suite, accumulated so the post-run purge
    # covers every tenant any experiment actually wrote to.
    all_tenants: list[str] = []
    for name in _ALL_EXPERIMENTS:
        if name in skip:
            typer.secho(f"skipping {name}", fg=typer.colors.YELLOW, err=True)
            summary[name] = {"status": "skipped"}
            continue
        module, config_name = dispatch[name]
        default_cfg = Path(__file__).resolve().parent / "configs" / config_name
        config_path = default_cfg if default_cfg.exists() else None
        cfg = _resolve_config(config_path, seed, label)
        if not cfg.api_key:
            typer.secho(
                f"WARNING ({name}): no MEMCLAW_API_KEY — calls to memclaw.net will fail.",
                fg=typer.colors.YELLOW,
                err=True,
            )
        # The tenants this experiment will actually write to — including
        # the seed-synthesised IDs used in the default mode. Tracked here
        # so the post-run purge covers them (cfg.tenant_ids is empty in
        # that mode, which is what made cleanup a silent no-op before).
        exp_tenants = module.tenant_ids_for(cfg)
        for t in exp_tenants:
            if t not in all_tenants:
                all_tenants.append(t)
        # Pre-clean so each experiment runs against a pristine tenant.
        # Reused tenants accumulate state that trips MemClaw's dedup gate
        # (409 near-duplicate), which is exactly what corrupted earlier
        # runs. Purging first makes every experiment reproducible.
        if cleanup:
            cleared = asyncio.run(_purge_tenants(cfg, exp_tenants))
            if cleared:
                typer.secho(
                    f"  cleaned {sum(cleared.values())} memories before {name}",
                    fg=typer.colors.BLUE,
                )
        typer.secho(f"\n>>> running {name}", fg=typer.colors.CYAN)
        try:
            result = asyncio.run(module.run(cfg))
            summary[name] = {"status": "ok", "run_dir": result.get("run_dir")}
        except Exception as e:  # noqa: BLE001
            logging.exception("experiment %s failed", name)
            summary[name] = {"status": "error", "error": str(e)}

    # Post-clean so the tenants are left empty for the next run / other
    # users. Purge every tenant the suite actually touched.
    if cleanup:
        cfg_final = _resolve_config(None, seed, label)
        cleared = asyncio.run(_purge_tenants(cfg_final, all_tenants))
        if cleared:
            typer.secho(
                f"\ncleanup: purged {sum(cleared.values())} memories across "
                f"{len(cleared)} tenant(s) after the suite",
                fg=typer.colors.BLUE,
            )
        else:
            typer.secho(
                "\ncleanup: nothing to purge after the suite "
                "(no tenants resolved — check seed/config)",
                fg=typer.colors.YELLOW,
                err=True,
            )

    typer.echo("\n=== run all — per-experiment outcome ===")
    typer.echo(json.dumps(summary, indent=2, default=str))

    if not no_report:
        from argusfleet.harness import report as report_mod

        out = load_config().output_dir
        result = report_mod.write_report(Path(out))
        typer.echo("\n=== consolidated report ===")
        typer.echo(result["markdown"])
        typer.secho(f"\nReport saved: {result['report_path']}", fg=typer.colors.GREEN)


@app.command("cleanup")
def cleanup(
    yes: bool = typer.Option(
        False, "--yes", "-y", help="Skip the confirmation prompt"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Debug logging"),
) -> None:
    """Soft-delete every memory in the configured tenant(s).

    Targets each tenant in ``MEMCLAW_TENANT_ID``. Soft-delete releases
    MemClaw's near-duplicate gate (its dedup queries filter
    ``deleted_at IS NULL``), so this is the reset to run between
    experiment campaigns on a shared tenant.
    """
    _configure_logging(verbose)
    cfg = _resolve_config(None, None, None)
    if not cfg.tenant_ids:
        typer.secho(
            "No MEMCLAW_TENANT_ID configured — nothing to purge.",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)
    if not cfg.api_key:
        typer.secho(
            "WARNING: no MEMCLAW_API_KEY set — the purge will fail with 401.",
            fg=typer.colors.YELLOW,
            err=True,
        )
    if not yes:
        target = ", ".join(cfg.tenant_ids)
        typer.confirm(
            f"Soft-delete ALL memories in tenant(s): {target}?", abort=True
        )
    cleared = asyncio.run(_purge_tenants(cfg, cfg.tenant_ids))
    typer.echo(json.dumps({"purged": cleared, "total": sum(cleared.values())}, indent=2))
    typer.secho(
        f"Purged {sum(cleared.values())} memories across {len(cleared)} tenant(s).",
        fg=typer.colors.GREEN,
    )


@report_app.command("latest")
def report_latest(
    output_dir: Path | None = typer.Option(
        None, "--output-dir", help="Override outputs root"
    ),
    no_copy_plots: bool = typer.Option(
        False, "--no-copy-plots", help="Skip copying PNGs into the report dir"
    ),
    quiet: bool = typer.Option(
        False, "--quiet", "-q", help="Suppress markdown stdout; print only the report path"
    ),
) -> None:
    """Generate a consolidated markdown report from the latest run per experiment."""
    from argusfleet.harness import report as report_mod

    cfg = load_config(overrides={"output_dir": output_dir} if output_dir else None)
    result = report_mod.write_report(Path(cfg.output_dir), copy_plots=not no_copy_plots)
    if not quiet:
        typer.echo(result["markdown"])
        typer.echo("")
    typer.secho(f"Report saved: {result['report_path']}", fg=typer.colors.GREEN)


@plot_app.command("latest")
def plot_latest(
    output_dir: Path | None = typer.Option(
        None, "--output-dir", help="Override outputs root"
    ),
) -> None:
    """Re-render plots from the most recent run's events.jsonl."""
    from argusfleet.harness.runner import latest_run_dir

    cfg = load_config(overrides={"output_dir": output_dir} if output_dir else None)
    runs_dir = Path(cfg.output_dir) / "runs"
    if not runs_dir.exists():
        typer.secho(f"no runs found under {runs_dir}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)
    # Select by run_start event ts (shared authority with report.py), not
    # filesystem mtime — re-materialising figures bumps mtime and would
    # otherwise reorder runs.
    latest = latest_run_dir(runs_dir)
    if latest is None:
        typer.secho(f"no runs found under {runs_dir}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)
    events = latest / "events.jsonl"
    if not events.exists():
        typer.secho(f"no events.jsonl in {latest}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)

    # Detect which experiment produced this run by reading the first
    # ``run_start`` event — fall back to leakage for legacy runs that
    # predate the dispatch table.
    experiment_name = _detect_experiment(events)
    if experiment_name == "contradiction":
        from argusfleet.experiments.contradiction import _materialize_outputs
    elif experiment_name == "provenance":
        from argusfleet.experiments.provenance import _materialize_outputs
    elif experiment_name == "propagation":
        from argusfleet.experiments.propagation import _materialize_outputs
    else:
        from argusfleet.experiments.leakage import _materialize_outputs

    result = _materialize_outputs(latest, events)
    typer.echo(json.dumps(result, indent=2, default=str))


def _detect_experiment(events_path: Path) -> str:
    import json as _json

    try:
        with open(events_path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = _json.loads(line)
                if rec.get("event") == "run_start":
                    return (rec.get("data") or {}).get("experiment", "leakage")
    except Exception:
        pass
    return "leakage"


if __name__ == "__main__":
    app()

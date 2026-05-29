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
        typer.secho(f"\n>>> running {name}", fg=typer.colors.CYAN)
        try:
            result = asyncio.run(module.run(cfg))
            summary[name] = {"status": "ok", "run_dir": result.get("run_dir")}
        except Exception as e:  # noqa: BLE001
            logging.exception("experiment %s failed", name)
            summary[name] = {"status": "error", "error": str(e)}

    typer.echo("\n=== run all — per-experiment outcome ===")
    typer.echo(json.dumps(summary, indent=2, default=str))

    if not no_report:
        from argusfleet.harness import report as report_mod

        out = load_config().output_dir
        result = report_mod.write_report(Path(out))
        typer.echo("\n=== consolidated report ===")
        typer.echo(result["markdown"])
        typer.secho(f"\nReport saved: {result['report_path']}", fg=typer.colors.GREEN)


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
    cfg = load_config(overrides={"output_dir": output_dir} if output_dir else None)
    runs_dir = Path(cfg.output_dir) / "runs"
    if not runs_dir.exists():
        typer.secho(f"no runs found under {runs_dir}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)
    latest = max(runs_dir.iterdir(), key=lambda p: p.stat().st_mtime, default=None)
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

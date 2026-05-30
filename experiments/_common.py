"""Shared helpers for experiment modules.

Kept private (``_common``) because nothing here is meant to be reused
outside the experiments package — these are conveniences, not stable API.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402


def bar_chart_from_frame(
    df: pd.DataFrame,
    *,
    x: str,
    y: str,
    out_path: Path,
    title: str,
    ylabel: str,
    label_fmt: Callable[[pd.Series], str] | None = None,
    ymax: float | None = None,
    yerr: list | None = None,
) -> Path:
    """Generic grouped-bar chart used by multiple experiments.

    ``yerr`` (optional) is a ``(2, N)`` sequence of asymmetric error-bar
    magnitudes ``[lower_errors, upper_errors]`` — e.g. Wilson-interval
    half-widths for proportion bars — drawn with caps.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(6, 4))
    if df.empty:
        ax.set_title(f"{title} (no data)")
        fig.tight_layout()
        fig.savefig(out_path, dpi=150)
        plt.close(fig)
        return out_path

    ax.bar(
        df[x].astype(str),
        df[y],
        yerr=yerr,
        capsize=4 if yerr is not None else 0.0,
        error_kw={"elinewidth": 1, "alpha": 0.8} if yerr is not None else None,
    )
    ax.set_xlabel(x)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    if ymax is not None:
        ax.set_ylim(0.0, ymax)
    else:
        ax.set_ylim(0.0, max(0.05, float(df[y].max()) * 1.2 + 0.01))
    if label_fmt is not None:
        # Lift the count label above the upper error cap (when present) so
        # the two don't collide.
        uppers = yerr[1] if yerr is not None else None
        for i, (_, row) in enumerate(df.iterrows()):
            y_text = row[y] + (uppers[i] if uppers is not None and i < len(uppers) else 0.0)
            ax.text(i, y_text, label_fmt(row), ha="center", va="bottom", fontsize=9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path

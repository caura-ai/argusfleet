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
) -> Path:
    """Generic grouped-bar chart used by multiple experiments."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(6, 4))
    if df.empty:
        ax.set_title(f"{title} (no data)")
        fig.tight_layout()
        fig.savefig(out_path, dpi=150)
        plt.close(fig)
        return out_path

    ax.bar(df[x].astype(str), df[y])
    ax.set_xlabel(x)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    if ymax is not None:
        ax.set_ylim(0.0, ymax)
    else:
        ax.set_ylim(0.0, max(0.05, float(df[y].max()) * 1.2 + 0.01))
    if label_fmt is not None:
        for i, (_, row) in enumerate(df.iterrows()):
            ax.text(i, row[y], label_fmt(row), ha="center", va="bottom", fontsize=9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path

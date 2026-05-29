"""Matplotlib plotting helpers shared across experiments.

Plots are deliberately spartan — single axes, default styling — so output
is reproducible across matplotlib versions and renders well in a paper
figure pipeline without extra theming.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless

import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402


def plot_leakage_rate(
    df: pd.DataFrame,
    *,
    by: str,
    out_path: Path,
    title: str = "Leak vs miss rate",
) -> Path:
    """Grouped bar chart: leak_rate (over expected-deny) and miss_rate
    (over expected-allow) side by side, grouped by ``by``.

    Splitting the two failure modes visually keeps the security story
    (leak_rate, smaller is better) from being washed out by retrieval
    recall noise (miss_rate).
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    grouped = (
        df.groupby(by)
        .agg(
            expected_allow=("expected_allow", "sum"),
            expected_deny=("expected_allow", lambda s: (s == False).sum()),  # noqa: E712
            leaks=("is_leak", "sum"),
            misses=("is_miss", "sum"),
        )
        .reset_index()
    )
    grouped["leak_rate"] = grouped["leaks"] / grouped["expected_deny"].clip(lower=1)
    grouped["miss_rate"] = grouped["misses"] / grouped["expected_allow"].clip(lower=1)
    ymax = max(0.1, float(max(grouped["leak_rate"].max(), grouped["miss_rate"].max())) * 1.2 + 0.01)

    fig, ax = plt.subplots(figsize=(7, 4.5))
    x = list(range(len(grouped)))
    width = 0.4
    ax.bar([i - width / 2 for i in x], grouped["leak_rate"], width=width, label="leak_rate")
    ax.bar([i + width / 2 for i in x], grouped["miss_rate"], width=width, label="miss_rate")
    ax.set_xticks(x, grouped[by].astype(str))
    ax.set_ylabel("Rate")
    ax.set_xlabel(by)
    ax.set_ylim(0.0, ymax)
    ax.set_title(title)
    ax.legend(loc="upper right")
    for i, row in grouped.iterrows():
        ax.text(i - width / 2, row["leak_rate"],
                f"{int(row['leaks'])}/{int(row['expected_deny'])}",
                ha="center", va="bottom", fontsize=8)
        ax.text(i + width / 2, row["miss_rate"],
                f"{int(row['misses'])}/{int(row['expected_allow'])}",
                ha="center", va="bottom", fontsize=8)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def plot_latency_hist(
    df: pd.DataFrame,
    *,
    col: str,
    out_path: Path,
    title: str = "Operation latency",
    bins: int = 30,
) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    series = pd.to_numeric(df[col], errors="coerce").dropna()
    fig, ax = plt.subplots(figsize=(6, 4))
    if not series.empty:
        ax.hist(series, bins=bins)
        ax.axvline(series.quantile(0.50), linestyle="--", linewidth=1, label="p50")
        ax.axvline(series.quantile(0.95), linestyle=":", linewidth=1, label="p95")
        ax.legend()
    ax.set_xlabel(col)
    ax.set_ylabel("count")
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path

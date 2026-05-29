#!/usr/bin/env bash
# Refresh paper/figures/ from the latest run dir per experiment.
#
# Usage (from the repo root):
#   bash scripts/refresh_paper_figures.sh
#
# Picks the most recent outputs/runs/<UTC-ts>-<experiment>-seed* for
# each of {leakage, contradiction, provenance, propagation} and copies
# the canonical plot set into paper/figures/ with experiment-prefixed
# filenames so the LaTeX references stay stable across runs.

set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
runs_dir="$repo_dir/outputs/runs"
figs_dir="$repo_dir/paper/figures"
mkdir -p "$figs_dir"

declare -A plots
plots[leakage]="leakage_by_axis.png search_latency.png"
plots[contradiction]="write_latency.png"
plots[provenance]="completeness_by_depth.png fetch_latency.png"
plots[propagation]="time_to_visibility.png visibility_by_relation.png"

for exp in leakage contradiction provenance propagation; do
    latest="$(find "$runs_dir" -maxdepth 1 -type d -name "*-${exp}-*" -printf '%T@ %p\n' \
        | sort -nr | head -n1 | cut -d' ' -f2-)"
    if [[ -z "$latest" ]]; then
        echo "WARN: no run found for $exp" >&2
        continue
    fi
    for plot in ${plots[$exp]}; do
        src="$latest/$plot"
        if [[ ! -f "$src" ]]; then
            echo "WARN: missing $src" >&2
            continue
        fi
        dst="$figs_dir/${exp}_${plot}"
        cp -f "$src" "$dst"
        echo "  $exp :: $plot -> $(basename "$dst")"
    done
    echo "[$exp] $(basename "$latest")"
done

echo
echo "Refreshed paper/figures/. Rebuild the paper with:"
echo "  cd paper && latexmk -pdf paper.tex"

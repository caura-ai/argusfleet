# Refresh paper/figures/ from the latest run dir per experiment.
#
# Usage (from the repo root):
#   pwsh ./scripts/refresh_paper_figures.ps1
#
# Picks the most recent ``outputs/runs/<UTC-ts>-<experiment>-seed*`` for
# each of {leakage, contradiction, provenance, propagation} and copies
# the canonical plot set into paper/figures/ with experiment-prefixed
# filenames so the LaTeX references stay stable across runs.

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$runs = Join-Path $repo 'outputs\runs'
$figs = Join-Path $repo 'paper\figures'
New-Item -ItemType Directory -Path $figs -Force | Out-Null

# Mapping: experiment -> list of plots the paper expects in figures/.
$map = @{
    'leakage'       = @('leakage_by_axis.png', 'search_latency.png')
    'contradiction' = @('write_latency.png')
    'provenance'    = @('completeness_by_depth.png', 'fetch_latency.png')
    'propagation'   = @('time_to_visibility.png', 'visibility_by_relation.png')
}

foreach ($exp in $map.Keys) {
    $latest = Get-ChildItem $runs -Directory `
        | Where-Object { $_.Name -like "*-$exp-*" } `
        | Sort-Object LastWriteTime -Descending `
        | Select-Object -First 1
    if (-not $latest) {
        Write-Warning ("no run found for " + $exp)
        continue
    }
    foreach ($plot in $map[$exp]) {
        $src = Join-Path $latest.FullName $plot
        if (-not (Test-Path $src)) {
            Write-Warning ("missing " + $src)
            continue
        }
        $dst = Join-Path $figs ($exp + '_' + $plot)
        Copy-Item $src $dst -Force
        Write-Output ("  " + $exp + " :: " + $plot + " -> " + (Split-Path -Leaf $dst))
    }
    Write-Output ("[" + $exp + "] " + $latest.Name)
}

Write-Output ""
Write-Output "Refreshed paper/figures/. Rebuild the paper with:"
Write-Output "  cd paper; latexmk -pdf paper.tex"

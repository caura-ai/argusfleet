# Paper

LaTeX source for *Governed Shared Memory for Multi-Agent LLM Systems*.

## Files

- `paper.tex` — main document (8 figures)
- `references.bib` — bibliography (starter; verify entries before submission)
- `extra - slide.tex` — single-slide Beamer summary (16:9 landscape, no nav chrome)
- `extra - onepager.md` — Markdown one-page executive summary
- `figures/` — PNG plots referenced by `paper.tex`
- `paper_overleaf.zip` — upload-ready bundle for Overleaf
- `../traces/` — committed `events.jsonl` + `summary.csv` for every run the paper cites

## Build

### Local (TeX Live / MiKTeX)

```bash
cd paper
pdflatex paper.tex
bibtex paper
pdflatex paper.tex
pdflatex paper.tex   # second pass to resolve cross-references
```

Or with `latexmk` (handles the bibtex passes automatically):

```bash
latexmk -pdf paper.tex
```

### Slide

The single-slide Beamer summary compiles standalone:

```bash
pdflatex "extra - slide.tex"
```

This produces `extra - slide.pdf` (one 16:9 page) — drop into a deck, print
as a poster handout, or screenshot for chat/social use.

### Overleaf

Upload `paper_overleaf.zip` as a new project: **New Project → Upload
Project → choose `paper_overleaf.zip`**. The zip contains `paper.tex`,
`references.bib`, and the `figures/` directory in the layout the LaTeX
expects.

After upload, Overleaf will run `pdflatex + bibtex + pdflatex + pdflatex`
automatically — no further configuration needed. The main TeX file
should be auto-detected as `paper.tex`; if Overleaf picks a different
file, set the "Main document" in Menu → Settings.

To **refresh** the project after a new local run:

```bash
# from the repo root
pwsh scripts/refresh_paper_figures.ps1            # update paper/figures/
pwsh -Command "Compress-Archive -Path paper\paper.tex,paper\references.bib,paper\figures -DestinationPath paper\paper_overleaf.zip -Force"
```

Then re-upload (or drag-drop individual files in Overleaf).

### Why explicit `figures/` paths

Every `\includegraphics` in `paper.tex` uses the explicit path
`figures/<name>.png`. `\graphicspath{{figures/}{./}}` is also set so
the same source compiles whether the figures sit in a `figures/`
subdirectory or alongside the `.tex` (some upload flows flatten the
tree). This is the most portable arrangement across Overleaf, local
builds, and CI.

## Source-of-truth for numbers and figures

All empirical numbers in §8 are taken from the latest run directories
under `../outputs/runs/<UTC-timestamp>-<experiment>-seed42/`. To
regenerate:

```bash
cd ..
python -m argusfleet run all
python -m argusfleet report latest
```

The consolidated report at `../outputs/reports/<UTC-timestamp>/report.md`
contains the same headline metrics that the paper cites. When numbers
in the paper drift from the latest run, regenerate the report and
update Table 1.

The exact event traces backing the published (N=200) numbers are committed
under `../traces/` (commit `2c55bb5`) — `events.jsonl` + `summary.csv` for
each run-of-record plus the four independent N=200 contradiction runs — so
the tables and figures regenerate from a frozen, verifiable source rather
than a transient local run. `../outputs/runs/` itself is gitignored.

## Refreshing figures after a re-run

Figures live in `paper/figures/` with names of the form
`<experiment>_<plot>.png`. They're pulled from the latest run dir per
experiment. After a new `run all`, refresh them with:

```bash
# PowerShell
pwsh ../scripts/refresh_paper_figures.ps1

# bash
bash ../scripts/refresh_paper_figures.sh
```

The script picks the most recent
`outputs/runs/<UTC-timestamp>-<experiment>-seed*` directory per
experiment and copies the canonical plot set into `paper/figures/` with
the names the LaTeX references. Filenames in `paper.tex` are stable —
only the underlying PNGs change. Then re-run `latexmk -pdf paper.tex`.

## How figures are wired into the LaTeX

`paper.tex` declares `\graphicspath{{figures/}}` so `\includegraphics`
calls don't need a directory prefix. Each figure environment looks like:

```latex
\begin{figure}[H]
\centering
\includegraphics[width=0.78\linewidth]{leakage_leakage_by_axis.png}
\caption{Leakage envelope by memory scope...}
\label{fig:leakage}
\end{figure}
```

And gets referenced in prose with `Fig.~\ref{fig:leakage}`. To add a new
figure: drop the PNG under `figures/`, add a `\begin{figure}` block, and
add a `\ref{}` where you mention it.

## Caveats before submission

- The bibliography contains placeholder entries; verify titles, venues,
  years, and pages.
- §8 numbers are from an N=200 clean-tenant run with traces committed
  under `../traces/`; re-verify against the latest run before the camera copy.
- The `traces/` reproducibility anchor is a branch commit hash
  (`2c55bb5`); convert it to a git tag or Zenodo DOI for submission.
- `MemClaw` and `ArgusFleet` citation entries are placeholder URLs;
  replace with the actual artifact links once finalised.

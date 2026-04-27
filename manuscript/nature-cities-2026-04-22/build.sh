#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/hadox/cmd-center/platforms/research/urban-sami"
MANUSCRIPT_DIR="$ROOT/manuscript/nature-cities-2026-04-22"
FIG_DIR="$MANUSCRIPT_DIR/figures"

mkdir -p "$FIG_DIR"

rsvg-convert -f pdf -o "$FIG_DIR/figure1_support_dependence.pdf" "$ROOT/dist/paper_figures/scale_comparison.svg"
rsvg-convert -f pdf -o "$FIG_DIR/figure2_city_overview.pdf" "$ROOT/reports/city-paper-figures-2026-04-22/figure1_city_overview.svg"
rsvg-convert -f pdf -o "$FIG_DIR/figure3_city_deviation_profiles.pdf" "$ROOT/reports/city-paper-figures-2026-04-22/figure2_city_deviation_profiles.svg"

cd "$MANUSCRIPT_DIR"
pdflatex -interaction=nonstopmode main.tex >/tmp/urban_sami_paper_pdflatex1.log
bibtex main >/tmp/urban_sami_paper_bibtex.log
pdflatex -interaction=nonstopmode main.tex >/tmp/urban_sami_paper_pdflatex2.log
pdflatex -interaction=nonstopmode main.tex >/tmp/urban_sami_paper_pdflatex3.log

echo "Built: $MANUSCRIPT_DIR/main.pdf"

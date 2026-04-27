#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/hadox/cmd-center/platforms/research/urban-sami"
MANUSCRIPT_DIR="$ROOT/manuscript/experimental-monograph-2026-04-24"

cd "$MANUSCRIPT_DIR"
pdflatex -interaction=nonstopmode main.tex >/tmp/urban_sami_experimental_monograph_pdflatex1.log
bibtex main >/tmp/urban_sami_experimental_monograph_bibtex.log
pdflatex -interaction=nonstopmode main.tex >/tmp/urban_sami_experimental_monograph_pdflatex2.log
pdflatex -interaction=nonstopmode main.tex >/tmp/urban_sami_experimental_monograph_pdflatex3.log

echo "Built: $MANUSCRIPT_DIR/main.pdf"

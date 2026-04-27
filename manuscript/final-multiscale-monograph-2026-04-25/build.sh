#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/hadox/cmd-center/platforms/research/urban-sami"
MANUSCRIPT_DIR="$ROOT/manuscript/final-multiscale-monograph-2026-04-25"

cd "$MANUSCRIPT_DIR"

run_pdflatex() {
  local log_path="$1"
  set +e
  pdflatex -interaction=nonstopmode main.tex >"$log_path"
  local status=$?
  set -e
  if rg -q "Undefined control sequence|Emergency stop|Fatal error" "$log_path"; then
    echo "pdflatex hard failure; see $log_path" >&2
    exit 1
  fi
  if [[ ! -f main.pdf ]]; then
    echo "pdflatex did not produce main.pdf; see $log_path" >&2
    exit 1
  fi
  if [[ $status -ne 0 ]]; then
    echo "pdflatex exited with code $status but produced a PDF; continuing" >&2
  fi
}

run_pdflatex /tmp/urban_sami_final_multiscale_monograph_pdflatex1.log
bibtex main >/tmp/urban_sami_final_multiscale_monograph_bibtex.log
run_pdflatex /tmp/urban_sami_final_multiscale_monograph_pdflatex2.log
run_pdflatex /tmp/urban_sami_final_multiscale_monograph_pdflatex3.log

echo "Built: $MANUSCRIPT_DIR/main.pdf"

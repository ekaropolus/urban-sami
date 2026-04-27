#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

from urban_sami.artifacts.figures import write_scale_comparison_figure


def _best_row(path: Path) -> dict:
    rows = list(csv.DictReader(path.open()))
    return max(rows, key=lambda row: float(row["r2"]))


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate cross-scale paper figures from independent baseline outputs")
    parser.add_argument("--state-summary", type=Path, default=Path("dist/independent_state_baseline/model_summary.csv"))
    parser.add_argument("--city-summary", type=Path, default=Path("dist/independent_city_baseline/model_summary.csv"))
    parser.add_argument("--ageb-summary", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=Path("dist/paper_figures"))
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    state = _best_row(args.state_summary)
    city = _best_row(args.city_summary)
    state["level"] = "state"
    city["level"] = "city"
    rows = [state, city]
    if args.ageb_summary:
        ageb = _best_row(args.ageb_summary)
        ageb["level"] = "ageb_u"
        rows.append(ageb)
    figure = write_scale_comparison_figure(
        rows,
        args.output_dir / "scale_comparison.svg",
        title="Cross-scale comparison of independent baselines",
    )

    report_path = args.output_dir / "paper_figures_manifest.csv"
    with report_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["figure_id", "path", "description"])
        writer.writeheader()
        writer.writerow(
            {
                "figure_id": "scale_comparison",
                "path": str(figure.resolve()),
                "description": "Best-fit β and R² for independent baselines across available scales.",
            }
        )
    print(figure)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

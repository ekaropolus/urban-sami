#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import subprocess
from pathlib import Path

from urban_sami.artifacts.figures import (
    write_model_overview_figure,
    write_residual_histogram_figure,
    write_scaling_scatter_figure,
)
from urban_sami.modeling import compute_deviation_score, fit_by_name


DOCKER_EXE = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DB_CONTAINER = "24-polisplexity-core-db-dev"
POSTGRES_USER = "postgres"
DB_NAME = "urban_sami_exp"


def _query(sql: str) -> list[dict[str, str]]:
    cmd = [
        DOCKER_EXE,
        "exec",
        "-i",
        DB_CONTAINER,
        "psql",
        "-U",
        POSTGRES_USER,
        "-d",
        DB_NAME,
        "-AtF",
        "\t",
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    ]
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    rows: list[dict[str, str]] = []
    for line in proc.stdout.splitlines():
        if not line.strip():
            continue
        state_code, state_name, est_count, population = line.split("\t")
        rows.append(
            {
                "state_code": state_code,
                "state_name": state_name,
                "est_count": est_count,
                "population": population,
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Run an independent state-level DENUE vs population baseline from urban_sami_exp")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("dist/independent_state_baseline"),
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    rows = _query(
        """
        WITH denue AS (
            SELECT state_code, MAX(state_name) AS state_name, COUNT(*)::double precision AS est_count
            FROM raw.denue_establishments
            WHERE country_code = 'MX' AND state_code <> ''
            GROUP BY state_code
        )
        SELECT p.unit_code AS state_code,
               COALESCE(NULLIF(p.unit_label, ''), denue.state_name, p.unit_code) AS state_name,
               COALESCE(denue.est_count, 0)::text AS est_count,
               COALESCE(p.population, 0)::text AS population
        FROM raw.population_units AS p
        LEFT JOIN denue ON denue.state_code = p.unit_code
        WHERE p.level = 'state' AND p.country_code = 'MX'
        ORDER BY p.unit_code
        """.strip()
    )

    counts_path = args.output_dir / "state_counts.csv"
    with counts_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["state_code", "state_name", "est_count", "population"])
        writer.writeheader()
        writer.writerows(rows)

    y = [float(row["est_count"]) for row in rows]
    n = [float(row["population"]) for row in rows]
    summary_rows = []
    for method in ("ols", "robust", "poisson", "negbin", "auto"):
        fit = fit_by_name(y, n, method)
        summary_rows.append(
            {
                "level": "state",
                "fit_method": method,
                "n_obs": len(rows),
                "alpha": fit.alpha,
                "beta": fit.beta,
                "r2": fit.r2,
                "resid_std": fit.residual_std,
            }
        )

    summary_path = args.output_dir / "model_summary.csv"
    with summary_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["level", "fit_method", "n_obs", "alpha", "beta", "r2", "resid_std"])
        writer.writeheader()
        writer.writerows(summary_rows)

    overview_path = write_model_overview_figure(summary_rows, args.output_dir / "model_overview.svg", title="Independent State Baseline")
    best_row = max(summary_rows, key=lambda row: float(row["r2"]))
    residual_rows = []
    residual_values = []
    fit_alpha = float(best_row["alpha"])
    fit_beta = float(best_row["beta"])
    for row in rows:
        population = float(row["population"])
        est_count = float(row["est_count"])
        if population <= 0.0 or est_count <= 0.0:
            continue
        score = compute_deviation_score(est_count, population, fit_alpha, fit_beta, float(best_row["resid_std"]))
        residual_values.append(score.epsilon_log)
        residual_rows.append(
            {
                "state_code": row["state_code"],
                "state_name": row["state_name"],
                "population": population,
                "est_count": est_count,
                "y_expected": score.y_expected,
                "epsilon_log": score.epsilon_log,
                "sami": score.sami,
                "z_residual": score.z_residual,
            }
        )
    scatter_path = write_scaling_scatter_figure(
        rows,
        args.output_dir / "scaling_scatter.svg",
        title="Mexico States: DENUE Establishments vs Population",
        x_key="population",
        y_key="est_count",
        fit_alpha=float(best_row["alpha"]),
        fit_beta=float(best_row["beta"]),
        annotation=f"{best_row['fit_method']}  β={float(best_row['beta']):.3f}  R²={float(best_row['r2']):.3f}",
    )
    residuals_path = args.output_dir / "residuals.csv"
    with residuals_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["state_code", "state_name", "population", "est_count", "y_expected", "epsilon_log", "sami", "z_residual"])
        writer.writeheader()
        writer.writerows(residual_rows)
    residual_hist_path = write_residual_histogram_figure(
        residual_values,
        args.output_dir / "residual_histogram.svg",
        title="State-level residual distribution",
        subtitle="Log residuals around the best-fitting state scaling model",
        bins=14,
    )

    report = {
        "workflow_id": "independent_state_baseline",
        "input_rows": len(rows),
        "counts_csv": str(counts_path.resolve()),
        "summary_csv": str(summary_path.resolve()),
        "overview_svg": str(overview_path.resolve()),
        "scatter_svg": str(scatter_path.resolve()),
        "residuals_csv": str(residuals_path.resolve()),
        "residual_histogram_svg": str(residual_hist_path.resolve()),
        "sami_definition": "sami_raw_log_deviation",
        "standardized_score_field": "z_residual",
        "best_fit_by_r2": best_row,
    }
    report_path = args.output_dir / "report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

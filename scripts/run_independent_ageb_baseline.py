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
TMP_POINT_TABLE = "staging.tmp_denue_city_points"


def _exec(sql: str) -> None:
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
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    ]
    subprocess.run(cmd, check=True, capture_output=True, text=True)


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
        unit_code, unit_label, city_code, city_name, est_count, population, households = line.split("\t")
        rows.append(
            {
                "unit_code": unit_code,
                "unit_label": unit_label,
                "city_code": city_code,
                "city_name": city_name,
                "est_count": est_count,
                "population": population,
                "households": households,
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Run an independent AGEB baseline for selected municipalities")
    parser.add_argument("--city-code", action="append", dest="city_codes", default=[], help="5-digit municipality code, repeatable")
    parser.add_argument("--output-dir", type=Path, default=Path("dist/independent_ageb_baseline"))
    args = parser.parse_args()

    city_codes = sorted(set(str(code).strip() for code in args.city_codes if str(code).strip()))
    if not city_codes:
        raise SystemExit("no city codes supplied")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    city_list_sql = ",".join(f"'{code}'" for code in city_codes)

    _exec(f"DROP TABLE IF EXISTS {TMP_POINT_TABLE};")
    _exec(
        f"""
        CREATE TABLE {TMP_POINT_TABLE} AS
        SELECT city_code,
               ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geom
        FROM raw.denue_establishments
        WHERE city_code IN ({city_list_sql})
          AND longitude IS NOT NULL
          AND latitude IS NOT NULL
        """.strip()
    )
    _exec(f"CREATE INDEX tmp_denue_city_points_city_idx ON {TMP_POINT_TABLE} (city_code);")
    _exec(f"CREATE INDEX tmp_denue_city_points_geom_gix ON {TMP_POINT_TABLE} USING GIST (geom);")
    _exec(f"ANALYZE {TMP_POINT_TABLE};")

    rows = _query(
        f"""
        WITH ageb AS (
            SELECT unit_code, unit_label, city_code, city_name, population, households, geom
            FROM raw.admin_units
            WHERE level = 'ageb_u' AND city_code IN ({city_list_sql})
        ),
        denue AS (
            SELECT a.unit_code, COUNT(*)::double precision AS est_count
            FROM ageb a
            JOIN {TMP_POINT_TABLE} d
              ON d.city_code = a.city_code
             AND ST_Covers(a.geom, d.geom)
            GROUP BY a.unit_code
        )
        SELECT a.unit_code,
               a.unit_label,
               a.city_code,
               a.city_name,
               COALESCE(d.est_count, 0)::text,
               COALESCE(a.population, 0)::text,
               COALESCE(a.households, 0)::text
        FROM ageb a
        LEFT JOIN denue d ON d.unit_code = a.unit_code
        ORDER BY a.city_code, a.unit_code
        """.strip()
    )
    _exec(f"DROP TABLE IF EXISTS {TMP_POINT_TABLE};")

    counts_path = args.output_dir / "ageb_counts.csv"
    with counts_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["unit_code", "unit_label", "city_code", "city_name", "est_count", "population", "households"])
        writer.writeheader()
        writer.writerows(rows)

    fit_rows = [row for row in rows if float(row["population"]) > 0.0 and float(row["est_count"]) > 0.0]
    y = [float(row["est_count"]) for row in fit_rows]
    n = [float(row["population"]) for row in fit_rows]
    summary_rows = []
    for method in ("ols", "robust", "poisson", "negbin", "auto"):
        fit = fit_by_name(y, n, method)
        summary_rows.append(
            {
                "level": "ageb_u",
                "fit_method": method,
                "n_obs": len(fit_rows),
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

    best_row = max(summary_rows, key=lambda row: float(row["r2"]))
    residual_rows = []
    residual_values = []
    fit_alpha = float(best_row["alpha"])
    fit_beta = float(best_row["beta"])
    for row in fit_rows:
        population = float(row["population"])
        est_count = float(row["est_count"])
        score = compute_deviation_score(est_count, population, fit_alpha, fit_beta, float(best_row["resid_std"]))
        residual_values.append(score.epsilon_log)
        residual_rows.append(
            {
                "unit_code": row["unit_code"],
                "unit_label": row["unit_label"],
                "city_code": row["city_code"],
                "city_name": row["city_name"],
                "population": population,
                "est_count": est_count,
                "households": float(row["households"]),
                "y_expected": score.y_expected,
                "epsilon_log": score.epsilon_log,
                "sami": score.sami,
                "z_residual": score.z_residual,
            }
        )

    overview_path = write_model_overview_figure(summary_rows, args.output_dir / "model_overview.svg", title="Independent AGEB Baseline")
    scatter_path = write_scaling_scatter_figure(
        fit_rows,
        args.output_dir / "scaling_scatter.svg",
        title=f"Urban AGEBs: DENUE Establishments vs Population ({', '.join(city_codes)})",
        x_key="population",
        y_key="est_count",
        fit_alpha=float(best_row["alpha"]),
        fit_beta=float(best_row["beta"]),
        annotation=f"{best_row['fit_method']}  β={float(best_row['beta']):.3f}  R²={float(best_row['r2']):.3f}",
    )
    residuals_path = args.output_dir / "residuals.csv"
    with residuals_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["unit_code", "unit_label", "city_code", "city_name", "population", "est_count", "households", "y_expected", "epsilon_log", "sami", "z_residual"])
        writer.writeheader()
        writer.writerows(residual_rows)
    residual_hist_path = write_residual_histogram_figure(
        residual_values,
        args.output_dir / "residual_histogram.svg",
        title="AGEB-level residual distribution",
        subtitle="Log residuals around the best-fitting AGEB scaling model",
        bins=24,
    )

    report = {
        "workflow_id": "independent_ageb_baseline",
        "city_codes": city_codes,
        "input_rows_total": len(rows),
        "input_rows_fit": len(fit_rows),
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

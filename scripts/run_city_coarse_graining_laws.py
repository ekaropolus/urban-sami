#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
import os
import shutil
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from urban_sami.analysis.linear_models import ols_fit


WINDOWS_DOCKER = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DOCKER_EXE = os.environ.get("DOCKER_EXE") or shutil.which("docker") or WINDOWS_DOCKER
DB_CONTAINER = os.environ.get("DB_CONTAINER", "24-polisplexity-core-db-dev")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
DB_NAME = os.environ.get("DB_NAME", "urban_sami_exp")

SOURCE_METHOD = "bettencourt_coarse_graining_laws_v1"
CITY_SOURCE_METHOD = "osm_drive_municipal_full_v1"
AGEB_SOURCE_METHOD = "osm_drive_ageb_overlay_v1"
OUTDIR = ROOT / "reports" / "city-coarse-graining-laws-2026-04-25"
THRESHOLDS = [3, 5, 10, 20]


def _bootstrap() -> None:
    script = ROOT / "scripts" / "bootstrap_experiment_db.sh"
    subprocess.run(["bash", str(script)], check=True)


def _psql_copy(sql: str) -> list[dict[str, str]]:
    script = f"""
\\set ON_ERROR_STOP on
COPY (
{sql}
) TO STDOUT WITH CSV HEADER;
"""
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
    ]
    proc = subprocess.run(cmd, input=script, text=True, capture_output=True, check=True)
    lines = proc.stdout.splitlines()
    if not lines:
        return []
    return list(csv.DictReader(lines))


def _psql_exec(sql: str) -> None:
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
    ]
    subprocess.run(cmd, input=sql, text=True, check=True)


def _copy_rows(table: str, columns: list[str], rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    lines: list[str] = []
    for row in rows:
        vals: list[str] = []
        for col in columns:
            value = row.get(col)
            if value is None:
                vals.append("\\N")
            else:
                text = str(value).replace("\t", " ").replace("\n", " ").replace("\r", " ")
                vals.append(text)
        lines.append("\t".join(vals) + "\n")
    sql = f"""
\\set ON_ERROR_STOP on
COPY {table} ({", ".join(columns)})
FROM STDIN WITH (FORMAT text);
"""
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
    ]
    subprocess.run(cmd, input=sql + "".join(lines) + "\\.\n", text=True, check=True)


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _sql_text(value: object) -> str:
    return str(value).replace("'", "''")


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _mean(values: list[float]) -> float:
    return sum(values) / float(len(values)) if values else 0.0


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2 == 1:
        return ordered[mid]
    return 0.5 * (ordered[mid - 1] + ordered[mid])


def _weighted_mean(values: list[float], weights: list[float]) -> float:
    total = sum(weights)
    if total <= 0:
        return 0.0
    return sum(v * w for v, w in zip(values, weights)) / total


def _query_manzana_rows() -> list[dict[str, object]]:
    rows = _psql_copy(
        """
        WITH est AS (
            SELECT
                city_code,
                ageb_code,
                manzana_code,
                COUNT(*)::double precision AS est_total
            FROM raw.denue_establishments
            WHERE city_code <> ''
              AND ageb_code <> ''
              AND manzana_code <> ''
            GROUP BY city_code, ageb_code, manzana_code
        )
        SELECT
            p.city_code,
            p.city_name,
            LEFT(p.city_code, 2) AS state_code,
            p.unit_code,
            p.population::text AS population,
            e.est_total::text AS est_total
        FROM raw.population_units p
        JOIN est e
          ON e.city_code = p.city_code
         AND e.ageb_code = p.ageb_code
         AND e.manzana_code = p.manzana_code
        WHERE p.level = 'manzana'
          AND COALESCE(p.population, 0.0) > 0
          AND e.est_total > 0
        ORDER BY p.city_code, p.unit_code
        """
    )
    out: list[dict[str, object]] = []
    for row in rows:
        out.append(
            {
                "city_code": row["city_code"],
                "city_name": row["city_name"],
                "state_code": row["state_code"],
                "unit_code": row["unit_code"],
                "population": _safe_float(row["population"]),
                "est_total": _safe_float(row["est_total"]),
            }
        )
    return out


def _query_ageb_rows() -> list[dict[str, object]]:
    rows = _psql_copy(
        f"""
        SELECT
            city_code,
            city_name,
            state_code,
            ageb_code AS unit_code,
            population::text,
            est_total::text
        FROM derived.ageb_network_metrics
        WHERE source_method = '{AGEB_SOURCE_METHOD}'
          AND COALESCE(population, 0.0) > 0
          AND COALESCE(est_total, 0.0) > 0
        ORDER BY city_code, ageb_code
        """
    )
    out: list[dict[str, object]] = []
    for row in rows:
        out.append(
            {
                "city_code": row["city_code"],
                "city_name": row["city_name"],
                "state_code": row["state_code"],
                "unit_code": row["unit_code"],
                "population": _safe_float(row["population"]),
                "est_total": _safe_float(row["est_total"]),
            }
        )
    return out


def _query_city_rows() -> list[dict[str, object]]:
    rows = _psql_copy(
        f"""
        SELECT
            city_code,
            city_name,
            state_code,
            city_code AS unit_code,
            population::text,
            est_total::text
        FROM derived.city_network_metrics
        WHERE source_method = '{CITY_SOURCE_METHOD}'
          AND COALESCE(population, 0.0) > 0
          AND COALESCE(est_total, 0.0) > 0
        ORDER BY city_code
        """
    )
    out: list[dict[str, object]] = []
    for row in rows:
        out.append(
            {
                "city_code": row["city_code"],
                "city_name": row["city_name"],
                "state_code": row["state_code"],
                "unit_code": row["unit_code"],
                "population": _safe_float(row["population"]),
                "est_total": _safe_float(row["est_total"]),
            }
        )
    return out


def _fit_unit_rows(rows: list[dict[str, object]]) -> dict[str, float] | None:
    if len(rows) < 3:
        return None
    x = [math.log(float(row["population"])) for row in rows]
    y = [math.log(float(row["est_total"])) for row in rows]
    design = [[1.0, value] for value in x]
    fit = ols_fit(design, y)
    resid_std = math.sqrt(fit.rss / float(fit.df_resid)) if fit.df_resid > 0 else 0.0
    return {
        "alpha": float(fit.coefficients[0]),
        "beta": float(fit.coefficients[1]),
        "r2": float(fit.r2),
        "adj_r2": float(fit.adj_r2),
        "rss": float(fit.rss),
        "resid_std": resid_std,
        "n_obs": float(fit.n_obs),
        "n_params": float(fit.n_params),
    }


def _fit_within_city_pooled(grouped_rows: dict[str, list[dict[str, object]]]) -> dict[str, float] | None:
    x_centered: list[float] = []
    y_centered: list[float] = []
    n_groups = 0
    total_obs = 0
    for city_rows in grouped_rows.values():
        if len(city_rows) < 2:
            continue
        x = [math.log(float(row["population"])) for row in city_rows]
        y = [math.log(float(row["est_total"])) for row in city_rows]
        x_bar = _mean(x)
        y_bar = _mean(y)
        if all(abs(value - x_bar) < 1e-12 for value in x):
            continue
        for xv, yv in zip(x, y):
            x_centered.append(xv - x_bar)
            y_centered.append(yv - y_bar)
        n_groups += 1
        total_obs += len(city_rows)
    if len(x_centered) < 3:
        return None
    fit = ols_fit([[value] for value in x_centered], y_centered)
    resid_std = math.sqrt(fit.rss / float(fit.df_resid)) if fit.df_resid > 0 else 0.0
    return {
        "alpha": 0.0,
        "beta": float(fit.coefficients[0]),
        "r2": float(fit.r2),
        "adj_r2": float(fit.adj_r2),
        "rss": float(fit.rss),
        "resid_std": resid_std,
        "n_obs": float(total_obs),
        "n_groups": float(n_groups),
        "n_params": float(fit.n_params),
    }


def _group_by_city(rows: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["city_code"])].append(row)
    return grouped


def _threshold_summary(
    rows: list[dict[str, object]],
    support_scale: str,
    threshold: int,
) -> dict[str, object]:
    usable = [row for row in rows if int(float(row["n_units"])) >= threshold]
    betas = [float(row["beta"]) for row in usable]
    r2s = [float(row["r2"]) for row in usable]
    resid = [float(row["resid_std"]) for row in usable]
    weights = [float(row["n_units"]) for row in usable]
    return {
        "support_scale": support_scale,
        "threshold_units": threshold,
        "n_cities": len(usable),
        "mean_beta": _mean(betas),
        "median_beta": _median(betas),
        "weighted_beta": _weighted_mean(betas, weights),
        "mean_r2": _mean(r2s),
        "median_r2": _median(r2s),
        "weighted_r2": _weighted_mean(r2s, weights),
        "mean_resid_std": _mean(resid),
    }


def _path_threshold_summary(paths: list[dict[str, object]], threshold: int) -> dict[str, object]:
    usable = [
        row for row in paths
        if int(float(row["n_manzana"])) >= threshold
        and int(float(row["n_ageb"])) >= threshold
    ]
    delta_beta = [float(row["delta_beta_ageb_minus_manzana"]) for row in usable]
    delta_r2 = [float(row["delta_r2_ageb_minus_manzana"]) for row in usable]
    delta_resid = [float(row["delta_resid_std_ageb_minus_manzana"]) for row in usable]
    weights = [min(float(row["n_manzana"]), float(row["n_ageb"])) for row in usable]
    return {
        "threshold_units": threshold,
        "n_cities": len(usable),
        "mean_delta_beta": _mean(delta_beta),
        "median_delta_beta": _median(delta_beta),
        "weighted_delta_beta": _weighted_mean(delta_beta, weights),
        "mean_delta_r2": _mean(delta_r2),
        "median_delta_r2": _median(delta_r2),
        "weighted_delta_r2": _weighted_mean(delta_r2, weights),
        "mean_delta_resid_std": _mean(delta_resid),
    }


def _prepare_tables() -> None:
    _psql_exec(
        f"""
        DELETE FROM experiments.city_coarse_graining_status
        WHERE source_method = '{SOURCE_METHOD}';
        DELETE FROM derived.city_coarse_graining_scale_summary
        WHERE source_method = '{SOURCE_METHOD}';
        DELETE FROM derived.city_coarse_graining_unit_fits
        WHERE source_method = '{SOURCE_METHOD}';
        DELETE FROM derived.city_coarse_graining_paths
        WHERE source_method = '{SOURCE_METHOD}';
        """
    )


def main() -> None:
    _bootstrap()
    OUTDIR.mkdir(parents=True, exist_ok=True)
    _prepare_tables()

    print("querying manzana rows...", flush=True)
    manzana_rows = _query_manzana_rows()
    print(f"manzana rows: {len(manzana_rows)}", flush=True)

    print("querying ageb rows...", flush=True)
    ageb_rows = _query_ageb_rows()
    print(f"ageb rows: {len(ageb_rows)}", flush=True)

    print("querying city rows...", flush=True)
    city_rows = _query_city_rows()
    print(f"city rows: {len(city_rows)}", flush=True)

    grouped_manzana = _group_by_city(manzana_rows)
    grouped_ageb = _group_by_city(ageb_rows)
    city_meta = {str(row["city_code"]): row for row in city_rows}

    scale_summary_rows: list[dict[str, object]] = []
    unit_fit_rows: list[dict[str, object]] = []
    path_rows: list[dict[str, object]] = []
    status_rows: list[dict[str, object]] = []

    for support_scale, rows in (
        ("manzana", manzana_rows),
        ("ageb", ageb_rows),
        ("city", city_rows),
    ):
        fit = _fit_unit_rows(rows)
        if fit:
            scale_summary_rows.append(
                {
                    "source_file": "raw.population_units+raw.denue_establishments",
                    "source_method": SOURCE_METHOD,
                    "support_scale": support_scale,
                    "scope": "national_raw",
                    "threshold_units": None,
                    "description": "National raw log-log law at this support scale.",
                    "n_obs": fit["n_obs"],
                    "n_groups": len({str(row['city_code']) for row in rows}),
                    "n_params": fit["n_params"],
                    "alpha": fit["alpha"],
                    "beta": fit["beta"],
                    "r2": fit["r2"],
                    "adj_r2": fit["adj_r2"],
                    "rss": fit["rss"],
                    "resid_std": fit["resid_std"],
                    "notes": "",
                }
            )

    for support_scale, grouped in (
        ("manzana", grouped_manzana),
        ("ageb", grouped_ageb),
    ):
        fit = _fit_within_city_pooled(grouped)
        if fit:
            scale_summary_rows.append(
                {
                    "source_file": "raw.population_units+raw.denue_establishments",
                    "source_method": SOURCE_METHOD,
                    "support_scale": support_scale,
                    "scope": "within_city_pooled",
                    "threshold_units": None,
                    "description": "Within-city pooled fixed-effect slope using city-demeaned logs.",
                    "n_obs": fit["n_obs"],
                    "n_groups": fit["n_groups"],
                    "n_params": fit["n_params"],
                    "alpha": None,
                    "beta": fit["beta"],
                    "r2": fit["r2"],
                    "adj_r2": fit["adj_r2"],
                    "rss": fit["rss"],
                    "resid_std": fit["resid_std"],
                    "notes": "",
                }
            )

    all_city_codes = sorted(set(grouped_manzana.keys()) | set(grouped_ageb.keys()) | set(city_meta.keys()))
    total_cities = len(all_city_codes)
    for idx, city_code in enumerate(all_city_codes, start=1):
        meta = city_meta.get(city_code) or {}
        city_name = str(meta.get("city_name") or "")
        state_code = str(meta.get("state_code") or city_code[:2])
        m_rows = grouped_manzana.get(city_code, [])
        a_rows = grouped_ageb.get(city_code, [])
        outputs = 0
        try:
            fits_by_scale: dict[str, dict[str, object]] = {}
            for support_scale, rows in (("manzana", m_rows), ("ageb", a_rows)):
                fit = _fit_unit_rows(rows)
                if not fit:
                    continue
                fits_by_scale[support_scale] = fit
                meta_pop = float(meta.get("population") or 0.0)
                meta_est = float(meta.get("est_total") or 0.0)
                unit_fit_rows.append(
                    {
                        "source_file": "raw.population_units+raw.denue_establishments",
                        "source_method": SOURCE_METHOD,
                        "support_scale": support_scale,
                        "city_code": city_code,
                        "city_name": city_name,
                        "state_code": state_code,
                        "population": meta_pop,
                        "est_total": meta_est,
                        "n_units": len(rows),
                        "alpha": fit["alpha"],
                        "beta": fit["beta"],
                        "r2": fit["r2"],
                        "adj_r2": fit["adj_r2"],
                        "rss": fit["rss"],
                        "resid_std": fit["resid_std"],
                        "notes": "",
                    }
                )
                outputs += 1
            if "manzana" in fits_by_scale and "ageb" in fits_by_scale:
                m_fit = fits_by_scale["manzana"]
                a_fit = fits_by_scale["ageb"]
                path_rows.append(
                    {
                        "source_file": "derived.city_coarse_graining_unit_fits",
                        "source_method": SOURCE_METHOD,
                        "city_code": city_code,
                        "city_name": city_name,
                        "state_code": state_code,
                        "population": float(meta.get("population") or 0.0),
                        "est_total": float(meta.get("est_total") or 0.0),
                        "n_manzana": len(m_rows),
                        "n_ageb": len(a_rows),
                        "beta_manzana": m_fit["beta"],
                        "beta_ageb": a_fit["beta"],
                        "delta_beta_ageb_minus_manzana": float(a_fit["beta"]) - float(m_fit["beta"]),
                        "r2_manzana": m_fit["r2"],
                        "r2_ageb": a_fit["r2"],
                        "delta_r2_ageb_minus_manzana": float(a_fit["r2"]) - float(m_fit["r2"]),
                        "resid_std_manzana": m_fit["resid_std"],
                        "resid_std_ageb": a_fit["resid_std"],
                        "delta_resid_std_ageb_minus_manzana": float(a_fit["resid_std"]) - float(m_fit["resid_std"]),
                        "notes": "",
                    }
                )
                outputs += 1
            status_rows.append(
                {
                    "source_method": SOURCE_METHOD,
                    "city_code": city_code,
                    "city_name": city_name,
                    "state_code": state_code,
                    "status": "success",
                    "n_manzana_rows": len(m_rows),
                    "n_ageb_rows": len(a_rows),
                    "output_rows": outputs,
                    "error_message": "",
                    "notes": f"{idx}/{total_cities}",
                }
            )
        except Exception as exc:
            status_rows.append(
                {
                    "source_method": SOURCE_METHOD,
                    "city_code": city_code,
                    "city_name": city_name,
                    "state_code": state_code,
                    "status": "error",
                    "n_manzana_rows": len(m_rows),
                    "n_ageb_rows": len(a_rows),
                    "output_rows": outputs,
                    "error_message": str(exc),
                    "notes": f"{idx}/{total_cities}",
                }
            )
        if idx % 250 == 0:
            print(f"processed {idx}/{total_cities} cities", flush=True)

    _copy_rows(
        "derived.city_coarse_graining_scale_summary",
        [
            "source_file",
            "source_method",
            "support_scale",
            "scope",
            "threshold_units",
            "description",
            "n_obs",
            "n_groups",
            "n_params",
            "alpha",
            "beta",
            "r2",
            "adj_r2",
            "rss",
            "resid_std",
            "notes",
        ],
        scale_summary_rows,
    )
    _copy_rows(
        "derived.city_coarse_graining_unit_fits",
        [
            "source_file",
            "source_method",
            "support_scale",
            "city_code",
            "city_name",
            "state_code",
            "population",
            "est_total",
            "n_units",
            "alpha",
            "beta",
            "r2",
            "adj_r2",
            "rss",
            "resid_std",
            "notes",
        ],
        unit_fit_rows,
    )
    _copy_rows(
        "derived.city_coarse_graining_paths",
        [
            "source_file",
            "source_method",
            "city_code",
            "city_name",
            "state_code",
            "population",
            "est_total",
            "n_manzana",
            "n_ageb",
            "beta_manzana",
            "beta_ageb",
            "delta_beta_ageb_minus_manzana",
            "r2_manzana",
            "r2_ageb",
            "delta_r2_ageb_minus_manzana",
            "resid_std_manzana",
            "resid_std_ageb",
            "delta_resid_std_ageb_minus_manzana",
            "notes",
        ],
        path_rows,
    )
    _copy_rows(
        "experiments.city_coarse_graining_status",
        [
            "source_method",
            "city_code",
            "city_name",
            "state_code",
            "status",
            "n_manzana_rows",
            "n_ageb_rows",
            "output_rows",
            "error_message",
            "notes",
        ],
        status_rows,
    )

    threshold_summary_rows: list[dict[str, object]] = []
    fits_by_scale: dict[str, list[dict[str, object]]] = {"manzana": [], "ageb": []}
    for row in unit_fit_rows:
        fits_by_scale[str(row["support_scale"])].append(row)
    for support_scale in ("manzana", "ageb"):
        for threshold in THRESHOLDS:
            threshold_summary_rows.append(
                _threshold_summary(fits_by_scale[support_scale], support_scale, threshold)
            )

    path_threshold_rows = [_path_threshold_summary(path_rows, threshold) for threshold in THRESHOLDS]

    leaderboard_pool = [
        row for row in path_rows
        if float(row["n_manzana"]) >= 10 and float(row["n_ageb"]) >= 10
    ]
    top_r2_gain = sorted(leaderboard_pool, key=lambda row: float(row["delta_r2_ageb_minus_manzana"]), reverse=True)[:25]
    bottom_r2_gain = sorted(leaderboard_pool, key=lambda row: float(row["delta_r2_ageb_minus_manzana"]))[:25]
    top_beta_gain = sorted(leaderboard_pool, key=lambda row: float(row["delta_beta_ageb_minus_manzana"]), reverse=True)[:25]
    bottom_beta_gain = sorted(leaderboard_pool, key=lambda row: float(row["delta_beta_ageb_minus_manzana"]))[:25]

    _write_csv(
        OUTDIR / "scale_summary.csv",
        scale_summary_rows,
        [
            "source_file",
            "source_method",
            "support_scale",
            "scope",
            "threshold_units",
            "description",
            "n_obs",
            "n_groups",
            "n_params",
            "alpha",
            "beta",
            "r2",
            "adj_r2",
            "rss",
            "resid_std",
            "notes",
        ],
    )
    _write_csv(
        OUTDIR / "city_unit_fits.csv",
        unit_fit_rows,
        [
            "source_file",
            "source_method",
            "support_scale",
            "city_code",
            "city_name",
            "state_code",
            "population",
            "est_total",
            "n_units",
            "alpha",
            "beta",
            "r2",
            "adj_r2",
            "rss",
            "resid_std",
            "notes",
        ],
    )
    _write_csv(
        OUTDIR / "city_paths.csv",
        path_rows,
        [
            "source_file",
            "source_method",
            "city_code",
            "city_name",
            "state_code",
            "population",
            "est_total",
            "n_manzana",
            "n_ageb",
            "beta_manzana",
            "beta_ageb",
            "delta_beta_ageb_minus_manzana",
            "r2_manzana",
            "r2_ageb",
            "delta_r2_ageb_minus_manzana",
            "resid_std_manzana",
            "resid_std_ageb",
            "delta_resid_std_ageb_minus_manzana",
            "notes",
        ],
    )
    _write_csv(
        OUTDIR / "threshold_summary.csv",
        threshold_summary_rows,
        [
            "support_scale",
            "threshold_units",
            "n_cities",
            "mean_beta",
            "median_beta",
            "weighted_beta",
            "mean_r2",
            "median_r2",
            "weighted_r2",
            "mean_resid_std",
        ],
    )
    _write_csv(
        OUTDIR / "path_threshold_summary.csv",
        path_threshold_rows,
        [
            "threshold_units",
            "n_cities",
            "mean_delta_beta",
            "median_delta_beta",
            "weighted_delta_beta",
            "mean_delta_r2",
            "median_delta_r2",
            "weighted_delta_r2",
            "mean_delta_resid_std",
        ],
    )
    leaderboard_fields = [
        "city_code",
        "city_name",
        "state_code",
        "population",
        "est_total",
        "n_manzana",
        "n_ageb",
        "beta_manzana",
        "beta_ageb",
        "delta_beta_ageb_minus_manzana",
        "r2_manzana",
        "r2_ageb",
        "delta_r2_ageb_minus_manzana",
        "resid_std_manzana",
        "resid_std_ageb",
        "delta_resid_std_ageb_minus_manzana",
    ]
    _write_csv(OUTDIR / "top_r2_gain.csv", top_r2_gain, leaderboard_fields)
    _write_csv(OUTDIR / "bottom_r2_gain.csv", bottom_r2_gain, leaderboard_fields)
    _write_csv(OUTDIR / "top_beta_gain.csv", top_beta_gain, leaderboard_fields)
    _write_csv(OUTDIR / "bottom_beta_gain.csv", bottom_beta_gain, leaderboard_fields)

    scale_lookup = {(row["support_scale"], row["scope"]): row for row in scale_summary_rows}
    manzana_raw = scale_lookup.get(("manzana", "national_raw"), {})
    ageb_raw = scale_lookup.get(("ageb", "national_raw"), {})
    city_raw = scale_lookup.get(("city", "national_raw"), {})
    manzana_within = scale_lookup.get(("manzana", "within_city_pooled"), {})
    ageb_within = scale_lookup.get(("ageb", "within_city_pooled"), {})
    path10 = next((row for row in path_threshold_rows if int(row["threshold_units"]) == 10), {})

    report_lines = [
        "# City Coarse-Graining Laws",
        "",
        "## Purpose",
        "",
        "This phase studies how the population-establishment law changes when the spatial support is coarse-grained from `manzana` to `AGEB` and then to `city`.",
        "",
        "The support-scale law is always:",
        "",
        "```math",
        "\\log Y = \\alpha(r) + \\beta(r) \\log N + \\varepsilon(r)",
        "```",
        "",
        "with `Y = total establishments`, `N = population`, and `r` the support scale.",
        "",
        "## National support-scale summary",
        "",
        f"- `manzana raw`: beta = `{float(manzana_raw.get('beta') or 0.0):.4f}`, adjR² = `{float(manzana_raw.get('adj_r2') or 0.0):.4f}`, n = `{int(float(manzana_raw.get('n_obs') or 0.0))}`",
        f"- `AGEB raw`: beta = `{float(ageb_raw.get('beta') or 0.0):.4f}`, adjR² = `{float(ageb_raw.get('adj_r2') or 0.0):.4f}`, n = `{int(float(ageb_raw.get('n_obs') or 0.0))}`",
        f"- `city raw`: beta = `{float(city_raw.get('beta') or 0.0):.4f}`, adjR² = `{float(city_raw.get('adj_r2') or 0.0):.4f}`, n = `{int(float(city_raw.get('n_obs') or 0.0))}`",
        "",
        "## Within-city pooled summary",
        "",
        f"- `manzana FE`: beta = `{float(manzana_within.get('beta') or 0.0):.4f}`, adjR² = `{float(manzana_within.get('adj_r2') or 0.0):.4f}`",
        f"- `AGEB FE`: beta = `{float(ageb_within.get('beta') or 0.0):.4f}`, adjR² = `{float(ageb_within.get('adj_r2') or 0.0):.4f}`",
        "",
        "## City path summary at threshold 10",
        "",
        f"- cities with both scales usable: `{int(path10.get('n_cities') or 0)}`",
        f"- mean `delta beta = beta_AGEB - beta_manzana`: `{float(path10.get('mean_delta_beta') or 0.0):.4f}`",
        f"- mean `delta R² = R²_AGEB - R²_manzana`: `{float(path10.get('mean_delta_r2') or 0.0):.4f}`",
        f"- mean `delta resid std = sigma_AGEB - sigma_manzana`: `{float(path10.get('mean_delta_resid_std') or 0.0):.4f}`",
        "",
        "## Files",
        f"- [scale_summary.csv]({(OUTDIR / 'scale_summary.csv').resolve()})",
        f"- [city_unit_fits.csv]({(OUTDIR / 'city_unit_fits.csv').resolve()})",
        f"- [city_paths.csv]({(OUTDIR / 'city_paths.csv').resolve()})",
        f"- [threshold_summary.csv]({(OUTDIR / 'threshold_summary.csv').resolve()})",
        f"- [path_threshold_summary.csv]({(OUTDIR / 'path_threshold_summary.csv').resolve()})",
        f"- [top_r2_gain.csv]({(OUTDIR / 'top_r2_gain.csv').resolve()})",
        f"- [bottom_r2_gain.csv]({(OUTDIR / 'bottom_r2_gain.csv').resolve()})",
        f"- [top_beta_gain.csv]({(OUTDIR / 'top_beta_gain.csv').resolve()})",
        f"- [bottom_beta_gain.csv]({(OUTDIR / 'bottom_beta_gain.csv').resolve()})",
        "",
    ]
    (OUTDIR / "report.md").write_text("\n".join(report_lines), encoding="utf-8")

    monograph_lines = [
        "# Coarse-Graining of the Population-Establishment Law",
        "",
        "## Why this phase exists",
        "",
        "The previous phases showed three things:",
        "",
        "1. the city-level law is strong,",
        "2. the law weakens when we descend inside cities,",
        "3. much of the internal economic organization lives below the `AGEB`, at `manzana` scale.",
        "",
        "This phase asks the statistical-physics question directly: how does the law change when we change the support scale?",
        "",
        "## Mathematical object",
        "",
        "At each support scale `r`, we fit:",
        "",
        "```math",
        "\\log Y_{q}^{(r)} = \\alpha(r) + \\beta(r) \\log N_{q}^{(r)} + \\varepsilon_{q}^{(r)}",
        "```",
        "",
        "where:",
        "- `r = manzana`, `AGEB`, or `city`",
        "- `q` is a unit at that support scale",
        "- `Y` is total establishments",
        "- `N` is population",
        "",
        "We then read the scale-flow through:",
        "",
        "```math",
        "\\beta(r),\\qquad R^2(r),\\qquad \\sigma_\\varepsilon(r).",
        "```",
        "",
        "## Two complementary readings",
        "",
        "### 1. National support laws",
        "",
        "These compare all units of the same support across the country.",
        "",
        "```math",
        "\\log Y = \\alpha(r)+\\beta(r)\\log N+\\varepsilon(r)",
        "```",
        "",
        "This gives the macro support law at `manzana`, `AGEB`, and `city`.",
        "",
        "### 2. Within-city pooled laws",
        "",
        "For `manzana` and `AGEB`, we also remove city baselines and fit the slope on city-demeaned logs:",
        "",
        "```math",
        "(\\log Y_{iq}^{(r)}-\\overline{\\log Y}_{i}^{(r)})",
        "=",
        "\\beta_{within}(r)\\,(\\log N_{iq}^{(r)}-\\overline{\\log N}_{i}^{(r)})+u_{iq}^{(r)}.",
        "```",
        "",
        "This isolates the support-scale relation inside cities instead of mixing cross-city intercept differences.",
        "",
        "## Why city is different",
        "",
        "At `city` scale there is only one unit per city, so there is no within-city path to estimate. The city-scale endpoint is therefore national, not per-city.",
        "",
        "This is not a bug. It is the point of the hierarchy:",
        "",
        "```math",
        "\\text{manzana} \\rightarrow \\text{AGEB} \\rightarrow \\text{city}.",
        "```",
        "",
        "The first two levels tell us how the law behaves inside cities. The last level tells us the emergent macro law once the support has already been coarse-grained into whole cities.",
        "",
        "## City path statistics",
        "",
        "For each city with usable fits at both `manzana` and `AGEB`, we compute:",
        "",
        "```math",
        "\\Delta\\beta_i = \\beta_{i,AGEB}-\\beta_{i,manzana}",
        "```",
        "",
        "```math",
        "\\Delta R^2_i = R^2_{i,AGEB}-R^2_{i,manzana}",
        "```",
        "",
        "```math",
        "\\Delta\\sigma_i = \\sigma_{i,AGEB}-\\sigma_{i,manzana}.",
        "```",
        "",
        "Interpretation:",
        "- if `Delta R² > 0`, aggregation to `AGEB` strengthens the law in that city",
        "- if `Delta sigma < 0`, aggregation reduces residual dispersion",
        "- if `Delta beta` shifts upward, the slope becomes steeper under coarse-graining",
        "",
        "## Outputs",
        "",
        f"- [report.md]({(OUTDIR / 'report.md').resolve()})",
        f"- [scale_summary.csv]({(OUTDIR / 'scale_summary.csv').resolve()})",
        f"- [city_paths.csv]({(OUTDIR / 'city_paths.csv').resolve()})",
        f"- [threshold_summary.csv]({(OUTDIR / 'threshold_summary.csv').resolve()})",
        f"- [path_threshold_summary.csv]({(OUTDIR / 'path_threshold_summary.csv').resolve()})",
        "",
        "## Persistence",
        "",
        "This phase is persisted in:",
        "- `experiments.city_coarse_graining_status`",
        "- `derived.city_coarse_graining_scale_summary`",
        "- `derived.city_coarse_graining_unit_fits`",
        "- `derived.city_coarse_graining_paths`",
        "",
        "So the experiment is no longer an external CSV workflow. It is now part of the persistent database program.",
        "",
    ]
    (OUTDIR / "monograph.md").write_text("\n".join(monograph_lines), encoding="utf-8")

    print("done", flush=True)


if __name__ == "__main__":
    main()

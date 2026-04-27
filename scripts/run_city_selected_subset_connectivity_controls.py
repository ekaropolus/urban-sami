#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
import random
import shutil
import subprocess
from pathlib import Path

from urban_sami.analysis.linear_models import compare_nested_models, ols_fit, pearson_corr
from urban_sami.indicators.denue import sector_prefix, size_class_from_per_ocu


DOCKER_EXE = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DB_CONTAINER = "24-polisplexity-core-db-dev"
POSTGRES_USER = "postgres"
DB_NAME = "urban_sami_exp"

N_RANDOM = 40
SEED = 20260423

BG = "#f8f6f1"
PANEL = "#fffdf8"
GRID = "#ddd6c8"
TEXT = "#1f1f1f"
MUTED = "#625d54"
AXIS = "#8b8478"
TEAL = "#0f766e"
RUST = "#b14d3b"
BLUE = "#315c80"
SANS = "Helvetica, Arial, sans-serif"
SERIF = "Georgia, 'Times New Roman', serif"

KEEP_SCIAN2 = {"81", "46", "31", "62", "54"}
KEEP_SIZE = {"micro", "medium"}


def _query_tsv(sql: str, columns: list[str]) -> list[dict[str, str]]:
    cmd = [
        DOCKER_EXE, "exec", "-i", DB_CONTAINER, "psql",
        "-U", POSTGRES_USER, "-d", DB_NAME, "-AtF", "\t", "-v", "ON_ERROR_STOP=1", "-c", sql,
    ]
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    rows: list[dict[str, str]] = []
    for line in proc.stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        rows.append({col: (parts[idx] if idx < len(parts) else "") for idx, col in enumerate(columns)})
    return rows


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _mean(values: list[float]) -> float:
    return sum(values) / float(len(values)) if values else 0.0


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mu = _mean(values)
    return math.sqrt(sum((v - mu) ** 2 for v in values) / float(len(values) - 1))


def _svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _fetch_ageb_units(city_code: str) -> list[dict[str, str]]:
    return _query_tsv(
        f"""
        SELECT
            unit_code,
            COALESCE(population, 0)::text AS population,
            (COALESCE(ST_Area(geom::geography), 0) / 1000000.0)::text AS area_km2
        FROM raw.admin_units
        WHERE level = 'ageb_u' AND city_code = '{city_code}'
        ORDER BY unit_code
        """.strip(),
        ["unit_code", "population", "area_km2"],
    )


def _fetch_assigned_points(city_code: str) -> list[dict[str, str]]:
    return _query_tsv(
        f"""
        WITH ageb AS (
            SELECT unit_code, geom
            FROM raw.admin_units
            WHERE level = 'ageb_u' AND city_code = '{city_code}'
        ),
        points AS (
            SELECT
                scian_code,
                per_ocu,
                ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geom
            FROM raw.denue_establishments
            WHERE city_code = '{city_code}'
              AND longitude IS NOT NULL
              AND latitude IS NOT NULL
        )
        SELECT
            a.unit_code,
            p.scian_code,
            p.per_ocu
        FROM ageb a
        JOIN points p
          ON ST_Covers(a.geom, p.geom)
        """.strip(),
        ["unit_code", "scian_code", "per_ocu"],
    )


def _build_ageb_feature_map(city_code: str) -> tuple[list[str], dict[str, dict[str, float]]]:
    units = _fetch_ageb_units(city_code)
    points = _fetch_assigned_points(city_code)
    by_code: dict[str, dict[str, float]] = {}
    for row in units:
        code = row["unit_code"]
        area = _safe_float(row["area_km2"], 0.0)
        pop = _safe_float(row["population"], 0.0)
        by_code[code] = {
            "population": pop,
            "area_km2": area,
            "est_total": 0.0,
            "scian2_81": 0.0,
            "scian2_46": 0.0,
            "scian2_31": 0.0,
            "scian2_62": 0.0,
            "scian2_54": 0.0,
            "size_micro": 0.0,
            "size_medium": 0.0,
        }
    for pt in points:
        code = pt["unit_code"]
        if code not in by_code:
            continue
        sc2 = sector_prefix(pt["scian_code"])
        size = size_class_from_per_ocu(pt["per_ocu"])
        by_code[code]["est_total"] += 1.0
        if sc2 in KEEP_SCIAN2:
            by_code[code][f"scian2_{sc2}"] += 1.0
        if size in KEEP_SIZE:
            by_code[code][f"size_{size}"] += 1.0
    return sorted(by_code), by_code


def _subset_controls(unit_codes: list[str], feature_map: dict[str, dict[str, float]], subset_area_km2: float) -> dict[str, float]:
    total_pop = sum(feature_map[c]["population"] for c in unit_codes)
    total_unit_area = sum(feature_map[c]["area_km2"] for c in unit_codes)
    total_est = sum(feature_map[c]["est_total"] for c in unit_codes)
    total_est = max(total_est, 1e-9)
    out = {
        "subset_population": total_pop,
        "subset_unit_area_km2": total_unit_area,
        "subset_population_density": total_pop / max(subset_area_km2, 1e-9),
        "share_81": sum(feature_map[c]["scian2_81"] for c in unit_codes) / total_est,
        "share_46": sum(feature_map[c]["scian2_46"] for c in unit_codes) / total_est,
        "share_31": sum(feature_map[c]["scian2_31"] for c in unit_codes) / total_est,
        "share_62": sum(feature_map[c]["scian2_62"] for c in unit_codes) / total_est,
        "share_54": sum(feature_map[c]["scian2_54"] for c in unit_codes) / total_est,
        "share_micro": sum(feature_map[c]["size_micro"] for c in unit_codes) / total_est,
        "share_medium": sum(feature_map[c]["size_medium"] for c in unit_codes) / total_est,
    }
    return out


def _add_city_fixed_effects(design: list[list[float]], rows: list[dict[str, float]], cities: list[str]) -> list[list[float]]:
    baseline = cities[0]
    out: list[list[float]] = []
    for vec, row in zip(design, rows):
        ext = vec[:]
        for code in cities[1:]:
            ext.append(1.0 if row["city_code"] == code else 0.0)
        out.append(ext)
    return out


def _fit_model(rows: list[dict[str, float]], predictor_keys: list[str], cities: list[str]):
    y = [float(r["selected_flag"]) for r in rows]
    design = [[1.0] + [float(r[k]) for k in predictor_keys] for r in rows]
    design = _add_city_fixed_effects(design, rows, cities)
    return ols_fit(design, y)


def _model_row(name: str, fit, predictor_keys: list[str], cities: list[str]) -> dict[str, object]:
    return {
        "model": name,
        "n_obs": fit.n_obs,
        "n_params": fit.n_params,
        "r2": fit.r2,
        "adj_r2": fit.adj_r2,
        "rss": fit.rss,
        "predictors": ",".join(predictor_keys),
        "city_fe_n": len(cities) - 1,
    }


def _coefficient_rows(model_name: str, fit, predictor_keys: list[str], cities: list[str]) -> list[dict[str, object]]:
    rows = []
    coef_names = ["intercept"] + predictor_keys + [f"city_fe::{c}" for c in cities[1:]]
    for name, coef, se in zip(coef_names, fit.coefficients, fit.stderr):
        if name.startswith("city_fe::") or name == "intercept":
            continue
        rows.append(
            {
                "model": model_name,
                "term": name,
                "coefficient": coef,
                "stderr": se,
                "t_approx": (coef / se) if se > 0 else 0.0,
            }
        )
    return rows


def _write_model_chart(path: Path, title: str, rows: list[dict[str, object]]) -> Path:
    width = 1080
    left = 220
    right = 70
    top = 96
    bottom = 78
    row_h = 30
    height = top + len(rows) * row_h + bottom
    vals = [float(r["adj_r2"]) for r in rows]
    xmax = max(vals) * 1.1 if vals else 1.0

    def px(v: float) -> float:
        return left + (v / max(xmax, 1e-9)) * (width - left - right)

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Selected subset indicator with city fixed effects.</text>',
    ]
    for i, row in enumerate(rows):
        y = top + i * row_h
        v = float(row["adj_r2"])
        body.append(f'<text x="{left-12}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{row["model"]}</text>')
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{px(v):.2f}" y2="{y:.2f}" stroke="{TEAL}" stroke-width="8"/>')
        body.append(f'<text x="{px(v)+8:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">adjR²={v:.3f}</text>')
    return _svg(path, width, height, "".join(body))


def _write_coef_chart(path: Path, title: str, rows: list[dict[str, object]]) -> Path:
    width = 1120
    left = 260
    right = 80
    top = 96
    bottom = 78
    row_h = 30
    height = top + len(rows) * row_h + bottom
    max_abs = max((abs(float(r["coefficient"])) + abs(float(r["stderr"]))) for r in rows) if rows else 1.0

    def px(v: float) -> float:
        return left + ((v + max_abs) / (2 * max_abs)) * (width - left - right)

    x0 = px(0.0)
    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Coefficients from the full city-fixed-effects model. Predictors are standardized within city.</text>',
        f'<line x1="{x0:.2f}" y1="{top-16}" x2="{x0:.2f}" y2="{height-bottom+8}" stroke="{GRID}" stroke-width="1.5"/>',
    ]
    for i, row in enumerate(rows):
        y = top + i * row_h
        coef = float(row["coefficient"])
        se = float(row["stderr"])
        lo = coef - se
        hi = coef + se
        color = TEAL if coef >= 0 else RUST
        body.append(f'<text x="{left-12}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{row["term"]}</text>')
        body.append(f'<line x1="{px(lo):.2f}" y1="{y:.2f}" x2="{px(hi):.2f}" y2="{y:.2f}" stroke="{AXIS}" stroke-width="2"/>')
        body.append(f'<circle cx="{px(coef):.2f}" cy="{y:.2f}" r="4.5" fill="{color}"/>')
        body.append(f'<text x="{px(coef)+8:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">{coef:.2f}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "city-selected-subset-connectivity-controls-2026-04-23"
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    selected_rows = _read_csv(root / "reports" / "city-selected-subset-random-connectivity-baseline-2026-04-23" / "city_selected_subset_metrics.csv")
    random_rows = _read_csv(root / "reports" / "city-selected-subset-random-connectivity-baseline-2026-04-23" / "city_random_subset_metrics.csv")
    city_summary = _read_csv(root / "reports" / "city-best-local-ageb-subsets-2026-04-22" / "city_best_subset_summary.csv")
    selected_members = _read_csv(root / "reports" / "city-best-local-ageb-subsets-2026-04-22" / "city_selected_ageb_members.csv")

    selected_codes_by_city: dict[str, list[str]] = {}
    for row in selected_members:
        selected_codes_by_city.setdefault(row["city_code"], []).append(row["unit_code"])

    feature_cache: dict[str, tuple[list[str], dict[str, dict[str, float]]]] = {}
    selected_metric_map = {row["city_code"]: row for row in selected_rows}
    random_metric_map: dict[tuple[str, int], dict[str, str]] = {
        (row["city_code"], int(row["sample_id"])): row for row in random_rows
    }

    subset_rows: list[dict[str, object]] = []
    for city in city_summary:
        city_code = city["city_code"]
        city_name = city["city_name"]
        city_state = city["state_code"]
        all_codes, fmap = _build_ageb_feature_map(city_code)
        feature_cache[city_code] = (all_codes, fmap)
        selected_codes = selected_codes_by_city[city_code]
        sel_metric = selected_metric_map[city_code]
        subset_rows.append(
            {
                "state_code": city_state,
                "city_code": city_code,
                "city_name": city_name,
                "sample_id": 0,
                "selected_flag": 1.0,
                **_subset_controls(selected_codes, fmap, _safe_float(sel_metric["subset_area_km2"])),
                "subset_area_km2": _safe_float(sel_metric["subset_area_km2"]),
                "boundary_entry_edges_per_km": _safe_float(sel_metric["boundary_entry_edges_per_km"]),
                "intersection_density_km2": _safe_float(sel_metric["intersection_density_km2"]),
                "street_density_km_per_km2": _safe_float(sel_metric["street_density_km_per_km2"]),
                "streets_per_node_avg": _safe_float(sel_metric["streets_per_node_avg"]),
                "mean_degree": _safe_float(sel_metric["mean_degree"]),
                "circuity_avg": _safe_float(sel_metric["circuity_avg"]),
            }
        )

        rng = random.Random(SEED + int(city_code))
        for sample_id in range(1, N_RANDOM + 1):
            sample_codes = rng.sample(all_codes, len(selected_codes))
            metric = random_metric_map[(city_code, sample_id)]
            subset_rows.append(
                {
                    "state_code": city_state,
                    "city_code": city_code,
                    "city_name": city_name,
                    "sample_id": sample_id,
                    "selected_flag": 0.0,
                    **_subset_controls(sample_codes, fmap, _safe_float(metric["subset_area_km2"])),
                    "subset_area_km2": _safe_float(metric["subset_area_km2"]),
                    "boundary_entry_edges_per_km": _safe_float(metric["boundary_entry_edges_per_km"]),
                    "intersection_density_km2": _safe_float(metric["intersection_density_km2"]),
                    "street_density_km_per_km2": _safe_float(metric["street_density_km_per_km2"]),
                    "streets_per_node_avg": _safe_float(metric["streets_per_node_avg"]),
                    "mean_degree": _safe_float(metric["mean_degree"]),
                    "circuity_avg": _safe_float(metric["circuity_avg"]),
                }
            )

    predictor_keys = [
        "log_area",
        "log_pop_density",
        "share_81",
        "share_46",
        "share_31",
        "share_62",
        "share_54",
        "share_micro",
        "share_medium",
        "boundary_entry_edges_per_km",
        "street_density_km_per_km2",
        "mean_degree",
    ]
    controls_only = predictor_keys[:9]
    connectivity_only = predictor_keys[9:]

    by_city: dict[str, list[dict[str, object]]] = {}
    for row in subset_rows:
        row["log_area"] = math.log(max(float(row["subset_area_km2"]), 1e-9))
        row["log_pop_density"] = math.log(max(float(row["subset_population_density"]), 1e-9))
        by_city.setdefault(str(row["city_code"]), []).append(row)

    # Standardize predictors within city to keep interpretation matched and comparable.
    for city_code, rows in by_city.items():
        for key in predictor_keys:
            vals = [float(r[key]) for r in rows]
            mu = _mean(vals)
            sd = _std(vals)
            for r in rows:
                r[key] = (float(r[key]) - mu) / sd if sd > 0 else 0.0

    pooled_rows = [row for rows in by_city.values() for row in rows]
    cities = sorted(by_city)

    fit0 = _fit_model(pooled_rows, [], cities)
    fit1 = _fit_model(pooled_rows, controls_only, cities)
    fit2 = _fit_model(pooled_rows, connectivity_only, cities)
    fit3 = _fit_model(pooled_rows, predictor_keys, cities)

    model_rows = [
        _model_row("M0 city_fe", fit0, [], cities),
        _model_row("M1 city_fe+controls", fit1, controls_only, cities),
        _model_row("M2 city_fe+connectivity", fit2, connectivity_only, cities),
        _model_row("M3 city_fe+controls+connectivity", fit3, predictor_keys, cities),
    ]
    _write_csv(outdir / "model_summary.csv", model_rows, list(model_rows[0].keys()))

    nested_rows = []
    for restricted_name, restricted_fit, full_name, full_fit in [
        ("M0 city_fe", fit0, "M1 city_fe+controls", fit1),
        ("M0 city_fe", fit0, "M2 city_fe+connectivity", fit2),
        ("M1 city_fe+controls", fit1, "M3 city_fe+controls+connectivity", fit3),
        ("M2 city_fe+connectivity", fit2, "M3 city_fe+controls+connectivity", fit3),
    ]:
        cmp = compare_nested_models(restricted_fit, full_fit)
        nested_rows.append(
            {
                "restricted": restricted_name,
                "full": full_name,
                "f_stat": cmp.f_stat,
                "df_num": cmp.df_num,
                "df_den": cmp.df_den,
                "p_value": cmp.p_value,
            }
        )
    _write_csv(outdir / "nested_tests.csv", nested_rows, list(nested_rows[0].keys()))

    coef_rows = _coefficient_rows("M3 city_fe+controls+connectivity", fit3, predictor_keys, cities)
    _write_csv(outdir / "full_model_coefficients.csv", coef_rows, list(coef_rows[0].keys()))

    corr_rows = []
    for a in predictor_keys:
        for b in predictor_keys:
            if a >= b:
                continue
            corr_rows.append({"var_a": a, "var_b": b, "corr": pearson_corr([float(r[a]) for r in pooled_rows], [float(r[b]) for r in pooled_rows])})
    _write_csv(outdir / "predictor_correlations.csv", corr_rows, list(corr_rows[0].keys()))

    _write_csv(outdir / "subset_feature_dataset.csv", pooled_rows, list(pooled_rows[0].keys()))

    fig1 = _write_model_chart(figdir / "model_adj_r2.svg", "Selected subset indicator models", model_rows)
    fig2 = _write_coef_chart(
        figdir / "full_model_coefficients.svg",
        "Connectivity and control coefficients in the full model",
        coef_rows,
    )
    _write_csv(
        figdir / "figures_manifest.csv",
        [
            {"figure_id": "model_adj_r2", "path": str(fig1.resolve()), "description": "Adjusted R2 across nested selected-vs-random models."},
            {"figure_id": "full_model_coefficients", "path": str(fig2.resolve()), "description": "Coefficients of the full pooled model."},
        ],
        ["figure_id", "path", "description"],
    )

    lines = [
        "# Connectivity With Density, Area, And Composition Controls",
        "",
        "Pooled selected-vs-random comparison across 20 cities. Response is `selected_flag` with city fixed effects.",
        "",
        "## Files",
        f"- [subset_feature_dataset.csv]({(outdir / 'subset_feature_dataset.csv').resolve()})",
        f"- [model_summary.csv]({(outdir / 'model_summary.csv').resolve()})",
        f"- [nested_tests.csv]({(outdir / 'nested_tests.csv').resolve()})",
        f"- [full_model_coefficients.csv]({(outdir / 'full_model_coefficients.csv').resolve()})",
        f"- [predictor_correlations.csv]({(outdir / 'predictor_correlations.csv').resolve()})",
        "",
        "## Figures",
        f"- [model_adj_r2.svg]({fig1.resolve()})",
        f"- [full_model_coefficients.svg]({fig2.resolve()})",
    ]
    (outdir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

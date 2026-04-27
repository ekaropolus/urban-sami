#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
import random
import shutil
import subprocess
from pathlib import Path

from urban_sami.analysis.linear_models import compare_nested_models, ols_fit
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
AXIS = "#8b8478"
TEXT = "#1f1f1f"
MUTED = "#625d54"
TEAL = "#0f766e"
RUST = "#b14d3b"
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


def _subset_features(unit_codes: list[str], fmap: dict[str, dict[str, float]], metric_row: dict[str, str]) -> dict[str, float]:
    pop = sum(fmap[c]["population"] for c in unit_codes)
    total_est = sum(fmap[c]["est_total"] for c in unit_codes)
    area_km2 = _safe_float(metric_row["subset_area_km2"])
    total_est_safe = max(total_est, 1e-9)
    return {
        "subset_population": pop,
        "subset_est_total": total_est,
        "subset_area_km2": area_km2,
        "subset_pop_density": pop / max(area_km2, 1e-9),
        "share_81": sum(fmap[c]["scian2_81"] for c in unit_codes) / total_est_safe,
        "share_46": sum(fmap[c]["scian2_46"] for c in unit_codes) / total_est_safe,
        "share_31": sum(fmap[c]["scian2_31"] for c in unit_codes) / total_est_safe,
        "share_62": sum(fmap[c]["scian2_62"] for c in unit_codes) / total_est_safe,
        "share_54": sum(fmap[c]["scian2_54"] for c in unit_codes) / total_est_safe,
        "share_micro": sum(fmap[c]["size_micro"] for c in unit_codes) / total_est_safe,
        "share_medium": sum(fmap[c]["size_medium"] for c in unit_codes) / total_est_safe,
        "boundary_entry_edges_per_km": _safe_float(metric_row["boundary_entry_edges_per_km"]),
        "mean_degree": _safe_float(metric_row["mean_degree"]),
        "street_density_km_per_km2": _safe_float(metric_row["street_density_km_per_km2"]),
    }


def _add_city_fe(design: list[list[float]], rows: list[dict[str, float]], cities: list[str]) -> list[list[float]]:
    out = []
    for vec, row in zip(design, rows):
        ext = vec[:]
        for code in cities[1:]:
            ext.append(1.0 if row["city_code"] == code else 0.0)
        out.append(ext)
    return out


def _fit(rows: list[dict[str, float]], predictor_keys: list[str], *, city_fe: bool):
    y = [math.log(max(float(r["subset_est_total"]), 1e-9)) for r in rows]
    design = [[1.0] + [float(r[k]) for k in predictor_keys] for r in rows]
    cities = []
    if city_fe:
        cities = sorted({str(r["city_code"]) for r in rows})
        design = _add_city_fe(design, rows, cities)
    fit = ols_fit(design, y)
    return fit, cities


def _coef_rows(model_name: str, fit, predictor_keys: list[str], cities: list[str]) -> list[dict[str, object]]:
    names = ["intercept"] + predictor_keys + [f"city_fe::{c}" for c in cities[1:]]
    out = []
    for name, coef, se in zip(names, fit.coefficients, fit.stderr):
        if name == "intercept" or name.startswith("city_fe::"):
            continue
        out.append(
            {
                "model": model_name,
                "term": name,
                "coefficient": coef,
                "stderr": se,
                "t_approx": (coef / se) if se > 0 else 0.0,
            }
        )
    return out


def _model_rows(name: str, fit, predictor_keys: list[str], cities: list[str]) -> dict[str, object]:
    return {
        "model": name,
        "n_obs": fit.n_obs,
        "n_params": fit.n_params,
        "r2": fit.r2,
        "adj_r2": fit.adj_r2,
        "rss": fit.rss,
        "predictors": ",".join(predictor_keys),
        "city_fe_n": max(0, len(cities) - 1),
    }


def _write_bar_chart(path: Path, title: str, rows: list[dict[str, object]], metric: str) -> Path:
    width = 1080
    left = 220
    right = 70
    top = 96
    bottom = 78
    row_h = 30
    height = top + len(rows) * row_h + bottom
    vals = [float(r[metric]) for r in rows]
    xmax = max(vals) * 1.1 if vals else 1.0

    def px(v: float) -> float:
        return left + (v / max(xmax, 1e-9)) * (width - left - right)

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Higher adjusted R² means better explanatory power for subset establishment totals.</text>',
    ]
    for i, row in enumerate(rows):
        y = top + i * row_h
        v = float(row[metric])
        body.append(f'<text x="{left-12}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{row["model"]}</text>')
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{px(v):.2f}" y2="{y:.2f}" stroke="{TEAL}" stroke-width="8"/>')
        body.append(f'<text x="{px(v)+8:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">{v:.3f}</text>')
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
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Coefficients from the selected-subset cross-city extended law.</text>',
        f'<line x1="{x0:.2f}" y1="{top-16}" x2="{x0:.2f}" y2="{height-bottom+8}" stroke="{AXIS}" stroke-width="1.5"/>',
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
    outdir = root / "reports" / "city-subset-extended-power-law-2026-04-23"
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    city_summary = _read_csv(root / "reports" / "city-best-local-ageb-subsets-2026-04-22" / "city_best_subset_summary.csv")
    selected_members = _read_csv(root / "reports" / "city-best-local-ageb-subsets-2026-04-22" / "city_selected_ageb_members.csv")
    selected_metric_rows = _read_csv(root / "reports" / "city-selected-subset-random-connectivity-baseline-2026-04-23" / "city_selected_subset_metrics.csv")
    random_metric_rows = _read_csv(root / "reports" / "city-selected-subset-random-connectivity-baseline-2026-04-23" / "city_random_subset_metrics.csv")

    selected_codes_by_city: dict[str, list[str]] = {}
    for row in selected_members:
        selected_codes_by_city.setdefault(row["city_code"], []).append(row["unit_code"])
    selected_metric_map = {row["city_code"]: row for row in selected_metric_rows}
    random_metric_map = {(row["city_code"], int(row["sample_id"])): row for row in random_metric_rows}

    all_subset_rows: list[dict[str, object]] = []
    selected_only_rows: list[dict[str, object]] = []
    for city in city_summary:
        city_code = city["city_code"]
        city_name = city["city_name"]
        state_code = city["state_code"]
        all_codes, fmap = _build_ageb_feature_map(city_code)
        selected_codes = selected_codes_by_city[city_code]
        selected_feat = _subset_features(selected_codes, fmap, selected_metric_map[city_code])
        selected_row = {
            "state_code": state_code,
            "city_code": city_code,
            "city_name": city_name,
            "sample_id": 0,
            "selected_flag": 1.0,
            **selected_feat,
        }
        all_subset_rows.append(selected_row)
        selected_only_rows.append(selected_row)

        rng = random.Random(SEED + int(city_code))
        for sample_id in range(1, N_RANDOM + 1):
            sample_codes = rng.sample(all_codes, len(selected_codes))
            feat = _subset_features(sample_codes, fmap, random_metric_map[(city_code, sample_id)])
            all_subset_rows.append(
                {
                    "state_code": state_code,
                    "city_code": city_code,
                    "city_name": city_name,
                    "sample_id": sample_id,
                    "selected_flag": 0.0,
                    **feat,
                }
            )

    # Prepare log terms for multiplicative law.
    for row in all_subset_rows:
        row["log_N"] = math.log(max(float(row["subset_population"]), 1e-9))
        row["log_density"] = math.log(max(float(row["subset_pop_density"]), 1e-9))
        row["log_boundary"] = math.log(max(float(row["boundary_entry_edges_per_km"]), 1e-9))
        row["log_degree"] = math.log(max(float(row["mean_degree"]), 1e-9))
    for row in selected_only_rows:
        row["log_N"] = math.log(max(float(row["subset_population"]), 1e-9))
        row["log_density"] = math.log(max(float(row["subset_pop_density"]), 1e-9))
        row["log_boundary"] = math.log(max(float(row["boundary_entry_edges_per_km"]), 1e-9))
        row["log_degree"] = math.log(max(float(row["mean_degree"]), 1e-9))

    pooled_predictor_sets = {
        "P0 city_fe + logN": ["log_N"],
        "P1 + density": ["log_N", "log_density"],
        "P2 + connectivity": ["log_N", "log_boundary", "log_degree"],
        "P3 + density + connectivity": ["log_N", "log_density", "log_boundary", "log_degree"],
        "P4 + density + connectivity + composition": ["log_N", "log_density", "log_boundary", "log_degree", "share_31", "share_46", "share_81", "share_62", "share_54", "share_micro", "share_medium"],
    }

    pooled_models = []
    pooled_fits = {}
    for name, predictors in pooled_predictor_sets.items():
        fit, cities = _fit(all_subset_rows, predictors, city_fe=True)
        pooled_models.append(_model_rows(name, fit, predictors, cities))
        pooled_fits[name] = (fit, predictors, cities)
    _write_csv(outdir / "pooled_model_summary.csv", pooled_models, list(pooled_models[0].keys()))

    pooled_nested = []
    for a, b in [
        ("P0 city_fe + logN", "P1 + density"),
        ("P0 city_fe + logN", "P2 + connectivity"),
        ("P1 + density", "P3 + density + connectivity"),
        ("P3 + density + connectivity", "P4 + density + connectivity + composition"),
    ]:
        cmp = compare_nested_models(pooled_fits[a][0], pooled_fits[b][0])
        pooled_nested.append({"restricted": a, "full": b, "f_stat": cmp.f_stat, "df_num": cmp.df_num, "df_den": cmp.df_den, "p_value": cmp.p_value})
    _write_csv(outdir / "pooled_nested_tests.csv", pooled_nested, list(pooled_nested[0].keys()))

    pooled_coefs = _coef_rows("P4 + density + connectivity + composition", pooled_fits["P4 + density + connectivity + composition"][0], pooled_predictor_sets["P4 + density + connectivity + composition"], pooled_fits["P4 + density + connectivity + composition"][2])
    _write_csv(outdir / "pooled_full_coefficients.csv", pooled_coefs, list(pooled_coefs[0].keys()))

    # Selected-only cross-city law.
    selected_predictor_sets = {
        "S0 logN": ["log_N"],
        "S1 logN + density": ["log_N", "log_density"],
        "S2 logN + connectivity": ["log_N", "log_boundary", "log_degree"],
        "S3 logN + density + connectivity": ["log_N", "log_density", "log_boundary", "log_degree"],
    }
    selected_models = []
    selected_fits = {}
    for name, predictors in selected_predictor_sets.items():
        fit, cities = _fit(selected_only_rows, predictors, city_fe=False)
        selected_models.append(_model_rows(name, fit, predictors, cities))
        selected_fits[name] = (fit, predictors, cities)
    _write_csv(outdir / "selected_only_model_summary.csv", selected_models, list(selected_models[0].keys()))

    selected_nested = []
    for a, b in [
        ("S0 logN", "S1 logN + density"),
        ("S0 logN", "S2 logN + connectivity"),
        ("S1 logN + density", "S3 logN + density + connectivity"),
    ]:
        cmp = compare_nested_models(selected_fits[a][0], selected_fits[b][0])
        selected_nested.append({"restricted": a, "full": b, "f_stat": cmp.f_stat, "df_num": cmp.df_num, "df_den": cmp.df_den, "p_value": cmp.p_value})
    _write_csv(outdir / "selected_only_nested_tests.csv", selected_nested, list(selected_nested[0].keys()))

    selected_coefs = _coef_rows("S3 logN + density + connectivity", selected_fits["S3 logN + density + connectivity"][0], selected_predictor_sets["S3 logN + density + connectivity"], [])
    _write_csv(outdir / "selected_only_coefficients.csv", selected_coefs, list(selected_coefs[0].keys()))

    # Candidate equation terms from selected-only model.
    coef_map = {row["term"]: float(row["coefficient"]) for row in selected_coefs}
    equation_rows = [
        {"equation": "extended_subset_law", "form": "log(Y_sel) = alpha + beta log(N_sel) + delta log(rho_sel) + lambda log(B_sel) + mu log(K_sel)", "beta": coef_map.get("log_N", 0.0), "delta": coef_map.get("log_density", 0.0), "lambda": coef_map.get("log_boundary", 0.0), "mu": coef_map.get("log_degree", 0.0)}
    ]
    _write_csv(outdir / "candidate_equation.csv", equation_rows, list(equation_rows[0].keys()))

    _write_csv(outdir / "all_subset_feature_rows.csv", all_subset_rows, list(all_subset_rows[0].keys()))
    _write_csv(outdir / "selected_only_feature_rows.csv", selected_only_rows, list(selected_only_rows[0].keys()))

    fig1 = _write_bar_chart(figdir / "pooled_model_adj_r2.svg", "Pooled subset models across all cities", pooled_models, "adj_r2")
    fig2 = _write_bar_chart(figdir / "selected_only_model_adj_r2.svg", "Selected-subset cross-city law", selected_models, "adj_r2")
    fig3 = _write_coef_chart(figdir / "selected_only_coefficients.svg", "Coefficients of the selected-subset extended power law", selected_coefs)
    _write_csv(
        figdir / "figures_manifest.csv",
        [
            {"figure_id": "pooled_model_adj_r2", "path": str(fig1.resolve()), "description": "Adjusted R2 across pooled subset models."},
            {"figure_id": "selected_only_model_adj_r2", "path": str(fig2.resolve()), "description": "Adjusted R2 across selected-only cross-city models."},
            {"figure_id": "selected_only_coefficients", "path": str(fig3.resolve()), "description": "Selected-only coefficient plot."},
        ],
        ["figure_id", "path", "description"],
    )

    lines = [
        "# Extended Power Law For City-Selected Subsets",
        "",
        "This pack uses all 20 city-selected subsets and their within-city random baselines to infer which terms should extend the simple subset scaling law.",
        "",
        "## Files",
        f"- [pooled_model_summary.csv]({(outdir / 'pooled_model_summary.csv').resolve()})",
        f"- [pooled_nested_tests.csv]({(outdir / 'pooled_nested_tests.csv').resolve()})",
        f"- [pooled_full_coefficients.csv]({(outdir / 'pooled_full_coefficients.csv').resolve()})",
        f"- [selected_only_model_summary.csv]({(outdir / 'selected_only_model_summary.csv').resolve()})",
        f"- [selected_only_nested_tests.csv]({(outdir / 'selected_only_nested_tests.csv').resolve()})",
        f"- [selected_only_coefficients.csv]({(outdir / 'selected_only_coefficients.csv').resolve()})",
        f"- [candidate_equation.csv]({(outdir / 'candidate_equation.csv').resolve()})",
        "",
        "## Figures",
        f"- [pooled_model_adj_r2.svg]({fig1.resolve()})",
        f"- [selected_only_model_adj_r2.svg]({fig2.resolve()})",
        f"- [selected_only_coefficients.svg]({fig3.resolve()})",
    ]
    (outdir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

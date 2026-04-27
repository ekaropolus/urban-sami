#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import shutil
import subprocess
from pathlib import Path

from pyproj import CRS, Transformer
from shapely.geometry import shape
from shapely.ops import transform
from urban_sami.analysis.linear_models import compare_nested_models, ols_fit
from urban_sami.indicators.denue import sector_prefix, size_class_from_per_ocu


DOCKER_EXE = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DB_CONTAINER = "24-polisplexity-core-db-dev"
POSTGRES_USER = "postgres"
DB_NAME = "urban_sami_exp"

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


def _svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _city_area_map(root: Path) -> dict[str, float]:
    out: dict[str, float] = {}
    raw_dir = root / "data" / "raw" / "inegi_municipal_geojson"
    for path in sorted(raw_dir.glob("*.geojson")):
        obj = json.loads(path.read_text(encoding="utf-8"))
        for feat in obj.get("features", []):
            props = feat["properties"]
            code = str(props.get("cvegeo", "")).strip()
            geom = feat["geometry"]
            if not code or not geom:
                continue
            g = shape(geom)
            lon, lat = g.centroid.x, g.centroid.y
            zone = int((lon + 180.0) // 6.0) + 1
            epsg = 32600 + zone if lat >= 0 else 32700 + zone
            transformer = Transformer.from_crs(CRS.from_epsg(4326), CRS.from_epsg(epsg), always_xy=True)
            g_proj = transform(transformer.transform, g)
            out[code] = g_proj.area / 1_000_000.0
    return out


def _city_denue_rows() -> list[dict[str, str]]:
    return _query_tsv(
        """
        SELECT city_code, scian_code, per_ocu
        FROM raw.denue_establishments
        WHERE city_code <> ''
        """.strip(),
        ["city_code", "scian_code", "per_ocu"],
    )


def _ageb_denue_rows() -> list[dict[str, str]]:
    return _query_tsv(
        """
        SELECT city_code, ageb_code, scian_code, per_ocu
        FROM raw.denue_establishments
        WHERE city_code <> ''
          AND ageb_code <> ''
        """.strip(),
        ["city_code", "ageb_code", "scian_code", "per_ocu"],
    )


def _ageb_units() -> list[dict[str, str]]:
    return _query_tsv(
        """
        SELECT
            city_code,
            RIGHT(unit_code, 4) AS ageb_code,
            COALESCE(population,0)::text AS population,
            COALESCE(area_km2,0)::text AS area_km2
        FROM raw.admin_units
        WHERE level='ageb_u'
          AND COALESCE(population,0) > 0
        ORDER BY city_code, unit_code
        """.strip(),
        ["city_code", "ageb_code", "population", "area_km2"],
    )


def _build_city_rows(root: Path) -> list[dict[str, float]]:
    city_counts = _read_csv(root / "dist" / "independent_city_baseline" / "city_counts.csv")
    area_map = _city_area_map(root)
    denue_rows = _city_denue_rows()

    counts: dict[str, dict[str, float]] = {}
    for row in city_counts:
        code = str(row["city_code"]).zfill(5)
        pop = _safe_float(row["population"])
        est = _safe_float(row["est_count"])
        area = area_map.get(code, 0.0)
        if pop <= 0 or est <= 0 or area <= 0:
            continue
        counts[code] = {
            "city_code": code,
            "city_name": row["city_name"],
            "state_code": str(row["state_code"]).zfill(2),
            "population": pop,
            "est_total": est,
            "area_km2": area,
            "share_81": 0.0,
            "share_46": 0.0,
            "share_31": 0.0,
            "share_62": 0.0,
            "share_54": 0.0,
            "share_micro": 0.0,
            "share_medium": 0.0,
        }

    total_by_city = {k: v["est_total"] for k, v in counts.items()}
    for row in denue_rows:
        code = str(row["city_code"]).zfill(5)
        if code not in counts:
            continue
        sc2 = sector_prefix(row["scian_code"])
        size = size_class_from_per_ocu(row["per_ocu"])
        denom = max(total_by_city[code], 1e-9)
        if sc2 in KEEP_SCIAN2:
            counts[code][f"share_{sc2}"] += 1.0 / denom
        if size in KEEP_SIZE:
            counts[code][f"share_{size}"] += 1.0 / denom

    rows = []
    for row in counts.values():
        row["pop_density"] = row["population"] / max(row["area_km2"], 1e-9)
        row["log_N"] = math.log(max(row["population"], 1e-9))
        row["log_density"] = math.log(max(row["pop_density"], 1e-9))
        rows.append(row)
    rows.sort(key=lambda r: r["city_code"])
    return rows


def _build_ageb_rows() -> list[dict[str, float]]:
    units = _ageb_units()
    denue_rows = _ageb_denue_rows()
    fmap: dict[tuple[str, str], dict[str, float]] = {}
    for row in units:
        city = str(row["city_code"]).zfill(5)
        ageb = row["ageb_code"]
        pop = _safe_float(row["population"])
        area = _safe_float(row["area_km2"])
        fmap[(city, ageb)] = {
            "city_code": city,
            "ageb_code": ageb,
            "population": pop,
            "area_km2": area,
            "est_total": 0.0,
            "share_81": 0.0,
            "share_46": 0.0,
            "share_31": 0.0,
            "share_62": 0.0,
            "share_54": 0.0,
            "share_micro": 0.0,
            "share_medium": 0.0,
        }
    totals: dict[tuple[str, str], float] = {k: 0.0 for k in fmap}
    temp_counts: dict[tuple[str, str], dict[str, float]] = {k: {f"share_{c}": 0.0 for c in KEEP_SCIAN2} | {"share_micro": 0.0, "share_medium": 0.0} for k in fmap}

    for row in denue_rows:
        city = str(row["city_code"]).zfill(5)
        ageb = row["ageb_code"]
        key = (city, ageb)
        if key not in fmap:
            continue
        totals[key] += 1.0
        sc2 = sector_prefix(row["scian_code"])
        size = size_class_from_per_ocu(row["per_ocu"])
        if sc2 in KEEP_SCIAN2:
            temp_counts[key][f"share_{sc2}"] += 1.0
        if size in KEEP_SIZE:
            temp_counts[key][f"share_{size}"] += 1.0

    out = []
    for key, row in fmap.items():
        total = totals[key]
        if total <= 0:
            continue
        row["est_total"] = total
        row["pop_density"] = row["population"] / max(row["area_km2"], 1e-9)
        row["log_N"] = math.log(max(row["population"], 1e-9))
        row["log_density"] = math.log(max(row["pop_density"], 1e-9))
        for name, count in temp_counts[key].items():
            row[name] = count / total
        out.append(row)
    out.sort(key=lambda r: (r["city_code"], r["ageb_code"]))
    return out


def _fit(rows: list[dict[str, float]], predictor_keys: list[str], *, city_fe: bool):
    y = [math.log(max(float(r["est_total"]), 1e-9)) for r in rows]
    X = [[1.0] + [float(r[k]) for k in predictor_keys] for r in rows]
    cities: list[str] = []
    if city_fe:
        cities = sorted({str(r["city_code"]) for r in rows})
        base = cities[0]
        X2 = []
        for vec, row in zip(X, rows):
            ext = vec[:]
            for code in cities[1:]:
                ext.append(1.0 if row["city_code"] == code else 0.0)
            X2.append(ext)
        X = X2
    fit = ols_fit(X, y)
    return fit, cities


def _model_row(name: str, fit, predictors: list[str], cities: list[str]) -> dict[str, object]:
    return {
        "model": name,
        "n_obs": fit.n_obs,
        "n_params": fit.n_params,
        "r2": fit.r2,
        "adj_r2": fit.adj_r2,
        "rss": fit.rss,
        "predictors": ",".join(predictors),
        "city_fe_n": max(0, len(cities) - 1),
    }


def _coef_rows(model_name: str, fit, predictors: list[str], cities: list[str]) -> list[dict[str, object]]:
    coef_names = ["intercept"] + predictors + [f"city_fe::{c}" for c in cities[1:]]
    out = []
    for name, coef, se in zip(coef_names, fit.coefficients, fit.stderr):
        if name == "intercept" or name.startswith("city_fe::"):
            continue
        out.append({"model": model_name, "term": name, "coefficient": coef, "stderr": se, "t_approx": (coef / se) if se > 0 else 0.0})
    return out


def _write_bar_chart(path: Path, title: str, rows: list[dict[str, object]]) -> Path:
    width = 1080
    left = 230
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
    ]
    for i, row in enumerate(rows):
        y = top + i * row_h
        v = float(row["adj_r2"])
        body.append(f'<text x="{left-12}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{row["model"]}</text>')
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{px(v):.2f}" y2="{y:.2f}" stroke="{TEAL}" stroke-width="8"/>')
        body.append(f'<text x="{px(v)+8:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">{v:.3f}</text>')
    return _svg(path, width, height, "".join(body))


def _run_scale(root: Path, outdir: Path, rows: list[dict[str, float]], *, city_fe: bool, label: str) -> None:
    predictor_sets = {
        "M0 logN": ["log_N"],
        "M1 logN + density": ["log_N", "log_density"],
        "M2 logN + composition": ["log_N", "share_31", "share_46", "share_81", "share_62", "share_54", "share_micro", "share_medium"],
        "M3 logN + density + composition": ["log_N", "log_density", "share_31", "share_46", "share_81", "share_62", "share_54", "share_micro", "share_medium"],
    }
    model_rows = []
    fits = {}
    for name, predictors in predictor_sets.items():
        fit, cities = _fit(rows, predictors, city_fe=city_fe)
        model_rows.append(_model_row(name, fit, predictors, cities))
        fits[name] = (fit, predictors, cities)
    _write_csv(outdir / f"{label}_model_summary.csv", model_rows, list(model_rows[0].keys()))

    nested_rows = []
    for a, b in [("M0 logN", "M1 logN + density"), ("M0 logN", "M2 logN + composition"), ("M1 logN + density", "M3 logN + density + composition")]:
        cmp = compare_nested_models(fits[a][0], fits[b][0])
        nested_rows.append({"restricted": a, "full": b, "f_stat": cmp.f_stat, "df_num": cmp.df_num, "df_den": cmp.df_den, "p_value": cmp.p_value})
    _write_csv(outdir / f"{label}_nested_tests.csv", nested_rows, list(nested_rows[0].keys()))

    coef_rows = _coef_rows("M3 logN + density + composition", fits["M3 logN + density + composition"][0], predictor_sets["M3 logN + density + composition"], fits["M3 logN + density + composition"][2])
    _write_csv(outdir / f"{label}_full_coefficients.csv", coef_rows, list(coef_rows[0].keys()))
    _write_csv(outdir / f"{label}_rows.csv", rows, list(rows[0].keys()))

    if label == "city":
        eq_rows = [{
            "equation": "city_extended_law",
            "form": "log(Y_city) = alpha + beta log(N_city) + delta log(rho_city) + composition",
            "beta": next((float(r["coefficient"]) for r in coef_rows if r["term"] == "log_N"), 0.0),
            "delta": next((float(r["coefficient"]) for r in coef_rows if r["term"] == "log_density"), 0.0),
        }]
        _write_csv(outdir / "city_candidate_equation.csv", eq_rows, list(eq_rows[0].keys()))
    else:
        eq_rows = [{
            "equation": "ageb_extended_law",
            "form": "log(Y_ageb) = alpha_city + beta log(N_ageb) + delta log(rho_ageb) + composition",
            "beta": next((float(r["coefficient"]) for r in coef_rows if r["term"] == "log_N"), 0.0),
            "delta": next((float(r["coefficient"]) for r in coef_rows if r["term"] == "log_density"), 0.0),
        }]
        _write_csv(outdir / "ageb_candidate_equation.csv", eq_rows, list(eq_rows[0].keys()))

    fig = _write_bar_chart(outdir / "figures" / f"{label}_model_adj_r2.svg", f"{label.upper()} extended-law model comparison", model_rows)
    _write_csv(outdir / "figures" / f"{label}_figures_manifest.csv", [{"figure_id": f"{label}_model_adj_r2", "path": str(fig.resolve()), "description": f"{label} model comparison"}], ["figure_id", "path", "description"])


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "multiscale-extended-power-law-2026-04-23"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "figures").mkdir(parents=True, exist_ok=True)

    city_rows = _build_city_rows(root)
    ageb_rows = _build_ageb_rows()

    _run_scale(root, outdir, city_rows, city_fe=False, label="city")
    _run_scale(root, outdir, ageb_rows, city_fe=True, label="ageb")

    lines = [
        "# Multiscale Extended Power Law",
        "",
        "This pack applies the same extension logic at two scales:",
        "- city: all available cities with density and composition terms",
        "- ageb: all loaded AGEB with city fixed effects, density and composition terms",
        "",
        "## Files",
        f"- [city_model_summary.csv]({(outdir / 'city_model_summary.csv').resolve()})",
        f"- [city_nested_tests.csv]({(outdir / 'city_nested_tests.csv').resolve()})",
        f"- [city_full_coefficients.csv]({(outdir / 'city_full_coefficients.csv').resolve()})",
        f"- [city_candidate_equation.csv]({(outdir / 'city_candidate_equation.csv').resolve()})",
        f"- [ageb_model_summary.csv]({(outdir / 'ageb_model_summary.csv').resolve()})",
        f"- [ageb_nested_tests.csv]({(outdir / 'ageb_nested_tests.csv').resolve()})",
        f"- [ageb_full_coefficients.csv]({(outdir / 'ageb_full_coefficients.csv').resolve()})",
        f"- [ageb_candidate_equation.csv]({(outdir / 'ageb_candidate_equation.csv').resolve()})",
        "",
        "## Figures",
        f"- [city_model_adj_r2.svg]({(outdir / 'figures' / 'city_model_adj_r2.svg').resolve()})",
        f"- [ageb_model_adj_r2.svg]({(outdir / 'figures' / 'ageb_model_adj_r2.svg').resolve()})",
    ]
    (outdir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

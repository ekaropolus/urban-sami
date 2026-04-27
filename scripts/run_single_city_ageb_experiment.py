#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import html
import json
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
TMP_POINT_TABLE = "staging.tmp_denue_one_city_points"

BG = "#f8f6f1"
PANEL = "#fffdf8"
GRID = "#ddd6c8"
TEXT = "#1f1f1f"
MUTED = "#625d54"
NEUTRAL = "#e9e3d8"

Y_DEFS = [
    ("all", "All establishments", "COUNT(*) FILTER (WHERE d.geom IS NOT NULL)::double precision"),
    ("micro", "Micro establishments", "COUNT(*) FILTER (WHERE d.per_ocu = '0 a 5 personas')::double precision"),
    ("scian2_46", "Retail trade (SCIAN 46)", "COUNT(*) FILTER (WHERE LEFT(d.scian_code, 2) = '46')::double precision"),
    ("scian2_54", "Professional services (SCIAN 54)", "COUNT(*) FILTER (WHERE LEFT(d.scian_code, 2) = '54')::double precision"),
]


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


def _query(sql: str) -> list[list[str]]:
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
    rows: list[list[str]] = []
    for line in proc.stdout.splitlines():
        if not line.strip():
            continue
        rows.append(line.split("\t"))
    return rows


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _fmt_num(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}"


def _collect_bounds(features: list[dict]) -> tuple[float, float, float, float]:
    min_x = min_y = float("inf")
    max_x = max_y = float("-inf")
    for feature in features:
        geom = feature.get("geometry", {})
        for ring in _flatten_coords(geom):
            for x, y in ring:
                min_x = min(min_x, x)
                max_x = max(max_x, x)
                min_y = min(min_y, y)
                max_y = max(max_y, y)
    return min_x, min_y, max_x, max_y


def _flatten_coords(geometry: dict) -> list[list[tuple[float, float]]]:
    geom_type = geometry.get("type")
    coords = geometry.get("coordinates", [])
    rings: list[list[tuple[float, float]]] = []
    if geom_type == "Polygon":
        for ring in coords:
            rings.append([(float(x), float(y)) for x, y in ring])
    elif geom_type == "MultiPolygon":
        for polygon in coords:
            for ring in polygon:
                rings.append([(float(x), float(y)) for x, y in ring])
    return rings


def _projector(bounds: tuple[float, float, float, float], width: int, height: int, pad: int):
    min_x, min_y, max_x, max_y = bounds
    span_x = max_x - min_x
    span_y = max_y - min_y
    usable_w = width - 2 * pad
    usable_h = height - 2 * pad
    scale = min(usable_w / span_x, usable_h / span_y)
    x_offset = pad + (usable_w - span_x * scale) / 2
    y_offset = pad + (usable_h - span_y * scale) / 2

    def project(x: float, y: float) -> tuple[float, float]:
        px = x_offset + (x - min_x) * scale
        py = pad + usable_h - ((y - min_y) * scale) - (y_offset - pad)
        return px, py

    return project


def _svg_path_from_geometry(geometry: dict, project) -> str:
    parts: list[str] = []
    for ring in _flatten_coords(geometry):
        if len(ring) < 2:
            continue
        first = True
        for x, y in ring:
            px, py = project(x, y)
            if first:
                parts.append(f"M {px:.1f} {py:.1f}")
                first = False
            else:
                parts.append(f"L {px:.1f} {py:.1f}")
        parts.append("Z")
    return " ".join(parts)


def _diverging_fill(value: float | None) -> str:
    if value is None:
        return "#f1ece2"
    clipped = max(-2.5, min(2.5, value))
    if abs(clipped) < 0.08:
        return NEUTRAL
    if clipped > 0:
        frac = clipped / 2.5
        r = 177
        g = 221 - int(frac * 90)
        b = 206 - int(frac * 95)
        return f"rgb({r},{g},{b})"
    frac = abs(clipped) / 2.5
    r = 217 - int(frac * 95)
    g = 229 - int(frac * 70)
    b = 242
    return f"rgb({r},{g},{b})"


def _write_svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _write_ageb_map(city_name: str, y_label: str, features: list[dict], value_lookup: dict[str, float], path: Path) -> Path:
    width = 1180
    height = 900
    map_x = 48
    map_y = 110
    map_w = 860
    map_h = 740
    bounds = _collect_bounds(features)
    project = _projector(bounds, map_w, map_h, 16)

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="28" font-family="Georgia, \'Times New Roman\', serif" fill="{TEXT}">{html.escape(city_name)} AGEBs: {html.escape(y_label)}</text>',
        f'<text x="44" y="76" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">AGEB-level SAMI within one city. Colors show deviations from the within-city AGEB scaling law.</text>',
        f'<g transform="translate({map_x},{map_y})">',
        f'<rect x="0" y="0" width="{map_w}" height="{map_h}" fill="#faf8f2" stroke="{GRID}"/>',
    ]
    vals = []
    for feature in features:
        unit_code = str((feature.get("properties") or {}).get("unit_code", "")).strip()
        value = value_lookup.get(unit_code)
        vals.append(value)
        fill = _diverging_fill(value)
        path_data = _svg_path_from_geometry(feature.get("geometry", {}), project)
        body.append(f'<path d="{path_data}" fill="{fill}" stroke="#ffffff" stroke-width="0.35"/>')
    body.append("</g>")
    legend_x = 940
    legend_y = 190
    body.append(f'<text x="{legend_x}" y="{legend_y-28}" font-size="13" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">AGEB SAMI</text>')
    for idx, val in enumerate([-2.0, -1.0, 0.0, 1.0, 2.0]):
        y = legend_y + idx * 28
        label = "near expected" if val == 0 else _fmt_num(val, 1)
        body.append(f'<rect x="{legend_x}" y="{y}" width="38" height="18" rx="3" fill="{_diverging_fill(val)}" stroke="#ffffff"/>')
        body.append(f'<text x="{legend_x+52}" y="{y+13}" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{label}</text>')
    valid = [v for v in vals if v is not None]
    body.append(f'<text x="{legend_x}" y="{legend_y+170}" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">n AGEB = {len(features)}</text>')
    if valid:
        body.append(f'<text x="{legend_x}" y="{legend_y+194}" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">min = {_fmt_num(min(valid),2)} | max = {_fmt_num(max(valid),2)}</text>')
    return _write_svg(path, width, height, "".join(body))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one-city AGEB experiment for multiple Y definitions")
    parser.add_argument("--city-code", default="14039")
    parser.add_argument("--output-dir", type=Path, default=Path("reports/ageb-one-city-guadalajara-2026-04-22"))
    args = parser.parse_args()

    city_code = str(args.city_code).strip()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "figures").mkdir(parents=True, exist_ok=True)

    _exec(f"DROP TABLE IF EXISTS {TMP_POINT_TABLE};")
    _exec(
        f"""
        CREATE TABLE {TMP_POINT_TABLE} AS
        SELECT city_code, per_ocu, scian_code,
               ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geom
        FROM raw.denue_establishments
        WHERE city_code = '{city_code}'
          AND longitude IS NOT NULL
          AND latitude IS NOT NULL
        """.strip()
    )
    _exec(f"CREATE INDEX tmp_denue_one_city_points_geom_gix ON {TMP_POINT_TABLE} USING GIST (geom);")
    _exec(f"ANALYZE {TMP_POINT_TABLE};")

    y_selects = ",\n               ".join([f"{sql} AS {key}" for key, _, sql in Y_DEFS])
    rows_raw = _query(
        f"""
        WITH ageb AS (
            SELECT unit_code, unit_label, city_code, city_name, population, households, ST_AsGeoJSON(geom) AS geom_json, geom
            FROM raw.admin_units
            WHERE level = 'ageb_u' AND city_code = '{city_code}'
        )
        SELECT a.unit_code,
               a.unit_label,
               a.city_code,
               a.city_name,
               COALESCE(a.population, 0)::text,
               COALESCE(a.households, 0)::text,
               a.geom_json,
               {y_selects}
        FROM ageb a
        LEFT JOIN {TMP_POINT_TABLE} d
          ON ST_Covers(a.geom, d.geom)
        GROUP BY a.unit_code, a.unit_label, a.city_code, a.city_name, a.population, a.households, a.geom_json
        ORDER BY a.unit_code
        """.strip()
    )
    _exec(f"DROP TABLE IF EXISTS {TMP_POINT_TABLE};")

    fieldnames = ["unit_code", "unit_label", "city_code", "city_name", "population", "households", "geom_json"] + [key for key, _, _ in Y_DEFS]
    ageb_rows: list[dict[str, str]] = []
    for row in rows_raw:
        record = {fieldnames[idx]: row[idx] for idx in range(len(fieldnames))}
        ageb_rows.append(record)
    with (args.output_dir / "ageb_y_counts.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(ageb_rows)

    city_name = ageb_rows[0]["city_name"] if ageb_rows else city_code
    features = [
        {
            "type": "Feature",
            "properties": {"unit_code": row["unit_code"], "unit_label": row["unit_label"]},
            "geometry": json.loads(row["geom_json"]),
        }
        for row in ageb_rows
    ]

    summary_rows: list[dict[str, object]] = []
    residual_long_rows: list[dict[str, object]] = []
    manifest_rows: list[dict[str, str]] = []

    for y_key, y_label, _ in Y_DEFS:
        fit_rows = [row for row in ageb_rows if _to_float(row["population"]) > 0 and _to_float(row[y_key]) > 0]
        y = [_to_float(row[y_key]) for row in fit_rows]
        n = [_to_float(row["population"]) for row in fit_rows]
        model_rows = []
        for method in ("ols", "robust", "poisson", "negbin", "auto"):
            fit = fit_by_name(y, n, method)
            model_rows.append(
                {
                    "y_key": y_key,
                    "y_label": y_label,
                    "fit_method": method,
                    "n_obs": len(fit_rows),
                    "alpha": fit.alpha,
                    "beta": fit.beta,
                    "r2": fit.r2,
                    "resid_std": fit.residual_std,
                }
            )
        best_row = max(model_rows, key=lambda row: float(row["r2"]))
        summary_rows.extend(model_rows)

        fit_alpha = float(best_row["alpha"])
        fit_beta = float(best_row["beta"])
        fit_resid_std = float(best_row["resid_std"])
        residual_values = []
        map_lookup: dict[str, float] = {}
        score_rows = []
        for row in fit_rows:
            score = compute_deviation_score(_to_float(row[y_key]), _to_float(row["population"]), fit_alpha, fit_beta, fit_resid_std)
            residual_values.append(score.epsilon_log)
            map_lookup[row["unit_code"]] = score.sami
            score_row = {
                "y_key": y_key,
                "y_label": y_label,
                "unit_code": row["unit_code"],
                "unit_label": row["unit_label"],
                "population": _to_float(row["population"]),
                "y_observed": _to_float(row[y_key]),
                "y_expected": score.y_expected,
                "epsilon_log": score.epsilon_log,
                "sami": score.sami,
                "z_residual": score.z_residual,
            }
            score_rows.append(score_row)
            residual_long_rows.append(score_row)

        score_rows.sort(key=lambda r: float(r["sami"]), reverse=True)
        with (args.output_dir / f"{y_key}_ageb_scores.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(score_rows[0].keys()))
            writer.writeheader()
            writer.writerows(score_rows)

        with (args.output_dir / f"{y_key}_model_summary.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(model_rows[0].keys()))
            writer.writeheader()
            writer.writerows(model_rows)

        overview_path = write_model_overview_figure(model_rows, args.output_dir / "figures" / f"{y_key}_model_overview.svg", title=f"{city_name} AGEBs: {y_label}")
        scatter_path = write_scaling_scatter_figure(
            fit_rows,
            args.output_dir / "figures" / f"{y_key}_scaling_scatter.svg",
            title=f"{city_name} AGEBs: {y_label}",
            x_key="population",
            y_key=y_key,
            fit_alpha=fit_alpha,
            fit_beta=fit_beta,
            annotation=f"{best_row['fit_method']}  β={fit_beta:.3f}  R²={float(best_row['r2']):.3f}",
        )
        hist_path = write_residual_histogram_figure(
            residual_values,
            args.output_dir / "figures" / f"{y_key}_residual_histogram.svg",
            title=f"{city_name} AGEB residuals: {y_label}",
            subtitle="Log residuals around the best-fitting within-city AGEB model",
            bins=24,
        )
        map_path = _write_ageb_map(city_name, y_label, features, map_lookup, args.output_dir / "figures" / f"{y_key}_sami_map.svg")
        manifest_rows.extend(
            [
                {"y_key": y_key, "artifact": overview_path.name, "kind": "model_overview"},
                {"y_key": y_key, "artifact": scatter_path.name, "kind": "scaling_scatter"},
                {"y_key": y_key, "artifact": hist_path.name, "kind": "residual_histogram"},
                {"y_key": y_key, "artifact": map_path.name, "kind": "sami_map"},
            ]
        )

    with (args.output_dir / "all_y_model_summary.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summary_rows[0].keys()))
        writer.writeheader()
        writer.writerows(summary_rows)
    with (args.output_dir / "all_y_ageb_scores_long.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(residual_long_rows[0].keys()))
        writer.writeheader()
        writer.writerows(residual_long_rows)
    _write_csv(args.output_dir / "figures_manifest.csv", manifest_rows, ["y_key", "artifact", "kind"])

    best_rows = []
    for y_key, y_label, _ in Y_DEFS:
        sub = [row for row in summary_rows if row["y_key"] == y_key]
        best = max(sub, key=lambda row: float(row["r2"]))
        best_rows.append(best)

    report_lines = [
        "# One-city AGEB Experiment",
        "",
        f"- city: `{city_code}` `{city_name}`",
        f"- AGEB units total: `{len(ageb_rows)}`",
        f"- Y definitions: `{', '.join(key for key, _, _ in Y_DEFS)}`",
        "",
        "Best-fit summary by Y:",
    ]
    for row in best_rows:
        report_lines.append(
            f"- `{row['y_key']}`: `{row['fit_method']}` | `β={float(row['beta']):.3f}` | `R²={float(row['r2']):.3f}` | `n={row['n_obs']}`"
        )
    report_lines.extend(
        [
            "",
            "Key files:",
            f"- [all_y_model_summary.csv]({(args.output_dir / 'all_y_model_summary.csv').as_posix()})",
            f"- [all_y_ageb_scores_long.csv]({(args.output_dir / 'all_y_ageb_scores_long.csv').as_posix()})",
            f"- [figures_manifest.csv]({(args.output_dir / 'figures_manifest.csv').as_posix()})",
        ]
    )
    (args.output_dir / "report.md").write_text("\n".join(report_lines), encoding="utf-8")
    print("\n".join(report_lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

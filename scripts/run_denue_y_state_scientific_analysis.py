#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from urban_sami.analysis.linear_models import compare_nested_models, ols_fit, pearson_corr
from urban_sami.artifacts.figures import BG, PANEL, GRID, AXIS, TEXT, MUTED, TEAL, BLUE, RUST, GOLD, SERIF, SANS
from urban_sami.modeling import bootstrap_fit_intervals, fit_ols


DOCKER_EXE = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DB_CONTAINER = "24-polisplexity-core-db-dev"
POSTGRES_USER = "postgres"
DB_NAME = "urban_sami_exp"
STATE_GEOJSON = Path("/home/hadox/cmd-center/platforms/si/app-hadox/src/static/geo/mx_admin1.geojson")
SCIAN2_LABELS = {
    "11": "Agriculture, forestry, fishing and hunting",
    "21": "Mining",
    "22": "Utilities",
    "23": "Construction",
    "31": "Manufacturing I",
    "32": "Manufacturing II",
    "33": "Manufacturing III",
    "43": "Wholesale trade",
    "46": "Retail trade",
    "48": "Transportation and warehousing",
    "49": "Transportation and warehousing",
    "51": "Information",
    "52": "Finance and insurance",
    "53": "Real estate and leasing",
    "54": "Professional, scientific and technical services",
    "55": "Management of companies",
    "56": "Administrative support and waste management",
    "61": "Educational services",
    "62": "Health care and social assistance",
    "71": "Arts, entertainment and recreation",
    "72": "Accommodation and food services",
    "81": "Other services except government",
    "93": "Government and international organizations",
}


@dataclass(frozen=True)
class StateDatum:
    state_code: str
    state_name: str
    population: float
    total_y: float


@dataclass(frozen=True)
class FamilyConfig:
    key: str
    label: str
    sql_expr: str
    all_categories_label: str


def _query_tsv(sql: str, columns: list[str]) -> list[dict[str, str]]:
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
        parts = line.split("\t")
        rows.append({col: (parts[idx] if idx < len(parts) else "") for idx, col in enumerate(columns)})
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


def _fmt(value: float, digits: int = 3) -> str:
    if abs(value) >= 1000:
        return f"{value:,.0f}"
    return f"{value:.{digits}f}"


def _pct(value: float, digits: int = 2) -> str:
    return f"{value * 100:.{digits}f}%"


def _category_label(family_key: str, category: str) -> str:
    if family_key == "scian2":
        label = SCIAN2_LABELS.get(category, "")
        return f"{category} {label}".strip()
    return category


def _beta_regime(beta: float) -> str:
    if beta > 1.05:
        return "superlinear"
    if beta < 0.95:
        return "sublinear"
    return "near-linear"


def _state_name_to_code_map() -> dict[str, str]:
    return {
        "Aguascalientes": "01",
        "Baja California": "02",
        "Baja California Sur": "03",
        "Campeche": "04",
        "Coahuila de Zaragoza": "05",
        "Colima": "06",
        "Chiapas": "07",
        "Chihuahua": "08",
        "Ciudad de México": "09",
        "Durango": "10",
        "Guanajuato": "11",
        "Guerrero": "12",
        "Hidalgo": "13",
        "Jalisco": "14",
        "México": "15",
        "Michoacán de Ocampo": "16",
        "Morelos": "17",
        "Nayarit": "18",
        "Nuevo León": "19",
        "Oaxaca": "20",
        "Puebla": "21",
        "Querétaro": "22",
        "Quintana Roo": "23",
        "San Luis Potosí": "24",
        "Sinaloa": "25",
        "Sonora": "26",
        "Tabasco": "27",
        "Tamaulipas": "28",
        "Tlaxcala": "29",
        "Veracruz de Ignacio de la Llave": "30",
        "Yucatán": "31",
        "Zacatecas": "32",
    }


def _state_shapes() -> tuple[list[dict], tuple[float, float, float, float]]:
    doc = json.loads(STATE_GEOJSON.read_text(encoding="utf-8"))
    features = doc["features"]
    minx = miny = 10**9
    maxx = maxy = -10**9
    rows = []
    code_map = _state_name_to_code_map()
    for feat in features:
        props = feat.get("properties", {})
        name = props.get("name", "")
        code = code_map.get(name, "")
        geom = feat.get("geometry", {})
        rows.append({"name": name, "code": code, "geometry": geom})
        coords = geom.get("coordinates", [])
        if geom.get("type") == "MultiPolygon":
            for poly in coords:
                for ring in poly:
                    for lon, lat in ring:
                        minx = min(minx, lon)
                        miny = min(miny, lat)
                        maxx = max(maxx, lon)
                        maxy = max(maxy, lat)
        elif geom.get("type") == "Polygon":
            for ring in coords:
                for lon, lat in ring:
                    minx = min(minx, lon)
                    miny = min(miny, lat)
                    maxx = max(maxx, lon)
                    maxy = max(maxy, lat)
    return rows, (minx, miny, maxx, maxy)


def _project_ring(ring: list[list[float]], bounds: tuple[float, float, float, float], box: tuple[float, float, float, float]) -> str:
    minx, miny, maxx, maxy = bounds
    x0, y0, w, h = box
    sx = w / (maxx - minx)
    sy = h / (maxy - miny)
    scale = min(sx, sy)
    ox = x0 + ((w - ((maxx - minx) * scale)) / 2.0)
    oy = y0 + ((h - ((maxy - miny) * scale)) / 2.0)
    pts = []
    for lon, lat in ring:
        px = ox + ((lon - minx) * scale)
        py = oy + (((maxy - lat)) * scale)
        pts.append(f"{px:.2f},{py:.2f}")
    return " ".join(pts)


def _write_svg(path: Path, body: str, width: int, height: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _color_continuous(value: float, vmax: float) -> str:
    if vmax <= 0.0:
        return "#ddd6c8"
    t = max(0.0, min(1.0, value / vmax))
    r = int(235 - (120 * t))
    g = int(226 - (110 * t))
    b = int(204 - (150 * t))
    return f"#{r:02x}{g:02x}{b:02x}"


def _color_diverging(value: float, vmax: float) -> str:
    if vmax <= 0.0:
        return "#ddd6c8"
    t = max(-1.0, min(1.0, value / vmax))
    if t >= 0:
        r = int(245 - (70 * t))
        g = int(241 - (120 * t))
        b = int(235 - (150 * t))
    else:
        u = abs(t)
        r = int(238 - (130 * u))
        g = int(242 - (70 * u))
        b = int(245 - (10 * u))
    return f"#{r:02x}{g:02x}{b:02x}"


def write_state_map(path: Path, *, title: str, subtitle: str, value_map: dict[str, float], diverging: bool = False) -> Path:
    width, height = 980, 660
    box = (64, 118, 620, 490)
    features, bounds = _state_shapes()
    vmax = max(abs(value) for value in value_map.values()) if value_map else 0.0
    parts = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">{subtitle}</text>',
    ]
    for feat in features:
        code = feat["code"]
        value = value_map.get(code, 0.0)
        fill = _color_diverging(value, vmax) if diverging else _color_continuous(value, vmax)
        geom = feat["geometry"]
        if geom.get("type") == "MultiPolygon":
            for poly in geom.get("coordinates", []):
                for ring in poly[:1]:
                    parts.append(f'<polygon points="{_project_ring(ring, bounds, box)}" fill="{fill}" stroke="#faf8f3" stroke-width="0.8"/>')
        elif geom.get("type") == "Polygon":
            parts.append(f'<polygon points="{_project_ring(geom.get("coordinates", [[]])[0], bounds, box)}" fill="{fill}" stroke="#faf8f3" stroke-width="0.8"/>')
    return _write_svg(path, "".join(parts), width, height)


def write_beta_forest(path: Path, *, title: str, subtitle: str, rows: list[dict]) -> Path:
    rows = sorted(rows, key=lambda row: _to_float(row["category_total"]), reverse=True)
    width = 1120
    row_h = 30
    top = 108
    bottom = 54
    left = 320
    right = 90
    chart_w = width - left - right
    height = top + (len(rows) * row_h) + bottom
    beta_values = [_to_float(row["beta"]) for row in rows]
    beta_min = min(beta_values) - 0.1
    beta_max = max(beta_values) + 0.1
    r2_x0 = left + (chart_w * 0.68)

    def beta_to_x(value: float) -> float:
        return left + ((value - beta_min) / (beta_max - beta_min)) * (chart_w * 0.55)

    def r2_to_x(value: float) -> float:
        return r2_x0 + max(0.0, min(1.0, value)) * (chart_w * 0.32)

    parts = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">{subtitle}</text>',
        f'<text x="{left}" y="95" font-size="14" font-family="{SANS}" fill="{MUTED}">β with 95% bootstrap interval</text>',
        f'<text x="{r2_x0:.2f}" y="95" font-size="14" font-family="{SANS}" fill="{MUTED}">R²</text>',
    ]
    for tick in range(5):
        value = beta_min + ((beta_max - beta_min) * tick / 4.0)
        x = beta_to_x(value)
        parts.append(f'<line x1="{x:.2f}" y1="{top-8}" x2="{x:.2f}" y2="{height-bottom+8}" stroke="{GRID}"/>')
        parts.append(f'<text x="{x:.2f}" y="{height-bottom+30}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">{value:.2f}</text>')
    x_one = beta_to_x(1.0)
    parts.append(f'<line x1="{x_one:.2f}" y1="{top-8}" x2="{x_one:.2f}" y2="{height-bottom+8}" stroke="{RUST}" stroke-width="1.5" stroke-dasharray="6,5"/>')
    for tick in range(6):
        value = tick / 5.0
        x = r2_to_x(value)
        parts.append(f'<line x1="{x:.2f}" y1="{top-8}" x2="{x:.2f}" y2="{height-bottom+8}" stroke="{GRID}"/>')
        parts.append(f'<text x="{x:.2f}" y="{height-bottom+30}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">{value:.1f}</text>')
    for idx, row in enumerate(rows):
        y = top + (idx * row_h)
        beta = _to_float(row["beta"])
        r2 = _to_float(row["r2"])
        low = _to_float(row["beta_ci_low"], beta)
        high = _to_float(row["beta_ci_high"], beta)
        share = _to_float(row["share_of_total"])
        parts.append(f'<line x1="{left-8}" y1="{y:.2f}" x2="{left+chart_w:.2f}" y2="{y:.2f}" stroke="#f0eadf"/>')
        parts.append(f'<text x="{left-14}" y="{y+5:.2f}" text-anchor="end" font-size="13" font-family="{SANS}" fill="{TEXT}">{row["category_label"]}</text>')
        parts.append(f'<text x="{left-14}" y="{y+18:.2f}" text-anchor="end" font-size="11" font-family="{SANS}" fill="{MUTED}">share={share:.3f}, n={row["n_obs"]}</text>')
        parts.append(f'<line x1="{beta_to_x(low):.2f}" y1="{y:.2f}" x2="{beta_to_x(high):.2f}" y2="{y:.2f}" stroke="{BLUE}" stroke-width="2.2"/>')
        parts.append(f'<circle cx="{beta_to_x(beta):.2f}" cy="{y:.2f}" r="5.0" fill="{TEAL}"/>')
        parts.append(f'<circle cx="{r2_to_x(r2):.2f}" cy="{y:.2f}" r="5.0" fill="{BLUE}"/>')
    return _write_svg(path, "".join(parts), width, height)


def write_influence_figure(path: Path, *, title: str, subtitle: str, rows: list[dict]) -> Path:
    rows = sorted(rows, key=lambda row: abs(_to_float(row["delta_r2"])), reverse=True)
    width = 1180
    height = 700
    left = 280
    right = 70
    top = 110
    bottom = 70
    plot_w = width - left - right
    plot_h = height - top - bottom
    row_h = plot_h / max(1, len(rows))
    beta_x0 = left
    beta_x1 = left + (plot_w * 0.46)
    r2_x0 = left + (plot_w * 0.58)
    r2_x1 = left + plot_w
    delta_beta_values = [_to_float(row["delta_beta"]) for row in rows]
    delta_beta_max = max(abs(value) for value in delta_beta_values) if delta_beta_values else 1.0

    def beta_to_x(value: float) -> float:
        center = (beta_x0 + beta_x1) / 2.0
        return center + ((value / max(delta_beta_max, 1e-9)) * ((beta_x1 - beta_x0) / 2.0))

    def r2_to_x(value: float) -> float:
        center = (r2_x0 + r2_x1) / 2.0
        max_abs = max(abs(_to_float(row["delta_r2"])) for row in rows) or 1e-9
        return center + ((value / max_abs) * ((r2_x1 - r2_x0) / 2.0))

    parts = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">{subtitle}</text>',
        f'<text x="{beta_x0}" y="96" font-size="14" font-family="{SANS}" fill="{MUTED}">Δβ when category is removed</text>',
        f'<text x="{r2_x0}" y="96" font-size="14" font-family="{SANS}" fill="{MUTED}">ΔR² when category is removed</text>',
    ]
    parts.append(f'<line x1="{(beta_x0+beta_x1)/2:.2f}" y1="{top-10}" x2="{(beta_x0+beta_x1)/2:.2f}" y2="{top+plot_h:.2f}" stroke="{AXIS}" stroke-dasharray="4,4"/>')
    parts.append(f'<line x1="{(r2_x0+r2_x1)/2:.2f}" y1="{top-10}" x2="{(r2_x0+r2_x1)/2:.2f}" y2="{top+plot_h:.2f}" stroke="{AXIS}" stroke-dasharray="4,4"/>')
    for idx, row in enumerate(rows):
        y = top + ((idx + 0.5) * row_h)
        db = _to_float(row["delta_beta"])
        dr2 = _to_float(row["delta_r2"])
        share = _to_float(row["share_of_total"])
        parts.append(f'<line x1="{left-8}" y1="{y:.2f}" x2="{left+plot_w:.2f}" y2="{y:.2f}" stroke="#f0eadf"/>')
        parts.append(f'<text x="{left-14}" y="{y+5:.2f}" text-anchor="end" font-size="13" font-family="{SANS}" fill="{TEXT}">{row["category_label"]}</text>')
        parts.append(f'<text x="{left-14}" y="{y+18:.2f}" text-anchor="end" font-size="11" font-family="{SANS}" fill="{MUTED}">share={share:.3f}</text>')
        parts.append(f'<circle cx="{beta_to_x(db):.2f}" cy="{y:.2f}" r="5.0" fill="{TEAL}"/>')
        parts.append(f'<circle cx="{r2_to_x(dr2):.2f}" cy="{y:.2f}" r="5.0" fill="{BLUE}"/>')
    return _write_svg(path, "".join(parts), width, height)


def write_composition_panels(path: Path, *, title: str, subtitle: str, top_categories: list[str], state_rows: list[dict], composition_rows: list[dict], x_field: str, y_title: str) -> Path:
    width = 1200
    height = 780
    cols = 3
    rows_n = math.ceil(len(top_categories) / cols)
    panel_w = 340
    panel_h = 260
    margin_x = 48
    margin_y = 120
    gap_x = 34
    gap_y = 38

    by_category = {cat: [row for row in composition_rows if row["category"] == cat] for cat in top_categories}
    x_values = [_to_float(row[x_field]) for row in composition_rows]
    x_min = min(x_values)
    x_max = max(x_values)

    parts = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">{subtitle}</text>',
    ]

    for idx, category in enumerate(top_categories):
        r = idx // cols
        c = idx % cols
        x0 = margin_x + (c * (panel_w + gap_x))
        y0 = margin_y + (r * (panel_h + gap_y))
        plot_left = x0 + 48
        plot_top = y0 + 24
        plot_w = panel_w - 72
        plot_h = panel_h - 58
        rows_here = by_category[category]
        if not rows_here:
            continue
        shares = [_to_float(row["share"]) for row in rows_here]
        y_max = max(max(shares), 0.001)

        def px(v: float) -> float:
            return plot_left + ((v - x_min) / max(x_max - x_min, 1e-9)) * plot_w

        def py(v: float) -> float:
            return plot_top + plot_h - ((v / y_max) * plot_h)

        parts.append(f'<rect x="{x0}" y="{y0}" width="{panel_w}" height="{panel_h}" rx="12" fill="#fcfaf5" stroke="{GRID}"/>')
        parts.append(f'<text x="{x0+16}" y="{y0+18}" font-size="13" font-family="{SANS}" fill="{TEXT}">{category}</text>')
        for tick in range(4):
            xv = x_min + ((x_max - x_min) * tick / 3.0)
            x = px(xv)
            parts.append(f'<line x1="{x:.2f}" y1="{plot_top}" x2="{x:.2f}" y2="{plot_top+plot_h}" stroke="{GRID}"/>')
        for tick in range(4):
            yv = y_max * tick / 3.0
            y = py(yv)
            parts.append(f'<line x1="{plot_left}" y1="{y:.2f}" x2="{plot_left+plot_w}" y2="{y:.2f}" stroke="{GRID}"/>')
        parts.append(f'<rect x="{plot_left}" y="{plot_top}" width="{plot_w}" height="{plot_h}" fill="none" stroke="{AXIS}"/>')
        xs = []
        ys = []
        for row in rows_here:
            xv = _to_float(row[x_field])
            yv = _to_float(row["share"])
            xs.append(xv)
            ys.append(yv)
            parts.append(f'<circle cx="{px(xv):.2f}" cy="{py(yv):.2f}" r="4.2" fill="{BLUE}" fill-opacity="0.65"/>')
        # simple line fit
        design = [[1.0, x] for x in xs]
        fit = ols_fit(design, ys)
        y_line0 = fit.coefficients[0] + (fit.coefficients[1] * x_min)
        y_line1 = fit.coefficients[0] + (fit.coefficients[1] * x_max)
        parts.append(f'<line x1="{px(x_min):.2f}" y1="{py(y_line0):.2f}" x2="{px(x_max):.2f}" y2="{py(y_line1):.2f}" stroke="{RUST}" stroke-width="2"/>')
        corr = pearson_corr(xs, ys)
        parts.append(f'<text x="{x0+16}" y="{y0+panel_h-14}" font-size="11" font-family="{SANS}" fill="{MUTED}">corr={corr:.3f}</text>')

    parts.append(f'<text x="{width/2:.2f}" y="{height-18}" text-anchor="middle" font-size="13" font-family="{SANS}" fill="{MUTED}">{y_title}</text>')
    return _write_svg(path, "".join(parts), width, height)


def write_model_comparison(path: Path, *, title: str, rows: list[dict]) -> Path:
    width = 1220
    row_h = 42
    top = 122
    height = top + (len(rows) * row_h) + 84
    columns = [
        ("Family", 46, "start"),
        ("Adj. R² common", 320, "end"),
        ("Adj. R² varying", 480, "end"),
        ("Δ Adj. R²", 640, "end"),
        ("F", 790, "end"),
        ("df", 900, "middle"),
        ("p", 1030, "end"),
        ("Decision", 1150, "end"),
    ]
    parts = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Nested OLS comparison. Null: one common slope after category intercepts. Alternative: category-specific slopes.</text>',
    ]
    for header, x, anchor in columns:
        parts.append(f'<text x="{x}" y="104" text-anchor="{anchor}" font-size="13" font-family="{SANS}" fill="{MUTED}">{header}</text>')
    for idx, row in enumerate(rows):
        y = top + (idx * row_h)
        decision = "reject common slope" if _to_float(row["p_value"], 1.0) < 0.05 else "no slope evidence"
        parts.append(f'<line x1="42" y1="{y-16}" x2="{width-42}" y2="{y-16}" stroke="{GRID}"/>')
        values = [
            row["family_label"],
            f"{_to_float(row['common_adj_r2']):.4f}",
            f"{_to_float(row['varying_adj_r2']):.4f}",
            f"{_to_float(row['delta_adj_r2']):+.4f}",
            f"{_to_float(row['f_stat']):.2f}",
            f"{row['df_num']}, {row['df_den']}",
            row["p_value"],
            decision,
        ]
        for (header, x, anchor), value in zip(columns, values):
            fill = TEXT if header in {"Family", "Decision"} else MUTED
            parts.append(f'<text x="{x}" y="{y}" text-anchor="{anchor}" font-size="13" font-family="{SANS}" fill="{fill}">{value}</text>')
    return _write_svg(path, "".join(parts), width, height)


def write_ranked_metric_chart(
    path: Path,
    *,
    title: str,
    subtitle: str,
    rows: list[dict],
    metric_field: str,
    order_field: str = "share_of_total",
    low_field: str | None = None,
    high_field: str | None = None,
    color: str = TEAL,
    ref_line: float | None = None,
    symmetric: bool = False,
    fixed_range: tuple[float, float] | None = None,
    value_formatter=lambda value: f"{value:.3f}",
) -> Path:
    rows = sorted(rows, key=lambda row: _to_float(row[order_field]), reverse=True)
    width = 1200
    left = 430
    right = 140
    top = 120
    bottom = 82
    row_h = 30
    height = top + (len(rows) * row_h) + bottom

    values = [_to_float(row[metric_field]) for row in rows]
    if low_field and high_field:
        values.extend(_to_float(row[low_field]) for row in rows)
        values.extend(_to_float(row[high_field]) for row in rows)
    if fixed_range is not None:
        x_min, x_max = fixed_range
    elif symmetric:
        vmax = max(abs(value) for value in values) if values else 1.0
        x_min, x_max = -vmax, vmax
    else:
        x_min = min(values) if values else 0.0
        x_max = max(values) if values else 1.0
        pad = max((x_max - x_min) * 0.08, 0.02)
        x_min -= pad
        x_max += pad
    plot_w = width - left - right

    def x_pos(value: float) -> float:
        return left + ((value - x_min) / max(x_max - x_min, 1e-9)) * plot_w

    parts = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">{subtitle}</text>',
    ]

    for tick in range(6):
        value = x_min + ((x_max - x_min) * tick / 5.0)
        x = x_pos(value)
        parts.append(f'<line x1="{x:.2f}" y1="{top-10}" x2="{x:.2f}" y2="{height-bottom+8}" stroke="{GRID}"/>')
        parts.append(f'<text x="{x:.2f}" y="{height-bottom+34}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">{value_formatter(value)}</text>')
    if ref_line is not None:
        x = x_pos(ref_line)
        parts.append(f'<line x1="{x:.2f}" y1="{top-10}" x2="{x:.2f}" y2="{height-bottom+8}" stroke="{RUST}" stroke-width="1.5" stroke-dasharray="6,5"/>')

    for idx, row in enumerate(rows):
        y = top + (idx * row_h)
        label = row["category_label"]
        share = _to_float(row.get("share_of_total", 0.0))
        value = _to_float(row[metric_field])
        parts.append(f'<line x1="{left-8}" y1="{y:.2f}" x2="{width-right+8:.2f}" y2="{y:.2f}" stroke="#f0eadf"/>')
        parts.append(f'<text x="{left-14}" y="{y+5:.2f}" text-anchor="end" font-size="13" font-family="{SANS}" fill="{TEXT}">{label}</text>')
        parts.append(f'<text x="{left-14}" y="{y+18:.2f}" text-anchor="end" font-size="11" font-family="{SANS}" fill="{MUTED}">share={_pct(share)}</text>')
        if low_field and high_field:
            low = _to_float(row[low_field], value)
            high = _to_float(row[high_field], value)
            parts.append(f'<line x1="{x_pos(low):.2f}" y1="{y:.2f}" x2="{x_pos(high):.2f}" y2="{y:.2f}" stroke="{BLUE}" stroke-width="2.2"/>')
        parts.append(f'<circle cx="{x_pos(value):.2f}" cy="{y:.2f}" r="5.0" fill="{color}"/>')
        parts.append(f'<text x="{width-right+14}" y="{y+4:.2f}" font-size="12" font-family="{SANS}" fill="{TEXT}">{value_formatter(value)}</text>')
    return _write_svg(path, "".join(parts), width, height)


def _merge_complete_rows(result: dict[str, list[dict]]) -> list[dict]:
    fits = {row["category"]: row for row in result["category_fits"]}
    influence = {row["category"]: row for row in result["influence_rows"]}
    comp = {row["category"]: row for row in result["composition_rows"]}
    rows: list[dict] = []
    for category, fit_row in fits.items():
        merged = dict(fit_row)
        merged.update({
            "beta_without": influence.get(category, {}).get("beta_without", ""),
            "r2_without": influence.get(category, {}).get("r2_without", ""),
            "delta_beta": influence.get(category, {}).get("delta_beta", ""),
            "delta_r2": influence.get(category, {}).get("delta_r2", ""),
            "corr_log_population_share": comp.get(category, {}).get("corr_log_population_share", ""),
            "share_on_log_population_slope": comp.get(category, {}).get("share_on_log_population_slope", ""),
            "share_on_log_population_r2": comp.get(category, {}).get("share_on_log_population_r2", ""),
            "corr_residual_share": comp.get(category, {}).get("corr_residual_share", ""),
            "share_on_residual_slope": comp.get(category, {}).get("share_on_residual_slope", ""),
            "share_on_residual_r2": comp.get(category, {}).get("share_on_residual_r2", ""),
        })
        rows.append(merged)
    rows.sort(key=lambda row: _to_float(row["share_of_total"]), reverse=True)
    for field, rank_field, absolute in [
        ("share_of_total", "share_rank", False),
        ("beta", "beta_rank", False),
        ("r2", "r2_rank", False),
        ("delta_beta", "abs_delta_beta_rank", True),
        ("delta_r2", "abs_delta_r2_rank", True),
        ("corr_log_population_share", "abs_corr_log_population_rank", True),
        ("corr_residual_share", "abs_corr_residual_rank", True),
    ]:
        ordered = sorted(
            rows,
            key=lambda row: abs(_to_float(row[field])) if absolute else _to_float(row[field]),
            reverse=True,
        )
        for idx, row in enumerate(ordered, start=1):
            row[rank_field] = idx
    return rows


def _markdown_complete_table(rows: list[dict]) -> list[str]:
    lines = [
        "| Category | Share | β | 95% CI | R² | Δβ | ΔR² | corr(share, log N) | corr(share, residual) |",
        "| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    row["category_label"],
                    _pct(_to_float(row["share_of_total"])),
                    f"{_to_float(row['beta']):.4f}",
                    f"[{_to_float(row['beta_ci_low']):.4f}, {_to_float(row['beta_ci_high']):.4f}]",
                    f"{_to_float(row['r2']):.4f}",
                    f"{_to_float(row['delta_beta']):+.4f}",
                    f"{_to_float(row['delta_r2']):+.4f}",
                    f"{_to_float(row['corr_log_population_share']):+.3f}",
                    f"{_to_float(row['corr_residual_share']):+.3f}",
                ]
            )
            + " |"
        )
    return lines


def _extreme_rows(rows: list[dict], field: str, *, n: int = 3, absolute: bool = False) -> tuple[list[dict], list[dict]]:
    ordered = sorted(rows, key=lambda row: abs(_to_float(row[field])) if absolute else _to_float(row[field]), reverse=True)
    top = ordered[:n]
    if absolute:
        bottom = sorted(rows, key=lambda row: abs(_to_float(row[field])))[:n]
    else:
        bottom = sorted(rows, key=lambda row: _to_float(row[field]))[:n]
    return top, bottom


def _fetch_state_population() -> list[StateDatum]:
    rows = _query_tsv(
        """
        SELECT unit_code, unit_label, population::text
        FROM raw.population_units
        WHERE level = 'state'
        ORDER BY unit_code
        """.strip(),
        ["state_code", "state_name", "population"],
    )
    total_counts = _query_tsv(
        """
        SELECT state_code, COUNT(*)::text AS total_y
        FROM raw.denue_establishments
        WHERE state_code <> ''
        GROUP BY state_code
        ORDER BY state_code
        """.strip(),
        ["state_code", "total_y"],
    )
    count_map = {row["state_code"]: _to_float(row["total_y"]) for row in total_counts}
    return [
        StateDatum(
            state_code=row["state_code"],
            state_name=row["state_name"],
            population=_to_float(row["population"]),
            total_y=count_map.get(row["state_code"], 0.0),
        )
        for row in rows
    ]


def _fetch_family_counts(config: FamilyConfig) -> list[dict[str, str]]:
    return _query_tsv(
        f"""
        SELECT state_code,
               {config.sql_expr} AS category,
               COUNT(*)::text AS est_count
        FROM raw.denue_establishments
        WHERE state_code <> ''
          AND {config.sql_expr} <> ''
        GROUP BY state_code, {config.sql_expr}
        ORDER BY category, state_code
        """.strip(),
        ["state_code", "category", "est_count"],
    )


def _family_analysis(config: FamilyConfig, states: list[StateDatum]) -> dict[str, list[dict]]:
    count_rows = _fetch_family_counts(config)
    state_codes = [state.state_code for state in states]
    pop_map = {state.state_code: state.population for state in states}
    total_map = {state.state_code: state.total_y for state in states}
    state_name_map = {state.state_code: state.state_name for state in states}
    counts_by_cat: dict[str, dict[str, float]] = {}
    for row in count_rows:
        counts_by_cat.setdefault(row["category"], {})[row["state_code"]] = _to_float(row["est_count"])
    categories = sorted(counts_by_cat.keys(), key=lambda cat: sum(counts_by_cat[cat].values()), reverse=True)

    category_fits: list[dict] = []
    share_rows: list[dict] = []
    influence_rows: list[dict] = []
    composition_rows: list[dict] = []

    baseline_y = [total_map[code] for code in state_codes if pop_map[code] > 0 and total_map[code] > 0]
    baseline_n = [pop_map[code] for code in state_codes if pop_map[code] > 0 and total_map[code] > 0]
    baseline_fit = fit_ols(baseline_y, baseline_n)
    baseline_resid_map = {}
    for code in state_codes:
        y = total_map[code]
        n = pop_map[code]
        if y > 0 and n > 0:
            baseline_resid_map[code] = math.log(y) - (baseline_fit.alpha + baseline_fit.beta * math.log(n))

    for category in categories:
        cat_map = counts_by_cat[category]
        category_label = _category_label(config.key, category)
        total_cat = sum(cat_map.values())
        share_of_total = total_cat / sum(total_map.values()) if sum(total_map.values()) > 0 else 0.0
        y = [cat_map.get(code, 0.0) for code in state_codes if pop_map[code] > 0 and cat_map.get(code, 0.0) > 0]
        n = [pop_map[code] for code in state_codes if pop_map[code] > 0 and cat_map.get(code, 0.0) > 0]
        if len(y) >= 2:
            fit = fit_ols(y, n)
            ci = bootstrap_fit_intervals(y, n, "ols", n_bootstrap=120, seed=42)
            category_fits.append(
                {
                    "family": config.key,
                    "family_label": config.label,
                    "category": category,
                    "category_label": category_label,
                    "n_obs": len(y),
                    "category_total": total_cat,
                    "share_of_total": share_of_total,
                    "alpha": fit.alpha,
                    "beta": fit.beta,
                    "beta_ci_low": ci["beta_low"] if ci["beta_low"] is not None else "",
                    "beta_ci_high": ci["beta_high"] if ci["beta_high"] is not None else "",
                    "r2": fit.r2,
                    "resid_std": fit.residual_std,
                }
            )

        y_minus = []
        n_minus = []
        for code in state_codes:
            y_val = total_map[code] - cat_map.get(code, 0.0)
            n_val = pop_map[code]
            if y_val > 0 and n_val > 0:
                y_minus.append(y_val)
                n_minus.append(n_val)
        if len(y_minus) >= 2:
            fit_minus = fit_ols(y_minus, n_minus)
            influence_rows.append(
                {
                    "family": config.key,
                    "family_label": config.label,
                    "category": category,
                    "category_label": category_label,
                    "category_total": total_cat,
                    "share_of_total": share_of_total,
                    "beta_without": fit_minus.beta,
                    "r2_without": fit_minus.r2,
                    "delta_beta": baseline_fit.beta - fit_minus.beta,
                    "delta_r2": baseline_fit.r2 - fit_minus.r2,
                }
            )

        shares = []
        logn = []
        residuals = []
        for code in state_codes:
            total = total_map[code]
            if total <= 0 or pop_map[code] <= 0:
                continue
            share = cat_map.get(code, 0.0) / total
            shares.append(share)
            logn.append(math.log(pop_map[code]))
            residuals.append(baseline_resid_map.get(code, 0.0))
            share_rows.append(
                {
                    "family": config.key,
                    "family_label": config.label,
                    "state_code": code,
                    "state_name": state_name_map[code],
                    "category": category,
                    "category_label": category_label,
                    "share": share,
                    "log_population": math.log(pop_map[code]),
                    "aggregate_residual": baseline_resid_map.get(code, 0.0),
                }
            )
        if len(shares) >= 2:
            share_fit = ols_fit([[1.0, x] for x in logn], shares)
            resid_fit = ols_fit([[1.0, r] for r in residuals], shares)
            composition_rows.append(
                {
                    "family": config.key,
                    "family_label": config.label,
                    "category": category,
                    "category_label": category_label,
                    "category_total": total_cat,
                    "share_of_total": share_of_total,
                    "corr_log_population_share": pearson_corr(logn, shares),
                    "share_on_log_population_slope": share_fit.coefficients[1],
                    "share_on_log_population_r2": share_fit.r2,
                    "corr_residual_share": pearson_corr(residuals, shares),
                    "share_on_residual_slope": resid_fit.coefficients[1],
                    "share_on_residual_r2": resid_fit.r2,
                }
            )

    # pooled comparison
    pooled_rows = []
    cat_index = {category: idx for idx, category in enumerate(categories)}
    reference = categories[0]
    for code in state_codes:
        n = pop_map[code]
        if n <= 0:
            continue
        logn = math.log(n)
        for category in categories:
            y = counts_by_cat.get(category, {}).get(code, 0.0)
            if y <= 0:
                continue
            pooled_rows.append((code, category, math.log(y), logn))
    common_design = []
    varying_design = []
    response = []
    for _code, category, logy, logn in pooled_rows:
        dummies = [1.0 if category == cat else 0.0 for cat in categories[1:]]
        common_design.append([1.0] + dummies + [logn])
        interactions = [dummy * logn for dummy in dummies]
        varying_design.append([1.0] + dummies + [logn] + interactions)
        response.append(logy)
    common_fit = ols_fit(common_design, response)
    varying_fit = ols_fit(varying_design, response)
    cmp = compare_nested_models(common_fit, varying_fit)
    pooled_summary = [
        {
            "family": config.key,
            "family_label": config.label,
            "n_obs": len(response),
            "n_categories": len(categories),
            "common_r2": common_fit.r2,
            "common_adj_r2": common_fit.adj_r2,
            "varying_r2": varying_fit.r2,
            "varying_adj_r2": varying_fit.adj_r2,
            "delta_adj_r2": varying_fit.adj_r2 - common_fit.adj_r2,
            "f_stat": cmp.f_stat,
            "df_num": cmp.df_num,
            "df_den": cmp.df_den,
            "p_value": f"{cmp.p_value:.6g}" if cmp.p_value is not None else "",
        }
    ]

    total_residual_rows = []
    for code in state_codes:
        total_residual_rows.append(
            {
                "state_code": code,
                "state_name": state_name_map[code],
                "population": pop_map[code],
                "total_y": total_map[code],
                "aggregate_log_residual": baseline_resid_map.get(code, 0.0),
            }
        )

    return {
        "category_fits": category_fits,
        "influence_rows": influence_rows,
        "share_rows": share_rows,
        "composition_rows": composition_rows,
        "pooled_summary": pooled_summary,
        "total_residual_rows": total_residual_rows,
        "baseline_fit": [
            {
                "family": config.key,
                "family_label": config.label,
                "alpha": baseline_fit.alpha,
                "beta": baseline_fit.beta,
                "r2": baseline_fit.r2,
                "resid_std": baseline_fit.residual_std,
                "n_obs": len(baseline_y),
            }
        ],
    }


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "denue-y-state-scientific-analysis-2026-04-21"
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    states = _fetch_state_population()
    families = [
        FamilyConfig("scian2", "SCIAN 2-digit", "SUBSTRING(scian_code FROM 1 FOR 2)", "all establishments"),
        FamilyConfig("per_ocu", "DENUE size band", "per_ocu", "all establishments"),
    ]

    report_lines = [
        "# State Scientific Y Analysis",
        "",
        "Date: `2026-04-21`",
        "",
        "This dossier is a **state aggregation experiment**, not the primary theory test.",
        "In the current project, `city` is the theory-native urban scaling object; `state` is used here to study what happens when cities are aggregated into a coarser administrative support.",
        "The question in this report is therefore experimental: does coarse aggregation manufacture a strong scaling law, and if so, which native categories impose that aggregate behavior?",
        "",
        "The durable repo framing is documented at:",
        f"- [theory-framing.md]({(root / 'docs' / 'theory-framing.md').resolve()})",
        "",
        "## How To Read The Statistics",
        "",
        "- `β`: scaling exponent in `Y ~ N^β`. In SAMI/scaling theory it is read relative to `1`, not as a standalone 'faster growth' score.",
        "- For count-like `Y`, `Y / N ~ N^(β-1)`. So `β > 1` means superlinear concentration and rising per-capita prevalence with size; `β ≈ 1` means near-proportional scaling and roughly stable per-capita prevalence; `β < 1` means sublinear scaling and declining per-capita prevalence with size.",
        "- Across categories, `β` alone is not an importance measure. Category weight, intercept `α`, and fit strength `R²` also matter.",
        "- `R²`: how much cross-state variance is explained by population for that category. This is a fit-strength statistic, not an effect-size statistic.",
        "- `Δβ` from leave-one-out: `β_all - β_without_category`. Positive means that category pushes the aggregate slope upward. Negative means it pulls the aggregate slope downward.",
        "- `ΔR²` from leave-one-out: `R²_all - R²_without_category`. Positive means removing the category makes the aggregate fit worse, so the category supports the aggregate fit. Negative means removing it improves the aggregate fit, so the category makes the aggregate fit noisier.",
        "- `corr(share, log N)`: whether larger states allocate a higher or lower share of total establishments to that category.",
        "- `corr(share, residual)`: whether states above or below the aggregate fit line tend to have more of that category. Positive means over-performing states have more of it; negative means they have less.",
        "- Nested F-test: null hypothesis is one common slope across categories after category intercepts are allowed. Low `p` means slope heterogeneity is real and categories should not be collapsed into a single common slope story.",
        "- Terminology note: in `urban-sami`, SAMI is the raw log residual `log(Y / Y_expected)`; the standardized residual `z = epsilon / sd(epsilon)` is kept only as a secondary diagnostic when needed.",
        "",
        "## Experimental Position In The Project",
        "",
        "- `city`: primary theory run",
        "- `state`: aggregation experiment over cities",
        "- `AGEB` and `manzana`: intra-urban extension experiments",
        "- non-administrative supports: geometry-support experiments",
        "",
        "## Files",
        "",
        f"- Full folder: [{outdir.name}]({outdir.resolve()})",
        f"- Figure manifest: [figures_manifest.csv]({(figdir / 'figures_manifest.csv').resolve()})",
        "",
    ]

    model_comparison_rows = []
    manifest_rows = []

    for family in families:
        result = _family_analysis(family, states)
        complete_rows = _merge_complete_rows(result)
        _write_csv(outdir / f"{family.key}_category_fits.csv", result["category_fits"], list(result["category_fits"][0].keys()) if result["category_fits"] else [])
        _write_csv(outdir / f"{family.key}_leave_one_out.csv", result["influence_rows"], list(result["influence_rows"][0].keys()) if result["influence_rows"] else [])
        _write_csv(outdir / f"{family.key}_state_shares.csv", result["share_rows"], list(result["share_rows"][0].keys()) if result["share_rows"] else [])
        _write_csv(outdir / f"{family.key}_composition_stats.csv", result["composition_rows"], list(result["composition_rows"][0].keys()) if result["composition_rows"] else [])
        _write_csv(outdir / f"{family.key}_pooled_model_comparison.csv", result["pooled_summary"], list(result["pooled_summary"][0].keys()) if result["pooled_summary"] else [])
        _write_csv(outdir / f"{family.key}_aggregate_baseline.csv", result["baseline_fit"], list(result["baseline_fit"][0].keys()))
        _write_csv(outdir / f"{family.key}_complete_statistics.csv", complete_rows, list(complete_rows[0].keys()) if complete_rows else [])
        if family.key == "scian2":
            _write_csv(outdir / "state_total_residuals.csv", result["total_residual_rows"], list(result["total_residual_rows"][0].keys()))

        share_fig = write_ranked_metric_chart(
            figdir / f"{family.key}_share_rank.svg",
            title=f"{family.label}: category weight in aggregate Y",
            subtitle="Categories ordered by share of total state establishments. In this state aggregation experiment, this shows who can impose the aggregate law.",
            rows=complete_rows,
            metric_field="share_of_total",
            fixed_range=(0.0, max(0.01, max(_to_float(row["share_of_total"]) for row in complete_rows) * 1.05)),
            color=GOLD,
            value_formatter=lambda value: _pct(value, 1),
        )
        beta_fig = write_ranked_metric_chart(
            figdir / f"{family.key}_beta_rank.svg",
            title=f"{family.label}: category-specific β",
            subtitle="State-level scaling exponents for each category, with 95% bootstrap intervals. Categories stay in the same share order as the weight figure.",
            rows=complete_rows,
            metric_field="beta",
            low_field="beta_ci_low",
            high_field="beta_ci_high",
            ref_line=1.0,
            color=TEAL,
            value_formatter=lambda value: f"{value:.3f}",
        )
        r2_fig = write_ranked_metric_chart(
            figdir / f"{family.key}_r2_rank.svg",
            title=f"{family.label}: category-specific R²",
            subtitle="How strongly state population explains cross-state variation for each category in this aggregation experiment. Same category order as the weight figure.",
            rows=complete_rows,
            metric_field="r2",
            fixed_range=(0.0, 1.0),
            color=BLUE,
            value_formatter=lambda value: f"{value:.3f}",
        )
        delta_beta_fig = write_ranked_metric_chart(
            figdir / f"{family.key}_delta_beta_rank.svg",
            title=f"{family.label}: leave-one-out influence on aggregate β",
            subtitle="Positive values mean the category pushes the aggregate state slope upward. Negative values mean it pulls the coarse-support slope downward.",
            rows=complete_rows,
            metric_field="delta_beta",
            symmetric=True,
            ref_line=0.0,
            color=TEAL,
            value_formatter=lambda value: f"{value:+.3f}",
        )
        delta_r2_fig = write_ranked_metric_chart(
            figdir / f"{family.key}_delta_r2_rank.svg",
            title=f"{family.label}: leave-one-out influence on aggregate R²",
            subtitle="Positive values mean the category supports aggregate fit at state support. Negative values mean removing it improves the coarse-support fit.",
            rows=complete_rows,
            metric_field="delta_r2",
            symmetric=True,
            ref_line=0.0,
            color=BLUE,
            value_formatter=lambda value: f"{value:+.3f}",
        )
        corr_size_fig = write_ranked_metric_chart(
            figdir / f"{family.key}_corr_log_population_rank.svg",
            title=f"{family.label}: correlation of category share with state size",
            subtitle="Correlation between category share and log population. Positive means larger states allocate more share to that category under coarse aggregation.",
            rows=complete_rows,
            metric_field="corr_log_population_share",
            fixed_range=(-1.0, 1.0),
            ref_line=0.0,
            color=RUST,
            value_formatter=lambda value: f"{value:+.3f}",
        )
        corr_resid_fig = write_ranked_metric_chart(
            figdir / f"{family.key}_corr_residual_rank.svg",
            title=f"{family.label}: correlation of category share with aggregate residual",
            subtitle="Positive means states above the aggregate fit line have more of that category. Negative means they have less under the state aggregation experiment.",
            rows=complete_rows,
            metric_field="corr_residual_share",
            fixed_range=(-1.0, 1.0),
            ref_line=0.0,
            color=RUST,
            value_formatter=lambda value: f"{value:+.3f}",
        )
        manifest_rows.extend(
            [
                {"figure_id": f"{family.key}_share_rank", "path": str(share_fig.resolve()), "description": f"{family.label} share of aggregate Y by category."},
                {"figure_id": f"{family.key}_beta_rank", "path": str(beta_fig.resolve()), "description": f"{family.label} category-specific beta with CI."},
                {"figure_id": f"{family.key}_r2_rank", "path": str(r2_fig.resolve()), "description": f"{family.label} category-specific R2."},
                {"figure_id": f"{family.key}_delta_beta_rank", "path": str(delta_beta_fig.resolve()), "description": f"{family.label} leave-one-out influence on aggregate beta."},
                {"figure_id": f"{family.key}_delta_r2_rank", "path": str(delta_r2_fig.resolve()), "description": f"{family.label} leave-one-out influence on aggregate R2."},
                {"figure_id": f"{family.key}_corr_log_population_rank", "path": str(corr_size_fig.resolve()), "description": f"{family.label} correlation of category share with state size."},
                {"figure_id": f"{family.key}_corr_residual_rank", "path": str(corr_resid_fig.resolve()), "description": f"{family.label} correlation of category share with aggregate residual."},
            ]
        )
        model_comparison_rows.extend(result["pooled_summary"])

        pooled = result["pooled_summary"][0]
        top_share, low_share = _extreme_rows(complete_rows, "share_of_total", n=3)
        top_beta, low_beta = _extreme_rows(complete_rows, "beta", n=3)
        top_r2, low_r2 = _extreme_rows(complete_rows, "r2", n=3)
        top_delta_beta, _ = _extreme_rows(complete_rows, "delta_beta", n=3, absolute=True)
        top_delta_r2, _ = _extreme_rows(complete_rows, "delta_r2", n=3, absolute=True)
        top_corr_size, _ = _extreme_rows(complete_rows, "corr_log_population_share", n=3, absolute=True)
        top_corr_resid, _ = _extreme_rows(complete_rows, "corr_residual_share", n=3, absolute=True)
        pos_delta_beta, neg_delta_beta = _extreme_rows(complete_rows, "delta_beta", n=2)
        pos_delta_r2, neg_delta_r2 = _extreme_rows(complete_rows, "delta_r2", n=2)
        pos_corr_size, neg_corr_size = _extreme_rows(complete_rows, "corr_log_population_share", n=2)
        pos_corr_resid, neg_corr_resid = _extreme_rows(complete_rows, "corr_residual_share", n=2)
        reject_common_slope = _to_float(pooled["p_value"], 1.0) < 0.05
        if _to_float(neg_delta_r2[0]["delta_r2"]) < 0:
            delta_r2_line = (
                f"- `delta_r2_rank`: the category that most supports aggregate fit is `{pos_delta_r2[0]['category_label']}` with `ΔR² = {_to_float(pos_delta_r2[0]['delta_r2']):+.4f}`; "
                f"the category whose removal improves fit most is `{neg_delta_r2[0]['category_label']}` with `ΔR² = {_to_float(neg_delta_r2[0]['delta_r2']):+.4f}`."
            )
        else:
            delta_r2_line = (
                f"- `delta_r2_rank`: the category that most supports aggregate fit is `{pos_delta_r2[0]['category_label']}` with `ΔR² = {_to_float(pos_delta_r2[0]['delta_r2']):+.4f}`; "
                f"the least supportive category is `{neg_delta_r2[0]['category_label']}` with `ΔR² = {_to_float(neg_delta_r2[0]['delta_r2']):+.4f}`."
            )
        report_lines.extend(
            [
                f"## {family.label}",
                "",
                f"- aggregate baseline OLS beta: `{float(result['baseline_fit'][0]['beta']):.4f}`",
                f"- aggregate baseline OLS R²: `{float(result['baseline_fit'][0]['r2']):.4f}`",
                f"- common-slope adjusted R²: `{float(pooled['common_adj_r2']):.4f}`",
                f"- varying-slope adjusted R²: `{float(pooled['varying_adj_r2']):.4f}`",
                f"- nested-model F statistic: `{float(pooled['f_stat']):.2f}` with df `({pooled['df_num']}, {pooled['df_den']})`, p-value `{pooled['p_value']}`",
                "",
                "### F-Test Reading",
                "",
                "Null hypothesis: after allowing category-specific intercepts, all categories share one common slope with population.",
                f"Decision for this family: `{'reject common slope' if reject_common_slope else 'do not reject common slope'}`.",
                (
                    f"For {family.label}, the p-value `{pooled['p_value']}` is below `0.05`, so the data support slope heterogeneity across categories."
                    if reject_common_slope
                    else f"For {family.label}, the p-value `{pooled['p_value']}` is far above `0.05`, so the state data do not support different slopes across categories once intercept differences are allowed."
                ),
                "",
                "### Figure Guide",
                "",
                f"- [{family.key}_share_rank.svg]({(figdir / f'{family.key}_share_rank.svg').resolve()}): who is numerically capable of dominating aggregate `Y`.",
                f"- [{family.key}_beta_rank.svg]({(figdir / f'{family.key}_beta_rank.svg').resolve()}): category scaling exponents only. Read each category relative to the `β = 1` reference line.",
                f"- [{family.key}_r2_rank.svg]({(figdir / f'{family.key}_r2_rank.svg').resolve()}): fit strength only. No `β` is mixed into this view.",
                f"- [{family.key}_delta_beta_rank.svg]({(figdir / f'{family.key}_delta_beta_rank.svg').resolve()}): how each category changes the aggregate slope when removed.",
                f"- [{family.key}_delta_r2_rank.svg]({(figdir / f'{family.key}_delta_r2_rank.svg').resolve()}): how each category changes aggregate fit quality when removed.",
                f"- [{family.key}_corr_log_population_rank.svg]({(figdir / f'{family.key}_corr_log_population_rank.svg').resolve()}): whether larger states shift their composition toward or away from the category.",
                f"- [{family.key}_corr_residual_rank.svg]({(figdir / f'{family.key}_corr_residual_rank.svg').resolve()}): whether over-performing or under-performing states are compositionally associated with the category.",
                "",
                "### Figure-By-Figure Reading",
                "",
                f"- `share_rank`: the dominant weights are `{top_share[0]['category_label']}` at `{_pct(_to_float(top_share[0]['share_of_total']))}`, `{top_share[1]['category_label']}` at `{_pct(_to_float(top_share[1]['share_of_total']))}`, and `{top_share[2]['category_label']}` at `{_pct(_to_float(top_share[2]['share_of_total']))}`.",
                f"- `beta_rank`: the highest exponent is `{top_beta[0]['category_label']}` with `β = {_to_float(top_beta[0]['beta']):.4f}`; the lowest exponent is `{low_beta[0]['category_label']}` with `β = {_to_float(low_beta[0]['beta']):.4f}`.",
                f"- `r2_rank`: the strongest fit is `{top_r2[0]['category_label']}` with `R² = {_to_float(top_r2[0]['r2']):.4f}`; the weakest is `{low_r2[0]['category_label']}` with `R² = {_to_float(low_r2[0]['r2']):.4f}`.",
                f"- `delta_beta_rank`: the strongest upward slope driver is `{pos_delta_beta[0]['category_label']}` with `Δβ = {_to_float(pos_delta_beta[0]['delta_beta']):+.4f}`; the strongest downward slope driver is `{neg_delta_beta[0]['category_label']}` with `Δβ = {_to_float(neg_delta_beta[0]['delta_beta']):+.4f}`.",
                delta_r2_line,
                f"- `corr_log_population_rank`: larger states shift most toward `{pos_corr_size[0]['category_label']}` with correlation `{_to_float(pos_corr_size[0]['corr_log_population_share']):+.3f}` and most away from `{neg_corr_size[0]['category_label']}` with correlation `{_to_float(neg_corr_size[0]['corr_log_population_share']):+.3f}`.",
                f"- `corr_residual_rank`: states above the aggregate fit line are most associated with `{pos_corr_resid[0]['category_label']}` with correlation `{_to_float(pos_corr_resid[0]['corr_residual_share']):+.3f}` and most negatively associated with `{neg_corr_resid[0]['category_label']}` with correlation `{_to_float(neg_corr_resid[0]['corr_residual_share']):+.3f}`.",
                "",
                "### Strongest Weights",
                "",
            ]
        )
        for row in top_share:
            report_lines.append(f"- `{row['category_label']}`: share `{_pct(_to_float(row['share_of_total']))}`, `β = {_to_float(row['beta']):.4f}`, `R² = {_to_float(row['r2']):.4f}`")
        report_lines.extend(["", "### Theory-Aware Reading Of β", ""])
        for row in top_share[:3]:
            beta = _to_float(row["beta"])
            report_lines.append(
                f"- `{row['category_label']}`: `{_beta_regime(beta)}` with `β = {beta:.4f}`. "
                f"For count-like `Y`, this implies `Y/N ~ N^({beta-1:+.4f})`."
            )
        report_lines.extend(["", "### Highest And Lowest β", ""])
        for row in top_beta:
            report_lines.append(f"- high: `{row['category_label']}` with `β = {_to_float(row['beta']):.4f}`, `R² = {_to_float(row['r2']):.4f}`")
        for row in low_beta:
            report_lines.append(f"- low: `{row['category_label']}` with `β = {_to_float(row['beta']):.4f}`, `R² = {_to_float(row['r2']):.4f}`")
        report_lines.extend(["", "### Highest And Lowest R²", ""])
        for row in top_r2:
            report_lines.append(f"- high: `{row['category_label']}` with `R² = {_to_float(row['r2']):.4f}`, `β = {_to_float(row['beta']):.4f}`")
        for row in low_r2:
            report_lines.append(f"- low: `{row['category_label']}` with `R² = {_to_float(row['r2']):.4f}`, `β = {_to_float(row['beta']):.4f}`")
        report_lines.extend(["", "### Strongest Leave-One-Out Effects", ""])
        for row in top_delta_beta:
            report_lines.append(f"- `|Δβ|`: `{row['category_label']}` with `Δβ = {_to_float(row['delta_beta']):+.4f}`, share `{_pct(_to_float(row['share_of_total']))}`")
        for row in top_delta_r2:
            report_lines.append(f"- `|ΔR²|`: `{row['category_label']}` with `ΔR² = {_to_float(row['delta_r2']):+.4f}`, share `{_pct(_to_float(row['share_of_total']))}`")
        report_lines.extend(["", "### Strongest Composition Correlations", ""])
        for row in top_corr_size:
            report_lines.append(f"- `corr(share, log N)`: `{row['category_label']}` = `{_to_float(row['corr_log_population_share']):+.3f}`")
        for row in top_corr_resid:
            report_lines.append(f"- `corr(share, residual)`: `{row['category_label']}` = `{_to_float(row['corr_residual_share']):+.3f}`")
        report_lines.extend(
            [
                "",
                "### Full Comparison Table",
                "",
                f"CSV: [{family.key}_complete_statistics.csv]({(outdir / f'{family.key}_complete_statistics.csv').resolve()})",
                "",
            ]
        )
        report_lines.extend(_markdown_complete_table(complete_rows))
        report_lines.extend(
            [
                "",
                f"Supporting tables: [{family.key}_category_fits.csv]({(outdir / f'{family.key}_category_fits.csv').resolve()}), [{family.key}_leave_one_out.csv]({(outdir / f'{family.key}_leave_one_out.csv').resolve()}), [{family.key}_composition_stats.csv]({(outdir / f'{family.key}_composition_stats.csv').resolve()}), [{family.key}_pooled_model_comparison.csv]({(outdir / f'{family.key}_pooled_model_comparison.csv').resolve()})",
                "",
            ]
        )

    comparison_fig = write_model_comparison(figdir / "pooled_model_comparison.svg", title="Category heterogeneity in the state aggregation experiment", rows=model_comparison_rows)
    manifest_rows.append({"figure_id": "pooled_model_comparison", "path": str(comparison_fig.resolve()), "description": "Nested model comparison for common vs varying category slopes."})

    # state maps
    residual_rows = _query_tsv(
        f"""
        SELECT state_code, aggregate_log_residual::text
        FROM (SELECT state_code, aggregate_log_residual FROM state_total_residuals_view) t
        """.strip(),
        ["state_code", "aggregate_log_residual"],
    ) if False else []
    # load from written csv instead
    residual_csv = outdir / "state_total_residuals.csv"
    residual_table = list(csv.DictReader(residual_csv.open("r", encoding="utf-8", newline="")))
    residual_map = {row["state_code"]: _to_float(row["aggregate_log_residual"]) for row in residual_table}
    residual_fig = write_state_map(
        figdir / "state_total_residual_map.svg",
        title="State map: aggregate residual",
        subtitle="Residual of state-level total-establishment fit",
        value_map=residual_map,
        diverging=True,
    )
    manifest_rows.append({"figure_id": "state_total_residual_map", "path": str(residual_fig.resolve()), "description": "State residual choropleth for aggregate Y."})

    scian_share_rows = list(csv.DictReader((outdir / "scian2_state_shares.csv").open("r", encoding="utf-8", newline="")))
    share46 = {row["state_code"]: _to_float(row["share"]) for row in scian_share_rows if row["category"] == "46"}
    share46_fig = write_state_map(
        figdir / "state_share_scian2_46_map.svg",
        title="State map: retail share in total Y",
        subtitle="SCIAN 46 share of total establishments by state",
        value_map=share46,
        diverging=False,
    )
    manifest_rows.append({"figure_id": "state_share_scian2_46_map", "path": str(share46_fig.resolve()), "description": "State choropleth of retail share in total Y."})

    per_share_rows = list(csv.DictReader((outdir / "per_ocu_state_shares.csv").open("r", encoding="utf-8", newline="")))
    share_micro = {row["state_code"]: _to_float(row["share"]) for row in per_share_rows if row["category"] == "0 a 5 personas"}
    share_micro_fig = write_state_map(
        figdir / "state_share_micro_map.svg",
        title="State map: micro-establishment share in total Y",
        subtitle="`per_ocu = 0 a 5 personas` share of total establishments by state",
        value_map=share_micro,
        diverging=False,
    )
    manifest_rows.append({"figure_id": "state_share_micro_map", "path": str(share_micro_fig.resolve()), "description": "State choropleth of micro-establishment share in total Y."})

    _write_csv(figdir / "figures_manifest.csv", manifest_rows, ["figure_id", "path", "description"])
    (outdir / "report.md").write_text("\n".join(report_lines), encoding="utf-8")
    print(json.dumps({"ok": True, "output_dir": str(outdir), "figure_count": len(manifest_rows)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

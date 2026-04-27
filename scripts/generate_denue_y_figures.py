#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
from pathlib import Path
import subprocess
import urllib.request

from urban_sami.artifacts.figures import BG, PANEL, GRID, AXIS, TEXT, MUTED, TEAL, BLUE, RUST, GOLD, SERIF, SANS


ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "reports" / "denue-y-native-experiments-2026-04-21"
STATE_GEOJSON = Path("/home/hadox/cmd-center/platforms/si/app-hadox/src/static/geo/mx_admin1.geojson")
DOCKER_EXE = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DB_CONTAINER = "24-polisplexity-core-db-dev"
POSTGRES_USER = "postgres"
DB_NAME = "urban_sami_exp"


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


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
    rows = []
    for line in proc.stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        rows.append({col: (parts[idx] if idx < len(parts) else "") for idx, col in enumerate(columns)})
    return rows


def _write_svg(path: Path, body: str, width: int, height: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _fmt(v: float) -> str:
    if abs(v) >= 1000:
        return f"{v:,.0f}"
    return f"{v:.3f}"


def _to_float(v: object, default: float = 0.0) -> float:
    try:
        return float(str(v).strip())
    except Exception:
        return default


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


def _color_scale(value: float, vmax: float) -> str:
    if vmax <= 0:
        return "#d9d2c2"
    t = max(0.0, min(1.0, value / vmax))
    # warm monochrome scientific palette
    r = int(236 - (102 * t))
    g = int(226 - (118 * t))
    b = int(206 - (154 * t))
    return f"#{r:02x}{g:02x}{b:02x}"


def write_state_choropleth(path: Path, *, title: str, value_map: dict[str, float], subtitle: str) -> Path:
    width, height = 980, 660
    box = (60, 110, 620, 500)
    features, bounds = _state_shapes()
    vmax = max(value_map.values()) if value_map else 0.0
    parts = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">{subtitle}</text>',
    ]
    for feat in features:
        code = feat["code"]
        val = value_map.get(code, 0.0)
        fill = _color_scale(val, vmax)
        geom = feat["geometry"]
        if geom.get("type") == "MultiPolygon":
            for poly in geom.get("coordinates", []):
                for ring in poly[:1]:
                    pts = _project_ring(ring, bounds, box)
                    parts.append(f'<polygon points="{pts}" fill="{fill}" stroke="#f8f6f1" stroke-width="0.8"/>')
        elif geom.get("type") == "Polygon":
            pts = _project_ring(geom.get("coordinates", [[]])[0], bounds, box)
            parts.append(f'<polygon points="{pts}" fill="{fill}" stroke="#f8f6f1" stroke-width="0.8"/>')
    # legend
    lx, ly, lw, lh = 740, 180, 26, 220
    for i in range(100):
        y = ly + (i * lh / 100.0)
        v = vmax * (1 - (i / 99.0))
        parts.append(f'<rect x="{lx}" y="{y:.2f}" width="{lw}" height="{lh/100.0+0.4:.2f}" fill="{_color_scale(v, vmax)}" stroke="none"/>')
    parts.append(f'<rect x="{lx}" y="{ly}" width="{lw}" height="{lh}" fill="none" stroke="{AXIS}"/>')
    parts.append(f'<text x="{lx+lw+16}" y="{ly+6}" font-size="11" font-family="{SANS}" fill="{MUTED}">{_fmt(vmax)}</text>')
    parts.append(f'<text x="{lx+lw+16}" y="{ly+lh}" font-size="11" font-family="{SANS}" fill="{MUTED}">0</text>')
    return _write_svg(path, "".join(parts), width, height)


def write_bar_distribution(path: Path, *, title: str, subtitle: str, rows: list[dict], label_key: str, value_key: str, max_bars: int = 12) -> Path:
    rows = rows[:max_bars]
    width, height = 980, 620
    left, right, top, bottom = 260, 50, 100, 70
    plot_w = width - left - right
    plot_h = height - top - bottom
    vmax = max(_to_float(r[value_key]) for r in rows) if rows else 1.0
    bar_h = plot_h / max(1, len(rows))
    parts = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">{subtitle}</text>',
    ]
    for i in range(5):
        x = left + (plot_w * i / 4.0)
        v = vmax * i / 4.0
        parts.append(f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top+plot_h}" stroke="{GRID}" />')
        parts.append(f'<text x="{x:.2f}" y="{height-18}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">{_fmt(v)}</text>')
    for idx, row in enumerate(rows):
        y = top + (idx * bar_h) + 8
        val = _to_float(row[value_key])
        w = (val / vmax) * plot_w if vmax else 0
        label = str(row[label_key])
        parts.append(f'<text x="{left-12}" y="{y+12:.2f}" text-anchor="end" font-size="13" font-family="{SANS}" fill="{TEXT}">{label}</text>')
        parts.append(f'<rect x="{left}" y="{y:.2f}" width="{w:.2f}" height="{bar_h-12:.2f}" fill="{BLUE}" rx="4"/>')
        parts.append(f'<text x="{left+w+8:.2f}" y="{y+12:.2f}" font-size="12" font-family="{SANS}" fill="{MUTED}">{_fmt(val)}</text>')
    return _write_svg(path, "".join(parts), width, height)


def write_level_comparison(path: Path, *, rows: list[dict], title: str) -> Path:
    width, height = 1120, 680
    left, right, top, bottom = 240, 70, 110, 80
    plot_w = width - left - right
    plot_h = height - top - bottom
    levels = ["state", "city", "ageb_u_top20"]
    ys = [row["y_label"] for row in rows]
    beta_min = min(_to_float(r["beta"]) for r in rows)
    beta_max = max(_to_float(r["beta"]) for r in rows)
    beta_min -= 0.08
    beta_max += 0.08

    def bx(v: float) -> float:
        return left + ((v - beta_min) / (beta_max - beta_min)) * (plot_w * 0.55)

    def rx(v: float) -> float:
        x0 = left + (plot_w * 0.66)
        x1 = left + plot_w
        return x0 + max(0.0, min(1.0, v)) * (x1 - x0)

    row_h = plot_h / max(1, len(rows))
    parts = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Best fit by level for selected native DENUE Y definitions</text>',
        f'<text x="{left}" y="95" font-size="14" font-family="{SANS}" fill="{MUTED}">β</text>',
        f'<text x="{left + plot_w*0.66:.2f}" y="95" font-size="14" font-family="{SANS}" fill="{MUTED}">R²</text>',
    ]
    for i in range(5):
        val = beta_min + ((beta_max - beta_min) * i / 4.0)
        x = bx(val)
        parts.append(f'<line x1="{x:.2f}" y1="{top-10}" x2="{x:.2f}" y2="{top+plot_h}" stroke="{GRID}"/>')
        parts.append(f'<text x="{x:.2f}" y="{height-26}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">{val:.2f}</text>')
    for i in range(6):
        val = i / 5.0
        x = rx(val)
        parts.append(f'<line x1="{x:.2f}" y1="{top-10}" x2="{x:.2f}" y2="{top+plot_h}" stroke="{GRID}"/>')
        parts.append(f'<text x="{x:.2f}" y="{height-26}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">{val:.1f}</text>')
    for idx, row in enumerate(rows):
        y = top + ((idx + 0.5) * row_h)
        level = row["level"]
        color = {"state": GOLD, "city": TEAL, "ageb_u_top20": BLUE}.get(level, BLUE)
        parts.append(f'<text x="{left-14}" y="{y+4:.2f}" text-anchor="end" font-size="13" font-family="{SANS}" fill="{TEXT}">{row["y_label"]} [{level}]</text>')
        parts.append(f'<line x1="{left-8}" y1="{y:.2f}" x2="{left+plot_w:.2f}" y2="{y:.2f}" stroke="#efe8db"/>')
        parts.append(f'<circle cx="{bx(_to_float(row["beta"])):.2f}" cy="{y:.2f}" r="5.4" fill="{color}"/>')
        parts.append(f'<circle cx="{rx(_to_float(row["r2"])):.2f}" cy="{y:.2f}" r="5.4" fill="{color}"/>')
        parts.append(f'<text x="{left+plot_w+8:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{MUTED}">{row["fit_method"]}, n={row["n_obs"]}</text>')
    return _write_svg(path, "".join(parts), width, height)


def write_city_sample_map(path: Path, *, title: str, value_map: dict[str, float], city_rows: list[dict[str, str]]) -> Path:
    points = []
    for row in city_rows:
        code = row["city_code"]
        if code not in value_map:
            continue
        points.append(
            {
                "code": code,
                "name": row["city_name"],
                "lon": _to_float(row["avg_lon"]),
                "lat": _to_float(row["avg_lat"]),
                "value": value_map[code],
            }
        )
    minx = min(p["lon"] for p in points)
    maxx = max(p["lon"] for p in points)
    miny = min(p["lat"] for p in points)
    maxy = max(p["lat"] for p in points)
    width, height = 980, 680
    box = (60, 120, 640, 500)
    vmax = max(value_map.values()) if value_map else 0.0
    parts = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Top-20 city sample, bubble map from DENUE city centroids</text>',
    ]
    parts.append(f'<rect x="{box[0]}" y="{box[1]}" width="{box[2]}" height="{box[3]}" fill="#fcfaf6" stroke="{GRID}"/>')
    ranked = []
    for pt in points:
        ranked.append((pt["name"], pt["value"]))
        x = box[0] + ((pt["lon"] - minx) / (maxx - minx)) * box[2]
        y = box[1] + ((maxy - pt["lat"]) / (maxy - miny)) * box[3]
        radius = 4 + (math.sqrt(pt["value"] / vmax) * 14 if vmax else 4)
        parts.append(f'<circle cx="{x:.2f}" cy="{y:.2f}" r="{radius:.2f}" fill="{TEAL}" fill-opacity="0.35" stroke="{TEAL}" stroke-width="1.2"/>')
        if pt["value"] >= sorted(value_map.values(), reverse=True)[7]:
            parts.append(f'<text x="{x+radius+4:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">{pt["name"]}</text>')
    ranked.sort(key=lambda t: t[1], reverse=True)
    y = 130
    parts.append(f'<text x="740" y="120" font-size="13" font-family="{SANS}" fill="{MUTED}">Top cities in sample</text>')
    for name, val in ranked[:12]:
        parts.append(f'<text x="740" y="{y}" font-size="12" font-family="{SANS}" fill="{TEXT}">{name}</text>')
        parts.append(f'<text x="930" y="{y}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{MUTED}">{_fmt(val)}</text>')
        y += 22
    return _write_svg(path, "".join(parts), width, height)


def main() -> int:
    outdir = REPORT_DIR / "figures"
    outdir.mkdir(parents=True, exist_ok=True)

    per_ocu = _read_csv(REPORT_DIR / "per_ocu_distribution.csv")
    scian2 = _read_csv(REPORT_DIR / "scian2_distribution.csv")
    best = _read_csv(REPORT_DIR / "y_experiment_best_fits.csv")
    city_sample = _read_csv(ROOT / "data" / "raw" / "city_samples" / "top20_population.csv")
    city_codes = [row["city_code"] for row in city_sample]
    city_centroids = _query_tsv(
        f"""
        SELECT city_code, MAX(city_name) AS city_name, AVG(longitude)::text AS avg_lon, AVG(latitude)::text AS avg_lat
        FROM raw.denue_establishments
        WHERE city_code IN ({",".join(repr(c) for c in city_codes)})
          AND longitude IS NOT NULL
          AND latitude IS NOT NULL
        GROUP BY city_code
        ORDER BY city_code
        """.strip(),
        ["city_code", "city_name", "avg_lon", "avg_lat"],
    )

    write_bar_distribution(outdir / "per_ocu_distribution.svg", title="DENUE size-band distribution", subtitle="Native establishment-size categories from `per_ocu`", rows=per_ocu, label_key="per_ocu", value_key="establishments")
    write_bar_distribution(outdir / "scian2_distribution.svg", title="DENUE SCIAN 2-digit distribution", subtitle="Most frequent native sector groups in loaded DENUE", rows=scian2, label_key="scian2", value_key="establishments")

    selected_keys = ["all", "per_ocu::0_a_5_personas", "scian2::46", "scian2::81", "scian2::72"]
    selected_rows = [row for row in best if row["y_key"] in selected_keys]
    selected_rows.sort(key=lambda r: (selected_keys.index(r["y_key"]), ["state", "city", "ageb_u_top20"].index(r["level"])))
    write_level_comparison(outdir / "y_level_comparison.svg", rows=selected_rows, title="Native Y definitions across scales")

    state_queries = {
        "all": "TRUE",
        "micro": "per_ocu = '0 a 5 personas'",
        "scian2_46": "SUBSTRING(scian_code FROM 1 FOR 2) = '46'",
    }
    for key, clause in state_queries.items():
        rows = _query_tsv(
            f"""
            SELECT state_code, COUNT(*)::text AS est_count
            FROM raw.denue_establishments
            WHERE state_code <> '' AND {clause}
            GROUP BY state_code
            ORDER BY state_code
            """.strip(),
            ["state_code", "est_count"],
        )
        title = {
            "all": "State map: all establishments",
            "micro": "State map: micro establishments",
            "scian2_46": "State map: retail establishments (SCIAN 46)",
        }[key]
        subtitle = {
            "all": "Raw establishment counts by state",
            "micro": "Counts restricted to `per_ocu = 0 a 5 personas`",
            "scian2_46": "Counts restricted to SCIAN 2-digit 46",
        }[key]
        write_state_choropleth(outdir / f"state_map_{key}.svg", title=title, subtitle=subtitle, value_map={r["state_code"]: _to_float(r["est_count"]) for r in rows})

    city_queries = {
        "all": "TRUE",
        "micro": "per_ocu = '0 a 5 personas'",
        "scian2_46": "SUBSTRING(scian_code FROM 1 FOR 2) = '46'",
    }
    for key, clause in city_queries.items():
        rows = _query_tsv(
            f"""
            SELECT city_code, COUNT(*)::text AS est_count
            FROM raw.denue_establishments
            WHERE city_code IN ({",".join(repr(c) for c in city_codes)}) AND {clause}
            GROUP BY city_code
            ORDER BY city_code
            """.strip(),
            ["city_code", "est_count"],
        )
        title = {
            "all": "City sample map: all establishments",
            "micro": "City sample map: micro establishments",
            "scian2_46": "City sample map: retail establishments (SCIAN 46)",
        }[key]
        write_city_sample_map(outdir / f"city_map_top20_{key}.svg", title=title, value_map={r["city_code"]: _to_float(r["est_count"]) for r in rows}, city_rows=city_centroids)

    manifest = [
        {"figure_id": "per_ocu_distribution", "path": str((outdir / "per_ocu_distribution.svg").resolve()), "description": "Distribution of DENUE establishments by size band."},
        {"figure_id": "scian2_distribution", "path": str((outdir / "scian2_distribution.svg").resolve()), "description": "Distribution of DENUE establishments by SCIAN 2-digit code."},
        {"figure_id": "y_level_comparison", "path": str((outdir / "y_level_comparison.svg").resolve()), "description": "Best-fit beta and R2 across levels for selected native Y definitions."},
        {"figure_id": "state_map_all", "path": str((outdir / "state_map_all.svg").resolve()), "description": "State choropleth of raw establishment counts."},
        {"figure_id": "state_map_micro", "path": str((outdir / "state_map_micro.svg").resolve()), "description": "State choropleth of micro-establishment counts."},
        {"figure_id": "state_map_scian2_46", "path": str((outdir / "state_map_scian2_46.svg").resolve()), "description": "State choropleth of retail-establishment counts."},
        {"figure_id": "city_map_top20_all", "path": str((outdir / "city_map_top20_all.svg").resolve()), "description": "Top-20 city sample map of raw establishment counts."},
        {"figure_id": "city_map_top20_micro", "path": str((outdir / "city_map_top20_micro.svg").resolve()), "description": "Top-20 city sample map of micro-establishment counts."},
        {"figure_id": "city_map_top20_scian2_46", "path": str((outdir / "city_map_top20_scian2_46.svg").resolve()), "description": "Top-20 city sample map of retail-establishment counts."},
    ]
    with (outdir / "figures_manifest.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["figure_id", "path", "description"])
        writer.writeheader()
        writer.writerows(manifest)
    print(json.dumps({"ok": True, "output_dir": str(outdir), "figure_count": len(manifest)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

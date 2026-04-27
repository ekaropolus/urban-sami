#!/usr/bin/env python3
from __future__ import annotations

import csv
import html
import json
import subprocess
from pathlib import Path


ROOT = Path("/home/hadox/cmd-center/platforms/research/urban-sami")
COMPARISON_DIR = ROOT / "reports" / "city-y-sami-comparison-pack-2026-04-21"
MAP_DIR = ROOT / "reports" / "city-map-pack-2026-04-22"
RAW_GEO_DIR = ROOT / "data" / "raw" / "inegi_municipal_geojson"
OUTPUT_DIR = ROOT / "reports" / "city-state-map-pack-2026-04-22"
RSVG_CONVERT = "/usr/bin/rsvg-convert"

BG = "#f8f6f1"
PANEL = "#fffdf8"
GRID = "#ddd6c8"
TEXT = "#1f1f1f"
MUTED = "#625d54"
NEUTRAL = "#e9e3d8"

STATE_NAMES = {
    "01": "Aguascalientes", "02": "Baja California", "03": "Baja California Sur", "04": "Campeche",
    "05": "Coahuila", "06": "Colima", "07": "Chiapas", "08": "Chihuahua", "09": "Ciudad de México",
    "10": "Durango", "11": "Guanajuato", "12": "Guerrero", "13": "Hidalgo", "14": "Jalisco",
    "15": "México", "16": "Michoacán", "17": "Morelos", "18": "Nayarit", "19": "Nuevo León",
    "20": "Oaxaca", "21": "Puebla", "22": "Querétaro", "23": "Quintana Roo", "24": "San Luis Potosí",
    "25": "Sinaloa", "26": "Sonora", "27": "Tabasco", "28": "Tamaulipas", "29": "Tlaxcala",
    "30": "Veracruz", "31": "Yucatán", "32": "Zacatecas",
}


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


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _fmt_num(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}"


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


def _collect_bounds(features: list[dict]) -> tuple[float, float, float, float]:
    min_x = min_y = float("inf")
    max_x = max_y = float("-inf")
    for feature in features:
        for ring in _flatten_coords(feature.get("geometry", {})):
            for x, y in ring:
                min_x = min(min_x, x)
                max_x = max(max_x, x)
                min_y = min(min_y, y)
                max_y = max(max_y, y)
    return min_x, min_y, max_x, max_y


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


def _write_svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _write_state_map(state_code: str, features: list[dict], value_lookup: dict[str, float], path: Path) -> Path:
    width = 1280
    height = 920
    map_x = 48
    map_y = 110
    map_w = 900
    map_h = 740
    bounds = _collect_bounds(features)
    project = _projector(bounds, map_w, map_h, 16)
    state_name = STATE_NAMES.get(state_code, state_code)

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="28" font-family="Georgia, \'Times New Roman\', serif" fill="{TEXT}">{html.escape(state_name)}: total city SAMI</text>',
        f'<text x="44" y="76" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">Municipal choropleth within state {state_code}. Same city-level scaling law as the national municipal run.</text>',
        f'<g transform="translate({map_x},{map_y})">',
        f'<rect x="0" y="0" width="{map_w}" height="{map_h}" fill="#faf8f2" stroke="{GRID}"/>',
    ]
    values = []
    for feature in features:
        city_code = str((feature.get("properties") or {}).get("cvegeo", "")).strip()
        value = value_lookup.get(city_code)
        values.append(value)
        fill = _diverging_fill(value)
        path_data = _svg_path_from_geometry(feature.get("geometry", {}), project)
        body.append(f'<path d="{path_data}" fill="{fill}" stroke="#ffffff" stroke-width="0.35"/>')
    body.append("</g>")

    legend_x = 1010
    legend_y = 190
    body.append(f'<text x="{legend_x}" y="{legend_y-28}" font-size="13" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">Municipal SAMI</text>')
    for idx, val in enumerate([-2.0, -1.0, 0.0, 1.0, 2.0]):
        y = legend_y + idx * 28
        label = "near expected" if val == 0 else _fmt_num(val, 1)
        body.append(f'<rect x="{legend_x}" y="{y}" width="38" height="18" rx="3" fill="{_diverging_fill(val)}" stroke="#ffffff"/>')
        body.append(f'<text x="{legend_x+52}" y="{y+13}" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{label}</text>')
    valid = [v for v in values if v is not None]
    body.append(f'<text x="{legend_x}" y="{legend_y+170}" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">n municipalities = {len(features)}</text>')
    if valid:
        body.append(f'<text x="{legend_x}" y="{legend_y+194}" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">min = {_fmt_num(min(valid),2)} | max = {_fmt_num(max(valid),2)}</text>')
    return _write_svg(path, width, height, "".join(body))


def _convert_svg_to_png(svg_path: Path, png_path: Path) -> None:
    subprocess.run([RSVG_CONVERT, "-o", str(png_path), str(svg_path)], check=True)


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "figures").mkdir(parents=True, exist_ok=True)

    city_summary = _read_csv(COMPARISON_DIR / "city_summary.csv")
    total_lookup = {row["city_code"]: _to_float(row["total_sami"]) for row in city_summary}

    manifest_rows: list[dict[str, str]] = []

    national_svg = MAP_DIR / "figures" / "01_total_sami_map.svg"
    national_png = OUTPUT_DIR / "figures" / "01_total_sami_map.png"
    if national_svg.exists():
        _convert_svg_to_png(national_svg, national_png)
        manifest_rows.append({"scope": "national", "state_code": "", "figure": national_png.name, "format": "png"})

    for idx in range(1, 33):
        state_code = f"{idx:02d}"
        geo_path = RAW_GEO_DIR / f"{state_code}.geojson"
        doc = json.loads(geo_path.read_text(encoding="utf-8"))
        features = doc.get("features", [])
        svg_path = OUTPUT_DIR / "figures" / f"state_{state_code}_total_sami_map.svg"
        png_path = OUTPUT_DIR / "figures" / f"state_{state_code}_total_sami_map.png"
        _write_state_map(state_code, features, total_lookup, svg_path)
        _convert_svg_to_png(svg_path, png_path)
        manifest_rows.append({"scope": "state", "state_code": state_code, "figure": svg_path.name, "format": "svg"})
        manifest_rows.append({"scope": "state", "state_code": state_code, "figure": png_path.name, "format": "png"})

    _write_csv(OUTPUT_DIR / "figures_manifest.csv", manifest_rows, ["scope", "state_code", "figure", "format"])

    report_lines = [
        "# City State Map Pack",
        "",
        "This pack makes the national total-SAMI map lighter via PNG and adds one total-SAMI map per state.",
        "",
        f"- [figures_manifest.csv]({(OUTPUT_DIR / 'figures_manifest.csv').as_posix()})",
        f"- national PNG: [01_total_sami_map.png]({(OUTPUT_DIR / 'figures' / '01_total_sami_map.png').as_posix()})",
        "",
        "Examples:",
        f"- [state_09_total_sami_map.png]({(OUTPUT_DIR / 'figures' / 'state_09_total_sami_map.png').as_posix()})",
        f"- [state_14_total_sami_map.png]({(OUTPUT_DIR / 'figures' / 'state_14_total_sami_map.png').as_posix()})",
        f"- [state_20_total_sami_map.png]({(OUTPUT_DIR / 'figures' / 'state_20_total_sami_map.png').as_posix()})",
        f"- [state_21_total_sami_map.png]({(OUTPUT_DIR / 'figures' / 'state_21_total_sami_map.png').as_posix()})",
        f"- [state_30_total_sami_map.png]({(OUTPUT_DIR / 'figures' / 'state_30_total_sami_map.png').as_posix()})",
    ]
    (OUTPUT_DIR / "report.md").write_text("\n".join(report_lines), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

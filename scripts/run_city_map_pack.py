#!/usr/bin/env python3
from __future__ import annotations

import csv
import html
import json
import math
import urllib.request
from pathlib import Path


ROOT = Path("/home/hadox/cmd-center/platforms/research/urban-sami")
COMPARISON_DIR = ROOT / "reports" / "city-y-sami-comparison-pack-2026-04-21"
MULTI_DIR = ROOT / "reports" / "city-multi-criteria-pack-2026-04-22"
RAW_GEO_DIR = ROOT / "data" / "raw" / "inegi_municipal_geojson"
OUTPUT_DIR = ROOT / "reports" / "city-map-pack-2026-04-22"

BG = "#f8f6f1"
PANEL = "#fffdf8"
GRID = "#ddd6c8"
TEXT = "#1f1f1f"
MUTED = "#625d54"
AXIS = "#8b8478"
NEUTRAL = "#e9e3d8"
BASE_URL = "https://gaia.inegi.org.mx/wscatgeo/v2/geo/mgem/{state_code}"

MAP_DEFS = [
    ("01_total_sami_map", "Total establishments", "all"),
    ("02_micro_sami_map", "Micro establishments", "size_class::micro"),
    ("03_retail_sami_map", "Retail trade", "scian2::46"),
    ("04_professional_sami_map", "Professional services", "scian2::54"),
]


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


def _fetch_state_geojson(state_code: str) -> dict:
    RAW_GEO_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_GEO_DIR / f"{state_code}.geojson"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    url = BASE_URL.format(state_code=state_code)
    with urllib.request.urlopen(url, timeout=60) as response:
        payload = response.read().decode("utf-8")
    path.write_text(payload, encoding="utf-8")
    return json.loads(payload)


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


def _signal_fill(signal_class: str) -> str:
    return {
        "recurrent_upper": "#b14d3b",
        "recurrent_lower": "#2b6cb0",
        "high_positive": "#d97706",
        "high_negative": "#6b46c1",
        "other": "#ece6db",
    }.get(signal_class, "#ece6db")


def _write_svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _write_choropleth(
    title: str,
    subtitle: str,
    features: list[dict],
    value_lookup: dict[str, float | None],
    path: Path,
) -> Path:
    width = 1180
    height = 860
    map_x = 48
    map_y = 110
    map_w = 840
    map_h = 690
    bounds = _collect_bounds(features)
    project = _projector(bounds, map_w, map_h, 12)

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="28" font-family="Georgia, \'Times New Roman\', serif" fill="{TEXT}">{html.escape(title)}</text>',
        f'<text x="44" y="76" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{html.escape(subtitle)}</text>',
        f'<g transform="translate({map_x},{map_y})">',
        f'<rect x="0" y="0" width="{map_w}" height="{map_h}" fill="#faf8f2" stroke="{GRID}"/>',
    ]

    for feature in features:
        city_code = str((feature.get("properties") or {}).get("cvegeo", "")).strip()
        value = value_lookup.get(city_code)
        fill = _diverging_fill(value)
        path_data = _svg_path_from_geometry(feature.get("geometry", {}), project)
        body.append(f'<path d="{path_data}" fill="{fill}" stroke="#ffffff" stroke-width="0.25"/>')
    body.append("</g>")

    legend_x = 930
    legend_y = 190
    body.append(f'<text x="{legend_x}" y="{legend_y-28}" font-size="13" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">Municipal SAMI</text>')
    legend_vals = [-2.0, -1.0, 0.0, 1.0, 2.0]
    for idx, val in enumerate(legend_vals):
        y = legend_y + idx * 28
        body.append(f'<rect x="{legend_x}" y="{y}" width="38" height="18" rx="3" fill="{_diverging_fill(val)}" stroke="#ffffff"/>')
        label = "near expected" if val == 0 else _fmt_num(val, 1)
        body.append(f'<text x="{legend_x+52}" y="{y+13}" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{label}</text>')
    return _write_svg(path, width, height, "".join(body))


def _write_signal_map(
    title: str,
    subtitle: str,
    features: list[dict],
    class_lookup: dict[str, str],
    path: Path,
) -> Path:
    width = 1180
    height = 860
    map_x = 48
    map_y = 110
    map_w = 840
    map_h = 690
    bounds = _collect_bounds(features)
    project = _projector(bounds, map_w, map_h, 12)

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="28" font-family="Georgia, \'Times New Roman\', serif" fill="{TEXT}">{html.escape(title)}</text>',
        f'<text x="44" y="76" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{html.escape(subtitle)}</text>',
        f'<g transform="translate({map_x},{map_y})">',
        f'<rect x="0" y="0" width="{map_w}" height="{map_h}" fill="#faf8f2" stroke="{GRID}"/>',
    ]
    for feature in features:
        city_code = str((feature.get("properties") or {}).get("cvegeo", "")).strip()
        signal_class = class_lookup.get(city_code, "other")
        fill = _signal_fill(signal_class)
        path_data = _svg_path_from_geometry(feature.get("geometry", {}), project)
        body.append(f'<path d="{path_data}" fill="{fill}" stroke="#ffffff" stroke-width="0.25"/>')
    body.append("</g>")

    legend_x = 930
    legend_y = 190
    labels = [
        ("recurrent_upper", "recurrent upper-tail"),
        ("recurrent_lower", "recurrent lower-tail"),
        ("high_positive", "highest positive total SAMI"),
        ("high_negative", "lowest total SAMI"),
        ("other", "other"),
    ]
    for idx, (key, label) in enumerate(labels):
        y = legend_y + idx * 28
        body.append(f'<rect x="{legend_x}" y="{y}" width="38" height="18" rx="3" fill="{_signal_fill(key)}" stroke="#ffffff"/>')
        body.append(f'<text x="{legend_x+52}" y="{y+13}" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{label}</text>')
    return _write_svg(path, width, height, "".join(body))


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "figures").mkdir(parents=True, exist_ok=True)

    city_summary = _read_csv(COMPARISON_DIR / "city_summary.csv")
    city_scores = _read_csv(COMPARISON_DIR / "city_y_sami_long.csv")
    city_master = _read_csv(MULTI_DIR / "city_master_enriched.csv")

    features: list[dict] = []
    metadata_sample = None
    for idx in range(1, 33):
        state_code = f"{idx:02d}"
        doc = _fetch_state_geojson(state_code)
        if metadata_sample is None:
            metadata_sample = doc.get("metadatos", {})
        features.extend(doc.get("features", []))

    total_lookup = {row["city_code"]: _to_float(row["total_sami"]) for row in city_summary}

    y_maps: list[dict[str, str]] = []
    score_by_y: dict[str, dict[str, float]] = {}
    for row in city_scores:
        y_key = row["y_key"]
        score_by_y.setdefault(y_key, {})[row["city_code"]] = _to_float(row["sami"])

    manifest_rows: list[dict[str, str]] = []
    for slug, label, y_key in MAP_DEFS:
        value_lookup = total_lookup if y_key == "all" else score_by_y.get(y_key, {})
        fig_path = OUTPUT_DIR / "figures" / f"{slug}.svg"
        _write_choropleth(
            title=f"Mexico municipalities: {label}",
            subtitle="Municipal choropleth of city-scale SAMI using official INEGI municipal geometries.",
            features=features,
            value_lookup=value_lookup,
            path=fig_path,
        )
        manifest_rows.append({"figure": fig_path.name, "kind": "choropleth", "y_key": y_key, "label": label})
        y_maps.append({"slug": slug, "label": label, "y_key": y_key, "path": str(fig_path.resolve())})

    upper_codes = {
        row["city_code"]
        for row in sorted(city_master, key=lambda r: (_to_float(r["stable_top_decile_count"]), _to_float(r["total_sami"])), reverse=True)[:12]
    }
    lower_codes = {
        row["city_code"]
        for row in sorted(city_master, key=lambda r: (_to_float(r["stable_bottom_decile_count"]), -_to_float(r["total_sami"])), reverse=True)[:12]
    }
    high_pos_codes = {row["city_code"] for row in sorted(city_master, key=lambda r: _to_float(r["total_sami"]), reverse=True)[:12]}
    high_neg_codes = {row["city_code"] for row in sorted(city_master, key=lambda r: _to_float(r["total_sami"]))[:12]}
    signal_lookup: dict[str, str] = {}
    for code in high_neg_codes:
        signal_lookup[code] = "high_negative"
    for code in high_pos_codes:
        signal_lookup[code] = "high_positive"
    for code in lower_codes:
        signal_lookup[code] = "recurrent_lower"
    for code in upper_codes:
        signal_lookup[code] = "recurrent_upper"

    signal_path = OUTPUT_DIR / "figures" / "05_signal_classes_map.svg"
    _write_signal_map(
        title="Mexico municipalities: signal classes",
        subtitle="Selected municipal groups derived from SAMI contrast and recurrent tail signal across retained Y.",
        features=features,
        class_lookup=signal_lookup,
        path=signal_path,
    )
    manifest_rows.append({"figure": signal_path.name, "kind": "signal_class", "y_key": "", "label": "signal classes"})

    _write_csv(OUTPUT_DIR / "figures_manifest.csv", manifest_rows, ["figure", "kind", "y_key", "label"])

    report_lines = [
        "# City Map Pack",
        "",
        "Official source: INEGI `wscatgeo` municipal geometry service.",
        "",
        f"- cached geometry folder: [{RAW_GEO_DIR.name}]({RAW_GEO_DIR.as_posix()})",
        f"- figure manifest: [figures_manifest.csv]({(OUTPUT_DIR / 'figures_manifest.csv').as_posix()})",
        "",
        "Maps:",
    ]
    for row in manifest_rows:
        report_lines.append(f"- [{row['figure']}]({(OUTPUT_DIR / 'figures' / row['figure']).as_posix()})")
    if metadata_sample:
        report_lines.extend(
            [
                "",
                "Source metadata sample:",
                f"- vector source: `{metadata_sample.get('Fuente_informacion_vectorial', '')}`",
                f"- statistical source: `{metadata_sample.get('Fuente_informacion_estadistica', '')}`",
            ]
        )
    (OUTPUT_DIR / "report.md").write_text("\n".join(report_lines), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

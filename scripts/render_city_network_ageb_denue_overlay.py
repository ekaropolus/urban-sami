#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import subprocess
from pathlib import Path

from shapely import wkt
from shapely.geometry import LineString, MultiLineString, MultiPolygon, Polygon


DOCKER_EXE = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DB_CONTAINER = "24-polisplexity-core-db-dev"
POSTGRES_USER = "postgres"
DB_NAME = "urban_sami_exp"

BG = "#f6f3ec"
PANEL = "#fffdf9"
GRID = "#ddd6c8"
TEXT = "#1f1f1f"
MUTED = "#6b665d"
EDGE = "#157a74"
BOUNDARY = "#b14d3b"
AGEB = "#d97706"
POINT = "#1d4ed8"
SERIF = "Georgia, 'Times New Roman', serif"
SANS = "Helvetica, Arial, sans-serif"


def _psql(sql: str) -> str:
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
        "-At",
        "-F",
        "\t",
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    ]
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return proc.stdout


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _iter_line_coords(geom):
    if isinstance(geom, LineString):
        yield list(geom.coords)
    elif isinstance(geom, MultiLineString):
        for part in geom.geoms:
            yield list(part.coords)


def _iter_polygon_rings(geom):
    if isinstance(geom, Polygon):
        yield list(geom.exterior.coords)
    elif isinstance(geom, MultiPolygon):
        for poly in geom.geoms:
            yield list(poly.exterior.coords)


def _fetch_city_overlay_data(source_method: str, city_code: str) -> dict[str, object]:
    meta_sql = f"""
    SELECT m.city_name, m.state_code, m.n_nodes::text, m.n_edges::text,
           m.boundary_entry_edges::text, m.street_density_km_per_km2::text,
           ST_AsText(g.geom)
    FROM derived.city_network_metrics m
    JOIN derived.city_network_geoms g
      ON g.source_method = m.source_method AND g.city_code = m.city_code
    WHERE m.source_method = '{source_method}'
      AND m.city_code = '{city_code}';
    """
    meta_line = _psql(meta_sql).strip()
    if not meta_line:
        raise SystemExit(f"Missing persisted network for city_code={city_code} source_method={source_method}")
    city_name, state_code, n_nodes, n_edges, boundary_entries, street_density, municipal_wkt = meta_line.split("\t", 6)

    edge_sql = f"""
    SELECT ST_AsText(geom)
    FROM derived.city_network_edges
    WHERE source_method = '{source_method}'
      AND city_code = '{city_code}'
    ORDER BY row_id;
    """
    edges = [wkt.loads(line) for line in _psql(edge_sql).splitlines() if line.strip()]

    ageb_sql = f"""
    SELECT unit_code, ST_AsText(geom)
    FROM raw.admin_units
    WHERE city_code = '{city_code}'
      AND level = 'ageb_u'
      AND geom IS NOT NULL
    ORDER BY unit_code;
    """
    agebs = []
    for line in _psql(ageb_sql).splitlines():
        unit_code, geom_wkt = line.split("\t", 1)
        agebs.append({"unit_code": unit_code, "geom": wkt.loads(geom_wkt)})

    est_sql = f"""
    SELECT longitude::text, latitude::text
    FROM raw.denue_establishments
    WHERE city_code = '{city_code}'
      AND longitude IS NOT NULL
      AND latitude IS NOT NULL
    ORDER BY obs_id;
    """
    est_points = []
    for line in _psql(est_sql).splitlines():
        lon, lat = line.split("\t")
        est_points.append((float(lon), float(lat)))

    est_bbox = None
    if est_points:
        xs = [x for x, _ in est_points]
        ys = [y for _, y in est_points]
        est_bbox = (min(xs), min(ys), max(xs), max(ys))

    ageb_bbox_sql = f"""
    SELECT ST_XMin(ST_Extent(geom)), ST_YMin(ST_Extent(geom)),
           ST_XMax(ST_Extent(geom)), ST_YMax(ST_Extent(geom))
    FROM raw.admin_units
    WHERE city_code = '{city_code}'
      AND level = 'ageb_u'
      AND geom IS NOT NULL;
    """
    ageb_bbox_line = _psql(ageb_bbox_sql).strip()
    ageb_bbox = tuple(float(v) for v in ageb_bbox_line.split("\t")) if ageb_bbox_line else None

    return {
        "city_code": city_code,
        "city_name": city_name,
        "state_code": state_code,
        "n_nodes": n_nodes,
        "n_edges": n_edges,
        "boundary_entry_edges": boundary_entries,
        "street_density_km_per_km2": street_density,
        "municipal_geom": wkt.loads(municipal_wkt),
        "edges": edges,
        "agebs": agebs,
        "establishments": est_points,
        "ageb_bbox": ageb_bbox,
        "est_bbox": est_bbox,
    }


def _expand_bbox(bounds: tuple[float, float, float, float], frac: float = 0.04) -> tuple[float, float, float, float]:
    minx, miny, maxx, maxy = bounds
    dx = max(maxx - minx, 1e-9)
    dy = max(maxy - miny, 1e-9)
    return (minx - dx * frac, miny - dy * frac, maxx + dx * frac, maxy + dy * frac)


def _clip_ok(coords: list[tuple[float, float]], bbox: tuple[float, float, float, float]) -> bool:
    minx, miny, maxx, maxy = bbox
    for x, y in coords:
        if minx <= x <= maxx and miny <= y <= maxy:
            return True
    return False


def _render_overlay(
    path: Path,
    data: dict[str, object],
    *,
    bbox: tuple[float, float, float, float],
    title_suffix: str,
    draw_network: bool,
    draw_ageb: bool,
    draw_points: bool,
    point_r: float,
    point_opacity: float,
    point_stroke: str = "#ffffff",
    point_stroke_opacity: float = 0.65,
    point_stroke_width: float = 0.25,
    width: int = 980,
    height: int = 920,
) -> Path:
    pad = 48
    inner_top = 106
    minx, miny, maxx, maxy = bbox
    dx = max(maxx - minx, 1e-9)
    dy = max(maxy - miny, 1e-9)
    scale = min((width - 2 * pad) / dx, (height - inner_top - pad) / dy)
    x_off = pad + (width - 2 * pad - dx * scale) / 2.0
    y_off = inner_top + (height - inner_top - pad - dy * scale) / 2.0

    def pt(x: float, y: float) -> tuple[float, float]:
        px = x_off + (x - minx) * scale
        py = y_off + (maxy - y) * scale
        return px, py

    municipal_geom = data["municipal_geom"]
    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="16" y="16" width="{width-32}" height="{height-32}" rx="22" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="40" y="50" font-size="28" font-family="{SERIF}" fill="{TEXT}">{data["city_name"]} {title_suffix}</text>',
        f'<text x="40" y="76" font-size="14" font-family="{SANS}" fill="{MUTED}">city_code {data["city_code"]} | nodes {data["n_nodes"]} | edges {data["n_edges"]} | boundary entries {data["boundary_entry_edges"]}</text>',
        f'<text x="40" y="96" font-size="13" font-family="{SANS}" fill="{MUTED}">red = municipal support | teal = drive network | amber = AGEB | blue = DENUE establishments</text>',
    ]

    if draw_network:
        for edge in data["edges"]:
            for coords in _iter_line_coords(edge):
                if len(coords) < 2 or not _clip_ok(coords, bbox):
                    continue
                pieces = []
                for idx, (x, y) in enumerate(coords):
                    px, py = pt(x, y)
                    pieces.append(f'{"M" if idx == 0 else "L"}{px:.2f},{py:.2f}')
                body.append(f'<path d="{" ".join(pieces)}" fill="none" stroke="{EDGE}" stroke-opacity="0.28" stroke-width="0.7" stroke-linecap="round"/>')

    if draw_ageb:
        for ageb in data["agebs"]:
            geom = ageb["geom"]
            for ring in _iter_polygon_rings(geom):
                if len(ring) < 3 or not _clip_ok(ring, bbox):
                    continue
                pieces = []
                for idx, (x, y) in enumerate(ring):
                    px, py = pt(x, y)
                    pieces.append(f'{"M" if idx == 0 else "L"}{px:.2f},{py:.2f}')
                pieces.append("Z")
                body.append(f'<path d="{" ".join(pieces)}" fill="none" stroke="{AGEB}" stroke-opacity="0.45" stroke-width="0.55"/>')

    if draw_points:
        minbx, minby, maxbx, maxby = bbox
        for x, y in data["establishments"]:
            if not (minbx <= x <= maxbx and minby <= y <= maxby):
                continue
            px, py = pt(x, y)
            body.append(
                f'<circle cx="{px:.2f}" cy="{py:.2f}" r="{point_r:.2f}" '
                f'fill="{POINT}" fill-opacity="{point_opacity:.3f}" '
                f'stroke="{point_stroke}" stroke-opacity="{point_stroke_opacity:.3f}" '
                f'stroke-width="{point_stroke_width:.2f}"/>'
            )

    for ring in _iter_polygon_rings(municipal_geom):
        if len(ring) < 3 or not _clip_ok(ring, bbox):
            continue
        pieces = []
        for idx, (x, y) in enumerate(ring):
            px, py = pt(x, y)
            pieces.append(f'{"M" if idx == 0 else "L"}{px:.2f},{py:.2f}')
        pieces.append("Z")
        body.append(f'<path d="{" ".join(pieces)}" fill="none" stroke="{BOUNDARY}" stroke-opacity="0.9" stroke-width="1.1"/>')

    return _svg(path, width, height, "".join(body))


def main() -> int:
    parser = argparse.ArgumentParser(description="Render city overlay figures with municipal boundary, network, AGEB polygons, and DENUE establishments.")
    parser.add_argument("--source-method", default="osm_drive_municipal_full_v1")
    parser.add_argument("--city-code", default="26030")
    parser.add_argument("--output-dir", type=Path, default=Path("reports/city-overlay-hermosillo-2026-04-24"))
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    out_dir = root / args.output_dir
    fig_dir = out_dir / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)

    data = _fetch_city_overlay_data(args.source_method, args.city_code)
    full_bbox = _expand_bbox(data["municipal_geom"].bounds, 0.02)
    if data["ageb_bbox"]:
        urban_bbox = _expand_bbox(data["ageb_bbox"], 0.04)
    elif data["est_bbox"]:
        urban_bbox = _expand_bbox(data["est_bbox"], 0.08)
    else:
        urban_bbox = full_bbox

    figs = [
        {
            "figure_id": "full_overlay",
            "filename": "full_overlay.svg",
            "title_suffix": "Municipal Overlay",
            "bbox": full_bbox,
            "draw_network": True,
            "draw_ageb": True,
            "draw_points": True,
            "point_r": 0.55,
            "point_opacity": 0.18,
            "point_stroke_width": 0.18,
            "description": "Municipal support with network, AGEB polygons, and all establishments.",
        },
        {
            "figure_id": "urban_zoom_overlay",
            "filename": "urban_zoom_overlay.svg",
            "title_suffix": "Urban Zoom Overlay",
            "bbox": urban_bbox,
            "draw_network": True,
            "draw_ageb": True,
            "draw_points": True,
            "point_r": 1.15,
            "point_opacity": 0.50,
            "point_stroke_width": 0.30,
            "description": "Urban zoom based on AGEB extent, with network, AGEB polygons, and establishments.",
        },
        {
            "figure_id": "urban_zoom_ageb_establishments",
            "filename": "urban_zoom_ageb_establishments.svg",
            "title_suffix": "Urban Zoom AGEB + Establishments",
            "bbox": urban_bbox,
            "draw_network": False,
            "draw_ageb": True,
            "draw_points": True,
            "point_r": 1.35,
            "point_opacity": 0.72,
            "point_stroke_width": 0.32,
            "description": "Urban zoom without network, to see AGEB support and establishment point cloud more clearly.",
        },
        {
            "figure_id": "urban_zoom_denue_emphasis",
            "filename": "urban_zoom_denue_emphasis.svg",
            "title_suffix": "Urban Zoom DENUE Emphasis",
            "bbox": urban_bbox,
            "draw_network": False,
            "draw_ageb": False,
            "draw_points": True,
            "point_r": 2.0 if len(data["establishments"]) <= 200 else 1.2,
            "point_opacity": 0.92,
            "point_stroke_width": 0.38,
            "description": "Urban zoom with DENUE points emphasized for visual inspection of the settlement footprint.",
        },
    ]

    manifest_rows: list[dict[str, str]] = []
    for spec in figs:
        path = fig_dir / spec["filename"]
        _render_overlay(
            path,
            data,
            bbox=spec["bbox"],
            title_suffix=spec["title_suffix"],
            draw_network=spec["draw_network"],
            draw_ageb=spec["draw_ageb"],
            draw_points=spec["draw_points"],
            point_r=spec["point_r"],
            point_opacity=spec["point_opacity"],
            point_stroke_width=spec["point_stroke_width"],
        )
        manifest_rows.append(
            {
                "figure_id": spec["figure_id"],
                "filename": spec["filename"],
                "city_code": data["city_code"],
                "city_name": data["city_name"],
                "n_ageb": str(len(data["agebs"])),
                "n_establishments": str(len(data["establishments"])),
                "description": spec["description"],
            }
        )

    _write_csv(out_dir / "figures_manifest.csv", manifest_rows, list(manifest_rows[0].keys()))

    report = out_dir / "report.md"
    lines = [
        f"# {data['city_name']} Overlay",
        "",
        f"Source method: `{args.source_method}`",
        f"City code: `{data['city_code']}`",
        "",
        f"- AGEB polygons loaded: `{len(data['agebs'])}`",
        f"- DENUE establishments plotted: `{len(data['establishments'])}`",
        "",
        "Figures:",
    ]
    for row in manifest_rows:
        lines.append(f'- [{row["figure_id"]}]({(fig_dir / row["filename"]).resolve()}): {row["description"]}')
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

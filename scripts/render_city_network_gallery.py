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
EDGE = "#0f766e"
OUTLINE = "#b14d3b"
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
        (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
            f'viewBox="0 0 {width} {height}" role="img">{body}</svg>'
        ),
        encoding="utf-8",
    )
    return path


def _get_completed_cities(source_method: str, limit: int, sort_by: str) -> list[dict[str, str]]:
    if sort_by == "n_nodes":
        order_sql = "m.n_nodes DESC NULLS LAST, s.city_code"
        sort_value_sql = "m.n_nodes::text"
    elif sort_by == "street_density_km_per_km2":
        order_sql = "m.street_density_km_per_km2 DESC NULLS LAST, s.city_code"
        sort_value_sql = "m.street_density_km_per_km2::text"
    elif sort_by == "boundary_entry_edges_per_km":
        order_sql = "m.boundary_entry_edges_per_km DESC NULLS LAST, s.city_code"
        sort_value_sql = "m.boundary_entry_edges_per_km::text"
    elif sort_by == "latest":
        order_sql = "s.finished_at DESC NULLS LAST, s.city_code"
        sort_value_sql = "to_char(s.finished_at, 'YYYY-MM-DD HH24:MI:SS')"
    else:
        raise ValueError(f"unsupported sort_by={sort_by}")

    sql = f"""
    SELECT s.city_code, s.city_name, g.state_code, m.n_nodes::text, m.n_edges::text, {sort_value_sql}
    FROM experiments.city_network_extract_status s
    LEFT JOIN derived.city_network_geoms g
      ON g.source_method = s.source_method AND g.city_code = s.city_code
    LEFT JOIN derived.city_network_metrics m
      ON m.source_method = s.source_method AND m.city_code = s.city_code
    WHERE s.source_method = '{source_method}'
      AND s.status = 'success'
    ORDER BY {order_sql}
    LIMIT {limit};
    """
    out = _psql(sql)
    rows: list[dict[str, str]] = []
    for line in out.splitlines():
        city_code, city_name, state_code, n_nodes, n_edges, sort_value = line.split("\t")
        rows.append(
            {
                "city_code": city_code,
                "city_name": city_name,
                "state_code": state_code,
                "n_nodes": n_nodes,
                "n_edges": n_edges,
                "sort_value": sort_value,
            }
        )
    return rows


def _fetch_city_data(source_method: str, city_code: str) -> tuple[Polygon | MultiPolygon, list[LineString | MultiLineString], dict[str, str]]:
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
    city_name, state_code, n_nodes, n_edges, boundary_entries, street_density, geom_wkt = meta_line.split("\t", 6)
    geom = wkt.loads(geom_wkt)

    edge_sql = f"""
    SELECT ST_AsText(geom)
    FROM derived.city_network_edges
    WHERE source_method = '{source_method}'
      AND city_code = '{city_code}'
    ORDER BY row_id;
    """
    edge_lines = _psql(edge_sql).splitlines()
    edges = [wkt.loads(line) for line in edge_lines if line.strip()]
    meta = {
        "city_code": city_code,
        "city_name": city_name,
        "state_code": state_code,
        "n_nodes": n_nodes,
        "n_edges": n_edges,
        "boundary_entry_edges": boundary_entries,
        "street_density_km_per_km2": street_density,
    }
    return geom, edges, meta


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


def _network_svg(path: Path, geom, edges: list, meta: dict[str, str], width: int = 900, height: int = 900) -> Path:
    pad = 48
    inner_top = 92
    minx, miny, maxx, maxy = geom.bounds
    dx = max(maxx - minx, 1e-9)
    dy = max(maxy - miny, 1e-9)
    scale = min((width - 2 * pad) / dx, (height - inner_top - pad) / dy)
    x_off = pad + (width - 2 * pad - dx * scale) / 2.0
    y_off = inner_top + (height - inner_top - pad - dy * scale) / 2.0

    def pt(x: float, y: float) -> tuple[float, float]:
        px = x_off + (x - minx) * scale
        py = y_off + (maxy - y) * scale
        return px, py

    edge_paths: list[str] = []
    for edge in edges:
        for coords in _iter_line_coords(edge):
            if len(coords) < 2:
                continue
            pieces = []
            for idx, (x, y) in enumerate(coords):
                px, py = pt(x, y)
                pieces.append(f'{"M" if idx == 0 else "L"}{px:.2f},{py:.2f}')
            edge_paths.append(" ".join(pieces))

    outline_paths: list[str] = []
    for ring in _iter_polygon_rings(geom):
        if len(ring) < 3:
            continue
        pieces = []
        for idx, (x, y) in enumerate(ring):
            px, py = pt(x, y)
            pieces.append(f'{"M" if idx == 0 else "L"}{px:.2f},{py:.2f}')
        pieces.append("Z")
        outline_paths.append(" ".join(pieces))

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="16" y="16" width="{width-32}" height="{height-32}" rx="22" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="40" y="52" font-size="28" font-family="{SERIF}" fill="{TEXT}">{meta["city_name"]}</text>',
        f'<text x="40" y="78" font-size="14" font-family="{SANS}" fill="{MUTED}">'
        f'city_code {meta["city_code"]} | nodes {meta["n_nodes"]} | edges {meta["n_edges"]} | boundary entries {meta["boundary_entry_edges"]}'
        f'</text>',
    ]
    for d in edge_paths:
        body.append(f'<path d="{d}" fill="none" stroke="{EDGE}" stroke-opacity="0.38" stroke-width="0.75" stroke-linecap="round"/>')
    for d in outline_paths:
        body.append(f'<path d="{d}" fill="none" stroke="{OUTLINE}" stroke-opacity="0.85" stroke-width="1.25"/>')
    return _svg(path, width, height, "".join(body))


def _gallery_svg(path: Path, tiles: list[dict[str, str]], title: str, subtitle: str, sort_by: str) -> Path:
    cols = 3
    tile_w = 360
    tile_h = 150
    gap = 18
    rows = (len(tiles) + cols - 1) // cols
    width = cols * tile_w + (cols + 1) * gap
    height = rows * tile_h + (rows + 1) * gap + 84
    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<text x="{gap}" y="42" font-size="28" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="{gap}" y="66" font-size="14" font-family="{SANS}" fill="{MUTED}">{subtitle}</text>',
    ]
    for idx, tile in enumerate(tiles):
        r = idx // cols
        c = idx % cols
        x = gap + c * (tile_w + gap)
        y = 84 + gap + r * (tile_h + gap)
        body.extend(
            [
                f'<a href="./{tile["filename"]}">',
                f'<rect x="{x}" y="{y}" width="{tile_w}" height="{tile_h}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
                f'<text x="{x+18}" y="{y+34}" font-size="24" font-family="{SERIF}" fill="{TEXT}">{tile["city_name"]}</text>',
                f'<text x="{x+18}" y="{y+58}" font-size="13" font-family="{SANS}" fill="{MUTED}">city_code {tile["city_code"]} | state {tile["state_code"]}</text>',
                f'<text x="{x+18}" y="{y+84}" font-size="13" font-family="{SANS}" fill="{MUTED}">nodes {tile["n_nodes"]} | edges {tile["n_edges"]}</text>',
                f'<text x="{x+18}" y="{y+108}" font-size="13" font-family="{SANS}" fill="{MUTED}">boundary entries {tile["boundary_entry_edges"]} | {sort_by} {tile["sort_value"]}</text>',
                f'<text x="{x+18}" y="{y+132}" font-size="13" font-family="{SANS}" fill="{EDGE}">open SVG</text>',
                '</a>',
            ]
        )
    return _svg(path, width, height, "".join(body))


def main() -> int:
    parser = argparse.ArgumentParser(description="Render SVG visualizations of persisted city OSM networks.")
    parser.add_argument("--source-method", default="osm_drive_municipal_full_v1")
    parser.add_argument("--max-cities", type=int, default=12)
    parser.add_argument(
        "--sort-by",
        default="n_nodes",
        choices=["n_nodes", "street_density_km_per_km2", "boundary_entry_edges_per_km", "latest"],
    )
    parser.add_argument("--title", default="")
    parser.add_argument("--subtitle", default="")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports/city-network-gallery-2026-04-23"),
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    out_dir = root / args.output_dir
    fig_dir = out_dir / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)

    cities = _get_completed_cities(args.source_method, args.max_cities, args.sort_by)
    manifest: list[dict[str, str]] = []
    for city in cities:
        geom, edges, meta = _fetch_city_data(args.source_method, city["city_code"])
        filename = f'{meta["city_code"]}_{meta["city_name"].replace("/", "-").replace(" ", "_")}_network.svg'
        svg_path = fig_dir / filename
        _network_svg(svg_path, geom, edges, meta)
        manifest.append(
            {
                "city_code": meta["city_code"],
                "city_name": meta["city_name"],
                "state_code": meta["state_code"],
                "n_nodes": meta["n_nodes"],
                "n_edges": meta["n_edges"],
                "boundary_entry_edges": meta["boundary_entry_edges"],
                "street_density_km_per_km2": meta["street_density_km_per_km2"],
                "sort_by": args.sort_by,
                "sort_value": city["sort_value"],
                "filename": filename,
            }
        )
        print(f"rendered {meta['city_code']} {meta['city_name']}", flush=True)

    _write_csv(out_dir / "figures_manifest.csv", manifest, list(manifest[0].keys()) if manifest else ["city_code"])
    default_titles = {
        "n_nodes": "City OSM Network Gallery: Largest Networks",
        "street_density_km_per_km2": "City OSM Network Gallery: Highest Street Density",
        "boundary_entry_edges_per_km": "City OSM Network Gallery: Highest Boundary Entry Rate",
        "latest": "City OSM Network Gallery: Latest Completed Cities",
    }
    default_subtitles = {
        "n_nodes": "Completed cities ranked by node count from derived.city_network_edges",
        "street_density_km_per_km2": "Completed cities ranked by street length per km²",
        "boundary_entry_edges_per_km": "Completed cities ranked by crossing edges per km of municipal boundary",
        "latest": "Most recently completed persisted city networks",
    }
    title = args.title or default_titles[args.sort_by]
    subtitle = args.subtitle or default_subtitles[args.sort_by]
    _gallery_svg(out_dir / "gallery.svg", manifest, title, subtitle, args.sort_by)

    report = out_dir / "report.md"
    lines = [
        f"# {title}",
        "",
        f"Source method: `{args.source_method}`",
        f"Sort by: `{args.sort_by}`",
        "",
        f"Rendered cities: `{len(manifest)}`",
        "",
        "Main gallery:",
        f"- [gallery.svg]({(out_dir / 'gallery.svg').as_posix()})",
        "",
        "Figures manifest:",
        f"- [figures_manifest.csv]({(out_dir / 'figures_manifest.csv').as_posix()})",
        "",
        "Figures:",
    ]
    for row in manifest:
        lines.append(f'- [{row["city_name"]}]({(fig_dir / row["filename"]).as_posix()})')
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
from __future__ import annotations

import csv
import html
import json
import math
import shutil
import subprocess
from collections import defaultdict
from pathlib import Path

from urban_sami.modeling.fit import fit_ols
from run_single_city_ageb_experiment import _write_ageb_map


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
BLUE = "#315c80"
RUST = "#b14d3b"
GOLD = "#b28a2e"
SANS = "Helvetica, Arial, sans-serif"
SERIF = "Georgia, 'Times New Roman', serif"


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


def _adj_r2(r2: float, n: int, p: int) -> float:
    df = n - p - 1
    if df <= 0:
        return r2
    return 1 - (1 - r2) * (n - 1) / df


def _quantile(vals: list[float], q: float) -> float:
    vals = sorted(vals)
    idx = min(len(vals) - 1, max(0, round((len(vals) - 1) * q)))
    return vals[idx]


def _fetch_features(city_code: str) -> list[dict]:
    rows = _query_tsv(
        f"""
        SELECT unit_code, unit_label, ST_AsGeoJSON(geom)
        FROM raw.admin_units
        WHERE level = 'ageb_u' AND city_code = '{city_code}'
        ORDER BY unit_code
        """.strip(),
        ["unit_code", "unit_label", "geom_json"],
    )
    return [
        {
            "type": "Feature",
            "properties": {"unit_code": row["unit_code"], "unit_label": row["unit_label"]},
            "geometry": json.loads(row["geom_json"]),
        }
        for row in rows
    ]


def _fetch_touch_edges(city_code: str, unit_codes: list[str]) -> list[tuple[str, str]]:
    if len(unit_codes) < 2:
        return []
    in_list = ",".join("'" + code + "'" for code in unit_codes)
    rows = _query_tsv(
        f"""
        SELECT a.unit_code, b.unit_code
        FROM raw.admin_units a
        JOIN raw.admin_units b
          ON a.unit_code < b.unit_code
         AND ST_Touches(a.geom, b.geom)
        WHERE a.level = 'ageb_u'
          AND b.level = 'ageb_u'
          AND a.city_code = '{city_code}'
          AND b.city_code = '{city_code}'
          AND a.unit_code IN ({in_list})
          AND b.unit_code IN ({in_list})
        """.strip(),
        ["u", "v"],
    )
    return [(row["u"], row["v"]) for row in rows]


def _components(unit_codes: list[str], edges: list[tuple[str, str]]) -> dict[str, float]:
    adj = defaultdict(set)
    for u, v in edges:
        adj[u].add(v)
        adj[v].add(u)
    seen = set()
    comp_sizes = []
    for u in unit_codes:
        if u in seen:
            continue
        stack = [u]
        seen.add(u)
        size = 0
        while stack:
            cur = stack.pop()
            size += 1
            for nxt in adj[cur]:
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
        comp_sizes.append(size)
    comp_sizes.sort(reverse=True)
    return {
        "component_count": len(comp_sizes),
        "largest_component_size": comp_sizes[0] if comp_sizes else 0,
        "largest_component_share": (comp_sizes[0] / len(unit_codes)) if comp_sizes else 0.0,
        "edge_count": len(edges),
    }


def _svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _write_rank(path: Path, rows: list[dict[str, object]]) -> Path:
    width = 1040
    left = 230
    right = 60
    top = 96
    bottom = 78
    row_h = 28
    height = top + len(rows) * row_h + bottom
    vals = [float(r["r2"]) for r in rows]
    xmax = max(vals) * 1.1 if vals else 1.0

    def px(v: float) -> float:
        return left + (v / max(xmax, 1e-9)) * (width - left - right)

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">Subset search by centrality rule</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">Each row is a rule-based AGEB subset. This is transparent subset search, not manual selection.</text>',
    ]
    for i, row in enumerate(rows):
        y = top + i * row_h
        label = f"{row['subset_rule']} ({int(row['n_obs'])})"
        r2 = float(row["r2"])
        beta = float(row["beta"])
        body.append(f'<text x="{left-12}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{html.escape(label)}</text>')
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{px(r2):.2f}" y2="{y:.2f}" stroke="{TEAL}" stroke-width="8"/>')
        body.append(f'<text x="{px(r2)+8:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">R²={r2:.3f} β={beta:+.3f}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "ageb-subset-search-guadalajara-2026-04-22"
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    city_code = "14039"
    city_name = "Guadalajara"
    rows = _read_csv(root / "reports" / "ageb-centrality-extension-guadalajara-2026-04-22" / "ageb_centrality_table.csv")
    rows = [
        {
            "unit_code": r["unit_code"],
            "unit_label": r["unit_label"],
            "population": _to_float(r["population"]),
            "y": _to_float(r["est_count"]),
            "dist": _to_float(r["dist_to_est_center_km"]),
        }
        for r in rows
        if _to_float(r["population"]) > 0 and _to_float(r["est_count"]) > 0
    ]
    dists = sorted(r["dist"] for r in rows)

    subset_rows = []
    candidates: list[tuple[str, list[dict[str, object]]]] = []
    for q in [0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40]:
        thresh_far = _quantile(dists, 1 - q)
        far = [r for r in rows if r["dist"] >= thresh_far]
        candidates.append((f"farthest_{int(q*100)}pct", far))
        thresh_near = _quantile(dists, q)
        near = [r for r in rows if r["dist"] <= thresh_near]
        candidates.append((f"nearest_{int(q*100)}pct", near))
    for name, subset in candidates:
        if len(subset) < 40:
            continue
        fit = fit_ols([r["y"] for r in subset], [r["population"] for r in subset])
        subset_rows.append(
            {
                "subset_rule": name,
                "n_obs": len(subset),
                "retention_rate": len(subset) / len(rows),
                "beta": fit.beta,
                "r2": fit.r2,
                "adj_r2": _adj_r2(fit.r2, len(subset), 1),
                "mean_distance_km": sum(r["dist"] for r in subset) / len(subset),
                "mean_population": sum(r["population"] for r in subset) / len(subset),
                "mean_y": sum(r["y"] for r in subset) / len(subset),
            }
        )

    subset_rows.sort(key=lambda r: (float(r["r2"]), float(r["n_obs"])), reverse=True)
    best = subset_rows[0]
    best_name = best["subset_rule"]
    best_subset = dict(candidates)[best_name]
    best_codes = [r["unit_code"] for r in best_subset]
    edges = _fetch_touch_edges(city_code, best_codes)
    graph_stats = _components(best_codes, edges)

    all_codes = {r["unit_code"] for r in rows}
    features = _fetch_features(city_code)
    value_lookup = {code: (1.0 if code in best_codes else -1.0) for code in all_codes}
    map_path = _write_ageb_map(city_name, f"Best subset: {best_name}", features, value_lookup, figdir / "best_subset_map.svg")
    rank_path = _write_rank(figdir / "subset_r2_rank.svg", subset_rows)

    _write_csv(outdir / "subset_search_summary.csv", subset_rows, list(subset_rows[0].keys()))
    _write_csv(
        outdir / "best_subset_members.csv",
        [{**r} for r in best_subset],
        list(best_subset[0].keys()) if best_subset else ["unit_code", "unit_label", "population", "y", "dist"],
    )
    _write_csv(outdir / "best_subset_graph_stats.csv", [{**best, **graph_stats}], list({**best, **graph_stats}.keys()))
    _write_csv(figdir / "figures_manifest.csv", [
        {"figure_id": "subset_r2_rank", "path": str(rank_path.resolve()), "description": "R² across candidate subsets."},
        {"figure_id": "best_subset_map", "path": str(map_path.resolve()), "description": "Retained AGEB subset map."},
    ], ["figure_id", "path", "description"])

    report = [
        "# AGEB Subset Search Guadalajara",
        "",
        "This is a transparent subset search over AGEBs for `Y = total establishments` and `N = population`.",
        "No manual AGEB choice was used. Candidate subsets were defined by distance-to-center rules.",
        "",
        "## Best subset",
        "",
        f"- rule: `{best_name}`",
        f"- retained AGEB: `{int(best['n_obs'])}` of `{len(rows)}` (`{best['retention_rate']*100:.1f}%`)",
        f"- `beta = {float(best['beta']):+.3f}`",
        f"- `R² = {float(best['r2']):.3f}`",
        f"- `adjR² = {float(best['adj_r2']):.3f}`",
        f"- mean distance to center = `{float(best['mean_distance_km']):.2f}` km",
        "",
        "## Spatial relation of retained AGEB",
        "",
        f"- connected components: `{graph_stats['component_count']}`",
        f"- largest component size: `{graph_stats['largest_component_size']}`",
        f"- largest component share: `{graph_stats['largest_component_share']:.3f}`",
        f"- touching edges inside subset: `{graph_stats['edge_count']}`",
        "",
        "## Files",
        "",
        f"- [subset_search_summary.csv]({(outdir / 'subset_search_summary.csv').resolve()})",
        f"- [best_subset_members.csv]({(outdir / 'best_subset_members.csv').resolve()})",
        f"- [best_subset_graph_stats.csv]({(outdir / 'best_subset_graph_stats.csv').resolve()})",
        "",
        "## Figures",
        "",
        f"- [subset_r2_rank.svg]({rank_path.resolve()})",
        f"- [best_subset_map.svg]({map_path.resolve()})",
    ]
    (outdir / "report.md").write_text("\n".join(report), encoding="utf-8")
    print(json.dumps({"ok": True, "output_dir": str(outdir), "best_subset": best_name}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

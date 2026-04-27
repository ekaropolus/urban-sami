#!/usr/bin/env python3
from __future__ import annotations

import csv
import html
import json
import math
import shutil
import subprocess
from collections import Counter, defaultdict
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


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _quantile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("empty values")
    idx = (len(ordered) - 1) * q
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return ordered[lo]
    frac = idx - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def _adj_r2(r2: float, n: int, p: int) -> float:
    df = n - p - 1
    if df <= 0:
        return r2
    return 1.0 - ((1.0 - r2) * (n - 1) / float(df))


def _fmt(v: float, digits: int = 3) -> str:
    return f"{v:.{digits}f}"


def _svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _fetch_feature_rows(city_code: str) -> list[dict[str, str]]:
    return _query_tsv(
        f"""
        WITH city_points AS (
            SELECT ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geom
            FROM raw.denue_establishments
            WHERE city_code = '{city_code}'
              AND longitude IS NOT NULL
              AND latitude IS NOT NULL
        ),
        city_center AS (
            SELECT ST_Centroid(ST_Collect(geom)) AS geom
            FROM city_points
        ),
        ageb AS (
            SELECT unit_code, unit_label, population, geom
            FROM raw.admin_units
            WHERE level = 'ageb_u' AND city_code = '{city_code}'
        ),
        counts AS (
            SELECT a.unit_code, COUNT(p.*)::int AS est_count
            FROM ageb a
            LEFT JOIN city_points p ON ST_Covers(a.geom, p.geom)
            GROUP BY a.unit_code
        ),
        geom_stats AS (
            SELECT
                a.unit_code,
                ST_Area(a.geom::geography) / 1000000.0 AS area_km2,
                ST_Perimeter(a.geom::geography) / 1000.0 AS perimeter_km,
                ST_X(ST_Centroid(a.geom)) AS centroid_lon,
                ST_Y(ST_Centroid(a.geom)) AS centroid_lat,
                ST_DistanceSphere(ST_Centroid(a.geom), cc.geom) / 1000.0 AS dist_to_center_km
            FROM ageb a
            CROSS JOIN city_center cc
        ),
        neigh AS (
            SELECT a.unit_code, COUNT(b.*)::int AS neighbor_degree
            FROM ageb a
            LEFT JOIN ageb b
              ON a.unit_code <> b.unit_code
             AND ST_Touches(a.geom, b.geom)
            GROUP BY a.unit_code
        )
        SELECT
            a.unit_code,
            a.unit_label,
            COALESCE(a.population, 0)::text AS population,
            c.est_count::text AS est_count,
            gs.area_km2::text AS area_km2,
            gs.perimeter_km::text AS perimeter_km,
            gs.centroid_lon::text AS centroid_lon,
            gs.centroid_lat::text AS centroid_lat,
            gs.dist_to_center_km::text AS dist_to_center_km,
            COALESCE(n.neighbor_degree, 0)::text AS neighbor_degree
        FROM ageb a
        JOIN counts c ON c.unit_code = a.unit_code
        JOIN geom_stats gs ON gs.unit_code = a.unit_code
        LEFT JOIN neigh n ON n.unit_code = a.unit_code
        ORDER BY a.unit_code
        """.strip(),
        [
            "unit_code",
            "unit_label",
            "population",
            "est_count",
            "area_km2",
            "perimeter_km",
            "centroid_lon",
            "centroid_lat",
            "dist_to_center_km",
            "neighbor_degree",
        ],
    )


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


def _rule_rows(rows: list[dict], feature: str, mode: str, qlo: float, qhi: float | None = None) -> tuple[str, list[dict]]:
    values = [float(r[feature]) for r in rows]
    if mode == "low":
        thr = _quantile(values, qlo)
        kept = [r for r in rows if float(r[feature]) <= thr]
        name = f"{feature}__low_{int(qlo*100)}"
        return name, kept
    if mode == "high":
        thr = _quantile(values, 1.0 - qlo)
        kept = [r for r in rows if float(r[feature]) >= thr]
        name = f"{feature}__high_{int(qlo*100)}"
        return name, kept
    if mode == "band":
        if qhi is None:
            raise ValueError("band requires qhi")
        lo = _quantile(values, qlo)
        hi = _quantile(values, qhi)
        kept = [r for r in rows if lo <= float(r[feature]) <= hi]
        name = f"{feature}__band_{int(qlo*100)}_{int(qhi*100)}"
        return name, kept
    raise ValueError(mode)


def _metric_row(name: str, subset: list[dict], total_n: int) -> dict[str, object]:
    fit = fit_ols([float(r["est_count"]) for r in subset], [float(r["population"]) for r in subset])
    return {
        "subset_rule": name,
        "n_obs": len(subset),
        "retention_rate": len(subset) / float(total_n),
        "beta": fit.beta,
        "r2": fit.r2,
        "adj_r2": _adj_r2(fit.r2, len(subset), 1),
        "mean_population": sum(float(r["population"]) for r in subset) / len(subset),
        "mean_est_count": sum(float(r["est_count"]) for r in subset) / len(subset),
        "mean_area_km2": sum(float(r["area_km2"]) for r in subset) / len(subset),
        "mean_dist_to_center_km": sum(float(r["dist_to_center_km"]) for r in subset) / len(subset),
        "mean_neighbor_degree": sum(float(r["neighbor_degree"]) for r in subset) / len(subset),
    }


def _jaccard(a: set[str], b: set[str]) -> float:
    inter = len(a & b)
    union = len(a | b)
    return inter / float(union) if union else 0.0


def _write_rank(path: Path, rows: list[dict[str, object]]) -> Path:
    width = 1120
    left = 360
    right = 70
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
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">Top AGEB subsets by fit</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">Transparent search over geometric and topological rules. Higher R² means a cleaner local law in that subset.</text>',
    ]
    for i, row in enumerate(rows):
        y = top + i * row_h
        label = f"{row['subset_rule']} (n={int(row['n_obs'])})"
        r2 = float(row["r2"])
        beta = float(row["beta"])
        body.append(f'<text x="{left-12}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{html.escape(label)}</text>')
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{px(r2):.2f}" y2="{y:.2f}" stroke="{TEAL}" stroke-width="8"/>')
        body.append(f'<text x="{px(r2)+8:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">R²={r2:.3f} β={beta:+.3f}</text>')
    return _svg(path, width, height, "".join(body))


def _write_size_vs_r2(path: Path, rows: list[dict[str, object]]) -> Path:
    width = 1040
    height = 620
    left = 86
    right = 40
    top = 96
    bottom = 90
    plot_w = width - left - right
    plot_h = height - top - bottom
    xvals = [float(r["retention_rate"]) for r in rows]
    yvals = [float(r["r2"]) for r in rows]
    xmin, xmax = min(xvals), max(xvals)
    ymin, ymax = min(yvals), max(yvals)
    xmin -= 0.01
    xmax += 0.01
    ymin = min(0.0, ymin - 0.02)
    ymax += 0.03

    def px(v: float) -> float:
        return left + ((v - xmin) / max(xmax - xmin, 1e-9)) * plot_w

    def py(v: float) -> float:
        return top + plot_h - ((v - ymin) / max(ymax - ymin, 1e-9)) * plot_h

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">Subset size versus fit</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">This checks whether the best subsets are just tiny fragments or whether they retain meaningful mass.</text>',
        f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="none" stroke="{AXIS}"/>',
    ]
    for row in rows:
        x = float(row["retention_rate"])
        y = float(row["r2"])
        n = int(row["n_obs"])
        body.append(f'<circle cx="{px(x):.2f}" cy="{py(y):.2f}" r="4.2" fill="{BLUE}" fill-opacity="0.75"/>')
        if y >= sorted(yvals, reverse=True)[min(9, len(yvals)-1)]:
            body.append(f'<text x="{px(x)+8:.2f}" y="{py(y)-6:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">{html.escape(str(row["subset_rule"]))}</text>')
        body.append(f'<text x="{px(x)+6:.2f}" y="{py(y)+14:.2f}" font-size="10" font-family="{SANS}" fill="{MUTED}">n={n}</text>')
    return _svg(path, width, height, "".join(body))


def _write_overlap_table_svg(path: Path, rows: list[dict[str, object]]) -> Path:
    width = 1240
    col_x = [44, 420, 710, 940, 1080]
    row_h = 28
    top = 106
    height = top + row_h * len(rows) + 50
    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">Relations among the top subsets</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">Jaccard overlap shows whether the best rules are discovering the same AGEBs or distinct regimes.</text>',
    ]
    headers = ["subset_a", "subset_b", "jaccard", "shared_agebs", "distinct_union"]
    for x, h in zip(col_x, headers):
        body.append(f'<text x="{x}" y="{top-18}" font-size="12" font-family="{SANS}" fill="{MUTED}">{h}</text>')
    for i, row in enumerate(rows):
        y = top + i * row_h
        vals = [row["subset_a"], row["subset_b"], f'{float(row["jaccard"]):.3f}', str(int(row["shared_agebs"])), str(int(row["distinct_union"]))]
        for x, v in zip(col_x, vals):
            body.append(f'<text x="{x}" y="{y}" font-size="12" font-family="{SANS}" fill="{TEXT}">{html.escape(v)}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "ageb-subset-discovery-guadalajara-2026-04-22"
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    city_code = "14039"
    city_name = "Guadalajara"

    raw_rows = _fetch_feature_rows(city_code)
    rows: list[dict[str, object]] = []
    for r in raw_rows:
        population = _to_float(r["population"])
        est = _to_float(r["est_count"])
        area = max(_to_float(r["area_km2"]), 1e-9)
        perim = max(_to_float(r["perimeter_km"]), 1e-9)
        area_m2 = area * 1_000_000.0
        perim_m = perim * 1000.0
        compactness = 4.0 * math.pi * area_m2 / max(perim_m * perim_m, 1e-9)
        rows.append(
            {
                "unit_code": r["unit_code"],
                "unit_label": r["unit_label"],
                "population": population,
                "est_count": est,
                "area_km2": area,
                "perimeter_km": perim,
                "dist_to_center_km": _to_float(r["dist_to_center_km"]),
                "neighbor_degree": _to_float(r["neighbor_degree"]),
                "centroid_lon": _to_float(r["centroid_lon"]),
                "centroid_lat": _to_float(r["centroid_lat"]),
                "population_density": population / area if area > 0 else 0.0,
                "compactness": compactness,
            }
        )
    rows = [r for r in rows if float(r["population"]) > 0 and float(r["est_count"]) > 0]
    total_n = len(rows)

    feature_fields = [
        "dist_to_center_km",
        "area_km2",
        "population_density",
        "compactness",
        "neighbor_degree",
    ]
    quantiles = [0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40]
    band_pairs = [(0.10, 0.30), (0.20, 0.40), (0.30, 0.50), (0.40, 0.60), (0.60, 0.80)]

    candidate_sets: dict[str, list[dict]] = {}
    for feature in feature_fields:
        for q in quantiles:
            name, kept = _rule_rows(rows, feature, "low", q)
            if len(kept) >= 35:
                candidate_sets[name] = kept
            name, kept = _rule_rows(rows, feature, "high", q)
            if len(kept) >= 35:
                candidate_sets[name] = kept
        for qlo, qhi in band_pairs:
            name, kept = _rule_rows(rows, feature, "band", qlo, qhi)
            if len(kept) >= 35:
                candidate_sets[name] = kept

    combo_specs = [
        ("dist_to_center_km__high_20", "compactness__high_20"),
        ("dist_to_center_km__high_20", "population_density__high_20"),
        ("dist_to_center_km__high_25", "area_km2__low_25"),
        ("dist_to_center_km__high_25", "neighbor_degree__low_25"),
        ("dist_to_center_km__high_30", "population_density__high_30"),
        ("dist_to_center_km__high_30", "compactness__high_30"),
        ("area_km2__low_25", "population_density__high_25"),
        ("area_km2__low_30", "neighbor_degree__low_30"),
        ("compactness__high_25", "neighbor_degree__low_25"),
        ("population_density__high_25", "neighbor_degree__low_25"),
    ]
    index = {name: {r["unit_code"] for r in subset} for name, subset in candidate_sets.items()}
    base_lookup = {name: subset for name, subset in candidate_sets.items()}
    for a, b in combo_specs:
        if a not in index or b not in index:
            continue
        keep_codes = index[a] & index[b]
        kept = [r for r in rows if r["unit_code"] in keep_codes]
        if len(kept) >= 35:
            candidate_sets[f"{a}__AND__{b}"] = kept

    summary_rows = []
    member_rows = []
    for name, subset in candidate_sets.items():
        metrics = _metric_row(name, subset, total_n)
        summary_rows.append(metrics)
        for r in subset:
            member_rows.append(
                {
                    "subset_rule": name,
                    "unit_code": r["unit_code"],
                    "unit_label": r["unit_label"],
                    "population": r["population"],
                    "est_count": r["est_count"],
                    "area_km2": r["area_km2"],
                    "dist_to_center_km": r["dist_to_center_km"],
                    "population_density": r["population_density"],
                    "compactness": r["compactness"],
                    "neighbor_degree": r["neighbor_degree"],
                }
            )

    summary_rows.sort(key=lambda r: (float(r["r2"]), float(r["n_obs"])), reverse=True)
    rank = 1
    for row in summary_rows:
        row["rank_r2"] = rank
        rank += 1

    _write_csv(
        outdir / "ageb_feature_table.csv",
        rows,
        [
            "unit_code",
            "unit_label",
            "population",
            "est_count",
            "area_km2",
            "perimeter_km",
            "dist_to_center_km",
            "neighbor_degree",
            "population_density",
            "compactness",
            "centroid_lon",
            "centroid_lat",
        ],
    )
    _write_csv(outdir / "subset_search_summary.csv", summary_rows, list(summary_rows[0].keys()))
    _write_csv(outdir / "subset_members_long.csv", member_rows, list(member_rows[0].keys()))

    top_rows = summary_rows[:12]
    _write_csv(outdir / "top_subsets.csv", top_rows, list(top_rows[0].keys()))

    top_sets = {row["subset_rule"]: {m["unit_code"] for m in member_rows if m["subset_rule"] == row["subset_rule"]} for row in top_rows}
    overlap_rows = []
    names = [str(r["subset_rule"]) for r in top_rows]
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            a = top_sets[names[i]]
            b = top_sets[names[j]]
            overlap_rows.append(
                {
                    "subset_a": names[i],
                    "subset_b": names[j],
                    "jaccard": _jaccard(a, b),
                    "shared_agebs": len(a & b),
                    "distinct_union": len(a | b),
                }
            )
    overlap_rows.sort(key=lambda r: float(r["jaccard"]), reverse=True)
    _write_csv(outdir / "top_subset_overlap.csv", overlap_rows, list(overlap_rows[0].keys()))

    counter: Counter[str] = Counter()
    for name in names:
        counter.update(top_sets[name])
    core_codes = {code for code, count in counter.items() if count >= 3}
    core_rows = [r for r in rows if r["unit_code"] in core_codes]
    if len(core_rows) >= 35:
        core_metrics = _metric_row("consensus_core_top12_ge3", core_rows, total_n)
    else:
        core_metrics = None

    core_member_rows = []
    for r in rows:
        if r["unit_code"] in counter:
            core_member_rows.append(
                {
                    "unit_code": r["unit_code"],
                    "unit_label": r["unit_label"],
                    "appearance_count_top12": counter[r["unit_code"]],
                    "in_consensus_core": 1 if r["unit_code"] in core_codes else 0,
                    "population": r["population"],
                    "est_count": r["est_count"],
                    "area_km2": r["area_km2"],
                    "dist_to_center_km": r["dist_to_center_km"],
                    "population_density": r["population_density"],
                    "compactness": r["compactness"],
                    "neighbor_degree": r["neighbor_degree"],
                }
            )
    core_member_rows.sort(key=lambda r: (int(r["appearance_count_top12"]), float(r["est_count"])), reverse=True)
    _write_csv(outdir / "consensus_core_members.csv", core_member_rows, list(core_member_rows[0].keys()))
    if core_metrics is not None:
        _write_csv(outdir / "consensus_core_summary.csv", [core_metrics], list(core_metrics.keys()))

    best = top_rows[0]
    best_codes = {m["unit_code"] for m in member_rows if m["subset_rule"] == best["subset_rule"]}
    best_edges = _fetch_touch_edges(city_code, sorted(best_codes))
    best_graph = _components(sorted(best_codes), best_edges)
    _write_csv(outdir / "best_subset_graph_stats.csv", [{**best_graph, "subset_rule": best["subset_rule"]}], ["subset_rule", "component_count", "largest_component_size", "largest_component_share", "edge_count"])

    features = _fetch_features(city_code)
    best_scores = {}
    best_summary_lookup = {r["unit_code"]: r for r in rows}
    fit = fit_ols([float(best_summary_lookup[c]["est_count"]) for c in best_codes], [float(best_summary_lookup[c]["population"]) for c in best_codes])
    for code in best_codes:
        row = best_summary_lookup[code]
        y = float(row["est_count"])
        n = float(row["population"])
        alpha = fit.alpha
        beta = fit.beta
        y_expected = math.exp(alpha + beta * math.log(max(n, 1e-9)))
        epsilon = math.log(max(y, 1e-9)) - math.log(max(y_expected, 1e-9))
        best_scores[code] = epsilon

    core_map_scores = {}
    if core_metrics is not None:
        core_fit = fit_ols([float(r["est_count"]) for r in core_rows], [float(r["population"]) for r in core_rows])
        for r in core_rows:
            y = float(r["est_count"])
            n = float(r["population"])
            y_expected = math.exp(core_fit.alpha + core_fit.beta * math.log(max(n, 1e-9)))
            epsilon = math.log(max(y, 1e-9)) - math.log(max(y_expected, 1e-9))
            core_map_scores[str(r["unit_code"])] = epsilon

    best_map = _write_ageb_map(
        city_name,
        f"Best subset: {best['subset_rule']} (n={int(best['n_obs'])}, R²={float(best['r2']):.3f})",
        features,
        best_scores,
        figdir / "best_subset_map.svg",
    )
    if core_map_scores:
        core_map = _write_ageb_map(
            city_name,
            f"Consensus core across top subsets (n={len(core_rows)})",
            features,
            core_map_scores,
            figdir / "consensus_core_map.svg",
        )
    else:
        core_map = None

    fig1 = _write_rank(figdir / "top_subset_r2_rank.svg", top_rows)
    fig2 = _write_size_vs_r2(figdir / "subset_size_vs_r2.svg", top_rows)
    fig3 = _write_overlap_table_svg(figdir / "top_subset_overlap_table.svg", overlap_rows[:18])
    manifest = [
        {"figure_id": "top_subset_r2_rank", "path": str(fig1.resolve()), "description": "Top subset rules by R²."},
        {"figure_id": "subset_size_vs_r2", "path": str(fig2.resolve()), "description": "Tradeoff between subset size and fit."},
        {"figure_id": "top_subset_overlap_table", "path": str(fig3.resolve()), "description": "Pairwise overlap among top subsets."},
        {"figure_id": "best_subset_map", "path": str(best_map.resolve()), "description": "Map of the best-fitting AGEB subset."},
    ]
    if core_map is not None:
        manifest.append({"figure_id": "consensus_core_map", "path": str(core_map.resolve()), "description": "Map of the consensus core."})
    _write_csv(figdir / "figures_manifest.csv", manifest, ["figure_id", "path", "description"])

    lines = [
        "# AGEB Subset Discovery in Guadalajara",
        "",
        "This experiment does not assume one local law for all AGEB. It searches for multiple transparent subsets defined by geometry and topology, then checks whether some of them recover a cleaner `log(Y) ~ log(N)` relation.",
        "",
        "Rules explored:",
        "- low / high quantile cuts",
        "- middle bands",
        "- selected pairwise intersections",
        "",
        "Feature axes explored:",
        "- distance to establishment center",
        "- area",
        "- population density",
        "- compactness",
        "- neighbor degree",
        "",
        "## Best subset",
        f"- rule = `{best['subset_rule']}`",
        f"- `n = {int(best['n_obs'])}`",
        f"- `beta = {float(best['beta']):+.3f}`",
        f"- `R² = {float(best['r2']):.3f}`",
        f"- `adjR² = {float(best['adj_r2']):.3f}`",
        "",
    ]
    if core_metrics is not None:
        lines.extend(
            [
                "## Consensus core",
                "The consensus core contains AGEBs that recur across the top subsets. This asks whether the good subsets are discovering the same local regime.",
                f"- `n = {int(core_metrics['n_obs'])}`",
                f"- `beta = {float(core_metrics['beta']):+.3f}`",
                f"- `R² = {float(core_metrics['r2']):.3f}`",
                f"- rule label = `consensus_core_top12_ge3`",
                "",
            ]
        )
    lines.extend(
        [
            "## Files",
            f"- [ageb_feature_table.csv]({(outdir / 'ageb_feature_table.csv').resolve()})",
            f"- [subset_search_summary.csv]({(outdir / 'subset_search_summary.csv').resolve()})",
            f"- [top_subsets.csv]({(outdir / 'top_subsets.csv').resolve()})",
            f"- [subset_members_long.csv]({(outdir / 'subset_members_long.csv').resolve()})",
            f"- [top_subset_overlap.csv]({(outdir / 'top_subset_overlap.csv').resolve()})",
            f"- [consensus_core_members.csv]({(outdir / 'consensus_core_members.csv').resolve()})",
            f"- [best_subset_graph_stats.csv]({(outdir / 'best_subset_graph_stats.csv').resolve()})",
            "",
            "## Figures",
            f"- [top_subset_r2_rank.svg]({fig1.resolve()})",
            f"- [subset_size_vs_r2.svg]({fig2.resolve()})",
            f"- [top_subset_overlap_table.svg]({fig3.resolve()})",
            f"- [best_subset_map.svg]({best_map.resolve()})",
        ]
    )
    if core_map is not None:
        lines.append(f"- [consensus_core_map.svg]({core_map.resolve()})")

    (outdir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

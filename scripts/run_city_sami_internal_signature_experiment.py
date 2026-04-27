#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
import random
import shutil
import subprocess
from collections import defaultdict
from itertools import combinations
from pathlib import Path

from urban_sami.analysis.linear_models import ols_fit
from urban_sami.modeling.fit import compute_deviation_score, fit_ols


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

TARGET_SIZES = [40, 80, 120]
N_STARTS = 10
MAX_STEPS = 220
STALL_LIMIT = 60
RANDOM_SEED = 20260422


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


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _mean(values: list[float]) -> float:
    return sum(values) / float(len(values)) if values else 0.0


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mu = _mean(values)
    return math.sqrt(sum((v - mu) ** 2 for v in values) / float(len(values) - 1))


def _corr(x: list[float], y: list[float]) -> float:
    if len(x) != len(y) or len(x) < 2:
        return 0.0
    mx = _mean(x)
    my = _mean(y)
    sxx = sum((v - mx) ** 2 for v in x)
    syy = sum((v - my) ** 2 for v in y)
    sxy = sum((a - mx) * (b - my) for a, b in zip(x, y))
    if sxx <= 0 or syy <= 0:
        return 0.0
    return sxy / math.sqrt(sxx * syy)


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


def _fetch_ageb_city_list() -> list[dict[str, str]]:
    return _query_tsv(
        """
        SELECT city_code, COALESCE(MAX(city_name), '') AS city_name, COUNT(*)::text AS n_ageb
        FROM raw.admin_units
        WHERE level = 'ageb_u'
        GROUP BY city_code
        HAVING COUNT(*) >= 40
        ORDER BY COUNT(*) DESC, city_code
        """.strip(),
        ["city_code", "city_name", "n_ageb"],
    )


def _fetch_ageb_rows(city_code: str) -> list[dict[str, str]]:
    return _query_tsv(
        f"""
        WITH city_points AS (
            SELECT scian_code, per_ocu, ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geom
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
            SELECT
                a.unit_code,
                COUNT(p.*)::int AS est_count,
                COUNT(*) FILTER (WHERE substring(COALESCE(p.scian_code,''),1,2) = '81')::int AS sc81,
                COUNT(*) FILTER (WHERE substring(COALESCE(p.scian_code,''),1,2) = '46')::int AS sc46,
                COUNT(*) FILTER (WHERE substring(COALESCE(p.scian_code,''),1,2) = '31')::int AS sc31,
                COUNT(*) FILTER (WHERE substring(COALESCE(p.scian_code,''),1,2) = '62')::int AS sc62,
                COUNT(*) FILTER (WHERE substring(COALESCE(p.scian_code,''),1,2) = '54')::int AS sc54,
                COUNT(*) FILTER (WHERE lower(COALESCE(p.per_ocu,'')) LIKE '0 a 5%')::int AS micro,
                COUNT(*) FILTER (
                    WHERE lower(COALESCE(p.per_ocu,'')) LIKE '31 a 50%'
                       OR lower(COALESCE(p.per_ocu,'')) LIKE '51 a 100%'
                       OR lower(COALESCE(p.per_ocu,'')) LIKE '101 a 250%'
                )::int AS medium
            FROM ageb a
            LEFT JOIN city_points p ON ST_Covers(a.geom, p.geom)
            GROUP BY a.unit_code
        ),
        geom_stats AS (
            SELECT
                a.unit_code,
                ST_Area(a.geom::geography) / 1000000.0 AS area_km2,
                ST_DistanceSphere(ST_Centroid(a.geom), cc.geom) / 1000.0 AS dist_to_center_km
            FROM ageb a
            CROSS JOIN city_center cc
        )
        SELECT
            a.unit_code,
            a.unit_label,
            COALESCE(a.population, 0)::text AS population,
            c.est_count::text AS est_count,
            g.area_km2::text AS area_km2,
            g.dist_to_center_km::text AS dist_to_center_km,
            c.sc81::text,
            c.sc46::text,
            c.sc31::text,
            c.sc62::text,
            c.sc54::text,
            c.micro::text,
            c.medium::text
        FROM ageb a
        JOIN counts c ON c.unit_code = a.unit_code
        JOIN geom_stats g ON g.unit_code = a.unit_code
        ORDER BY a.unit_code
        """.strip(),
        [
            "unit_code",
            "unit_label",
            "population",
            "est_count",
            "area_km2",
            "dist_to_center_km",
            "sc81",
            "sc46",
            "sc31",
            "sc62",
            "sc54",
            "micro",
            "medium",
        ],
    )


def _score_subset(indices: list[int], pop: list[float], est: list[float]) -> dict[str, float]:
    fit = fit_ols([est[i] for i in indices], [pop[i] for i in indices])
    return {"beta": fit.beta, "r2": fit.r2, "adj_r2": _adj_r2(fit.r2, len(indices), 1)}


def _improve_subset(target_size: int, pop: list[float], est: list[float], rng: random.Random) -> tuple[list[int], dict[str, float]]:
    n_total = len(pop)
    current = sorted(rng.sample(range(n_total), target_size))
    current_set = set(current)
    current_score = _score_subset(current, pop, est)
    best = list(current)
    best_score = dict(current_score)
    stall = 0
    for _ in range(MAX_STEPS):
        if stall >= STALL_LIMIT:
            break
        out_idx = rng.choice(current)
        in_idx = rng.randrange(n_total)
        if in_idx in current_set:
            continue
        trial = [i for i in current if i != out_idx] + [in_idx]
        trial.sort()
        trial_score = _score_subset(trial, pop, est)
        better = (
            trial_score["adj_r2"] > current_score["adj_r2"] + 1e-12
            or (
                abs(trial_score["adj_r2"] - current_score["adj_r2"]) <= 1e-12
                and trial_score["r2"] > current_score["r2"] + 1e-12
            )
        )
        if better:
            current = trial
            current_set = set(current)
            current_score = trial_score
            stall = 0
            if (
                current_score["adj_r2"] > best_score["adj_r2"] + 1e-12
                or (
                    abs(current_score["adj_r2"] - best_score["adj_r2"]) <= 1e-12
                    and current_score["r2"] > best_score["r2"] + 1e-12
                )
            ):
                best = list(current)
                best_score = dict(current_score)
        else:
            stall += 1
    return best, best_score


def _city_internal_signature(city_code: str, city_name: str, rng_seed: int) -> dict[str, object] | None:
    raw_rows = _fetch_ageb_rows(city_code)
    rows = []
    for r in raw_rows:
        population = _safe_float(r["population"])
        est = _safe_float(r["est_count"])
        if population <= 0 or est <= 0:
            continue
        area = max(_safe_float(r["area_km2"]), 1e-9)
        total = max(est, 1e-9)
        rows.append(
            {
                "unit_code": r["unit_code"],
                "population": population,
                "est_count": est,
                "log_population": math.log(population),
                "log_est_count": math.log(est),
                "log_density": math.log(max(population / area, 1e-9)),
                "log_distance": math.log(max(_safe_float(r["dist_to_center_km"]), 1e-9)),
                "share_81": _safe_float(r["sc81"]) / total,
                "share_46": _safe_float(r["sc46"]) / total,
                "share_31": _safe_float(r["sc31"]) / total,
                "share_62": _safe_float(r["sc62"]) / total,
                "share_54": _safe_float(r["sc54"]) / total,
                "share_micro": _safe_float(r["micro"]) / total,
                "share_medium": _safe_float(r["medium"]) / total,
            }
        )
    if len(rows) < 60:
        return None

    y = [r["log_est_count"] for r in rows]
    m0 = ols_fit([[1.0, r["log_population"]] for r in rows], y)
    m1 = ols_fit([[1.0, r["log_population"], r["log_density"]] for r in rows], y)
    m2 = ols_fit([[1.0, r["log_population"], r["log_distance"]] for r in rows], y)
    m3 = ols_fit(
        [
            [
                1.0,
                r["log_population"],
                r["log_density"],
                r["share_81"],
                r["share_46"],
                r["share_31"],
                r["share_62"],
                r["share_54"],
                r["share_micro"],
                r["share_medium"],
            ]
            for r in rows
        ],
        y,
    )

    pop = [r["population"] for r in rows]
    est = [r["est_count"] for r in rows]
    rng = random.Random(rng_seed)
    best_sets: dict[int, tuple[list[int], dict[str, float]]] = {}
    for target in TARGET_SIZES:
        target_eff = min(target, len(rows) - 5)
        if target_eff < 35:
            continue
        best_indices = None
        best_score = None
        for _ in range(N_STARTS):
            subset_indices, score = _improve_subset(target_eff, pop, est, rng)
            if best_score is None or score["adj_r2"] > best_score["adj_r2"] + 1e-12 or (
                abs(score["adj_r2"] - best_score["adj_r2"]) <= 1e-12 and score["r2"] > best_score["r2"] + 1e-12
            ):
                best_indices = subset_indices
                best_score = score
        assert best_indices is not None and best_score is not None
        best_sets[target_eff] = (best_indices, best_score)

    appear = defaultdict(int)
    for target, (idxs, _) in best_sets.items():
        for idx in idxs:
            appear[rows[idx]["unit_code"]] += 1
    consensus_codes = {code for code, c in appear.items() if c >= 2}
    consensus_rows = [r for r in rows if r["unit_code"] in consensus_codes]
    if len(consensus_rows) >= 35:
        consensus_fit = fit_ols([r["est_count"] for r in consensus_rows], [r["population"] for r in consensus_rows])
        consensus_r2 = consensus_fit.r2
        consensus_beta = consensus_fit.beta
        consensus_share = len(consensus_rows) / float(len(rows))
    else:
        consensus_r2 = 0.0
        consensus_beta = 0.0
        consensus_share = 0.0

    return {
        "city_code": city_code,
        "city_name": city_name,
        "ageb_n": len(rows),
        "ageb_m0_beta": m0.coefficients[1],
        "ageb_m0_r2": m0.r2,
        "ageb_m1_density_adj_r2": m1.adj_r2,
        "ageb_m2_distance_adj_r2": m2.adj_r2,
        "ageb_m3_full_adj_r2": m3.adj_r2,
        "density_gain": m1.adj_r2 - m0.adj_r2,
        "distance_gain": m2.adj_r2 - m0.adj_r2,
        "full_gain": m3.adj_r2 - m0.adj_r2,
        "subset40_r2": best_sets[min(best_sets)][1]["r2"] if best_sets else 0.0,
        "subset80_r2": best_sets.get(80, best_sets.get(79, ([0], {"r2": 0.0} )))[1]["r2"] if best_sets else 0.0,
        "subset120_r2": best_sets.get(120, best_sets.get(max(best_sets), ([0], {"r2": 0.0})))[1]["r2"] if best_sets else 0.0,
        "consensus_r2": consensus_r2,
        "consensus_beta": consensus_beta,
        "consensus_share": consensus_share,
        "mean_log_density": _mean([r["log_density"] for r in rows]),
        "std_log_density": _std([r["log_density"] for r in rows]),
        "mean_share_81": _mean([r["share_81"] for r in rows]),
        "mean_share_46": _mean([r["share_46"] for r in rows]),
        "mean_share_54": _mean([r["share_54"] for r in rows]),
        "mean_share_micro": _mean([r["share_micro"] for r in rows]),
    }


def _distance(a: dict[str, float], b: dict[str, float], features: list[str], means: dict[str, float], stds: dict[str, float]) -> float:
    total = 0.0
    for feat in features:
        denom = stds[feat] if stds[feat] > 0 else 1.0
        total += (((float(a[feat]) - float(b[feat])) / denom) ** 2)
    return math.sqrt(total)


def _write_scatter(path: Path, x: list[float], y: list[float], labels: list[str], title: str, subtitle: str, xlabel: str, ylabel: str) -> Path:
    width = 1040
    height = 620
    left = 88
    right = 40
    top = 96
    bottom = 88
    plot_w = width - left - right
    plot_h = height - top - bottom
    xmin, xmax = min(x), max(x)
    ymin, ymax = min(y), max(y)
    xpad = (xmax - xmin) * 0.06 if xmax > xmin else 1.0
    ypad = (ymax - ymin) * 0.06 if ymax > ymin else 1.0
    xmin -= xpad
    xmax += xpad
    ymin -= ypad
    ymax += ypad

    def px(v: float) -> float:
        return left + ((v - xmin) / max(xmax - xmin, 1e-9)) * plot_w

    def py(v: float) -> float:
        return top + plot_h - ((v - ymin) / max(ymax - ymin, 1e-9)) * plot_h

    corr = _corr(x, y)
    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">{subtitle} corr={corr:+.3f}</text>',
        f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="none" stroke="{AXIS}"/>',
    ]
    for xv, yv, label in zip(x, y, labels):
        body.append(f'<circle cx="{px(xv):.2f}" cy="{py(yv):.2f}" r="4.2" fill="{BLUE}" fill-opacity="0.8"/>')
        body.append(f'<text x="{px(xv)+6:.2f}" y="{py(yv)-6:.2f}" font-size="10" font-family="{SANS}" fill="{TEXT}">{label}</text>')
    body.append(f'<text x="{left + plot_w/2:.2f}" y="{height-24}" text-anchor="middle" font-size="12" font-family="{SANS}" fill="{MUTED}">{xlabel}</text>')
    body.append(f'<text x="24" y="{top + plot_h/2:.2f}" transform="rotate(-90 24 {top + plot_h/2:.2f})" text-anchor="middle" font-size="12" font-family="{SANS}" fill="{MUTED}">{ylabel}</text>')
    return _svg(path, width, height, "".join(body))


def _write_neighbor_table(path: Path, rows: list[dict[str, object]]) -> Path:
    width = 1280
    col_x = [44, 180, 430, 680, 900, 1100]
    row_h = 28
    top = 106
    height = top + row_h * len(rows) + 50
    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">Nearest neighbors by SAMI versus internal signature</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">This checks whether SAMI-near cities are also internal-structure-near.</text>',
    ]
    headers = ["city", "nn_sami", "nn_internal", "sami_dist", "internal_dist", "same_neighbor"]
    for x, h in zip(col_x, headers):
        body.append(f'<text x="{x}" y="{top-18}" font-size="12" font-family="{SANS}" fill="{MUTED}">{h}</text>')
    for i, row in enumerate(rows):
        y = top + i * row_h
        vals = [
            str(row["city_name"]),
            str(row["nn_sami"]),
            str(row["nn_internal"]),
            f'{float(row["sami_distance"]):.3f}',
            f'{float(row["internal_distance"]):.3f}',
            str(int(row["same_neighbor"])),
        ]
        for x, v in zip(col_x, vals):
            body.append(f'<text x="{x}" y="{y}" font-size="12" font-family="{SANS}" fill="{TEXT}">{v}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "city-sami-vs-internal-structure-2026-04-22"
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    city_counts = _read_csv(root / "dist" / "independent_city_baseline" / "city_counts.csv")
    city_fit = fit_ols([_safe_float(r["est_count"]) for r in city_counts if _safe_float(r["population"]) > 0 and _safe_float(r["est_count"]) > 0], [_safe_float(r["population"]) for r in city_counts if _safe_float(r["population"]) > 0 and _safe_float(r["est_count"]) > 0])
    city_sami = {}
    city_info = {}
    for r in city_counts:
        pop = _safe_float(r["population"])
        est = _safe_float(r["est_count"])
        if pop <= 0 or est <= 0:
            continue
        score = compute_deviation_score(est, pop, city_fit.alpha, city_fit.beta, city_fit.residual_std)
        city_sami[r["city_code"]] = score.sami
        city_info[r["city_code"]] = {"city_name": r["city_name"], "population": pop, "est_count": est}

    ageb_cities = _fetch_ageb_city_list()
    sample = [c for c in ageb_cities if c["city_code"] in city_sami]

    signature_rows = []
    for idx, c in enumerate(sample):
        city_code = c["city_code"]
        city_name = city_info[city_code]["city_name"] or c["city_name"] or city_code
        sig = _city_internal_signature(city_code, city_name, RANDOM_SEED + idx)
        if sig is None:
            continue
        sig["city_sami"] = city_sami[city_code]
        sig["city_population"] = city_info[city_code]["population"]
        sig["city_est_count"] = city_info[city_code]["est_count"]
        signature_rows.append(sig)

    feature_names = [
        "ageb_m0_beta",
        "ageb_m0_r2",
        "density_gain",
        "distance_gain",
        "full_gain",
        "subset40_r2",
        "subset80_r2",
        "subset120_r2",
        "consensus_r2",
        "consensus_beta",
        "consensus_share",
        "mean_log_density",
        "std_log_density",
        "mean_share_81",
        "mean_share_46",
        "mean_share_54",
        "mean_share_micro",
    ]
    means = {f: _mean([float(r[f]) for r in signature_rows]) for f in feature_names}
    stds = {f: max(_std([float(r[f]) for r in signature_rows]), 1e-9) for f in feature_names}

    pair_rows = []
    sami_distances = []
    internal_distances = []
    for a, b in combinations(signature_rows, 2):
        ds = abs(float(a["city_sami"]) - float(b["city_sami"]))
        di = _distance(a, b, feature_names, means, stds)
        pair_rows.append(
            {
                "city_code_a": a["city_code"],
                "city_name_a": a["city_name"],
                "city_code_b": b["city_code"],
                "city_name_b": b["city_name"],
                "sami_distance": ds,
                "internal_distance": di,
            }
        )
        sami_distances.append(ds)
        internal_distances.append(di)
    pair_rows.sort(key=lambda r: float(r["sami_distance"]))
    _write_csv(outdir / "city_pair_distances.csv", pair_rows, list(pair_rows[0].keys()))

    nearest_rows = []
    for a in signature_rows:
        others = [b for b in signature_rows if b["city_code"] != a["city_code"]]
        nn_sami = min(others, key=lambda b: abs(float(a["city_sami"]) - float(b["city_sami"])))
        nn_int = min(others, key=lambda b: _distance(a, b, feature_names, means, stds))
        nearest_rows.append(
            {
                "city_code": a["city_code"],
                "city_name": a["city_name"],
                "nn_sami": nn_sami["city_name"],
                "nn_internal": nn_int["city_name"],
                "sami_distance": abs(float(a["city_sami"]) - float(nn_sami["city_sami"])),
                "internal_distance": _distance(a, nn_int, feature_names, means, stds),
                "same_neighbor": 1 if nn_sami["city_code"] == nn_int["city_code"] else 0,
            }
        )
    _write_csv(outdir / "nearest_neighbor_comparison.csv", nearest_rows, list(nearest_rows[0].keys()))

    sami_sorted = sorted(float(r["city_sami"]) for r in signature_rows)
    q1 = sami_sorted[len(sami_sorted) // 3]
    q2 = sami_sorted[(2 * len(sami_sorted)) // 3]
    for r in signature_rows:
        s = float(r["city_sami"])
        if s <= q1:
            r["sami_band"] = "low"
        elif s <= q2:
            r["sami_band"] = "mid"
        else:
            r["sami_band"] = "high"
    band_rows = []
    for band in ["low", "mid", "high"]:
        band_cities = [r for r in signature_rows if r["sami_band"] == band]
        band_pairs = [
            _distance(a, b, feature_names, means, stds)
            for a, b in combinations(band_cities, 2)
        ]
        cross_pairs = []
        rest = [r for r in signature_rows if r["sami_band"] != band]
        for a in band_cities:
            for b in rest:
                cross_pairs.append(_distance(a, b, feature_names, means, stds))
        band_rows.append(
            {
                "sami_band": band,
                "n_cities": len(band_cities),
                "mean_within_internal_distance": _mean(band_pairs),
                "mean_cross_internal_distance": _mean(cross_pairs),
                "within_minus_cross": _mean(band_pairs) - _mean(cross_pairs),
            }
        )
    _write_csv(outdir / "sami_band_internal_distance.csv", band_rows, list(band_rows[0].keys()))

    summary_rows = [
        {
            "n_cities": len(signature_rows),
            "city_beta": city_fit.beta,
            "city_r2": city_fit.r2,
            "pair_corr_sami_vs_internal": _corr(sami_distances, internal_distances),
            "same_nearest_neighbor_rate": _mean([float(r["same_neighbor"]) for r in nearest_rows]),
        }
    ]
    _write_csv(outdir / "summary.csv", summary_rows, list(summary_rows[0].keys()))
    _write_csv(outdir / "city_internal_signatures.csv", signature_rows, list(signature_rows[0].keys()))

    fig1 = _write_scatter(
        figdir / "pair_distance_scatter.svg",
        sami_distances,
        internal_distances,
        ["" for _ in sami_distances],
        "Pairwise SAMI distance versus internal-structure distance",
        "Each point is a city pair.",
        "SAMI distance",
        "Internal signature distance",
    )
    fig2 = _write_neighbor_table(figdir / "nearest_neighbor_table.svg", nearest_rows)
    x = [float(r["city_sami"]) for r in signature_rows]
    y = [float(r["consensus_r2"]) for r in signature_rows]
    labels = [str(r["city_name"]) for r in signature_rows]
    fig3 = _write_scatter(
        figdir / "sami_vs_consensus_r2.svg",
        x,
        y,
        labels,
        "City SAMI versus internal consensus-core fit",
        "This checks whether aggregate deviation aligns with the strength of the best internal regime.",
        "City SAMI",
        "Consensus-core R²",
    )
    _write_csv(
        figdir / "figures_manifest.csv",
        [
            {"figure_id": "pair_distance_scatter", "path": str(fig1.resolve()), "description": "Pairwise SAMI distance vs internal distance."},
            {"figure_id": "nearest_neighbor_table", "path": str(fig2.resolve()), "description": "Nearest-neighbor comparison."},
            {"figure_id": "sami_vs_consensus_r2", "path": str(fig3.resolve()), "description": "SAMI vs consensus-core R²."},
        ],
        ["figure_id", "path", "description"],
    )

    lines = [
        "# City SAMI Versus Internal Structure",
        "",
        "This experiment asks whether cities that are close in national SAMI are also close in intra-urban structure.",
        "",
        "Internal signature per city includes:",
        "- AGEB total-law beta and R²",
        "- gain from density and distance models",
        "- gain from a fuller density+composition model",
        "- best unconstrained subset fits at fixed sizes",
        "- consensus-core size and fit",
        "",
        "## Summary",
        f"- cities analyzed = `{len(signature_rows)}`",
        f"- national city law: `beta = {city_fit.beta:+.3f}`, `R² = {city_fit.r2:.3f}`",
        f"- corr(pairwise SAMI distance, internal distance) = `{summary_rows[0]['pair_corr_sami_vs_internal']:+.3f}`",
        f"- same nearest-neighbor rate = `{summary_rows[0]['same_nearest_neighbor_rate']:.3f}`",
        "",
        "## Files",
        f"- [summary.csv]({(outdir / 'summary.csv').resolve()})",
        f"- [city_internal_signatures.csv]({(outdir / 'city_internal_signatures.csv').resolve()})",
        f"- [city_pair_distances.csv]({(outdir / 'city_pair_distances.csv').resolve()})",
        f"- [nearest_neighbor_comparison.csv]({(outdir / 'nearest_neighbor_comparison.csv').resolve()})",
        f"- [sami_band_internal_distance.csv]({(outdir / 'sami_band_internal_distance.csv').resolve()})",
        "",
        "## Figures",
        f"- [pair_distance_scatter.svg]({fig1.resolve()})",
        f"- [nearest_neighbor_table.svg]({fig2.resolve()})",
        f"- [sami_vs_consensus_r2.svg]({fig3.resolve()})",
    ]
    (outdir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

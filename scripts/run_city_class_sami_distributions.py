#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import shutil
from collections import Counter
from pathlib import Path

from urban_sami.analysis.linear_models import compare_nested_models, ols_fit


BG = "#f8f6f1"
PANEL = "#fffdf8"
GRID = "#ddd6c8"
TEXT = "#1f1f1f"
MUTED = "#625d54"
TEAL = "#0f766e"
RUST = "#b14d3b"
BLUE = "#2563eb"
GOLD = "#b45309"
ROSE = "#be185d"
SLATE = "#475569"
OLIVE = "#65743a"
SERIF = "Georgia, 'Times New Roman', serif"
SANS = "Helvetica, Arial, sans-serif"
PALETTE = [TEAL, RUST, BLUE, GOLD, ROSE, SLATE, OLIVE]


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


def _svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
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


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    idx = q * (len(ordered) - 1)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return ordered[lo]
    frac = idx - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def _fit_class_mean_model(rows: list[dict[str, object]], class_order: list[str], base_class: str) -> tuple[list[dict[str, object]], list[dict[str, object]], dict[str, float]]:
    y = [float(r["sami"]) for r in rows]

    x0 = [[1.0] for _ in rows]
    fit0 = ols_fit(x0, y)

    others = [c for c in class_order if c != base_class]
    x1 = []
    for row in rows:
        vec = [1.0]
        for c in others:
            vec.append(1.0 if row["class_label"] == c else 0.0)
        x1.append(vec)
    fit1 = ols_fit(x1, y)
    cmp01 = compare_nested_models(fit0, fit1)

    model_summary = [
        {
            "model_id": "M0_intercept_only",
            "description": "SAMI = mu + eta",
            "n_obs": fit0.n_obs,
            "n_params": fit0.n_params,
            "adj_r2": fit0.adj_r2,
            "r2": fit0.r2,
            "rss": fit0.rss,
        },
        {
            "model_id": "M1_class_means",
            "description": f"SAMI = mu + gamma_c + eta, base class = {base_class}",
            "n_obs": fit1.n_obs,
            "n_params": fit1.n_params,
            "adj_r2": fit1.adj_r2,
            "r2": fit1.r2,
            "rss": fit1.rss,
        },
    ]
    nested_tests = [
        {
            "comparison": "M0_to_M1",
            "null": "topology classes do not shift mean SAMI",
            "f_stat": cmp01.f_stat,
            "df_num": cmp01.df_num,
            "df_den": cmp01.df_den,
            "p_value": cmp01.p_value,
        }
    ]

    class_means = {base_class: fit1.coefficients[0]}
    for idx, c in enumerate(others):
        class_means[c] = fit1.coefficients[0] + fit1.coefficients[1 + idx]
    return model_summary, nested_tests, class_means


def _cramers_v(table: list[list[int]]) -> tuple[float, float]:
    n = sum(sum(row) for row in table)
    if n <= 0:
        return 0.0, 0.0
    n_rows = len(table)
    n_cols = len(table[0]) if table else 0
    row_sums = [sum(row) for row in table]
    col_sums = [sum(table[i][j] for i in range(n_rows)) for j in range(n_cols)]
    chi2 = 0.0
    for i in range(n_rows):
        for j in range(n_cols):
            expected = row_sums[i] * col_sums[j] / float(n)
            if expected > 0:
                chi2 += ((table[i][j] - expected) ** 2) / expected
    denom = n * max(1, min(n_rows - 1, n_cols - 1))
    return (math.sqrt(chi2 / denom) if denom > 0 else 0.0, chi2)


def _distribution_svg(path: Path, rows: list[dict[str, object]], class_order: list[str], color_map: dict[str, str]) -> Path:
    ordered = sorted(rows, key=lambda r: float(r["mean_sami"]), reverse=True)
    width = 1280
    left = 330
    right = 90
    top = 96
    bottom = 76
    row_h = 54
    height = top + len(ordered) * row_h + bottom
    xmin = min(float(r["min_sami"]) for r in ordered)
    xmax = max(float(r["max_sami"]) for r in ordered)
    xmin = min(xmin, -0.9)
    xmax = max(xmax, 0.9)
    xr = max(xmax - xmin, 1e-9)

    def px(v: float) -> float:
        return left + ((v - xmin) / xr) * (width - left - right)

    zero_x = px(0.0)
    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="20" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="28" font-family="{SERIF}" fill="{TEXT}">SAMI Distribution by Topology Class</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Whiskers = p10-p90, box = p25-p75, black tick = median, diamond = mean. Global zero line shown.</text>',
        f'<line x1="{zero_x:.2f}" y1="{top-8}" x2="{zero_x:.2f}" y2="{height-bottom+10}" stroke="{GRID}" stroke-dasharray="4 4"/>',
    ]
    for i, row in enumerate(ordered):
        y = top + i * row_h
        cy = y + 16
        color = color_map[row["class_label"]]
        p10 = float(row["p10_sami"])
        p25 = float(row["p25_sami"])
        med = float(row["median_sami"])
        p75 = float(row["p75_sami"])
        p90 = float(row["p90_sami"])
        mean = float(row["mean_sami"])
        body.append(f'<text x="{left-12}" y="{cy+4:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{row["class_label"]}</text>')
        body.append(f'<line x1="{px(p10):.2f}" y1="{cy:.2f}" x2="{px(p90):.2f}" y2="{cy:.2f}" stroke="{color}" stroke-width="5"/>')
        body.append(f'<rect x="{px(p25):.2f}" y="{cy-10:.2f}" width="{max(1.5, px(p75)-px(p25)):.2f}" height="20" fill="{color}" fill-opacity="0.18" stroke="{color}"/>')
        body.append(f'<line x1="{px(med):.2f}" y1="{cy-12:.2f}" x2="{px(med):.2f}" y2="{cy+12:.2f}" stroke="{TEXT}" stroke-width="2"/>')
        mx = px(mean)
        diamond = f"{mx:.2f},{cy-7:.2f} {mx+7:.2f},{cy:.2f} {mx:.2f},{cy+7:.2f} {mx-7:.2f},{cy:.2f}"
        body.append(f'<polygon points="{diamond}" fill="{color}" stroke="{TEXT}" stroke-width="0.8"/>')
        body.append(f'<text x="{px(p90)+10:.2f}" y="{cy+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">mean {mean:+.3f}</text>')
    return _svg(path, width, height, "".join(body))


def _mix_svg(path: Path, band_rows: list[dict[str, object]], class_order: list[str], color_map: dict[str, str]) -> Path:
    width = 1280
    height = 760
    left = 110
    right = 120
    top = 104
    bottom = 110
    plot_w = width - left - right
    plot_h = height - top - bottom
    bar_gap = 16
    n_bands = max(int(r["sami_decile"]) for r in band_rows)
    bar_w = (plot_w - (n_bands - 1) * bar_gap) / float(n_bands)
    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="20" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="28" font-family="{SERIF}" fill="{TEXT}">Topology-Class Mix Across SAMI Deciles</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Each bar is one SAMI decile, normalized to 100% composition across classes.</text>',
    ]
    for g in range(6):
        y = top + g * plot_h / 5.0
        share = 1.0 - g / 5.0
        body.append(f'<line x1="{left:.2f}" y1="{y:.2f}" x2="{left+plot_w:.2f}" y2="{y:.2f}" stroke="{GRID}" stroke-opacity="0.7"/>')
        body.append(f'<text x="{left-10}" y="{y+4:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{MUTED}">{share:.1f}</text>')
    for band in range(1, n_bands + 1):
        x0 = left + (band - 1) * (bar_w + bar_gap)
        subset = [r for r in band_rows if int(r["sami_decile"]) == band]
        y_cursor = top + plot_h
        for class_label in class_order:
            row = next(r for r in subset if r["class_label"] == class_label)
            share = float(row["share_within_decile"])
            h = share * plot_h
            y_cursor -= h
            body.append(f'<rect x="{x0:.2f}" y="{y_cursor:.2f}" width="{bar_w:.2f}" height="{h:.2f}" fill="{color_map[class_label]}"/>')
        body.append(f'<text x="{x0+bar_w/2:.2f}" y="{top+plot_h+24:.2f}" text-anchor="middle" font-size="12" font-family="{SANS}" fill="{TEXT}">{band}</text>')
    legend_x = width - 280
    legend_y = top + 18
    for idx, class_label in enumerate(class_order):
        y = legend_y + idx * 26
        body.append(f'<rect x="{legend_x}" y="{y-10}" width="14" height="14" fill="{color_map[class_label]}"/>')
        body.append(f'<text x="{legend_x+22}" y="{y+2}" font-size="12" font-family="{SANS}" fill="{TEXT}">{class_label}</text>')
    return _svg(path, width, height, "".join(body))


def _neighbor_svg(path: Path, rows: list[dict[str, object]]) -> Path:
    width = 880
    height = 560
    left = 110
    right = 80
    top = 110
    bottom = 90
    plot_w = width - left - right
    plot_h = height - top - bottom
    ymax = max(float(r["rate"]) for r in rows) * 1.15
    ymax = max(ymax, 0.3)
    bar_w = plot_w / (len(rows) * 1.8)
    gap = bar_w * 0.8
    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="20" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="28" font-family="{SERIF}" fill="{TEXT}">Does SAMI Recover Topology Class?</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Bars compare class agreement among nearest SAMI neighbors against baselines.</text>',
    ]
    for g in range(5):
        y = top + g * plot_h / 4.0
        value = ymax * (1.0 - g / 4.0)
        body.append(f'<line x1="{left:.2f}" y1="{y:.2f}" x2="{left+plot_w:.2f}" y2="{y:.2f}" stroke="{GRID}" stroke-opacity="0.7"/>')
        body.append(f'<text x="{left-10}" y="{y+4:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{MUTED}">{value:.2f}</text>')
    for idx, row in enumerate(rows):
        x = left + idx * (bar_w + gap) + 24
        h = (float(row["rate"]) / ymax) * plot_h
        y = top + plot_h - h
        color = row["color"]
        body.append(f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_w:.2f}" height="{h:.2f}" fill="{color}"/>')
        body.append(f'<text x="{x+bar_w/2:.2f}" y="{y-8:.2f}" text-anchor="middle" font-size="12" font-family="{SANS}" fill="{TEXT}">{float(row["rate"]):.3f}</text>')
        body.append(f'<text x="{x+bar_w/2:.2f}" y="{top+plot_h+24:.2f}" text-anchor="middle" font-size="12" font-family="{SANS}" fill="{TEXT}">{row["label"]}</text>')
    return _svg(path, width, height, "".join(body))


def _pair_table_svg(path: Path, rows: list[dict[str, object]]) -> Path:
    width = 1500
    col_x = [44, 300, 560, 820, 1080, 1340]
    row_h = 28
    top = 106
    height = top + row_h * len(rows) + 50
    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">Same-SAMI, different-topology pairs</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Consecutive cities in SAMI order with nearly identical SAMI but different topology classes.</text>',
    ]
    headers = ["city_a", "class_a", "city_b", "class_b", "SAMI dist", "SAMI level"]
    for x, h in zip(col_x, headers):
        body.append(f'<text x="{x}" y="{top-18}" font-size="12" font-family="{SANS}" fill="{MUTED}">{h}</text>')
    for i, row in enumerate(rows):
        y = top + i * row_h
        vals = [
            str(row["city_name_a"]),
            str(row["class_a"]),
            str(row["city_name_b"]),
            str(row["class_b"]),
            f'{float(row["sami_distance"]):.2e}',
            f'{float(row["mean_sami"]):+.3f}',
        ]
        for x, v in zip(col_x, vals):
            body.append(f'<text x="{x}" y="{y}" font-size="12" font-family="{SANS}" fill="{TEXT}">{v}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    parser = argparse.ArgumentParser(description="Study how global city SAMI aligns or mixes with network-topology classes.")
    parser.add_argument(
        "--residuals-csv",
        type=Path,
        default=Path("dist/independent_city_baseline/residuals.csv"),
    )
    parser.add_argument(
        "--cluster-members-csv",
        type=Path,
        default=Path("reports/city-network-typology-2026-04-24/cluster_members.csv"),
    )
    parser.add_argument(
        "--cluster-summary-csv",
        type=Path,
        default=Path("reports/city-network-typology-2026-04-24/cluster_summary.csv"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports/city-class-sami-distributions-2026-04-24"),
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    outdir = root / args.output_dir
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    residual_rows = _read_csv(root / args.residuals_csv)
    cluster_members = _read_csv(root / args.cluster_members_csv)
    cluster_summary = _read_csv(root / args.cluster_summary_csv)
    class_map = {r["city_code"]: r for r in cluster_members}
    class_order = [r["class_label"] for r in cluster_summary]
    rep_lookup = {r["class_label"]: r for r in cluster_summary}
    color_map = {class_label: PALETTE[idx % len(PALETTE)] for idx, class_label in enumerate(class_order)}

    rows: list[dict[str, object]] = []
    for row in residual_rows:
        class_row = class_map.get(str(row["city_code"]))
        if not class_row:
            continue
        rows.append(
            {
                "city_code": row["city_code"],
                "city_name": row["city_name"],
                "state_code": row["state_code"],
                "population": _safe_float(row["population"]),
                "est_count": _safe_float(row["est_count"]),
                "sami": _safe_float(row["sami"]),
                "epsilon_log": _safe_float(row["epsilon_log"]),
                "z_residual": _safe_float(row["z_residual"]),
                "class_label": class_row["class_label"],
                "cluster_id": class_row["cluster_id"],
                "is_representative": class_row["is_representative"],
            }
        )

    rows.sort(key=lambda r: float(r["sami"]))
    total = len(rows)
    for idx, row in enumerate(rows, start=1):
        row["sami_decile"] = min(10, math.ceil(10.0 * idx / total))
        row["sami_rank"] = idx

    base_class = max(class_order, key=lambda c: sum(1 for r in rows if r["class_label"] == c))
    model_summary, nested_tests, class_means = _fit_class_mean_model(rows, class_order, base_class)

    class_summary_rows: list[dict[str, object]] = []
    for class_label in class_order:
        subset = [r for r in rows if r["class_label"] == class_label]
        samis = [float(r["sami"]) for r in subset]
        rep = rep_lookup[class_label]
        class_summary_rows.append(
            {
                "class_label": class_label,
                "cluster_id": rep["cluster_id"],
                "n_cities": len(subset),
                "mean_sami": _mean(samis),
                "median_sami": _quantile(samis, 0.5),
                "std_sami": _std(samis),
                "min_sami": min(samis),
                "p10_sami": _quantile(samis, 0.10),
                "p25_sami": _quantile(samis, 0.25),
                "p75_sami": _quantile(samis, 0.75),
                "p90_sami": _quantile(samis, 0.90),
                "max_sami": max(samis),
                "share_positive": sum(1 for s in samis if s > 0.0) / float(len(samis)),
                "share_top_decile": sum(1 for r in subset if int(r["sami_decile"]) == 10) / float(len(subset)),
                "share_bottom_decile": sum(1 for r in subset if int(r["sami_decile"]) == 1) / float(len(subset)),
                "representative_city": rep["representative_city"],
                "representative_city_code": rep["representative_city_code"],
                "rep_overlay_path": rep["rep_overlay_path"],
                "rep_denue_path": rep["rep_denue_path"],
                "model_mean_sami": class_means[class_label],
            }
        )

    band_rows: list[dict[str, object]] = []
    contingency: list[list[int]] = []
    for decile in range(1, 11):
        subset = [r for r in rows if int(r["sami_decile"]) == decile]
        counts = Counter(r["class_label"] for r in subset)
        contingency.append([counts.get(class_label, 0) for class_label in class_order])
        for class_label in class_order:
            count = counts.get(class_label, 0)
            band_rows.append(
                {
                    "sami_decile": decile,
                    "class_label": class_label,
                    "count": count,
                    "share_within_decile": count / float(len(subset)),
                }
            )
    cramers_v, chi2 = _cramers_v(contingency)

    nearest_rows: list[dict[str, object]] = []
    same_class_nn = 0
    for idx, row in enumerate(rows):
        if idx == 0:
            neighbor = rows[1]
        elif idx == len(rows) - 1:
            neighbor = rows[-2]
        else:
            left = rows[idx - 1]
            right = rows[idx + 1]
            dl = abs(float(left["sami"]) - float(row["sami"]))
            dr = abs(float(right["sami"]) - float(row["sami"]))
            neighbor = left if dl <= dr else right
        is_same = 1 if neighbor["class_label"] == row["class_label"] else 0
        same_class_nn += is_same
        nearest_rows.append(
            {
                "city_code": row["city_code"],
                "city_name": row["city_name"],
                "class_label": row["class_label"],
                "city_sami": row["sami"],
                "nn_city_code": neighbor["city_code"],
                "nn_city_name": neighbor["city_name"],
                "nn_class_label": neighbor["class_label"],
                "nn_city_sami": neighbor["sami"],
                "sami_distance": abs(float(neighbor["sami"]) - float(row["sami"])),
                "same_class": is_same,
            }
        )
    nearest_same_rate = same_class_nn / float(len(rows))
    class_counts = Counter(r["class_label"] for r in rows)
    random_same_class = sum((count / float(len(rows))) ** 2 for count in class_counts.values())
    majority_class_share = max(class_counts.values()) / float(len(rows))

    diff_pairs: list[dict[str, object]] = []
    for idx in range(len(rows) - 1):
        a = rows[idx]
        b = rows[idx + 1]
        if a["class_label"] == b["class_label"]:
            continue
        diff_pairs.append(
            {
                "city_code_a": a["city_code"],
                "city_name_a": a["city_name"],
                "class_a": a["class_label"],
                "sami_a": a["sami"],
                "city_code_b": b["city_code"],
                "city_name_b": b["city_name"],
                "class_b": b["class_label"],
                "sami_b": b["sami"],
                "mean_sami": 0.5 * (float(a["sami"]) + float(b["sami"])),
                "sami_distance": abs(float(a["sami"]) - float(b["sami"])),
            }
        )
    diff_pairs.sort(key=lambda r: float(r["sami_distance"]))
    top_diff_pairs = diff_pairs[:15]

    summary_row = [
        {
            "n_cities": len(rows),
            "n_classes": len(class_order),
            "base_class": base_class,
            "class_model_r2": model_summary[1]["r2"],
            "class_model_adj_r2": model_summary[1]["adj_r2"],
            "class_mean_shift_p_value": nested_tests[0]["p_value"],
            "sami_decile_class_cramers_v": cramers_v,
            "sami_decile_class_chi2": chi2,
            "nearest_sami_neighbor_same_class_rate": nearest_same_rate,
            "random_pair_same_class_rate": random_same_class,
            "majority_class_share": majority_class_share,
        }
    ]

    _write_csv(outdir / "class_sami_summary.csv", class_summary_rows, list(class_summary_rows[0].keys()))
    _write_csv(outdir / "model_summary.csv", model_summary, list(model_summary[0].keys()))
    _write_csv(outdir / "nested_tests.csv", nested_tests, list(nested_tests[0].keys()))
    _write_csv(outdir / "sami_decile_class_mix.csv", band_rows, list(band_rows[0].keys()))
    _write_csv(outdir / "nearest_sami_neighbor_class_agreement.csv", nearest_rows, list(nearest_rows[0].keys()))
    _write_csv(outdir / "same_sami_different_class_pairs.csv", top_diff_pairs, list(top_diff_pairs[0].keys()))
    _write_csv(outdir / "summary.csv", summary_row, list(summary_row[0].keys()))

    dist_fig = _distribution_svg(figdir / "class_sami_distribution.svg", class_summary_rows, class_order, color_map)
    mix_fig = _mix_svg(figdir / "sami_decile_class_mix.svg", band_rows, class_order, color_map)
    neighbor_fig = _neighbor_svg(
        figdir / "nearest_sami_neighbor_class.svg",
        [
            {"label": "SAMI NN", "rate": nearest_same_rate, "color": TEAL},
            {"label": "Random pair", "rate": random_same_class, "color": RUST},
            {"label": "Majority class", "rate": majority_class_share, "color": BLUE},
        ],
    )
    pair_fig = _pair_table_svg(figdir / "same_sami_different_class_pairs.svg", top_diff_pairs)
    _write_csv(
        figdir / "figures_manifest.csv",
        [
            {"figure_id": "class_sami_distribution", "path": str(dist_fig.resolve()), "description": "SAMI distribution by topology class."},
            {"figure_id": "sami_decile_class_mix", "path": str(mix_fig.resolve()), "description": "Class composition across SAMI deciles."},
            {"figure_id": "nearest_sami_neighbor_class", "path": str(neighbor_fig.resolve()), "description": "Nearest SAMI neighbor class agreement vs baselines."},
            {"figure_id": "same_sami_different_class_pairs", "path": str(pair_fig.resolve()), "description": "Cities with nearly identical SAMI but different topology classes."},
        ],
        ["figure_id", "path", "description"],
    )

    strongest_positive = max(class_summary_rows, key=lambda r: float(r["mean_sami"]))
    strongest_negative = min(class_summary_rows, key=lambda r: float(r["mean_sami"]))
    top_heavy = max(class_summary_rows, key=lambda r: float(r["share_top_decile"]))
    bottom_heavy = max(class_summary_rows, key=lambda r: float(r["share_bottom_decile"]))

    lines = [
        "# City SAMI by Topology Class",
        "",
        "This experiment treats `SAMI` as the residual from the global national city scaling law and asks whether the network-topology classes induce their own SAMI structure, or whether SAMI still mixes classes heavily.",
        "",
        "Global city law and SAMI definition:",
        "- `log(E_i) = alpha + beta log(N_i) + epsilon_i`",
        "- `SAMI_i = epsilon_i = log(E_i) - (alpha + beta log(N_i))`",
        "- Here `E_i = total establishments`, `N_i = population`, and `alpha, beta` come from the independent national city baseline.",
        "",
        "Class-mean test:",
        "- `M0: SAMI_i = mu + eta_i`",
        "- `M1: SAMI_i = mu + gamma_c + eta_i`",
        "- If `M1` wins, topology classes shift the expected SAMI level.",
        "",
        f"Usable cities: `{len(rows)}` across `{len(class_order)}` topology classes.",
        "",
        "## Mean-Shift Result",
        f"- `M1` adjusted `R² = {float(model_summary[1]['adj_r2']):.3f}`",
        f"- `M0 -> M1` F-test `p = {float(nested_tests[0]['p_value']):.2e}`",
        f"- So topology class shifts mean `SAMI`, but only explains about `{100.0 * float(model_summary[1]['r2']):.1f}%` of total `SAMI` variance.",
        "",
        "## Class Means",
        f"- strongest positive mean: `{strongest_positive['class_label']}` with `mean SAMI = {float(strongest_positive['mean_sami']):+.3f}`",
        f"- strongest negative mean: `{strongest_negative['class_label']}` with `mean SAMI = {float(strongest_negative['mean_sami']):+.3f}`",
        f"- most overrepresented in top SAMI decile: `{top_heavy['class_label']}` with share `{float(top_heavy['share_top_decile']):.3f}`",
        f"- most overrepresented in bottom SAMI decile: `{bottom_heavy['class_label']}` with share `{float(bottom_heavy['share_bottom_decile']):.3f}`",
        "",
        "## Does SAMI Separate Classes?",
        f"- class x SAMI-decile association: `Cramér's V = {cramers_v:.3f}`",
        f"- nearest SAMI neighbor same-class rate: `{nearest_same_rate:.3f}`",
        f"- random same-class baseline: `{random_same_class:.3f}`",
        f"- majority-class share baseline: `{majority_class_share:.3f}`",
        "- Interpretation: SAMI is not independent of topology, but it is far from a sufficient class identifier.",
        "",
        "## Same-SAMI Different-Class Pairs",
        "- Consecutive cities in SAMI order often belong to different topology classes, sometimes with SAMI differences near machine precision for reporting purposes.",
        "",
        "## Key Files",
        f"- [class_sami_summary.csv]({(outdir / 'class_sami_summary.csv').resolve()})",
        f"- [model_summary.csv]({(outdir / 'model_summary.csv').resolve()})",
        f"- [nested_tests.csv]({(outdir / 'nested_tests.csv').resolve()})",
        f"- [sami_decile_class_mix.csv]({(outdir / 'sami_decile_class_mix.csv').resolve()})",
        f"- [nearest_sami_neighbor_class_agreement.csv]({(outdir / 'nearest_sami_neighbor_class_agreement.csv').resolve()})",
        f"- [same_sami_different_class_pairs.csv]({(outdir / 'same_sami_different_class_pairs.csv').resolve()})",
        "",
        "## Figures",
        f"- [class_sami_distribution.svg]({dist_fig.resolve()})",
        f"- [sami_decile_class_mix.svg]({mix_fig.resolve()})",
        f"- [nearest_sami_neighbor_class.svg]({neighbor_fig.resolve()})",
        f"- [same_sami_different_class_pairs.svg]({pair_fig.resolve()})",
        "",
        "## Interpretation",
        "Topology classes matter in expectation: they shift the center of the SAMI distribution. But the overlap remains large. So the national residual carries some topological signal without becoming a full topology state variable. In physics language, SAMI behaves more like a compressed macroscopic projection than a sufficient coordinate of the underlying urban microstructure.",
    ]
    (outdir / "report.md").write_text("\n".join(lines), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

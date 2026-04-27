#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
import random
from collections import defaultdict
from pathlib import Path

from urban_sami.analysis.experiment_pack import UnitDatum, fit_metrics, load_units_csv
from urban_sami.modeling import fit_by_name


ROOT = Path(__file__).resolve().parents[1]
UNITS_CSV = Path(
    "/home/hadox/cmd-center/platforms/polisplexity/core/data/sami_experiments/mx/"
    "matrix-y-denue_est_count-n-population-lvl-manzana-fit-ols-robust-poisson-negbin-auto-filt-raw-scope-cities-19001-14001-14002-19002-14003-plus187/"
    "20260315_221905/population__manzana__raw__ols_units.csv"
)
CITY_COUNTS_CSV = ROOT / "dist" / "independent_city_baseline" / "city_counts.csv"
OUTPUT_DIR = ROOT / "reports" / "manzana-within-ageb-systems-2026-04-24"
FIG_DIR = OUTPUT_DIR / "figures"

SANS = "Arial, Helvetica, sans-serif"
BG = "#fbfaf7"
TEXT = "#161616"
MUTED = "#666666"
TEAL = "#1e8f8f"
RUST = "#c75b39"
SLATE = "#5a6f8e"
GRID = "#dddddd"


def _read_city_names() -> dict[str, str]:
    out: dict[str, str] = {}
    with CITY_COUNTS_CSV.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            out[str(row.get("city_code") or "").strip()] = str(row.get("city_name") or "").strip()
    return out


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not fieldnames:
        seen: list[str] = []
        seen_set: set[str] = set()
        for row in rows:
            for key in row.keys():
                if key not in seen_set:
                    seen_set.add(key)
                    seen.append(str(key))
        fieldnames = seen or ["empty"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        if rows:
            writer.writerows(rows)
        else:
            writer.writerow({fieldnames[0]: ""})


def _mean(values: list[float]) -> float:
    return sum(values) / float(len(values)) if values else 0.0


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return ordered[0]
    idx = (len(ordered) - 1) * float(p)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return ordered[lo]
    frac = idx - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def _fit_arrays(y: list[float], n: list[float], *, fit_method: str = "ols") -> dict[str, float]:
    fit = fit_by_name(y, n, fit_method)
    yhat = [math.exp(fit.alpha + (fit.beta * math.log(max(1e-9, value)))) for value in n]
    residual = [
        math.log(max(1e-9, yi)) - math.log(max(1e-9, yhi))
        for yi, yhi in zip(y, yhat)
    ]
    resid_std = 0.0
    if len(residual) > 1:
        mu = _mean(residual)
        resid_std = math.sqrt(sum((value - mu) ** 2 for value in residual) / float(len(residual) - 1))
    return {
        "alpha": float(fit.alpha),
        "beta": float(fit.beta),
        "r2": float(fit.r2),
        "resid_std": resid_std,
    }


def _ageb_key(unit_code: str) -> str:
    parts = str(unit_code).split(":")
    return ":".join(parts[:2])


def _city_key(unit_code: str) -> str:
    return str(unit_code).split(":", 1)[0]


def _stable_code_seed(text: str) -> int:
    total = 0
    for idx, ch in enumerate(str(text)):
        total += (idx + 1) * ord(ch)
    return total


def _line_chart_threshold(rows: list[dict[str, object]], path: Path) -> Path:
    width = 920
    height = 420
    margin = 70
    inner_w = width - 2 * margin
    inner_h = height - 2 * margin
    thresholds = [int(row["min_manzanas"]) for row in rows]
    series = {
        "weighted_r2": [float(row["weighted_r2"]) for row in rows],
        "mean_r2": [float(row["mean_r2"]) for row in rows],
        "median_r2": [float(row["median_r2"]) for row in rows],
    }
    y_max = max(max(values) for values in series.values()) if rows else 1.0
    y_max = max(y_max, 0.2)

    def x_pos(value: int) -> float:
        if len(thresholds) == 1:
            return margin + inner_w / 2.0
        lo = min(thresholds)
        hi = max(thresholds)
        return margin + ((value - lo) / float(max(1, hi - lo))) * inner_w

    def y_pos(value: float) -> float:
        return margin + inner_h - ((value / y_max) * inner_h)

    colors = {"weighted_r2": TEAL, "mean_r2": RUST, "median_r2": SLATE}
    labels = {"weighted_r2": "weighted R²", "mean_r2": "mean R²", "median_r2": "median R²"}

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<text x="{margin}" y="34" font-family="{SANS}" font-size="22" font-weight="700" fill="{TEXT}">Manzana within-AGEB threshold summary</text>',
        f'<text x="{margin}" y="56" font-family="{SANS}" font-size="13" fill="{MUTED}">Observed OLS fit summaries as the minimum number of manzanas per AGEB increases</text>',
    ]
    for frac in (0.0, 0.25, 0.5, 0.75, 1.0):
        y = y_pos(y_max * frac)
        parts.append(f'<line x1="{margin}" y1="{y:.2f}" x2="{width-margin}" y2="{y:.2f}" stroke="{GRID}" stroke-width="1"/>')
        parts.append(f'<text x="{margin-12}" y="{y+4:.2f}" text-anchor="end" font-family="{SANS}" font-size="11" fill="{MUTED}">{y_max*frac:.2f}</text>')
    parts.append(f'<line x1="{margin}" y1="{margin}" x2="{margin}" y2="{height-margin}" stroke="{TEXT}" stroke-width="1.5"/>')
    parts.append(f'<line x1="{margin}" y1="{height-margin}" x2="{width-margin}" y2="{height-margin}" stroke="{TEXT}" stroke-width="1.5"/>')
    for threshold in thresholds:
        x = x_pos(threshold)
        parts.append(f'<line x1="{x:.2f}" y1="{height-margin}" x2="{x:.2f}" y2="{height-margin+6}" stroke="{TEXT}" stroke-width="1"/>')
        parts.append(f'<text x="{x:.2f}" y="{height-margin+24}" text-anchor="middle" font-family="{SANS}" font-size="11" fill="{MUTED}">{threshold}</text>')
    for key, values in series.items():
        coords = " ".join(f"{x_pos(t):.2f},{y_pos(v):.2f}" for t, v in zip(thresholds, values))
        parts.append(f'<polyline fill="none" stroke="{colors[key]}" stroke-width="3" points="{coords}"/>')
        for t, v in zip(thresholds, values):
            parts.append(f'<circle cx="{x_pos(t):.2f}" cy="{y_pos(v):.2f}" r="4.5" fill="{colors[key]}"/>')
    lx = width - margin - 180
    ly = 78
    for idx, key in enumerate(("weighted_r2", "mean_r2", "median_r2")):
        y = ly + idx * 22
        parts.append(f'<line x1="{lx}" y1="{y}" x2="{lx+22}" y2="{y}" stroke="{colors[key]}" stroke-width="3"/>')
        parts.append(f'<text x="{lx+30}" y="{y+4}" font-family="{SANS}" font-size="12" fill="{TEXT}">{labels[key]}</text>')
    parts.append("</svg>")
    path.write_text("".join(parts), encoding="utf-8")
    return path


def _histogram(values: list[float], path: Path, *, title: str, subtitle: str, fill: str, bins: int = 24, x_max: float | None = None) -> Path:
    width = 920
    height = 420
    margin = 70
    inner_w = width - 2 * margin
    inner_h = height - 2 * margin
    if not values:
        values = [0.0]
    lo = min(values)
    hi = max(values) if x_max is None else x_max
    if hi <= lo:
        hi = lo + 1.0
    step = (hi - lo) / float(max(1, bins))
    counts = [0 for _ in range(bins)]
    for value in values:
        idx = int((min(value, hi - 1e-9) - lo) / step)
        idx = max(0, min(bins - 1, idx))
        counts[idx] += 1
    ymax = max(counts) if counts else 1

    def x_pos(value: float) -> float:
        return margin + ((value - lo) / (hi - lo)) * inner_w

    def y_pos(value: float) -> float:
        return margin + inner_h - ((value / ymax) * inner_h if ymax > 0 else 0.0)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<text x="{margin}" y="34" font-family="{SANS}" font-size="22" font-weight="700" fill="{TEXT}">{title}</text>',
        f'<text x="{margin}" y="56" font-family="{SANS}" font-size="13" fill="{MUTED}">{subtitle}</text>',
    ]
    for frac in (0.0, 0.25, 0.5, 0.75, 1.0):
        y = y_pos(ymax * frac)
        parts.append(f'<line x1="{margin}" y1="{y:.2f}" x2="{width-margin}" y2="{y:.2f}" stroke="{GRID}" stroke-width="1"/>')
    parts.append(f'<line x1="{margin}" y1="{margin}" x2="{margin}" y2="{height-margin}" stroke="{TEXT}" stroke-width="1.5"/>')
    parts.append(f'<line x1="{margin}" y1="{height-margin}" x2="{width-margin}" y2="{height-margin}" stroke="{TEXT}" stroke-width="1.5"/>')
    for i, count in enumerate(counts):
        x0 = margin + (i / bins) * inner_w
        x1 = margin + ((i + 1) / bins) * inner_w
        y = y_pos(count)
        parts.append(f'<rect x="{x0+1:.2f}" y="{y:.2f}" width="{max(1.0, x1-x0-2):.2f}" height="{height-margin-y:.2f}" fill="{fill}" opacity="0.88"/>')
    for frac in (0.0, 0.25, 0.5, 0.75, 1.0):
        x = x_pos(lo + (hi - lo) * frac)
        label = lo + (hi - lo) * frac
        parts.append(f'<line x1="{x:.2f}" y1="{height-margin}" x2="{x:.2f}" y2="{height-margin+6}" stroke="{TEXT}" stroke-width="1"/>')
        parts.append(f'<text x="{x:.2f}" y="{height-margin+24}" text-anchor="middle" font-family="{SANS}" font-size="11" fill="{MUTED}">{label:.2f}</text>')
    parts.append("</svg>")
    path.write_text("".join(parts), encoding="utf-8")
    return path


def _bar_chart(rows: list[dict[str, object]], path: Path) -> Path:
    width = 920
    height = 420
    margin = 70
    inner_w = width - 2 * margin
    inner_h = height - 2 * margin
    labels = [str(row["metric"]) for row in rows]
    values = [float(row["value"]) for row in rows]
    ymax = max(values) if values else 1.0
    ymax = max(ymax, 0.15)
    bar_w = inner_w / max(1, len(rows))

    def y_pos(v: float) -> float:
        return margin + inner_h - ((v / ymax) * inner_h)

    colors = [TEAL, RUST, SLATE, "#8f7d2b"]
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<text x="{margin}" y="34" font-family="{SANS}" font-size="22" font-weight="700" fill="{TEXT}">Observed vs null comparison</text>',
        f'<text x="{margin}" y="56" font-family="{SANS}" font-size="13" fill="{MUTED}">Threshold fixed at 10 manzanas per AGEB</text>',
    ]
    for frac in (0.0, 0.25, 0.5, 0.75, 1.0):
        y = y_pos(ymax * frac)
        parts.append(f'<line x1="{margin}" y1="{y:.2f}" x2="{width-margin}" y2="{y:.2f}" stroke="{GRID}" stroke-width="1"/>')
        parts.append(f'<text x="{margin-12}" y="{y+4:.2f}" text-anchor="end" font-family="{SANS}" font-size="11" fill="{MUTED}">{ymax*frac:.2f}</text>')
    parts.append(f'<line x1="{margin}" y1="{margin}" x2="{margin}" y2="{height-margin}" stroke="{TEXT}" stroke-width="1.5"/>')
    parts.append(f'<line x1="{margin}" y1="{height-margin}" x2="{width-margin}" y2="{height-margin}" stroke="{TEXT}" stroke-width="1.5"/>')
    for idx, (label, value) in enumerate(zip(labels, values)):
        x0 = margin + idx * bar_w + 8
        y = y_pos(value)
        h = height - margin - y
        color = colors[idx % len(colors)]
        parts.append(f'<rect x="{x0:.2f}" y="{y:.2f}" width="{max(10.0, bar_w-16):.2f}" height="{h:.2f}" fill="{color}" opacity="0.90"/>')
        parts.append(f'<text x="{x0 + (bar_w-16)/2:.2f}" y="{y-8:.2f}" text-anchor="middle" font-family="{SANS}" font-size="11" fill="{TEXT}">{value:.3f}</text>')
        parts.append(f'<text x="{x0 + (bar_w-16)/2:.2f}" y="{height-margin+20:.2f}" text-anchor="middle" font-family="{SANS}" font-size="11" fill="{MUTED}">{label}</text>')
    parts.append("</svg>")
    path.write_text("".join(parts), encoding="utf-8")
    return path


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    city_name_map = _read_city_names()
    rows = load_units_csv(UNITS_CSV, level="manzana", fit_method="ols")
    by_ageb: dict[str, list[UnitDatum]] = defaultdict(list)
    for row in rows:
        by_ageb[_ageb_key(row.unit_code)].append(row)

    pooled_rows: list[dict[str, object]] = []
    for method in ("ols", "robust", "poisson", "negbin", "auto"):
        metrics = fit_metrics(rows, fit_method=method)
        metrics["level"] = "manzana"
        pooled_rows.append(metrics)
    _write_csv(OUTPUT_DIR / "pooled_model_summary.csv", pooled_rows)

    threshold_rows: list[dict[str, object]] = []
    per_threshold_cache: dict[int, list[dict[str, object]]] = {}
    for min_manzanas in (5, 8, 10, 12, 15, 20):
        ageb_rows: list[dict[str, object]] = []
        for ageb_code, ageb_units in by_ageb.items():
            if len(ageb_units) < min_manzanas:
                continue
            fit = fit_metrics(ageb_units, fit_method="ols")
            city_code = _city_key(ageb_units[0].unit_code)
            ageb_rows.append(
                {
                    "city_code": city_code,
                    "city_name": city_name_map.get(city_code, city_code),
                    "ageb_code": ageb_code,
                    "units": len(ageb_units),
                    "alpha": float(fit["alpha"]),
                    "beta": float(fit["beta"]),
                    "r2": float(fit["r2"]),
                    "resid_std": float(fit["resid_std"]),
                }
            )
        per_threshold_cache[min_manzanas] = ageb_rows
        r2_vals = [float(row["r2"]) for row in ageb_rows]
        beta_vals = [float(row["beta"]) for row in ageb_rows]
        units = [float(row["units"]) for row in ageb_rows]
        weighted_r2 = sum(r * u for r, u in zip(r2_vals, units)) / sum(units) if units else 0.0
        threshold_rows.append(
            {
                "min_manzanas": min_manzanas,
                "n_agebs": len(ageb_rows),
                "mean_r2": _mean(r2_vals),
                "median_r2": _percentile(r2_vals, 0.5),
                "weighted_r2": weighted_r2,
                "mean_beta": _mean(beta_vals),
                "beta_p05": _percentile(beta_vals, 0.05),
                "beta_p95": _percentile(beta_vals, 0.95),
            }
        )
    _write_csv(OUTPUT_DIR / "threshold_summary.csv", threshold_rows)

    chosen_min = 10
    chosen_ageb_rows = per_threshold_cache[chosen_min]

    observed_null_rows: list[dict[str, object]] = []
    random.seed(42)
    repeats = 20
    for row in chosen_ageb_rows:
        ageb_units = by_ageb[str(row["ageb_code"])]
        y_values = [unit.y for unit in ageb_units]
        n_values = [unit.n for unit in ageb_units]
        null_r2: list[float] = []
        code_seed = _stable_code_seed(str(row["ageb_code"]))
        for rep in range(repeats):
            shuffled = list(y_values)
            random.Random(1000 + rep + code_seed).shuffle(shuffled)
            fit = _fit_arrays(shuffled, n_values, fit_method="ols")
            null_r2.append(float(fit["r2"]))
        row2 = dict(row)
        row2["null_repeats"] = repeats
        row2["null_mean_r2"] = _mean(null_r2)
        row2["null_p95_r2"] = _percentile(null_r2, 0.95)
        row2["delta_r2_vs_null_mean"] = float(row2["r2"]) - row2["null_mean_r2"]
        row2["above_null_p95"] = int(float(row2["r2"]) > row2["null_p95_r2"])
        observed_null_rows.append(row2)
    observed_null_rows.sort(key=lambda row: (str(row["city_code"]), str(row["ageb_code"])))
    _write_csv(OUTPUT_DIR / "ageb_fit_distribution.csv", observed_null_rows)

    top_agebs = sorted(observed_null_rows, key=lambda row: float(row["r2"]), reverse=True)[:30]
    bottom_agebs = sorted(observed_null_rows, key=lambda row: float(row["r2"]))[:30]
    _write_csv(OUTPUT_DIR / "top_agebs.csv", top_agebs)
    _write_csv(OUTPUT_DIR / "bottom_agebs.csv", bottom_agebs)

    city_rollup: list[dict[str, object]] = []
    by_city_rollup: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in observed_null_rows:
        by_city_rollup[str(row["city_code"])].append(row)
    for city_code, local_rows in sorted(by_city_rollup.items()):
        r2_vals = [float(row["r2"]) for row in local_rows]
        beta_vals = [float(row["beta"]) for row in local_rows]
        units = [float(row["units"]) for row in local_rows]
        city_rollup.append(
            {
                "city_code": city_code,
                "city_name": city_name_map.get(city_code, city_code),
                "n_agebs": len(local_rows),
                "mean_r2": _mean(r2_vals),
                "median_r2": _percentile(r2_vals, 0.5),
                "weighted_r2": sum(r * u for r, u in zip(r2_vals, units)) / sum(units),
                "share_above_null_p95": _mean([float(row["above_null_p95"]) for row in local_rows]),
                "mean_beta": _mean(beta_vals),
            }
        )
    _write_csv(OUTPUT_DIR / "city_rollup.csv", city_rollup)

    obs_r2 = [float(row["r2"]) for row in observed_null_rows]
    null_mean_r2 = [float(row["null_mean_r2"]) for row in observed_null_rows]
    comparison_rows = [
        {"metric": "obs mean R²", "value": _mean(obs_r2)},
        {"metric": "obs median R²", "value": _percentile(obs_r2, 0.5)},
        {"metric": "null mean R²", "value": _mean(null_mean_r2)},
        {"metric": "share obs>null95", "value": _mean([float(row["above_null_p95"]) for row in observed_null_rows])},
    ]
    _write_csv(OUTPUT_DIR / "observed_vs_null_summary.csv", comparison_rows, ["metric", "value"])

    threshold_fig = _line_chart_threshold(threshold_rows, FIG_DIR / "threshold_summary.svg")
    r2_fig = _histogram(
        obs_r2,
        FIG_DIR / "ageb_r2_histogram.svg",
        title="R² across AGEB-local manzana laws",
        subtitle="Each AGEB is treated as a local system and its manzanas as the fine units",
        fill=TEAL,
        bins=28,
        x_max=1.0,
    )
    beta_fig = _histogram(
        [float(row["beta"]) for row in observed_null_rows],
        FIG_DIR / "ageb_beta_histogram.svg",
        title="β across AGEB-local manzana laws",
        subtitle="Threshold fixed at 10 manzanas per AGEB",
        fill=RUST,
        bins=28,
    )
    comparison_fig = _bar_chart(comparison_rows, FIG_DIR / "observed_vs_null_summary.svg")

    pooled_ols = next(row for row in pooled_rows if str(row["fit_method"]) == "ols")
    threshold10 = next(row for row in threshold_rows if int(row["min_manzanas"]) == chosen_min)
    top_good = sorted(observed_null_rows, key=lambda row: float(row["delta_r2_vs_null_mean"]), reverse=True)[:10]
    top_bad = sorted(observed_null_rows, key=lambda row: float(row["delta_r2_vs_null_mean"]))[:10]

    report_lines = [
        "# Manzana Within AGEB Systems",
        "",
        "Question: if we treat each `AGEB` as a local system and each `manzana` as a city inside that system, does a new scaling law appear?",
        "",
        "Mathematical setup:",
        "",
        "For manzana `m` inside AGEB `a`, fit",
        "",
        "```math",
        "\\log y_{am} = \\alpha_a + \\beta_a \\log n_{am} + \\varepsilon_{am}",
        "```",
        "",
        "where:",
        "- `y_{am}` = establishments in manzana `m`",
        "- `n_{am}` = population in manzana `m`",
        "- `a` indexes AGEB systems",
        "",
        "Pooled manzana law over all available units:",
        f"- `n = {int(pooled_ols['units'])}` manzanas",
        f"- `β = {float(pooled_ols['beta']):.3f}`",
        f"- `R² = {float(pooled_ols['r2']):.3f}`",
        "",
        f"AGEB-local summary with threshold `min manzanas = {chosen_min}`:",
        f"- retained `AGEB` systems = `{int(threshold10['n_agebs'])}`",
        f"- mean `R² = {float(threshold10['mean_r2']):.3f}`",
        f"- median `R² = {float(threshold10['median_r2']):.3f}`",
        f"- weighted `R² = {float(threshold10['weighted_r2']):.3f}`",
        f"- mean `β = {float(threshold10['mean_beta']):.3f}`",
        f"- `β` 5th to 95th percentile = `{float(threshold10['beta_p05']):.3f}` to `{float(threshold10['beta_p95']):.3f}`",
        "",
        "Null test inside each AGEB:",
        "- shuffle `y` across manzanas within the same AGEB, keeping the manzana population vector fixed",
        "- refit OLS 20 times per AGEB",
        "",
        "Observed vs null:",
        f"- observed mean `R² = {_mean(obs_r2):.3f}`",
        f"- observed median `R² = {_percentile(obs_r2, 0.5):.3f}`",
        f"- null mean `R² = {_mean(null_mean_r2):.3f}`",
        f"- share of AGEBs with observed `R²` above their own null `p95` = `{_mean([float(row['above_null_p95']) for row in observed_null_rows]):.3f}`",
        "",
        "Interpretation:",
        "- unlike the AGEB experiment, manzana does not recover a generally strong law by simply localizing the support to AGEB systems",
        "- the pooled manzana law is very weak, and the typical AGEB-local law remains weak",
        "- there is a high-dispersion tail of special AGEBs with strong fits, but they are not representative of the whole distribution",
        "- this suggests that going one scale finer does not reveal a cleaner universal mechanism; instead it amplifies discreteness and local heterogeneity",
        "",
        "Most informative outputs:",
        f"- [pooled_model_summary.csv]({(OUTPUT_DIR / 'pooled_model_summary.csv').resolve()})",
        f"- [threshold_summary.csv]({(OUTPUT_DIR / 'threshold_summary.csv').resolve()})",
        f"- [ageb_fit_distribution.csv]({(OUTPUT_DIR / 'ageb_fit_distribution.csv').resolve()})",
        f"- [city_rollup.csv]({(OUTPUT_DIR / 'city_rollup.csv').resolve()})",
        f"- [top_agebs.csv]({(OUTPUT_DIR / 'top_agebs.csv').resolve()})",
        f"- [bottom_agebs.csv]({(OUTPUT_DIR / 'bottom_agebs.csv').resolve()})",
        "",
        "Figures:",
        f"- [threshold_summary.svg]({threshold_fig.resolve()})",
        f"- [ageb_r2_histogram.svg]({r2_fig.resolve()})",
        f"- [ageb_beta_histogram.svg]({beta_fig.resolve()})",
        f"- [observed_vs_null_summary.svg]({comparison_fig.resolve()})",
        "",
        "AGEBs with strongest positive excess over null:",
    ]
    for row in top_good[:10]:
        report_lines.append(
            f"- `{row['ageb_code']}` ({row['city_name']}) -> `R²={float(row['r2']):.3f}`, `β={float(row['beta']):.3f}`, `ΔR²={float(row['delta_r2_vs_null_mean']):+.3f}`, `units={int(row['units'])}`"
        )
    report_lines.extend(["", "AGEBs with weakest or null-like behavior:"])
    for row in top_bad[:10]:
        report_lines.append(
            f"- `{row['ageb_code']}` ({row['city_name']}) -> `R²={float(row['r2']):.6f}`, `β={float(row['beta']):.3f}`, `ΔR²={float(row['delta_r2_vs_null_mean']):+.3f}`, `units={int(row['units'])}`"
        )

    (OUTPUT_DIR / "report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

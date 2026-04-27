#!/usr/bin/env python3
from __future__ import annotations

import csv
import html
import json
import math
import shutil
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, median, pstdev

from urban_sami.modeling.fit import fit_ols


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
SERIF = "Georgia, 'Times New Roman', serif"
SANS = "Helvetica, Arial, sans-serif"


@dataclass
class Obs:
    unit_code: str
    unit_label: str
    population: float
    y: float
    sami: float | None = None
    y_expected: float | None = None


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


def _fmt(value: float, digits: int = 3) -> str:
    return f"{value:.{digits}f}"


def _fmt_int(value: float) -> str:
    return f"{int(round(value)):,}"


def _pct(value: float, digits: int = 1) -> str:
    return f"{value * 100:.{digits}f}%"


def _corr(xs: list[float], ys: list[float]) -> float:
    if len(xs) < 2 or len(ys) < 2:
        return 0.0
    x_bar = mean(xs)
    y_bar = mean(ys)
    num = sum((x - x_bar) * (y - y_bar) for x, y in zip(xs, ys))
    den_x = math.sqrt(sum((x - x_bar) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - y_bar) ** 2 for y in ys))
    if den_x <= 0 or den_y <= 0:
        return 0.0
    return num / (den_x * den_y)


def _quantile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    idx = min(len(sorted_values) - 1, max(0, round((len(sorted_values) - 1) * q)))
    return sorted_values[idx]


def _svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _load_city_system() -> list[Obs]:
    rows = _read_csv(Path("/home/hadox/cmd-center/platforms/research/urban-sami/dist/independent_city_baseline/city_counts.csv"))
    out = []
    for row in rows:
        population = _to_float(row["population"])
        y = _to_float(row["est_count"])
        if population > 0 and y > 0:
            out.append(Obs(row["city_code"], row["city_name"], population, y))
    return out


def _load_ageb_system() -> list[Obs]:
    rows = _read_csv(Path("/home/hadox/cmd-center/platforms/research/urban-sami/reports/ageb-one-city-guadalajara-2026-04-22/all_ageb_scores.csv"))
    out = []
    for row in rows:
        population = _to_float(row["population"])
        y = _to_float(row["y_observed"])
        if population > 0 and y > 0:
            out.append(
                Obs(
                    row["unit_code"],
                    row["unit_label"],
                    population,
                    y,
                    sami=_to_float(row["sami"]),
                    y_expected=_to_float(row["y_expected"]),
                )
            )
    return out


def _augment_sami(obs: list[Obs]) -> tuple[float, float, float]:
    y = [o.y for o in obs]
    n = [o.population for o in obs]
    fit = fit_ols(y, n)
    alpha = fit.alpha
    beta = fit.beta
    for o in obs:
        zhat = alpha + beta * math.log(o.population)
        o.y_expected = math.exp(zhat)
        o.sami = math.log(o.y) - zhat
    return alpha, beta, fit.r2


def _variance_decomposition(obs: list[Obs]) -> dict[str, float]:
    logn = [math.log(o.population) for o in obs]
    logy = [math.log(o.y) for o in obs]
    fit = fit_ols([o.y for o in obs], [o.population for o in obs])
    fitted = [fit.alpha + fit.beta * x for x in logn]
    residual = [y - f for y, f in zip(logy, fitted)]
    y_bar = mean(logy)
    sst = sum((y - y_bar) ** 2 for y in logy)
    ssr = sum((f - y_bar) ** 2 for f in fitted)
    sse = sum(e * e for e in residual)
    return {
        "alpha": fit.alpha,
        "beta": fit.beta,
        "r2": fit.r2,
        "corr_raw": _corr([o.population for o in obs], [o.y for o in obs]),
        "corr_log": _corr(logn, logy),
        "var_logy": pstdev(logy) ** 2 if len(logy) > 1 else 0.0,
        "ss_total": sst,
        "ss_fit": ssr,
        "ss_resid": sse,
        "fit_share": (ssr / sst) if sst > 0 else 0.0,
        "resid_share": (sse / sst) if sst > 0 else 0.0,
    }


def _bin_stats(obs: list[Obs], system_name: str) -> list[dict[str, object]]:
    obs = sorted(obs, key=lambda o: o.population)
    n = len(obs)
    rows = []
    for i in range(10):
        chunk = obs[i * n // 10 : (i + 1) * n // 10]
        pops = [o.population for o in chunk]
        ys = [o.y for o in chunk]
        logy = [math.log(o.y) for o in chunk]
        samis = [o.sami for o in chunk if o.sami is not None]
        ys_sorted = sorted(ys)
        p10 = _quantile(ys_sorted, 0.10)
        p90 = _quantile(ys_sorted, 0.90)
        rows.append(
            {
                "system": system_name,
                "population_decile": i + 1,
                "n_obs": len(chunk),
                "population_min": min(pops),
                "population_median": median(pops),
                "population_max": max(pops),
                "y_mean": mean(ys),
                "y_median": median(ys),
                "y_sd": pstdev(ys) if len(ys) > 1 else 0.0,
                "y_cv": ((pstdev(ys) / mean(ys)) if mean(ys) > 0 and len(ys) > 1 else 0.0),
                "y_p10": p10,
                "y_p90": p90,
                "y_p90_p10_ratio": (p90 / p10) if p10 > 0 else 0.0,
                "logy_mean": mean(logy),
                "logy_sd": pstdev(logy) if len(logy) > 1 else 0.0,
                "sami_min": min(samis) if samis else 0.0,
                "sami_max": max(samis) if samis else 0.0,
            }
        )
    return rows


def _between_within_share(bin_rows: list[dict[str, object]], obs: list[Obs]) -> dict[str, float]:
    logy = [math.log(o.y) for o in obs]
    grand_mean = mean(logy)
    sst = sum((y - grand_mean) ** 2 for y in logy)
    ssb = 0.0
    for row in bin_rows:
        n_obs = int(row["n_obs"])
        mu = float(row["logy_mean"])
        ssb += n_obs * (mu - grand_mean) ** 2
    ssw = sst - ssb
    return {
        "between_share": (ssb / sst) if sst > 0 else 0.0,
        "within_share": (ssw / sst) if sst > 0 else 0.0,
    }


def _central_window_extremes(obs: list[Obs], system_name: str) -> tuple[list[dict[str, object]], dict[str, float]]:
    pops_sorted = sorted(o.population for o in obs)
    lo = _quantile(pops_sorted, 0.30)
    hi = _quantile(pops_sorted, 0.70)
    window = [o for o in obs if lo <= o.population <= hi]
    window_sorted = sorted(window, key=lambda o: o.y)
    low = window_sorted[:10]
    high = window_sorted[-10:]
    rows = []
    for side, group in [("low", low), ("high", reversed(high))]:
        for o in group:
            rows.append(
                {
                    "system": system_name,
                    "side": side,
                    "unit_code": o.unit_code,
                    "unit_label": o.unit_label,
                    "population": o.population,
                    "y_observed": o.y,
                    "y_expected": o.y_expected or 0.0,
                    "sami": o.sami or 0.0,
                }
            )
    summary = {
        "population_lo": lo,
        "population_hi": hi,
        "n_window": len(window),
        "y_min": min(o.y for o in window),
        "y_max": max(o.y for o in window),
        "y_ratio_max_min": (max(o.y for o in window) / min(o.y for o in window)),
    }
    return rows, summary


def _write_scatter_comparison(path: Path, city_obs: list[Obs], ageb_obs: list[Obs]) -> Path:
    width = 1300
    height = 620
    top = 92
    bottom = 72
    left = 78
    panel_gap = 34
    panel_w = (width - left * 2 - panel_gap) / 2
    panel_h = height - top - bottom

    all_x = [math.log(o.population) for o in city_obs + ageb_obs]
    all_y = [math.log(o.y) for o in city_obs + ageb_obs]
    x_min, x_max = min(all_x), max(all_x)
    y_min, y_max = min(all_y), max(all_y)
    x_pad = (x_max - x_min) * 0.05
    y_pad = (y_max - y_min) * 0.08
    x_min -= x_pad
    x_max += x_pad
    y_min -= y_pad
    y_max += y_pad

    def proj(xv: float, yv: float, x0: float) -> tuple[float, float]:
        px = x0 + ((xv - x_min) / max(x_max - x_min, 1e-9)) * panel_w
        py = top + panel_h - ((yv - y_min) / max(y_max - y_min, 1e-9)) * panel_h
        return px, py

    city_fit = fit_ols([o.y for o in city_obs], [o.population for o in city_obs])
    ageb_fit = fit_ols([o.y for o in ageb_obs], [o.population for o in ageb_obs])

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">Same law, different support: cities versus AGEBs</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">Both panels use Y = total establishments and N = population in log-log space.</text>',
    ]
    panels = [
        ("Mexico cities", city_obs, city_fit, left),
        ("Guadalajara AGEB", ageb_obs, ageb_fit, left + panel_w + panel_gap),
    ]
    for title, obs, fit, x0 in panels:
        body.append(f'<rect x="{x0}" y="{top}" width="{panel_w}" height="{panel_h}" fill="none" stroke="{AXIS}"/>')
        body.append(f'<text x="{x0+8}" y="{top-16}" font-size="16" font-family="{SANS}" fill="{TEXT}">{html.escape(title)}</text>')
        for tick in range(6):
            xv = x_min + (x_max - x_min) * tick / 5
            x, _ = proj(xv, y_min, x0)
            body.append(f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top+panel_h}" stroke="{GRID}"/>')
        for tick in range(6):
            yv = y_min + (y_max - y_min) * tick / 5
            _, y = proj(x_min, yv, x0)
            body.append(f'<line x1="{x0}" y1="{y:.2f}" x2="{x0+panel_w}" y2="{y:.2f}" stroke="{GRID}"/>')
        for o in obs:
            px, py = proj(math.log(o.population), math.log(o.y), x0)
            body.append(f'<circle cx="{px:.2f}" cy="{py:.2f}" r="2.3" fill="{TEAL}" fill-opacity="0.55"/>')
        x1 = x_min
        x2 = x_max
        y1 = fit.alpha + fit.beta * x1
        y2 = fit.alpha + fit.beta * x2
        p1 = proj(x1, y1, x0)
        p2 = proj(x2, y2, x0)
        body.append(f'<line x1="{p1[0]:.2f}" y1="{p1[1]:.2f}" x2="{p2[0]:.2f}" y2="{p2[1]:.2f}" stroke="{RUST}" stroke-width="2.4"/>')
        body.append(f'<text x="{x0+8}" y="{top+panel_h+24}" font-size="12" font-family="{SANS}" fill="{MUTED}">β={fit.beta:+.3f}   R²={fit.r2:.3f}   n={len(obs)}</text>')
    return _svg(path, width, height, "".join(body))


def _write_bin_dispersion(path: Path, city_bins: list[dict[str, object]], ageb_bins: list[dict[str, object]]) -> Path:
    width = 1240
    height = 700
    left = 100
    right = 34
    top = 96
    bottom = 92
    plot_w = width - left - right
    plot_h = height - top - bottom
    all_vals = []
    for row in city_bins + ageb_bins:
        all_vals.extend([float(row["y_p10"]), float(row["y_p90"])])
    y_min = 0.0
    y_max = max(all_vals) * 1.05

    def px(group_idx: int, decile_idx: int) -> float:
        group_w = plot_w / 10
        offset = 0.23 if group_idx == 0 else 0.67
        return left + decile_idx * group_w + offset * group_w

    def py(value: float) -> float:
        return top + plot_h - ((value - y_min) / max(y_max - y_min, 1e-9)) * plot_h

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">Dispersion inside population deciles</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">Each interval shows Y p10 to p90 within a population decile. Wide bands mean similar-size units behave very differently.</text>',
        f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="none" stroke="{AXIS}"/>',
    ]
    for tick in range(6):
        yv = y_min + (y_max - y_min) * tick / 5
        y = py(yv)
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left+plot_w}" y2="{y:.2f}" stroke="{GRID}"/>')
        body.append(f'<text x="{left-14}" y="{y+4:.2f}" text-anchor="end" font-size="11" font-family="{SANS}" fill="{MUTED}">{int(round(yv))}</text>')
    for i in range(10):
        group_w = plot_w / 10
        x0 = left + i * group_w
        body.append(f'<line x1="{x0:.2f}" y1="{top}" x2="{x0:.2f}" y2="{top+plot_h}" stroke="{GRID}"/>')
        body.append(f'<text x="{x0 + group_w/2:.2f}" y="{top+plot_h+26:.2f}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">D{i+1}</text>')
    for rows, color, label, group_idx in [(city_bins, BLUE, "Cities", 0), (ageb_bins, RUST, "AGEB", 1)]:
        for i, row in enumerate(rows):
            x = px(group_idx, i)
            y1 = py(float(row["y_p10"]))
            y2 = py(float(row["y_p90"]))
            ym = py(float(row["y_median"]))
            body.append(f'<line x1="{x:.2f}" y1="{y1:.2f}" x2="{x:.2f}" y2="{y2:.2f}" stroke="{color}" stroke-width="4"/>')
            body.append(f'<circle cx="{x:.2f}" cy="{ym:.2f}" r="4.5" fill="{color}"/>')
    body.append(f'<circle cx="{width-190}" cy="120" r="5" fill="{BLUE}"/><text x="{width-176}" y="124" font-size="12" font-family="{SANS}" fill="{TEXT}">Cities</text>')
    body.append(f'<circle cx="{width-110}" cy="120" r="5" fill="{RUST}"/><text x="{width-96}" y="124" font-size="12" font-family="{SANS}" fill="{TEXT}">AGEB</text>')
    return _svg(path, width, height, "".join(body))


def _write_metric_bars(path: Path, city_metrics: dict[str, float], ageb_metrics: dict[str, float], city_bw: dict[str, float], ageb_bw: dict[str, float]) -> Path:
    width = 980
    height = 560
    left = 150
    right = 120
    top = 96
    bottom = 80
    plot_w = width - left - right
    plot_h = height - top - bottom
    rows = [
        ("R²", city_metrics["r2"], ageb_metrics["r2"]),
        ("corr(log N, log Y)", city_metrics["corr_log"], ageb_metrics["corr_log"]),
        ("between-decile share", city_bw["between_share"], ageb_bw["between_share"]),
        ("within-decile share", city_bw["within_share"], ageb_bw["within_share"]),
    ]
    x_min = -0.1
    x_max = 1.0

    def px(v: float) -> float:
        return left + ((v - x_min) / max(x_max - x_min, 1e-9)) * plot_w

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">What collapses mathematically at AGEB scale</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">Blue = Mexico cities, rust = Guadalajara AGEB. Between-decile share is how much of log(Y) variance is explained just by population strata.</text>',
        f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="none" stroke="{AXIS}"/>',
    ]
    for tick in range(6):
        xv = x_min + (x_max - x_min) * tick / 5
        x = px(xv)
        body.append(f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top+plot_h}" stroke="{GRID}"/>')
        body.append(f'<text x="{x:.2f}" y="{top+plot_h+28:.2f}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">{xv:.1f}</text>')
    row_h = plot_h / len(rows)
    for idx, (label, cval, aval) in enumerate(rows):
        y = top + idx * row_h + row_h / 2
        body.append(f'<text x="{left-14}" y="{y+4:.2f}" text-anchor="end" font-size="13" font-family="{SANS}" fill="{TEXT}">{label}</text>')
        body.append(f'<line x1="{px(0):.2f}" y1="{y-8:.2f}" x2="{px(cval):.2f}" y2="{y-8:.2f}" stroke="{BLUE}" stroke-width="8"/>')
        body.append(f'<line x1="{px(0):.2f}" y1="{y+8:.2f}" x2="{px(aval):.2f}" y2="{y+8:.2f}" stroke="{RUST}" stroke-width="8"/>')
        body.append(f'<text x="{px(cval)+8:.2f}" y="{y-4:.2f}" font-size="11" font-family="{SANS}" fill="{BLUE}">{cval:.3f}</text>')
        body.append(f'<text x="{px(aval)+8:.2f}" y="{y+18:.2f}" font-size="11" font-family="{SANS}" fill="{RUST}">{aval:.3f}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "city-vs-ageb-breakdown-guadalajara-2026-04-22"
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    city_obs = _load_city_system()
    ageb_obs = _load_ageb_system()
    _augment_sami(city_obs)
    _augment_sami(ageb_obs)

    city_metrics = _variance_decomposition(city_obs)
    ageb_metrics = _variance_decomposition(ageb_obs)
    city_bins = _bin_stats(city_obs, "cities")
    ageb_bins = _bin_stats(ageb_obs, "ageb")
    city_bw = _between_within_share(city_bins, city_obs)
    ageb_bw = _between_within_share(ageb_bins, ageb_obs)
    city_extremes, city_window = _central_window_extremes(city_obs, "cities")
    ageb_extremes, ageb_window = _central_window_extremes(ageb_obs, "ageb")

    _write_csv(outdir / "system_summary.csv", [
        {"system": "cities", **city_metrics, **city_bw, "n_obs": len(city_obs)},
        {"system": "ageb_guadalajara", **ageb_metrics, **ageb_bw, "n_obs": len(ageb_obs)},
    ], ["system", "alpha", "beta", "r2", "corr_raw", "corr_log", "var_logy", "ss_total", "ss_fit", "ss_resid", "fit_share", "resid_share", "between_share", "within_share", "n_obs"])
    _write_csv(outdir / "population_decile_stats.csv", city_bins + ageb_bins, list(city_bins[0].keys()))
    _write_csv(outdir / "central_window_extremes.csv", city_extremes + ageb_extremes, list((city_extremes or ageb_extremes)[0].keys()))
    _write_csv(outdir / "central_window_summary.csv", [
        {"system": "cities", **city_window},
        {"system": "ageb_guadalajara", **ageb_window},
    ], ["system", "population_lo", "population_hi", "n_window", "y_min", "y_max", "y_ratio_max_min"])

    scatter_fig = _write_scatter_comparison(figdir / "scatter_comparison.svg", city_obs, ageb_obs)
    disp_fig = _write_bin_dispersion(figdir / "within_decile_dispersion.svg", city_bins, ageb_bins)
    metric_fig = _write_metric_bars(figdir / "metric_comparison.svg", city_metrics, ageb_metrics, city_bw, ageb_bw)
    _write_csv(figdir / "figures_manifest.csv", [
        {"figure_id": "scatter_comparison", "path": str(scatter_fig.resolve()), "description": "Cities vs AGEB scatter comparison."},
        {"figure_id": "within_decile_dispersion", "path": str(disp_fig.resolve()), "description": "Within-decile dispersion comparison."},
        {"figure_id": "metric_comparison", "path": str(metric_fig.resolve()), "description": "High-level metric comparison."},
    ], ["figure_id", "path", "description"])

    report = [
        "# City vs AGEB Breakdown Case Study",
        "",
        "This case study compares the **same law** in two systems:",
        "- national Mexico city system",
        "- AGEB system inside Guadalajara",
        "",
        "In both cases:",
        "- `Y = total establishments`",
        "- `N = population`",
        "- fit = `log(Y) = alpha + beta log(N)`",
        "",
        "## Summary",
        "",
        f"- cities: `beta = {city_metrics['beta']:+.3f}`, `R² = {city_metrics['r2']:.3f}`, `corr(logN,logY) = {city_metrics['corr_log']:+.3f}`, `between-decile share = {city_bw['between_share']:.3f}`",
        f"- Guadalajara AGEB: `beta = {ageb_metrics['beta']:+.3f}`, `R² = {ageb_metrics['r2']:.3f}`, `corr(logN,logY) = {ageb_metrics['corr_log']:+.3f}`, `between-decile share = {ageb_bw['between_share']:.3f}`",
        "",
        "## Mathematical Reading",
        "",
        f"- At city scale, population strata explain about `{_pct(city_bw['between_share'])}` of the variance in `log(Y)`.",
        f"- At AGEB scale in Guadalajara, population strata explain only `{_pct(ageb_bw['between_share'])}` of the variance in `log(Y)`.",
        f"- That means the AGEB collapse is not mainly a small-sample problem. It is a **within-stratum heterogeneity** problem: units with similar population produce very different establishment counts.",
        "",
        f"- In the central population band for cities, `Y_max / Y_min = {city_window['y_ratio_max_min']:.1f}`.",
        f"- In the central population band for Guadalajara AGEB, `Y_max / Y_min = {ageb_window['y_ratio_max_min']:.1f}`.",
        "",
        "## Key Files",
        "",
        f"- [system_summary.csv]({(outdir / 'system_summary.csv').resolve()})",
        f"- [population_decile_stats.csv]({(outdir / 'population_decile_stats.csv').resolve()})",
        f"- [central_window_extremes.csv]({(outdir / 'central_window_extremes.csv').resolve()})",
        f"- [central_window_summary.csv]({(outdir / 'central_window_summary.csv').resolve()})",
        "",
        "## Figures",
        "",
        f"- [scatter_comparison.svg]({scatter_fig.resolve()})",
        f"- [within_decile_dispersion.svg]({disp_fig.resolve()})",
        f"- [metric_comparison.svg]({metric_fig.resolve()})",
    ]
    (outdir / "report.md").write_text("\n".join(report), encoding="utf-8")
    print(json.dumps({"ok": True, "output_dir": str(outdir)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

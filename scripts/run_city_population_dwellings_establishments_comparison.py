#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
import shutil
from pathlib import Path

from urban_sami.analysis.linear_models import ols_fit


BG = "#f8f6f1"
PANEL = "#fffdf8"
GRID = "#ddd6c8"
TEXT = "#1f1f1f"
MUTED = "#625d54"
TEAL = "#0f766e"
RUST = "#b14d3b"
BLUE = "#2563eb"
SANS = "Helvetica, Arial, sans-serif"
SERIF = "Georgia, 'Times New Roman', serif"


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


def _svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _corr(xs: list[float], ys: list[float]) -> float:
    mx = sum(xs) / len(xs)
    my = sum(ys) / len(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = (sum((x - mx) ** 2 for x in xs) * sum((y - my) ** 2 for y in ys)) ** 0.5
    return num / den if den > 0 else 0.0


def _build_rows(root: Path) -> list[dict[str, float | str]]:
    counts = _read_csv(root / "dist" / "independent_city_baseline" / "city_counts.csv")
    rows: list[dict[str, float | str]] = []
    for row in counts:
        pop = _safe_float(row["population"])
        dwell = _safe_float(row["households"])
        est = _safe_float(row["est_count"])
        if pop <= 0 or dwell <= 0 or est <= 0:
            continue
        rows.append(
            {
                "city_code": str(row["city_code"]).zfill(5),
                "city_name": row["city_name"],
                "state_code": str(row["state_code"]).zfill(2),
                "population": pop,
                "occupied_dwellings": dwell,
                "est_total": est,
                "persons_per_dwelling": pop / dwell,
                "establishments_per_1000_people": 1000.0 * est / pop,
                "dwellings_per_1000_people": 1000.0 * dwell / pop,
            }
        )
    return rows


def _fit_loglog(rows: list[dict[str, float | str]], y_key: str, x_key: str) -> dict[str, object]:
    y = [math.log(float(r[y_key])) for r in rows]
    x = [math.log(float(r[x_key])) for r in rows]
    fit = ols_fit([[1.0, xi] for xi in x], y)
    return {
        "y_key": y_key,
        "x_key": x_key,
        "alpha": fit.coefficients[0],
        "beta": fit.coefficients[1],
        "alpha_stderr": fit.stderr[0],
        "beta_stderr": fit.stderr[1],
        "r2": fit.r2,
        "adj_r2": fit.adj_r2,
        "rss": fit.rss,
        "n_obs": fit.n_obs,
        "corr_log": _corr(x, y),
    }


def _augment(rows: list[dict[str, float | str]], dwell_fit: dict[str, object], est_fit: dict[str, object]) -> list[dict[str, float | str]]:
    out: list[dict[str, float | str]] = []
    ad = float(dwell_fit["alpha"])
    bd = float(dwell_fit["beta"])
    ae = float(est_fit["alpha"])
    be = float(est_fit["beta"])
    for row in rows:
        pop = float(row["population"])
        dwell = float(row["occupied_dwellings"])
        est = float(row["est_total"])
        ln_exp_d = ad + bd * math.log(pop)
        ln_exp_e = ae + be * math.log(pop)
        exp_d = math.exp(ln_exp_d)
        exp_e = math.exp(ln_exp_e)
        resid_d = math.log(dwell) - ln_exp_d
        resid_e = math.log(est) - ln_exp_e
        item = dict(row)
        item["expected_dwellings_from_population"] = exp_d
        item["expected_establishments_from_population"] = exp_e
        item["dwelling_residual_log"] = resid_d
        item["establishment_residual_log"] = resid_e
        item["residual_gap_est_minus_dwell"] = resid_e - resid_d
        item["abs_residual_gap"] = abs(resid_e - resid_d)
        item["dwelling_residual_pct"] = 100.0 * (dwell / exp_d - 1.0)
        item["establishment_residual_pct"] = 100.0 * (est / exp_e - 1.0)
        out.append(item)
    return out


def _dual_scatter_svg(path: Path, rows: list[dict[str, float | str]], dwell_fit: dict[str, object], est_fit: dict[str, object]) -> Path:
    width = 1200
    height = 680
    panel_w = 540
    gap = 40
    margin_x = 40
    margin_y = 70
    plot_w = 420
    plot_h = 500

    def panel(rows, x_key, y_key, fit, title, x0):
        xs = [math.log(float(r[x_key])) for r in rows]
        ys = [math.log(float(r[y_key])) for r in rows]
        xmin, xmax = min(xs), max(xs)
        ymin, ymax = min(ys), max(ys)

        def px(v: float) -> float:
            return x0 + 70 + (v - xmin) / max(xmax - xmin, 1e-9) * plot_w

        def py(v: float) -> float:
            return margin_y + plot_h - (v - ymin) / max(ymax - ymin, 1e-9) * plot_h

        alpha = float(fit["alpha"])
        beta = float(fit["beta"])
        line_x0, line_x1 = xmin, xmax
        line_y0, line_y1 = alpha + beta * line_x0, alpha + beta * line_x1

        parts = [
            f'<text x="{x0+20}" y="46" font-size="24" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
            f'<text x="{x0+20}" y="66" font-size="13" font-family="{SANS}" fill="{MUTED}">beta = {beta:.3f}, adjR2 = {float(fit["adj_r2"]):.3f}</text>',
            f'<line x1="{x0+70}" y1="{margin_y+plot_h}" x2="{x0+70+plot_w}" y2="{margin_y+plot_h}" stroke="{MUTED}" stroke-width="1"/>',
            f'<line x1="{x0+70}" y1="{margin_y}" x2="{x0+70}" y2="{margin_y+plot_h}" stroke="{MUTED}" stroke-width="1"/>',
            f'<line x1="{px(line_x0):.2f}" y1="{py(line_y0):.2f}" x2="{px(line_x1):.2f}" y2="{py(line_y1):.2f}" stroke="{RUST}" stroke-width="3"/>',
        ]
        for row, x, y in zip(rows, xs, ys):
            parts.append(f'<circle cx="{px(x):.2f}" cy="{py(y):.2f}" r="2.2" fill="{TEAL}" fill-opacity="0.35"/>')
        parts.append(f'<text x="{x0+70+plot_w/2:.2f}" y="{height-26}" text-anchor="middle" font-size="12.5" font-family="{SANS}" fill="{MUTED}">log(population)</text>')
        parts.append(f'<text x="{x0+18}" y="{margin_y+plot_h/2:.2f}" transform="rotate(-90 {x0+18} {margin_y+plot_h/2:.2f})" text-anchor="middle" font-size="12.5" font-family="{SANS}" fill="{MUTED}">log({y_key})</text>')
        return "".join(parts)

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="16" y="16" width="{width-32}" height="{height-32}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        panel(rows, "population", "occupied_dwellings", dwell_fit, "Occupied dwellings vs population", margin_x),
        panel(rows, "population", "est_total", est_fit, "Establishments vs population", margin_x + panel_w + gap),
    ]
    return _svg(path, width, height, "".join(body))


def _residual_scatter_svg(path: Path, rows: list[dict[str, float | str]]) -> Path:
    width = 980
    height = 760
    left = 96
    right = 48
    top = 76
    bottom = 78
    xs = [float(r["dwelling_residual_log"]) for r in rows]
    ys = [float(r["establishment_residual_log"]) for r in rows]
    xmin, xmax = min(xs), max(xs)
    ymin, ymax = min(ys), max(ys)

    def px(v: float) -> float:
        return left + (v - xmin) / max(xmax - xmin, 1e-9) * (width - left - right)

    def py(v: float) -> float:
        return height - bottom - (v - ymin) / max(ymax - ymin, 1e-9) * (height - top - bottom)

    top_gap = sorted(rows, key=lambda r: float(r["abs_residual_gap"]), reverse=True)[:15]
    top_codes = {str(r["city_code"]) for r in top_gap}
    corr = _corr(xs, ys)
    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="16" y="16" width="{width-32}" height="{height-32}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="40" y="46" font-size="28" font-family="{SERIF}" fill="{TEXT}">Residual comparison by city</text>',
        f'<text x="40" y="68" font-size="14" font-family="{SANS}" fill="{MUTED}">Do cities that sit above the dwelling law also sit above the establishments law?</text>',
        f'<text x="{width-220}" y="46" font-size="13" font-family="{SANS}" fill="{TEXT}">corr = {corr:.3f}</text>',
        f'<line x1="{left}" y1="{height-bottom}" x2="{width-right}" y2="{height-bottom}" stroke="{MUTED}" stroke-width="1"/>',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{height-bottom}" stroke="{MUTED}" stroke-width="1"/>',
        f'<line x1="{px(0):.2f}" y1="{top}" x2="{px(0):.2f}" y2="{height-bottom}" stroke="{GRID}" stroke-width="1.5"/>',
        f'<line x1="{left}" y1="{py(0):.2f}" x2="{width-right}" y2="{py(0):.2f}" stroke="{GRID}" stroke-width="1.5"/>',
    ]
    for row in rows:
        x = float(row["dwelling_residual_log"])
        y = float(row["establishment_residual_log"])
        code = str(row["city_code"])
        color = RUST if code in top_codes else TEAL
        radius = 4.0 if code in top_codes else 2.2
        opacity = 0.85 if code in top_codes else 0.35
        body.append(f'<circle cx="{px(x):.2f}" cy="{py(y):.2f}" r="{radius}" fill="{color}" fill-opacity="{opacity}"/>')
    for row in top_gap:
        x = float(row["dwelling_residual_log"])
        y = float(row["establishment_residual_log"])
        label = str(row["city_name"]).replace("&", "&amp;")
        body.append(f'<text x="{px(x)+6:.2f}" y="{py(y)-4:.2f}" font-size="10.5" font-family="{SANS}" fill="{TEXT}">{label}</text>')
    body.append(f'<text x="{width/2:.2f}" y="{height-24}" text-anchor="middle" font-size="13" font-family="{SANS}" fill="{MUTED}">dwelling residual (log)</text>')
    body.append(f'<text x="20" y="{height/2:.2f}" transform="rotate(-90 20 {height/2:.2f})" text-anchor="middle" font-size="13" font-family="{SANS}" fill="{MUTED}">establishment residual (log)</text>')
    return _svg(path, width, height, "".join(body))


def _gap_rank_svg(path: Path, rows: list[dict[str, float | str]]) -> Path:
    top_pos = sorted(rows, key=lambda r: float(r["residual_gap_est_minus_dwell"]), reverse=True)[:20]
    top_neg = sorted(rows, key=lambda r: float(r["residual_gap_est_minus_dwell"]))[:20]
    display = list(top_pos) + list(reversed(top_neg))
    width = 1180
    height = 100 + len(display) * 26 + 60
    left = 330
    mid = width / 2
    scale = max(abs(float(r["residual_gap_est_minus_dwell"])) for r in display)

    def px(v: float) -> float:
        span = width / 2 - left - 40
        return mid + (v / max(scale, 1e-9)) * span

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="16" y="16" width="{width-32}" height="{height-32}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="40" y="46" font-size="28" font-family="{SERIF}" fill="{TEXT}">Cities where the two laws separate most</text>',
        f'<text x="40" y="68" font-size="14" font-family="{SANS}" fill="{MUTED}">Positive = establishments sit higher than dwellings relative to population.</text>',
        f'<line x1="{mid:.2f}" y1="86" x2="{mid:.2f}" y2="{height-32}" stroke="{MUTED}" stroke-width="1"/>',
    ]
    for i, row in enumerate(display):
        y = 100 + i * 26
        v = float(row["residual_gap_est_minus_dwell"])
        x2 = px(v)
        color = RUST if v > 0 else BLUE
        label = f'{row["city_name"]} ({row["state_code"]})'.replace("&", "&amp;")
        body.append(f'<text x="{left-14}" y="{y+4:.2f}" text-anchor="end" font-size="11.5" font-family="{SANS}" fill="{TEXT}">{label}</text>')
        body.append(f'<line x1="{mid:.2f}" y1="{y:.2f}" x2="{x2:.2f}" y2="{y:.2f}" stroke="{color}" stroke-width="7"/>')
        tx = x2 + 8 if v >= 0 else x2 - 8
        ta = "start" if v >= 0 else "end"
        body.append(f'<text x="{tx:.2f}" y="{y+4:.2f}" text-anchor="{ta}" font-size="11" font-family="{SANS}" fill="{TEXT}">{v:.3f}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "city-population-dwellings-establishments-comparison-2026-04-23"
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    base_rows = _build_rows(root)
    dwell_fit = _fit_loglog(base_rows, "occupied_dwellings", "population")
    est_fit = _fit_loglog(base_rows, "est_total", "population")
    rows = _augment(base_rows, dwell_fit, est_fit)
    rows = sorted(rows, key=lambda r: float(r["population"]), reverse=True)

    top_gap = sorted(rows, key=lambda r: float(r["abs_residual_gap"]), reverse=True)
    largest30 = rows[:30]
    _write_csv(outdir / "model_comparison.csv", [dwell_fit, est_fit], list(dwell_fit.keys()))
    _write_csv(outdir / "city_law_comparison_master.csv", rows, list(rows[0].keys()))
    _write_csv(outdir / "largest30_law_comparison.csv", largest30, list(largest30[0].keys()))
    _write_csv(outdir / "top_residual_gap.csv", top_gap[:50], list(top_gap[0].keys()))

    fig1 = _dual_scatter_svg(figdir / "dual_scatter_population_laws.svg", rows, dwell_fit, est_fit)
    fig2 = _residual_scatter_svg(figdir / "residual_comparison_scatter.svg", rows)
    fig3 = _gap_rank_svg(figdir / "residual_gap_rank.svg", rows)
    _write_csv(
        figdir / "figures_manifest.csv",
        [
            {"figure_id": "dual_scatter", "path": str(fig1.resolve()), "description": "Side by side city laws."},
            {"figure_id": "residual_scatter", "path": str(fig2.resolve()), "description": "Residual comparison between both laws."},
            {"figure_id": "gap_rank", "path": str(fig3.resolve()), "description": "Cities with largest residual gap between laws."},
        ],
        ["figure_id", "path", "description"],
    )

    lines = [
        "# City Population Laws Comparison",
        "",
        "This pack compares two city-scale power laws against the same size variable `population`.",
        "",
        "Models:",
        "- `log(occupied_dwellings) = alpha_d + beta_d log(population)`",
        "- `log(establishments) = alpha_e + beta_e log(population)`",
        "",
        f"Universe: `{len(rows)}` cities",
        "",
        "## Main Comparison",
        f"- occupied dwellings: `beta = {float(dwell_fit['beta']):.6f}`, `adjR2 = {float(dwell_fit['adj_r2']):.6f}`",
        f"- establishments: `beta = {float(est_fit['beta']):.6f}`, `adjR2 = {float(est_fit['adj_r2']):.6f}`",
        "",
        "Interpretation key:",
        "- `dwelling_residual_log > 0`: more dwellings than expected for population",
        "- `establishment_residual_log > 0`: more establishments than expected for population",
        "- `residual_gap_est_minus_dwell > 0`: establishments sit higher than dwellings relative to their own population laws",
        "",
        "## Files",
        f"- [model_comparison.csv]({(outdir / 'model_comparison.csv').resolve()})",
        f"- [city_law_comparison_master.csv]({(outdir / 'city_law_comparison_master.csv').resolve()})",
        f"- [largest30_law_comparison.csv]({(outdir / 'largest30_law_comparison.csv').resolve()})",
        f"- [top_residual_gap.csv]({(outdir / 'top_residual_gap.csv').resolve()})",
        "",
        "## Figures",
        f"- [dual_scatter_population_laws.svg]({fig1.resolve()})",
        f"- [residual_comparison_scatter.svg]({fig2.resolve()})",
        f"- [residual_gap_rank.svg]({fig3.resolve()})",
    ]
    (outdir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

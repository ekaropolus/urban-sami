#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import shutil
from pathlib import Path

from pyproj import CRS, Transformer
from shapely.geometry import shape
from shapely.ops import transform

from urban_sami.analysis.linear_models import ols_fit


BG = "#f8f6f1"
PANEL = "#fffdf8"
GRID = "#ddd6c8"
TEXT = "#1f1f1f"
MUTED = "#625d54"
TEAL = "#0f766e"
RUST = "#b14d3b"
SAND = "#c98f52"
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


def _city_area_map(root: Path) -> dict[str, float]:
    out: dict[str, float] = {}
    raw_dir = root / "data" / "raw" / "inegi_municipal_geojson"
    for path in sorted(raw_dir.glob("*.geojson")):
        obj = json.loads(path.read_text(encoding="utf-8"))
        for feat in obj.get("features", []):
            props = feat["properties"]
            code = str(props.get("cvegeo", "")).strip()
            if not code:
                continue
            geom = shape(feat["geometry"])
            lon, lat = geom.centroid.x, geom.centroid.y
            zone = int((lon + 180.0) // 6.0) + 1
            epsg = 32600 + zone if lat >= 0 else 32700 + zone
            transformer = Transformer.from_crs(CRS.from_epsg(4326), CRS.from_epsg(epsg), always_xy=True)
            geom_proj = transform(transformer.transform, geom)
            out[code] = geom_proj.area / 1_000_000.0
    return out


def _build_rows(root: Path) -> list[dict[str, float | str]]:
    counts = _read_csv(root / "dist" / "independent_city_baseline" / "city_counts.csv")
    area_map = _city_area_map(root)
    rows: list[dict[str, float | str]] = []
    for row in counts:
        code = str(row["city_code"]).zfill(5)
        pop = _safe_float(row["population"])
        dwell = _safe_float(row["households"])
        est = _safe_float(row["est_count"])
        area = area_map.get(code, 0.0)
        if pop <= 0 or dwell <= 0 or est <= 0 or area <= 0:
            continue
        rows.append(
            {
                "city_code": code,
                "city_name": row["city_name"],
                "state_code": str(row["state_code"]).zfill(2),
                "population": pop,
                "occupied_dwellings": dwell,
                "est_total": est,
                "area_km2": area,
                "rho_pop": pop / area,
                "rho_dwellings": dwell / area,
                "persons_per_dwelling": pop / dwell,
                "dwellings_per_1000_people": 1000.0 * dwell / pop,
            }
        )
    return rows


def _corr(xs: list[float], ys: list[float]) -> float:
    mx = sum(xs) / len(xs)
    my = sum(ys) / len(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = (sum((x - mx) ** 2 for x in xs) * sum((y - my) ** 2 for y in ys)) ** 0.5
    return num / den if den > 0 else 0.0


def _fit_population_to_dwellings(rows: list[dict[str, float | str]]) -> dict[str, object]:
    y = [math.log(float(r["occupied_dwellings"])) for r in rows]
    x = [math.log(float(r["population"])) for r in rows]
    fit = ols_fit([[1.0, xi] for xi in x], y)
    return {
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


def _augment(rows: list[dict[str, float | str]], fit: dict[str, object]) -> list[dict[str, float | str]]:
    alpha = float(fit["alpha"])
    beta = float(fit["beta"])
    out: list[dict[str, float | str]] = []
    for row in rows:
        pop = float(row["population"])
        dwell = float(row["occupied_dwellings"])
        ln_exp = alpha + beta * math.log(pop)
        expected = math.exp(ln_exp)
        residual = math.log(dwell) - ln_exp
        item = dict(row)
        item["expected_dwellings_from_population"] = expected
        item["dwelling_residual_log"] = residual
        item["dwelling_residual_pct"] = 100.0 * (dwell / expected - 1.0)
        out.append(item)
    return out


def _scatter_svg(path: Path, rows: list[dict[str, float | str]], fit: dict[str, object]) -> Path:
    width = 1100
    height = 760
    left = 96
    right = 48
    top = 76
    bottom = 78
    xs = [math.log(float(r["population"])) for r in rows]
    ys = [math.log(float(r["occupied_dwellings"])) for r in rows]
    xmin, xmax = min(xs), max(xs)
    ymin, ymax = min(ys), max(ys)

    def px(v: float) -> float:
        return left + (v - xmin) / max(xmax - xmin, 1e-9) * (width - left - right)

    def py(v: float) -> float:
        return height - bottom - (v - ymin) / max(ymax - ymin, 1e-9) * (height - top - bottom)

    alpha = float(fit["alpha"])
    beta = float(fit["beta"])
    line_x0, line_x1 = xmin, xmax
    line_y0, line_y1 = alpha + beta * line_x0, alpha + beta * line_x1
    top15 = sorted(rows, key=lambda r: float(r["population"]), reverse=True)[:15]
    top_codes = {str(r["city_code"]) for r in top15}

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="16" y="16" width="{width-32}" height="{height-32}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="40" y="46" font-size="28" font-family="{SERIF}" fill="{TEXT}">Population vs occupied dwellings</text>',
        f'<text x="40" y="68" font-size="14" font-family="{SANS}" fill="{MUTED}">Cities only. Log-log fit of occupied dwellings against population.</text>',
        f'<line x1="{left}" y1="{height-bottom}" x2="{width-right}" y2="{height-bottom}" stroke="{MUTED}" stroke-width="1"/>',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{height-bottom}" stroke="{MUTED}" stroke-width="1"/>',
        f'<line x1="{px(line_x0):.2f}" y1="{py(line_y0):.2f}" x2="{px(line_x1):.2f}" y2="{py(line_y1):.2f}" stroke="{RUST}" stroke-width="3"/>',
        f'<text x="{width-360}" y="46" font-size="13" font-family="{SANS}" fill="{TEXT}">beta = {beta:.3f}, adjR2 = {float(fit["adj_r2"]):.3f}</text>',
    ]
    for row, x, y in zip(rows, xs, ys):
        code = str(row["city_code"])
        color = RUST if code in top_codes else TEAL
        radius = 3.5 if code in top_codes else 2.1
        opacity = 0.85 if code in top_codes else 0.35
        body.append(f'<circle cx="{px(x):.2f}" cy="{py(y):.2f}" r="{radius}" fill="{color}" fill-opacity="{opacity}"/>')
    for row in top15:
        x = math.log(float(row["population"]))
        y = math.log(float(row["occupied_dwellings"]))
        label = str(row["city_name"]).replace("&", "&amp;")
        body.append(f'<text x="{px(x)+6:.2f}" y="{py(y)-4:.2f}" font-size="10.5" font-family="{SANS}" fill="{TEXT}">{label}</text>')
    body.append(f'<text x="{width/2:.2f}" y="{height-24}" text-anchor="middle" font-size="13" font-family="{SANS}" fill="{MUTED}">log(population)</text>')
    body.append(f'<text x="20" y="{height/2:.2f}" transform="rotate(-90 20 {height/2:.2f})" text-anchor="middle" font-size="13" font-family="{SANS}" fill="{MUTED}">log(occupied dwellings)</text>')
    return _svg(path, width, height, "".join(body))


def _top_bottom_svg(path: Path, title: str, rows_low: list[dict[str, float | str]], rows_high: list[dict[str, float | str]], metric: str, fmt: str) -> Path:
    rows = list(rows_high) + list(reversed(rows_low))
    width = 1160
    height = 100 + len(rows) * 26 + 60
    left = 330
    mid = width / 2
    scale = max(max(abs(float(r[metric])) for r in rows), 1e-9)

    def px(v: float) -> float:
        span = width / 2 - left - 40
        return mid + (v / scale) * span

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="16" y="16" width="{width-32}" height="{height-32}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="40" y="46" font-size="28" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="40" y="68" font-size="14" font-family="{SANS}" fill="{MUTED}">Top and bottom cities.</text>',
        f'<line x1="{mid:.2f}" y1="86" x2="{mid:.2f}" y2="{height-32}" stroke="{MUTED}" stroke-width="1"/>',
    ]
    for i, row in enumerate(rows):
        y = 100 + i * 26
        v = float(row[metric])
        x2 = px(v)
        color = RUST if v > 0 else BLUE
        label = f'{row["city_name"]} ({row["state_code"]})'.replace("&", "&amp;")
        anchor = "end" if v > 0 else "start"
        lx = left - 12 if v > 0 else left + (width - left * 2) + 12
        body.append(f'<text x="{left-14}" y="{y+4:.2f}" text-anchor="end" font-size="11.5" font-family="{SANS}" fill="{TEXT}">{label}</text>')
        body.append(f'<line x1="{mid:.2f}" y1="{y:.2f}" x2="{x2:.2f}" y2="{y:.2f}" stroke="{color}" stroke-width="7"/>')
        tx = x2 + 8 if v >= 0 else x2 - 8
        ta = "start" if v >= 0 else "end"
        body.append(f'<text x="{tx:.2f}" y="{y+4:.2f}" text-anchor="{ta}" font-size="11" font-family="{SANS}" fill="{TEXT}">{format(v, fmt)}</text>')
    return _svg(path, width, height, "".join(body))


def _largest_cities_svg(path: Path, rows: list[dict[str, float | str]], metric: str, title: str, fmt: str) -> Path:
    width = 1080
    height = 100 + len(rows) * 26 + 60
    left = 330
    right = 90
    top = 96
    bottom = 56
    vals = [float(r[metric]) for r in rows]
    xmin = min(vals)
    xmax = max(vals)

    def px(v: float) -> float:
        return left + (v - xmin) / max(xmax - xmin, 1e-9) * (width - left - right)

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="16" y="16" width="{width-32}" height="{height-32}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="40" y="46" font-size="28" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="40" y="68" font-size="14" font-family="{SANS}" fill="{MUTED}">Largest cities by population.</text>',
    ]
    for i, row in enumerate(rows):
        y = top + i * 26
        v = float(row[metric])
        label = f'{row["city_name"]} ({int(float(row["population"])):,})'.replace(",", " ").replace("&", "&amp;")
        body.append(f'<text x="{left-12}" y="{y+4:.2f}" text-anchor="end" font-size="11.5" font-family="{SANS}" fill="{TEXT}">{label}</text>')
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{px(v):.2f}" y2="{y:.2f}" stroke="{TEAL}" stroke-width="7"/>')
        body.append(f'<text x="{px(v)+8:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">{format(v, fmt)}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "city-population-vs-dwellings-2026-04-23"
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    base_rows = _build_rows(root)
    fit = _fit_population_to_dwellings(base_rows)
    rows = _augment(base_rows, fit)

    master = sorted(rows, key=lambda r: float(r["population"]), reverse=True)
    highest_ppd = sorted(rows, key=lambda r: float(r["persons_per_dwelling"]), reverse=True)[:20]
    lowest_ppd = sorted(rows, key=lambda r: float(r["persons_per_dwelling"]))[:20]
    highest_resid = sorted(rows, key=lambda r: float(r["dwelling_residual_log"]), reverse=True)[:20]
    lowest_resid = sorted(rows, key=lambda r: float(r["dwelling_residual_log"]))[:20]
    largest30 = master[:30]

    _write_csv(outdir / "city_population_dwellings_master.csv", master, list(master[0].keys()))
    _write_csv(outdir / "highest_persons_per_dwelling.csv", highest_ppd, list(highest_ppd[0].keys()))
    _write_csv(outdir / "lowest_persons_per_dwelling.csv", lowest_ppd, list(lowest_ppd[0].keys()))
    _write_csv(outdir / "highest_dwelling_residual.csv", highest_resid, list(highest_resid[0].keys()))
    _write_csv(outdir / "lowest_dwelling_residual.csv", lowest_resid, list(lowest_resid[0].keys()))
    _write_csv(outdir / "largest30_city_comparison.csv", largest30, list(largest30[0].keys()))
    _write_csv(outdir / "model_summary.csv", [fit], list(fit.keys()))

    fig1 = _scatter_svg(figdir / "population_vs_occupied_dwellings_scatter.svg", rows, fit)
    fig2 = _top_bottom_svg(figdir / "persons_per_dwelling_top_bottom.svg", "Persons per dwelling", lowest_ppd, highest_ppd, "persons_per_dwelling", ".2f")
    fig3 = _top_bottom_svg(figdir / "dwelling_residual_top_bottom.svg", "Dwelling residual vs population law", lowest_resid, highest_resid, "dwelling_residual_log", ".3f")
    fig4 = _largest_cities_svg(figdir / "largest30_persons_per_dwelling.svg", largest30, "persons_per_dwelling", "Largest cities: persons per dwelling", ".2f")

    _write_csv(
        figdir / "figures_manifest.csv",
        [
            {"figure_id": "scatter", "path": str(fig1.resolve()), "description": "Population vs occupied dwellings scatter."},
            {"figure_id": "persons_per_dwelling_top_bottom", "path": str(fig2.resolve()), "description": "Top and bottom persons per dwelling."},
            {"figure_id": "dwelling_residual_top_bottom", "path": str(fig3.resolve()), "description": "Cities with more/fewer dwellings than expected from population."},
            {"figure_id": "largest30_persons_per_dwelling", "path": str(fig4.resolve()), "description": "Largest cities ranked by persons per dwelling."},
        ],
        ["figure_id", "path", "description"],
    )

    lines = [
        "# City Population vs Occupied Dwellings",
        "",
        "This pack compares population and occupied dwellings city by city.",
        "",
        "Source note:",
        "- `occupied_dwellings` is the INEGI municipal field `total_viviendas_habitadas`.",
        "",
        "Model:",
        "- `log(occupied_dwellings) = alpha + beta log(population)`",
        "",
        f"Universe: `{len(rows)}` cities",
        "",
        "## Fit",
        f"- `alpha = {float(fit['alpha']):.6f}`",
        f"- `beta = {float(fit['beta']):.6f}`",
        f"- `adjR2 = {float(fit['adj_r2']):.6f}`",
        f"- `corr(log population, log occupied_dwellings) = {float(fit['corr_log']):.6f}`",
        "",
        "## Files",
        f"- [city_population_dwellings_master.csv]({(outdir / 'city_population_dwellings_master.csv').resolve()})",
        f"- [highest_persons_per_dwelling.csv]({(outdir / 'highest_persons_per_dwelling.csv').resolve()})",
        f"- [lowest_persons_per_dwelling.csv]({(outdir / 'lowest_persons_per_dwelling.csv').resolve()})",
        f"- [highest_dwelling_residual.csv]({(outdir / 'highest_dwelling_residual.csv').resolve()})",
        f"- [lowest_dwelling_residual.csv]({(outdir / 'lowest_dwelling_residual.csv').resolve()})",
        f"- [largest30_city_comparison.csv]({(outdir / 'largest30_city_comparison.csv').resolve()})",
        f"- [model_summary.csv]({(outdir / 'model_summary.csv').resolve()})",
        "",
        "## Figures",
        f"- [population_vs_occupied_dwellings_scatter.svg]({fig1.resolve()})",
        f"- [persons_per_dwelling_top_bottom.svg]({fig2.resolve()})",
        f"- [dwelling_residual_top_bottom.svg]({fig3.resolve()})",
        f"- [largest30_persons_per_dwelling.svg]({fig4.resolve()})",
    ]
    (outdir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

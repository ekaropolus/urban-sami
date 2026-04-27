#!/usr/bin/env python3
from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path


ROOT = Path("/home/hadox/cmd-center/platforms/research/urban-sami")
MASTER = ROOT / "reports" / "city-partition-study-pack-2026-04-22" / "city_comparison_master.csv"
CITY_Y = ROOT / "reports" / "city-y-sami-comparison-pack-2026-04-21" / "city_y_sami_long.csv"
SCIAN2 = ROOT / "reports" / "city-y-curated-results-pack-2026-04-22" / "scian2_retained.csv"
OUTPUT = ROOT / "reports" / "city-multi-criteria-pack-2026-04-22"

TEXT = "#1f1f1f"
MUTED = "#5f5a53"
GRID = "#e6e0d8"
FRAME = "#8a8277"
PAGE_BG = "#f8f6f1"
BLUE = "#295f86"
RUST = "#a24d3f"
GREEN = "#466b4f"


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


def _fmt(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}"


def _write_svg(path: Path, body: str, width: int, height: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _size_band(pop: float) -> str:
    if pop >= 1000000:
        return "macro"
    if pop >= 250000:
        return "large"
    if pop >= 50000:
        return "medium"
    return "small"


def _sami_band(sami: float) -> str:
    if sami >= 1.0:
        return "high_upper_tail"
    if sami >= 0.3:
        return "moderate_upper_tail"
    if sami <= -1.0:
        return "high_lower_tail"
    if sami <= -0.3:
        return "moderate_lower_tail"
    return "near_expectation"


def _rank_bar_chart(rows: list[dict], label_key: str, value_key: str, title: str, subtitle: str, path: Path, color: str) -> Path:
    width = 1200
    top = 100
    bottom = 60
    left = 260
    right = 80
    row_h = 28
    height = top + bottom + row_h * len(rows)
    vals = [_to_float(r[value_key]) for r in rows]
    vmax = max(vals) if vals else 1.0

    def px(v: float) -> float:
        return left + (v / vmax) * (width - left - right)

    body = [
        f'<rect width="{width}" height="{height}" fill="{PAGE_BG}"/>',
        f'<text x="42" y="44" font-size="28" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">{title}</text>',
        f'<text x="42" y="68" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{subtitle}</text>',
    ]
    for frac in (0, 0.25, 0.5, 0.75, 1):
        gv = vmax * frac
        gx = px(gv)
        body.append(f'<line x1="{gx:.2f}" y1="{top-8}" x2="{gx:.2f}" y2="{height-bottom+8}" stroke="{GRID}" stroke-width="1"/>')
        body.append(f'<text x="{gx:.2f}" y="{height-18}" text-anchor="middle" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{_fmt(gv,1)}</text>')
    for i, row in enumerate(rows):
        y = top + i * row_h + 10
        v = _to_float(row[value_key])
        body.append(f'<text x="{left-12}" y="{y+4:.2f}" text-anchor="end" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">{row[label_key]}</text>')
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{px(v):.2f}" y2="{y:.2f}" stroke="{color}" stroke-width="4"/>')
        body.append(f'<circle cx="{px(v):.2f}" cy="{y:.2f}" r="4.4" fill="{color}"/>')
        body.append(f'<text x="{px(v)+8:.2f}" y="{y+4:.2f}" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{_fmt(v,1)}</text>')
    return _write_svg(path, "".join(body), width, height)


def _state_grid(rows: list[dict], path: Path) -> Path:
    width = 1180
    height = 860
    x0 = 90
    y0 = 120
    cell_w = 95
    cell_h = 42
    states = rows[:32]
    vmax = max(abs(_to_float(r["median_total_sami"])) for r in states) if states else 1.0
    body = [
        f'<rect width="{width}" height="{height}" fill="{PAGE_BG}"/>',
        f'<text x="42" y="44" font-size="28" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">State-level city profile summary</text>',
        f'<text x="42" y="68" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">Each cell reports city count and median total SAMI by state.</text>',
    ]
    cols = 5
    for idx, row in enumerate(states):
        c = idx % cols
        r = idx // cols
        x = x0 + c * (cell_w + 20)
        y = y0 + r * (cell_h + 18)
        sami = _to_float(row["median_total_sami"])
        t = max(-1.0, min(1.0, sami / max(vmax, 1e-9)))
        if t >= 0:
            fill = f"rgb({int(255 - 110*t)},{int(250 - 140*t)},{int(247 - 150*t)})"
        else:
            t = abs(t)
            fill = f"rgb({int(255 - 170*t)},{int(250 - 120*t)},{int(247 - 80*t)})"
        body.append(f'<rect x="{x}" y="{y}" width="{cell_w}" height="{cell_h}" rx="6" fill="{fill}" stroke="#d5cec2"/>')
        body.append(f'<text x="{x+10}" y="{y+15}" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">{row["state_code"]}</text>')
        body.append(f'<text x="{x+47.5}" y="{y+16}" text-anchor="middle" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">{row["n_cities"]}</text>')
        body.append(f'<text x="{x+47.5}" y="{y+33}" text-anchor="middle" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{_fmt(sami,2)}</text>')
    return _write_svg(path, "".join(body), width, height)


def _size_scian2_chart(rows: list[dict], band: str, path: Path) -> Path:
    band_rows = [r for r in rows if r["size_band"] == band]
    top = sorted(band_rows, key=lambda r: _to_float(r["median_sami"]), reverse=True)[:10]
    return _rank_bar_chart(
        top,
        "category_label",
        "median_sami",
        f"Top scian2 by median SAMI in {band} cities",
        "Median SAMI for retained scian2 sectors within this population-size band.",
        path,
        RUST,
    )


def main() -> int:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    figdir = OUTPUT / "figures"
    figdir.mkdir(parents=True, exist_ok=True)

    master = _read_csv(MASTER)
    city_y = _read_csv(CITY_Y)
    scian2 = _read_csv(SCIAN2)
    scian2_keys = {r["y_key"]: r["category_label"] for r in scian2}
    master_by_city = {r["city_code"]: r for r in master}

    size_rows = []
    for row in master:
        row["size_band"] = _size_band(_to_float(row["population"]))
        row["sami_band"] = _sami_band(_to_float(row["total_sami"]))
        size_rows.append(row)
    _write_csv(OUTPUT / "city_master_enriched.csv", size_rows, list(size_rows[0].keys()) if size_rows else [])

    # Size band summaries
    size_summary = []
    for band in ["small", "medium", "large", "macro"]:
        subset = [r for r in size_rows if r["size_band"] == band]
        pops = sorted(_to_float(r["population"]) for r in subset)
        ests = sorted(_to_float(r["total_establishments"]) for r in subset)
        samis = sorted(_to_float(r["total_sami"]) for r in subset)
        rates = sorted(_to_float(r["est_per_1000_pop"]) for r in subset)
        size_summary.append({
            "size_band": band,
            "n_cities": len(subset),
            "median_population": pops[len(pops)//2] if pops else 0.0,
            "median_establishments": ests[len(ests)//2] if ests else 0.0,
            "median_total_sami": samis[len(samis)//2] if samis else 0.0,
            "median_est_per_1000_pop": rates[len(rates)//2] if rates else 0.0,
        })
    _write_csv(OUTPUT / "size_band_summary.csv", size_summary, list(size_summary[0].keys()) if size_summary else [])

    # SAMI band summaries
    sami_summary = []
    for band in ["high_upper_tail", "moderate_upper_tail", "near_expectation", "moderate_lower_tail", "high_lower_tail"]:
        subset = [r for r in size_rows if r["sami_band"] == band]
        pops = sorted(_to_float(r["population"]) for r in subset)
        ests = sorted(_to_float(r["total_establishments"]) for r in subset)
        rates = sorted(_to_float(r["est_per_1000_pop"]) for r in subset)
        sami_summary.append({
            "sami_band": band,
            "n_cities": len(subset),
            "median_population": pops[len(pops)//2] if pops else 0.0,
            "median_establishments": ests[len(ests)//2] if ests else 0.0,
            "median_est_per_1000_pop": rates[len(rates)//2] if rates else 0.0,
        })
    _write_csv(OUTPUT / "sami_band_summary.csv", sami_summary, list(sami_summary[0].keys()) if sami_summary else [])

    # State summaries
    by_state = defaultdict(list)
    for row in size_rows:
        by_state[row["state_code"]].append(row)
    state_rows = []
    for state_code, subset in by_state.items():
        samis = sorted(_to_float(r["total_sami"]) for r in subset)
        rates = sorted(_to_float(r["est_per_1000_pop"]) for r in subset)
        state_rows.append({
            "state_code": state_code,
            "n_cities": len(subset),
            "median_total_sami": samis[len(samis)//2] if samis else 0.0,
            "median_est_per_1000_pop": rates[len(rates)//2] if rates else 0.0,
        })
    state_rows.sort(key=lambda r: r["state_code"])
    _write_csv(OUTPUT / "state_city_summary.csv", state_rows, list(state_rows[0].keys()) if state_rows else [])

    # SCIAN2 within size bands
    scian2_band_rows = []
    grouped = defaultdict(list)
    for row in city_y:
        if row["y_key"] not in scian2_keys:
            continue
        city = master_by_city.get(row["city_code"])
        if not city:
            continue
        grouped[(city["size_band"], row["y_key"])].append(row)
    for (band, y_key), subset in grouped.items():
        med = sorted(_to_float(r["sami"]) for r in subset)
        scian2_band_rows.append({
            "size_band": band,
            "y_key": y_key,
            "category_label": scian2_keys[y_key],
            "n_cities": len(subset),
            "median_sami": med[len(med)//2] if med else 0.0,
        })
    scian2_band_rows.sort(key=lambda r: (r["size_band"], -_to_float(r["median_sami"])))
    _write_csv(OUTPUT / "scian2_by_size_band.csv", scian2_band_rows, list(scian2_band_rows[0].keys()) if scian2_band_rows else [])

    # Figures
    figs = []
    fig = _rank_bar_chart(
        sorted(size_summary, key=lambda r: ["small","medium","large","macro"].index(r["size_band"])),
        "size_band",
        "median_est_per_1000_pop",
        "Median establishment intensity by city size band",
        "Cities grouped into small, medium, large and macro bands by population.",
        figdir / "intensity_by_size_band.svg",
        BLUE,
    )
    figs.append({"figure_key": "intensity_by_size_band", "path": str(fig)})
    fig = _rank_bar_chart(
        sorted(sami_summary, key=lambda r: ["high_upper_tail","moderate_upper_tail","near_expectation","moderate_lower_tail","high_lower_tail"].index(r["sami_band"])),
        "sami_band",
        "median_est_per_1000_pop",
        "Median establishment intensity by SAMI band",
        "Cities grouped by total-establishment SAMI band.",
        figdir / "intensity_by_sami_band.svg",
        GREEN,
    )
    figs.append({"figure_key": "intensity_by_sami_band", "path": str(fig)})
    fig = _state_grid(state_rows, figdir / "state_city_summary_grid.svg")
    figs.append({"figure_key": "state_city_summary_grid", "path": str(fig)})
    for band in ["small", "medium", "large", "macro"]:
        fig = _size_scian2_chart(scian2_band_rows, band, figdir / f"scian2_top_{band}_cities.svg")
        figs.append({"figure_key": f"scian2_top_{band}_cities", "path": str(fig)})
    _write_csv(OUTPUT / "figures_manifest.csv", figs, ["figure_key", "path"])

    report = f"""# City Multi-Criteria Pack

Date: `2026-04-22`

This pack extends the first city comparison pack in four directions:
- population-size bands: small, medium, large, macro
- state summaries
- SAMI bands
- retained `SCIAN2` sectors within each population-size band

Main outputs:
- [city_master_enriched.csv]({OUTPUT / 'city_master_enriched.csv'})
- [size_band_summary.csv]({OUTPUT / 'size_band_summary.csv'})
- [sami_band_summary.csv]({OUTPUT / 'sami_band_summary.csv'})
- [state_city_summary.csv]({OUTPUT / 'state_city_summary.csv'})
- [scian2_by_size_band.csv]({OUTPUT / 'scian2_by_size_band.csv'})

Figures:
- [intensity_by_size_band.svg]({figdir / 'intensity_by_size_band.svg'})
- [intensity_by_sami_band.svg]({figdir / 'intensity_by_sami_band.svg'})
- [state_city_summary_grid.svg]({figdir / 'state_city_summary_grid.svg'})
- [scian2_top_small_cities.svg]({figdir / 'scian2_top_small_cities.svg'})
- [scian2_top_medium_cities.svg]({figdir / 'scian2_top_medium_cities.svg'})
- [scian2_top_large_cities.svg]({figdir / 'scian2_top_large_cities.svg'})
- [scian2_top_macro_cities.svg]({figdir / 'scian2_top_macro_cities.svg'})

Interpretive use:
- size bands let you compare peer city systems
- state summaries let you see where city profiles cluster geographically
- SAMI bands let you inspect what kinds of cities sit above, near, or below expectation
- `SCIAN2` by size band tells you which broad sectors dominate the deviation structure in each city-size regime
"""
    (OUTPUT / "report.md").write_text(report, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
from collections import defaultdict
from pathlib import Path


ROOT = Path("/home/hadox/cmd-center/platforms/research/urban-sami")
CITY_COUNTS = ROOT / "dist" / "independent_city_baseline" / "city_counts.csv"
CITY_SUMMARY = ROOT / "reports" / "city-y-sami-comparison-pack-2026-04-21" / "city_summary.csv"
OUTPUT_DIR = ROOT / "reports" / "city-partition-study-pack-2026-04-22"

TEXT = "#1f1f1f"
MUTED = "#5f5a53"
GRID = "#e6e0d8"
FRAME = "#8a8277"
BLUE = "#295f86"
RUST = "#a24d3f"
GREEN = "#466b4f"
SLATE = "#6e7f8e"
PANEL_BG = "#ffffff"
PAGE_BG = "#f8f6f1"


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


def _pct(value: float, digits: int = 1) -> str:
    return f"{value * 100:.{digits}f}%"


def _write_svg(path: Path, body: str, width: int, height: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _assign_quantile_groups(rows: list[dict], value_key: str, out_key: str, group_count: int = 10) -> None:
    ordered = sorted(rows, key=lambda row: _to_float(row[value_key]))
    n = len(ordered)
    for idx, row in enumerate(ordered):
        group = min(group_count, int((idx * group_count) / max(1, n)) + 1)
        row[out_key] = group


def _quantile_breaks(rows: list[dict], value_key: str, group_key: str, group_count: int = 10) -> list[dict]:
    out = []
    for group in range(1, group_count + 1):
        subset = [row for row in rows if int(row[group_key]) == group]
        vals = sorted(_to_float(row[value_key]) for row in subset)
        out.append(
            {
                "group": group,
                "n_cities": len(subset),
                "min_value": vals[0] if vals else 0.0,
                "median_value": vals[len(vals)//2] if vals else 0.0,
                "max_value": vals[-1] if vals else 0.0,
            }
        )
    return out


def _enrich_rows() -> list[dict]:
    counts = {row["city_code"]: row for row in _read_csv(CITY_COUNTS)}
    summary = _read_csv(CITY_SUMMARY)
    rows = []
    for row in summary:
        c = counts.get(row["city_code"])
        if not c:
            continue
        population = _to_float(row["population"])
        est = _to_float(row["total_establishments"])
        households = _to_float(row["households"])
        if population <= 0 or est <= 0:
            continue
        rows.append(
            {
                "city_code": row["city_code"],
                "city_name": row["city_name"],
                "state_code": row["state_code"],
                "population": population,
                "households": households,
                "total_establishments": est,
                "total_sami": _to_float(row["total_sami"]),
                "total_sami_rank_desc": int(float(row["total_sami_rank_desc"])),
                "stable_top_decile_count": int(float(row["stable_top_decile_count"])),
                "stable_bottom_decile_count": int(float(row["stable_bottom_decile_count"])),
                "est_per_1000_pop": (est / population) * 1000.0,
                "est_per_household": est / households if households > 0 else 0.0,
                "log10_population": math.log10(population),
                "log10_establishments": math.log10(est),
            }
        )
    _assign_quantile_groups(rows, "population", "population_decile")
    _assign_quantile_groups(rows, "total_establishments", "establishment_decile")
    _assign_quantile_groups(rows, "est_per_1000_pop", "intensity_decile")
    return rows


def _sami_rank_by_group_figure(rows: list[dict], group_key: str, title: str, subtitle: str, path: Path) -> Path:
    width = 1400
    height = 1120
    cols = 2
    rows_n = 5
    panel_w = 650
    panel_h = 180
    margin_x = 50
    margin_y = 100
    gap_x = 40
    gap_y = 24
    body = [
        f'<rect width="{width}" height="{height}" fill="{PAGE_BG}"/>',
        f'<text x="42" y="48" font-size="28" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">{title}</text>',
        f'<text x="42" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{subtitle}</text>',
    ]
    for group in range(1, 11):
        subset = sorted([row for row in rows if int(row[group_key]) == group], key=lambda r: r["total_sami"])
        col = (group - 1) % cols
        row_idx = (group - 1) // cols
        x = margin_x + col * (panel_w + gap_x)
        y = margin_y + row_idx * (panel_h + gap_y)
        plot_x0 = x + 52
        plot_y0 = y + 28
        plot_w = panel_w - 100
        plot_h = panel_h - 58
        body.append(f'<rect x="{x}" y="{y}" width="{panel_w}" height="{panel_h}" rx="8" fill="{PANEL_BG}" stroke="#d8d1c5"/>')
        body.append(f'<text x="{x+14}" y="{y+18}" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">Decile {group}</text>')
        if group_key == "population_decile":
            key_text = f"Population"
        elif group_key == "establishment_decile":
            key_text = f"Establishments"
        else:
            key_text = f"Intensity"
        body.append(f'<text x="{x+86}" y="{y+18}" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{key_text}</text>')
        if not subset:
            continue
        ys = [_to_float(r["total_sami"]) for r in subset]
        y_min = min(-2.5, min(ys))
        y_max = max(2.5, max(ys))

        def px(idx: int) -> float:
            if len(subset) == 1:
                return plot_x0 + plot_w / 2
            return plot_x0 + (idx / (len(subset) - 1)) * plot_w

        def py(val: float) -> float:
            return plot_y0 + (1 - ((val - y_min) / (y_max - y_min))) * plot_h

        for frac in (0.0, 0.5, 1.0):
            gv = y_min + frac * (y_max - y_min)
            gy = py(gv)
            body.append(f'<line x1="{plot_x0}" y1="{gy:.2f}" x2="{plot_x0+plot_w}" y2="{gy:.2f}" stroke="{GRID}" stroke-width="1"/>')
            body.append(f'<text x="{plot_x0-8}" y="{gy+4:.2f}" text-anchor="end" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{_fmt(gv,1)}</text>')
        pts = " ".join(f"{px(i):.2f},{py(_to_float(r['total_sami'])):.2f}" for i, r in enumerate(subset))
        body.append(f'<polyline fill="none" stroke="{SLATE}" stroke-width="1.4" points="{pts}"/>')
        top = max(subset, key=lambda r: _to_float(r["total_sami"]))
        bottom = min(subset, key=lambda r: _to_float(r["total_sami"]))
        for item, color in ((top, RUST), (bottom, BLUE)):
            i = subset.index(item)
            cx = px(i)
            cy = py(_to_float(item["total_sami"]))
            body.append(f'<circle cx="{cx:.2f}" cy="{cy:.2f}" r="4.4" fill="{color}"/>')
            anchor = "start" if i < len(subset) * 0.7 else "end"
            dx = 8 if anchor == "start" else -8
            body.append(f'<text x="{cx+dx:.2f}" y="{cy-8:.2f}" text-anchor="{anchor}" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">{item["city_name"]}</text>')
        body.append(f'<line x1="{plot_x0}" y1="{py(0):.2f}" x2="{plot_x0+plot_w}" y2="{py(0):.2f}" stroke="{FRAME}" stroke-width="1.1" stroke-dasharray="4,4"/>')
    return _write_svg(path, "".join(body), width, height)


def _cross_grid_figure(rows: list[dict], path: Path) -> Path:
    width = 980
    height = 940
    x0 = 140
    y0 = 120
    cell = 72
    body = [
        f'<rect width="{width}" height="{height}" fill="{PAGE_BG}"/>',
        f'<text x="42" y="48" font-size="28" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">Population decile × establishment decile</text>',
        f'<text x="42" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">Each cell reports city count and median total SAMI. This lets you inspect where cities concentrate in the joint size space.</text>',
    ]
    medians = []
    table = {}
    for p in range(1, 11):
        for e in range(1, 11):
            subset = [r for r in rows if int(r["population_decile"]) == p and int(r["establishment_decile"]) == e]
            if subset:
                vals = sorted(_to_float(r["total_sami"]) for r in subset)
                med = vals[len(vals)//2]
            else:
                med = None
            table[(p, e)] = (subset, med)
            if med is not None:
                medians.append(abs(med))
    vmax = max(medians) if medians else 1.0
    for p in range(1, 11):
        body.append(f'<text x="{x0 + (p-0.5)*cell:.2f}" y="{y0-18}" text-anchor="middle" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">{p}</text>')
        body.append(f'<text x="{x0-18}" y="{y0 + (p-0.5)*cell + 4:.2f}" text-anchor="end" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">{p}</text>')
    body.append(f'<text x="{x0+5*cell:.2f}" y="{y0-48}" text-anchor="middle" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">Establishment decile</text>')
    body.append(f'<text x="28" y="{y0+5*cell:.2f}" text-anchor="middle" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}" transform="rotate(-90 28 {y0+5*cell:.2f})">Population decile</text>')
    for p in range(1, 11):
        for e in range(1, 11):
            subset, med = table[(p, e)]
            x = x0 + (e-1) * cell
            y = y0 + (p-1) * cell
            if med is None:
                fill = "#f1ede5"
            else:
                t = max(-1.0, min(1.0, med / max(vmax, 1e-9)))
                if t >= 0:
                    fill = f"rgb({int(255 - (95*t))},{int(250 - (150*t))},{int(247 - (170*t))})"
                else:
                    t = abs(t)
                    fill = f"rgb({int(255 - (180*t))},{int(250 - (120*t))},{int(247 - (70*t))})"
            body.append(f'<rect x="{x}" y="{y}" width="{cell}" height="{cell}" fill="{fill}" stroke="#d5cec2"/>')
            body.append(f'<text x="{x+cell/2:.2f}" y="{y+28:.2f}" text-anchor="middle" font-size="13" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">{len(subset)}</text>')
            med_text = "" if med is None else _fmt(med, 2)
            body.append(f'<text x="{x+cell/2:.2f}" y="{y+48:.2f}" text-anchor="middle" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{med_text}</text>')
    return _write_svg(path, "".join(body), width, height)


def _rate_profile_figure(group_rows: list[dict], group_key: str, path: Path) -> Path:
    width = 1180
    height = 720
    x0 = 120
    y0 = 120
    bar_w = 70
    gap = 34
    plot_h = 420
    max_rate = max(_to_float(row["median_est_per_1000_pop"]) for row in group_rows) * 1.12
    body = [
        f'<rect width="{width}" height="{height}" fill="{PAGE_BG}"/>',
        f'<text x="42" y="48" font-size="28" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">Establishment intensity by partition</text>',
        f'<text x="42" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">Median establishments per 1,000 population and median total SAMI by {group_key.replace("_", " ")}.</text>',
    ]
    for frac in (0, 0.25, 0.5, 0.75, 1):
        gv = max_rate * frac
        gy = y0 + plot_h - frac * plot_h
        body.append(f'<line x1="{x0}" y1="{gy:.2f}" x2="{width-80}" y2="{gy:.2f}" stroke="{GRID}" stroke-width="1"/>')
        body.append(f'<text x="{x0-10}" y="{gy+4:.2f}" text-anchor="end" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{_fmt(gv,1)}</text>')
    for i, row in enumerate(group_rows):
        x = x0 + i * (bar_w + gap)
        rate = _to_float(row["median_est_per_1000_pop"])
        h = (rate / max_rate) * plot_h
        y = y0 + plot_h - h
        sami = _to_float(row["median_total_sami"])
        color = BLUE if sami < 0 else RUST
        body.append(f'<rect x="{x}" y="{y:.2f}" width="{bar_w}" height="{h:.2f}" fill="{color}" fill-opacity="0.82"/>')
        body.append(f'<text x="{x+bar_w/2:.2f}" y="{y0+plot_h+22}" text-anchor="middle" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">{row["group"]}</text>')
        body.append(f'<text x="{x+bar_w/2:.2f}" y="{y0+plot_h+40}" text-anchor="middle" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{_fmt(sami,2)}</text>')
    body.append(f'<text x="{x0 + 5*(bar_w+gap) - gap/2:.2f}" y="{height-18}" text-anchor="middle" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{group_key.replace("_", " ")}</text>')
    body.append(f'<text x="{width-250}" y="{y0+40}" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">bar height: median establishments per 1,000 population</text>')
    body.append(f'<text x="{width-250}" y="{y0+60}" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">bar colour sign: median total SAMI</text>')
    return _write_svg(path, "".join(body), width, height)


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    figures_dir = OUTPUT_DIR / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)

    rows = _enrich_rows()
    rows.sort(key=lambda r: r["population"])
    _write_csv(OUTPUT_DIR / "city_comparison_master.csv", rows, list(rows[0].keys()) if rows else [])

    population_breaks = _quantile_breaks(rows, "population", "population_decile")
    establishment_breaks = _quantile_breaks(rows, "total_establishments", "establishment_decile")
    intensity_breaks = _quantile_breaks(rows, "est_per_1000_pop", "intensity_decile")
    _write_csv(OUTPUT_DIR / "population_decile_breaks.csv", population_breaks, list(population_breaks[0].keys()) if population_breaks else [])
    _write_csv(OUTPUT_DIR / "establishment_decile_breaks.csv", establishment_breaks, list(establishment_breaks[0].keys()) if establishment_breaks else [])
    _write_csv(OUTPUT_DIR / "intensity_decile_breaks.csv", intensity_breaks, list(intensity_breaks[0].keys()) if intensity_breaks else [])

    group_summaries = []
    for key in ("population_decile", "establishment_decile", "intensity_decile"):
        for group in range(1, 11):
            subset = [r for r in rows if int(r[key]) == group]
            pops = sorted(_to_float(r["population"]) for r in subset)
            ests = sorted(_to_float(r["total_establishments"]) for r in subset)
            samis = sorted(_to_float(r["total_sami"]) for r in subset)
            rates = sorted(_to_float(r["est_per_1000_pop"]) for r in subset)
            group_summaries.append(
                {
                    "grouping": key,
                    "group": group,
                    "n_cities": len(subset),
                    "median_population": pops[len(pops)//2] if pops else 0.0,
                    "median_establishments": ests[len(ests)//2] if ests else 0.0,
                    "median_total_sami": samis[len(samis)//2] if samis else 0.0,
                    "median_est_per_1000_pop": rates[len(rates)//2] if rates else 0.0,
                    "top_city_by_sami": max(subset, key=lambda r: _to_float(r["total_sami"]))["city_name"] if subset else "",
                    "bottom_city_by_sami": min(subset, key=lambda r: _to_float(r["total_sami"]))["city_name"] if subset else "",
                }
            )
    _write_csv(OUTPUT_DIR / "group_summaries.csv", group_summaries, list(group_summaries[0].keys()) if group_summaries else [])

    top_bottom_rows = []
    for key in ("population_decile", "establishment_decile", "intensity_decile"):
        for group in range(1, 11):
            subset = sorted([r for r in rows if int(r[key]) == group], key=lambda r: _to_float(r["total_sami"]))
            if not subset:
                continue
            for side, selection in (("bottom", subset[:10]), ("top", list(reversed(subset[-10:])))):
                for rank, row in enumerate(selection, start=1):
                    top_bottom_rows.append(
                        {
                            "grouping": key,
                            "group": group,
                            "side": side,
                            "rank_within_group": rank,
                            "city_code": row["city_code"],
                            "city_name": row["city_name"],
                            "state_code": row["state_code"],
                            "population": row["population"],
                            "total_establishments": row["total_establishments"],
                            "est_per_1000_pop": row["est_per_1000_pop"],
                            "total_sami": row["total_sami"],
                        }
                    )
    _write_csv(OUTPUT_DIR / "top_bottom_cities_by_group.csv", top_bottom_rows, list(top_bottom_rows[0].keys()) if top_bottom_rows else [])

    pop_summary_rows = [r for r in group_summaries if r["grouping"] == "population_decile"]
    est_summary_rows = [r for r in group_summaries if r["grouping"] == "establishment_decile"]

    figures_manifest = []
    fig = _sami_rank_by_group_figure(
        rows,
        "population_decile",
        "Total SAMI within population deciles",
        "Cities are split into ten population groups; each panel ranks cities by total-establishment SAMI within that size band.",
        figures_dir / "total_sami_by_population_decile.svg",
    )
    figures_manifest.append({"figure_key": "total_sami_by_population_decile", "path": str(fig)})
    fig = _sami_rank_by_group_figure(
        rows,
        "establishment_decile",
        "Total SAMI within establishment deciles",
        "Cities are split into ten establishment-count groups; each panel ranks cities by total-establishment SAMI within that activity band.",
        figures_dir / "total_sami_by_establishment_decile.svg",
    )
    figures_manifest.append({"figure_key": "total_sami_by_establishment_decile", "path": str(fig)})
    fig = _cross_grid_figure(rows, figures_dir / "population_x_establishment_decile_grid.svg")
    figures_manifest.append({"figure_key": "population_x_establishment_decile_grid", "path": str(fig)})
    fig = _rate_profile_figure(pop_summary_rows, "population_decile", figures_dir / "intensity_by_population_decile.svg")
    figures_manifest.append({"figure_key": "intensity_by_population_decile", "path": str(fig)})
    fig = _rate_profile_figure(est_summary_rows, "establishment_decile", figures_dir / "intensity_by_establishment_decile.svg")
    figures_manifest.append({"figure_key": "intensity_by_establishment_decile", "path": str(fig)})
    _write_csv(OUTPUT_DIR / "figures_manifest.csv", figures_manifest, ["figure_key", "path"])

    report = f"""# City Partition Study Pack

Date: `2026-04-22`

This pack is designed for readable city comparison.

Instead of throwing all cities into one uninterpretable figure, it partitions the urban system by:
- population deciles
- establishment-count deciles
- establishment intensity deciles (`establishments per 1,000 population`)

Main outputs:
- [city_comparison_master.csv]({OUTPUT_DIR / 'city_comparison_master.csv'})
- [group_summaries.csv]({OUTPUT_DIR / 'group_summaries.csv'})
- [top_bottom_cities_by_group.csv]({OUTPUT_DIR / 'top_bottom_cities_by_group.csv'})
- [population_decile_breaks.csv]({OUTPUT_DIR / 'population_decile_breaks.csv'})
- [establishment_decile_breaks.csv]({OUTPUT_DIR / 'establishment_decile_breaks.csv'})
- [intensity_decile_breaks.csv]({OUTPUT_DIR / 'intensity_decile_breaks.csv'})

Figures:
- [total_sami_by_population_decile.svg]({figures_dir / 'total_sami_by_population_decile.svg'})
- [total_sami_by_establishment_decile.svg]({figures_dir / 'total_sami_by_establishment_decile.svg'})
- [population_x_establishment_decile_grid.svg]({figures_dir / 'population_x_establishment_decile_grid.svg'})
- [intensity_by_population_decile.svg]({figures_dir / 'intensity_by_population_decile.svg'})
- [intensity_by_establishment_decile.svg]({figures_dir / 'intensity_by_establishment_decile.svg'})

How to read it:
- use the population-decile figure to compare cities against peers of similar size
- use the establishment-decile figure to compare cities against peers with similar economic mass
- use the 10×10 grid to see where the national city system concentrates
- use the intensity figures to see where establishments per 1,000 population rise or fall across partitions

Core variables in the master table:
- `population`
- `total_establishments`
- `total_sami`
- `est_per_1000_pop`
- `est_per_household`
- `population_decile`
- `establishment_decile`
- `intensity_decile`
"""
    (OUTPUT_DIR / "report.md").write_text(report, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

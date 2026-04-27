#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
from pathlib import Path


ROOT = Path("/home/hadox/cmd-center/platforms/research/urban-sami")
CITY_BASELINE = ROOT / "dist" / "independent_city_baseline" / "city_counts.csv"
FAMILY_SUMMARY = ROOT / "reports" / "city-y-interpretive-pack-2026-04-22" / "family_interpretive_summary.csv"
Y_SUMMARY = ROOT / "reports" / "city-y-interpretive-pack-2026-04-22" / "y_interpretive_summary.csv"
CITY_SIGNAL = ROOT / "reports" / "city-y-interpretive-pack-2026-04-22" / "city_signal_summary.csv"
OUTPUT_DIR = ROOT / "reports" / "city-paper-figures-2026-04-22"

W_MM = 183
H_MM = 148
PX_W = 1380
PX_H = 1115

TEXT = "#1f1f1f"
MUTED = "#5f5a53"
GRID = "#e6e0d8"
FRAME = "#8a8277"
BLUE = "#295f86"
SLATE = "#5e7690"
RUST = "#a24d3f"
GREEN = "#466b4f"
LIGHT_BLUE = "#7aa3c2"
LIGHT_RUST = "#d28c7c"


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_svg(path: Path, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W_MM}mm" height="{H_MM}mm" '
        f'viewBox="0 0 {PX_W} {PX_H}" role="img">{body}</svg>'
    )
    path.write_text(svg, encoding="utf-8")
    return path


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _fmt(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}"


def _panel_label(letter: str, title: str, x: int, y: int) -> str:
    return (
        f'<text x="{x}" y="{y}" font-size="22" font-family="Helvetica, Arial, sans-serif" font-weight="700" fill="{TEXT}">{letter}</text>'
        f'<text x="{x+30}" y="{y}" font-size="20" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">{title}</text>'
    )


def _scatter_panel(rows: list[dict[str, str]], x: int, y: int, w: int, h: int) -> str:
    pad_l, pad_r, pad_t, pad_b = 76, 24, 20, 56
    plot_x0 = x + pad_l
    plot_y0 = y + pad_t
    plot_w = w - pad_l - pad_r
    plot_h = h - pad_t - pad_b

    fit_rows = [row for row in rows if _to_float(row["population"]) > 0 and _to_float(row["est_count"]) > 0]
    xs = [math.log10(_to_float(row["population"])) for row in fit_rows]
    ys = [math.log10(_to_float(row["est_count"])) for row in fit_rows]
    x_min, x_max = min(xs), max(xs)
    y_min, y_max = min(ys), max(ys)
    x_pad = (x_max - x_min) * 0.05
    y_pad = (y_max - y_min) * 0.08
    x_min -= x_pad
    x_max += x_pad
    y_min -= y_pad
    y_max += y_pad

    def px(v: float) -> float:
        return plot_x0 + ((v - x_min) / (x_max - x_min)) * plot_w

    def py(v: float) -> float:
        return plot_y0 + (1 - ((v - y_min) / (y_max - y_min))) * plot_h

    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" fill="white"/>',
        f'<rect x="{plot_x0}" y="{plot_y0}" width="{plot_w}" height="{plot_h}" fill="none" stroke="{FRAME}" stroke-width="1.2"/>',
    ]
    for frac in (0, 0.25, 0.5, 0.75, 1):
        gx = plot_x0 + frac * plot_w
        gy = plot_y0 + frac * plot_h
        xv = x_min + frac * (x_max - x_min)
        yv = y_max - frac * (y_max - y_min)
        parts.append(f'<line x1="{gx:.2f}" y1="{plot_y0}" x2="{gx:.2f}" y2="{plot_y0+plot_h}" stroke="{GRID}" stroke-width="1"/>')
        parts.append(f'<line x1="{plot_x0}" y1="{gy:.2f}" x2="{plot_x0+plot_w}" y2="{gy:.2f}" stroke="{GRID}" stroke-width="1"/>')
        parts.append(f'<text x="{gx:.2f}" y="{y+h-18}" text-anchor="middle" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{_fmt(xv,1)}</text>')
        parts.append(f'<text x="{x+58}" y="{gy+4:.2f}" text-anchor="end" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{_fmt(yv,1)}</text>')
    for row in fit_rows:
        cx = px(math.log10(_to_float(row["population"])))
        cy = py(math.log10(_to_float(row["est_count"])))
        parts.append(f'<circle cx="{cx:.2f}" cy="{cy:.2f}" r="2.4" fill="{BLUE}" fill-opacity="0.42"/>')

    alpha = -2.7945126110318235
    beta = 0.9495638769896361
    line_pts = []
    for t in [x_min, x_max]:
        yy = alpha + beta * t
        line_pts.append(f"{px(t):.2f},{py(yy):.2f}")
    parts.append(f'<polyline fill="none" stroke="{RUST}" stroke-width="2.2" points="{" ".join(line_pts)}"/>')
    parts.append(f'<text x="{plot_x0+12}" y="{plot_y0+18}" font-size="13" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">beta = 0.95   R² = 0.85</text>')
    parts.append(f'<text x="{plot_x0 + plot_w/2:.2f}" y="{y+h+18}" text-anchor="middle" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">log10 population</text>')
    parts.append(f'<text x="{x+18}" y="{plot_y0 + plot_h/2:.2f}" text-anchor="middle" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}" transform="rotate(-90 {x+18} {plot_y0 + plot_h/2:.2f})">log10 establishments</text>')
    return "".join(parts)


def _family_summary_panel(rows: list[dict[str, str]], x: int, y: int, w: int, h: int) -> str:
    pad_l, pad_r, pad_t, pad_b = 90, 20, 20, 56
    plot_x0 = x + pad_l
    plot_y0 = y + pad_t
    plot_w = w - pad_l - pad_r
    plot_h = h - pad_t - pad_b
    families = [row["family"] for row in rows if row["family"] != "scian3"]
    data = [row for row in rows if row["family"] != "scian3"]
    values = [_to_float(row["median_r2"]) for row in data]
    vmin, vmax = 0.0, max(values) + 0.05

    def px(v: float) -> float:
        return plot_x0 + (v / vmax) * plot_w

    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" fill="white"/>',
        f'<line x1="{plot_x0}" y1="{plot_y0+plot_h}" x2="{plot_x0+plot_w}" y2="{plot_y0+plot_h}" stroke="{FRAME}" stroke-width="1.2"/>',
    ]
    for frac in (0, 0.25, 0.5, 0.75, 1):
        gv = vmax * frac
        gx = px(gv)
        parts.append(f'<line x1="{gx:.2f}" y1="{plot_y0}" x2="{gx:.2f}" y2="{plot_y0+plot_h}" stroke="{GRID}" stroke-width="1"/>')
        parts.append(f'<text x="{gx:.2f}" y="{y+h-18}" text-anchor="middle" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{_fmt(gv,2)}</text>')
    row_h = plot_h / max(1, len(data))
    color_map = {"total": BLUE, "per_ocu": SLATE, "size_class": GREEN, "scian2": RUST}
    for i, row in enumerate(data):
        yy = plot_y0 + (i + 0.5) * row_h
        r2 = _to_float(row["median_r2"])
        beta = _to_float(row["median_beta"])
        parts.append(f'<text x="{plot_x0-12}" y="{yy+4:.2f}" text-anchor="end" font-size="13" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">{row["family"]}</text>')
        parts.append(f'<line x1="{plot_x0}" y1="{yy:.2f}" x2="{px(r2):.2f}" y2="{yy:.2f}" stroke="{color_map[row["family"]]}" stroke-width="4.2"/>')
        parts.append(f'<circle cx="{px(r2):.2f}" cy="{yy:.2f}" r="5.2" fill="{color_map[row["family"]]}"/>')
        parts.append(f'<text x="{px(r2)+10:.2f}" y="{yy+4:.2f}" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">beta {beta:.2f}</text>')
    parts.append(f'<text x="{plot_x0 + plot_w/2:.2f}" y="{y+h+18}" text-anchor="middle" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">median R² by retained family</text>')
    return "".join(parts)


def _rank_panel(rows: list[dict[str, str]], value_key: str, x: int, y: int, w: int, h: int, color: str, label: str) -> str:
    rows = rows[:8]
    pad_l, pad_r, pad_t, pad_b = 170, 20, 20, 46
    plot_x0 = x + pad_l
    plot_y0 = y + pad_t
    plot_w = w - pad_l - pad_r
    plot_h = h - pad_t - pad_b
    vals = [_to_float(r[value_key]) for r in rows]
    vmin = 0.0 if value_key == "r2" else min(vals) - 0.05
    vmax = max(vals) + 0.05

    def px(v: float) -> float:
        return plot_x0 + ((v - vmin) / (vmax - vmin)) * plot_w

    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" fill="white"/>',
        f'<line x1="{plot_x0}" y1="{plot_y0+plot_h}" x2="{plot_x0+plot_w}" y2="{plot_y0+plot_h}" stroke="{FRAME}" stroke-width="1.2"/>',
    ]
    for frac in (0, 0.25, 0.5, 0.75, 1):
        gv = vmin + frac * (vmax - vmin)
        gx = px(gv)
        parts.append(f'<line x1="{gx:.2f}" y1="{plot_y0}" x2="{gx:.2f}" y2="{plot_y0+plot_h}" stroke="{GRID}" stroke-width="1"/>')
        parts.append(f'<text x="{gx:.2f}" y="{y+h-12}" text-anchor="middle" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{_fmt(gv,2)}</text>')
    row_h = plot_h / max(1, len(rows))
    for i, row in enumerate(rows):
        yy = plot_y0 + (i + 0.5) * row_h
        value = _to_float(row[value_key])
        parts.append(f'<text x="{plot_x0-12}" y="{yy+4:.2f}" text-anchor="end" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">{row["category_label"]}</text>')
        parts.append(f'<line x1="{plot_x0}" y1="{yy:.2f}" x2="{px(value):.2f}" y2="{yy:.2f}" stroke="{color}" stroke-width="3.8"/>')
        parts.append(f'<circle cx="{px(value):.2f}" cy="{yy:.2f}" r="4.7" fill="{color}"/>')
    parts.append(f'<text x="{plot_x0 + plot_w/2:.2f}" y="{y+h+12}" text-anchor="middle" font-size="13" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{label}</text>')
    return "".join(parts)


def _tail_panel(rows: list[dict[str, str]], key: str, x: int, y: int, w: int, h: int, color: str, title: str) -> str:
    rows = rows[:8]
    pad_l, pad_r, pad_t, pad_b = 180, 20, 20, 46
    plot_x0 = x + pad_l
    plot_y0 = y + pad_t
    plot_w = w - pad_l - pad_r
    plot_h = h - pad_t - pad_b
    vmax = max(_to_float(r[key]) for r in rows) + 2

    def px(v: float) -> float:
        return plot_x0 + (v / vmax) * plot_w

    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" fill="white"/>',
        f'<line x1="{plot_x0}" y1="{plot_y0+plot_h}" x2="{plot_x0+plot_w}" y2="{plot_y0+plot_h}" stroke="{FRAME}" stroke-width="1.2"/>',
    ]
    for frac in (0, 0.25, 0.5, 0.75, 1):
        gv = vmax * frac
        gx = px(gv)
        parts.append(f'<line x1="{gx:.2f}" y1="{plot_y0}" x2="{gx:.2f}" y2="{plot_y0+plot_h}" stroke="{GRID}" stroke-width="1"/>')
        parts.append(f'<text x="{gx:.2f}" y="{y+h-12}" text-anchor="middle" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{int(round(gv))}</text>')
    row_h = plot_h / max(1, len(rows))
    for i, row in enumerate(rows):
        yy = plot_y0 + (i + 0.5) * row_h
        value = _to_float(row[key])
        parts.append(f'<text x="{plot_x0-12}" y="{yy+4:.2f}" text-anchor="end" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="{TEXT}">{row["city_name"]}</text>')
        parts.append(f'<line x1="{plot_x0}" y1="{yy:.2f}" x2="{px(value):.2f}" y2="{yy:.2f}" stroke="{color}" stroke-width="3.8"/>')
        parts.append(f'<circle cx="{px(value):.2f}" cy="{yy:.2f}" r="4.7" fill="{color}"/>')
    parts.append(f'<text x="{plot_x0 + plot_w/2:.2f}" y="{y+h+12}" text-anchor="middle" font-size="13" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{title}</text>')
    return "".join(parts)


def main() -> int:
    city_rows = _read_csv(CITY_BASELINE)
    family_rows = _read_csv(FAMILY_SUMMARY)
    y_rows = _read_csv(Y_SUMMARY)
    signal_rows = _read_csv(CITY_SIGNAL)
    scian2 = [r for r in y_rows if r["family"] == "scian2"]
    scian2_r2 = sorted(scian2, key=lambda r: _to_float(r["r2"]), reverse=True)
    upper = sorted(signal_rows, key=lambda r: int(r["positive_top_decile_count"]), reverse=True)
    lower = sorted(signal_rows, key=lambda r: int(r["negative_bottom_decile_count"]), reverse=True)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    manifest = []

    # Figure 1
    parts = [
        f'<rect width="{PX_W}" height="{PX_H}" fill="white"/>',
        _panel_label("a", "Aggregate city scaling", 36, 40),
        _scatter_panel(city_rows, 30, 55, 640, 470),
        _panel_label("b", "Retained family fit", 720, 40),
        _family_summary_panel(family_rows, 710, 55, 640, 470),
        _panel_label("c", "Sector fit at scian2", 36, 585),
        _rank_panel(scian2_r2, "r2", 30, 600, 640, 455, RUST, "R²"),
        _panel_label("d", "Residual spread across retained y", 720, 585),
        _rank_panel(sorted(y_rows, key=lambda r: _to_float(r["sami_q90_range"]), reverse=True)[:8], "sami_q90_range", 710, 600, 640, 455, GREEN, "SAMI 90% range"),
    ]
    fig1 = _write_svg(OUTPUT_DIR / "figure1_city_overview.svg", "".join(parts))
    manifest.append({"figure_key": "figure1_city_overview", "path": str(fig1)})

    # Figure 2
    parts = [
        f'<rect width="{PX_W}" height="{PX_H}" fill="white"/>',
        _panel_label("a", "Scian2 beta", 36, 40),
        _rank_panel(sorted(scian2, key=lambda r: _to_float(r["beta"]), reverse=True)[:8], "beta", 30, 55, 640, 470, RUST, "beta"),
        _panel_label("b", "Scian2 fit", 720, 40),
        _rank_panel(scian2_r2, "r2", 710, 55, 640, 470, RUST, "R²"),
        _panel_label("c", "Upper-tail deviation recurrence", 36, 585),
        _tail_panel(upper, "positive_top_decile_count", 30, 600, 640, 455, BLUE, "Top-decile recurrence across retained y"),
        _panel_label("d", "Lower-tail deviation recurrence", 720, 585),
        _tail_panel(lower, "negative_bottom_decile_count", 710, 600, 640, 455, GREEN, "Bottom-decile recurrence across retained y"),
    ]
    fig2 = _write_svg(OUTPUT_DIR / "figure2_city_deviation_profiles.svg", "".join(parts))
    manifest.append({"figure_key": "figure2_city_deviation_profiles", "path": str(fig2)})

    with (OUTPUT_DIR / "figures_manifest.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["figure_key", "path"])
        writer.writeheader()
        writer.writerows(manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

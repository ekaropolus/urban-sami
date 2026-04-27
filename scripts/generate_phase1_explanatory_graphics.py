#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "docs" / "figures" / "phase1"

CITY_COUNTS = ROOT / "dist" / "independent_city_baseline" / "city_counts.csv"
MODEL_SUMMARY = ROOT / "dist" / "independent_city_baseline" / "model_summary.csv"
RESIDUALS = ROOT / "dist" / "independent_city_baseline" / "residuals.csv"
FITABILITY = ROOT / "reports" / "city-y-fitability-audit-2026-04-21" / "city_y_fitability_full.csv"
CORE_SUMMARY = ROOT / "reports" / "city-y-curated-results-pack-2026-04-22" / "core_family_summary.csv"
CURATED = ROOT / "reports" / "city-y-curated-results-pack-2026-04-22" / "curated_y_catalog.csv"

INK = "#202124"
MUTED = "#5f646d"
GRID = "#d7dce2"
BG = "#fbfaf7"
PANEL = "#ffffff"
BLUE = "#2c6fbb"
TEAL = "#1b8a7a"
RUST = "#bb5a3c"
GOLD = "#c7932b"
GREEN = "#4f8a46"
GRAY = "#9aa1aa"

CLASS_COLORS = {
    "A_strong": TEAL,
    "B_usable": BLUE,
    "C_exploratory": GOLD,
    "D_unfit": RUST,
}

STATE_PALETTE = [
    "#1f77b4",
    "#ff7f0e",
    "#2ca02c",
    "#d62728",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#7f7f7f",
    "#bcbd22",
    "#17becf",
    "#4e79a7",
    "#f28e2b",
    "#59a14f",
    "#e15759",
    "#76b7b2",
    "#edc948",
    "#b07aa1",
    "#ff9da7",
    "#9c755f",
    "#bab0ab",
    "#006d77",
    "#83c5be",
    "#e29578",
    "#6d597a",
    "#b56576",
    "#355070",
    "#588157",
    "#a3b18a",
    "#bc6c25",
    "#606c38",
    "#3a86ff",
    "#fb5607",
]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def f(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def svg(path: Path, body: str, width: int = 1200, height: int = 760) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>\n',
        encoding="utf-8",
    )
    return path


def esc(text: object) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def frame(width: int, height: int, title: str, subtitle: str) -> list[str]:
    return [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="18" y="18" width="{width-36}" height="{height-36}" rx="8" fill="{PANEL}" stroke="#d9d2c4"/>',
        f'<text x="48" y="54" font-size="26" font-family="Georgia, Times, serif" fill="{INK}">{esc(title)}</text>',
        f'<text x="48" y="80" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="{MUTED}">{esc(subtitle)}</text>',
    ]


def log_ticks(vmin: float, vmax: float) -> list[float]:
    lo = math.floor(math.log10(vmin))
    hi = math.ceil(math.log10(vmax))
    return [10.0**k for k in range(lo, hi + 1) if vmin <= 10.0**k <= vmax]


def write_scaling_law() -> Path:
    rows = [r for r in read_csv(CITY_COUNTS) if f(r["population"]) > 0 and f(r["est_count"]) > 0]
    best = next(r for r in read_csv(MODEL_SUMMARY) if r["fit_method"] == "ols")
    alpha = f(best["alpha"])
    beta = f(best["beta"])
    r2 = f(best["r2"])

    width, height = 1200, 820
    left, top, right, bottom = 120, 120, 70, 115
    plot_w = width - left - right
    plot_h = height - top - bottom

    xs = [f(r["population"]) for r in rows]
    ys = [f(r["est_count"]) for r in rows]
    state_codes = sorted({r["state_code"] for r in rows})
    state_colors = {state: STATE_PALETTE[idx % len(STATE_PALETTE)] for idx, state in enumerate(state_codes)}
    xmin, xmax = min(xs), max(xs)
    ymin, ymax = min(ys), max(ys)
    lxmin, lxmax = math.log10(xmin) - 0.05, math.log10(xmax) + 0.05
    lymin, lymax = math.log10(ymin) - 0.15, math.log10(ymax) + 0.15

    def px(x: float) -> float:
        return left + ((math.log10(x) - lxmin) / (lxmax - lxmin)) * plot_w

    def py(y: float) -> float:
        return top + plot_h - ((math.log10(y) - lymin) / (lymax - lymin)) * plot_h

    parts = frame(width, height, "Phase I: the first city law", "Each point is one Mexican municipality. The line is the fitted DENUE establishments law.")
    parts.append(f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="#fbfdff" stroke="#8c939b"/>')

    for tick in log_ticks(xmin, xmax):
        x = px(tick)
        parts.append(f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{top+plot_h}" stroke="{GRID}"/>')
        parts.append(f'<text x="{x:.1f}" y="{top+plot_h+28}" text-anchor="middle" font-size="12" font-family="Helvetica" fill="{MUTED}">{tick:,.0f}</text>')
    for tick in log_ticks(ymin, ymax):
        y = py(tick)
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left+plot_w}" y2="{y:.1f}" stroke="{GRID}"/>')
        parts.append(f'<text x="{left-14}" y="{y+4:.1f}" text-anchor="end" font-size="12" font-family="Helvetica" fill="{MUTED}">{tick:,.0f}</text>')

    for row in rows:
        x = f(row["population"])
        y = f(row["est_count"])
        color = state_colors[row["state_code"]]
        parts.append(f'<circle cx="{px(x):.1f}" cy="{py(y):.1f}" r="2.25" fill="{color}" fill-opacity="0.58"/>')

    x1, x2 = xmin, xmax
    y1 = math.exp(alpha + beta * math.log(x1))
    y2 = math.exp(alpha + beta * math.log(x2))
    parts.append(f'<line x1="{px(x1):.1f}" y1="{py(y1):.1f}" x2="{px(x2):.1f}" y2="{py(y2):.1f}" stroke="{RUST}" stroke-width="3"/>')

    note_x, note_y = left + 36, top + 36
    parts.append(f'<rect x="{note_x}" y="{note_y}" width="280" height="96" rx="6" fill="#fff8ed" stroke="#e4c99c"/>')
    parts.append(f'<text x="{note_x+18}" y="{note_y+30}" font-size="17" font-family="Helvetica" font-weight="700" fill="{INK}">log E = alpha + beta log N</text>')
    parts.append(f'<text x="{note_x+18}" y="{note_y+58}" font-size="15" font-family="Helvetica" fill="{INK}">beta = {beta:.3f}   adj. R² = {r2:.3f}</text>')
    parts.append(f'<text x="{note_x+18}" y="{note_y+82}" font-size="13" font-family="Helvetica" fill="{MUTED}">n = {len(rows):,} cities with positive N and E</text>')

    legend_x = left + plot_w - 330
    legend_y = top + plot_h - 142
    parts.append(f'<rect x="{legend_x}" y="{legend_y}" width="295" height="104" rx="6" fill="#ffffff" fill-opacity="0.88" stroke="#d9d2c4"/>')
    parts.append(f'<text x="{legend_x+14}" y="{legend_y+24}" font-size="13" font-family="Helvetica" font-weight="700" fill="{INK}">Color = state_code</text>')
    for idx, state in enumerate(state_codes):
        col = idx % 8
        row = idx // 8
        x = legend_x + 15 + col * 34
        y = legend_y + 44 + row * 17
        parts.append(f'<circle cx="{x}" cy="{y}" r="4" fill="{state_colors[state]}" fill-opacity="0.9"/>')
        parts.append(f'<text x="{x+7}" y="{y+4}" font-size="10" font-family="Helvetica" fill="{MUTED}">{esc(state)}</text>')

    parts.append(f'<text x="{left+plot_w/2}" y="{height-38}" text-anchor="middle" font-size="15" font-family="Helvetica" fill="{INK}">Population N, log scale</text>')
    parts.append(f'<text x="34" y="{top+plot_h/2}" text-anchor="middle" font-size="15" font-family="Helvetica" fill="{INK}" transform="rotate(-90 34 {top+plot_h/2})">DENUE establishments E, log scale</text>')
    return svg(OUT_DIR / "phase1_city_scaling_law.svg", "".join(parts), width, height)


def write_residual_histogram() -> Path:
    rows = read_csv(RESIDUALS)
    vals = [f(r["epsilon_log"]) for r in rows]
    width, height = 1200, 700
    left, top, right, bottom = 90, 120, 70, 90
    plot_w = width - left - right
    plot_h = height - top - bottom
    bins = 34
    vmin, vmax = -2.5, 2.5
    counts = [0] * bins
    for value in vals:
        idx = int((min(max(value, vmin), vmax - 1e-9) - vmin) / (vmax - vmin) * bins)
        counts[idx] += 1
    ymax = max(counts)

    def px(value: float) -> float:
        return left + ((value - vmin) / (vmax - vmin)) * plot_w

    def py(count: float) -> float:
        return top + plot_h - (count / ymax) * plot_h

    parts = frame(width, height, "Phase I: residuals become SAMI", "The fitted law creates a log residual for every city; that residual becomes the object studied later.")
    parts.append(f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="#fbfdff" stroke="#8c939b"/>')
    for tick in [-2, -1, 0, 1, 2]:
        x = px(tick)
        parts.append(f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{top+plot_h}" stroke="{GRID}"/>')
        parts.append(f'<text x="{x:.1f}" y="{top+plot_h+28}" text-anchor="middle" font-size="12" font-family="Helvetica" fill="{MUTED}">{tick:+.0f}</text>')
    for idx, count in enumerate(counts):
        x0 = left + idx * plot_w / bins
        w = plot_w / bins - 2
        y0 = py(count)
        parts.append(f'<rect x="{x0:.1f}" y="{y0:.1f}" width="{w:.1f}" height="{top+plot_h-y0:.1f}" fill="{TEAL}" fill-opacity="0.75"/>')
    x0 = px(0)
    parts.append(f'<line x1="{x0:.1f}" y1="{top}" x2="{x0:.1f}" y2="{top+plot_h}" stroke="{RUST}" stroke-width="3"/>')
    parts.append(f'<text x="{x0+10:.1f}" y="{top+26}" font-size="13" font-family="Helvetica" fill="{RUST}">expected by law</text>')
    parts.append(f'<text x="{left+plot_w/2}" y="{height-34}" text-anchor="middle" font-size="15" font-family="Helvetica" fill="{INK}">epsilon = log(observed / expected)</text>')
    parts.append(f'<text x="34" y="{top+plot_h/2}" text-anchor="middle" font-size="15" font-family="Helvetica" fill="{INK}" transform="rotate(-90 34 {top+plot_h/2})">number of cities</text>')
    return svg(OUT_DIR / "phase1_residual_histogram.svg", "".join(parts), width, height)


def write_sami_by_state_multipanel() -> Path:
    rows = [r for r in read_csv(RESIDUALS) if f(r["population"]) > 0]
    state_codes = sorted({r["state_code"] for r in rows})
    state_colors = {state: STATE_PALETTE[idx % len(STATE_PALETTE)] for idx, state in enumerate(state_codes)}
    by_state: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        by_state[row["state_code"]].append(row)
    for state_rows in by_state.values():
        state_rows.sort(key=lambda row: f(row["population"]), reverse=True)

    width, height = 1600, 1120
    left, top = 70, 130
    cols, rows_n = 8, 4
    gap_x, gap_y = 22, 42
    panel_w = (width - left - 50 - gap_x * (cols - 1)) / cols
    panel_h = (height - top - 120 - gap_y * (rows_n - 1)) / rows_n
    y_min, y_max = -2.5, 2.5

    def py(value: float, y0: float) -> float:
        clipped = min(max(value, y_min), y_max)
        return y0 + panel_h - ((clipped - y_min) / (y_max - y_min)) * panel_h

    parts = frame(
        width,
        height,
        "Phase I: SAMI by city, faceted by state",
        "Each panel is one state. Points are cities ordered by population; y is the city residual from the national establishments law.",
    )

    for idx, state in enumerate(state_codes):
        col = idx % cols
        row_idx = idx // cols
        x0 = left + col * (panel_w + gap_x)
        y0 = top + row_idx * (panel_h + gap_y)
        state_rows = by_state[state]
        n = len(state_rows)
        color = state_colors[state]
        mean_sami = sum(f(r["sami"]) for r in state_rows) / n if n else 0.0

        parts.append(f'<rect x="{x0:.1f}" y="{y0:.1f}" width="{panel_w:.1f}" height="{panel_h:.1f}" fill="#fbfdff" stroke="#c7ced6"/>')
        for tick in [-2, -1, 0, 1, 2]:
            y = py(tick, y0)
            stroke = RUST if tick == 0 else GRID
            width_tick = 1.4 if tick == 0 else 0.8
            parts.append(f'<line x1="{x0:.1f}" y1="{y:.1f}" x2="{x0+panel_w:.1f}" y2="{y:.1f}" stroke="{stroke}" stroke-width="{width_tick}"/>')
            if col == 0:
                parts.append(f'<text x="{x0-8:.1f}" y="{y+4:.1f}" text-anchor="end" font-size="10" font-family="Helvetica" fill="{MUTED}">{tick:+d}</text>')

        if n == 1:
            x_positions = [x0 + panel_w / 2]
        else:
            x_positions = [x0 + 6 + rank * (panel_w - 12) / (n - 1) for rank in range(n)]
        for x, city in zip(x_positions, state_rows):
            y = py(f(city["sami"]), y0)
            parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="1.8" fill="{color}" fill-opacity="0.72"/>')

        y_mean = py(mean_sami, y0)
        parts.append(f'<line x1="{x0:.1f}" y1="{y_mean:.1f}" x2="{x0+panel_w:.1f}" y2="{y_mean:.1f}" stroke="{color}" stroke-width="1.8" stroke-opacity="0.85"/>')
        parts.append(f'<text x="{x0+7:.1f}" y="{y0+16:.1f}" font-size="13" font-family="Helvetica" font-weight="700" fill="{INK}">state {esc(state)}</text>')
        parts.append(f'<text x="{x0+panel_w-7:.1f}" y="{y0+16:.1f}" text-anchor="end" font-size="10" font-family="Helvetica" fill="{MUTED}">n={n}, mean={mean_sami:+.2f}</text>')

    parts.append(f'<text x="{width/2:.1f}" y="{height-48}" text-anchor="middle" font-size="15" font-family="Helvetica" fill="{INK}">cities within state, ordered by population rank</text>')
    parts.append(f'<text x="28" y="{top + (height - top - 120)/2:.1f}" text-anchor="middle" font-size="15" font-family="Helvetica" fill="{INK}" transform="rotate(-90 28 {top + (height - top - 120)/2:.1f})">SAMI_i = log(E_i / Ehat_i)</text>')
    parts.append(f'<text x="70" y="{height-82}" font-size="12" font-family="Helvetica" fill="{MUTED}">Red line: national expectation, SAMI=0. Colored line: mean SAMI within that state.</text>')
    return svg(OUT_DIR / "phase1_sami_by_state_multipanel.svg", "".join(parts), width, height)


def write_y_beta_r2_map() -> Path:
    retained = [
        row
        for row in read_csv(CURATED)
        if row["family"] in {"total", "per_ocu", "size_class", "scian2", "scian3"}
    ]
    family_order = {"total": 0, "per_ocu": 1, "size_class": 2, "scian2": 3, "scian3": 4}
    retained.sort(key=lambda row: (family_order[row["family"]], -f(row["r2"]), f(row["beta"])))
    family_colors = {
        "total": INK,
        "per_ocu": BLUE,
        "size_class": TEAL,
        "scian2": RUST,
        "scian3": GOLD,
    }

    width, height = 1600, 1750
    left, top, right, bottom = 345, 125, 240, 95
    plot_w = width - left - right
    plot_h = height - top - bottom
    beta_min, beta_max = 0.55, 1.25
    row_step = plot_h / max(1, len(retained) - 1)

    def px(beta: float) -> float:
        clipped = min(max(beta, beta_min), beta_max)
        return left + ((clipped - beta_min) / (beta_max - beta_min)) * plot_w

    def short_label(row: dict[str, str]) -> str:
        label = row["category_label"] or row["category"]
        family = row["family"]
        if family == "total":
            label = "all establishments"
        elif family in {"scian2", "scian3"}:
            label = f'{family.upper()} {row["category"]}'
        if len(label) > 30:
            label = label[:27] + "..."
        return label

    parts = frame(
        width,
        height,
        "Phase I: each retained Y has its own beta and R2",
        "Each row is one retained outcome. Arrows connect the outcome label Y to its beta; R2 is written as a second numeric axis.",
    )
    parts.append(f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="#fbfdff" stroke="#8c939b"/>')
    parts.append(f'<text x="{left-155}" y="{top-18}" font-size="13" font-family="Helvetica" font-weight="700" fill="{INK}">Y outcome</text>')
    parts.append(f'<text x="{left+plot_w+28}" y="{top-18}" font-size="13" font-family="Helvetica" font-weight="700" fill="{INK}">R2</text>')
    parts.append(f'<line x1="{left+plot_w+16}" y1="{top}" x2="{left+plot_w+16}" y2="{top+plot_h}" stroke="#8c939b"/>')

    for tick in [0.6, 0.8, 1.0, 1.2]:
        x = px(tick)
        stroke = RUST if abs(tick - 1.0) < 1e-9 else GRID
        dash = ' stroke-dasharray="6 5"' if abs(tick - 1.0) < 1e-9 else ""
        parts.append(f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{top+plot_h}" stroke="{stroke}" stroke-width="1.2"{dash}/>')
        parts.append(f'<text x="{x:.1f}" y="{top+plot_h+28}" text-anchor="middle" font-size="12" font-family="Helvetica" fill="{MUTED}">{tick:.1f}</text>')
    parts.append(f'<text x="{px(1.0)+8:.1f}" y="{top+18}" font-size="12" font-family="Helvetica" fill="{RUST}">linear beta = 1</text>')

    last_family = ""
    for idx, row in enumerate(retained):
        y = top + idx * row_step
        family = row["family"]
        beta = f(row["beta"])
        r2 = f(row["r2"])
        point_x = px(beta)
        label = short_label(row)
        if idx % 2 == 0:
            parts.append(f'<rect x="{left}" y="{y-row_step/2:.1f}" width="{plot_w}" height="{row_step:.1f}" fill="#eef3f8" fill-opacity="0.38"/>')
        if family != last_family:
            parts.append(f'<text x="58" y="{y+4:.1f}" font-size="14" font-family="Helvetica" font-weight="700" fill="{family_colors[family]}">{esc(family)}</text>')
            last_family = family
        parts.append(f'<rect x="175" y="{y-6:.1f}" width="8" height="12" fill="{family_colors[family]}"/>')
        parts.append(f'<text x="193" y="{y+4:.1f}" font-size="11" font-family="Helvetica" fill="{INK}">{esc(label)}</text>')
        parts.append(f'<line x1="{left-20}" y1="{y:.1f}" x2="{point_x-9:.1f}" y2="{y:.1f}" stroke="#aeb8c2" stroke-width="1.1"/>')
        parts.append(f'<path d="M {point_x-9:.1f} {y:.1f} l -6 -4 l 0 8 z" fill="#aeb8c2"/>')
        parts.append(f'<circle cx="{point_x:.1f}" cy="{y:.1f}" r="5.2" fill="{family_colors[family]}" fill-opacity="0.84" stroke="#ffffff" stroke-width="0.8"/>')
        parts.append(f'<text x="{point_x+8:.1f}" y="{y-5:.1f}" font-size="9" font-family="Helvetica" fill="{family_colors[family]}">{esc(label)}</text>')
        parts.append(f'<text x="{left+plot_w+31}" y="{y+3.5:.1f}" font-size="10" font-family="Helvetica" fill="{INK}">{r2:.2f}</text>')

    parts.append(f'<text x="{left+plot_w/2}" y="{height-34}" text-anchor="middle" font-size="15" font-family="Helvetica" fill="{INK}">scaling exponent beta in log Y_i = alpha_Y + beta_Y log N_i + epsilon_i</text>')
    parts.append(f'<text x="58" y="{height-34}" font-size="12" font-family="Helvetica" fill="{MUTED}">Rows grouped by Y family; right column is the fitted R2 for the same Y.</text>')
    return svg(OUT_DIR / "phase1_y_beta_r2_map.svg", "".join(parts), width, height)


def write_fitability_map() -> Path:
    rows = read_csv(FITABILITY)
    width, height = 1380, 820
    left, top, right, bottom = 105, 120, 285, 105
    plot_w = width - left - right
    plot_h = height - top - bottom

    def px(x: float) -> float:
        return left + max(0, min(1, x)) * plot_w

    def py(y: float) -> float:
        return top + plot_h - max(0, min(1, y)) * plot_h

    parts = frame(width, height, "Phase I: not every outcome is a scaling object", "Fitability combines coverage, sparsity, and R² before an outcome is interpreted.")
    parts.append(f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="#fbfdff" stroke="#8c939b"/>')
    for tick in [0, 0.25, 0.5, 0.75, 1.0]:
        x = px(tick)
        y = py(tick)
        parts.append(f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{top+plot_h}" stroke="{GRID}"/>')
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left+plot_w}" y2="{y:.1f}" stroke="{GRID}"/>')
        parts.append(f'<text x="{x:.1f}" y="{top+plot_h+28}" text-anchor="middle" font-size="12" font-family="Helvetica" fill="{MUTED}">{tick:.2f}</text>')
        parts.append(f'<text x="{left-14}" y="{y+4:.1f}" text-anchor="end" font-size="12" font-family="Helvetica" fill="{MUTED}">{tick:.2f}</text>')

    for row in rows:
        klass = row["fitability_class"]
        radius = 1.8 if f(row["share_of_total"]) < 0.01 else 3.2
        parts.append(
            f'<circle cx="{px(f(row["coverage_rate"])):.1f}" cy="{py(f(row["r2"])):.1f}" r="{radius:.1f}" fill="{CLASS_COLORS.get(klass, GRAY)}" fill-opacity="0.58"/>'
        )

    legend_x = left + plot_w + 38
    legend_y = top
    parts.append(f'<rect x="{legend_x}" y="{legend_y}" width="205" height="118" rx="6" fill="#fff8ed" stroke="#e4c99c"/>')
    for idx, klass in enumerate(["A_strong", "B_usable", "C_exploratory", "D_unfit"]):
        y = legend_y + 24 + idx * 23
        parts.append(f'<circle cx="{legend_x+18}" cy="{y:.1f}" r="5" fill="{CLASS_COLORS[klass]}"/>')
        parts.append(f'<text x="{legend_x+34}" y="{y+4:.1f}" font-size="12" font-family="Helvetica" fill="{INK}">{klass}</text>')
    parts.append(f'<text x="{left+plot_w/2}" y="{height-34}" text-anchor="middle" font-size="15" font-family="Helvetica" fill="{INK}">coverage rate across eligible cities</text>')
    parts.append(f'<text x="34" y="{top+plot_h/2}" text-anchor="middle" font-size="15" font-family="Helvetica" fill="{INK}" transform="rotate(-90 34 {top+plot_h/2})">R²</text>')
    return svg(OUT_DIR / "phase1_fitability_map.svg", "".join(parts), width, height)


def write_beta_r2_catalog() -> Path:
    rows = [r for r in read_csv(CURATED) if r["family"] in {"total", "per_ocu", "size_class", "scian2", "scian3"}]
    width, height = 1200, 800
    left, top, right, bottom = 105, 120, 90, 105
    plot_w = width - left - right
    plot_h = height - top - bottom
    beta_min, beta_max = 0.4, 1.4
    r2_min, r2_max = 0.45, 0.9
    family_colors = {
        "total": INK,
        "per_ocu": BLUE,
        "size_class": TEAL,
        "scian2": RUST,
        "scian3": GOLD,
    }

    def px(beta: float) -> float:
        return left + ((beta - beta_min) / (beta_max - beta_min)) * plot_w

    def py(r2: float) -> float:
        return top + plot_h - ((r2 - r2_min) / (r2_max - r2_min)) * plot_h

    parts = frame(width, height, "Phase I: retained outcomes have different exponents", "Each point is one retained Y. The catalog is not one law repeated; it is a family of city laws.")
    parts.append(f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="#fbfdff" stroke="#8c939b"/>')
    for tick in [0.5, 0.75, 1.0, 1.25]:
        x = px(tick)
        parts.append(f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{top+plot_h}" stroke="{GRID}"/>')
        parts.append(f'<text x="{x:.1f}" y="{top+plot_h+28}" text-anchor="middle" font-size="12" font-family="Helvetica" fill="{MUTED}">{tick:.2f}</text>')
    for tick in [0.5, 0.6, 0.7, 0.8, 0.9]:
        y = py(tick)
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left+plot_w}" y2="{y:.1f}" stroke="{GRID}"/>')
        parts.append(f'<text x="{left-14}" y="{y+4:.1f}" text-anchor="end" font-size="12" font-family="Helvetica" fill="{MUTED}">{tick:.1f}</text>')
    x_lin = px(1.0)
    parts.append(f'<line x1="{x_lin:.1f}" y1="{top}" x2="{x_lin:.1f}" y2="{top+plot_h}" stroke="{RUST}" stroke-width="2" stroke-dasharray="6 5"/>')
    parts.append(f'<text x="{x_lin+8:.1f}" y="{top+22}" font-size="12" font-family="Helvetica" fill="{RUST}">linear beta = 1</text>')

    for row in rows:
        beta = f(row["beta"])
        r2 = f(row["r2"])
        if not (beta_min <= beta <= beta_max and r2_min <= r2 <= r2_max):
            continue
        family = row["family"]
        radius = 5.0 if family == "total" else 3.6
        parts.append(f'<circle cx="{px(beta):.1f}" cy="{py(r2):.1f}" r="{radius}" fill="{family_colors[family]}" fill-opacity="0.72"/>')

    legend_x = width - 275
    legend_y = top + 22
    parts.append(f'<rect x="{legend_x}" y="{legend_y}" width="205" height="145" rx="6" fill="#fff8ed" stroke="#e4c99c"/>')
    for idx, family in enumerate(["total", "per_ocu", "size_class", "scian2", "scian3"]):
        y = legend_y + 25 + idx * 23
        parts.append(f'<circle cx="{legend_x+18}" cy="{y:.1f}" r="5" fill="{family_colors[family]}"/>')
        parts.append(f'<text x="{legend_x+34}" y="{y+4:.1f}" font-size="12" font-family="Helvetica" fill="{INK}">{family}</text>')
    parts.append(f'<text x="{left+plot_w/2}" y="{height-34}" text-anchor="middle" font-size="15" font-family="Helvetica" fill="{INK}">scaling exponent beta</text>')
    parts.append(f'<text x="34" y="{top+plot_h/2}" text-anchor="middle" font-size="15" font-family="Helvetica" fill="{INK}" transform="rotate(-90 34 {top+plot_h/2})">R²</text>')
    return svg(OUT_DIR / "phase1_beta_r2_catalog.svg", "".join(parts), width, height)


def write_workflow() -> Path:
    width, height = 1550, 650
    parts = frame(
        width,
        height,
        "Phase I workflow as equations",
        "Phase I turns persisted population and DENUE rows into a reproducible catalog of city scaling laws.",
    )
    boxes = [
        {
            "title": "1. Persistent inputs",
            "equations": [
                "city i has population N_i",
                "DENUE row r has city c(r)",
                "attributes a(r): SCIAN, size, per_ocu",
            ],
            "note": "raw.population_units + raw.denue_establishments",
            "color": BLUE,
        },
        {
            "title": "2. City aggregation",
            "equations": [
                "E_i = sum_{r: c(r)=i} 1",
                "Y_i^(f,k) = sum_r 1[c(r)=i]",
                "              * 1[f(r)=k]",
                "one city table: (N_i, E_i, Y_i^(f,k))",
            ],
            "note": "city-level extensive variables",
            "color": TEAL,
        },
        {
            "title": "3. Baseline law",
            "equations": [
                "log E_i = alpha + beta log N_i + eps_i",
                "E_i = exp(alpha) N_i^beta exp(eps_i)",
                "beta_hat = 0.950, adj. R2 = 0.846",
            ],
            "note": "first Bettencourt-style city law",
            "color": RUST,
        },
        {
            "title": "4. Outcome catalog",
            "equations": [
                "log Y_i^(f,k) = alpha_fk",
                "                 + beta_fk log N_i",
                "                 + eps_i^(f,k)",
                "f: total, size, per_ocu, SCIAN",
            ],
            "note": "same experiment repeated across outcomes",
            "color": GOLD,
        },
        {
            "title": "5. Fitability audit",
            "equations": [
                "A_fk = (n_fk, coverage_fk,",
                "       zero_fk, R2_fk)",
                "keep if A_fk is interpretable",
                "|F_retained| = 71 outcomes",
            ],
            "note": "separates signal from sparse categories",
            "color": GREEN,
        },
    ]
    x0, y0 = 48, 150
    bw, bh, gap = 270, 245, 30
    for idx, box in enumerate(boxes):
        x = x0 + idx * (bw + gap)
        color = box["color"]
        parts.append(f'<rect x="{x}" y="{y0}" width="{bw}" height="{bh}" rx="8" fill="#fbfdff" stroke="{color}" stroke-width="2"/>')
        parts.append(f'<text x="{x+18}" y="{y0+34}" font-size="17" font-family="Helvetica" font-weight="700" fill="{INK}">{esc(box["title"])}</text>')
        for j, line in enumerate(box["equations"]):
            parts.append(f'<text x="{x+18}" y="{y0+78+j*28}" font-size="14" font-family="Georgia" font-style="italic" fill="{INK}">{esc(line)}</text>')
        parts.append(f'<line x1="{x+18}" y1="{y0+187}" x2="{x+bw-18}" y2="{y0+187}" stroke="{color}" stroke-width="1.2" stroke-opacity="0.55"/>')
        parts.append(f'<text x="{x+18}" y="{y0+216}" font-size="12" font-family="Helvetica" fill="{MUTED}">{esc(box["note"])}</text>')
        if idx < len(boxes) - 1:
            x1 = x + bw + 8
            x2 = x + bw + gap - 8
            y = y0 + bh / 2
            parts.append(f'<line x1="{x1}" y1="{y}" x2="{x2}" y2="{y}" stroke="{INK}" stroke-width="1.8"/>')
            parts.append(f'<path d="M {x2} {y} l -8 -5 l 0 10 z" fill="{INK}"/>')
            labels = ["aggregate", "fit", "expand", "curate"]
            parts.append(f'<text x="{(x1+x2)/2}" y="{y-14}" text-anchor="middle" font-size="11" font-family="Helvetica" fill="{MUTED}">{labels[idx]}</text>')

    scripts = [
        "run_independent_city_baseline.py",
        "run_city_y_fitability_audit.py",
        "run_city_y_curated_results_pack.py",
        "run_city_grouped_profiles.py",
    ]
    parts.append(f'<text x="50" y="475" font-size="17" font-family="Helvetica" font-weight="700" fill="{INK}">Executable trace</text>')
    sx, sy = 50, 505
    pill_w = [300, 305, 355, 300]
    for idx, script in enumerate(scripts):
        x = sx + sum(pill_w[:idx]) + idx * 28
        parts.append(f'<rect x="{x}" y="{sy}" width="{pill_w[idx]}" height="46" rx="23" fill="#fff8ed" stroke="#dec79e"/>')
        parts.append(f'<text x="{x+18}" y="{sy+29}" font-size="13" font-family="Helvetica" fill="{INK}">{esc(script)}</text>')
        if idx < len(scripts) - 1:
            ax1 = x + pill_w[idx] + 7
            ax2 = x + pill_w[idx] + 22
            ay = sy + 23
            parts.append(f'<line x1="{ax1}" y1="{ay}" x2="{ax2}" y2="{ay}" stroke="{INK}" stroke-width="1.4"/>')
            parts.append(f'<path d="M {ax2} {ay} l -6 -4 l 0 8 z" fill="{INK}"/>')

    parts.append(f'<text x="50" y="600" font-size="13" font-family="Helvetica" fill="{MUTED}">Interpretation: Phase I begins with one extensive urban quantity, E_i, and then asks whether many DENUE-derived Y_i variables obey comparable population laws.</text>')
    return svg(OUT_DIR / "phase1_workflow.svg", "".join(parts), width, height)


def write_guide(paths: list[Path]) -> Path:
    guide = ROOT / "docs" / "PHASE1_VISUAL_GUIDE.md"
    rels = [p.relative_to(guide.parent) for p in paths]
    guide.write_text(
        "\n".join(
            [
                "# Phase I Visual Guide",
                "",
                "This guide is the visual companion to Phase I of the final monograph.",
                "",
                "Phase I asks a deliberately simple question first: do total DENUE establishments scale with city population in Mexico? It then expands the same idea from one outcome to a catalog of economic outcomes.",
                "",
                "## 1. Workflow",
                "",
                f"![Phase I workflow]({rels[0]})",
                "",
                "This figure shows the executable path from persistent database inputs to the fitted city-law catalog.",
                "",
                "## 2. First City Law",
                "",
                f"![City scaling law]({rels[1]})",
                "",
                "Each point is one city and point color identifies its Mexican state code. The red line is the fitted law `log E = alpha + beta log N`. The core result is beta near 0.95 with adjusted R2 near 0.846.",
                "",
                "## 3. Residuals Become SAMI",
                "",
                f"![Residual histogram]({rels[2]})",
                "",
                "After fitting the law, every city has a log deviation from expectation. This residual is the object later mapped and studied as SAMI.",
                "",
                "## 4. SAMI by State",
                "",
                f"![SAMI by state]({rels[3]})",
                "",
                "This multi-panel figure keeps cities visible instead of collapsing them into one national histogram. Each panel is a state, each point is a city ordered by population, the red line is the national expectation `SAMI = 0`, and the colored line is the state mean.",
                "",
                "## 5. Y, Beta, and R2",
                "",
                f"![Y beta R2 map]({rels[4]})",
                "",
                "This figure opens the retained catalog. Each row is one retained outcome `Y`; the arrow points to its fitted exponent `beta`; the right-side numeric column gives the corresponding `R2`.",
                "",
                "## 6. Fitability",
                "",
                f"![Fitability map]({rels[5]})",
                "",
                "This plot explains why outcome curation was necessary. Some categories have strong fits and broad coverage; others are too sparse or weak to interpret as city scaling objects.",
                "",
                "## 7. Exponents Across Retained Outcomes",
                "",
                f"![Beta and R2 catalog]({rels[6]})",
                "",
                "Retained outcomes have different exponents and fit quality. Phase I therefore creates a family of scaling laws, not a single repeated result.",
                "",
                "## Reproduce",
                "",
                "```bash",
                "PYTHONPATH=src python3 scripts/generate_phase1_explanatory_graphics.py",
                "```",
                "",
                "The script reads local Phase I outputs from `dist/independent_city_baseline/` and `reports/city-y-*`, then writes the versioned SVGs under `docs/figures/phase1/`.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return guide


def main() -> int:
    required = [CITY_COUNTS, MODEL_SUMMARY, RESIDUALS, FITABILITY, CORE_SUMMARY, CURATED]
    missing = [str(path.relative_to(ROOT)) for path in required if not path.exists()]
    if missing:
        raise SystemExit("Missing required Phase I artifacts:\n" + "\n".join(missing))

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    paths = [
        write_workflow(),
        write_scaling_law(),
        write_residual_histogram(),
        write_sami_by_state_multipanel(),
        write_y_beta_r2_map(),
        write_fitability_map(),
        write_beta_r2_catalog(),
    ]
    guide = write_guide(paths)
    print("Wrote:")
    for path in [guide, *paths]:
        print(path.relative_to(ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

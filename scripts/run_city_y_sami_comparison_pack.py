#!/usr/bin/env python3
from __future__ import annotations

import csv
import html
import json
import math
import shutil
from collections import defaultdict
from pathlib import Path
from statistics import mean, median

from PIL import Image, ImageDraw, ImageFont

from run_denue_y_state_scientific_analysis import BLUE, write_ranked_metric_chart
from urban_sami.artifacts.figures import (
    AXIS,
    BG,
    GRID,
    MUTED,
    PANEL,
    RUST,
    SANS,
    SERIF,
    TEXT,
    write_residual_histogram_figure,
    write_scaling_scatter_figure,
)
from urban_sami.modeling import compute_deviation_score


SOURCE_PACK = "denue-y-city-native-experiments-2026-04-21"
OUTPUT_PACK = "city-y-sami-comparison-pack-2026-04-21"
DIAGNOSTIC_STABLE_THRESHOLD = 500
TOP_CITY_COUNT = 20
PROFILE_FAMILIES = {"total", "scian2", "per_ocu", "size_class"}
HEATMAP_PAGE_COLS = 40
HEATMAP_CELL_W = 14
HEATMAP_CELL_H = 2
FIG_BG = (250, 247, 241)
FIG_PANEL = (255, 252, 247)
FIG_GRID = (224, 216, 203)
FIG_AXIS = (126, 118, 106)
FIG_TEXT = (44, 40, 34)
FIG_MUTED = (102, 96, 87)
FIG_POS = (177, 69, 56)
FIG_NEG = (47, 103, 154)
FIG_MISSING = (232, 227, 218)
FIG_POINT = (43, 121, 148)


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


def _pct(value: float, digits: int = 2) -> str:
    return f"{value * 100:.{digits}f}%"


def _fmt_int(value: float) -> str:
    return f"{int(round(value)):,}"


def _safe_slug(text: str) -> str:
    return (
        str(text)
        .strip()
        .lower()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ñ", "n")
        .replace(" ", "_")
        .replace("/", "_")
        .replace("-", "_")
        .replace(".", "")
        .replace(",", "")
        .replace("(", "")
        .replace(")", "")
    )


def _write_svg(path: Path, body: str, width: int, height: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _font(size: int, *, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/dejavu/DejaVuSans.ttf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size=size)
        except Exception:
            continue
    return ImageFont.load_default()


def _write_png(path: Path, image: Image.Image) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    image.save(path, format="PNG", optimize=True)
    return path


def _draw_rotated_text(image: Image.Image, *, x: int, y: int, text: str, font: ImageFont.ImageFont, fill: tuple[int, int, int], angle: int = 90) -> None:
    tmp = Image.new("RGBA", (1, 1), (255, 255, 255, 0))
    probe = ImageDraw.Draw(tmp)
    bbox = probe.textbbox((0, 0), text, font=font)
    w = max(1, bbox[2] - bbox[0] + 4)
    h = max(1, bbox[3] - bbox[1] + 4)
    label = Image.new("RGBA", (w, h), (255, 255, 255, 0))
    draw = ImageDraw.Draw(label)
    draw.text((2, 2), text, font=font, fill=fill)
    rotated = label.rotate(angle, expand=True)
    image.alpha_composite(rotated, (x, y))


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    idx = (len(ordered) - 1) * p
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return ordered[lo]
    frac = idx - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def _robust_abs_max(values: list[float]) -> float:
    if not values:
        return 1.0
    vmax = _percentile([abs(v) for v in values], 0.95)
    return max(vmax, 1e-6)


def _diverging_color(value: float | None, vmax: float) -> tuple[int, int, int]:
    if value is None:
        return FIG_MISSING
    t = max(-1.0, min(1.0, value / max(vmax, 1e-9)))
    if t >= 0.0:
        base = FIG_PANEL
        target = FIG_POS
        mix = abs(t)
    else:
        base = FIG_PANEL
        target = FIG_NEG
        mix = abs(t)
    return tuple(int(base[i] + ((target[i] - base[i]) * mix)) for i in range(3))


def _ranked_percentile(rank: int, total: int) -> float:
    if total <= 1:
        return 1.0
    return float(total - rank) / float(total - 1)


def _family_label(row: dict[str, str]) -> str:
    return str(row.get("family_label") or row.get("family") or "").strip()


def _load_official_city_metadata(root: Path) -> dict[str, dict]:
    path = root / "dist" / "independent_city_baseline" / "city_counts.csv"
    if not path.exists():
        return {}
    rows = _read_csv(path)
    out: dict[str, dict] = {}
    for row in rows:
        out[row["city_code"]] = {
            "city_code": row["city_code"],
            "city_name": row["city_name"],
            "state_code": row["state_code"],
            "population": row["population"],
            "households": row["households"],
            "total_establishments": row["est_count"],
        }
    return out


def _build_city_summary(
    city_meta_rows: list[dict],
    city_score_rows: list[dict],
) -> list[dict]:
    by_city: dict[str, list[dict]] = defaultdict(list)
    for row in city_score_rows:
        by_city[row["city_code"]].append(row)

    total_scores = {row["city_code"]: row for row in city_score_rows if row["y_key"] == "all"}
    summary_rows: list[dict] = []
    for meta in city_meta_rows:
        city_code = meta["city_code"]
        rows = by_city.get(city_code, [])
        stable_rows = [row for row in rows if int(row["n_obs"]) >= DIAGNOSTIC_STABLE_THRESHOLD]
        total_row = total_scores.get(city_code, {})
        summary_rows.append(
            {
                "city_code": city_code,
                "city_name": meta["city_name"],
                "state_code": meta["state_code"],
                "population": meta["population"],
                "households": meta["households"],
                "total_establishments": meta["total_establishments"],
                "total_sami": total_row.get("sami", ""),
                "total_sami_rank_desc": total_row.get("sami_rank_desc", ""),
                "total_sami_percentile": total_row.get("sami_percentile", ""),
                "positive_y_count": len(rows),
                "stable_y_count": len(stable_rows),
                "stable_top_decile_count": sum(1 for row in stable_rows if _to_float(row["sami_percentile"]) >= 0.9),
                "stable_bottom_decile_count": sum(1 for row in stable_rows if _to_float(row["sami_percentile"]) <= 0.1),
                "stable_mean_z_residual": mean([_to_float(row["z_residual"]) for row in stable_rows]) if stable_rows else "",
                "stable_median_z_residual": median([_to_float(row["z_residual"]) for row in stable_rows]) if stable_rows else "",
            }
        )

    total_ranked = [row for row in summary_rows if str(row["total_sami"]) != ""]
    total_ranked.sort(key=lambda row: _to_float(row["total_sami"]), reverse=True)
    for idx, row in enumerate(total_ranked, start=1):
        row["total_sami_rank_desc"] = idx
        row["total_sami_percentile"] = _ranked_percentile(idx, len(total_ranked))

    summary_rows.sort(key=lambda row: _to_float(row["population"]), reverse=True)
    return summary_rows


def _write_total_figures(
    output_dir: Path,
    city_meta_rows: list[dict],
    total_score_rows: list[dict],
    total_fit: dict[str, str] | None,
) -> list[dict]:
    figures_dir = output_dir / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)
    manifest: list[dict] = []
    rows = [row for row in city_meta_rows if str(row["total_sami"]) != ""]
    if not rows:
        return manifest

    ranked = sorted(rows, key=lambda row: _to_float(row["total_sami"]), reverse=True)
    font_title = _font(28, bold=True)
    font_sub = _font(14)
    font_axis = _font(12)
    font_small = _font(11)

    # Rank plot
    width = 1500
    height = 900
    left = 90
    right = 60
    top = 90
    bottom = 90
    plot_w = width - left - right
    plot_h = height - top - bottom
    image = Image.new("RGB", (width, height), FIG_BG)
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((20, 20, width - 20, height - 20), radius=18, fill=FIG_PANEL, outline=FIG_GRID, width=1)
    draw.text((44, 38), "City total-SAMI ranking", font=font_title, fill=FIG_TEXT)
    draw.text((44, 70), "All cities with positive total establishments. X = city rank by total SAMI, Y = raw log deviation.", font=font_sub, fill=FIG_MUTED)

    ys = [_to_float(row["total_sami"]) for row in ranked]
    ymax = max(abs(min(ys)), abs(max(ys)), 1e-6)
    zero_y = top + int((ymax / (2.0 * ymax)) * plot_h)
    draw.line((left, top + plot_h, left + plot_w, top + plot_h), fill=FIG_AXIS, width=1)
    draw.line((left, top, left, top + plot_h), fill=FIG_AXIS, width=1)
    draw.line((left, zero_y, left + plot_w, zero_y), fill=FIG_GRID, width=1)

    for tick in range(6):
        rank_val = 1 + int((len(ranked) - 1) * tick / 5.0)
        x = left + int((rank_val - 1) / max(len(ranked) - 1, 1) * plot_w)
        draw.line((x, top + plot_h, x, top + plot_h + 6), fill=FIG_AXIS, width=1)
        draw.text((x - 10, top + plot_h + 12), f"{rank_val}", font=font_axis, fill=FIG_MUTED)
    for tick in range(7):
        val = -ymax + ((2.0 * ymax) * tick / 6.0)
        y = top + int((1.0 - ((val + ymax) / (2.0 * ymax))) * plot_h)
        draw.line((left - 6, y, left, y), fill=FIG_AXIS, width=1)
        draw.text((16, y - 7), f"{val:.2f}", font=font_axis, fill=FIG_MUTED)

    points: list[tuple[int, int]] = []
    for idx, row in enumerate(ranked):
        x = left + int(idx / max(len(ranked) - 1, 1) * plot_w)
        yv = _to_float(row["total_sami"])
        y = top + int((1.0 - ((yv + ymax) / (2.0 * ymax))) * plot_h)
        points.append((x, y))
    if len(points) > 1:
        draw.line(points, fill=FIG_POINT, width=2)

    for row in ranked[:5] + ranked[-5:]:
        idx = ranked.index(row)
        x = left + int(idx / max(len(ranked) - 1, 1) * plot_w)
        yv = _to_float(row["total_sami"])
        y = top + int((1.0 - ((yv + ymax) / (2.0 * ymax))) * plot_h)
        draw.ellipse((x - 4, y - 4, x + 4, y + 4), fill=FIG_POS if yv >= 0 else FIG_NEG)
        label = row["city_name"]
        draw.text((x + 8, y - 8), label, font=font_small, fill=FIG_TEXT)
    rank_path = _write_png(figures_dir / "total_sami_rank_all.png", image)
    manifest.append({"figure_type": "total_sami_rank_all", "path": str(rank_path.resolve())})

    # Population scatter
    width = 1200
    height = 900
    left = 90
    right = 60
    top = 90
    bottom = 90
    plot_w = width - left - right
    plot_h = height - top - bottom
    image = Image.new("RGB", (width, height), FIG_BG)
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((20, 20, width - 20, height - 20), radius=18, fill=FIG_PANEL, outline=FIG_GRID, width=1)
    draw.text((44, 38), "Total SAMI versus city population", font=font_title, fill=FIG_TEXT)
    draw.text((44, 70), "This should not show a strong size trend if the cross-city scaling baseline is working properly.", font=font_sub, fill=FIG_MUTED)
    draw.line((left, top + plot_h, left + plot_w, top + plot_h), fill=FIG_AXIS, width=1)
    draw.line((left, top, left, top + plot_h), fill=FIG_AXIS, width=1)
    draw.line((left, top + plot_h // 2, left + plot_w, top + plot_h // 2), fill=FIG_GRID, width=1)
    xs = [math.log10(max(1.0, _to_float(row["population"]))) for row in rows]
    ys = [_to_float(row["total_sami"]) for row in rows]
    x_min, x_max = min(xs), max(xs)
    y_max = max(abs(min(ys)), abs(max(ys)), 1e-6)

    def px(value: float) -> int:
        return left + int(((value - x_min) / max(x_max - x_min, 1e-9)) * plot_w)

    def py(value: float) -> int:
        return top + int((1.0 - ((value + y_max) / (2.0 * y_max))) * plot_h)

    for tick in range(6):
        xv = x_min + ((x_max - x_min) * tick / 5.0)
        x = px(xv)
        draw.line((x, top + plot_h, x, top + plot_h + 6), fill=FIG_AXIS, width=1)
        draw.text((x - 16, top + plot_h + 12), f"{xv:.2f}", font=font_axis, fill=FIG_MUTED)
    for tick in range(7):
        yv = -y_max + ((2.0 * y_max) * tick / 6.0)
        y = py(yv)
        draw.line((left - 6, y, left, y), fill=FIG_AXIS, width=1)
        draw.text((20, y - 7), f"{yv:.2f}", font=font_axis, fill=FIG_MUTED)
    for row in rows:
        x = px(math.log10(max(1.0, _to_float(row["population"]))))
        y = py(_to_float(row["total_sami"]))
        draw.ellipse((x - 2, y - 2, x + 2, y + 2), fill=FIG_POINT)
    scatter_path = _write_png(figures_dir / "total_sami_vs_population.png", image)
    manifest.append({"figure_type": "total_sami_vs_population", "path": str(scatter_path.resolve())})

    if total_fit is not None and total_score_rows:
        scaling_path = write_scaling_scatter_figure(
            total_score_rows,
            figures_dir / "total_scaling_scatter.svg",
            title="Total establishments across Mexican cities",
            x_key="population",
            y_key="y_observed",
            fit_alpha=_to_float(total_fit["alpha"]),
            fit_beta=_to_float(total_fit["beta"]),
            annotation=f"β={_to_float(total_fit['beta']):.3f}; R²={_to_float(total_fit['r2']):.3f}; n={int(round(_to_float(total_fit['n_obs'])))}",
        )
        manifest.append({"figure_type": "total_scaling_scatter", "path": str(scaling_path.resolve())})

        residual_path = write_residual_histogram_figure(
            [_to_float(row["sami"]) for row in total_score_rows],
            figures_dir / "total_sami_histogram.svg",
            title="Total-establishment SAMI distribution across cities",
            subtitle="Distribution of raw log deviations for the total-establishment city law",
        )
        manifest.append({"figure_type": "total_sami_histogram", "path": str(residual_path.resolve())})
    return manifest


def _write_city_rank_curve_svg(
    path: Path,
    *,
    title: str,
    subtitle: str,
    rows: list[dict],
    value_field: str = "sami",
    label_field: str = "city_name",
) -> Path:
    ranked = [row for row in rows if str(row.get(value_field, "")) != ""]
    ranked.sort(key=lambda row: _to_float(row[value_field]), reverse=True)
    width = 1200
    height = 760
    left = 110
    right = 54
    top = 96
    bottom = 92
    plot_w = width - left - right
    plot_h = height - top - bottom

    if not ranked:
        return _write_svg(
            path,
            "".join(
                [
                    f'<rect width="{width}" height="{height}" fill="{BG}"/>',
                    f'<text x="40" y="50" font-size="24" font-family="{SERIF}" fill="{TEXT}">{html.escape(title)}</text>',
                ]
            ),
            width,
            height,
        )

    ys = [_to_float(row[value_field]) for row in ranked]
    ymax = max(abs(min(ys)), abs(max(ys)), 1e-6)

    def px(rank_idx: int) -> float:
        return left + (rank_idx / max(len(ranked) - 1, 1)) * plot_w

    def py(value: float) -> float:
        return top + plot_h - (((value + ymax) / (2.0 * ymax)) * plot_h)

    zero_y = py(0.0)
    parts = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{html.escape(title)}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">{html.escape(subtitle)}</text>',
        f'<line x1="{left}" y1="{top+plot_h}" x2="{left+plot_w}" y2="{top+plot_h}" stroke="{AXIS}"/>',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top+plot_h}" stroke="{AXIS}"/>',
        f'<line x1="{left}" y1="{zero_y:.2f}" x2="{left+plot_w}" y2="{zero_y:.2f}" stroke="{GRID}"/>',
    ]

    for tick in range(6):
        rank_val = 1 + int((len(ranked) - 1) * tick / 5.0)
        x = px(rank_val - 1)
        parts.append(f'<line x1="{x:.2f}" y1="{top+plot_h}" x2="{x:.2f}" y2="{top+plot_h+6}" stroke="{AXIS}"/>')
        parts.append(
            f'<text x="{x:.2f}" y="{top+plot_h+24}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">{rank_val}</text>'
        )
    for tick in range(7):
        val = -ymax + ((2.0 * ymax) * tick / 6.0)
        y = py(val)
        parts.append(f'<line x1="{left-6}" y1="{y:.2f}" x2="{left}" y2="{y:.2f}" stroke="{AXIS}"/>')
        parts.append(
            f'<text x="{left-14}" y="{y+4:.2f}" text-anchor="end" font-size="11" font-family="{SANS}" fill="{MUTED}">{val:.2f}</text>'
        )

    line_points = [f"{px(idx):.2f},{py(_to_float(row[value_field])):.2f}" for idx, row in enumerate(ranked)]
    parts.append(f'<polyline fill="none" stroke="{BLUE}" stroke-width="2" points="{" ".join(line_points)}"/>')

    label_rows = ranked[:4] + ranked[-4:]
    used = set()
    for row in label_rows:
        row_id = (row.get("city_code", ""), row.get(value_field, ""))
        if row_id in used:
            continue
        used.add(row_id)
        idx = ranked.index(row)
        x = px(idx)
        y = py(_to_float(row[value_field]))
        fill = RUST if _to_float(row[value_field]) >= 0 else BLUE
        anchor = "start" if idx < len(ranked) / 2 else "end"
        dx = 8 if anchor == "start" else -8
        parts.append(f'<circle cx="{x:.2f}" cy="{y:.2f}" r="4.5" fill="{fill}"/>')
        parts.append(
            f'<text x="{x+dx:.2f}" y="{y-8:.2f}" text-anchor="{anchor}" font-size="11" font-family="{SANS}" fill="{TEXT}">{html.escape(str(row.get(label_field, "")))}</text>'
        )

    parts.append(
        f'<text x="{left + plot_w/2:.2f}" y="{height-18}" text-anchor="middle" font-size="13" font-family="{SANS}" fill="{MUTED}">city rank by SAMI</text>'
    )
    parts.append(
        f'<text x="24" y="{top + plot_h/2:.2f}" text-anchor="middle" font-size="13" font-family="{SANS}" fill="{MUTED}" transform="rotate(-90 24 {top + plot_h/2:.2f})">SAMI</text>'
    )
    return _write_svg(path, "".join(parts), width, height)


def _write_family_metric_figures(family_dir: Path, *, family: str, family_label: str, rows: list[dict]) -> list[dict]:
    manifest: list[dict] = []
    if not rows:
        return manifest

    share_path = family_dir / "share_rank_all.svg"
    write_ranked_metric_chart(
        share_path,
        title=f"{family_label}: city-level category weight",
        subtitle="All categories included. Ordered by share of total establishments.",
        rows=rows,
        metric_field="share_of_total",
        order_field="share_of_total",
        value_formatter=lambda value: _pct(value),
    )
    manifest.append({"family": family, "family_label": family_label, "figure_type": "share_rank_all", "path": str(share_path.resolve())})

    nobs_path = family_dir / "n_obs_rank_all.svg"
    write_ranked_metric_chart(
        nobs_path,
        title=f"{family_label}: city coverage by category",
        subtitle="All categories included. Ordered by number of cities entering the fit.",
        rows=rows,
        metric_field="n_obs",
        order_field="n_obs",
        value_formatter=lambda value: _fmt_int(value),
    )
    manifest.append({"family": family, "family_label": family_label, "figure_type": "n_obs_rank_all", "path": str(nobs_path.resolve())})

    beta_path = family_dir / "beta_rank_all.svg"
    write_ranked_metric_chart(
        beta_path,
        title=f"{family_label}: city-level OLS beta",
        subtitle="All categories included. Ordered by beta. Read relative to the β = 1 reference line.",
        rows=rows,
        metric_field="beta",
        order_field="beta",
        ref_line=1.0,
        value_formatter=lambda value: f"{value:.3f}",
    )
    manifest.append({"family": family, "family_label": family_label, "figure_type": "beta_rank_all", "path": str(beta_path.resolve())})

    r2_path = family_dir / "r2_rank_all.svg"
    write_ranked_metric_chart(
        r2_path,
        title=f"{family_label}: city-level OLS R²",
        subtitle="All categories included. Beta is not mixed into this figure.",
        rows=rows,
        metric_field="r2",
        order_field="r2",
        color=BLUE,
        fixed_range=(0.0, 1.0),
        value_formatter=lambda value: f"{value:.3f}",
    )
    manifest.append({"family": family, "family_label": family_label, "figure_type": "r2_rank_all", "path": str(r2_path.resolve())})
    return manifest


def _write_category_dossiers(
    family_dir: Path,
    *,
    family: str,
    family_label: str,
    category_rows_sorted: list[dict[str, str]],
    family_scores: list[dict[str, str]],
) -> tuple[list[dict], list[dict]]:
    manifest: list[dict] = []
    index_rows: list[dict] = []
    if family not in PROFILE_FAMILIES:
        return manifest, index_rows

    rows_by_y: dict[str, list[dict]] = defaultdict(list)
    for row in family_scores:
        rows_by_y[row["y_key"]].append(row)

    category_root = family_dir / "categories"
    category_root.mkdir(parents=True, exist_ok=True)

    for meta in category_rows_sorted:
        y_key = meta["y_key"]
        category_rows = rows_by_y.get(y_key, [])
        if not category_rows:
            continue
        ranked = sorted(category_rows, key=lambda row: _to_float(row["sami"]), reverse=True)
        out_dir = category_root / _safe_slug(y_key)
        out_dir.mkdir(parents=True, exist_ok=True)

        top_rows = ranked[:TOP_CITY_COUNT]
        bottom_rows = list(reversed(ranked[-TOP_CITY_COUNT:])) if ranked else []
        top_path = _write_csv(out_dir / "top20_sami_cities.csv", top_rows, list(top_rows[0].keys()) if top_rows else [])
        bottom_path = _write_csv(out_dir / "bottom20_sami_cities.csv", bottom_rows, list(bottom_rows[0].keys()) if bottom_rows else [])

        rank_path = _write_city_rank_curve_svg(
            out_dir / "city_sami_rank.svg",
            title=f"{family_label}: {meta['category_label']}",
            subtitle="All cities ranked by SAMI for this category.",
            rows=category_rows,
            value_field="sami",
            label_field="city_name",
        )
        scatter_path = write_scaling_scatter_figure(
            category_rows,
            out_dir / "scaling_scatter.svg",
            title=f"{family_label}: {meta['category_label']}",
            x_key="population",
            y_key="y_observed",
            fit_alpha=_to_float(meta["alpha"]),
            fit_beta=_to_float(meta["beta"]),
            annotation=f"β={_to_float(meta['beta']):.3f}; R²={_to_float(meta['r2']):.3f}; n={int(round(_to_float(meta['n_obs'])))}",
        )

        manifest.extend(
            [
                {"family": family, "family_label": family_label, "figure_type": "city_sami_rank", "path": str(rank_path.resolve())},
                {"family": family, "family_label": family_label, "figure_type": "scaling_scatter", "path": str(scatter_path.resolve())},
            ]
        )
        index_rows.append(
            {
                "family": family,
                "family_label": family_label,
                "category": meta["category"],
                "category_label": meta["category_label"],
                "y_key": y_key,
                "folder": str(out_dir.resolve()),
                "city_sami_rank_svg": str(rank_path.resolve()),
                "scaling_scatter_svg": str(scatter_path.resolve()),
                "top20_csv": str(top_path.resolve()),
                "bottom20_csv": str(bottom_path.resolve()),
            }
        )

    return manifest, index_rows


def _write_family_heatmaps(
    family_dir: Path,
    *,
    family: str,
    family_label: str,
    category_rows_sorted: list[dict[str, str]],
    family_scores: list[dict[str, str]],
    city_meta_rows: list[dict[str, str]],
) -> list[dict]:
    manifest: list[dict] = []
    score_lookup: dict[str, dict[str, dict[str, str]]] = defaultdict(dict)
    for row in family_scores:
        score_lookup[row["y_key"]][row["city_code"]] = row

    city_order_rows = sorted(city_meta_rows, key=lambda row: _to_float(row["population"]), reverse=True)
    city_codes = [row["city_code"] for row in city_order_rows]
    y_order = [row["y_key"] for row in category_rows_sorted]

    font_title = _font(24, bold=True)
    font_sub = _font(13)
    font_axis = _font(11)
    values = [
        _to_float(score_lookup[y_key][city_code]["sami"])
        for y_key in y_order
        for city_code in city_codes
        if city_code in score_lookup.get(y_key, {})
    ]
    vmax = _robust_abs_max(values)
    pages = max(1, math.ceil(len(y_order) / HEATMAP_PAGE_COLS))

    for page_idx in range(pages):
        start = page_idx * HEATMAP_PAGE_COLS
        end = min(len(y_order), start + HEATMAP_PAGE_COLS)
        page_keys = y_order[start:end]
        page_labels = [next(row["category_label"] for row in category_rows_sorted if row["y_key"] == key) for key in page_keys]
        left = 220
        right = 30
        top = 180
        bottom = 50
        width = left + (len(page_keys) * HEATMAP_CELL_W) + right
        height = top + (len(city_codes) * HEATMAP_CELL_H) + bottom
        image = Image.new("RGBA", (width, height), FIG_BG + (255,))
        draw = ImageDraw.Draw(image)
        draw.rounded_rectangle((10, 10, width - 10, height - 10), radius=16, fill=FIG_PANEL, outline=FIG_GRID, width=1)
        draw.text((24, 26), f"{family_label}: city x category SAMI heatmap", font=font_title, fill=FIG_TEXT)
        subtitle = f"All cities included. Rows ordered by population. Columns {start+1}-{end} of {len(y_order)}, ordered by national category share."
        draw.text((24, 58), subtitle, font=font_sub, fill=FIG_MUTED)
        draw.text((24, 78), "Color = SAMI. Red = above expected for size, blue = below expected, gray = zero/not defined.", font=font_sub, fill=FIG_MUTED)

        for idx, label in enumerate(page_labels):
            x = left + (idx * HEATMAP_CELL_W)
            _draw_rotated_text(image, x=x + 2, y=96, text=label, font=font_axis, fill=FIG_TEXT)

        row_tick_every = 150
        for ridx, city in enumerate(city_order_rows):
            y = top + (ridx * HEATMAP_CELL_H)
            if ridx % row_tick_every == 0 or ridx == len(city_order_rows) - 1:
                draw.line((left - 4, y, left, y), fill=FIG_AXIS, width=1)
                draw.text((20, max(12, y - 6)), city["city_name"], font=font_axis, fill=FIG_MUTED)
            for cidx, y_key in enumerate(page_keys):
                score_row = score_lookup.get(y_key, {}).get(city["city_code"])
                value = _to_float(score_row["sami"]) if score_row is not None else None
                color = _diverging_color(value, vmax)
                x0 = left + (cidx * HEATMAP_CELL_W)
                y0 = y
                draw.rectangle((x0, y0, x0 + HEATMAP_CELL_W - 1, y0 + HEATMAP_CELL_H - 1), fill=color)

        legend_x = 24
        legend_y = height - 34
        legend_w = 160
        for i in range(legend_w):
            t = -1.0 + (2.0 * i / max(legend_w - 1, 1))
            draw.line((legend_x + i, legend_y, legend_x + i, legend_y + 12), fill=_diverging_color(t * vmax, vmax), width=1)
        draw.rectangle((legend_x, legend_y, legend_x + legend_w, legend_y + 12), outline=FIG_GRID, width=1)
        draw.text((legend_x, legend_y - 16), f"-{vmax:.2f}", font=font_axis, fill=FIG_MUTED)
        draw.text((legend_x + legend_w - 26, legend_y - 16), f"+{vmax:.2f}", font=font_axis, fill=FIG_MUTED)
        draw.text((legend_x + 58, legend_y + 16), "SAMI", font=font_axis, fill=FIG_MUTED)

        out_path = _write_png(family_dir / f"sami_heatmap_p{page_idx+1:03d}.png", image.convert("RGB"))
        manifest.append(
            {
                "family": family,
                "family_label": family_label,
                "figure_type": "sami_heatmap_page",
                "page": page_idx + 1,
                "page_from_col": start + 1,
                "page_to_col": end,
                "path": str(out_path.resolve()),
            }
        )
    return manifest


def _write_family_outputs(
    output_dir: Path,
    city_meta_rows: list[dict],
    category_index_rows: list[dict],
    city_score_rows: list[dict],
    city_unit_rows: list[dict],
) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    family_index_rows: list[dict] = []
    figureless_manifest_rows: list[dict] = []
    figure_manifest_rows: list[dict] = []
    category_dossier_rows: list[dict] = []

    category_rows_by_family: dict[str, list[dict]] = defaultdict(list)
    for row in category_index_rows:
        category_rows_by_family[row["family"]].append(row)

    scores_by_family: dict[str, list[dict]] = defaultdict(list)
    for row in city_score_rows:
        scores_by_family[row["family"]].append(row)

    observed_by_family_y_city: dict[str, dict[str, dict[str, str]]] = defaultdict(lambda: defaultdict(dict))
    for row in city_unit_rows:
        observed_by_family_y_city[row["family"]][row["y_key"]][row["city_code"]] = row["y_value"]

    city_order = [row["city_code"] for row in city_meta_rows]
    city_meta_map = {row["city_code"]: row for row in city_meta_rows}

    for family, category_rows in sorted(category_rows_by_family.items()):
        family_dir = output_dir / "families" / family
        family_dir.mkdir(parents=True, exist_ok=True)

        category_rows_sorted = sorted(category_rows, key=lambda row: _to_float(row["share_of_total"]), reverse=True)
        _write_csv(family_dir / "category_index.csv", category_rows_sorted, list(category_rows_sorted[0].keys()) if category_rows_sorted else [])

        family_scores = sorted(
            scores_by_family.get(family, []),
            key=lambda row: (row["y_key"], -_to_float(row["sami"]), row["city_code"]),
        )
        _write_csv(family_dir / "complete_city_scores_long.csv", family_scores, list(family_scores[0].keys()) if family_scores else [])
        family_label = _family_label(category_rows_sorted[0]) if category_rows_sorted else family
        figure_manifest_rows.extend(
            _write_family_metric_figures(
                family_dir,
                family=family,
                family_label=family_label,
                rows=category_rows_sorted,
            )
        )
        dossier_manifest, dossier_index_rows = _write_category_dossiers(
            family_dir,
            family=family,
            family_label=family_label,
            category_rows_sorted=category_rows_sorted,
            family_scores=family_scores,
        )
        figure_manifest_rows.extend(dossier_manifest)
        category_dossier_rows.extend(dossier_index_rows)

        y_order = [row["y_key"] for row in category_rows_sorted]
        y_to_label = {row["y_key"]: row["category_label"] for row in category_rows_sorted}
        y_to_n_obs = {row["y_key"]: row["n_obs"] for row in category_rows_sorted}

        score_lookup: dict[str, dict[str, dict[str, str]]] = defaultdict(dict)
        for row in family_scores:
            score_lookup[row["y_key"]][row["city_code"]] = row

        matrix_specs = [
            ("city_by_category_observed.csv", "y_observed"),
            ("city_by_category_sami.csv", "sami"),
            ("city_by_category_z_residual.csv", "z_residual"),
            ("city_by_category_sami_rank.csv", "sami_rank_desc"),
        ]
        for filename, value_field in matrix_specs:
            fieldnames = [
                "city_code",
                "city_name",
                "state_code",
                "population",
                "households",
                "total_establishments",
                "total_sami",
                "total_sami_rank_desc",
            ] + y_order
            matrix_rows: list[dict] = []
            for city_code in city_order:
                meta = city_meta_map[city_code]
                row = {
                    "city_code": city_code,
                    "city_name": meta["city_name"],
                    "state_code": meta["state_code"],
                    "population": meta["population"],
                    "households": meta["households"],
                    "total_establishments": meta["total_establishments"],
                    "total_sami": meta["total_sami"],
                    "total_sami_rank_desc": meta["total_sami_rank_desc"],
                }
                for y_key in y_order:
                    if value_field == "y_observed":
                        row[y_key] = observed_by_family_y_city[family].get(y_key, {}).get(city_code, "0")
                    else:
                        score_row = score_lookup.get(y_key, {}).get(city_code, {})
                        row[y_key] = score_row.get(value_field, "")
                matrix_rows.append(row)
            _write_csv(family_dir / filename, matrix_rows, fieldnames)

        family_index_rows.append(
            {
                "family": family,
                "family_label": family_label,
                "category_count": len(category_rows_sorted),
                "folder": str(family_dir.resolve()),
                "category_index_csv": str((family_dir / "category_index.csv").resolve()),
                "complete_city_scores_long_csv": str((family_dir / "complete_city_scores_long.csv").resolve()),
                "observed_matrix_csv": str((family_dir / "city_by_category_observed.csv").resolve()),
                "sami_matrix_csv": str((family_dir / "city_by_category_sami.csv").resolve()),
                "z_residual_matrix_csv": str((family_dir / "city_by_category_z_residual.csv").resolve()),
                "rank_matrix_csv": str((family_dir / "city_by_category_sami_rank.csv").resolve()),
            }
        )
        figureless_manifest_rows.append(
            {
                "family": family,
                "family_label": family_label,
                "categories": len(category_rows_sorted),
                "top_category_label": y_to_label.get(y_order[0], "") if y_order else "",
                "top_category_n_obs": y_to_n_obs.get(y_order[0], "") if y_order else "",
            }
        )

    return family_index_rows, figureless_manifest_rows, figure_manifest_rows, category_dossier_rows


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    source_dir = root / "reports" / SOURCE_PACK
    output_dir = root / "reports" / OUTPUT_PACK
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    fit_rows = _read_csv(source_dir / "city_y_ols_fits.csv")
    unit_rows = _read_csv(source_dir / "city_y_unit_counts.csv")
    fit_by_y = {row["y_key"]: row for row in fit_rows}

    city_meta_map: dict[str, dict] = _load_official_city_metadata(root)
    for row in unit_rows:
        if row["y_key"] != "all":
            continue
        city_meta_map[row["city_code"]] = {
            **city_meta_map.get(row["city_code"], {}),
            "city_code": row["city_code"],
            "city_name": row["city_name"],
            "state_code": row["state_code"],
            "population": row["population"],
            "households": row["households"],
            "total_establishments": row["y_value"],
        }

    city_score_rows: list[dict] = []
    rows_by_y: dict[str, list[dict]] = defaultdict(list)
    for row in unit_rows:
        y_key = row["y_key"]
        fit = fit_by_y.get(y_key)
        if fit is None:
            continue
        population = _to_float(row["population"])
        y_observed = _to_float(row["y_value"])
        if population <= 0.0 or y_observed <= 0.0:
            continue
        score = compute_deviation_score(
            y_observed,
            population,
            _to_float(fit["alpha"]),
            _to_float(fit["beta"]),
            _to_float(fit["resid_std"]),
        )
        total_establishments = _to_float(city_meta_map.get(row["city_code"], {}).get("total_establishments"))
        out = {
            "city_code": row["city_code"],
            "city_name": row["city_name"],
            "state_code": row["state_code"],
            "population": row["population"],
            "households": row["households"],
            "total_establishments": city_meta_map.get(row["city_code"], {}).get("total_establishments", ""),
            "family": row["family"],
            "family_label": row["family_label"],
            "category": row["category"],
            "category_label": row["category_label"],
            "y_key": y_key,
            "y_observed": y_observed,
            "y_expected": score.y_expected,
            "sami": score.sami,
            "z_residual": score.z_residual,
            "share_of_city_total": (y_observed / total_establishments) if total_establishments > 0.0 else "",
            "alpha": fit["alpha"],
            "beta": fit["beta"],
            "r2": fit["r2"],
            "resid_std": fit["resid_std"],
            "n_obs": fit["n_obs"],
            "coverage_tier": fit["coverage_tier"],
            "share_of_national_total": fit["share_of_total"],
        }
        city_score_rows.append(out)
        rows_by_y[y_key].append(out)

    for y_key, rows in rows_by_y.items():
        ranked = sorted(rows, key=lambda row: _to_float(row["sami"]), reverse=True)
        total = len(ranked)
        for idx, row in enumerate(ranked, start=1):
            row["sami_rank_desc"] = idx
            row["sami_rank_asc"] = total - idx + 1
            row["sami_percentile"] = _ranked_percentile(idx, total)

    city_score_rows.sort(key=lambda row: (row["family"], row["y_key"], int(row["sami_rank_desc"])))
    city_score_fieldnames = [
        "city_code",
        "city_name",
        "state_code",
        "population",
        "households",
        "total_establishments",
        "family",
        "family_label",
        "category",
        "category_label",
        "y_key",
        "y_observed",
        "y_expected",
        "sami",
        "z_residual",
        "share_of_city_total",
        "share_of_national_total",
        "sami_rank_desc",
        "sami_rank_asc",
        "sami_percentile",
        "alpha",
        "beta",
        "r2",
        "resid_std",
        "n_obs",
        "coverage_tier",
    ]
    _write_csv(output_dir / "city_y_sami_long.csv", city_score_rows, city_score_fieldnames)

    city_meta_rows = _build_city_summary(
        city_meta_rows=list(city_meta_map.values()),
        city_score_rows=city_score_rows,
    )
    city_meta_fieldnames = [
        "city_code",
        "city_name",
        "state_code",
        "population",
        "households",
        "total_establishments",
        "total_sami",
        "total_sami_rank_desc",
        "total_sami_percentile",
        "positive_y_count",
        "stable_y_count",
        "stable_top_decile_count",
        "stable_bottom_decile_count",
        "stable_mean_z_residual",
        "stable_median_z_residual",
    ]
    _write_csv(output_dir / "city_summary.csv", city_meta_rows, city_meta_fieldnames)
    total_score_rows = [row for row in city_score_rows if row["y_key"] == "all"]
    overall_figure_manifest = _write_total_figures(output_dir, city_meta_rows, total_score_rows, fit_by_y.get("all"))

    family_index_rows, family_manifest_rows, family_figure_manifest, category_dossier_rows = _write_family_outputs(
        output_dir,
        city_meta_rows=city_meta_rows,
        category_index_rows=fit_rows,
        city_score_rows=city_score_rows,
        city_unit_rows=unit_rows,
    )
    _write_csv(
        output_dir / "family_index.csv",
        family_index_rows,
        [
            "family",
            "family_label",
            "category_count",
            "folder",
            "category_index_csv",
            "complete_city_scores_long_csv",
            "observed_matrix_csv",
            "sami_matrix_csv",
            "z_residual_matrix_csv",
            "rank_matrix_csv",
        ],
    )
    _write_csv(
        output_dir / "category_dossier_index.csv",
        category_dossier_rows,
        [
            "family",
            "family_label",
            "category",
            "category_label",
            "y_key",
            "folder",
            "city_sami_rank_svg",
            "scaling_scatter_svg",
            "top20_csv",
            "bottom20_csv",
        ],
    )
    _write_csv(
        output_dir / "figures_manifest.csv",
        overall_figure_manifest + family_figure_manifest,
        ["family", "family_label", "figure_type", "page", "page_from_col", "page_to_col", "path"],
    )

    top_total = [row for row in city_meta_rows if str(row["total_sami"]) != ""]
    top_total.sort(key=lambda row: _to_float(row["total_sami"]), reverse=True)
    top_total_rows = top_total[:TOP_CITY_COUNT]
    bottom_total_rows = list(reversed(top_total[-TOP_CITY_COUNT:])) if top_total else []
    _write_csv(output_dir / "top_total_sami_cities.csv", top_total_rows, city_meta_fieldnames)
    _write_csv(output_dir / "bottom_total_sami_cities.csv", bottom_total_rows, city_meta_fieldnames)

    report_lines = [
        "# City x Y SAMI Comparison Pack",
        "",
        "Date: `2026-04-21`",
        "",
        "This pack is the missing city-comparison layer for the city runs.",
        "The previous city-native `Y` pack estimated one cross-city scaling law per `Y`. This pack holds those laws fixed and computes, for each city and each `Y`, SAMI:",
        "",
        "- `Y_expected = exp(alpha + beta * log(N))`",
        "- `SAMI = log(Y / Y_expected)`",
        "",
        "## How To Compare Cities Correctly",
        "",
        "- compare cities **within the same `Y`** using `SAMI`",
        "- do **not** compare cities by `beta`; `beta` belongs to the whole urban system for a given `Y`, not to an individual city",
        "- do **not** compare raw `SAMI` magnitudes across different `Y` as if they were the same scale; different `Y` have different residual widths",
        "- if you need cross-`Y` browsing inside one city, use `z_residual` only as a secondary diagnostic",
        "- zeros remain visible in the observed matrices, but raw `SAMI` is only defined where `Y > 0`",
        "",
        "## Main Files",
        "",
        f"- Full long table: [city_y_sami_long.csv]({(output_dir / 'city_y_sami_long.csv').resolve()})",
        f"- City summary: [city_summary.csv]({(output_dir / 'city_summary.csv').resolve()})",
        f"- Family index: [family_index.csv]({(output_dir / 'family_index.csv').resolve()})",
        f"- Category dossier index: [category_dossier_index.csv]({(output_dir / 'category_dossier_index.csv').resolve()})",
        f"- Figures manifest: [figures_manifest.csv]({(output_dir / 'figures_manifest.csv').resolve()})",
        f"- Top total-SAMI cities: [top_total_sami_cities.csv]({(output_dir / 'top_total_sami_cities.csv').resolve()})",
        f"- Bottom total-SAMI cities: [bottom_total_sami_cities.csv]({(output_dir / 'bottom_total_sami_cities.csv').resolve()})",
        "",
        "## Figures",
        "",
        f"- [total_scaling_scatter.svg]({(output_dir / 'figures' / 'total_scaling_scatter.svg').resolve()})",
        f"- [total_sami_rank_all.png]({(output_dir / 'figures' / 'total_sami_rank_all.png').resolve()})",
        f"- [total_sami_vs_population.png]({(output_dir / 'figures' / 'total_sami_vs_population.png').resolve()})",
        f"- [total_sami_histogram.svg]({(output_dir / 'figures' / 'total_sami_histogram.svg').resolve()})",
        "- Each family folder contains coefficient figures with `share`, `n_obs`, `β`, and `R²` separated.",
        "- Interpretable families (`total`, `SCIAN 2-digit`, `DENUE size band`, `derived size class`) also contain per-category city dossiers with a SAMI rank figure and a scaling scatter.",
        "",
        "## Family Matrices",
        "",
        "Each family folder contains:",
        "- `category_index.csv` with the category definitions and cross-city fit statistics",
        "- `complete_city_scores_long.csv` with all city-category positive observations and SAMIs",
        "- `city_by_category_observed.csv` with cities as rows and categories as columns",
        "- `city_by_category_sami.csv` with the canonical SAMI values",
        "- `city_by_category_z_residual.csv` with the standardized diagnostic",
        "- `city_by_category_sami_rank.csv` with city rank within each `Y`",
        "- `share_rank_all.svg`, `n_obs_rank_all.svg`, `beta_rank_all.svg`, and `r2_rank_all.svg` for the family-level behavior",
        "",
    ]
    for row in family_index_rows:
        report_lines.append(f"- [{row['family_label']}]({row['folder']}): `{row['category_count']}` categories")
    report_lines.extend(
        [
            "",
            "## Reading Order",
            "",
            "1. Start with `figures/total_scaling_scatter.svg` to see the theory-native cross-city law for total establishments.",
            "2. Use `figures/total_sami_rank_all.png` and `figures/total_sami_histogram.svg` to understand the city-level deviation structure for total establishments.",
            "3. Pick a family in `family_index.csv`, then open `share_rank_all.svg`, `beta_rank_all.svg`, `r2_rank_all.svg`, and `n_obs_rank_all.svg` in that order.",
            "4. For interpretable families, use `category_dossier_index.csv` to open a specific category dossier and compare cities within that `Y`.",
            "5. Use `city_by_category_sami.csv` or `city_by_category_sami_rank.csv` only for exact lookup after the figures.",
            "6. Use `city_summary.csv` only as a navigation layer, not as the theory object itself.",
            "",
            "## Current Scope",
            "",
            f"- city-summary rows: `{len(city_meta_rows):,}`",
            f"- positive city x Y SAMI rows: `{len(city_score_rows):,}`",
            f"- Y definitions with OLS fits: `{len(fit_rows):,}`",
            f"- category dossiers generated: `{len(category_dossier_rows):,}`",
            f"- stable diagnostic threshold: `n_obs >= {DIAGNOSTIC_STABLE_THRESHOLD}`",
        ]
    )
    (output_dir / "report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    report_json = {
        "workflow_id": "city_y_sami_comparison_pack",
        "source_pack": SOURCE_PACK,
        "output_dir": str(output_dir.resolve()),
        "city_count": len(city_meta_rows),
        "positive_city_y_rows": len(city_score_rows),
        "y_count": len(fit_rows),
        "family_count": len(family_index_rows),
        "category_dossier_count": len(category_dossier_rows),
        "stable_threshold": DIAGNOSTIC_STABLE_THRESHOLD,
        "figure_count": len(overall_figure_manifest) + len(family_figure_manifest),
        "top_total_sami_city": top_total_rows[0]["city_name"] if top_total_rows else "",
        "bottom_total_sami_city": bottom_total_rows[0]["city_name"] if bottom_total_rows else "",
    }
    (output_dir / "report.json").write_text(json.dumps(report_json, indent=2), encoding="utf-8")
    print(json.dumps(report_json, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

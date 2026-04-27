#!/usr/bin/env python3
from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median

from run_denue_y_state_scientific_analysis import BLUE, GOLD, RUST, TEAL, write_ranked_metric_chart


ROOT = Path("/home/hadox/cmd-center/platforms/research/urban-sami")
CURATED_DIR = ROOT / "reports" / "city-y-curated-results-pack-2026-04-22"
CITY_SAMI_DIR = ROOT / "reports" / "city-y-sami-comparison-pack-2026-04-21"
OUTPUT_DIR = ROOT / "reports" / "city-y-interpretive-pack-2026-04-22"
PRIMARY_FAMILIES = ["total", "per_ocu", "size_class", "scian2"]
SECONDARY_FAMILY = "scian3"
SCIAN3_KEEP = 12
EXTREME_COUNT = 10


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


def _pct(value: float, digits: int = 1) -> str:
    return f"{value * 100:.{digits}f}%"


def _quantile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    idx = (len(sorted_values) - 1) * q
    lo = int(idx)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = idx - lo
    return sorted_values[lo] * (1.0 - frac) + sorted_values[hi] * frac


def _write_svg(path: Path, body: str, width: int, height: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _scatter(rows: list[dict], *, x_key: str, y_key: str, path: Path, title: str, subtitle: str) -> Path:
    width = 1020
    height = 680
    left = 100
    right = 60
    top = 96
    bottom = 86
    plot_w = width - left - right
    plot_h = height - top - bottom
    xs = [_to_float(row[x_key]) for row in rows]
    ys = [_to_float(row[y_key]) for row in rows]
    x_min, x_max = min(xs), max(xs)
    y_min, y_max = min(ys), max(ys)
    x_pad = max((x_max - x_min) * 0.08, 0.02)
    y_pad = max((y_max - y_min) * 0.08, 0.02)
    x_min -= x_pad
    x_max += x_pad
    y_min -= y_pad
    y_max += y_pad

    def px(value: float) -> float:
        return left + ((value - x_min) / max(x_max - x_min, 1e-9)) * plot_w

    def py(value: float) -> float:
        return top + (1.0 - ((value - y_min) / max(y_max - y_min, 1e-9))) * plot_h

    family_colors = {
        "total": TEAL,
        "per_ocu": BLUE,
        "size_class": GOLD,
        "scian2": RUST,
        "scian3": "#4d7a57",
    }

    body = [
        f'<rect width="{width}" height="{height}" fill="#f8f6f1"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="#fffdf8" stroke="#ddd6c8"/>',
        f'<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">{subtitle}</text>',
    ]
    for frac in (0.0, 0.25, 0.5, 0.75, 1.0):
        x = left + (frac * plot_w)
        xv = x_min + ((x_max - x_min) * frac)
        body.append(f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top+plot_h}" stroke="#e4ddd1" stroke-width="1"/>')
        body.append(f'<text x="{x:.2f}" y="{height-34}" text-anchor="middle" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="#625d54">{xv:.2f}</text>')
    for frac in (0.0, 0.25, 0.5, 0.75, 1.0):
        y = top + ((1.0 - frac) * plot_h)
        yv = y_min + ((y_max - y_min) * frac)
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left+plot_w}" y2="{y:.2f}" stroke="#e4ddd1" stroke-width="1"/>')
        body.append(f'<text x="{left-12}" y="{y+4:.2f}" text-anchor="end" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="#625d54">{yv:.2f}</text>')
    body.append(f'<line x1="{left}" y1="{top+plot_h}" x2="{left+plot_w}" y2="{top+plot_h}" stroke="#8b8478" stroke-width="1.2"/>')
    body.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top+plot_h}" stroke="#8b8478" stroke-width="1.2"/>')
    body.append(f'<text x="{left + (plot_w/2):.2f}" y="{height-8}" text-anchor="middle" font-size="13" font-family="Helvetica, Arial, sans-serif" fill="#625d54">{x_key}</text>')
    body.append(f'<text x="28" y="{top + (plot_h/2):.2f}" text-anchor="middle" font-size="13" font-family="Helvetica, Arial, sans-serif" fill="#625d54" transform="rotate(-90 28 {top + (plot_h/2):.2f})">{y_key}</text>')

    labeled = sorted(rows, key=lambda row: _to_float(row[y_key]), reverse=True)[:4] + sorted(rows, key=lambda row: _to_float(row[y_key]))[:4]
    labeled_keys = {row["y_key"] for row in labeled}
    for row in rows:
        x = _to_float(row[x_key])
        y = _to_float(row[y_key])
        share = _to_float(row.get("share_of_total", 0.0))
        radius = 4.0 + (18.0 * (share ** 0.35))
        color = family_colors.get(row["family"], "#6b6b6b")
        body.append(
            f'<circle cx="{px(x):.2f}" cy="{py(y):.2f}" r="{radius:.2f}" fill="{color}" fill-opacity="0.76" stroke="#ffffff" stroke-width="0.8">'
            f'<title>{row["y_key"]} | {row["category_label"]} | {x_key}={x:.3f} | {y_key}={y:.3f}</title></circle>'
        )
        if row["y_key"] in labeled_keys:
            body.append(
                f'<text x="{px(x)+8:.2f}" y="{py(y)-8:.2f}" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="#1f1f1f">{row["category_label"]}</text>'
            )
    legend_x = width - 230
    legend_y = 104
    for idx, family in enumerate(["total", "per_ocu", "size_class", "scian2", "scian3"]):
        y = legend_y + idx * 22
        body.append(f'<circle cx="{legend_x}" cy="{y}" r="6" fill="{family_colors.get(family)}" fill-opacity="0.8"/>')
        body.append(f'<text x="{legend_x+14}" y="{y+4}" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="#1f1f1f">{family}</text>')
    return _write_svg(path, "".join(body), width, height)


def main() -> int:
    curated_rows = _read_csv(CURATED_DIR / "curated_y_catalog.csv")
    city_sami_rows = _read_csv(CITY_SAMI_DIR / "city_y_sami_long.csv")

    scian3_rows = [row for row in curated_rows if row["family"] == "scian3"]
    scian3_keep = sorted(scian3_rows, key=lambda row: int(row["r2_rank_desc"]))[:SCIAN3_KEEP]
    keep_keys = {
        row["y_key"]
        for row in curated_rows
        if row["family"] in PRIMARY_FAMILIES
    } | {row["y_key"] for row in scian3_keep}

    kept_rows = [row for row in curated_rows if row["y_key"] in keep_keys]
    city_rows = [row for row in city_sami_rows if row["y_key"] in keep_keys]
    city_by_y: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in city_rows:
        city_by_y[row["y_key"]].append(row)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    figures_dir = OUTPUT_DIR / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)

    y_summary_rows: list[dict] = []
    extreme_rows: list[dict] = []
    city_signal_counts: dict[str, dict] = defaultdict(lambda: {"positive_top_decile_count": 0, "negative_bottom_decile_count": 0, "selected_y_count": 0})

    for row in kept_rows:
        y_key = row["y_key"]
        runs = sorted(city_by_y[y_key], key=lambda item: _to_float(item["sami"]), reverse=True)
        sami_vals = sorted(_to_float(item["sami"]) for item in runs)
        abs_vals = sorted(abs(_to_float(item["sami"])) for item in runs)
        q05 = _quantile(sami_vals, 0.05)
        q25 = _quantile(sami_vals, 0.25)
        q50 = _quantile(sami_vals, 0.50)
        q75 = _quantile(sami_vals, 0.75)
        q95 = _quantile(sami_vals, 0.95)
        top_n = max(1, int(round(len(runs) * 0.10)))
        top_slice = runs[:top_n]
        bottom_slice = runs[-top_n:]
        for item in top_slice:
            city_signal_counts[item["city_code"]]["positive_top_decile_count"] += 1
            city_signal_counts[item["city_code"]]["selected_y_count"] = len(keep_keys)
            city_signal_counts[item["city_code"]]["city_name"] = item["city_name"]
            city_signal_counts[item["city_code"]]["state_code"] = item["state_code"]
        for item in bottom_slice:
            city_signal_counts[item["city_code"]]["negative_bottom_decile_count"] += 1
            city_signal_counts[item["city_code"]]["selected_y_count"] = len(keep_keys)
            city_signal_counts[item["city_code"]]["city_name"] = item["city_name"]
            city_signal_counts[item["city_code"]]["state_code"] = item["state_code"]

        y_summary_rows.append(
            {
                "family": row["family"],
                "category_label": row["category_label"],
                "y_key": y_key,
                "fitability_class": row["fitability_class"],
                "share_of_total": row["share_of_total"],
                "beta": row["beta"],
                "r2": row["r2"],
                "resid_std": row["resid_std"],
                "coverage_rate": row["coverage_rate"],
                "n_obs": row["n_obs"],
                "sami_q05": q05,
                "sami_q25": q25,
                "sami_q50": q50,
                "sami_q75": q75,
                "sami_q95": q95,
                "sami_iqr": q75 - q25,
                "sami_q90_range": q95 - q05,
                "median_abs_sami": _quantile(abs_vals, 0.50),
                "top_city": runs[0]["city_name"] if runs else "",
                "top_city_sami": _to_float(runs[0]["sami"]) if runs else 0.0,
                "bottom_city": runs[-1]["city_name"] if runs else "",
                "bottom_city_sami": _to_float(runs[-1]["sami"]) if runs else 0.0,
            }
        )
        for rank, item in enumerate(runs[:EXTREME_COUNT], start=1):
            extreme_rows.append(
                {
                    "y_key": y_key,
                    "family": row["family"],
                    "category_label": row["category_label"],
                    "extreme_side": "top",
                    "rank_within_side": rank,
                    "city_code": item["city_code"],
                    "city_name": item["city_name"],
                    "state_code": item["state_code"],
                    "population": item["population"],
                    "y_observed": item["y_observed"],
                    "y_expected": item["y_expected"],
                    "sami": item["sami"],
                    "share_of_city_total": item["share_of_city_total"],
                }
            )
        for rank, item in enumerate(sorted(runs[-EXTREME_COUNT:], key=lambda x: _to_float(x["sami"])), start=1):
            extreme_rows.append(
                {
                    "y_key": y_key,
                    "family": row["family"],
                    "category_label": row["category_label"],
                    "extreme_side": "bottom",
                    "rank_within_side": rank,
                    "city_code": item["city_code"],
                    "city_name": item["city_name"],
                    "state_code": item["state_code"],
                    "population": item["population"],
                    "y_observed": item["y_observed"],
                    "y_expected": item["y_expected"],
                    "sami": item["sami"],
                    "share_of_city_total": item["share_of_city_total"],
                }
            )

    y_summary_rows = sorted(y_summary_rows, key=lambda row: (PRIMARY_FAMILIES.index(row["family"]) if row["family"] in PRIMARY_FAMILIES else 99, -_to_float(row["r2"])))
    _write_csv(OUTPUT_DIR / "y_interpretive_summary.csv", y_summary_rows, list(y_summary_rows[0].keys()) if y_summary_rows else [])
    _write_csv(OUTPUT_DIR / "y_city_extremes.csv", extreme_rows, list(extreme_rows[0].keys()) if extreme_rows else [])

    family_rows: list[dict] = []
    by_family: dict[str, list[dict]] = defaultdict(list)
    for row in y_summary_rows:
        by_family[row["family"]].append(row)
    for family in PRIMARY_FAMILIES + [SECONDARY_FAMILY]:
        rows = by_family.get(family, [])
        if not rows:
            continue
        family_rows.append(
            {
                "family": family,
                "y_count": len(rows),
                "median_beta": median(_to_float(row["beta"]) for row in rows),
                "median_r2": median(_to_float(row["r2"]) for row in rows),
                "median_sami_iqr": median(_to_float(row["sami_iqr"]) for row in rows),
                "median_sami_q90_range": median(_to_float(row["sami_q90_range"]) for row in rows),
                "top_r2_y": max(rows, key=lambda row: _to_float(row["r2"]))["category_label"],
                "widest_sami_y": max(rows, key=lambda row: _to_float(row["sami_q90_range"]))["category_label"],
            }
        )
    _write_csv(OUTPUT_DIR / "family_interpretive_summary.csv", family_rows, list(family_rows[0].keys()) if family_rows else [])

    city_signal_rows = []
    for city_code, data in city_signal_counts.items():
        city_signal_rows.append(
            {
                "city_code": city_code,
                "city_name": data["city_name"],
                "state_code": data["state_code"],
                "selected_y_count": data["selected_y_count"],
                "positive_top_decile_count": data["positive_top_decile_count"],
                "negative_bottom_decile_count": data["negative_bottom_decile_count"],
                "net_signal": data["positive_top_decile_count"] - data["negative_bottom_decile_count"],
            }
        )
    city_signal_rows.sort(key=lambda row: (-int(row["positive_top_decile_count"]), int(row["negative_bottom_decile_count"]), row["city_name"]))
    _write_csv(OUTPUT_DIR / "city_signal_summary.csv", city_signal_rows, list(city_signal_rows[0].keys()) if city_signal_rows else [])

    figures_manifest: list[dict[str, str]] = []

    for filename, rows, metric, title, subtitle, color in [
        ("retained_y_r2_rank.svg", y_summary_rows, "r2", "Retained city Y ranked by R²", "Only retained Y are included; this is the fit layer for interpretation.", BLUE),
        ("retained_y_sami_q90_range_rank.svg", y_summary_rows, "sami_q90_range", "Retained city Y ranked by SAMI 90% range", "Spread of city deviations within each retained Y.", RUST),
        ("retained_y_beta_rank.svg", y_summary_rows, "beta", "Retained city Y ranked by beta", "Interpret beta only within retained Y.", TEAL),
    ]:
        fig = write_ranked_metric_chart(
            figures_dir / filename,
            title=title,
            subtitle=subtitle,
            rows=rows,
            metric_field=metric,
            order_field=metric,
            color=color,
            value_formatter=lambda value: f"{value:.3f}",
        )
        figures_manifest.append({"figure_key": filename.replace(".svg", ""), "path": str(fig)})

    fig = _scatter(
        y_summary_rows,
        x_key="r2",
        y_key="sami_q90_range",
        path=figures_dir / "retained_y_sami_spread_vs_r2.svg",
        title="Retained city Y: SAMI spread versus R²",
        subtitle="A strong fit does not eliminate deviation; spread shows how much cities still separate around each retained law.",
    )
    figures_manifest.append({"figure_key": "retained_y_sami_spread_vs_r2", "path": str(fig)})

    fig = _scatter(
        y_summary_rows,
        x_key="beta",
        y_key="sami_q90_range",
        path=figures_dir / "retained_y_sami_spread_vs_beta.svg",
        title="Retained city Y: SAMI spread versus beta",
        subtitle="Shows whether more superlinear or sublinear retained Y also produce wider city separation.",
    )
    figures_manifest.append({"figure_key": "retained_y_sami_spread_vs_beta", "path": str(fig)})

    positive_city_rows = sorted(city_signal_rows, key=lambda row: (-int(row["positive_top_decile_count"]), row["city_name"]))[:25]
    negative_city_rows = sorted(city_signal_rows, key=lambda row: (-int(row["negative_bottom_decile_count"]), row["city_name"]))[:25]
    _write_csv(OUTPUT_DIR / "top_positive_signal_cities.csv", positive_city_rows, list(positive_city_rows[0].keys()) if positive_city_rows else [])
    _write_csv(OUTPUT_DIR / "top_negative_signal_cities.csv", negative_city_rows, list(negative_city_rows[0].keys()) if negative_city_rows else [])

    _write_csv(OUTPUT_DIR / "figures_manifest.csv", figures_manifest, ["figure_key", "path"])

    total_row = next(row for row in y_summary_rows if row["family"] == "total")
    scian2_top = sorted([row for row in y_summary_rows if row["family"] == "scian2"], key=lambda row: _to_float(row["r2"]), reverse=True)[:5]
    widest_spread = sorted(y_summary_rows, key=lambda row: _to_float(row["sami_q90_range"]), reverse=True)[:5]
    positive_leaders = positive_city_rows[:10]
    negative_leaders = negative_city_rows[:10]

    report_lines = [
        "# City Y Interpretive Pack",
        "",
        "Date: `2026-04-22`",
        "",
        "This pack moves from screening to interpretation for the city system.",
        "",
        "It uses only retained city `Y` and asks three questions:",
        "- which retained `Y` define the strongest city scaling laws",
        "- which retained `Y` generate the widest city separation in `SAMI`",
        "- which cities appear repeatedly at the positive or negative side of the retained laws",
        "",
        "## Inputs",
        "",
        f"- Curated pack: [report.md]({CURATED_DIR / 'report.md'})",
        f"- City SAMI comparison pack: [report.md]({CITY_SAMI_DIR / 'report.md'})",
        "",
        "## Outputs",
        "",
        f"- Y summary: [y_interpretive_summary.csv]({OUTPUT_DIR / 'y_interpretive_summary.csv'})",
        f"- Family summary: [family_interpretive_summary.csv]({OUTPUT_DIR / 'family_interpretive_summary.csv'})",
        f"- City extremes: [y_city_extremes.csv]({OUTPUT_DIR / 'y_city_extremes.csv'})",
        f"- City signal summary: [city_signal_summary.csv]({OUTPUT_DIR / 'city_signal_summary.csv'})",
        f"- Positive leaders: [top_positive_signal_cities.csv]({OUTPUT_DIR / 'top_positive_signal_cities.csv'})",
        f"- Negative leaders: [top_negative_signal_cities.csv]({OUTPUT_DIR / 'top_negative_signal_cities.csv'})",
        f"- Figure manifest: [figures_manifest.csv]({OUTPUT_DIR / 'figures_manifest.csv'})",
        "",
        "Key figures:",
        f"- [retained_y_r2_rank.svg]({figures_dir / 'retained_y_r2_rank.svg'})",
        f"- [retained_y_beta_rank.svg]({figures_dir / 'retained_y_beta_rank.svg'})",
        f"- [retained_y_sami_q90_range_rank.svg]({figures_dir / 'retained_y_sami_q90_range_rank.svg'})",
        f"- [retained_y_sami_spread_vs_r2.svg]({figures_dir / 'retained_y_sami_spread_vs_r2.svg'})",
        f"- [retained_y_sami_spread_vs_beta.svg]({figures_dir / 'retained_y_sami_spread_vs_beta.svg'})",
        "",
        "## Scope",
        "",
        f"- retained city `Y` used here: `{len(y_summary_rows)}`",
        f"- primary families included: `{', '.join(PRIMARY_FAMILIES)}`",
        f"- secondary SCIAN3 retained for interpretation: `{len(scian3_keep)}`",
        "",
        "## Main Reading",
        "",
        f"- aggregate city law remains the strongest baseline: `beta={_fmt(_to_float(total_row['beta']))}`, `R²={_fmt(_to_float(total_row['r2']))}`, `SAMI 90% range={_fmt(_to_float(total_row['sami_q90_range']))}`",
        "- size-based families remain the most coherent retained block; all `per_ocu` and all `size_class` categories survive into interpretation",
        "- sectoral interpretation is strongest at SCIAN2 and selective at SCIAN3",
        "",
        "## Top SCIAN2 Retained Laws by R²",
    ]
    for row in scian2_top:
        report_lines.append(
            f"- `{row['category_label']}`: `beta={_fmt(_to_float(row['beta']))}`, `R²={_fmt(_to_float(row['r2']))}`, `SAMI 90% range={_fmt(_to_float(row['sami_q90_range']))}`"
        )
    report_lines.extend(
        [
            "",
            "## Retained Y With the Widest City Separation",
        ]
    )
    for row in widest_spread:
        report_lines.append(
            f"- `{row['category_label']}` ({row['family']}): `SAMI 90% range={_fmt(_to_float(row['sami_q90_range']))}`, `R²={_fmt(_to_float(row['r2']))}`, `beta={_fmt(_to_float(row['beta']))}`"
        )

    report_lines.extend(
        [
            "",
            "## Recurrent Positive-Signal Cities",
        ]
    )
    for row in positive_leaders:
        report_lines.append(
            f"- `{row['city_name']}` (`{row['state_code']}`): top decile in `{row['positive_top_decile_count']}` retained `Y`, bottom decile in `{row['negative_bottom_decile_count']}`"
        )
    report_lines.extend(
        [
            "",
            "## Recurrent Negative-Signal Cities",
        ]
    )
    for row in negative_leaders:
        report_lines.append(
            f"- `{row['city_name']}` (`{row['state_code']}`): bottom decile in `{row['negative_bottom_decile_count']}` retained `Y`, top decile in `{row['positive_top_decile_count']}`"
        )

    report_lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "The city layer is now interpretable in two distinct ways.",
            "",
            "First, by fit:",
            "- `R²` tells us which retained `Y` are tightly structured by city size",
            "- `beta` tells us whether the retained law is near-linear, sublinear, or superlinear",
            "",
            "Second, by deviation:",
            "- `SAMI` is only interpreted within the same retained `Y`",
            "- the `SAMI 90% range` tells us how much cities separate around that retained law",
            "- repeated top-decile or bottom-decile presence across retained `Y` marks consistent urban over- or under-performance profiles",
            "",
            "The key result is that strong city laws still leave room for substantial urban differentiation. A good fit does not remove city heterogeneity; it defines the baseline around which that heterogeneity becomes interpretable.",
            "",
            "## Next Step",
            "",
            "The next correct move is to build a city-results narrative on top of this pack:",
            "- one section on the aggregate and size-family laws",
            "- one section on the retained SCIAN2 sectors",
            "- one section on recurrent city outliers across retained `Y`",
        ]
    )

    (OUTPUT_DIR / "report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

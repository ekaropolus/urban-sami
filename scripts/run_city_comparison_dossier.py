#!/usr/bin/env python3
from __future__ import annotations

import csv
import html
from pathlib import Path


ROOT = Path("/home/hadox/cmd-center/platforms/research/urban-sami")
COMPARISON_DIR = ROOT / "reports" / "city-y-sami-comparison-pack-2026-04-21"
CURATED_DIR = ROOT / "reports" / "city-y-curated-results-pack-2026-04-22"
OUTPUT_DIR = ROOT / "reports" / "city-comparison-dossier-2026-04-22"

BG = "#f8f6f1"
PANEL = "#fffdf8"
GRID = "#ddd6c8"
TEXT = "#1f1f1f"
MUTED = "#625d54"
AXIS = "#8b8478"
NEG = "#2b6cb0"
POS = "#b14d3b"
NEUTRAL = "#f3eee4"
SERIF = "Georgia, 'Times New Roman', serif"
SANS = "Helvetica, Arial, sans-serif"

LARGE_CITY_POP_MIN = 250_000
GROUP_SIZE = 15


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


def _safe_slug(text: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(text).strip())
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_") or "item"


def _fmt_num(value: float, digits: int = 3) -> str:
    return f"{value:.{digits}f}"


def _fmt_int(value: float) -> str:
    return f"{int(round(value)):,}"


def _family_rank(family: str) -> int:
    order = {"total": 0, "per_ocu": 1, "size_class": 2, "scian2": 3}
    return order.get(family, 99)


def _sami_fill(value: float) -> str:
    clipped = max(-2.5, min(2.5, value))
    if abs(clipped) < 0.08:
        return NEUTRAL
    if clipped > 0:
        frac = clipped / 2.5
        light = 94 - int(frac * 28)
        return f"rgb(177,{light+40},{light+26})"
    frac = abs(clipped) / 2.5
    light = 94 - int(frac * 28)
    return f"rgb({light+24},{light+54},176)"


def _text_fill_for_bg(value: float) -> str:
    return "#ffffff" if abs(value) >= 1.15 else TEXT


def _svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _city_header_label(city: dict[str, str]) -> str:
    return f"{city['city_name']} ({_fmt_int(_to_float(city['population']))})"


def _write_rank_plot(group_name: str, rows: list[dict[str, str]], path: Path) -> Path:
    width = 1100
    row_h = 28
    top = 110
    left = 350
    bottom = 50
    right = 70
    chart_w = width - left - right
    height = top + row_h * len(rows) + bottom
    sami_vals = [_to_float(row["total_sami"]) for row in rows]
    vmin = min(sami_vals + [-0.2])
    vmax = max(sami_vals + [0.2])
    if abs(vmax - vmin) < 0.1:
        vmin -= 0.5
        vmax += 0.5
    pad = (vmax - vmin) * 0.08
    vmin -= pad
    vmax += pad

    def px(val: float) -> float:
        return left + ((val - vmin) / (vmax - vmin)) * chart_w

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{html.escape(group_name)}: total SAMI across cities</text>',
        f'<text x="44" y="76" font-size="14" font-family="{SANS}" fill="{MUTED}">Each point is a city. Total-city scaling uses one cross-city law; city values are SAMI deviations from that law.</text>',
    ]
    for tick in range(6):
        frac = tick / 5
        val = vmin + frac * (vmax - vmin)
        x = px(val)
        body.append(f'<line x1="{x:.2f}" y1="{top-10}" x2="{x:.2f}" y2="{height-bottom+5}" stroke="{GRID}" stroke-width="1"/>')
        body.append(f'<text x="{x:.2f}" y="{height-16}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">{_fmt_num(val,2)}</text>')
    x_zero = px(0.0)
    body.append(f'<line x1="{x_zero:.2f}" y1="{top-12}" x2="{x_zero:.2f}" y2="{height-bottom+5}" stroke="{AXIS}" stroke-width="1.5" stroke-dasharray="5,5"/>')
    body.append(f'<text x="{x_zero+8:.2f}" y="{top-20}" font-size="11" font-family="{SANS}" fill="{MUTED}">SAMI = 0</text>')

    for idx, row in enumerate(rows):
        y = top + idx * row_h
        name = row["city_name"]
        pop = _fmt_int(_to_float(row["population"]))
        est = _fmt_int(_to_float(row["total_establishments"]))
        sami = _to_float(row["total_sami"])
        color = POS if sami >= 0 else NEG
        body.append(f'<line x1="{left-10}" y1="{y:.2f}" x2="{width-right}" y2="{y:.2f}" stroke="#f0eadf" stroke-width="1"/>')
        body.append(f'<text x="{left-14}" y="{y+4:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{html.escape(name)}</text>')
        body.append(f'<text x="44" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{MUTED}">pop {pop} | est {est}</text>')
        body.append(f'<circle cx="{px(sami):.2f}" cy="{y:.2f}" r="5.4" fill="{color}"/>')
        body.append(f'<text x="{width-right+8}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{MUTED}">{_fmt_num(sami,3)}</text>')
    return _svg(path, width, height, "".join(body))


def _write_profile_table(
    title: str,
    subtitle: str,
    cities: list[dict[str, str]],
    y_rows: list[dict[str, str]],
    score_lookup: dict[tuple[str, str], dict[str, str]],
    path: Path,
) -> Path:
    left_name = 360
    left_beta = 95
    left_r2 = 85
    cell_w = 94
    cell_h = 30
    header_h = 170
    top = 96
    right = 30
    width = left_name + left_beta + left_r2 + (cell_w * len(cities)) + right
    height = top + header_h + (cell_h * len(y_rows)) + 40

    x_name = 40
    x_beta = x_name + left_name
    x_r2 = x_beta + left_beta
    x_cells = x_r2 + left_r2
    y0 = top + header_h

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{html.escape(title)}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">{html.escape(subtitle)}</text>',
        f'<text x="{x_name}" y="{top+header_h-18}" font-size="12" font-family="{SANS}" fill="{MUTED}">Y</text>',
        f'<text x="{x_beta + left_beta/2:.2f}" y="{top+header_h-18}" text-anchor="middle" font-size="12" font-family="{SANS}" fill="{MUTED}">β</text>',
        f'<text x="{x_r2 + left_r2/2:.2f}" y="{top+header_h-18}" text-anchor="middle" font-size="12" font-family="{SANS}" fill="{MUTED}">R²</text>',
    ]

    for idx, city in enumerate(cities):
        x = x_cells + idx * cell_w + (cell_w / 2)
        label = html.escape(city["city_name"])
        body.append(
            f'<text x="{x:.2f}" y="{top+header_h-12}" text-anchor="start" font-size="11" font-family="{SANS}" fill="{TEXT}" '
            f'transform="rotate(-60 {x:.2f} {top+header_h-12})">{label}</text>'
        )
        body.append(
            f'<text x="{x+16:.2f}" y="{top+header_h-64}" text-anchor="start" font-size="10" font-family="{SANS}" fill="{MUTED}" '
            f'transform="rotate(-60 {x+16:.2f} {top+header_h-64})">pop {_fmt_int(_to_float(city["population"]))}</text>'
        )

    for row_idx, y_row in enumerate(y_rows):
        y = y0 + row_idx * cell_h
        label = y_row["category_label"]
        beta = _to_float(y_row["beta"])
        r2 = _to_float(y_row["r2"])
        body.append(f'<line x1="36" y1="{y:.2f}" x2="{width-right}" y2="{y:.2f}" stroke="#efe8da" stroke-width="1"/>')
        body.append(f'<text x="{x_name}" y="{y+5:.2f}" font-size="12" font-family="{SANS}" fill="{TEXT}">{html.escape(label)}</text>')
        body.append(f'<text x="{x_beta + left_beta/2:.2f}" y="{y+5:.2f}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">{_fmt_num(beta,3)}</text>')
        body.append(f'<text x="{x_r2 + left_r2/2:.2f}" y="{y+5:.2f}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">{_fmt_num(r2,3)}</text>')
        for city_idx, city in enumerate(cities):
            x = x_cells + city_idx * cell_w
            score = score_lookup.get((city["city_code"], y_row["y_key"]))
            sami = _to_float(score["sami"]) if score else 0.0
            fill = _sami_fill(sami) if score else "#f7f3eb"
            text_fill = _text_fill_for_bg(sami) if score else MUTED
            body.append(f'<rect x="{x+2}" y="{y-12}" width="{cell_w-4}" height="{cell_h-4}" rx="5" fill="{fill}" stroke="#ffffff"/>')
            text = _fmt_num(sami,2) if score else "NA"
            body.append(f'<text x="{x + cell_w/2:.2f}" y="{y+6:.2f}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{text_fill}">{text}</text>')

    legend_y = 100
    legend_x = width - 270
    legend = [(-2.0, "below"), (-1.0, ""), (0.0, "near expected"), (1.0, ""), (2.0, "above")]
    body.append(f'<text x="{legend_x}" y="{legend_y-12}" font-size="11" font-family="{SANS}" fill="{MUTED}">Cell = city SAMI within each Y</text>')
    for idx, (val, label) in enumerate(legend):
        x = legend_x + idx * 44
        body.append(f'<rect x="{x}" y="{legend_y}" width="30" height="16" rx="3" fill="{_sami_fill(val)}"/>')
        if label:
            body.append(f'<text x="{x+15}" y="{legend_y+30}" text-anchor="middle" font-size="10" font-family="{SANS}" fill="{MUTED}">{label}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "figures").mkdir(parents=True, exist_ok=True)

    city_summary = _read_csv(COMPARISON_DIR / "city_summary.csv")
    city_scores = _read_csv(COMPARISON_DIR / "city_y_sami_long.csv")
    primary_y_rows = _read_csv(CURATED_DIR / "primary_y_shortlist.csv")
    scian2_rows = _read_csv(CURATED_DIR / "scian2_retained.csv")

    score_lookup: dict[tuple[str, str], dict[str, str]] = {}
    for row in city_scores:
        score_lookup[(row["city_code"], row["y_key"])] = row

    city_summary.sort(key=lambda row: _to_float(row["population"]), reverse=True)
    largest = city_summary[:GROUP_SIZE]

    large_cities = [row for row in city_summary if _to_float(row["population"]) >= LARGE_CITY_POP_MIN]
    upper_large = sorted(large_cities, key=lambda row: _to_float(row["total_sami"]), reverse=True)[:GROUP_SIZE]
    lower_large = sorted(large_cities, key=lambda row: _to_float(row["total_sami"]))[:GROUP_SIZE]

    primary_rows = sorted(primary_y_rows, key=lambda row: (_family_rank(row["family"]), row["category_label"]))
    scian2_sorted = sorted(scian2_rows, key=lambda row: int(row["r2_rank_desc"]))
    scian2_page1 = scian2_sorted[:10]
    scian2_page2 = scian2_sorted[10:20]

    groups = [
        ("largest_15_cities", "15 largest cities by population", largest),
        ("upper_15_large_cities", "15 upper-tail cities among cities with population >= 250k", upper_large),
        ("lower_15_large_cities", "15 lower-tail cities among cities with population >= 250k", lower_large),
    ]

    master_rows: list[dict[str, str]] = []
    for group_key, group_label, cities in groups:
        for city in cities:
            master_rows.append(
                {
                    "group_key": group_key,
                    "group_label": group_label,
                    "city_code": city["city_code"],
                    "city_name": city["city_name"],
                    "state_code": city["state_code"],
                    "population": city["population"],
                    "households": city["households"],
                    "total_establishments": city["total_establishments"],
                    "total_sami": city["total_sami"],
                    "stable_y_count": city["stable_y_count"],
                    "stable_top_decile_count": city["stable_top_decile_count"],
                    "stable_bottom_decile_count": city["stable_bottom_decile_count"],
                }
            )
    _write_csv(OUTPUT_DIR / "city_group_master.csv", master_rows, list(master_rows[0].keys()))

    profile_rows: list[dict[str, str]] = []
    for group_key, group_label, cities in groups:
        for y_row in primary_rows + scian2_page1 + scian2_page2:
            for city in cities:
                score = score_lookup.get((city["city_code"], y_row["y_key"]))
                profile_rows.append(
                    {
                        "group_key": group_key,
                        "group_label": group_label,
                        "city_code": city["city_code"],
                        "city_name": city["city_name"],
                        "y_key": y_row["y_key"],
                        "family": y_row["family"],
                        "category_label": y_row["category_label"],
                        "beta": y_row["beta"],
                        "r2": y_row["r2"],
                        "sami": score["sami"] if score else "",
                        "y_observed": score["y_observed"] if score else "",
                        "y_expected": score["y_expected"] if score else "",
                    }
                )
    _write_csv(OUTPUT_DIR / "city_y_profiles_long.csv", profile_rows, list(profile_rows[0].keys()))

    figure_manifest: list[dict[str, str]] = []

    for group_key, group_label, cities in groups:
        fig = OUTPUT_DIR / "figures" / f"{group_key}_total_sami_rank.svg"
        _write_rank_plot(group_label, cities, fig)
        figure_manifest.append({"group_key": group_key, "figure": fig.name, "description": "Total city SAMI rank"})

        fig = OUTPUT_DIR / "figures" / f"{group_key}_primary_y_profiles.svg"
        _write_profile_table(
            f"{group_label}: primary Y profiles",
            "Rows are retained Y. β and R² belong to the cross-city law for that Y; cell values are city SAMI.",
            cities,
            primary_rows,
            score_lookup,
            fig,
        )
        figure_manifest.append({"group_key": group_key, "figure": fig.name, "description": "Primary Y city profile table"})

    for idx, page_rows in enumerate((scian2_page1, scian2_page2), start=1):
        fig = OUTPUT_DIR / "figures" / f"largest_15_cities_scian2_profiles_p{idx}.svg"
        _write_profile_table(
            f"15 largest cities: SCIAN2 profiles (page {idx})",
            "Rows are retained SCIAN2 sectors. β and R² belong to the cross-city law for that sector; cell values are city SAMI.",
            largest,
            page_rows,
            score_lookup,
            fig,
        )
        figure_manifest.append({"group_key": "largest_15_cities", "figure": fig.name, "description": f"SCIAN2 city profile table page {idx}"})

    _write_csv(OUTPUT_DIR / "figures_manifest.csv", figure_manifest, ["group_key", "figure", "description"])

    report = OUTPUT_DIR / "report.md"
    report.write_text(
        "\n".join(
            [
                "# City Comparison Dossier",
                "",
                "This dossier is city-centered.",
                "",
                "- `beta` and `R²` are properties of each cross-city law for a given `Y`.",
                "- `SAMI` is the city-specific deviation within that same `Y`.",
                "- The profile figures therefore show `beta` and `R²` at the row level and city `SAMI` inside each cell.",
                "",
                "Files:",
                f"- `city_group_master.csv`: [city_group_master.csv]({(OUTPUT_DIR / 'city_group_master.csv').as_posix()})",
                f"- `city_y_profiles_long.csv`: [city_y_profiles_long.csv]({(OUTPUT_DIR / 'city_y_profiles_long.csv').as_posix()})",
                f"- `figures_manifest.csv`: [figures_manifest.csv]({(OUTPUT_DIR / 'figures_manifest.csv').as_posix()})",
                "",
                "Main figures:",
                f"- largest cities rank: [largest_15_cities_total_sami_rank.svg]({(OUTPUT_DIR / 'figures' / 'largest_15_cities_total_sami_rank.svg').as_posix()})",
                f"- largest cities primary profiles: [largest_15_cities_primary_y_profiles.svg]({(OUTPUT_DIR / 'figures' / 'largest_15_cities_primary_y_profiles.svg').as_posix()})",
                f"- upper-tail large cities primary profiles: [upper_15_large_cities_primary_y_profiles.svg]({(OUTPUT_DIR / 'figures' / 'upper_15_large_cities_primary_y_profiles.svg').as_posix()})",
                f"- lower-tail large cities primary profiles: [lower_15_large_cities_primary_y_profiles.svg]({(OUTPUT_DIR / 'figures' / 'lower_15_large_cities_primary_y_profiles.svg').as_posix()})",
                f"- largest cities SCIAN2 page 1: [largest_15_cities_scian2_profiles_p1.svg]({(OUTPUT_DIR / 'figures' / 'largest_15_cities_scian2_profiles_p1.svg').as_posix()})",
                f"- largest cities SCIAN2 page 2: [largest_15_cities_scian2_profiles_p2.svg]({(OUTPUT_DIR / 'figures' / 'largest_15_cities_scian2_profiles_p2.svg').as_posix()})",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

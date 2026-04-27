#!/usr/bin/env python3
from __future__ import annotations

import csv
import html
from collections import Counter
from pathlib import Path


ROOT = Path("/home/hadox/cmd-center/platforms/research/urban-sami")
COMPARISON_DIR = ROOT / "reports" / "city-y-sami-comparison-pack-2026-04-21"
CURATED_DIR = ROOT / "reports" / "city-y-curated-results-pack-2026-04-22"
MULTI_DIR = ROOT / "reports" / "city-multi-criteria-pack-2026-04-22"
OUTPUT_DIR = ROOT / "reports" / "city-grouped-fit-profiles-2026-04-22"

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

STATE_NAMES = {
    "01": "Aguascalientes", "02": "Baja California", "03": "Baja California Sur", "04": "Campeche",
    "05": "Coahuila", "06": "Colima", "07": "Chiapas", "08": "Chihuahua", "09": "Ciudad de México",
    "10": "Durango", "11": "Guanajuato", "12": "Guerrero", "13": "Hidalgo", "14": "Jalisco",
    "15": "México", "16": "Michoacán", "17": "Morelos", "18": "Nayarit", "19": "Nuevo León",
    "20": "Oaxaca", "21": "Puebla", "22": "Querétaro", "23": "Quintana Roo", "24": "San Luis Potosí",
    "25": "Sinaloa", "26": "Sonora", "27": "Tabasco", "28": "Tamaulipas", "29": "Tlaxcala",
    "30": "Veracruz", "31": "Yucatán", "32": "Zacatecas",
}

SIZE_ORDER = {"small": 0, "medium": 1, "large": 2, "macro": 3}
GROUP_CITY_LIMIT = 12


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


def _fmt_num(value: float, digits: int = 3) -> str:
    return f"{value:.{digits}f}"


def _fmt_int(value: float) -> str:
    return f"{int(round(value)):,}"


def _safe_slug(text: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(text).strip())
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_") or "item"


def _family_rank(family: str) -> int:
    return {"total": 0, "per_ocu": 1, "size_class": 2, "scian2": 3}.get(family, 99)


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


def _write_rank_plot(group_label: str, rows: list[dict[str, str]], path: Path) -> Path:
    width = 1060
    row_h = 28
    top = 110
    left = 340
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
        f'<text x="44" y="50" font-size="25" font-family="{SERIF}" fill="{TEXT}">{html.escape(group_label)}: total SAMI</text>',
        f'<text x="44" y="76" font-size="14" font-family="{SANS}" fill="{MUTED}">Cities within the same group, ranked by total-city SAMI.</text>',
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
        sami = _to_float(row["total_sami"])
        color = POS if sami >= 0 else NEG
        body.append(f'<line x1="{left-10}" y1="{y:.2f}" x2="{width-right}" y2="{y:.2f}" stroke="#f0eadf" stroke-width="1"/>')
        body.append(f'<text x="{left-14}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{html.escape(row["city_name"])}</text>')
        body.append(f'<text x="44" y="{y+5:.2f}" font-size="11" font-family="{SANS}" fill="{MUTED}">pop {_fmt_int(_to_float(row["population"]))} | est {_fmt_int(_to_float(row["total_establishments"]))}</text>')
        body.append(f'<circle cx="{px(sami):.2f}" cy="{y:.2f}" r="5.2" fill="{color}"/>')
        body.append(f'<text x="{width-right+8}" y="{y+5:.2f}" font-size="11" font-family="{SANS}" fill="{MUTED}">{_fmt_num(sami,3)}</text>')
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
    cell_w = 92
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
        f'<text x="44" y="50" font-size="25" font-family="{SERIF}" fill="{TEXT}">{html.escape(title)}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">{html.escape(subtitle)}</text>',
        f'<text x="{x_name}" y="{top+header_h-18}" font-size="12" font-family="{SANS}" fill="{MUTED}">Y</text>',
        f'<text x="{x_beta + left_beta/2:.2f}" y="{top+header_h-18}" text-anchor="middle" font-size="12" font-family="{SANS}" fill="{MUTED}">β</text>',
        f'<text x="{x_r2 + left_r2/2:.2f}" y="{top+header_h-18}" text-anchor="middle" font-size="12" font-family="{SANS}" fill="{MUTED}">R²</text>',
    ]
    for idx, city in enumerate(cities):
        x = x_cells + idx * cell_w + (cell_w / 2)
        body.append(
            f'<text x="{x:.2f}" y="{top+header_h-12}" text-anchor="start" font-size="11" font-family="{SANS}" fill="{TEXT}" '
            f'transform="rotate(-60 {x:.2f} {top+header_h-12})">{html.escape(city["city_name"])}</text>'
        )
        body.append(
            f'<text x="{x+16:.2f}" y="{top+header_h-64}" text-anchor="start" font-size="10" font-family="{SANS}" fill="{MUTED}" '
            f'transform="rotate(-60 {x+16:.2f} {top+header_h-64})">pop {_fmt_int(_to_float(city["population"]))}</text>'
        )
    for row_idx, y_row in enumerate(y_rows):
        y = y0 + row_idx * cell_h
        beta = _to_float(y_row["beta"])
        r2 = _to_float(y_row["r2"])
        body.append(f'<line x1="36" y1="{y:.2f}" x2="{width-right}" y2="{y:.2f}" stroke="#efe8da" stroke-width="1"/>')
        body.append(f'<text x="{x_name}" y="{y+5:.2f}" font-size="12" font-family="{SANS}" fill="{TEXT}">{html.escape(y_row["category_label"])}</text>')
        body.append(f'<text x="{x_beta + left_beta/2:.2f}" y="{y+5:.2f}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">{_fmt_num(beta,3)}</text>')
        body.append(f'<text x="{x_r2 + left_r2/2:.2f}" y="{y+5:.2f}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">{_fmt_num(r2,3)}</text>')
        for city_idx, city in enumerate(cities):
            x = x_cells + city_idx * cell_w
            score = score_lookup.get((city["city_code"], y_row["y_key"]))
            sami = _to_float(score["sami"]) if score else 0.0
            fill = _sami_fill(sami) if score else "#f7f3eb"
            text_fill = _text_fill_for_bg(sami) if score else MUTED
            body.append(f'<rect x="{x+2}" y="{y-12}" width="{cell_w-4}" height="{cell_h-4}" rx="5" fill="{fill}" stroke="#ffffff"/>')
            body.append(f'<text x="{x + cell_w/2:.2f}" y="{y+6:.2f}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{text_fill}">{_fmt_num(sami,2) if score else "NA"}</text>')
    legend_y = 100
    legend_x = width - 270
    legend = [(-2.0, "below"), (-1.0, ""), (0.0, "near expected"), (1.0, ""), (2.0, "above")]
    body.append(f'<text x="{legend_x}" y="{legend_y-12}" font-size="11" font-family="{SANS}" fill="{MUTED}">Cell = city SAMI within each retained Y</text>')
    for idx, (val, label) in enumerate(legend):
        x = legend_x + idx * 44
        body.append(f'<rect x="{x}" y="{legend_y}" width="30" height="16" rx="3" fill="{_sami_fill(val)}"/>')
        if label:
            body.append(f'<text x="{x+15}" y="{legend_y+30}" text-anchor="middle" font-size="10" font-family="{SANS}" fill="{MUTED}">{label}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "figures").mkdir(parents=True, exist_ok=True)

    city_master = _read_csv(MULTI_DIR / "city_master_enriched.csv")
    city_scores = _read_csv(COMPARISON_DIR / "city_y_sami_long.csv")
    primary_y = _read_csv(CURATED_DIR / "primary_y_shortlist.csv")

    score_lookup: dict[tuple[str, str], dict[str, str]] = {}
    for row in city_scores:
        score_lookup[(row["city_code"], row["y_key"])] = row

    primary_rows = sorted(primary_y, key=lambda row: (_family_rank(row["family"]), row["category_label"]))

    size_groups: list[tuple[str, str, list[dict[str, str]]]] = []
    for size_band in sorted({row["size_band"] for row in city_master}, key=lambda s: SIZE_ORDER.get(s, 99)):
        rows = [row for row in city_master if row["size_band"] == size_band]
        rows.sort(key=lambda row: _to_float(row["population"]), reverse=True)
        selected = rows[:GROUP_CITY_LIMIT]
        size_groups.append((f"size_{size_band}", f"{size_band.capitalize()} cities", selected))

    state_counts = Counter(row["state_code"] for row in city_master)
    top_states = sorted(state_counts.keys(), key=lambda code: int(code))
    state_groups: list[tuple[str, str, list[dict[str, str]]]] = []
    for state_code in top_states:
        rows = [row for row in city_master if row["state_code"] == state_code]
        rows.sort(key=lambda row: _to_float(row["population"]), reverse=True)
        state_label = STATE_NAMES.get(state_code, state_code)
        if len(rows) <= GROUP_CITY_LIMIT:
            state_groups.append((f"state_{state_code}", f"{state_label} cities", rows))
        else:
            page = 1
            for start in range(0, len(rows), GROUP_CITY_LIMIT):
                selected = rows[start:start + GROUP_CITY_LIMIT]
                state_groups.append(
                    (f"state_{state_code}_p{page}", f"{state_label} cities (page {page})", selected)
                )
                page += 1

    recurrent_upper = sorted(
        city_master,
        key=lambda row: (_to_float(row["stable_top_decile_count"]), _to_float(row["total_sami"])),
        reverse=True,
    )[:GROUP_CITY_LIMIT]
    recurrent_lower = sorted(
        city_master,
        key=lambda row: (_to_float(row["stable_bottom_decile_count"]), -_to_float(row["total_sami"])),
        reverse=True,
    )[:GROUP_CITY_LIMIT]
    high_contrast_positive = sorted(city_master, key=lambda row: _to_float(row["total_sami"]), reverse=True)[:GROUP_CITY_LIMIT]
    high_contrast_negative = sorted(city_master, key=lambda row: _to_float(row["total_sami"]))[:GROUP_CITY_LIMIT]

    signal_groups = [
        ("signal_recurrent_upper", "Cities with recurrent upper-tail signal", recurrent_upper),
        ("signal_recurrent_lower", "Cities with recurrent lower-tail signal", recurrent_lower),
        ("signal_high_contrast_positive", "Cities with highest positive total SAMI", high_contrast_positive),
        ("signal_high_contrast_negative", "Cities with lowest total SAMI", high_contrast_negative),
    ]

    all_groups = size_groups + signal_groups + state_groups

    group_catalog: list[dict[str, str]] = []
    city_group_rows: list[dict[str, str]] = []
    for group_key, group_label, cities in all_groups:
        group_catalog.append({"group_key": group_key, "group_label": group_label, "city_count": str(len(cities))})
        for city in cities:
            city_group_rows.append(
                {
                    "group_key": group_key,
                    "group_label": group_label,
                    "city_code": city["city_code"],
                    "city_name": city["city_name"],
                    "state_code": city["state_code"],
                    "state_name": STATE_NAMES.get(city["state_code"], city["state_code"]),
                    "size_band": city["size_band"],
                    "population": city["population"],
                    "total_establishments": city["total_establishments"],
                    "total_sami": city["total_sami"],
                    "stable_top_decile_count": city.get("stable_top_decile_count", ""),
                    "stable_bottom_decile_count": city.get("stable_bottom_decile_count", ""),
                }
            )
    _write_csv(OUTPUT_DIR / "group_catalog.csv", group_catalog, ["group_key", "group_label", "city_count"])
    _write_csv(OUTPUT_DIR / "city_groups_master.csv", city_group_rows, list(city_group_rows[0].keys()))

    reading_order = [
        {
            "reading_step": "1",
            "block": "signal",
            "group_key": "signal_high_contrast_positive",
            "group_label": "Cities with highest positive total SAMI",
            "why_read": "Fastest entry to see the strongest upward deviations.",
        },
        {
            "reading_step": "2",
            "block": "signal",
            "group_key": "signal_high_contrast_negative",
            "group_label": "Cities with lowest total SAMI",
            "why_read": "Fastest entry to see the strongest downward deviations.",
        },
        {
            "reading_step": "3",
            "block": "signal",
            "group_key": "signal_recurrent_upper",
            "group_label": "Cities with recurrent upper-tail signal",
            "why_read": "Shows cities repeatedly appearing in the upper tail across retained Y.",
        },
        {
            "reading_step": "4",
            "block": "signal",
            "group_key": "signal_recurrent_lower",
            "group_label": "Cities with recurrent lower-tail signal",
            "why_read": "Shows cities repeatedly appearing in the lower tail across retained Y.",
        },
        {
            "reading_step": "5",
            "block": "size",
            "group_key": "size_macro",
            "group_label": "Macro cities",
            "why_read": "Best place to compare major cities with broad support across Y.",
        },
        {
            "reading_step": "6",
            "block": "size",
            "group_key": "size_large",
            "group_label": "Large cities",
            "why_read": "Good intermediate layer between macro and medium systems.",
        },
        {
            "reading_step": "7",
            "block": "size",
            "group_key": "size_medium",
            "group_label": "Medium cities",
            "why_read": "Lets you inspect whether deviations change outside the largest urban tier.",
        },
        {
            "reading_step": "8",
            "block": "size",
            "group_key": "size_small",
            "group_label": "Small cities",
            "why_read": "Useful last because small-city variability is high and harder to read first.",
        },
    ]
    _write_csv(OUTPUT_DIR / "reading_order.csv", reading_order, ["reading_step", "block", "group_key", "group_label", "why_read"])

    profile_rows: list[dict[str, str]] = []
    for group_key, group_label, cities in all_groups:
        for y_row in primary_rows:
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
                    }
                )
    _write_csv(OUTPUT_DIR / "city_group_profiles_long.csv", profile_rows, list(profile_rows[0].keys()))

    manifest: list[dict[str, str]] = []
    for group_key, group_label, cities in all_groups:
        rank_path = OUTPUT_DIR / "figures" / f"{group_key}_total_sami_rank.svg"
        _write_rank_plot(group_label, cities, rank_path)
        manifest.append({"group_key": group_key, "figure": rank_path.name, "description": "Total SAMI rank within group"})

        profile_path = OUTPUT_DIR / "figures" / f"{group_key}_primary_y_profiles.svg"
        _write_profile_table(
            f"{group_label}: retained Y profiles",
            "Rows are retained Y with fit class A/B. Row β and R² are from the cross-city law; cells show city SAMI.",
            cities,
            primary_rows,
            score_lookup,
            profile_path,
        )
        manifest.append({"group_key": group_key, "figure": profile_path.name, "description": "Primary Y profiles within group"})
    _write_csv(OUTPUT_DIR / "figures_manifest.csv", manifest, ["group_key", "figure", "description"])

    report = OUTPUT_DIR / "report.md"
    report.write_text(
        "\n".join(
            [
                "# City Grouped Fit Profiles",
                "",
                "This pack reuses the existing city partitions and filters only retained Y with fit class A/B.",
                "",
                "- Group axes used here: `size_band` and `state_code`.",
                "- Only primary retained Y are shown: `total`, `per_ocu`, `size_class`, `scian2` retained shortlist.",
                "- For each group, the figures show city SAMI inside the same table that displays row-level `β` and `R²`.",
                "",
                "Files:",
                f"- [group_catalog.csv]({(OUTPUT_DIR / 'group_catalog.csv').as_posix()})",
                f"- [reading_order.csv]({(OUTPUT_DIR / 'reading_order.csv').as_posix()})",
                f"- [city_groups_master.csv]({(OUTPUT_DIR / 'city_groups_master.csv').as_posix()})",
                f"- [city_group_profiles_long.csv]({(OUTPUT_DIR / 'city_group_profiles_long.csv').as_posix()})",
                f"- [figures_manifest.csv]({(OUTPUT_DIR / 'figures_manifest.csv').as_posix()})",
                "",
                "Reading order:",
                "1. high contrast positive",
                f"- [signal_high_contrast_positive_primary_y_profiles.svg]({(OUTPUT_DIR / 'figures' / 'signal_high_contrast_positive_primary_y_profiles.svg').as_posix()})",
                "2. high contrast negative",
                f"- [signal_high_contrast_negative_primary_y_profiles.svg]({(OUTPUT_DIR / 'figures' / 'signal_high_contrast_negative_primary_y_profiles.svg').as_posix()})",
                "3. recurrent upper tail",
                f"- [signal_recurrent_upper_primary_y_profiles.svg]({(OUTPUT_DIR / 'figures' / 'signal_recurrent_upper_primary_y_profiles.svg').as_posix()})",
                "4. recurrent lower tail",
                f"- [signal_recurrent_lower_primary_y_profiles.svg]({(OUTPUT_DIR / 'figures' / 'signal_recurrent_lower_primary_y_profiles.svg').as_posix()})",
                "5. macro, then large, then medium, then small",
                f"- [size_macro_primary_y_profiles.svg]({(OUTPUT_DIR / 'figures' / 'size_macro_primary_y_profiles.svg').as_posix()})",
                f"- [size_large_primary_y_profiles.svg]({(OUTPUT_DIR / 'figures' / 'size_large_primary_y_profiles.svg').as_posix()})",
                f"- [size_medium_primary_y_profiles.svg]({(OUTPUT_DIR / 'figures' / 'size_medium_primary_y_profiles.svg').as_posix()})",
                f"- [size_small_primary_y_profiles.svg]({(OUTPUT_DIR / 'figures' / 'size_small_primary_y_profiles.svg').as_posix()})",
                "6. states only after the contrast and size blocks",
                f"- [state_20_p1_primary_y_profiles.svg]({(OUTPUT_DIR / 'figures' / 'state_20_p1_primary_y_profiles.svg').as_posix()})",
                f"- [state_20_p2_primary_y_profiles.svg]({(OUTPUT_DIR / 'figures' / 'state_20_p2_primary_y_profiles.svg').as_posix()})",
                f"- [state_21_p1_primary_y_profiles.svg]({(OUTPUT_DIR / 'figures' / 'state_21_p1_primary_y_profiles.svg').as_posix()})",
                f"- [state_30_p1_primary_y_profiles.svg]({(OUTPUT_DIR / 'figures' / 'state_30_p1_primary_y_profiles.svg').as_posix()})",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

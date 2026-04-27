#!/usr/bin/env python3
from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median

from run_denue_y_state_scientific_analysis import BLUE, GOLD, RUST, TEAL, write_ranked_metric_chart


ROOT = Path("/home/hadox/cmd-center/platforms/research/urban-sami")
FITABILITY_DIR = ROOT / "reports" / "city-y-fitability-audit-2026-04-21"
OUTPUT_DIR = ROOT / "reports" / "city-y-curated-results-pack-2026-04-22"
PRIMARY_FAMILIES = ["total", "per_ocu", "size_class", "scian2"]
SECONDARY_FAMILIES = ["scian3"]
RETAINED_CLASSES = {"A_strong", "B_usable"}

CLASS_LABELS = {
    "A_strong": "A strong",
    "B_usable": "B usable",
    "C_exploratory": "C exploratory",
    "D_unfit": "D unfit",
}
CLASS_COLORS = {
    "A_strong": TEAL,
    "B_usable": BLUE,
    "C_exploratory": GOLD,
    "D_unfit": RUST,
}


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


def _pct(value: float, digits: int = 1) -> str:
    return f"{value * 100:.{digits}f}%"


def _fmt(value: float, digits: int = 3) -> str:
    return f"{value:.{digits}f}"


def _family_sort_key(family: str) -> tuple[int, str]:
    ordering = {key: idx for idx, key in enumerate(PRIMARY_FAMILIES + SECONDARY_FAMILIES)}
    return (ordering.get(family, 99), family)


def _write_svg(path: Path, body: str, width: int, height: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _stacked_class_chart(summary_rows: list[dict], path: Path) -> Path:
    width = 980
    height = 620
    left = 120
    top = 110
    plot_h = 400
    bar_w = 110
    gap = 35
    scale = plot_h / max(max(1, int(row["y_count"])) for row in summary_rows)

    body = [
        f'<rect width="{width}" height="{height}" fill="#f8f6f1"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="#fffdf8" stroke="#ddd6c8"/>',
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">City Y fitability classes by family</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">Primary families retained for interpretation; stacked counts show how much of each family is paper-grade.</text>',
    ]
    for tick in range(5):
        value = max(max(1, int(row["y_count"])) for row in summary_rows) * tick / 4
        y = top + plot_h - (value * scale)
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{width-60}" y2="{y:.2f}" stroke="#e4ddd1" stroke-width="1"/>')
        body.append(f'<text x="{left-12}" y="{y+4:.2f}" text-anchor="end" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="#625d54">{int(round(value))}</text>')
    x = left + 10
    for row in summary_rows:
        y0 = top + plot_h
        for klass in ("D_unfit", "C_exploratory", "B_usable", "A_strong"):
            count = int(row.get(f"{klass.lower()}_count", 0))
            h = count * scale
            y0 -= h
            body.append(
                f'<rect x="{x}" y="{y0:.2f}" width="{bar_w}" height="{h:.2f}" fill="{CLASS_COLORS[klass]}" fill-opacity="0.86"/>'
            )
        body.append(f'<rect x="{x}" y="{top}" width="{bar_w}" height="{plot_h}" fill="none" stroke="#8b8478" stroke-width="1.1"/>')
        body.append(
            f'<text x="{x + (bar_w/2):.2f}" y="{top + plot_h + 26}" text-anchor="middle" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="#1f1f1f">{row["family"]}</text>'
        )
        body.append(
            f'<text x="{x + (bar_w/2):.2f}" y="{top + plot_h + 44}" text-anchor="middle" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="#625d54">n={row["y_count"]}</text>'
        )
        x += bar_w + gap
    body.append(f'<line x1="{left}" y1="{top + plot_h}" x2="{width-60}" y2="{top + plot_h}" stroke="#8b8478" stroke-width="1.2"/>')
    legend_x = width - 255
    legend_y = 104
    for idx, klass in enumerate(("A_strong", "B_usable", "C_exploratory", "D_unfit")):
        y = legend_y + idx * 24
        body.append(f'<rect x="{legend_x}" y="{y}" width="14" height="14" fill="{CLASS_COLORS[klass]}" fill-opacity="0.86"/>')
        body.append(f'<text x="{legend_x+22}" y="{y+12}" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="#1f1f1f">{CLASS_LABELS[klass]}</text>')
    return _write_svg(path, "".join(body), width, height)


def _beta_r2_scatter(rows: list[dict], path: Path) -> Path:
    width = 1020
    height = 680
    left = 100
    right = 60
    top = 96
    bottom = 86
    plot_w = width - left - right
    plot_h = height - top - bottom
    min_beta = min(_to_float(row["beta"]) for row in rows)
    max_beta = max(_to_float(row["beta"]) for row in rows)
    min_beta -= 0.05
    max_beta += 0.05

    def px(beta: float) -> float:
        return left + ((beta - min_beta) / (max_beta - min_beta)) * plot_w

    def py(r2: float) -> float:
        return top + (1.0 - r2) * plot_h

    body = [
        f'<rect width="{width}" height="{height}" fill="#f8f6f1"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="#fffdf8" stroke="#ddd6c8"/>',
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">Retained city Y: beta versus R²</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">Only A-strong and B-usable Y are shown. Size encodes national share of total establishments.</text>',
    ]
    for frac in (0.0, 0.25, 0.5, 0.75, 1.0):
        x = left + (frac * plot_w)
        beta = min_beta + (frac * (max_beta - min_beta))
        body.append(f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top+plot_h}" stroke="#e4ddd1" stroke-width="1"/>')
        body.append(f'<text x="{x:.2f}" y="{height-34}" text-anchor="middle" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="#625d54">{beta:.2f}</text>')
    for frac in (0.0, 0.25, 0.5, 0.75, 1.0):
        y = top + ((1.0 - frac) * plot_h)
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left+plot_w}" y2="{y:.2f}" stroke="#e4ddd1" stroke-width="1"/>')
        body.append(f'<text x="{left-12}" y="{y+4:.2f}" text-anchor="end" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="#625d54">{frac:.2f}</text>')
    body.append(f'<line x1="{left}" y1="{top+plot_h}" x2="{left+plot_w}" y2="{top+plot_h}" stroke="#8b8478" stroke-width="1.2"/>')
    body.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top+plot_h}" stroke="#8b8478" stroke-width="1.2"/>')
    body.append(f'<text x="{left + (plot_w/2):.2f}" y="{height-8}" text-anchor="middle" font-size="13" font-family="Helvetica, Arial, sans-serif" fill="#625d54">beta</text>')
    body.append(f'<text x="28" y="{top + (plot_h/2):.2f}" text-anchor="middle" font-size="13" font-family="Helvetica, Arial, sans-serif" fill="#625d54" transform="rotate(-90 28 {top + (plot_h/2):.2f})">R²</text>')
    for row in rows:
        beta = _to_float(row["beta"])
        r2 = _to_float(row["r2"])
        share = _to_float(row["share_of_total"])
        radius = 3.5 + (22.0 * (share ** 0.35))
        family = row["family"]
        color = {
            "total": TEAL,
            "per_ocu": BLUE,
            "size_class": GOLD,
            "scian2": RUST,
            "scian3": "#4d7a57",
        }.get(family, "#6b6b6b")
        body.append(
            f'<circle cx="{px(beta):.2f}" cy="{py(r2):.2f}" r="{radius:.2f}" fill="{color}" fill-opacity="0.72" stroke="#ffffff" stroke-width="0.8">'
            f'<title>{row["y_key"]} | {row["category_label"]} | beta={beta:.3f} | R²={r2:.3f}</title></circle>'
        )
    legend_x = width - 230
    legend_y = 104
    for idx, (family, label, color) in enumerate(
        [
            ("total", "Total", TEAL),
            ("per_ocu", "per_ocu", BLUE),
            ("size_class", "size_class", GOLD),
            ("scian2", "SCIAN2", RUST),
            ("scian3", "SCIAN3", "#4d7a57"),
        ]
    ):
        y = legend_y + idx * 22
        body.append(f'<circle cx="{legend_x}" cy="{y}" r="6" fill="{color}" fill-opacity="0.8"/>')
        body.append(f'<text x="{legend_x+14}" y="{y+4}" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="#1f1f1f">{label}</text>')
    return _write_svg(path, "".join(body), width, height)


def main() -> int:
    full_rows = _read_csv(FITABILITY_DIR / "city_y_fitability_full.csv")
    family_rows = _read_csv(FITABILITY_DIR / "family_fitability_summary.csv")

    retained_rows = [row for row in full_rows if row["fitability_class"] in RETAINED_CLASSES]
    primary_rows = [row for row in retained_rows if row["family"] in PRIMARY_FAMILIES]
    secondary_rows = [row for row in retained_rows if row["family"] in SECONDARY_FAMILIES]
    all_curated_rows = sorted(primary_rows + secondary_rows, key=lambda row: (_family_sort_key(row["family"]), int(row["r2_rank_desc"])))

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    figures_dir = OUTPUT_DIR / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)

    _write_csv(
        OUTPUT_DIR / "curated_y_catalog.csv",
        all_curated_rows,
        list(all_curated_rows[0].keys()) if all_curated_rows else [],
    )

    primary_shortlist = sorted(primary_rows, key=lambda row: (_family_sort_key(row["family"]), int(row["r2_rank_desc"])))
    _write_csv(
        OUTPUT_DIR / "primary_y_shortlist.csv",
        primary_shortlist,
        list(primary_shortlist[0].keys()) if primary_shortlist else [],
    )

    scian2_rows = [row for row in primary_shortlist if row["family"] == "scian2"]
    per_ocu_rows = [row for row in primary_shortlist if row["family"] == "per_ocu"]
    size_class_rows = [row for row in primary_shortlist if row["family"] == "size_class"]
    scian3_rows = [row for row in secondary_rows if row["family"] == "scian3"]
    total_rows = [row for row in primary_shortlist if row["family"] == "total"]

    for name, rows in (
        ("total_retained.csv", total_rows),
        ("per_ocu_retained.csv", per_ocu_rows),
        ("size_class_retained.csv", size_class_rows),
        ("scian2_retained.csv", scian2_rows),
        ("scian3_retained.csv", scian3_rows),
    ):
        if rows:
            _write_csv(OUTPUT_DIR / name, rows, list(rows[0].keys()))

    core_summary_rows = []
    class_counts = Counter(row["fitability_class"] for row in full_rows)
    for family_row in sorted(
        [row for row in family_rows if row["family"] in PRIMARY_FAMILIES + SECONDARY_FAMILIES],
        key=lambda row: _family_sort_key(row["family"]),
    ):
        family = family_row["family"]
        family_retained = [row for row in retained_rows if row["family"] == family]
        family_primary = [row for row in primary_rows if row["family"] == family]
        family_secondary = [row for row in secondary_rows if row["family"] == family]
        family_all = family_primary if family in PRIMARY_FAMILIES else family_secondary
        median_beta = median(_to_float(row["beta"]) for row in family_all) if family_all else 0.0
        median_r2 = median(_to_float(row["r2"]) for row in family_all) if family_all else 0.0
        top_row = min(family_retained, key=lambda row: int(row["r2_rank_desc"])) if family_retained else None
        core_summary_rows.append(
            {
                "family": family,
                "family_label": family_row["family_label"],
                "y_count_total": family_row["y_count"],
                "retained_count": len(family_retained),
                "retained_share": len(family_retained) / max(1, int(family_row["y_count"])),
                "a_strong_count": family_row["a_strong_count"],
                "b_usable_count": family_row["b_usable_count"],
                "median_r2_retained": median_r2,
                "median_beta_retained": median_beta,
                "best_y_key": top_row["y_key"] if top_row else "",
                "best_category_label": top_row["category_label"] if top_row else "",
                "best_r2": _to_float(top_row["r2"]) if top_row else 0.0,
            }
        )

    _write_csv(
        OUTPUT_DIR / "core_family_summary.csv",
        core_summary_rows,
        list(core_summary_rows[0].keys()) if core_summary_rows else [],
    )

    figures_manifest: list[dict[str, str]] = []
    stacked_fig = _stacked_class_chart(
        [row for row in family_rows if row["family"] in PRIMARY_FAMILIES + SECONDARY_FAMILIES],
        figures_dir / "fitability_classes_primary_families.svg",
    )
    figures_manifest.append({"figure_key": "fitability_classes_primary_families", "path": str(stacked_fig)})

    scatter_fig = _beta_r2_scatter(all_curated_rows, figures_dir / "retained_beta_vs_r2.svg")
    figures_manifest.append({"figure_key": "retained_beta_vs_r2", "path": str(scatter_fig)})

    def add_rank_chart(rows: list[dict], metric_key: str, title: str, subtitle: str, filename: str, value_format: str = "float3") -> None:
        if not rows:
            return
        fig = write_ranked_metric_chart(
            figures_dir / filename,
            title=title,
            subtitle=subtitle,
            rows=rows,
            metric_field=metric_key,
            color=BLUE if "per_ocu" in filename else (GOLD if "size_class" in filename else RUST),
            value_formatter=(lambda value: f"{value:.3f}") if value_format == "float3" else str,
        )
        figures_manifest.append({"figure_key": filename.replace(".svg", ""), "path": str(fig)})

    add_rank_chart(scian2_rows, "beta", "City SCIAN2 beta by retained category", "Only A-strong and B-usable SCIAN2 sectors", "scian2_beta_rank.svg")
    add_rank_chart(scian2_rows, "r2", "City SCIAN2 R² by retained category", "Only A-strong and B-usable SCIAN2 sectors", "scian2_r2_rank.svg")
    add_rank_chart(per_ocu_rows, "beta", "City per_ocu beta by retained category", "All DENUE size bands are retained", "per_ocu_beta_rank.svg")
    add_rank_chart(per_ocu_rows, "r2", "City per_ocu R² by retained category", "All DENUE size bands are retained", "per_ocu_r2_rank.svg")
    add_rank_chart(size_class_rows, "beta", "City size_class beta by retained category", "Derived size classes retained for interpretation", "size_class_beta_rank.svg")
    add_rank_chart(size_class_rows, "r2", "City size_class R² by retained category", "Derived size classes retained for interpretation", "size_class_r2_rank.svg")
    add_rank_chart(scian3_rows, "beta", "City SCIAN3 beta by retained category", "Retained SCIAN3 categories only", "scian3_beta_rank.svg")
    add_rank_chart(scian3_rows, "r2", "City SCIAN3 R² by retained category", "Retained SCIAN3 categories only", "scian3_r2_rank.svg")

    _write_csv(OUTPUT_DIR / "figures_manifest.csv", figures_manifest, ["figure_key", "path"])

    scian2_unfit = [row for row in full_rows if row["family"] == "scian2" and row["fitability_class"] == "D_unfit"]
    scian2_usable = [row for row in full_rows if row["family"] == "scian2" and row["fitability_class"] in {"A_strong", "B_usable"}]

    report_lines = [
        "# Curated City Y Results Pack",
        "",
        "Date: `2026-04-22`",
        "",
        "This pack converts the full city-level fitability audit into a curated scientific reading layer.",
        "",
        "The goal is simple:",
        "- keep only city `Y` that are interpretable under the scaling framework",
        "- separate the paper-grade primary families from the noisier long tail",
        "- produce a compact results layer that can drive the next stage of analysis and writing",
        "",
        "## Inputs",
        "",
        f"- Fitability audit: [city_y_fitability_full.csv]({FITABILITY_DIR / 'city_y_fitability_full.csv'})",
        f"- Family summary: [family_fitability_summary.csv]({FITABILITY_DIR / 'family_fitability_summary.csv'})",
        "",
        "## Outputs",
        "",
        f"- Curated catalog: [curated_y_catalog.csv]({OUTPUT_DIR / 'curated_y_catalog.csv'})",
        f"- Primary shortlist: [primary_y_shortlist.csv]({OUTPUT_DIR / 'primary_y_shortlist.csv'})",
        f"- Core family summary: [core_family_summary.csv]({OUTPUT_DIR / 'core_family_summary.csv'})",
        f"- Figure manifest: [figures_manifest.csv]({OUTPUT_DIR / 'figures_manifest.csv'})",
        "",
        "Family tables:",
        f"- [total_retained.csv]({OUTPUT_DIR / 'total_retained.csv'})",
        f"- [per_ocu_retained.csv]({OUTPUT_DIR / 'per_ocu_retained.csv'})",
        f"- [size_class_retained.csv]({OUTPUT_DIR / 'size_class_retained.csv'})",
        f"- [scian2_retained.csv]({OUTPUT_DIR / 'scian2_retained.csv'})",
        f"- [scian3_retained.csv]({OUTPUT_DIR / 'scian3_retained.csv'})",
        "",
        "Key figures:",
        f"- [fitability_classes_primary_families.svg]({stacked_fig})",
        f"- [retained_beta_vs_r2.svg]({scatter_fig})",
        f"- [scian2_beta_rank.svg]({figures_dir / 'scian2_beta_rank.svg'})",
        f"- [scian2_r2_rank.svg]({figures_dir / 'scian2_r2_rank.svg'})",
        f"- [per_ocu_beta_rank.svg]({figures_dir / 'per_ocu_beta_rank.svg'})",
        f"- [per_ocu_r2_rank.svg]({figures_dir / 'per_ocu_r2_rank.svg'})",
        "",
        "## Main Counts",
        "",
        f"- total audited city `Y`: `{len(full_rows)}`",
        f"- retained city `Y` (`A_strong` + `B_usable`): `{len(all_curated_rows)}`",
        f"- retained primary city `Y` (`total`, `per_ocu`, `size_class`, `scian2`): `{len(primary_shortlist)}`",
        f"- retained secondary city `Y` (`scian3` only in this pack): `{len(scian3_rows)}`",
        "",
        "Across the full city catalog:",
        f"- `A_strong`: `{class_counts['A_strong']}`",
        f"- `B_usable`: `{class_counts['B_usable']}`",
        f"- `C_exploratory`: `{class_counts['C_exploratory']}`",
        f"- `D_unfit`: `{class_counts['D_unfit']}`",
        "",
        "## Primary Family Reading",
        "",
        f"- `total`: `{len(total_rows)}/1` retained; this remains the cleanest city-scale baseline with `beta={_fmt(_to_float(total_rows[0]['beta']))}` and `R²={_fmt(_to_float(total_rows[0]['r2']))}`",
        f"- `per_ocu`: `{len(per_ocu_rows)}/7` retained; all DENUE size bands are interpretable at city scale",
        f"- `size_class`: `{len(size_class_rows)}/4` retained; all derived size classes are interpretable at city scale",
        f"- `scian2`: `{len(scian2_usable)}/23` retained; sector-level city scaling is broadly usable but not universal",
        f"- `scian3`: `{len(scian3_rows)}/91` retained; this is the first finer family still worth keeping as a secondary layer",
        "",
        "SCIAN2 sectors excluded at city scale:",
    ]
    for row in sorted(scian2_unfit, key=lambda item: int(item["r2_rank_desc"])):
        report_lines.append(
            f"- `{row['category_label']}`: `R²={_fmt(_to_float(row['r2']))}`, coverage `{_pct(_to_float(row['coverage_rate']))}`, zero-rate `{_pct(_to_float(row['zero_rate']))}`"
        )

    report_lines.extend(
        [
            "",
            "## Scientific Reading",
            "",
            "The city system now has a defensible retained Y layer.",
            "",
            "Three points matter most:",
            "",
            "1. The strongest city law is still the aggregate baseline.",
            f"   Total establishments remain near-linear and strong: `beta={_fmt(_to_float(total_rows[0]['beta']))}`, `R²={_fmt(_to_float(total_rows[0]['r2']))}`.",
            "",
            "2. Size-based decomposition is much more robust than the long categorical tail.",
            "   Both `per_ocu` and `size_class` survive almost completely, which means city size organizes the establishment-size structure far more consistently than it organizes very narrow activity niches.",
            "",
            "3. Sectoral interpretation is viable at SCIAN2 and selective at SCIAN3.",
            "   This is the correct city-scale scientific layer for the paper: broad sectors and size categories, not the full categorical tail.",
            "",
            "## Next Step",
            "",
            "The next correct move is to use this retained city Y layer to build the actual results narrative:",
            "- compare `beta`, `R²`, and SAMI only within retained Y",
            "- isolate the few sectoral families that are both strong and theoretically meaningful",
            "- move from screening to interpretation for cities",
        ]
    )

    (OUTPUT_DIR / "report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

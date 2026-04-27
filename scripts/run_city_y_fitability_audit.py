#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import shutil
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median

from run_denue_y_state_scientific_analysis import BLUE, GOLD, RUST, TEAL, write_ranked_metric_chart


SOURCE_CITY_NATIVE = "denue-y-city-native-experiments-2026-04-21"
OUTPUT_PACK = "city-y-fitability-audit-2026-04-21"
ELIGIBLE_CITY_COUNTS = Path("/home/hadox/cmd-center/platforms/research/urban-sami/dist/independent_city_baseline/city_counts.csv")

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
        .replace("|", "__")
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


def _eligible_city_rows() -> list[dict[str, str]]:
    rows = _read_csv(ELIGIBLE_CITY_COUNTS)
    return [row for row in rows if _to_float(row["population"]) > 0.0]


def _fitability_class(*, r2: float, n_obs: int, coverage_rate: float, zero_rate: float) -> tuple[str, str]:
    if r2 >= 0.70 and n_obs >= 500 and coverage_rate >= 0.70 and zero_rate <= 0.30:
        return "A_strong", "strong fit, broad coverage, low sparsity"
    if r2 >= 0.45 and n_obs >= 200 and coverage_rate >= 0.35 and zero_rate <= 0.65:
        return "B_usable", "usable fit with moderate support"
    if r2 >= 0.15 and n_obs >= 50 and coverage_rate >= 0.10:
        return "C_exploratory", "weak fit or sparse support; exploratory only"
    return "D_unfit", "too sparse or too weak for substantive interpretation"


def _write_class_scatter(path: Path, *, rows: list[dict[str, str]]) -> Path:
    width = 1100
    height = 820
    left = 110
    right = 40
    top = 90
    bottom = 90
    plot_w = width - left - right
    plot_h = height - top - bottom

    valid = [row for row in rows if _to_float(row["coverage_rate"]) >= 0 and _to_float(row["r2"]) >= 0]
    x_min = 0.0
    x_max = 1.0
    y_min = 0.0
    y_max = 1.0

    def px(xv: float) -> float:
        return left + ((xv - x_min) / max(x_max - x_min, 1e-9)) * plot_w

    def py(yv: float) -> float:
        return top + plot_h - ((yv - y_min) / max(y_max - y_min, 1e-9)) * plot_h

    parts = [
        f'<rect width="{width}" height="{height}" fill="#f8f6f1"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="#fffdf8" stroke="#ddd6c8"/>',
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">City Y fitability: R² versus coverage</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">Each point is one Y definition. X = city coverage rate, Y = OLS R², color = fitability class.</text>',
        f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="none" stroke="#8b8478" stroke-width="1.2"/>',
    ]
    for tick in range(6):
        xv = tick / 5.0
        x = px(xv)
        parts.append(f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top+plot_h}" stroke="#ddd6c8"/>')
        parts.append(f'<text x="{x:.2f}" y="{height-44}" text-anchor="middle" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="#625d54">{xv:.1f}</text>')
        yv = tick / 5.0
        y = py(yv)
        parts.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left+plot_w}" y2="{y:.2f}" stroke="#ddd6c8"/>')
        parts.append(f'<text x="{left-14}" y="{y+4:.2f}" text-anchor="end" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="#625d54">{yv:.1f}</text>')

    for row in valid:
        x = px(_to_float(row["coverage_rate"]))
        y = py(_to_float(row["r2"]))
        color = CLASS_COLORS[row["fitability_class"]]
        radius = 2.0 if _to_float(row["share_of_total"]) < 0.01 else 3.0
        parts.append(f'<circle cx="{x:.2f}" cy="{y:.2f}" r="{radius:.1f}" fill="{color}" fill-opacity="0.70"/>')

    legend_x = width - 250
    legend_y = 120
    parts.append(f'<rect x="{legend_x}" y="{legend_y}" width="180" height="116" rx="12" fill="#fffdfa" stroke="#ddd6c8"/>')
    for idx, (klass, color) in enumerate(CLASS_COLORS.items()):
        y = legend_y + 24 + (idx * 22)
        parts.append(f'<circle cx="{legend_x+18}" cy="{y:.2f}" r="5" fill="{color}"/>')
        parts.append(f'<text x="{legend_x+32}" y="{y+4:.2f}" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="#1f1f1f">{klass}</text>')
    parts.append(f'<text x="{left + plot_w/2:.2f}" y="{height-12}" text-anchor="middle" font-size="13" font-family="Helvetica, Arial, sans-serif" fill="#625d54">coverage rate across eligible cities</text>')
    parts.append(f'<text x="26" y="{top + plot_h/2:.2f}" text-anchor="middle" font-size="13" font-family="Helvetica, Arial, sans-serif" fill="#625d54" transform="rotate(-90 26 {top + plot_h/2:.2f})">R²</text>')
    return _write_svg(path, "".join(parts), width, height)


def _write_family_class_chart(path: Path, *, rows: list[dict[str, str]]) -> Path:
    families = [row["family"] for row in rows]
    family_order = sorted(set(families))
    counts = {family: Counter() for family in family_order}
    for row in rows:
        counts[row["family"]][row["fitability_class"]] += 1

    width = 1200
    height = 760
    left = 110
    right = 40
    top = 90
    bottom = 110
    plot_w = width - left - right
    plot_h = height - top - bottom
    max_total = max(sum(counts[family].values()) for family in family_order) if family_order else 1

    def px(idx: int) -> float:
        return left + (idx + 0.5) * (plot_w / max(len(family_order), 1))

    def bar_w() -> float:
        return (plot_w / max(len(family_order), 1)) * 0.62

    def py(value: float) -> float:
        return top + plot_h - ((value / max(max_total, 1)) * plot_h)

    parts = [
        f'<rect width="{width}" height="{height}" fill="#f8f6f1"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="#fffdf8" stroke="#ddd6c8"/>',
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">City Y fitability classes by family</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">Stacked counts of Y definitions classified as A, B, C, or D within each family.</text>',
        f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="none" stroke="#8b8478" stroke-width="1.2"/>',
    ]
    for tick in range(6):
        yv = max_total * tick / 5.0
        y = py(yv)
        parts.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left+plot_w}" y2="{y:.2f}" stroke="#ddd6c8"/>')
        parts.append(f'<text x="{left-14}" y="{y+4:.2f}" text-anchor="end" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="#625d54">{int(round(yv))}</text>')

    for idx, family in enumerate(family_order):
        x_center = px(idx)
        x0 = x_center - (bar_w() / 2.0)
        y_cursor = top + plot_h
        for klass in ["A_strong", "B_usable", "C_exploratory", "D_unfit"]:
            count = counts[family][klass]
            if count <= 0:
                continue
            h = (count / max(max_total, 1)) * plot_h
            y0 = y_cursor - h
            parts.append(f'<rect x="{x0:.2f}" y="{y0:.2f}" width="{bar_w():.2f}" height="{h:.2f}" fill="{CLASS_COLORS[klass]}"/>')
            y_cursor = y0
        parts.append(f'<text x="{x_center:.2f}" y="{top+plot_h+24}" text-anchor="middle" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="#625d54" transform="rotate(-28 {x_center:.2f} {top+plot_h+24})">{family}</text>')

    legend_x = width - 245
    legend_y = 120
    parts.append(f'<rect x="{legend_x}" y="{legend_y}" width="182" height="116" rx="12" fill="#fffdfa" stroke="#ddd6c8"/>')
    for idx, (klass, color) in enumerate(CLASS_COLORS.items()):
        y = legend_y + 24 + (idx * 22)
        parts.append(f'<rect x="{legend_x+12}" y="{y-7:.2f}" width="12" height="12" fill="{color}"/>')
        parts.append(f'<text x="{legend_x+32}" y="{y+4:.2f}" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="#1f1f1f">{klass}</text>')

    return _write_svg(path, "".join(parts), width, height)


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    source_dir = root / "reports" / SOURCE_CITY_NATIVE
    outdir = root / "reports" / OUTPUT_PACK
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    fit_rows = _read_csv(source_dir / "city_y_ols_fits.csv")
    unit_rows = _read_csv(source_dir / "city_y_unit_counts.csv")
    eligible_rows = _eligible_city_rows()
    eligible_city_count = len(eligible_rows)
    eligible_city_codes = {row["city_code"] for row in eligible_rows}

    positive_counts_by_y: dict[str, int] = defaultdict(int)
    for row in unit_rows:
        if row["city_code"] in eligible_city_codes and _to_float(row["population"]) > 0:
            positive_counts_by_y[row["y_key"]] += 1

    audit_rows: list[dict[str, str]] = []
    for row in fit_rows:
        y_key = row["y_key"]
        n_obs = int(round(_to_float(row["n_obs"])))
        positive_cities = positive_counts_by_y.get(y_key, n_obs)
        zero_cities = max(eligible_city_count - positive_cities, 0)
        coverage_rate = positive_cities / eligible_city_count if eligible_city_count else 0.0
        zero_rate = zero_cities / eligible_city_count if eligible_city_count else 0.0
        fit_class, fit_note = _fitability_class(
            r2=_to_float(row["r2"]),
            n_obs=n_obs,
            coverage_rate=coverage_rate,
            zero_rate=zero_rate,
        )
        audit_rows.append(
            {
                "family": row["family"],
                "family_label": row["family_label"],
                "category": row["category"],
                "category_label": row["category_label"],
                "y_key": y_key,
                "n_obs": n_obs,
                "eligible_city_count": eligible_city_count,
                "positive_city_count": positive_cities,
                "zero_city_count": zero_cities,
                "coverage_rate": coverage_rate,
                "zero_rate": zero_rate,
                "coverage_tier": row["coverage_tier"],
                "total_count": _to_float(row["total_count"]),
                "share_of_total": _to_float(row["share_of_total"]),
                "alpha": _to_float(row["alpha"]),
                "beta": _to_float(row["beta"]),
                "r2": _to_float(row["r2"]),
                "resid_std": _to_float(row["resid_std"]),
                "fitability_class": fit_class,
                "fitability_note": fit_note,
            }
        )

    audit_rows.sort(key=lambda row: (_to_float(row["r2"]), _to_float(row["coverage_rate"])), reverse=True)
    for idx, row in enumerate(sorted(audit_rows, key=lambda r: _to_float(r["r2"]), reverse=True), start=1):
        row["r2_rank_desc"] = idx
    for idx, row in enumerate(sorted(audit_rows, key=lambda r: _to_float(r["coverage_rate"]), reverse=True), start=1):
        row["coverage_rank_desc"] = idx
    for idx, row in enumerate(sorted(audit_rows, key=lambda r: _to_float(r["zero_rate"]), reverse=True), start=1):
        row["zero_rate_rank_desc"] = idx

    _write_csv(
        outdir / "city_y_fitability_full.csv",
        audit_rows,
        [
            "family",
            "family_label",
            "category",
            "category_label",
            "y_key",
            "n_obs",
            "eligible_city_count",
            "positive_city_count",
            "zero_city_count",
            "coverage_rate",
            "zero_rate",
            "coverage_tier",
            "total_count",
            "share_of_total",
            "alpha",
            "beta",
            "r2",
            "resid_std",
            "fitability_class",
            "fitability_note",
            "r2_rank_desc",
            "coverage_rank_desc",
            "zero_rate_rank_desc",
        ],
    )

    family_rows: list[dict[str, str]] = []
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in audit_rows:
        grouped[row["family"]].append(row)
    for family, rows in sorted(grouped.items()):
        class_counts = Counter(row["fitability_class"] for row in rows)
        family_rows.append(
            {
                "family": family,
                "family_label": rows[0]["family_label"],
                "y_count": len(rows),
                "median_r2": median(_to_float(row["r2"]) for row in rows),
                "median_coverage_rate": median(_to_float(row["coverage_rate"]) for row in rows),
                "median_zero_rate": median(_to_float(row["zero_rate"]) for row in rows),
                "a_strong_count": class_counts["A_strong"],
                "b_usable_count": class_counts["B_usable"],
                "c_exploratory_count": class_counts["C_exploratory"],
                "d_unfit_count": class_counts["D_unfit"],
                "a_strong_share": class_counts["A_strong"] / len(rows) if rows else 0.0,
                "b_usable_share": class_counts["B_usable"] / len(rows) if rows else 0.0,
                "c_exploratory_share": class_counts["C_exploratory"] / len(rows) if rows else 0.0,
                "d_unfit_share": class_counts["D_unfit"] / len(rows) if rows else 0.0,
            }
        )
    _write_csv(
        outdir / "family_fitability_summary.csv",
        family_rows,
        [
            "family",
            "family_label",
            "y_count",
            "median_r2",
            "median_coverage_rate",
            "median_zero_rate",
            "a_strong_count",
            "b_usable_count",
            "c_exploratory_count",
            "d_unfit_count",
            "a_strong_share",
            "b_usable_share",
            "c_exploratory_share",
            "d_unfit_share",
        ],
    )

    strong_rows = [row for row in audit_rows if row["fitability_class"] == "A_strong"]
    unfit_rows = [row for row in audit_rows if row["fitability_class"] == "D_unfit"]
    _write_csv(outdir / "y_fitability_strong.csv", strong_rows, list(strong_rows[0].keys()) if strong_rows else [])
    _write_csv(outdir / "y_fitability_unfit.csv", unfit_rows, list(unfit_rows[0].keys()) if unfit_rows else [])

    fig_manifest: list[dict[str, str]] = []
    r2_fig = figdir / "r2_rank_all.svg"
    write_ranked_metric_chart(
        r2_fig,
        title="City Y fitability: all Y ordered by R²",
        subtitle="One row per Y. Read this first for pure fit quality.",
        rows=audit_rows,
        metric_field="r2",
        order_field="r2",
        color=BLUE,
        fixed_range=(0.0, 1.0),
        value_formatter=lambda value: f"{value:.3f}",
    )
    fig_manifest.append({"figure_id": "r2_rank_all", "path": str(r2_fig.resolve())})

    coverage_fig = figdir / "coverage_rank_all.svg"
    write_ranked_metric_chart(
        coverage_fig,
        title="City Y fitability: all Y ordered by city coverage",
        subtitle="Coverage rate = positive cities / eligible cities.",
        rows=audit_rows,
        metric_field="coverage_rate",
        order_field="coverage_rate",
        color=BLUE,
        fixed_range=(0.0, 1.0),
        value_formatter=lambda value: _pct(value),
    )
    fig_manifest.append({"figure_id": "coverage_rank_all", "path": str(coverage_fig.resolve())})

    zero_fig = figdir / "zero_rate_rank_all.svg"
    write_ranked_metric_chart(
        zero_fig,
        title="City Y fitability: all Y ordered by sparsity",
        subtitle="Zero-rate = cities with Y=0 / eligible cities. Higher means sparser.",
        rows=audit_rows,
        metric_field="zero_rate",
        order_field="zero_rate",
        color=RUST,
        fixed_range=(0.0, 1.0),
        value_formatter=lambda value: _pct(value),
    )
    fig_manifest.append({"figure_id": "zero_rate_rank_all", "path": str(zero_fig.resolve())})

    scatter_fig = figdir / "r2_vs_coverage.svg"
    _write_class_scatter(scatter_fig, rows=audit_rows)
    fig_manifest.append({"figure_id": "r2_vs_coverage", "path": str(scatter_fig.resolve())})

    class_fig = figdir / "fitability_class_by_family.svg"
    _write_family_class_chart(class_fig, rows=audit_rows)
    fig_manifest.append({"figure_id": "fitability_class_by_family", "path": str(class_fig.resolve())})

    _write_csv(outdir / "figures_manifest.csv", fig_manifest, ["figure_id", "path"])

    class_counts = Counter(row["fitability_class"] for row in audit_rows)
    report_lines = [
        "# City Y Fitability Audit",
        "",
        "Date: `2026-04-21`",
        "",
        "This audit stays only at the city level. It evaluates every city-level `Y` definition for whether a cross-city scaling fit is strong enough to interpret substantively, only exploratory, or not fit for interpretation.",
        "",
        "## Fitability Rule",
        "",
        "- `A_strong`: `R² >= 0.70`, `n_obs >= 500`, coverage `>= 70%`, zero-rate `<= 30%`",
        "- `B_usable`: `R² >= 0.45`, `n_obs >= 200`, coverage `>= 35%`, zero-rate `<= 65%`",
        "- `C_exploratory`: `R² >= 0.15`, `n_obs >= 50`, coverage `>= 10%`",
        "- `D_unfit`: below those thresholds",
        "",
        "## Main Files",
        "",
        f"- Full Y audit: [{(outdir / 'city_y_fitability_full.csv').name}]({(outdir / 'city_y_fitability_full.csv').resolve()})",
        f"- Family summary: [{(outdir / 'family_fitability_summary.csv').name}]({(outdir / 'family_fitability_summary.csv').resolve()})",
        f"- Strong Y only: [{(outdir / 'y_fitability_strong.csv').name}]({(outdir / 'y_fitability_strong.csv').resolve()})",
        f"- Unfit Y only: [{(outdir / 'y_fitability_unfit.csv').name}]({(outdir / 'y_fitability_unfit.csv').resolve()})",
        f"- Figures manifest: [{(outdir / 'figures_manifest.csv').name}]({(outdir / 'figures_manifest.csv').resolve()})",
        "",
        "## Figures",
        "",
    ]
    for row in fig_manifest:
        report_lines.append(f"- [{row['figure_id']}]({row['path']})")
    report_lines.extend(
        [
            "",
            "## Scope",
            "",
            f"- eligible city universe: `{eligible_city_count:,}`",
            f"- Y definitions audited: `{len(audit_rows):,}`",
            f"- `A_strong`: `{class_counts['A_strong']:,}`",
            f"- `B_usable`: `{class_counts['B_usable']:,}`",
            f"- `C_exploratory`: `{class_counts['C_exploratory']:,}`",
            f"- `D_unfit`: `{class_counts['D_unfit']:,}`",
            "",
            "## Interpretation Rule",
            "",
            "- low `R²` alone is not treated as a data failure",
            "- low fit plus high sparsity and low coverage is treated as poor support",
            "- low fit with broad coverage is kept as substantive weak-structure evidence, but only exploratory unless the thresholds are met",
        ]
    )
    (outdir / "report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    report_json = {
        "workflow_id": "city_y_fitability_audit",
        "output_dir": str(outdir.resolve()),
        "eligible_city_count": eligible_city_count,
        "y_count": len(audit_rows),
        "class_counts": dict(class_counts),
        "figure_count": len(fig_manifest),
    }
    (outdir / "report.json").write_text(json.dumps(report_json, indent=2), encoding="utf-8")
    print(json.dumps(report_json, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

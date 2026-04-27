#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shutil
from collections import Counter, defaultdict
from pathlib import Path

from run_denue_y_state_scientific_analysis import BLUE, GOLD, RUST, TEAL, write_ranked_metric_chart


SOURCE_PACK = "ageb-city-native-experiments-guadalajara-2026-04-22"
OUTPUT_PACK = "ageb-fitability-audit-guadalajara-2026-04-22"

CLASS_COLORS = {
    "A_strong": TEAL,
    "B_usable": BLUE,
    "C_exploratory": GOLD,
    "D_unfit": RUST,
}

MODEL_ORDER = ["ols", "robust", "poisson", "negbin"]


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


def _sign(value: float, tol: float = 1e-9) -> str:
    if value > tol:
        return "positive"
    if value < -tol:
        return "negative"
    return "near_zero"


def _coverage_tier(coverage_rate: float) -> str:
    if coverage_rate >= 0.90:
        return "near-universal"
    if coverage_rate >= 0.70:
        return "broad"
    if coverage_rate >= 0.40:
        return "partial"
    return "sparse"


def _fitability_class(
    *,
    r2: float,
    n_obs: int,
    coverage_rate: float,
    zero_rate: float,
    sign_stable: bool,
) -> tuple[str, str]:
    if r2 >= 0.25 and n_obs >= 300 and coverage_rate >= 0.75 and zero_rate <= 0.25 and sign_stable:
        return "A_strong", "strong intra-urban structure"
    if r2 >= 0.10 and n_obs >= 250 and coverage_rate >= 0.60 and zero_rate <= 0.40 and sign_stable:
        return "B_usable", "usable intra-urban law with stable sign"
    if r2 >= 0.03 and n_obs >= 150 and coverage_rate >= 0.25:
        return "C_exploratory", "weak but non-trivial intra-urban structure"
    return "D_unfit", "too weak or too sparse for substantive AGEB interpretation"


def _write_class_chart(path: Path, *, rows: list[dict[str, object]]) -> Path:
    width = 920
    height = 540
    left = 120
    right = 40
    top = 90
    bottom = 110
    plot_w = width - left - right
    plot_h = height - top - bottom
    class_order = ["A_strong", "B_usable", "C_exploratory", "D_unfit"]
    counts = Counter(str(row["fitability_class"]) for row in rows)
    max_count = max(counts.values()) if counts else 1

    def y_pos(value: float) -> float:
        return top + plot_h - ((value / max(max_count, 1)) * plot_h)

    bar_w = plot_w / max(len(class_order), 1) * 0.56
    parts = [
        f'<rect width="{width}" height="{height}" fill="#f8f6f1"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="#fffdf8" stroke="#ddd6c8"/>',
        '<text x="44" y="50" font-size="24" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">Guadalajara AGEB fitability classes</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">Classification of Y definitions treated as intra-urban systems of AGEBs.</text>',
        f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="none" stroke="#8b8478"/>',
    ]
    for tick in range(max_count + 1):
        y = y_pos(tick)
        parts.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left+plot_w}" y2="{y:.2f}" stroke="#eee7da"/>')
        parts.append(f'<text x="{left-14}" y="{y+4:.2f}" text-anchor="end" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="#625d54">{tick}</text>')
    for idx, klass in enumerate(class_order):
        x_center = left + (idx + 0.5) * (plot_w / len(class_order))
        count = counts.get(klass, 0)
        height_bar = (count / max(max_count, 1)) * plot_h
        y0 = top + plot_h - height_bar
        parts.append(
            f'<rect x="{x_center - bar_w/2:.2f}" y="{y0:.2f}" width="{bar_w:.2f}" height="{height_bar:.2f}" fill="{CLASS_COLORS[klass]}"/>'
        )
        parts.append(f'<text x="{x_center:.2f}" y="{y0-8:.2f}" text-anchor="middle" font-size="12" font-family="Helvetica, Arial, sans-serif" fill="#1f1f1f">{count}</text>')
        parts.append(f'<text x="{x_center:.2f}" y="{top+plot_h+28:.2f}" text-anchor="middle" font-size="11" font-family="Helvetica, Arial, sans-serif" fill="#625d54">{klass}</text>')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{"".join(parts)}</svg>',
        encoding="utf-8",
    )
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run AGEB fitability audit from a one-city AGEB Y catalog")
    parser.add_argument("--source-pack", default=SOURCE_PACK)
    parser.add_argument("--output-pack", default=OUTPUT_PACK)
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    source_dir = root / "reports" / args.source_pack
    outdir = root / "reports" / args.output_pack
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    count_rows = [row for row in _read_csv(source_dir / "ageb_y_unit_counts.csv") if _to_float(row["population"]) > 0]
    model_rows = _read_csv(source_dir / "ageb_y_all_fits.csv")
    if not count_rows or not model_rows:
        raise SystemExit("Missing source AGEB experiment tables.")

    ageb_universe = {(row["unit_code"], row["population"]) for row in count_rows}
    n_total_ageb = len(ageb_universe)

    by_y_models: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in model_rows:
        by_y_models[row["y_key"]].append(row)

    by_y_positive_units: dict[str, dict[str, float]] = defaultdict(dict)
    y_meta: dict[str, dict[str, str]] = {}
    city_code = ""
    city_name = ""
    for row in count_rows:
        by_y_positive_units[row["y_key"]][row["unit_code"]] = _to_float(row["y_value"])
        y_meta[row["y_key"]] = {
            "family": row["family"],
            "family_label": row["family_label"],
            "category": row["category"],
            "category_label": row["category_label"],
            "y_label": row["category_label"],
        }
        city_code = row["city_code"]
        city_name = row["city_name"]

    unit_codes = sorted({row["unit_code"] for row in count_rows})
    audit_rows: list[dict[str, object]] = []
    for y_key in sorted(by_y_models.keys()):
        positive_map = by_y_positive_units.get(y_key, {})
        vals = [positive_map.get(unit_code, 0.0) for unit_code in unit_codes]
        positive_count = sum(value > 0 for value in vals)
        zero_count = n_total_ageb - positive_count
        coverage_rate = positive_count / n_total_ageb if n_total_ageb else 0.0
        zero_rate = zero_count / n_total_ageb if n_total_ageb else 0.0
        total_count = sum(vals)
        mean_positive = (sum(value for value in vals if value > 0) / positive_count) if positive_count else 0.0
        max_count = max(vals) if vals else 0.0

        model_set = {row["fit_method"]: row for row in by_y_models[y_key]}
        ols = model_set["ols"]
        betas = [_to_float(model_set[method]["beta"]) for method in MODEL_ORDER if method in model_set]
        r2s = [_to_float(model_set[method]["r2"]) for method in MODEL_ORDER if method in model_set]
        beta_signs = {_sign(beta) for beta in betas}
        sign_stable = len(beta_signs) == 1
        beta_range = (max(betas) - min(betas)) if betas else 0.0
        r2_range = (max(r2s) - min(r2s)) if r2s else 0.0
        best_row = max(by_y_models[y_key], key=lambda row: _to_float(row["r2"]))
        fitability_class, fit_note = _fitability_class(
            r2=_to_float(ols["r2"]),
            n_obs=int(round(_to_float(ols["n_obs"]))),
            coverage_rate=coverage_rate,
            zero_rate=zero_rate,
            sign_stable=sign_stable,
        )
        audit_rows.append(
            {
                "city_code": city_code,
                "city_name": city_name,
                "family": y_meta[y_key]["family"],
                "family_label": y_meta[y_key]["family_label"],
                "category": y_meta[y_key]["category"],
                "category_label": y_meta[y_key]["category_label"],
                "y_key": y_key,
                "y_label": ols["category_label"] if "category_label" in ols else y_meta[y_key]["y_label"],
                "n_total_ageb": n_total_ageb,
                "n_obs_ols": int(round(_to_float(ols["n_obs"]))),
                "positive_ageb_count": positive_count,
                "zero_ageb_count": zero_count,
                "coverage_rate": coverage_rate,
                "zero_rate": zero_rate,
                "coverage_tier": _coverage_tier(coverage_rate),
                "total_count": total_count,
                "mean_positive_count": mean_positive,
                "max_count": max_count,
                "beta_ols": _to_float(ols["beta"]),
                "r2_ols": _to_float(ols["r2"]),
                "resid_std_ols": _to_float(ols["resid_std"]),
                "best_method": best_row["fit_method"],
                "best_r2": _to_float(best_row["r2"]),
                "beta_range_across_models": beta_range,
                "r2_range_across_models": r2_range,
                "beta_sign_mode": "/".join(sorted(beta_signs)),
                "beta_sign_stable": "yes" if sign_stable else "no",
                "fitability_class": fitability_class,
                "fitability_note": fit_note,
            }
        )

    audit_rows.sort(key=lambda row: (_to_float(row["r2_ols"]), _to_float(row["coverage_rate"])), reverse=True)
    for idx, row in enumerate(audit_rows, start=1):
        row["r2_rank"] = idx
    for idx, row in enumerate(sorted(audit_rows, key=lambda row: _to_float(row["coverage_rate"]), reverse=True), start=1):
        row["coverage_rank"] = idx
    for idx, row in enumerate(sorted(audit_rows, key=lambda row: _to_float(row["zero_rate"])), start=1):
        row["zero_rate_rank"] = idx

    family_summary: list[dict[str, object]] = []
    grouped_rows: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in audit_rows:
        grouped_rows[str(row["family"])].append(row)
    for family, rows in sorted(grouped_rows.items()):
        by_class = Counter(str(row["fitability_class"]) for row in rows)
        by_tier = Counter(str(row["coverage_tier"]) for row in rows)
        family_summary.append(
            {
                "city_code": city_code,
                "city_name": city_name,
                "family": family,
                "family_label": rows[0]["family_label"],
                "n_total_ageb": n_total_ageb,
                "n_y": len(rows),
                "A_strong": by_class.get("A_strong", 0),
                "B_usable": by_class.get("B_usable", 0),
                "C_exploratory": by_class.get("C_exploratory", 0),
                "D_unfit": by_class.get("D_unfit", 0),
                "near_universal_coverage": by_tier.get("near-universal", 0),
                "broad_coverage": by_tier.get("broad", 0),
                "partial_coverage": by_tier.get("partial", 0),
                "sparse_coverage": by_tier.get("sparse", 0),
            }
        )

    _write_csv(outdir / "ageb_y_fitability_full.csv", audit_rows, list(audit_rows[0].keys()))
    _write_csv(outdir / "family_fitability_summary.csv", family_summary, list(family_summary[0].keys()))
    _write_csv(outdir / "y_fitability_usable.csv", [row for row in audit_rows if row["fitability_class"] in {"A_strong", "B_usable"}], list(audit_rows[0].keys()))
    _write_csv(outdir / "y_fitability_unfit.csv", [row for row in audit_rows if row["fitability_class"] == "D_unfit"], list(audit_rows[0].keys()))

    manifest_rows = []
    r2_fig = write_ranked_metric_chart(
        figdir / "r2_rank_all.svg",
        title="Guadalajara AGEB: OLS R² by Y",
        subtitle="Fit strength for each Y when AGEBs are treated as the units of an intra-urban scaling system.",
        rows=audit_rows,
        metric_field="r2_ols",
        order_field="r2_ols",
        fixed_range=(0.0, max(0.15, max(_to_float(row["r2_ols"]) for row in audit_rows) * 1.08)),
        color=BLUE,
        value_formatter=lambda value: f"{value:.3f}",
    )
    coverage_fig = write_ranked_metric_chart(
        figdir / "coverage_rank_all.svg",
        title="Guadalajara AGEB: coverage rate by Y",
        subtitle="Share of AGEBs with positive counts for each Y. This is the intra-urban support of the candidate law.",
        rows=audit_rows,
        metric_field="coverage_rate",
        order_field="coverage_rate",
        fixed_range=(0.0, 1.0),
        color=GOLD,
        value_formatter=lambda value: _pct(value, 1),
    )
    zero_fig = write_ranked_metric_chart(
        figdir / "zero_rate_rank_all.svg",
        title="Guadalajara AGEB: zero-rate by Y",
        subtitle="Share of AGEBs with zero counts. Higher values mean greater sparsity inside the city.",
        rows=audit_rows,
        metric_field="zero_rate",
        order_field="zero_rate",
        fixed_range=(0.0, 1.0),
        color=RUST,
        value_formatter=lambda value: _pct(value, 1),
    )
    beta_fig = write_ranked_metric_chart(
        figdir / "beta_rank_all.svg",
        title="Guadalajara AGEB: OLS β by Y",
        subtitle="Intra-urban scaling exponents. Negative values mean the activity concentrates away from the most populated residential AGEBs.",
        rows=audit_rows,
        metric_field="beta_ols",
        order_field="r2_ols",
        ref_line=0.0,
        symmetric=True,
        color=TEAL,
        value_formatter=lambda value: f"{value:+.3f}",
    )
    class_fig = _write_class_chart(figdir / "fitability_class_summary.svg", rows=audit_rows)
    manifest_rows.extend(
        [
            {"figure_id": "r2_rank_all", "path": str(r2_fig.resolve()), "description": "AGEB OLS R² by Y."},
            {"figure_id": "coverage_rank_all", "path": str(coverage_fig.resolve()), "description": "AGEB coverage rate by Y."},
            {"figure_id": "zero_rate_rank_all", "path": str(zero_fig.resolve()), "description": "AGEB zero-rate by Y."},
            {"figure_id": "beta_rank_all", "path": str(beta_fig.resolve()), "description": "AGEB OLS beta by Y."},
            {"figure_id": "fitability_class_summary", "path": str(class_fig.resolve()), "description": "AGEB fitability class counts."},
        ]
    )
    _write_csv(figdir / "figures_manifest.csv", manifest_rows, ["figure_id", "path", "description"])

    total_by_class = Counter(str(row["fitability_class"]) for row in audit_rows)
    usable_rows = [row for row in audit_rows if row["fitability_class"] in {"A_strong", "B_usable"}]
    report_lines = [
        "# Guadalajara AGEB Fitability Audit",
        "",
        "This audit treats **AGEBs as the units of an intra-urban system**, analogous to how cities were treated in the city-scale audit.",
        "The question is not whether Guadalajara as a whole scales, but **which Y definitions have enough structure inside Guadalajara** to support a defensible AGEB-level law.",
        "",
        "## Scope",
        "",
        f"- city: `{city_name}` (`{city_code}`)",
        f"- populated AGEB universe: `{n_total_ageb}`",
        f"- audited Y definitions: `{len(audit_rows)}`",
        f"- source experiment pack: [{args.source_pack}]({source_dir.resolve()})",
        "",
        "## Classification Logic",
        "",
        "- `A_strong`: strong AGEB-level fit, broad coverage, low sparsity, stable beta sign across estimators.",
        "- `B_usable`: usable AGEB-level structure with stable sign, but weaker than a strong law.",
        "- `C_exploratory`: non-trivial signal exists, but weak enough that interpretation should remain exploratory.",
        "- `D_unfit`: too weak or too sparse to sustain substantive intra-urban interpretation.",
        "",
        "## Main Reading",
        "",
        f"- strong Y: `{total_by_class.get('A_strong', 0)}`",
        f"- usable Y: `{total_by_class.get('B_usable', 0)}`",
        f"- exploratory Y: `{total_by_class.get('C_exploratory', 0)}`",
        f"- unfit Y: `{total_by_class.get('D_unfit', 0)}`",
        "",
        "### Working Conclusion",
        "",
    ]
    if usable_rows:
        best = usable_rows[0]
        report_lines.extend(
            [
                f"- The highest usable AGEB-level Y is `{best['y_label']}` with `β = {_to_float(best['beta_ols']):+.3f}`, `R² = {_to_float(best['r2_ols']):.3f}`, coverage `{_pct(_to_float(best['coverage_rate']), 1)}`, and zero-rate `{_pct(_to_float(best['zero_rate']), 1)}`.",
                "- Most Y definitions still collapse or remain too weak to support a substantive intra-urban law with population as the sole denominator.",
            ]
        )
    else:
        report_lines.extend(
            [
                "- In this Guadalajara AGEB catalog, **none** of the tested Y definitions clears the usable threshold.",
                "- That means population alone is not organizing these AGEB-level economic counts strongly enough to sustain a defensible intra-urban scaling law.",
            ]
        )
    report_lines.extend(
        [
            "",
            "## Figure Guide",
            "",
            f"- [r2_rank_all.svg]({(figdir / 'r2_rank_all.svg').resolve()}): fit strength only.",
            f"- [coverage_rank_all.svg]({(figdir / 'coverage_rank_all.svg').resolve()}): how much of the city is actually represented by positive AGEB counts for each Y.",
            f"- [zero_rate_rank_all.svg]({(figdir / 'zero_rate_rank_all.svg').resolve()}): sparsity inside the city.",
            f"- [beta_rank_all.svg]({(figdir / 'beta_rank_all.svg').resolve()}): sign and magnitude of the intra-urban exponent.",
            f"- [fitability_class_summary.svg]({(figdir / 'fitability_class_summary.svg').resolve()}): final class counts.",
            "",
            "## Full Tables",
            "",
            f"- [ageb_y_fitability_full.csv]({(outdir / 'ageb_y_fitability_full.csv').resolve()})",
            f"- [family_fitability_summary.csv]({(outdir / 'family_fitability_summary.csv').resolve()})",
            f"- [y_fitability_usable.csv]({(outdir / 'y_fitability_usable.csv').resolve()})",
            f"- [y_fitability_unfit.csv]({(outdir / 'y_fitability_unfit.csv').resolve()})",
            "",
            "## Table Reading",
            "",
            "- `beta_ols`: intra-urban scaling exponent inside Guadalajara.",
            "- `r2_ols`: explanatory power of AGEB population alone for that Y.",
            "- `coverage_rate`: share of populated AGEBs with positive counts.",
            "- `zero_rate`: share of populated AGEBs with zero counts.",
            "- `beta_sign_stable`: whether OLS, robust, Poisson, and negative binomial keep the same sign.",
            "- `fitability_class`: the final scientific status of that Y for AGEB analysis.",
        ]
    )
    (outdir / "report.md").write_text("\n".join(report_lines), encoding="utf-8")
    print(json.dumps({"ok": True, "output_dir": str(outdir), "n_y": len(audit_rows)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

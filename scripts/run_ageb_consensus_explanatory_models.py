#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
import shutil
from pathlib import Path

from urban_sami.analysis.linear_models import compare_nested_models, ols_fit


BG = "#f8f6f1"
PANEL = "#fffdf8"
GRID = "#ddd6c8"
AXIS = "#8b8478"
TEXT = "#1f1f1f"
MUTED = "#625d54"
TEAL = "#0f766e"
BLUE = "#315c80"
RUST = "#b14d3b"
SANS = "Helvetica, Arial, sans-serif"
SERIF = "Georgia, 'Times New Roman', serif"


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


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _write_model_chart(path: Path, title: str, subtitle: str, rows: list[dict[str, object]]) -> Path:
    width = 1080
    left = 180
    right = 70
    top = 96
    bottom = 78
    row_h = 28
    height = top + len(rows) * row_h + bottom
    vals = [float(r["adj_r2"]) for r in rows]
    xmax = max(vals) * 1.1 if vals else 1.0

    def px(v: float) -> float:
        return left + (v / max(xmax, 1e-9)) * (width - left - right)

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">{subtitle}</text>',
    ]
    for i, row in enumerate(rows):
        y = top + i * row_h
        label = str(row["model"])
        adj = float(row["adj_r2"])
        body.append(f'<text x="{left-12}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{label}</text>')
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{px(adj):.2f}" y2="{y:.2f}" stroke="{TEAL}" stroke-width="8"/>')
        body.append(f'<text x="{px(adj)+8:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">adjR²={adj:.3f}</text>')
    return _svg(path, width, height, "".join(body))


def _fit_models(rows: list[dict[str, float]], *, add_core: bool) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    y = [math.log(max(r["est_count"], 1e-9)) for r in rows]
    names_designs = []
    base = [[1.0, math.log(max(r["population"], 1e-9))] for r in rows]
    names_designs.append(("M0 logN", base))
    dens = [[1.0, math.log(max(r["population"], 1e-9)), math.log(max(r["population_density"], 1e-9))] for r in rows]
    names_designs.append(("M1 logN+logDensity", dens))
    comp = [
        [
            1.0,
            math.log(max(r["population"], 1e-9)),
            r["share_81"],
            r["share_46"],
            r["share_31"],
            r["share_62"],
            r["share_54"],
            r["share_micro"],
            r["share_medium"],
        ]
        for r in rows
    ]
    names_designs.append(("M2 logN+composition", comp))
    full = [
        [
            1.0,
            math.log(max(r["population"], 1e-9)),
            math.log(max(r["population_density"], 1e-9)),
            r["share_81"],
            r["share_46"],
            r["share_31"],
            r["share_62"],
            r["share_54"],
            r["share_micro"],
            r["share_medium"],
        ]
        for r in rows
    ]
    names_designs.append(("M3 logN+logDensity+composition", full))
    if add_core:
        with_core = [
            [
                1.0,
                math.log(max(r["population"], 1e-9)),
                math.log(max(r["population_density"], 1e-9)),
                r["share_81"],
                r["share_46"],
                r["share_31"],
                r["share_62"],
                r["share_54"],
                r["share_micro"],
                r["share_medium"],
                r["in_core"],
            ]
            for r in rows
        ]
        inter = [
            [
                1.0,
                math.log(max(r["population"], 1e-9)),
                math.log(max(r["population_density"], 1e-9)),
                r["share_81"],
                r["share_46"],
                r["share_31"],
                r["share_62"],
                r["share_54"],
                r["share_micro"],
                r["share_medium"],
                r["in_core"],
                r["in_core"] * math.log(max(r["population"], 1e-9)),
            ]
            for r in rows
        ]
        names_designs.append(("M4 full+coreDummy", with_core))
        names_designs.append(("M5 full+coreDummy+coreLogN", inter))

    fitted = []
    fit_lookup = {}
    for name, design in names_designs:
        fit = ols_fit(design, y)
        fitted.append(
            {
                "model": name,
                "n_obs": fit.n_obs,
                "n_params": fit.n_params,
                "r2": fit.r2,
                "adj_r2": fit.adj_r2,
                "rss": fit.rss,
            }
        )
        fit_lookup[name] = fit

    nested = []
    for a, b in [("M0 logN", "M1 logN+logDensity"), ("M0 logN", "M2 logN+composition"), ("M1 logN+logDensity", "M3 logN+logDensity+composition")]:
        cmp = compare_nested_models(fit_lookup[a], fit_lookup[b])
        nested.append({"restricted": a, "full": b, "f_stat": cmp.f_stat, "df_num": cmp.df_num, "df_den": cmp.df_den, "p_value": cmp.p_value})
    if add_core:
        for a, b in [("M3 logN+logDensity+composition", "M4 full+coreDummy"), ("M4 full+coreDummy", "M5 full+coreDummy+coreLogN")]:
            cmp = compare_nested_models(fit_lookup[a], fit_lookup[b])
            nested.append({"restricted": a, "full": b, "f_stat": cmp.f_stat, "df_num": cmp.df_num, "df_den": cmp.df_den, "p_value": cmp.p_value})
    return fitted, nested


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "ageb-consensus-explanatory-models-guadalajara-2026-04-22"
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    features = _read_csv(root / "reports" / "ageb-subset-discovery-guadalajara-2026-04-22" / "ageb_feature_table.csv")
    consensus = _read_csv(root / "reports" / "ageb-unconstrained-subset-search-guadalajara-2026-04-22" / "consensus_members.csv")
    yrows = _read_csv(root / "reports" / "ageb-city-native-experiments-guadalajara-2026-04-22" / "ageb_y_unit_counts.csv")

    in_core = {r["unit_code"]: int(r["in_consensus_core"]) for r in consensus}
    share_map: dict[str, dict[str, float]] = {}
    totals: dict[str, float] = {}
    for r in yrows:
        code = r["unit_code"]
        fam = r["family"]
        cat = r["category"]
        val = _safe_float(r["y_value"])
        share_map.setdefault(code, {})
        if fam == "total" and cat == "all":
            totals[code] = val
        elif fam == "scian2" and cat in {"81", "46", "31", "62", "54"}:
            share_map[code][f"share_{cat}"] = val
        elif fam == "size_class" and cat in {"micro", "medium"}:
            share_map[code][f"share_{cat}"] = val

    rows = []
    for r in features:
        code = r["unit_code"]
        total = max(totals.get(code, 0.0), 1e-9)
        rows.append(
            {
                "unit_code": code,
                "in_core": float(in_core.get(code, 0)),
                "population": _safe_float(r["population"]),
                "est_count": _safe_float(r["est_count"]),
                "population_density": _safe_float(r["population_density"]),
                "share_81": share_map.get(code, {}).get("share_81", 0.0) / total,
                "share_46": share_map.get(code, {}).get("share_46", 0.0) / total,
                "share_31": share_map.get(code, {}).get("share_31", 0.0) / total,
                "share_62": share_map.get(code, {}).get("share_62", 0.0) / total,
                "share_54": share_map.get(code, {}).get("share_54", 0.0) / total,
                "share_micro": share_map.get(code, {}).get("share_micro", 0.0) / total,
                "share_medium": share_map.get(code, {}).get("share_medium", 0.0) / total,
            }
        )
    rows = [r for r in rows if r["population"] > 0 and r["est_count"] > 0]
    core_rows = [r for r in rows if r["in_core"] == 1.0]
    rest_rows = [r for r in rows if r["in_core"] == 0.0]

    all_models, all_nested = _fit_models(rows, add_core=True)
    core_models, core_nested = _fit_models(core_rows, add_core=False)
    rest_models, rest_nested = _fit_models(rest_rows, add_core=False)

    _write_csv(outdir / "all_models.csv", all_models, list(all_models[0].keys()))
    _write_csv(outdir / "all_nested_tests.csv", all_nested, list(all_nested[0].keys()))
    _write_csv(outdir / "core_models.csv", core_models, list(core_models[0].keys()))
    _write_csv(outdir / "core_nested_tests.csv", core_nested, list(core_nested[0].keys()))
    _write_csv(outdir / "rest_models.csv", rest_models, list(rest_models[0].keys()))
    _write_csv(outdir / "rest_nested_tests.csv", rest_nested, list(rest_nested[0].keys()))

    fig1 = _write_model_chart(
        figdir / "all_models.svg",
        "All AGEB models with and without core indicator",
        "This asks whether density, composition, and the core label explain the citywide breakdown.",
        all_models,
    )
    fig2 = _write_model_chart(
        figdir / "core_models.svg",
        "Consensus core explanatory models",
        "This asks what explains the clean law inside the selected core itself.",
        core_models,
    )
    fig3 = _write_model_chart(
        figdir / "rest_models.svg",
        "Rest-of-city explanatory models",
        "This asks whether the same variables recover structure outside the core.",
        rest_models,
    )
    _write_csv(
        figdir / "figures_manifest.csv",
        [
            {"figure_id": "all_models", "path": str(fig1.resolve()), "description": "All AGEB model comparison."},
            {"figure_id": "core_models", "path": str(fig2.resolve()), "description": "Consensus core model comparison."},
            {"figure_id": "rest_models", "path": str(fig3.resolve()), "description": "Rest-of-city model comparison."},
        ],
        ["figure_id", "path", "description"],
    )

    lines = [
        "# Explanatory Models for the AGEB Consensus Core",
        "",
        "We compare three explanatory layers:",
        "- population only",
        "- density",
        "- composition (SCIAN2 and size shares)",
        "",
        "## All AGEB",
    ]
    for row in all_models:
        lines.append(f"- `{row['model']}`: `adjR²={float(row['adj_r2']):.3f}`, `R²={float(row['r2']):.3f}`")
    lines.extend(["", "## Consensus core"])
    for row in core_models:
        lines.append(f"- `{row['model']}`: `adjR²={float(row['adj_r2']):.3f}`, `R²={float(row['r2']):.3f}`")
    lines.extend(["", "## Rest of city"])
    for row in rest_models:
        lines.append(f"- `{row['model']}`: `adjR²={float(row['adj_r2']):.3f}`, `R²={float(row['r2']):.3f}`")
    lines.extend(
        [
            "",
            "## Files",
            f"- [all_models.csv]({(outdir / 'all_models.csv').resolve()})",
            f"- [all_nested_tests.csv]({(outdir / 'all_nested_tests.csv').resolve()})",
            f"- [core_models.csv]({(outdir / 'core_models.csv').resolve()})",
            f"- [core_nested_tests.csv]({(outdir / 'core_nested_tests.csv').resolve()})",
            f"- [rest_models.csv]({(outdir / 'rest_models.csv').resolve()})",
            f"- [rest_nested_tests.csv]({(outdir / 'rest_nested_tests.csv').resolve()})",
            "",
            "## Figures",
            f"- [all_models.svg]({fig1.resolve()})",
            f"- [core_models.svg]({fig2.resolve()})",
            f"- [rest_models.svg]({fig3.resolve()})",
        ]
    )
    (outdir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

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
TEXT = "#1f1f1f"
TEAL = "#0f766e"
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


def _fit(rows: list[dict[str, float]], predictors: list[str], *, city_fe: bool):
    y = [math.log(max(float(r["est_total"]), 1e-9)) for r in rows]
    X = [[1.0] + [float(r[k]) for k in predictors] for r in rows]
    cities: list[str] = []
    if city_fe:
        cities = sorted({str(r["city_code"]) for r in rows})
        X2 = []
        for vec, row in zip(X, rows):
            ext = vec[:]
            for c in cities[1:]:
                ext.append(1.0 if row["city_code"] == c else 0.0)
            X2.append(ext)
        X = X2
    return ols_fit(X, y), cities


def _model_row(name: str, fit, predictors: list[str], cities: list[str]) -> dict[str, object]:
    return {
        "model": name,
        "n_obs": fit.n_obs,
        "n_params": fit.n_params,
        "r2": fit.r2,
        "adj_r2": fit.adj_r2,
        "rss": fit.rss,
        "predictors": ",".join(predictors),
        "city_fe_n": max(0, len(cities) - 1),
    }


def _coef_rows(model_name: str, fit, predictors: list[str], cities: list[str]) -> list[dict[str, object]]:
    names = ["intercept"] + predictors + [f"city_fe::{c}" for c in cities[1:]]
    out = []
    for name, coef, se in zip(names, fit.coefficients, fit.stderr):
        if name == "intercept" or name.startswith("city_fe::"):
            continue
        out.append({"model": model_name, "term": name, "coefficient": coef, "stderr": se, "t_approx": (coef / se) if se > 0 else 0.0})
    return out


def _write_bar(path: Path, title: str, rows: list[dict[str, object]]) -> Path:
    width = 1040
    left = 220
    right = 70
    top = 96
    bottom = 78
    row_h = 30
    height = top + len(rows) * row_h + bottom
    xmax = max(float(r["adj_r2"]) for r in rows) * 1.1

    def px(v: float) -> float:
        return left + (v / xmax) * (width - left - right)

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
    ]
    for i, row in enumerate(rows):
        y = top + i * row_h
        v = float(row["adj_r2"])
        body.append(f'<text x="{left-12}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{row["model"]}</text>')
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{px(v):.2f}" y2="{y:.2f}" stroke="{TEAL}" stroke-width="8"/>')
        body.append(f'<text x="{px(v)+8:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">{v:.3f}</text>')
    return _svg(path, width, height, ''.join(body))


def _run(label: str, rows: list[dict[str, float]], outdir: Path, *, city_fe: bool):
    for r in rows:
        r["log_N"] = math.log(max(float(r["population"]), 1e-9))
        r["log_area"] = math.log(max(float(r["area_km2"]), 1e-9))

    predictor_sets = {
        "D0 logN": ["log_N"],
        "D1 logN + logArea": ["log_N", "log_area"],
        "D2 logN + composition": ["log_N", "share_31", "share_46", "share_81", "share_62", "share_54", "share_micro", "share_medium"],
        "D3 logN + logArea + composition": ["log_N", "log_area", "share_31", "share_46", "share_81", "share_62", "share_54", "share_micro", "share_medium"],
    }
    fits = {}
    model_rows = []
    for name, predictors in predictor_sets.items():
        fit, cities = _fit(rows, predictors, city_fe=city_fe)
        fits[name] = (fit, predictors, cities)
        model_rows.append(_model_row(name, fit, predictors, cities))
    _write_csv(outdir / f"{label}_decomp_model_summary.csv", model_rows, list(model_rows[0].keys()))

    nested_rows = []
    for a, b in [("D0 logN", "D1 logN + logArea"), ("D0 logN", "D2 logN + composition"), ("D1 logN + logArea", "D3 logN + logArea + composition")]:
        cmp = compare_nested_models(fits[a][0], fits[b][0])
        nested_rows.append({"restricted": a, "full": b, "f_stat": cmp.f_stat, "df_num": cmp.df_num, "df_den": cmp.df_den, "p_value": cmp.p_value})
    _write_csv(outdir / f"{label}_decomp_nested_tests.csv", nested_rows, list(nested_rows[0].keys()))

    coefs = _coef_rows("D3 logN + logArea + composition", fits["D3 logN + logArea + composition"][0], predictor_sets["D3 logN + logArea + composition"], fits["D3 logN + logArea + composition"][2])
    _write_csv(outdir / f"{label}_decomp_coefficients.csv", coefs, list(coefs[0].keys()))

    if label == "city":
        eq = [{"equation": "city_area_decomp_law", "form": "log(Y_city)=alpha+beta log(N_city)+gamma log(A_city)+composition", "beta": next(float(r["coefficient"]) for r in coefs if r["term"]=="log_N"), "gamma": next(float(r["coefficient"]) for r in coefs if r["term"]=="log_area")}]
    else:
        eq = [{"equation": "ageb_area_decomp_law", "form": "log(Y_ageb)=alpha_city+beta log(N_ageb)+gamma log(A_ageb)+composition", "beta": next(float(r["coefficient"]) for r in coefs if r["term"]=="log_N"), "gamma": next(float(r["coefficient"]) for r in coefs if r["term"]=="log_area")}]
    _write_csv(outdir / f"{label}_decomp_equation.csv", eq, list(eq[0].keys()))
    fig = _write_bar(outdir / "figures" / f"{label}_decomp_adj_r2.svg", f"{label.upper()} N decomposition models", model_rows)
    return fig


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    source = root / "reports" / "multiscale-extended-power-law-2026-04-23"
    outdir = root / "reports" / "multiscale-n-decomposition-2026-04-23"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "figures").mkdir(parents=True, exist_ok=True)

    city_rows = _read_csv(source / "city_rows.csv")
    ageb_rows = _read_csv(source / "ageb_rows.csv")

    fig1 = _run("city", city_rows, outdir, city_fe=False)
    fig2 = _run("ageb", ageb_rows, outdir, city_fe=True)
    _write_csv(outdir / "figures" / "figures_manifest.csv", [
        {"figure_id": "city_decomp_adj_r2", "path": str(fig1.resolve()), "description": "City N decomposition models"},
        {"figure_id": "ageb_decomp_adj_r2", "path": str(fig2.resolve()), "description": "AGEB N decomposition models"},
    ], ["figure_id","path","description"])
    (outdir / "report.md").write_text(
        "\n".join([
            "# Multiscale N Decomposition",
            "",
            "This pack decomposes the scaling law by replacing density with explicit area terms.",
            "",
            "## Files",
            f"- [city_decomp_model_summary.csv]({(outdir / 'city_decomp_model_summary.csv').resolve()})",
            f"- [city_decomp_nested_tests.csv]({(outdir / 'city_decomp_nested_tests.csv').resolve()})",
            f"- [city_decomp_coefficients.csv]({(outdir / 'city_decomp_coefficients.csv').resolve()})",
            f"- [city_decomp_equation.csv]({(outdir / 'city_decomp_equation.csv').resolve()})",
            f"- [ageb_decomp_model_summary.csv]({(outdir / 'ageb_decomp_model_summary.csv').resolve()})",
            f"- [ageb_decomp_nested_tests.csv]({(outdir / 'ageb_decomp_nested_tests.csv').resolve()})",
            f"- [ageb_decomp_coefficients.csv]({(outdir / 'ageb_decomp_coefficients.csv').resolve()})",
            f"- [ageb_decomp_equation.csv]({(outdir / 'ageb_decomp_equation.csv').resolve()})",
            "",
            "## Figures",
            f"- [city_decomp_adj_r2.svg]({fig1.resolve()})",
            f"- [ageb_decomp_adj_r2.svg]({fig2.resolve()})",
        ]) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
from collections import defaultdict
from pathlib import Path

from run_single_city_ageb_experiment import _write_ageb_map
from urban_sami.artifacts.figures import write_scaling_scatter_figure
from urban_sami.modeling import compute_deviation_score


DOCKER_EXE = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DB_CONTAINER = "24-polisplexity-core-db-dev"
POSTGRES_USER = "postgres"
DB_NAME = "urban_sami_exp"

FITABILITY_PACK = "ageb-fitability-audit-guadalajara-2026-04-22"
SOURCE_PACK = "ageb-city-native-experiments-guadalajara-2026-04-22"
OUTPUT_PACK = "ageb-usable-map-pack-guadalajara-2026-04-22"


def _query_tsv(sql: str, columns: list[str]) -> list[dict[str, str]]:
    cmd = [
        DOCKER_EXE,
        "exec",
        "-i",
        DB_CONTAINER,
        "psql",
        "-U",
        POSTGRES_USER,
        "-d",
        DB_NAME,
        "-AtF",
        "\t",
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    ]
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    rows: list[dict[str, str]] = []
    for line in proc.stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        rows.append({col: (parts[idx] if idx < len(parts) else "") for idx, col in enumerate(columns)})
    return rows


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


def _fetch_ageb_features(city_code: str) -> list[dict]:
    rows = _query_tsv(
        f"""
        SELECT unit_code, unit_label, ST_AsGeoJSON(geom)
        FROM raw.admin_units
        WHERE level = 'ageb_u' AND city_code = '{city_code}'
        ORDER BY unit_code
        """.strip(),
        ["unit_code", "unit_label", "geom_json"],
    )
    return [
        {
            "type": "Feature",
            "properties": {"unit_code": row["unit_code"], "unit_label": row["unit_label"]},
            "geometry": json.loads(row["geom_json"]),
        }
        for row in rows
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate maps for usable AGEB Y in one city")
    parser.add_argument("--fitability-pack", default=FITABILITY_PACK)
    parser.add_argument("--source-pack", default=SOURCE_PACK)
    parser.add_argument("--output-pack", default=OUTPUT_PACK)
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    fit_dir = root / "reports" / args.fitability_pack
    source_dir = root / "reports" / args.source_pack
    outdir = root / "reports" / args.output_pack
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    fit_rows = _read_csv(fit_dir / "y_fitability_usable.csv")
    ols_rows = {row["y_key"]: row for row in _read_csv(source_dir / "ageb_y_ols_fits.csv")}
    unit_rows = _read_csv(source_dir / "ageb_y_unit_counts.csv")
    if not fit_rows or not unit_rows:
        raise SystemExit("Missing fitability or source AGEB catalog rows.")

    city_code = unit_rows[0]["city_code"]
    city_name = unit_rows[0]["city_name"]
    features = _fetch_ageb_features(city_code)
    unit_meta = {
        row["unit_code"]: {
            "unit_label": row["unit_label"],
            "population": _to_float(row["population"]),
        }
        for row in unit_rows
    }
    by_y_values: dict[str, dict[str, float]] = defaultdict(dict)
    by_y_labels: dict[str, str] = {}
    for row in unit_rows:
        by_y_values[row["y_key"]][row["unit_code"]] = _to_float(row["y_value"])
        by_y_labels[row["y_key"]] = row["category_label"]

    fit_rows = sorted(fit_rows, key=lambda row: (_to_float(row["r2_ols"]), _to_float(row["coverage_rate"])), reverse=True)
    manifest_rows = []
    summary_rows = []
    family_groups: dict[str, list[dict[str, str]]] = defaultdict(list)

    for row in fit_rows:
        y_key = row["y_key"]
        ols = ols_rows[y_key]
        alpha = _to_float(ols["alpha"])
        beta = _to_float(ols["beta"])
        resid_std = _to_float(ols["resid_std"])
        values = by_y_values[y_key]

        fit_points = []
        sami_lookup: dict[str, float] = {}
        score_rows = []
        for unit_code, meta in unit_meta.items():
            population = meta["population"]
            y_value = values.get(unit_code, 0.0)
            if population <= 0 or y_value <= 0:
                continue
            fit_points.append({"unit_code": unit_code, "population": population, "y": y_value})
            score = compute_deviation_score(y_value, population, alpha, beta, resid_std)
            sami_lookup[unit_code] = score.sami
            score_rows.append(
                {
                    "unit_code": unit_code,
                    "unit_label": meta["unit_label"],
                    "population": population,
                    "y_observed": y_value,
                    "y_expected": score.y_expected,
                    "epsilon_log": score.epsilon_log,
                    "sami": score.sami,
                    "z_residual": score.z_residual,
                }
            )
        score_rows.sort(key=lambda r: float(r["sami"]), reverse=True)

        family_dir = figdir / row["family"]
        family_dir.mkdir(parents=True, exist_ok=True)
        stem = y_key.replace("::", "__")
        map_path = _write_ageb_map(city_name, by_y_labels.get(y_key, row["category_label"]), features, sami_lookup, family_dir / f"{stem}_sami_map.svg")
        scatter_path = write_scaling_scatter_figure(
            [{"population": pt["population"], "y": pt["y"]} for pt in fit_points],
            family_dir / f"{stem}_scaling_scatter.svg",
            title=f"{city_name} AGEBs: {by_y_labels.get(y_key, row['category_label'])}",
            x_key="population",
            y_key="y",
            fit_alpha=alpha,
            fit_beta=beta,
            annotation=f"ols  β={beta:.3f}  R²={_to_float(row['r2_ols']):.3f}",
        )
        score_csv = family_dir / f"{stem}_ageb_scores.csv"
        _write_csv(score_csv, score_rows, list(score_rows[0].keys()) if score_rows else ["unit_code", "unit_label", "population", "y_observed", "y_expected", "epsilon_log", "sami", "z_residual"])

        family_groups[row["family"]].append(row)
        summary_rows.append(
            {
                "family": row["family"],
                "category_label": row["category_label"],
                "y_key": y_key,
                "fitability_class": row["fitability_class"],
                "beta_ols": row["beta_ols"],
                "r2_ols": row["r2_ols"],
                "coverage_rate": row["coverage_rate"],
                "zero_rate": row["zero_rate"],
                "map_path": str(map_path.resolve()),
                "scatter_path": str(scatter_path.resolve()),
                "scores_csv": str(score_csv.resolve()),
            }
        )
        manifest_rows.extend(
            [
                {"family": row["family"], "y_key": y_key, "category_label": row["category_label"], "artifact": "sami_map", "path": str(map_path.resolve())},
                {"family": row["family"], "y_key": y_key, "category_label": row["category_label"], "artifact": "scaling_scatter", "path": str(scatter_path.resolve())},
            ]
        )

    _write_csv(outdir / "usable_map_summary.csv", summary_rows, list(summary_rows[0].keys()) if summary_rows else ["family", "category_label", "y_key", "fitability_class", "beta_ols", "r2_ols", "coverage_rate", "zero_rate", "map_path", "scatter_path", "scores_csv"])
    _write_csv(figdir / "figures_manifest.csv", manifest_rows, ["family", "y_key", "category_label", "artifact", "path"])

    report_lines = [
        "# Guadalajara AGEB Usable Map Pack",
        "",
        f"- city: `{city_name}` (`{city_code}`)",
        f"- usable Y mapped: `{len(fit_rows)}`",
        f"- source fitability pack: [{args.fitability_pack}]({fit_dir.resolve()})",
        f"- source AGEB catalog: [{args.source_pack}]({source_dir.resolve()})",
        "",
        "## Reading Order",
        "",
    ]
    for idx, row in enumerate(fit_rows, start=1):
        stem = row["y_key"].replace("::", "__")
        report_lines.append(
            f"{idx}. `{row['category_label']}` | family `{row['family']}` | `β={_to_float(row['beta_ols']):+.3f}` | `R²={_to_float(row['r2_ols']):.3f}` | "
            f"[map]({(figdir / row['family'] / f'{stem}_sami_map.svg').resolve()}) | "
            f"[scatter]({(figdir / row['family'] / f'{stem}_scaling_scatter.svg').resolve()})"
        )
    report_lines.extend(
        [
            "",
            "## Family Folders",
            "",
        ]
    )
    for family, rows in sorted(family_groups.items()):
        report_lines.append(f"- `{family}`: `{len(rows)}` usable Y under [figures/{family}]({(figdir / family).resolve()})")
    (outdir / "report.md").write_text("\n".join(report_lines), encoding="utf-8")
    print(json.dumps({"ok": True, "output_dir": str(outdir), "n_maps": len(fit_rows)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

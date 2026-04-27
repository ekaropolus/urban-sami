#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
from collections import defaultdict
from pathlib import Path
from statistics import median

from run_denue_y_state_scientific_analysis import BLUE, SCIAN2_LABELS, write_ranked_metric_chart
from urban_sami.indicators.denue import sector_prefix, size_class_from_per_ocu
from urban_sami.modeling import fit_by_name


DOCKER_EXE = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DB_CONTAINER = "24-polisplexity-core-db-dev"
POSTGRES_USER = "postgres"
DB_NAME = "urban_sami_exp"

FIT_METHODS = ("ols", "robust", "poisson", "negbin", "auto")


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


def _safe_key(text: str) -> str:
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
        .replace("+", "plus")
        .replace(".", "_")
        .replace(",", "_")
        .replace("(", "")
        .replace(")", "")
    )


def _coverage_tier(n_obs: int, total_units: int) -> str:
    rate = (n_obs / total_units) if total_units else 0.0
    if rate >= 0.90:
        return "near-universal"
    if rate >= 0.70:
        return "broad"
    if rate >= 0.40:
        return "partial"
    return "sparse"


def _family_label(family: str) -> str:
    return {
        "total": "Total",
        "scian2": "SCIAN 2-digit",
        "per_ocu": "DENUE size band",
        "size_class": "Derived size class",
        "scian2_size_class": "SCIAN 2-digit x derived size class",
    }.get(family, family)


def _category_label(family: str, category: str) -> str:
    if family == "total":
        return "all establishments"
    if family == "scian2":
        return f"{category} {SCIAN2_LABELS.get(category, '')}".strip()
    if family == "per_ocu":
        return category
    if family == "size_class":
        return category
    if family == "scian2_size_class":
        scian2, size_class = category.split("|", 1)
        return f"{scian2} {SCIAN2_LABELS.get(scian2, '')} | {size_class}".strip()
    return category


def _fetch_ageb_units(city_code: str) -> list[dict[str, str]]:
    return _query_tsv(
        f"""
        SELECT
            unit_code,
            unit_label,
            city_code,
            city_name,
            COALESCE(population, 0)::text AS population,
            COALESCE(households, 0)::text AS households
        FROM raw.admin_units
        WHERE level = 'ageb_u' AND city_code = '{city_code}'
        ORDER BY unit_code
        """.strip(),
        ["unit_code", "unit_label", "city_code", "city_name", "population", "households"],
    )


def _fetch_assigned_points(city_code: str) -> list[dict[str, str]]:
    return _query_tsv(
        f"""
        WITH ageb AS (
            SELECT unit_code, geom
            FROM raw.admin_units
            WHERE level = 'ageb_u' AND city_code = '{city_code}'
        ),
        points AS (
            SELECT
                scian_code,
                per_ocu,
                ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geom
            FROM raw.denue_establishments
            WHERE city_code = '{city_code}'
              AND longitude IS NOT NULL
              AND latitude IS NOT NULL
        )
        SELECT
            a.unit_code,
            p.scian_code,
            p.per_ocu
        FROM ageb a
        JOIN points p
          ON ST_Covers(a.geom, p.geom)
        """.strip(),
        ["unit_code", "scian_code", "per_ocu"],
    )


def _build_family_maps(ageb_units: list[dict[str, str]], points: list[dict[str, str]]) -> dict[str, dict[str, dict[str, int]]]:
    families: dict[str, dict[str, dict[str, int]]] = {
        "total": {"all": defaultdict(int)},
        "scian2": defaultdict(lambda: defaultdict(int)),
        "per_ocu": defaultdict(lambda: defaultdict(int)),
        "size_class": defaultdict(lambda: defaultdict(int)),
        "scian2_size_class": defaultdict(lambda: defaultdict(int)),
    }
    for point in points:
        unit_code = point["unit_code"].strip()
        scian2 = sector_prefix(point["scian_code"])
        per_ocu = point["per_ocu"].strip()
        size_class = size_class_from_per_ocu(per_ocu)

        families["total"]["all"][unit_code] += 1
        if scian2:
            families["scian2"][scian2][unit_code] += 1
        if per_ocu:
            families["per_ocu"][per_ocu][unit_code] += 1
        if size_class:
            families["size_class"][size_class][unit_code] += 1
        if scian2 and size_class:
            families["scian2_size_class"][f"{scian2}|{size_class}"][unit_code] += 1

    normalized: dict[str, dict[str, dict[str, int]]] = {}
    for family, family_map in families.items():
        normalized[family] = {category: dict(counts) for category, counts in family_map.items()}
    return normalized


def _fit_category(
    ageb_units: list[dict[str, str]],
    *,
    family: str,
    category: str,
    category_label: str,
    total_count: int,
    share_of_total: float,
    ageb_counts: dict[str, int],
) -> tuple[list[dict], dict]:
    filtered = [
        row
        for row in ageb_units
        if _to_float(row["population"]) > 0.0 and float(ageb_counts.get(row["unit_code"], 0)) > 0.0
    ]
    n_obs = len(filtered)
    catalog = {
        "level": "ageb_u",
        "family": family,
        "family_label": _family_label(family),
        "category": category,
        "category_label": category_label,
        "y_key": "all" if family == "total" else f"{family}::{_safe_key(category)}",
        "total_count": total_count,
        "share_of_total": share_of_total,
        "positive_units": len(ageb_counts),
        "zero_units": len(ageb_units) - len(ageb_counts),
        "n_obs_fit": n_obs,
        "coverage_tier": _coverage_tier(n_obs, len(ageb_units)),
    }
    if n_obs < 2:
        return [], catalog

    y = [float(ageb_counts[row["unit_code"]]) for row in filtered]
    n = [_to_float(row["population"]) for row in filtered]
    fit_rows: list[dict] = []
    for method in FIT_METHODS:
        fit = fit_by_name(y, n, method)
        fit_rows.append(
            {
                "level": "ageb_u",
                "family": family,
                "family_label": _family_label(family),
                "category": category,
                "category_label": category_label,
                "y_key": catalog["y_key"],
                "fit_method": method,
                "n_obs": n_obs,
                "coverage_tier": catalog["coverage_tier"],
                "total_count": total_count,
                "share_of_total": share_of_total,
                "alpha": fit.alpha,
                "beta": fit.beta,
                "r2": fit.r2,
                "resid_std": fit.residual_std,
            }
        )
    return fit_rows, catalog


def _family_summary_rows(ols_rows: list[dict], catalog_rows: list[dict]) -> list[dict]:
    catalog_by_family: dict[str, list[dict]] = defaultdict(list)
    fits_by_family: dict[str, list[dict]] = defaultdict(list)
    for row in catalog_rows:
        catalog_by_family[row["family"]].append(row)
    for row in ols_rows:
        fits_by_family[row["family"]].append(row)
    out: list[dict] = []
    for family, family_catalog in sorted(catalog_by_family.items()):
        family_fits = fits_by_family.get(family, [])
        family_fits_sorted_share = sorted(family_fits, key=lambda item: float(item["share_of_total"]), reverse=True)
        family_fits_sorted_r2 = sorted(family_fits, key=lambda item: float(item["r2"]), reverse=True)
        family_fits_sorted_beta = sorted(family_fits, key=lambda item: float(item["beta"]), reverse=True)
        out.append(
            {
                "family": family,
                "family_label": _family_label(family),
                "categories_cataloged": len(family_catalog),
                "categories_fitted": len(family_fits),
                "median_n_obs": median([int(row["n_obs"]) for row in family_fits]) if family_fits else 0,
                "median_beta_ols": median([float(row["beta"]) for row in family_fits]) if family_fits else 0.0,
                "median_r2_ols": median([float(row["r2"]) for row in family_fits]) if family_fits else 0.0,
                "top_share_category": family_fits_sorted_share[0]["category_label"] if family_fits_sorted_share else "",
                "top_share_pct": family_fits_sorted_share[0]["share_of_total"] if family_fits_sorted_share else 0.0,
                "top_r2_category": family_fits_sorted_r2[0]["category_label"] if family_fits_sorted_r2 else "",
                "top_r2_value": family_fits_sorted_r2[0]["r2"] if family_fits_sorted_r2 else 0.0,
                "top_beta_category": family_fits_sorted_beta[0]["category_label"] if family_fits_sorted_beta else "",
                "top_beta_value": family_fits_sorted_beta[0]["beta"] if family_fits_sorted_beta else 0.0,
                "bottom_beta_category": family_fits_sorted_beta[-1]["category_label"] if family_fits_sorted_beta else "",
                "bottom_beta_value": family_fits_sorted_beta[-1]["beta"] if family_fits_sorted_beta else 0.0,
            }
        )
    return out


def _add_rank_fields(rows: list[dict]) -> list[dict]:
    ranked = [dict(row) for row in rows]
    rank_specs = [
        ("share_rank", "share_of_total", True),
        ("n_obs_rank", "n_obs", True),
        ("total_count_rank", "total_count", True),
        ("beta_rank_high", "beta", True),
        ("beta_rank_low", "beta", False),
        ("r2_rank", "r2", True),
    ]
    for rank_field, metric_field, descending in rank_specs:
        ordered = sorted(ranked, key=lambda row: _to_float(row[metric_field]), reverse=descending)
        for idx, row in enumerate(ordered, start=1):
            row[rank_field] = idx
    return ranked


def _write_family_dossiers(base_dir: Path, ols_rows: list[dict]) -> tuple[list[dict], list[dict]]:
    manifest: list[dict] = []
    family_index_rows: list[dict] = []
    family_rows: dict[str, list[dict]] = defaultdict(list)
    for row in ols_rows:
        family_rows[row["family"]].append(row)

    for family, rows in sorted(family_rows.items()):
        family_dir = base_dir / family
        family_dir.mkdir(parents=True, exist_ok=True)
        complete_rows = _add_rank_fields(rows)
        rows_by_share = sorted(complete_rows, key=lambda item: float(item["share_of_total"]), reverse=True)
        title_prefix = _family_label(family)
        stats_path = family_dir / "complete_statistics.csv"
        _write_csv(stats_path, rows_by_share, list(rows_by_share[0].keys()) if rows_by_share else [])
        family_index_rows.append(
            {
                "family": family,
                "family_label": title_prefix,
                "category_count": len(rows_by_share),
                "folder": str(family_dir.resolve()),
                "complete_statistics_csv": str(stats_path.resolve()),
            }
        )

        share_path = family_dir / "share_rank_all.svg"
        write_ranked_metric_chart(
            share_path,
            title=f"{title_prefix}: AGEB-level category weight",
            subtitle="All categories included. Ordered by share of total establishments in Guadalajara AGEBs.",
            rows=rows_by_share,
            metric_field="share_of_total",
            order_field="share_of_total",
            value_formatter=lambda value: _pct(value),
        )
        manifest.append({"family": family, "family_label": title_prefix, "figure_type": "share_rank_all", "path": str(share_path.resolve())})

        nobs_path = family_dir / "n_obs_rank_all.svg"
        write_ranked_metric_chart(
            nobs_path,
            title=f"{title_prefix}: AGEB coverage by category",
            subtitle="All categories included. Ordered by number of AGEBs entering the fit.",
            rows=rows_by_share,
            metric_field="n_obs",
            order_field="n_obs",
            value_formatter=lambda value: f"{int(round(value))}",
        )
        manifest.append({"family": family, "family_label": title_prefix, "figure_type": "n_obs_rank_all", "path": str(nobs_path.resolve())})

        beta_path = family_dir / "beta_rank_all.svg"
        write_ranked_metric_chart(
            beta_path,
            title=f"{title_prefix}: AGEB-level OLS beta",
            subtitle="All categories included. Ordered by beta.",
            rows=rows_by_share,
            metric_field="beta",
            order_field="beta",
            ref_line=0.0,
            value_formatter=lambda value: f"{value:+.3f}",
        )
        manifest.append({"family": family, "family_label": title_prefix, "figure_type": "beta_rank_all", "path": str(beta_path.resolve())})

        r2_path = family_dir / "r2_rank_all.svg"
        write_ranked_metric_chart(
            r2_path,
            title=f"{title_prefix}: AGEB-level OLS R²",
            subtitle="All categories included. Ordered by R².",
            rows=rows_by_share,
            metric_field="r2",
            order_field="r2",
            color=BLUE,
            fixed_range=(0.0, 1.0),
            value_formatter=lambda value: f"{value:.3f}",
        )
        manifest.append({"family": family, "family_label": title_prefix, "figure_type": "r2_rank_all", "path": str(r2_path.resolve())})
    return manifest, family_index_rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Run city-native AGEB Y catalog for one city")
    parser.add_argument("--city-code", default="14039")
    parser.add_argument("--output-dir", type=Path, default=Path("reports/ageb-city-native-experiments-guadalajara-2026-04-22"))
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    output_dir = args.output_dir if args.output_dir.is_absolute() else root / args.output_dir
    figdir = output_dir / "figures"
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    ageb_units = _fetch_ageb_units(str(args.city_code))
    for row in ageb_units:
        row["population"] = _to_float(row["population"])
        row["households"] = _to_float(row["households"])
    points = _fetch_assigned_points(str(args.city_code))
    city_name = ageb_units[0]["city_name"] if ageb_units else str(args.city_code)

    family_maps = _build_family_maps(ageb_units, points)
    total_establishments = len(points)

    catalog_rows: list[dict] = []
    fit_rows: list[dict] = []
    unit_counts_path = output_dir / "ageb_y_unit_counts.csv"
    with unit_counts_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "unit_code",
                "unit_label",
                "city_code",
                "city_name",
                "population",
                "households",
                "family",
                "family_label",
                "category",
                "category_label",
                "y_key",
                "y_value",
            ],
        )
        writer.writeheader()
        for family, family_map in family_maps.items():
            for category, ageb_counts in sorted(
                family_map.items(),
                key=lambda item: (sum(item[1].values()), item[0]),
                reverse=True,
            ):
                total_count = int(sum(ageb_counts.values()))
                share_of_total = (float(total_count) / float(total_establishments)) if total_establishments > 0 else 0.0
                category_label = _category_label(family, category)
                local_fit_rows, catalog_row = _fit_category(
                    ageb_units,
                    family=family,
                    category=category,
                    category_label=category_label,
                    total_count=total_count,
                    share_of_total=share_of_total,
                    ageb_counts=ageb_counts,
                )
                catalog_rows.append(catalog_row)
                fit_rows.extend(local_fit_rows)
                y_key = catalog_row["y_key"]
                for ageb in ageb_units:
                    y_value = int(ageb_counts.get(ageb["unit_code"], 0))
                    if y_value <= 0:
                        continue
                    writer.writerow(
                        {
                            "unit_code": ageb["unit_code"],
                            "unit_label": ageb["unit_label"],
                            "city_code": ageb["city_code"],
                            "city_name": ageb["city_name"],
                            "population": ageb["population"],
                            "households": ageb["households"],
                            "family": family,
                            "family_label": _family_label(family),
                            "category": category,
                            "category_label": category_label,
                            "y_key": y_key,
                            "y_value": y_value,
                        }
                    )

    catalog_rows.sort(key=lambda row: (row["family"], -int(row["total_count"]), row["category"]))
    fit_rows.sort(key=lambda row: (row["family"], row["y_key"], row["fit_method"]))
    best_by_key: dict[str, dict] = {}
    for row in fit_rows:
        current = best_by_key.get(row["y_key"])
        if current is None or float(row["r2"]) > float(current["r2"]):
            best_by_key[row["y_key"]] = row
    best_rows = sorted(best_by_key.values(), key=lambda row: (row["family"], -float(row["r2"]), row["category"]))
    ols_rows = [row for row in fit_rows if row["fit_method"] == "ols"]
    family_summary = _family_summary_rows(ols_rows, catalog_rows)

    _write_csv(output_dir / "ageb_y_catalog.csv", catalog_rows, list(catalog_rows[0].keys()) if catalog_rows else [])
    _write_csv(output_dir / "ageb_y_all_fits.csv", fit_rows, list(fit_rows[0].keys()) if fit_rows else [])
    _write_csv(output_dir / "ageb_y_best_fits.csv", best_rows, list(best_rows[0].keys()) if best_rows else [])
    _write_csv(output_dir / "ageb_y_ols_fits.csv", ols_rows, list(ols_rows[0].keys()) if ols_rows else [])
    _write_csv(output_dir / "ageb_y_family_summary.csv", family_summary, list(family_summary[0].keys()) if family_summary else [])

    family_dir = output_dir / "families"
    figure_manifest, family_index_rows = _write_family_dossiers(family_dir, ols_rows)
    _write_csv(output_dir / "family_index.csv", family_index_rows, ["family", "family_label", "category_count", "folder", "complete_statistics_csv"])
    _write_csv(figdir / "figures_manifest.csv", figure_manifest, ["family", "family_label", "figure_type", "path"])

    total_ols = next((row for row in ols_rows if row["y_key"] == "all"), None)
    total_best = next((row for row in best_rows if row["y_key"] == "all"), None)
    report_lines = [
        "# AGEB City-Native Y Experiment",
        "",
        f"- city: `{args.city_code}` `{city_name}`",
        f"- AGEB units: `{len(ageb_units)}`",
        f"- assigned establishments: `{total_establishments}`",
        f"- Y catalog rows: `{len(catalog_rows)}`",
        f"- Y fitted rows (OLS): `{len(ols_rows)}`",
        "",
        "Families included:",
        "- total",
        "- scian2",
        "- per_ocu",
        "- size_class",
        "- scian2 x size_class",
        "",
        "Outputs:",
        f"- [ageb_y_unit_counts.csv]({unit_counts_path.resolve()})",
        f"- [ageb_y_catalog.csv]({(output_dir / 'ageb_y_catalog.csv').resolve()})",
        f"- [ageb_y_all_fits.csv]({(output_dir / 'ageb_y_all_fits.csv').resolve()})",
        f"- [ageb_y_ols_fits.csv]({(output_dir / 'ageb_y_ols_fits.csv').resolve()})",
        f"- [ageb_y_best_fits.csv]({(output_dir / 'ageb_y_best_fits.csv').resolve()})",
        f"- [ageb_y_family_summary.csv]({(output_dir / 'ageb_y_family_summary.csv').resolve()})",
        f"- [family_index.csv]({(output_dir / 'family_index.csv').resolve()})",
        "",
        "Total baseline:",
        f"- OLS total: `beta = {float(total_ols['beta']):+.3f}`, `R2 = {float(total_ols['r2']):.3f}`, `n = {int(total_ols['n_obs'])}`" if total_ols else "- OLS total unavailable",
        f"- Best total fit: `{total_best['fit_method']}` with `R2 = {float(total_best['r2']):.3f}`" if total_best else "- Best total fit unavailable",
    ]
    (output_dir / "report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    report_json = {
        "city_code": str(args.city_code),
        "city_name": city_name,
        "ageb_units": len(ageb_units),
        "assigned_establishments": total_establishments,
        "catalog_rows": len(catalog_rows),
        "ols_rows": len(ols_rows),
        "best_rows": len(best_rows),
    }
    (output_dir / "report.json").write_text(json.dumps(report_json, indent=2), encoding="utf-8")
    print(json.dumps(report_json, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

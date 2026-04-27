#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import shutil
import subprocess
from collections import defaultdict
from pathlib import Path
from statistics import median

from run_denue_y_state_scientific_analysis import BLUE, SCIAN2_LABELS, write_ranked_metric_chart
from urban_sami.indicators.denue import size_class_from_per_ocu
from urban_sami.modeling import fit_by_name


DOCKER_EXE = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DB_CONTAINER = "24-polisplexity-core-db-dev"
POSTGRES_USER = "postgres"
DB_NAME = "urban_sami_exp"

FIT_METHODS = ("ols", "robust", "poisson", "negbin", "auto")
STABLE_N_OBS_THRESHOLD = 500


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


def _fmt(value: float, digits: int = 4) -> str:
    return f"{value:.{digits}f}"


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


def _coverage_tier(n_obs: int) -> str:
    if n_obs >= 1000:
        return "broad"
    if n_obs >= 500:
        return "stable"
    if n_obs >= 100:
        return "moderate"
    if n_obs >= 30:
        return "sparse"
    if n_obs >= 2:
        return "very_sparse"
    return "insufficient"


def _category_label(family: str, category: str) -> str:
    if family == "total":
        return "all establishments"
    if family == "scian2":
        return f"{category} {SCIAN2_LABELS.get(category, '')}".strip()
    if family == "per_ocu":
        return category
    if family == "size_class":
        return category
    if family == "scian2_per_ocu":
        scian2, per_ocu = category.split("|", 1)
        return f"{scian2} {SCIAN2_LABELS.get(scian2, '')} | {per_ocu}".strip()
    if family == "scian2_size_class":
        scian2, size_class = category.split("|", 1)
        return f"{scian2} {SCIAN2_LABELS.get(scian2, '')} | {size_class}".strip()
    return category


def _family_label(family: str) -> str:
    return {
        "total": "Total",
        "scian2": "SCIAN 2-digit",
        "scian3": "SCIAN 3-digit",
        "scian4": "SCIAN 4-digit",
        "scian6": "SCIAN 6-digit",
        "per_ocu": "DENUE size band",
        "size_class": "Derived size class",
        "scian2_per_ocu": "SCIAN 2-digit x DENUE size band",
        "scian2_size_class": "SCIAN 2-digit x derived size class",
    }.get(family, family)


def _fetch_city_units() -> list[dict]:
    rows = _query_tsv(
        """
        SELECT
            p.unit_code AS city_code,
            p.unit_label AS city_name,
            SUBSTRING(p.unit_code FROM 1 FOR 2) AS state_code,
            COALESCE(p.population, 0)::text AS population,
            COALESCE(p.households, 0)::text AS households
        FROM raw.population_units AS p
        WHERE p.level = 'city' AND p.country_code = 'MX'
        ORDER BY p.unit_code
        """.strip(),
        ["city_code", "city_name", "state_code", "population", "households"],
    )
    for row in rows:
        row["population"] = _to_float(row["population"])
        row["households"] = _to_float(row["households"])
    return rows


def _fetch_overview() -> dict[str, float]:
    row = _query_tsv(
        """
        SELECT
            (SELECT COUNT(*) FROM raw.denue_establishments WHERE city_code <> '')::text,
            (SELECT COUNT(DISTINCT city_code) FROM raw.denue_establishments WHERE city_code <> '')::text,
            (SELECT COUNT(DISTINCT SUBSTRING(scian_code FROM 1 FOR 2)) FROM raw.denue_establishments WHERE city_code <> '' AND char_length(scian_code) >= 2)::text,
            (SELECT COUNT(DISTINCT SUBSTRING(scian_code FROM 1 FOR 3)) FROM raw.denue_establishments WHERE city_code <> '' AND char_length(scian_code) >= 3)::text,
            (SELECT COUNT(DISTINCT SUBSTRING(scian_code FROM 1 FOR 4)) FROM raw.denue_establishments WHERE city_code <> '' AND char_length(scian_code) >= 4)::text,
            (SELECT COUNT(DISTINCT scian_code) FROM raw.denue_establishments WHERE city_code <> '' AND char_length(scian_code) >= 6)::text,
            (SELECT COUNT(DISTINCT per_ocu) FROM raw.denue_establishments WHERE city_code <> '' AND per_ocu <> '')::text
        """.strip(),
        [
            "establishments_total",
            "distinct_cities_with_denue",
            "distinct_scian2",
            "distinct_scian3",
            "distinct_scian4",
            "distinct_scian6",
            "distinct_per_ocu",
        ],
    )[0]
    return {key: _to_float(value) for key, value in row.items()}


def _fetch_city_totals() -> dict[str, dict[str, int]]:
    rows = _query_tsv(
        """
        SELECT city_code, COUNT(*)::text AS est_count
        FROM raw.denue_establishments
        WHERE city_code <> ''
        GROUP BY city_code
        ORDER BY city_code
        """.strip(),
        ["city_code", "est_count"],
    )
    return {"all": {row["city_code"]: int(row["est_count"]) for row in rows}}


def _fetch_category_family(sql_expr: str, where_sql: str) -> dict[str, dict[str, int]]:
    rows = _query_tsv(
        f"""
        SELECT
            city_code,
            {sql_expr} AS category,
            COUNT(*)::text AS est_count
        FROM raw.denue_establishments
        WHERE city_code <> ''
          AND {where_sql}
        GROUP BY city_code, category
        ORDER BY category, city_code
        """.strip(),
        ["city_code", "category", "est_count"],
    )
    out: dict[str, dict[str, int]] = defaultdict(dict)
    for row in rows:
        category = str(row["category"]).strip()
        if not category:
            continue
        out[category][row["city_code"]] = int(row["est_count"])
    return dict(out)


def _fetch_cross_family() -> tuple[dict[str, dict[str, int]], dict[str, dict[str, int]]]:
    rows = _query_tsv(
        """
        SELECT
            city_code,
            SUBSTRING(scian_code FROM 1 FOR 2) AS scian2,
            per_ocu,
            COUNT(*)::text AS est_count
        FROM raw.denue_establishments
        WHERE city_code <> ''
          AND char_length(scian_code) >= 2
          AND per_ocu <> ''
        GROUP BY city_code, scian2, per_ocu
        ORDER BY scian2, per_ocu, city_code
        """.strip(),
        ["city_code", "scian2", "per_ocu", "est_count"],
    )
    raw_cross: dict[str, dict[str, int]] = defaultdict(dict)
    size_cross: dict[str, dict[str, int]] = defaultdict(dict)
    for row in rows:
        scian2 = row["scian2"].strip()
        per_ocu = row["per_ocu"].strip()
        city_code = row["city_code"].strip()
        count = int(row["est_count"])
        if not scian2 or not per_ocu:
            continue
        raw_key = f"{scian2}|{per_ocu}"
        raw_cross[raw_key][city_code] = raw_cross[raw_key].get(city_code, 0) + count
        size_class = size_class_from_per_ocu(per_ocu)
        size_key = f"{scian2}|{size_class}"
        size_cross[size_key][city_code] = size_cross[size_key].get(city_code, 0) + count
    return dict(raw_cross), dict(size_cross)


def _derive_size_class_family(per_ocu_family: dict[str, dict[str, int]]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = defaultdict(dict)
    for per_ocu, city_counts in per_ocu_family.items():
        size_class = size_class_from_per_ocu(per_ocu)
        for city_code, count in city_counts.items():
            out[size_class][city_code] = out[size_class].get(city_code, 0) + count
    return dict(out)


def _fit_category(
    city_units: list[dict],
    *,
    family: str,
    category: str,
    category_label: str,
    total_count: int,
    share_of_total: float,
    city_counts: dict[str, int],
) -> tuple[list[dict], dict]:
    filtered = [
        row
        for row in city_units
        if row["population"] > 0.0 and float(city_counts.get(row["city_code"], 0)) > 0.0
    ]
    n_obs = len(filtered)
    catalog = {
        "level": "city",
        "family": family,
        "family_label": _family_label(family),
        "category": category,
        "category_label": category_label,
        "y_key": "all" if family == "total" else f"{family}::{_safe_key(category)}",
        "total_count": total_count,
        "share_of_total": share_of_total,
        "positive_cities": len(city_counts),
        "zero_cities": len(city_units) - len(city_counts),
        "n_obs_fit": n_obs,
        "coverage_tier": _coverage_tier(n_obs),
    }
    if n_obs < 2:
        return [], catalog
    y = [float(city_counts[row["city_code"]]) for row in filtered]
    n = [float(row["population"]) for row in filtered]
    fit_rows: list[dict] = []
    for method in FIT_METHODS:
        fit = fit_by_name(y, n, method)
        fit_rows.append(
            {
                "level": "city",
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


def _build_family_maps() -> dict[str, dict[str, dict[str, int]]]:
    per_ocu = _fetch_category_family("per_ocu", "per_ocu <> ''")
    scian2_per_ocu, scian2_size_class = _fetch_cross_family()
    return {
        "total": _fetch_city_totals(),
        "scian2": _fetch_category_family("SUBSTRING(scian_code FROM 1 FOR 2)", "char_length(scian_code) >= 2"),
        "scian3": _fetch_category_family("SUBSTRING(scian_code FROM 1 FOR 3)", "char_length(scian_code) >= 3"),
        "scian4": _fetch_category_family("SUBSTRING(scian_code FROM 1 FOR 4)", "char_length(scian_code) >= 4"),
        "scian6": _fetch_category_family("scian_code", "char_length(scian_code) >= 6"),
        "per_ocu": per_ocu,
        "size_class": _derive_size_class_family(per_ocu),
        "scian2_per_ocu": scian2_per_ocu,
        "scian2_size_class": scian2_size_class,
    }


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
            title=f"{title_prefix}: city-level category weight",
            subtitle="All categories included. Ordered by share of total establishments.",
            rows=rows_by_share,
            metric_field="share_of_total",
            order_field="share_of_total",
            value_formatter=lambda value: _pct(value),
        )
        manifest.append({"family": family, "family_label": title_prefix, "figure_type": "share_rank_all", "path": str(share_path.resolve())})

        nobs_path = family_dir / "n_obs_rank_all.svg"
        write_ranked_metric_chart(
            nobs_path,
            title=f"{title_prefix}: city coverage by category",
            subtitle="All categories included. Ordered by number of cities entering the fit.",
            rows=rows_by_share,
            metric_field="n_obs",
            order_field="n_obs",
            value_formatter=lambda value: f"{int(round(value))}",
        )
        manifest.append({"family": family, "family_label": title_prefix, "figure_type": "n_obs_rank_all", "path": str(nobs_path.resolve())})

        beta_path = family_dir / "beta_rank_all.svg"
        write_ranked_metric_chart(
            beta_path,
            title=f"{title_prefix}: city-level OLS beta",
            subtitle="All categories included. Ordered by beta. Reference line at β = 1.",
            rows=rows_by_share,
            metric_field="beta",
            order_field="beta",
            ref_line=1.0,
            value_formatter=lambda value: f"{value:.3f}",
        )
        manifest.append({"family": family, "family_label": title_prefix, "figure_type": "beta_rank_all", "path": str(beta_path.resolve())})

        r2_path = family_dir / "r2_rank_all.svg"
        write_ranked_metric_chart(
            r2_path,
            title=f"{title_prefix}: city-level OLS R²",
            subtitle="All categories included. Ordered by R². Beta is not mixed into this figure.",
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
    root = Path(__file__).resolve().parents[1]
    output_dir = root / "reports" / "denue-y-city-native-experiments-2026-04-21"
    figdir = output_dir / "figures"
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    city_units = _fetch_city_units()
    overview = _fetch_overview()
    total_establishments = int(overview["establishments_total"])
    family_maps = _build_family_maps()

    catalog_rows: list[dict] = []
    fit_rows: list[dict] = []
    unit_counts_path = output_dir / "city_y_unit_counts.csv"
    with unit_counts_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "city_code",
                "city_name",
                "state_code",
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
            for category, city_counts in sorted(
                family_map.items(),
                key=lambda item: (sum(item[1].values()), item[0]),
                reverse=True,
            ):
                total_count = int(sum(city_counts.values()))
                share_of_total = (float(total_count) / float(total_establishments)) if total_establishments > 0 else 0.0
                category_label = _category_label(family, category)
                local_fit_rows, catalog_row = _fit_category(
                    city_units,
                    family=family,
                    category=category,
                    category_label=category_label,
                    total_count=total_count,
                    share_of_total=share_of_total,
                    city_counts=city_counts,
                )
                catalog_rows.append(catalog_row)
                fit_rows.extend(local_fit_rows)
                y_key = catalog_row["y_key"]
                for city in city_units:
                    y_value = int(city_counts.get(city["city_code"], 0))
                    if y_value <= 0:
                        continue
                    writer.writerow(
                        {
                            "city_code": city["city_code"],
                            "city_name": city["city_name"],
                            "state_code": city["state_code"],
                            "population": city["population"],
                            "households": city["households"],
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
    best_rows: list[dict] = []
    best_by_key: dict[str, dict] = {}
    for row in fit_rows:
        current = best_by_key.get(row["y_key"])
        if current is None or float(row["r2"]) > float(current["r2"]):
            best_by_key[row["y_key"]] = row
    best_rows = sorted(best_by_key.values(), key=lambda row: (row["family"], -float(row["r2"]), row["category"]))
    ols_rows = [row for row in fit_rows if row["fit_method"] == "ols"]

    family_summary = _family_summary_rows(ols_rows, catalog_rows)
    stable_rows = [row for row in ols_rows if int(row["n_obs"]) >= STABLE_N_OBS_THRESHOLD]
    top_r2_stable = sorted(stable_rows, key=lambda row: float(row["r2"]), reverse=True)[:50]
    top_beta_stable = sorted(stable_rows, key=lambda row: float(row["beta"]), reverse=True)[:50]
    bottom_beta_stable = sorted(stable_rows, key=lambda row: float(row["beta"]))[:50]

    _write_csv(output_dir / "denue_city_summary.csv", [overview], list(overview.keys()))
    _write_csv(output_dir / "city_y_catalog.csv", catalog_rows, list(catalog_rows[0].keys()) if catalog_rows else [])
    _write_csv(output_dir / "city_y_all_fits.csv", fit_rows, list(fit_rows[0].keys()) if fit_rows else [])
    _write_csv(output_dir / "city_y_best_fits.csv", best_rows, list(best_rows[0].keys()) if best_rows else [])
    _write_csv(output_dir / "city_y_ols_fits.csv", ols_rows, list(ols_rows[0].keys()) if ols_rows else [])
    _write_csv(output_dir / "city_y_family_summary.csv", family_summary, list(family_summary[0].keys()) if family_summary else [])
    _write_csv(output_dir / "city_y_top_r2_stable.csv", top_r2_stable, list(top_r2_stable[0].keys()) if top_r2_stable else [])
    _write_csv(output_dir / "city_y_top_beta_stable.csv", top_beta_stable, list(top_beta_stable[0].keys()) if top_beta_stable else [])
    _write_csv(output_dir / "city_y_bottom_beta_stable.csv", bottom_beta_stable, list(bottom_beta_stable[0].keys()) if bottom_beta_stable else [])

    family_dir = output_dir / "families"
    figure_manifest, family_index_rows = _write_family_dossiers(family_dir, ols_rows)
    _write_csv(output_dir / "family_index.csv", family_index_rows, ["family", "family_label", "category_count", "folder", "complete_statistics_csv"])
    _write_csv(figdir / "figures_manifest.csv", figure_manifest, ["family", "family_label", "figure_type", "path"])

    total_ols = next((row for row in ols_rows if row["y_key"] == "all"), None)
    total_best = next((row for row in best_rows if row["y_key"] == "all"), None)
    family_counts_lines = [
        "| Family | Categories cataloged | Categories fitted | Median n | Median β (OLS) | Median R² (OLS) |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in family_summary:
        family_counts_lines.append(
            f"| {row['family_label']} | {row['categories_cataloged']} | {row['categories_fitted']} | {row['median_n_obs']} | {_fmt(float(row['median_beta_ols']), 4)} | {_fmt(float(row['median_r2_ols']), 4)} |"
        )

    family_folder_lines = [
        f"- [{row['family_label']}]({row['folder']}): `{row['category_count']}` categories"
        for row in family_index_rows
    ]

    report_lines = [
        "# City Native Y Experiment",
        "",
        "Date: `2026-04-21`",
        "",
        "This is the first city-native `Y` catalog experiment in `urban-sami` built entirely from the independent raw DENUE snapshot and the official municipality population table.",
        "In the project framing, `city` is the main theory object. This report screens the full space of count-like `Y` definitions that can currently be derived from the loaded DENUE fields before moving to more selective theory tests.",
        "",
        "Theory framing:",
        f"- [theory-framing.md]({(root / 'docs' / 'theory-framing.md').resolve()})",
        "",
        "## Current Native Y Families",
        "",
        "- total establishments",
        "- SCIAN 2-digit counts",
        "- SCIAN 3-digit counts",
        "- SCIAN 4-digit counts",
        "- SCIAN 6-digit counts",
        "- raw `per_ocu` size-band counts",
        "- derived `size_class` counts from `per_ocu`",
        "- `SCIAN 2-digit x per_ocu` counts",
        "- `SCIAN 2-digit x size_class` counts",
        "",
        "Not included in this pass:",
        "- `tipoUniEco`, because it is not yet loaded into `raw.denue_establishments`",
        "- bounded or compositional indicators such as shares and entropy; this pass is restricted to count-like `Y`",
        "",
        "## Inputs",
        "",
        f"- total DENUE establishments with city code: `{int(overview['establishments_total']):,}`",
        f"- municipalities in population table: `{len(city_units):,}`",
        f"- municipalities present in DENUE: `{int(overview['distinct_cities_with_denue']):,}`",
        f"- distinct SCIAN 2-digit codes: `{int(overview['distinct_scian2'])}`",
        f"- distinct SCIAN 3-digit codes: `{int(overview['distinct_scian3'])}`",
        f"- distinct SCIAN 4-digit codes: `{int(overview['distinct_scian4'])}`",
        f"- distinct SCIAN 6-digit codes: `{int(overview['distinct_scian6'])}`",
        f"- distinct raw `per_ocu` bands: `{int(overview['distinct_per_ocu'])}`",
        f"- stable screening threshold for summary tables: `n_obs >= {STABLE_N_OBS_THRESHOLD}` cities",
        "",
        "## Outputs",
        "",
        f"- Full folder: [{output_dir.name}]({output_dir.resolve()})",
        f"- Aggregated city-level input data: [city_y_unit_counts.csv]({unit_counts_path.resolve()})",
        f"- Y catalog: [city_y_catalog.csv]({(output_dir / 'city_y_catalog.csv').resolve()})",
        f"- All fits: [city_y_all_fits.csv]({(output_dir / 'city_y_all_fits.csv').resolve()})",
        f"- OLS-only table for cross-Y comparison: [city_y_ols_fits.csv]({(output_dir / 'city_y_ols_fits.csv').resolve()})",
        f"- Best fit per Y: [city_y_best_fits.csv]({(output_dir / 'city_y_best_fits.csv').resolve()})",
        f"- Family summary: [city_y_family_summary.csv]({(output_dir / 'city_y_family_summary.csv').resolve()})",
        f"- Family index: [family_index.csv]({(output_dir / 'family_index.csv').resolve()})",
        f"- Stable top R² rows: [city_y_top_r2_stable.csv]({(output_dir / 'city_y_top_r2_stable.csv').resolve()})",
        f"- Stable top β rows: [city_y_top_beta_stable.csv]({(output_dir / 'city_y_top_beta_stable.csv').resolve()})",
        f"- Stable bottom β rows: [city_y_bottom_beta_stable.csv]({(output_dir / 'city_y_bottom_beta_stable.csv').resolve()})",
        f"- Figure manifest: [figures_manifest.csv]({(figdir / 'figures_manifest.csv').resolve()})",
        "",
        "## Family Dossiers",
        "",
        "Each family now has its own folder with:",
        "- `complete_statistics.csv` containing all categories and explicit ranks for share, coverage, beta, and R²",
        "- `share_rank_all.svg` with all categories ordered by share",
        "- `n_obs_rank_all.svg` with all categories ordered by number of cities in the fit",
        "- `beta_rank_all.svg` with all categories ordered by beta",
        "- `r2_rank_all.svg` with all categories ordered by R²",
        "",
        *family_folder_lines,
        "",
        "## Why OLS Is The Comparison Frame Here",
        "",
        "All five fit families are written to disk. But for a screening comparison across hundreds of `Y` definitions, this report uses `ols` as the common frame for comparing `β` and `R²` because the log-log functional form is identical across rows. The `best_fit` table is still provided separately. The family dossiers are the primary analysis objects; the short stable tables are only navigation aids.",
        "",
        "## Total City Baseline",
        "",
        f"- OLS total `Y = all establishments`: `beta = {_fmt(float(total_ols['beta']))}`, `R2 = {_fmt(float(total_ols['r2']))}`, `n = {int(total_ols['n_obs'])}`" if total_ols else "- OLS total baseline unavailable",
        f"- Best-fit total row: `fit_method = {total_best['fit_method']}`, `beta = {_fmt(float(total_best['beta']))}`, `R2 = {_fmt(float(total_best['r2']))}`" if total_best else "- Best-fit total row unavailable",
        "",
        "## Family Coverage And Central Tendency",
        "",
        *family_counts_lines,
        "",
        "## How To Read The Dossiers",
        "",
        "- start with `family_index.csv` to see how many categories each family has",
        "- then open the family folder you care about",
        "- use `complete_statistics.csv` to inspect all categories numerically",
        "- use `share_rank_all.svg` first to see whether a few categories dominate the family",
        "- then compare `beta_rank_all.svg` and `r2_rank_all.svg` to see whether the dominant categories also control the scaling regime and fit quality",
        "- use `n_obs_rank_all.svg` to separate broad city coverage from sparse long-tail categories",
        "",
        "## Navigation Aids",
        "",
        "The short stable tables remain useful for scanning, but they are no longer the primary presentation:",
        f"- [city_y_top_r2_stable.csv]({(output_dir / 'city_y_top_r2_stable.csv').resolve()})",
        f"- [city_y_top_beta_stable.csv]({(output_dir / 'city_y_top_beta_stable.csv').resolve()})",
        f"- [city_y_bottom_beta_stable.csv]({(output_dir / 'city_y_bottom_beta_stable.csv').resolve()})",
        "",
        "## First Reading",
        "",
        "- the city-level total baseline remains the main scaling reference for this project",
        "- once `Y` is disaggregated, the scaling regime becomes heterogeneous across sector and size families",
        "- the full hierarchy is now organized family by family, so the next step is theory-guided reading of those dossiers rather than another coarse screening pass",
        "- the sparse long tail, especially in higher-resolution SCIAN families, should be treated as exploratory unless `n_obs` is high",
    ]

    (output_dir / "report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    report_json = {
        "workflow_id": "denue_y_city_native_experiments",
        "output_dir": str(output_dir.resolve()),
        "establishments_total": total_establishments,
        "city_units_total": len(city_units),
        "catalog_rows": len(catalog_rows),
        "fit_rows": len(fit_rows),
        "ols_rows": len(ols_rows),
        "best_rows": len(best_rows),
        "stable_threshold_n_obs": STABLE_N_OBS_THRESHOLD,
        "figure_count": len(figure_manifest),
        "total_ols_beta": float(total_ols["beta"]) if total_ols else None,
        "total_ols_r2": float(total_ols["r2"]) if total_ols else None,
        "total_best_fit_method": str(total_best["fit_method"]) if total_best else "",
        "total_best_r2": float(total_best["r2"]) if total_best else None,
    }
    (output_dir / "report.json").write_text(json.dumps(report_json, indent=2), encoding="utf-8")
    print(json.dumps(report_json, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import shutil
import subprocess
from pathlib import Path

from run_denue_y_state_scientific_analysis import BLUE, SCIAN2_LABELS, write_ranked_metric_chart


DOCKER_EXE = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DB_CONTAINER = "24-polisplexity-core-db-dev"
POSTGRES_USER = "postgres"
DB_NAME = "urban_sami_exp"

SOURCE_CITY_NATIVE = "denue-y-city-native-experiments-2026-04-21"
SOURCE_CITY_SAMI = "city-y-sami-comparison-pack-2026-04-21"
OUTPUT_PACK = "scian-explainer-2026-04-21"

OFFICIAL_SCIAN_MX_2023 = "https://naics-scian.inegi.org.mx/naics_scian/Trip_esp.htm"
OFFICIAL_SCIAN_PDF = "https://en.www.inegi.org.mx/contenidos/app/scian/scian.pdf"

OFFICIAL_SECTORS = [
    {"sector_group_code": "11", "display_code": "11", "official_sector_counted_as_one": "yes", "group_label": "Agricultura, cría y explotación de animales, aprovechamiento forestal, pesca y caza", "mx_2023_code_note": "single 2-digit sector"},
    {"sector_group_code": "21", "display_code": "21", "official_sector_counted_as_one": "yes", "group_label": "Minería", "mx_2023_code_note": "single 2-digit sector"},
    {"sector_group_code": "22", "display_code": "22", "official_sector_counted_as_one": "yes", "group_label": "Generación, transmisión, distribución y comercialización de energía eléctrica, suministro de agua y de gas natural por ductos al consumidor final", "mx_2023_code_note": "single 2-digit sector"},
    {"sector_group_code": "23", "display_code": "23", "official_sector_counted_as_one": "yes", "group_label": "Construcción", "mx_2023_code_note": "single 2-digit sector"},
    {"sector_group_code": "31-33", "display_code": "31|32|33", "official_sector_counted_as_one": "yes", "group_label": "Industrias manufactureras", "mx_2023_code_note": "one official sector represented by three 2-digit codes"},
    {"sector_group_code": "43", "display_code": "43", "official_sector_counted_as_one": "yes", "group_label": "Comercio al por mayor", "mx_2023_code_note": "single 2-digit sector"},
    {"sector_group_code": "46", "display_code": "46", "official_sector_counted_as_one": "yes", "group_label": "Comercio al por menor", "mx_2023_code_note": "single 2-digit sector"},
    {"sector_group_code": "48-49", "display_code": "48|49", "official_sector_counted_as_one": "yes", "group_label": "Transportes, correos y almacenamiento", "mx_2023_code_note": "one official sector represented by two 2-digit codes"},
    {"sector_group_code": "51", "display_code": "51", "official_sector_counted_as_one": "yes", "group_label": "Información en medios masivos", "mx_2023_code_note": "single 2-digit sector"},
    {"sector_group_code": "52", "display_code": "52", "official_sector_counted_as_one": "yes", "group_label": "Servicios financieros y de seguros", "mx_2023_code_note": "single 2-digit sector"},
    {"sector_group_code": "53", "display_code": "53", "official_sector_counted_as_one": "yes", "group_label": "Servicios inmobiliarios y de alquiler de bienes muebles e intangibles", "mx_2023_code_note": "single 2-digit sector"},
    {"sector_group_code": "54", "display_code": "54", "official_sector_counted_as_one": "yes", "group_label": "Servicios profesionales, científicos y técnicos", "mx_2023_code_note": "single 2-digit sector"},
    {"sector_group_code": "55", "display_code": "55", "official_sector_counted_as_one": "yes", "group_label": "Dirección y administración de grupos empresariales o corporativos", "mx_2023_code_note": "single 2-digit sector"},
    {"sector_group_code": "56", "display_code": "56", "official_sector_counted_as_one": "yes", "group_label": "Servicios de apoyo a los negocios y manejo de residuos, y servicios de remediación", "mx_2023_code_note": "single 2-digit sector"},
    {"sector_group_code": "61", "display_code": "61", "official_sector_counted_as_one": "yes", "group_label": "Servicios educativos", "mx_2023_code_note": "single 2-digit sector"},
    {"sector_group_code": "62", "display_code": "62", "official_sector_counted_as_one": "yes", "group_label": "Servicios de salud y de asistencia social", "mx_2023_code_note": "single 2-digit sector"},
    {"sector_group_code": "71", "display_code": "71", "official_sector_counted_as_one": "yes", "group_label": "Servicios de esparcimiento culturales y deportivos, y otros servicios recreativos", "mx_2023_code_note": "single 2-digit sector"},
    {"sector_group_code": "72", "display_code": "72", "official_sector_counted_as_one": "yes", "group_label": "Servicios de alojamiento temporal y de preparación de alimentos y bebidas", "mx_2023_code_note": "single 2-digit sector"},
    {"sector_group_code": "81", "display_code": "81", "official_sector_counted_as_one": "yes", "group_label": "Otros servicios excepto actividades gubernamentales", "mx_2023_code_note": "single 2-digit sector"},
    {"sector_group_code": "93", "display_code": "93", "official_sector_counted_as_one": "yes", "group_label": "Actividades legislativas, gubernamentales, de impartición de justicia y de organismos internacionales y extraterritoriales", "mx_2023_code_note": "Mexico uses 93 where Canada uses 91 and the United States uses 92"},
]


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


def _pct(value: float, digits: int = 2) -> str:
    return f"{value * 100:.{digits}f}%"


def _load_scian2_city_rows(root: Path) -> list[dict[str, str]]:
    path = root / "reports" / SOURCE_CITY_NATIVE / "families" / "scian2" / "complete_statistics.csv"
    return _read_csv(path)


def _load_scian2_city_dossiers(root: Path) -> list[dict[str, str]]:
    path = root / "reports" / SOURCE_CITY_SAMI / "category_dossier_index.csv"
    return [row for row in _read_csv(path) if row["family"] == "scian2"]


def _fetch_scian_hierarchy_counts() -> list[dict[str, str]]:
    sql = """
    SELECT
        SUBSTRING(scian_code FROM 1 FOR 2) AS scian2,
        COUNT(*)::text AS establishments,
        COUNT(DISTINCT city_code)::text AS cities_present,
        COUNT(DISTINCT CASE WHEN char_length(scian_code) >= 3 THEN SUBSTRING(scian_code FROM 1 FOR 3) END)::text AS distinct_scian3,
        COUNT(DISTINCT CASE WHEN char_length(scian_code) >= 4 THEN SUBSTRING(scian_code FROM 1 FOR 4) END)::text AS distinct_scian4,
        COUNT(DISTINCT CASE WHEN char_length(scian_code) >= 5 THEN SUBSTRING(scian_code FROM 1 FOR 5) END)::text AS distinct_scian5,
        COUNT(DISTINCT CASE WHEN char_length(scian_code) >= 6 THEN scian_code END)::text AS distinct_scian6
    FROM raw.denue_establishments
    WHERE city_code <> '' AND char_length(scian_code) >= 2
    GROUP BY scian2
    ORDER BY scian2
    """.strip()
    return _query_tsv(
        sql,
        ["scian2", "establishments", "cities_present", "distinct_scian3", "distinct_scian4", "distinct_scian5", "distinct_scian6"],
    )


def _build_scian2_overview(city_rows: list[dict[str, str]], hierarchy_rows: list[dict[str, str]], dossier_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    hierarchy_map = {row["scian2"]: row for row in hierarchy_rows}
    dossier_map = {row["category"]: row for row in dossier_rows}
    out: list[dict[str, str]] = []
    for row in city_rows:
        code = row["category"]
        h = hierarchy_map.get(code, {})
        d = dossier_map.get(code, {})
        beta = _to_float(row["beta"])
        if beta > 1.05:
            regime = "superlinear"
        elif beta < 0.95:
            regime = "sublinear"
        else:
            regime = "near_linear"
        out.append(
            {
                "scian2": code,
                "category_label": row["category_label"],
                "share_of_total": row["share_of_total"],
                "total_count": row["total_count"],
                "cities_in_fit": row["n_obs"],
                "coverage_tier": row["coverage_tier"],
                "beta": row["beta"],
                "r2": row["r2"],
                "alpha": row["alpha"],
                "resid_std": row["resid_std"],
                "beta_regime": regime,
                "distinct_scian3_present": h.get("distinct_scian3", ""),
                "distinct_scian4_present": h.get("distinct_scian4", ""),
                "distinct_scian5_present": h.get("distinct_scian5", ""),
                "distinct_scian6_present": h.get("distinct_scian6", ""),
                "cities_present_in_raw_denue": h.get("cities_present", ""),
                "dossier_folder": d.get("folder", ""),
                "dossier_city_rank_svg": d.get("city_sami_rank_svg", ""),
                "dossier_scaling_scatter_svg": d.get("scaling_scatter_svg", ""),
            }
        )
    return out


def _write_summary_graphs(outdir: Path, rows: list[dict[str, str]]) -> list[dict[str, str]]:
    figdir = outdir / "figures"
    figdir.mkdir(parents=True, exist_ok=True)
    manifest: list[dict[str, str]] = []

    share_fig = figdir / "scian2_share_rank.svg"
    write_ranked_metric_chart(
        share_fig,
        title="SCIAN 2-digit: weight in loaded DENUE",
        subtitle="23 Mexican two-digit codes present in the loaded DENUE data. Ordered by share of all establishments.",
        rows=rows,
        metric_field="share_of_total",
        order_field="share_of_total",
        value_formatter=lambda value: _pct(value),
    )
    manifest.append({"figure_id": "scian2_share_rank", "path": str(share_fig.resolve())})

    beta_fig = figdir / "scian2_beta_rank.svg"
    write_ranked_metric_chart(
        beta_fig,
        title="SCIAN 2-digit: city scaling exponent by sector",
        subtitle="Each point is the cross-city OLS beta for one sector. Read relative to the β = 1 reference line.",
        rows=rows,
        metric_field="beta",
        order_field="beta",
        ref_line=1.0,
        value_formatter=lambda value: f"{value:.3f}",
    )
    manifest.append({"figure_id": "scian2_beta_rank", "path": str(beta_fig.resolve())})

    r2_fig = figdir / "scian2_r2_rank.svg"
    write_ranked_metric_chart(
        r2_fig,
        title="SCIAN 2-digit: city scaling fit quality by sector",
        subtitle="Cross-city OLS R² for each SCIAN 2-digit sector.",
        rows=rows,
        metric_field="r2",
        order_field="r2",
        color=BLUE,
        fixed_range=(0.0, 1.0),
        value_formatter=lambda value: f"{value:.3f}",
    )
    manifest.append({"figure_id": "scian2_r2_rank", "path": str(r2_fig.resolve())})

    class6_fig = figdir / "scian2_class6_rank.svg"
    write_ranked_metric_chart(
        class6_fig,
        title="SCIAN 2-digit: detail depth in loaded DENUE",
        subtitle="How many distinct 6-digit SCIAN classes appear inside each 2-digit sector in the loaded DENUE data.",
        rows=rows,
        metric_field="distinct_scian6_present",
        order_field="distinct_scian6_present",
        color=BLUE,
        value_formatter=lambda value: f"{int(round(value))}",
    )
    manifest.append({"figure_id": "scian2_class6_rank", "path": str(class6_fig.resolve())})

    coverage_fig = figdir / "scian2_city_coverage_rank.svg"
    write_ranked_metric_chart(
        coverage_fig,
        title="SCIAN 2-digit: city coverage by sector",
        subtitle="How many cities contribute positive observations to each sector-level fit.",
        rows=rows,
        metric_field="cities_in_fit",
        order_field="cities_in_fit",
        color=BLUE,
        value_formatter=lambda value: f"{int(round(value))}",
    )
    manifest.append({"figure_id": "scian2_city_coverage_rank", "path": str(coverage_fig.resolve())})

    return manifest


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / OUTPUT_PACK
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    city_rows = _load_scian2_city_rows(root)
    hierarchy_rows = _fetch_scian_hierarchy_counts()
    dossier_rows = _load_scian2_city_dossiers(root)
    overview_rows = _build_scian2_overview(city_rows, hierarchy_rows, dossier_rows)
    overview_rows.sort(key=lambda row: _to_float(row["share_of_total"]), reverse=True)

    official_rows = OFFICIAL_SECTORS
    official_path = _write_csv(
        outdir / "official_scian_sector_list.csv",
        official_rows,
        ["sector_group_code", "display_code", "official_sector_counted_as_one", "group_label", "mx_2023_code_note"],
    )
    hierarchy_path = _write_csv(
        outdir / "scian2_hierarchy_counts.csv",
        hierarchy_rows,
        ["scian2", "establishments", "cities_present", "distinct_scian3", "distinct_scian4", "distinct_scian5", "distinct_scian6"],
    )
    overview_path = _write_csv(
        outdir / "scian2_city_overview.csv",
        overview_rows,
        [
            "scian2",
            "category_label",
            "share_of_total",
            "total_count",
            "cities_in_fit",
            "coverage_tier",
            "beta",
            "r2",
            "alpha",
            "resid_std",
            "beta_regime",
            "distinct_scian3_present",
            "distinct_scian4_present",
            "distinct_scian5_present",
            "distinct_scian6_present",
            "cities_present_in_raw_denue",
            "dossier_folder",
            "dossier_city_rank_svg",
            "dossier_scaling_scatter_svg",
        ],
    )

    manifest_rows = _write_summary_graphs(outdir, overview_rows)
    manifest_path = _write_csv(outdir / "figures_manifest.csv", manifest_rows, ["figure_id", "path"])

    report_lines = [
        "# SCIAN Explainer Pack",
        "",
        "Date: `2026-04-21`",
        "",
        "This pack is for understanding what SCIAN is before interpreting the city SAMI results.",
        "",
        "## The Main Clarification",
        "",
        "- SCIAN Mexico 2023 is officially organized into **20 sectors**.",
        "- But at the **2-digit code display level**, Mexico shows **23 codes** in practice because two official sectors are split across several 2-digit codes:",
        "  - manufacturing = `31`, `32`, `33`",
        "  - transportation, post and storage = `48`, `49`",
        "- So: `20` official sectors, but `23` two-digit codes visible in the data.",
        "",
        "## Why does `93` appear?",
        "",
        "- In Mexico, the public-administration sector is coded as `93`.",
        "- In the INEGI tripartite SCIAN table, the corresponding sector is `91` in Canada, `92` in the United States, and `93` in Mexico.",
        "- Its official Mexican title is: `Actividades legislativas, gubernamentales, de impartición de justicia y de organismos internacionales y extraterritoriales`.",
        "",
        "## Main Files",
        "",
        f"- Official sector list: [official_scian_sector_list.csv]({official_path.resolve()})",
        f"- Loaded DENUE hierarchy counts: [scian2_hierarchy_counts.csv]({hierarchy_path.resolve()})",
        f"- City-fit overview by SCIAN 2-digit: [scian2_city_overview.csv]({overview_path.resolve()})",
        f"- Figure manifest: [figures_manifest.csv]({manifest_path.resolve()})",
        "",
        "## How To Read The 23 SCIAN 2-digit Codes",
        "",
        "- `share_of_total`: how much of all DENUE establishments belongs to the sector",
        "- `beta`: city scaling exponent for that sector",
        "- `r2`: fit quality of the cross-city scaling law for that sector",
        "- `distinct_scian3_present`, `distinct_scian4_present`, `distinct_scian6_present`: how much internal detail appears inside that sector in the loaded DENUE data",
        "- `dossier_*`: links to the city-comparison dossier for that sector",
        "",
        "## Figures",
        "",
    ]
    for row in manifest_rows:
        report_lines.append(f"- [{row['figure_id']}]({row['path']})")
    report_lines.extend(
        [
            "",
            "## Official Sources Used",
            "",
            f"- INEGI tripartite SCIAN structure: {OFFICIAL_SCIAN_MX_2023}",
            f"- INEGI SCIAN Mexico PDF: {OFFICIAL_SCIAN_PDF}",
            "",
            "## Practical Reading",
            "",
            "- Start with `official_scian_sector_list.csv` to understand the 20-sector official structure.",
            "- Then open `scian2_city_overview.csv` to see the 23 Mexican two-digit codes we are actually fitting.",
            "- Then inspect the sector graphs to see which sectors dominate the data and which sectors have stronger or weaker city scaling.",
        ]
    )
    (outdir / "report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    report_json = {
        "workflow_id": "scian_explainer_pack",
        "output_dir": str(outdir.resolve()),
        "official_sector_groups": 20,
        "mexico_two_digit_codes_in_data": len(overview_rows),
        "source_city_native": SOURCE_CITY_NATIVE,
        "source_city_sami": SOURCE_CITY_SAMI,
        "figure_count": len(manifest_rows),
        "official_source_tripartite": OFFICIAL_SCIAN_MX_2023,
        "official_source_pdf": OFFICIAL_SCIAN_PDF,
    }
    (outdir / "report.json").write_text(json.dumps(report_json, indent=2), encoding="utf-8")
    print(json.dumps(report_json, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

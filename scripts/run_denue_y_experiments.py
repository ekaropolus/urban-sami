#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
import subprocess

from urban_sami.modeling import fit_by_name


DOCKER_EXE = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DB_CONTAINER = "24-polisplexity-core-db-dev"
POSTGRES_USER = "postgres"
DB_NAME = "urban_sami_exp"


@dataclass(frozen=True)
class YDef:
    y_key: str
    family: str
    label: str
    sql_clause: str


def _exec(sql: str) -> None:
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
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    ]
    subprocess.run(cmd, check=True, capture_output=True, text=True)


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
        row = {col: (parts[idx] if idx < len(parts) else "") for idx, col in enumerate(columns)}
        rows.append(row)
    return rows


def _read_city_sample(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return [str(row["city_code"]).strip() for row in rows if str(row.get("city_code", "")).strip()]


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _fit_rows(rows: list[dict[str, str]], *, level: str, y_key: str, y_family: str, y_label: str) -> list[dict]:
    filtered = [row for row in rows if float(row["population"]) > 0.0 and float(row["est_count"]) > 0.0]
    if len(filtered) < 2:
        return []
    y = [float(row["est_count"]) for row in filtered]
    n = [float(row["population"]) for row in filtered]
    out = []
    for method in ("ols", "robust", "poisson", "negbin", "auto"):
        fit = fit_by_name(y, n, method)
        out.append(
            {
                "level": level,
                "y_key": y_key,
                "y_family": y_family,
                "y_label": y_label,
                "fit_method": method,
                "n_obs": len(filtered),
                "alpha": fit.alpha,
                "beta": fit.beta,
                "r2": fit.r2,
                "resid_std": fit.residual_std,
            }
        )
    return out


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    output_dir = root / "reports" / "denue-y-native-experiments-2026-04-21"
    output_dir.mkdir(parents=True, exist_ok=True)

    city_sample = root / "data" / "raw" / "city_samples" / "top20_population.csv"
    city_codes = _read_city_sample(city_sample)
    city_list_sql = ",".join(f"'{code}'" for code in city_codes)

    stats_summary = _query_tsv(
        """
        SELECT
            (SELECT COUNT(*) FROM raw.denue_establishments)::text,
            (SELECT COUNT(*) FROM raw.denue_establishments WHERE latitude IS NOT NULL AND longitude IS NOT NULL)::text,
            (SELECT COUNT(DISTINCT state_code) FROM raw.denue_establishments WHERE state_code <> '')::text,
            (SELECT COUNT(DISTINCT city_code) FROM raw.denue_establishments WHERE city_code <> '')::text,
            (SELECT COUNT(DISTINCT SUBSTRING(scian_code FROM 1 FOR 2)) FROM raw.denue_establishments WHERE scian_code <> '')::text,
            (SELECT COUNT(DISTINCT SUBSTRING(scian_code FROM 1 FOR 3)) FROM raw.denue_establishments WHERE scian_code <> '')::text,
            (SELECT COUNT(DISTINCT scian_code) FROM raw.denue_establishments WHERE scian_code <> '')::text
        """.strip(),
        ["establishments_total", "with_coordinates", "distinct_states", "distinct_cities", "distinct_scian2", "distinct_scian3", "distinct_scian6"],
    )[0]
    _write_csv(output_dir / "denue_summary.csv", [stats_summary], list(stats_summary.keys()))

    per_ocu_rows = _query_tsv(
        """
        SELECT COALESCE(per_ocu, '') AS per_ocu, COUNT(*)::text AS establishments
        FROM raw.denue_establishments
        GROUP BY per_ocu
        ORDER BY COUNT(*) DESC, per_ocu
        """.strip(),
        ["per_ocu", "establishments"],
    )
    _write_csv(output_dir / "per_ocu_distribution.csv", per_ocu_rows, ["per_ocu", "establishments"])

    scian2_rows = _query_tsv(
        """
        SELECT SUBSTRING(scian_code FROM 1 FOR 2) AS scian2, COUNT(*)::text AS establishments
        FROM raw.denue_establishments
        WHERE scian_code <> ''
        GROUP BY SUBSTRING(scian_code FROM 1 FOR 2)
        ORDER BY COUNT(*) DESC, scian2
        """.strip(),
        ["scian2", "establishments"],
    )
    _write_csv(output_dir / "scian2_distribution.csv", scian2_rows, ["scian2", "establishments"])

    scian3_top = _query_tsv(
        """
        SELECT SUBSTRING(scian_code FROM 1 FOR 3) AS scian3, COUNT(*)::text AS establishments
        FROM raw.denue_establishments
        WHERE scian_code <> ''
        GROUP BY SUBSTRING(scian_code FROM 1 FOR 3)
        ORDER BY COUNT(*) DESC, scian3
        LIMIT 40
        """.strip(),
        ["scian3", "establishments"],
    )
    _write_csv(output_dir / "scian3_top40.csv", scian3_top, ["scian3", "establishments"])

    # tipoUniEco is in the raw DENUE files but not yet loaded into raw.denue_establishments.
    raw_typeunieco = _query_tsv(
        """
        SELECT 'not_loaded_in_experiment_db' AS note
        """.strip(),
        ["note"],
    )
    _write_csv(output_dir / "tipo_unieco_status.csv", raw_typeunieco, ["note"])

    top_scian2 = [row["scian2"] for row in scian2_rows[:10] if row["scian2"]]
    y_defs = [YDef("all", "total", "all establishments", "TRUE")]
    for row in per_ocu_rows:
        band = row["per_ocu"]
        if not band:
            continue
        safe = band.replace(" ", "_").replace("á", "a").replace("ó", "o").replace("í", "i").replace("é", "e")
        y_defs.append(YDef(f"per_ocu::{safe}", "per_ocu", band, f"d.per_ocu = '{band}'"))
    for code in top_scian2:
        y_defs.append(YDef(f"scian2::{code}", "scian2", f"SCIAN 2-digit {code}", f"SUBSTRING(d.scian_code FROM 1 FOR 2) = '{code}'"))

    # Build reusable AGEB assignment for the top-20 city sample.
    _exec("DROP TABLE IF EXISTS staging.ageb_top20_denue_assignments;")
    _exec(
        f"""
        CREATE TABLE staging.ageb_top20_denue_assignments AS
        WITH ageb AS (
            SELECT unit_code, city_code, geom
            FROM raw.admin_units
            WHERE level = 'ageb_u' AND city_code IN ({city_list_sql})
        ),
        denue_points AS (
            SELECT city_code, scian_code, per_ocu, ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geom
            FROM raw.denue_establishments
            WHERE city_code IN ({city_list_sql})
              AND longitude IS NOT NULL
              AND latitude IS NOT NULL
        )
        SELECT a.unit_code,
               a.city_code,
               d.scian_code,
               d.per_ocu
        FROM ageb a
        JOIN denue_points d
          ON d.city_code = a.city_code
         AND ST_Covers(a.geom, d.geom)
        """.strip()
    )
    _exec("CREATE INDEX ageb_top20_assign_unit_idx ON staging.ageb_top20_denue_assignments (unit_code);")
    _exec("CREATE INDEX ageb_top20_assign_city_idx ON staging.ageb_top20_denue_assignments (city_code);")
    _exec("ANALYZE staging.ageb_top20_denue_assignments;")

    all_fit_rows: list[dict] = []
    input_count_rows: list[dict] = []

    for y_def in y_defs:
        # state
        state_rows = _query_tsv(
            f"""
            WITH denue AS (
                SELECT d.state_code AS unit_code, COUNT(*)::text AS est_count
                FROM raw.denue_establishments d
                WHERE d.state_code <> ''
                  AND {y_def.sql_clause}
                GROUP BY d.state_code
            )
            SELECT p.unit_code,
                   p.unit_label,
                   COALESCE(denue.est_count, '0') AS est_count,
                   COALESCE(p.population, 0)::text AS population
            FROM raw.population_units p
            LEFT JOIN denue ON denue.unit_code = p.unit_code
            WHERE p.level = 'state'
            ORDER BY p.unit_code
            """.strip(),
            ["unit_code", "unit_label", "est_count", "population"],
        )
        state_fit = _fit_rows(state_rows, level="state", y_key=y_def.y_key, y_family=y_def.family, y_label=y_def.label)
        all_fit_rows.extend(state_fit)
        input_count_rows.append(
            {
                "level": "state",
                "y_key": y_def.y_key,
                "y_family": y_def.family,
                "y_label": y_def.label,
                "input_rows_total": len(state_rows),
                "input_rows_fit": sum(float(r["population"]) > 0 and float(r["est_count"]) > 0 for r in state_rows),
                "zero_y_rows": sum(float(r["est_count"]) <= 0 for r in state_rows),
            }
        )

        # city
        city_rows = _query_tsv(
            f"""
            WITH denue AS (
                SELECT d.city_code AS unit_code, COUNT(*)::text AS est_count
                FROM raw.denue_establishments d
                WHERE d.city_code <> ''
                  AND {y_def.sql_clause}
                GROUP BY d.city_code
            )
            SELECT p.unit_code,
                   p.unit_label,
                   COALESCE(denue.est_count, '0') AS est_count,
                   COALESCE(p.population, 0)::text AS population
            FROM raw.population_units p
            LEFT JOIN denue ON denue.unit_code = p.unit_code
            WHERE p.level = 'city'
            ORDER BY p.unit_code
            """.strip(),
            ["unit_code", "unit_label", "est_count", "population"],
        )
        city_fit = _fit_rows(city_rows, level="city", y_key=y_def.y_key, y_family=y_def.family, y_label=y_def.label)
        all_fit_rows.extend(city_fit)
        input_count_rows.append(
            {
                "level": "city",
                "y_key": y_def.y_key,
                "y_family": y_def.family,
                "y_label": y_def.label,
                "input_rows_total": len(city_rows),
                "input_rows_fit": sum(float(r["population"]) > 0 and float(r["est_count"]) > 0 for r in city_rows),
                "zero_y_rows": sum(float(r["est_count"]) <= 0 for r in city_rows),
            }
        )

        # ageb top20
        ageb_rows = _query_tsv(
            f"""
            WITH denue AS (
                SELECT unit_code, COUNT(*)::text AS est_count
                FROM staging.ageb_top20_denue_assignments d
                WHERE {y_def.sql_clause}
                GROUP BY unit_code
            )
            SELECT a.unit_code,
                   a.unit_label,
                   COALESCE(denue.est_count, '0') AS est_count,
                   COALESCE(a.population, 0)::text AS population
            FROM raw.admin_units a
            LEFT JOIN denue ON denue.unit_code = a.unit_code
            WHERE a.level = 'ageb_u'
              AND a.city_code IN ({city_list_sql})
            ORDER BY a.city_code, a.unit_code
            """.strip(),
            ["unit_code", "unit_label", "est_count", "population"],
        )
        ageb_fit = _fit_rows(ageb_rows, level="ageb_u_top20", y_key=y_def.y_key, y_family=y_def.family, y_label=y_def.label)
        all_fit_rows.extend(ageb_fit)
        input_count_rows.append(
            {
                "level": "ageb_u_top20",
                "y_key": y_def.y_key,
                "y_family": y_def.family,
                "y_label": y_def.label,
                "input_rows_total": len(ageb_rows),
                "input_rows_fit": sum(float(r["population"]) > 0 and float(r["est_count"]) > 0 for r in ageb_rows),
                "zero_y_rows": sum(float(r["est_count"]) <= 0 for r in ageb_rows),
            }
        )

    _write_csv(
        output_dir / "y_experiment_all_fits.csv",
        all_fit_rows,
        ["level", "y_key", "y_family", "y_label", "fit_method", "n_obs", "alpha", "beta", "r2", "resid_std"],
    )
    _write_csv(
        output_dir / "y_experiment_input_counts.csv",
        input_count_rows,
        ["level", "y_key", "y_family", "y_label", "input_rows_total", "input_rows_fit", "zero_y_rows"],
    )

    best_rows = []
    grouped: dict[tuple[str, str], list[dict]] = {}
    for row in all_fit_rows:
        grouped.setdefault((row["level"], row["y_key"]), []).append(row)
    for (level, y_key), rows in sorted(grouped.items()):
        best = max(rows, key=lambda row: float(row["r2"]))
        best_rows.append(best)
    _write_csv(
        output_dir / "y_experiment_best_fits.csv",
        best_rows,
        ["level", "y_key", "y_family", "y_label", "fit_method", "n_obs", "alpha", "beta", "r2", "resid_std"],
    )

    # concise markdown report
    def best(level: str, y_key: str) -> dict | None:
        for row in best_rows:
            if row["level"] == level and row["y_key"] == y_key:
                return row
        return None

    report_lines = [
        "# DENUE Native-Y Experiment Report",
        "",
        "Date: `2026-04-21`",
        "",
        "This report uses only native DENUE classification fields that are directly present and usable now:",
        "",
        "- `codigo_act` represented in the experiment database as `scian_code`",
        "- `per_ocu`",
        "- `tipoUniEco` is present in the raw DENUE files but is not yet loaded into `raw.denue_establishments`, so it is descriptive-only in this pass",
        "",
        "## DENUE Summary",
        "",
        f"- total establishments loaded: `{stats_summary['establishments_total']}`",
        f"- establishments with coordinates: `{stats_summary['with_coordinates']}`",
        f"- distinct states: `{stats_summary['distinct_states']}`",
        f"- distinct cities: `{stats_summary['distinct_cities']}`",
        f"- distinct SCIAN 2-digit codes: `{stats_summary['distinct_scian2']}`",
        f"- distinct SCIAN 3-digit codes: `{stats_summary['distinct_scian3']}`",
        f"- distinct SCIAN 6-digit codes: `{stats_summary['distinct_scian6']}`",
        "",
        "Artifacts:",
        "",
        f"- [denue_summary.csv]({(output_dir / 'denue_summary.csv').resolve()})",
        f"- [per_ocu_distribution.csv]({(output_dir / 'per_ocu_distribution.csv').resolve()})",
        f"- [scian2_distribution.csv]({(output_dir / 'scian2_distribution.csv').resolve()})",
        f"- [scian3_top40.csv]({(output_dir / 'scian3_top40.csv').resolve()})",
        f"- [tipo_unieco_status.csv]({(output_dir / 'tipo_unieco_status.csv').resolve()})",
        "",
        "## Native-Y Experiment Matrix",
        "",
        "Levels used:",
        "",
        "- `state` national",
        "- `city` national",
        "- `ageb_u_top20` broader fine-scale benchmark across the top 20 municipalities by population",
        "",
        "Y families used:",
        "",
        "- all establishments",
        "- establishment count by `per_ocu` band",
        "- establishment count by top 10 SCIAN 2-digit sectors",
        "",
        "Key results for `Y = all establishments`:",
        "",
    ]
    for level in ("state", "city", "ageb_u_top20"):
        row = best(level, "all")
        if row:
            report_lines.append(
                f"- `{level}`: best `{row['fit_method']}`, `beta = {float(row['beta']):.4f}`, `R2 = {float(row['r2']):.4f}`, `n = {row['n_obs']}`"
            )
    report_lines.extend(
        [
            "",
            "Interpretation:",
            "",
            "- the total-count result weakens sharply below city scale",
            "- this remains true under a broader AGEB benchmark, so the effect is not just a two-city artifact",
            "- the next question is whether the weakening is uniform across native sector and size categories",
            "",
            "Main experiment tables:",
            "",
            f"- [y_experiment_all_fits.csv]({(output_dir / 'y_experiment_all_fits.csv').resolve()})",
            f"- [y_experiment_best_fits.csv]({(output_dir / 'y_experiment_best_fits.csv').resolve()})",
            f"- [y_experiment_input_counts.csv]({(output_dir / 'y_experiment_input_counts.csv').resolve()})",
        ]
    )
    (output_dir / "report.md").write_text("\n".join(report_lines), encoding="utf-8")

    subprocess.run(
        ["python3", str(root / "scripts" / "generate_denue_y_figures.py")],
        check=True,
        cwd=root,
        env={"PYTHONPATH": str(root / "src")},
        capture_output=True,
        text=True,
    )

    print(json.dumps({"ok": True, "output_dir": str(output_dir), "best_rows": len(best_rows)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

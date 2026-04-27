#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import shutil
import subprocess
from pathlib import Path


WINDOWS_DOCKER = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DOCKER_EXE = os.environ.get("DOCKER_EXE") or shutil.which("docker") or WINDOWS_DOCKER
DB_CONTAINER = os.environ.get("DB_CONTAINER", "24-polisplexity-core-db-dev")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
DB_NAME = os.environ.get("DB_NAME", "urban_sami_exp")

AGEB_SOURCE_METHOD = "denue_ageb_mix_v1"
RESULT_SOURCE_METHOD = "bettencourt_spatial_information_selection_v1"
FAMILIES = ("scian2", "size_class")


def _psql_exec(sql: str) -> None:
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
    ]
    subprocess.run(cmd, input=sql, text=True, check=True)


def _psql_copy(sql: str) -> list[dict[str, str]]:
    script = f"""
\\set ON_ERROR_STOP on
COPY (
{sql}
) TO STDOUT WITH CSV HEADER;
"""
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
    ]
    proc = subprocess.run(cmd, input=script, text=True, capture_output=True, check=True)
    lines = proc.stdout.splitlines()
    if not lines:
        return []
    return list(csv.DictReader(lines))


def _bootstrap() -> None:
    script = Path(__file__).resolve().parents[1] / "scripts" / "bootstrap_experiment_db.sh"
    subprocess.run(["bash", str(script)], check=True)


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _size_case_sql(column: str = "per_ocu") -> str:
    return f"""
CASE
    WHEN lower(coalesce({column}, '')) = '' THEN 'unknown'
    WHEN lower(coalesce({column}, '')) ~ '(251 y mas|251\\+|mas de 250)' THEN 'large'
    WHEN lower(coalesce({column}, '')) ~ '(101 a 250|101-250|51 a 100|51-100)' THEN 'medium'
    WHEN lower(coalesce({column}, '')) ~ '(31 a 50|31-50|11 a 30|11-30)' THEN 'small'
    WHEN lower(coalesce({column}, '')) ~ '(0 a 5|1 a 5|0-5|1-5|6 a 10|6-10|hasta 10)' THEN 'micro'
    ELSE
        CASE
            WHEN regexp_replace(lower(coalesce({column}, '')), '[^0-9]+', ' ', 'g') ~ '^ *$' THEN 'unknown'
            ELSE
                CASE
                    WHEN (
                        SELECT max(x::int)
                        FROM unnest(regexp_split_to_array(trim(regexp_replace(lower(coalesce({column}, '')), '[^0-9]+', ' ', 'g')), ' +')) AS x
                    ) <= 10 THEN 'micro'
                    WHEN (
                        SELECT max(x::int)
                        FROM unnest(regexp_split_to_array(trim(regexp_replace(lower(coalesce({column}, '')), '[^0-9]+', ' ', 'g')), ' +')) AS x
                    ) <= 50 THEN 'small'
                    WHEN (
                        SELECT max(x::int)
                        FROM unnest(regexp_split_to_array(trim(regexp_replace(lower(coalesce({column}, '')), '[^0-9]+', ' ', 'g')), ' +')) AS x
                    ) <= 250 THEN 'medium'
                    ELSE 'large'
                END
        END
END
""".strip()


def _source_counts_sql(level: str) -> str:
    families_sql = ", ".join(f"'{x}'" for x in FAMILIES)
    if level == "ageb":
        return f"""
SELECT
    city_code,
    city_name,
    state_code,
    ageb_code AS unit_code,
    ageb_label AS unit_label,
    family,
    category,
    est_count::double precision AS est_count
FROM derived.ageb_economic_mix
WHERE source_method = '{AGEB_SOURCE_METHOD}'
  AND family IN ({families_sql})
  AND COALESCE(est_count, 0) > 0
"""
    if level == "manzana":
        size_case = _size_case_sql("d.per_ocu")
        return f"""
WITH denue_clean AS (
    SELECT
        d.city_code,
        m.city_name,
        LEFT(d.city_code, 2) AS state_code,
        m.unit_code,
        m.unit_label,
        LEFT(regexp_replace(COALESCE(d.scian_code, ''), '[^0-9]', '', 'g'), 2) AS scian2,
        {size_case} AS size_class
    FROM raw.denue_establishments d
    JOIN raw.population_units m
      ON m.level = 'manzana'
     AND m.city_code = d.city_code
     AND m.ageb_code = d.ageb_code
     AND m.manzana_code = d.manzana_code
    WHERE d.city_code <> ''
      AND d.ageb_code <> ''
      AND d.manzana_code <> ''
)
SELECT city_code, city_name, state_code, unit_code, unit_label, 'scian2'::text AS family, scian2 AS category, COUNT(*)::double precision AS est_count
FROM denue_clean
WHERE scian2 <> ''
GROUP BY city_code, city_name, state_code, unit_code, unit_label, scian2
UNION ALL
SELECT city_code, city_name, state_code, unit_code, unit_label, 'size_class'::text AS family, size_class AS category, COUNT(*)::double precision AS est_count
FROM denue_clean
WHERE size_class <> '' AND size_class <> 'unknown'
GROUP BY city_code, city_name, state_code, unit_code, unit_label, size_class
"""
    raise ValueError(level)


def _persist_level(level: str, refresh: bool) -> None:
    delete_sql = ""
    if refresh:
        delete_sql = f"""
DELETE FROM derived.city_spatial_information_pair_lift
WHERE source_method = '{RESULT_SOURCE_METHOD}'
  AND neighborhood_level = '{level}';
DELETE FROM derived.city_spatial_information_unit_scores
WHERE source_method = '{RESULT_SOURCE_METHOD}'
  AND neighborhood_level = '{level}';
DELETE FROM derived.city_spatial_information_category_scores
WHERE source_method = '{RESULT_SOURCE_METHOD}'
  AND neighborhood_level = '{level}';
"""
    sql = f"""
\\set ON_ERROR_STOP on
BEGIN;
{delete_sql}
COMMIT;

BEGIN;
CREATE TEMP TABLE tmp_source_counts ON COMMIT DROP AS
{_source_counts_sql(level)};

CREATE INDEX tmp_source_counts_city_family_idx
    ON tmp_source_counts (city_code, family);
CREATE INDEX tmp_source_counts_unit_idx
    ON tmp_source_counts (city_code, family, unit_code);
CREATE INDEX tmp_source_counts_category_idx
    ON tmp_source_counts (city_code, family, category);

CREATE TEMP TABLE tmp_city_tot ON COMMIT DROP AS
SELECT
    city_code,
    MAX(city_name) AS city_name,
    MAX(state_code) AS state_code,
    family,
    SUM(est_count)::double precision AS total_count
FROM tmp_source_counts
GROUP BY city_code, family;

CREATE TEMP TABLE tmp_unit_tot ON COMMIT DROP AS
SELECT
    city_code,
    family,
    unit_code,
    MAX(unit_label) AS unit_label,
    SUM(est_count)::double precision AS unit_total
FROM tmp_source_counts
GROUP BY city_code, family, unit_code;

CREATE TEMP TABLE tmp_cat_tot ON COMMIT DROP AS
SELECT
    city_code,
    family,
    category,
    SUM(est_count)::double precision AS category_total
FROM tmp_source_counts
GROUP BY city_code, family, category;

CREATE TEMP TABLE tmp_pair_base ON COMMIT DROP AS
SELECT
    s.city_code,
    ct.city_name,
    ct.state_code,
    s.family,
    s.unit_code,
    ut.unit_label,
    s.category,
    s.est_count,
    ct.total_count,
    ut.unit_total,
    gt.category_total,
    s.est_count / ct.total_count AS p_joint,
    ut.unit_total / ct.total_count AS p_unit,
    gt.category_total / ct.total_count AS p_category,
    s.est_count / ut.unit_total AS share_within_unit,
    s.est_count / gt.category_total AS share_within_category,
    (s.est_count * ct.total_count) / (ut.unit_total * gt.category_total) AS lift,
    LN((s.est_count * ct.total_count) / (ut.unit_total * gt.category_total)) AS log_lift_nats,
    (s.est_count / ct.total_count) * LN((s.est_count * ct.total_count) / (ut.unit_total * gt.category_total)) AS mi_term_nats
FROM tmp_source_counts s
JOIN tmp_city_tot ct
  ON ct.city_code = s.city_code
 AND ct.family = s.family
JOIN tmp_unit_tot ut
  ON ut.city_code = s.city_code
 AND ut.family = s.family
 AND ut.unit_code = s.unit_code
JOIN tmp_cat_tot gt
  ON gt.city_code = s.city_code
 AND gt.family = s.family
 AND gt.category = s.category;

CREATE INDEX tmp_pair_base_city_idx
    ON tmp_pair_base (city_code, family);
CREATE INDEX tmp_pair_base_unit_idx
    ON tmp_pair_base (city_code, family, unit_code);
CREATE INDEX tmp_pair_base_category_idx
    ON tmp_pair_base (city_code, family, category);

INSERT INTO derived.city_spatial_information_pair_lift (
    source_file, source_method, neighborhood_level, family, city_code, city_name, state_code,
    unit_code, unit_label, category, est_count, total_count, unit_total, category_total,
    p_joint, p_unit, p_category, share_within_unit, share_within_category,
    lift, log_lift_nats, mi_term_nats, notes
)
SELECT
    'derived.ageb_economic_mix|raw.population_units|raw.denue_establishments',
    '{RESULT_SOURCE_METHOD}',
    '{level}',
    family,
    city_code,
    city_name,
    state_code,
    unit_code,
    unit_label,
    category,
    est_count,
    total_count,
    unit_total,
    category_total,
    p_joint,
    p_unit,
    p_category,
    share_within_unit,
    share_within_category,
    lift,
    log_lift_nats,
    mi_term_nats,
    ''
FROM tmp_pair_base;

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY city_code, family, unit_code
            ORDER BY share_within_unit DESC, category
        ) AS rn
    FROM tmp_pair_base
),
unit_scores AS (
    SELECT
        city_code,
        MAX(city_name) AS city_name,
        MAX(state_code) AS state_code,
        family,
        unit_code,
        MAX(unit_label) AS unit_label,
        MAX(unit_total) AS unit_total,
        MAX(p_unit) AS p_unit,
        -SUM(share_within_unit * LN(share_within_unit))::double precision AS local_entropy_nats,
        EXP(-SUM(share_within_unit * LN(share_within_unit)))::double precision AS effective_categories_in_unit,
        SUM(share_within_unit * LN(share_within_unit / p_category))::double precision AS kl_to_city_nats,
        SUM(mi_term_nats)::double precision AS mi_contribution_nats,
        MAX(CASE WHEN rn = 1 THEN category ELSE '' END) AS dominant_category,
        MAX(CASE WHEN rn = 1 THEN share_within_unit ELSE NULL END)::double precision AS dominant_category_share
    FROM ranked
    GROUP BY city_code, family, unit_code
)
INSERT INTO derived.city_spatial_information_unit_scores (
    source_file, source_method, neighborhood_level, family, city_code, city_name, state_code,
    unit_code, unit_label, unit_total, p_unit, local_entropy_nats,
    effective_categories_in_unit, kl_to_city_nats, mi_contribution_nats,
    dominant_category, dominant_category_share, notes
)
SELECT
    'derived.city_spatial_information_pair_lift',
    '{RESULT_SOURCE_METHOD}',
    '{level}',
    family,
    city_code,
    city_name,
    state_code,
    unit_code,
    unit_label,
    unit_total,
    p_unit,
    local_entropy_nats,
    effective_categories_in_unit,
    kl_to_city_nats,
    mi_contribution_nats,
    dominant_category,
    dominant_category_share,
    ''
FROM unit_scores;

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY city_code, family, category
            ORDER BY share_within_category DESC, unit_code
        ) AS rn
    FROM tmp_pair_base
),
category_scores AS (
    SELECT
        city_code,
        MAX(city_name) AS city_name,
        MAX(state_code) AS state_code,
        family,
        category,
        MAX(category_total) AS category_total,
        MAX(p_category) AS p_category,
        -SUM(share_within_category * LN(share_within_category))::double precision AS localization_entropy_nats,
        EXP(-SUM(share_within_category * LN(share_within_category)))::double precision AS effective_units_for_category,
        SUM(share_within_category * LN(share_within_category / p_unit))::double precision AS kl_to_units_nats,
        SUM(mi_term_nats)::double precision AS mi_contribution_nats,
        MAX(CASE WHEN rn = 1 THEN unit_code ELSE '' END) AS dominant_unit_code,
        MAX(CASE WHEN rn = 1 THEN unit_label ELSE '' END) AS dominant_unit_label,
        MAX(CASE WHEN rn = 1 THEN share_within_category ELSE NULL END)::double precision AS dominant_unit_share
    FROM ranked
    GROUP BY city_code, family, category
)
INSERT INTO derived.city_spatial_information_category_scores (
    source_file, source_method, neighborhood_level, family, city_code, city_name, state_code,
    category, category_total, p_category, localization_entropy_nats,
    effective_units_for_category, kl_to_units_nats, mi_contribution_nats,
    dominant_unit_code, dominant_unit_label, dominant_unit_share, notes
)
SELECT
    'derived.city_spatial_information_pair_lift',
    '{RESULT_SOURCE_METHOD}',
    '{level}',
    family,
    city_code,
    city_name,
    state_code,
    category,
    category_total,
    p_category,
    localization_entropy_nats,
    effective_units_for_category,
    kl_to_units_nats,
    mi_contribution_nats,
    dominant_unit_code,
    dominant_unit_label,
    dominant_unit_share,
    ''
FROM category_scores;
COMMIT;
"""
    _psql_exec(sql)


def _summary_query() -> str:
    return f"""
SELECT *
FROM (
    SELECT 'pair_lift'::text AS table_name, neighborhood_level, family, COUNT(*)::text AS rows, COUNT(DISTINCT city_code)::text AS cities
    FROM derived.city_spatial_information_pair_lift
    WHERE source_method = '{RESULT_SOURCE_METHOD}'
    GROUP BY neighborhood_level, family
    UNION ALL
    SELECT 'unit_scores'::text AS table_name, neighborhood_level, family, COUNT(*)::text AS rows, COUNT(DISTINCT city_code)::text AS cities
    FROM derived.city_spatial_information_unit_scores
    WHERE source_method = '{RESULT_SOURCE_METHOD}'
    GROUP BY neighborhood_level, family
    UNION ALL
    SELECT 'category_scores'::text AS table_name, neighborhood_level, family, COUNT(*)::text AS rows, COUNT(DISTINCT city_code)::text AS cities
    FROM derived.city_spatial_information_category_scores
    WHERE source_method = '{RESULT_SOURCE_METHOD}'
    GROUP BY neighborhood_level, family
) q
ORDER BY 1, 2, 3
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Persist Bettencourt-style neighborhood selection details: w(j,l), D(j), and D(l).")
    parser.add_argument("--levels", nargs="*", choices=("ageb", "manzana"), default=["ageb", "manzana"])
    parser.add_argument("--no-refresh", action="store_true", help="Append without deleting previous rows for this source method and level.")
    args = parser.parse_args()

    _bootstrap()
    refresh = not args.no_refresh
    for level in args.levels:
        _persist_level(level, refresh=refresh)

    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "spatial-information-selection-details-2026-04-25"
    outdir.mkdir(parents=True, exist_ok=True)
    summary_rows = _psql_copy(_summary_query())
    typed_rows: list[dict[str, object]] = []
    for row in summary_rows:
        typed_rows.append(
            {
                "table_name": row["table_name"],
                "neighborhood_level": row["neighborhood_level"],
                "family": row["family"],
                "rows": int(row["rows"]),
                "cities": int(row["cities"]),
            }
        )
    _write_csv(outdir / "summary.csv", typed_rows, ["table_name", "neighborhood_level", "family", "rows", "cities"])
    report = [
        "# Spatial Information Selection Details",
        "",
        "This materializes the neighborhood-selection layer implied by Bettencourt Lecture 11 using economic categories instead of income groups.",
        "",
        "Persisted objects:",
        f"- `derived.city_spatial_information_pair_lift` with `w_{{j\\ell}}` / lift terms for every nonzero `(unit, category)` pair under `{RESULT_SOURCE_METHOD}`",
        f"- `derived.city_spatial_information_unit_scores` with unit-specific `D(j)` and local entropy",
        f"- `derived.city_spatial_information_category_scores` with category-specific `D(\\ell)` and localization entropy",
        "",
        "Files:",
        f"- [summary.csv]({(outdir / 'summary.csv').resolve()})",
        "",
    ]
    (outdir / "report.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    print(f"[selection-details] done -> {outdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

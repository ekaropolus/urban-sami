#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import shutil
import subprocess
import time
from pathlib import Path


WINDOWS_DOCKER = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DOCKER_EXE = os.environ.get("DOCKER_EXE") or shutil.which("docker") or WINDOWS_DOCKER
DB_CONTAINER = os.environ.get("DB_CONTAINER", "24-polisplexity-core-db-dev")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
DB_NAME = os.environ.get("DB_NAME", "urban_sami_exp")

PAIR_SOURCE_METHOD = "bettencourt_spatial_information_selection_v1"
SUMMARY_SOURCE_METHOD = "bettencourt_spatial_information_phase1_v1"
RESULT_SOURCE_METHOD = "bettencourt_spatial_information_multiscale_v2"

ROOT = Path(__file__).resolve().parents[1]
OUTDIR = ROOT / "reports" / "spatial-information-multiscale-decomposition-2026-04-25"
LOG_PATH = ROOT / "logs" / "spatial_information_multiscale_decomposition_v2.log"


def _log(message: str) -> None:
    stamp = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{stamp}] {message}"
    print(line, flush=True)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")


def _psql(sql: str, *, capture_output: bool = False) -> str:
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
    if capture_output:
        cmd.extend(["-AtF", "\t"])
    proc = subprocess.run(cmd, input=sql, text=True, capture_output=capture_output, check=True)
    return proc.stdout if capture_output else ""


def _bootstrap() -> None:
    script = ROOT / "scripts" / "bootstrap_experiment_db.sh"
    subprocess.run(["bash", str(script)], check=True)


def _sql_text(value: object) -> str:
    return str(value).replace("'", "''")


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _set_status(
    *,
    city_code: str,
    city_name: str,
    state_code: str,
    family: str,
    status: str,
    ageb_rows: int | None = None,
    decomposition_rows: int | None = None,
    error_message: str = "",
    notes: str = "",
    touch_finished: bool = False,
) -> None:
    finished_sql = "NOW()" if touch_finished else "NULL"
    _psql(
        f"""
        INSERT INTO experiments.spatial_information_decomposition_status (
            source_method, city_code, city_name, state_code, family, status,
            started_at, finished_at, ageb_rows, decomposition_rows, error_message, notes
        ) VALUES (
            '{RESULT_SOURCE_METHOD}',
            '{_sql_text(city_code)}',
            '{_sql_text(city_name)}',
            '{_sql_text(state_code)}',
            '{_sql_text(family)}',
            '{_sql_text(status)}',
            NOW(),
            {finished_sql},
            {('NULL' if ageb_rows is None else str(int(ageb_rows)))},
            {('NULL' if decomposition_rows is None else str(int(decomposition_rows)))},
            '{_sql_text(error_message[:1000])}',
            '{_sql_text(notes[:1000])}'
        )
        ON CONFLICT (source_method, family, city_code) DO UPDATE SET
            city_name = EXCLUDED.city_name,
            state_code = EXCLUDED.state_code,
            status = EXCLUDED.status,
            ageb_rows = EXCLUDED.ageb_rows,
            decomposition_rows = EXCLUDED.decomposition_rows,
            error_message = EXCLUDED.error_message,
            notes = EXCLUDED.notes,
            finished_at = EXCLUDED.finished_at,
            started_at = CASE
                WHEN EXCLUDED.status = 'running' THEN NOW()
                ELSE experiments.spatial_information_decomposition_status.started_at
            END;
        """
    )


def _work_rows(retry_errors_only: bool) -> list[dict[str, str]]:
    if retry_errors_only:
        sql = f"""
        SELECT
            s.city_code,
            COALESCE(MAX(s.city_name), MAX(m.city_name), '') AS city_name,
            COALESCE(MAX(s.state_code), MAX(m.state_code), '') AS state_code,
            s.family
        FROM experiments.spatial_information_decomposition_status s
        LEFT JOIN derived.city_spatial_information_summary m
          ON m.source_method = '{SUMMARY_SOURCE_METHOD}'
         AND m.neighborhood_level = 'manzana'
         AND m.city_code = s.city_code
         AND m.family = s.family
        WHERE s.source_method = '{RESULT_SOURCE_METHOD}'
          AND s.status = 'error'
        GROUP BY s.city_code, s.family
        ORDER BY s.city_code, s.family;
        """
    else:
        sql = f"""
        SELECT
            city_code,
            MAX(city_name) AS city_name,
            MAX(state_code) AS state_code,
            family
        FROM derived.city_spatial_information_summary
        WHERE source_method = '{SUMMARY_SOURCE_METHOD}'
          AND neighborhood_level = 'manzana'
        GROUP BY city_code, family
        ORDER BY city_code, family;
        """
    out = _psql(sql, capture_output=True)
    rows: list[dict[str, str]] = []
    for line in out.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        while len(parts) < 4:
            parts.append("")
        city_code, city_name, state_code, family = parts[:4]
        rows.append(
            {
                "city_code": city_code.strip(),
                "city_name": city_name.strip(),
                "state_code": state_code.strip(),
                "family": family.strip(),
            }
        )
    return rows


def _marked_success(city_code: str, family: str) -> bool:
    out = _psql(
        f"""
        SELECT COUNT(*)
        FROM experiments.spatial_information_decomposition_status
        WHERE source_method = '{RESULT_SOURCE_METHOD}'
          AND city_code = '{_sql_text(city_code)}'
          AND family = '{_sql_text(family)}'
          AND status = 'success';
        """,
        capture_output=True,
    ).strip()
    return int(out or "0") > 0


def _reset_results() -> None:
    _psql(
        f"""
        DELETE FROM derived.ageb_spatial_information_within
        WHERE source_method = '{RESULT_SOURCE_METHOD}';
        DELETE FROM derived.city_spatial_information_decomposition
        WHERE source_method = '{RESULT_SOURCE_METHOD}';
        DELETE FROM experiments.spatial_information_decomposition_status
        WHERE source_method = '{RESULT_SOURCE_METHOD}';
        """
    )


def _count_result_rows(city_code: str, family: str) -> tuple[int, int]:
    out = _psql(
        f"""
        SELECT
            COALESCE((SELECT COUNT(*)
                      FROM derived.ageb_spatial_information_within
                      WHERE source_method = '{RESULT_SOURCE_METHOD}'
                        AND city_code = '{_sql_text(city_code)}'
                        AND family = '{_sql_text(family)}'), 0),
            COALESCE((SELECT COUNT(*)
                      FROM derived.city_spatial_information_decomposition
                      WHERE source_method = '{RESULT_SOURCE_METHOD}'
                        AND city_code = '{_sql_text(city_code)}'
                        AND family = '{_sql_text(family)}'), 0);
        """,
        capture_output=True,
    ).strip()
    ageb_rows, decomposition_rows = (out.split("\t", 1) + ["0"])[:2]
    return int(ageb_rows or "0"), int(decomposition_rows or "0")


def _persist_city_family(city_code: str, family: str) -> None:
    sql = f"""
\\set ON_ERROR_STOP on
BEGIN;
DELETE FROM derived.ageb_spatial_information_within
WHERE source_method = '{RESULT_SOURCE_METHOD}'
  AND city_code = '{_sql_text(city_code)}'
  AND family = '{_sql_text(family)}';

DELETE FROM derived.city_spatial_information_decomposition
WHERE source_method = '{RESULT_SOURCE_METHOD}'
  AND city_code = '{_sql_text(city_code)}'
  AND family = '{_sql_text(family)}';

CREATE TEMP TABLE tmp_units ON COMMIT DROP AS
SELECT
    u.city_code,
    u.city_name,
    u.state_code,
    substring(u.unit_code from 1 for 13) AS ageb_code,
    u.unit_code AS manzana_code,
    u.unit_label AS manzana_label,
    u.unit_total,
    u.p_unit
FROM derived.city_spatial_information_unit_scores u
WHERE u.source_method = '{PAIR_SOURCE_METHOD}'
  AND u.neighborhood_level = 'manzana'
  AND u.city_code = '{_sql_text(city_code)}'
  AND u.family = '{_sql_text(family)}';

CREATE TEMP TABLE tmp_pairs ON COMMIT DROP AS
SELECT
    p.city_code,
    p.city_name,
    p.state_code,
    substring(p.unit_code from 1 for 13) AS ageb_code,
    p.unit_code AS manzana_code,
    p.unit_label AS manzana_label,
    p.category,
    p.est_count
FROM derived.city_spatial_information_pair_lift p
WHERE p.source_method = '{PAIR_SOURCE_METHOD}'
  AND p.neighborhood_level = 'manzana'
  AND p.city_code = '{_sql_text(city_code)}'
  AND p.family = '{_sql_text(family)}';

CREATE INDEX tmp_units_ageb_idx
    ON tmp_units (ageb_code, manzana_code);
CREATE INDEX tmp_pairs_ageb_idx
    ON tmp_pairs (ageb_code, manzana_code);
CREATE INDEX tmp_pairs_ageb_cat_idx
    ON tmp_pairs (ageb_code, category);

CREATE TEMP TABLE tmp_ageb_tot ON COMMIT DROP AS
SELECT
    u.city_code,
    MAX(u.city_name) AS city_name,
    MAX(u.state_code) AS state_code,
    u.ageb_code,
    COALESCE(MAX(a.unit_label), u.ageb_code) AS ageb_label,
    SUM(u.unit_total)::double precision AS ageb_total,
    SUM(u.p_unit)::double precision AS p_ageb
FROM tmp_units u
LEFT JOIN raw.admin_units a
  ON a.level = 'ageb_u'
 AND a.city_code = u.city_code
 AND a.unit_code = u.ageb_code
GROUP BY u.city_code, u.ageb_code;

CREATE TEMP TABLE tmp_ageb_cat_tot ON COMMIT DROP AS
SELECT
    city_code,
    ageb_code,
    category,
    SUM(est_count)::double precision AS ageb_category_total
FROM tmp_pairs
GROUP BY city_code, ageb_code, category;

CREATE TEMP TABLE tmp_pair_cond ON COMMIT DROP AS
SELECT
    p.city_code,
    at.city_name,
    at.state_code,
    '{_sql_text(family)}'::text AS family,
    p.ageb_code,
    at.ageb_label,
    p.manzana_code,
    MAX(p.manzana_label) AS manzana_label,
    p.category,
    SUM(p.est_count)::double precision AS est_count,
    at.ageb_total,
    u.unit_total AS manzana_total,
    gt.ageb_category_total,
    SUM(p.est_count)::double precision / at.ageb_total AS p_joint_cond,
    u.unit_total / at.ageb_total AS p_manz_cond,
    gt.ageb_category_total / at.ageb_total AS p_cat_cond,
    SUM(p.est_count)::double precision / u.unit_total AS share_within_manzana,
    SUM(p.est_count)::double precision / gt.ageb_category_total AS share_within_ageb_category,
    (SUM(p.est_count)::double precision * at.ageb_total) / (u.unit_total * gt.ageb_category_total) AS cond_lift,
    (SUM(p.est_count)::double precision / at.ageb_total) * LN((SUM(p.est_count)::double precision * at.ageb_total) / (u.unit_total * gt.ageb_category_total)) AS mi_term_cond_nats
FROM tmp_pairs p
JOIN tmp_units u
  ON u.city_code = p.city_code
 AND u.ageb_code = p.ageb_code
 AND u.manzana_code = p.manzana_code
JOIN tmp_ageb_tot at
  ON at.city_code = p.city_code
 AND at.ageb_code = p.ageb_code
JOIN tmp_ageb_cat_tot gt
  ON gt.city_code = p.city_code
 AND gt.ageb_code = p.ageb_code
 AND gt.category = p.category
GROUP BY
    p.city_code, at.city_name, at.state_code, p.ageb_code, at.ageb_label,
    p.manzana_code, p.category, at.ageb_total, u.unit_total, gt.ageb_category_total;

WITH manz_entropy AS (
    SELECT
        at.city_code,
        '{_sql_text(family)}'::text AS family,
        at.ageb_code,
        -SUM((u.unit_total / at.ageb_total) * LN(u.unit_total / at.ageb_total))::double precision AS h_manzanas_cond_nats,
        COUNT(*)::double precision AS n_manzanas_nonzero,
        EXP(-SUM((u.unit_total / at.ageb_total) * LN(u.unit_total / at.ageb_total)))::double precision AS effective_manzanas
    FROM tmp_units u
    JOIN tmp_ageb_tot at
      ON at.city_code = u.city_code
     AND at.ageb_code = u.ageb_code
    GROUP BY at.city_code, at.ageb_code
),
cat_entropy AS (
    SELECT
        at.city_code,
        '{_sql_text(family)}'::text AS family,
        at.ageb_code,
        -SUM((gt.ageb_category_total / at.ageb_total) * LN(gt.ageb_category_total / at.ageb_total))::double precision AS h_categories_cond_nats,
        COUNT(*)::double precision AS n_categories_nonzero,
        EXP(-SUM((gt.ageb_category_total / at.ageb_total) * LN(gt.ageb_category_total / at.ageb_total)))::double precision AS effective_categories
    FROM tmp_ageb_cat_tot gt
    JOIN tmp_ageb_tot at
      ON at.city_code = gt.city_code
     AND at.ageb_code = gt.ageb_code
    GROUP BY at.city_code, at.ageb_code
),
dominant_manz AS (
    SELECT *
    FROM (
        SELECT
            u.city_code,
            '{_sql_text(family)}'::text AS family,
            u.ageb_code,
            u.manzana_code,
            u.manzana_label,
            u.unit_total / at.ageb_total AS dominant_manzana_share,
            ROW_NUMBER() OVER (
                PARTITION BY u.city_code, u.ageb_code
                ORDER BY u.unit_total / at.ageb_total DESC, u.manzana_code
            ) AS rn
        FROM tmp_units u
        JOIN tmp_ageb_tot at
          ON at.city_code = u.city_code
         AND at.ageb_code = u.ageb_code
    ) z
    WHERE rn = 1
),
ageb_within AS (
    SELECT
        at.city_code,
        at.city_name,
        at.state_code,
        '{_sql_text(family)}'::text AS family,
        at.ageb_code,
        at.ageb_label,
        at.ageb_total,
        at.p_ageb,
        me.n_manzanas_nonzero,
        ce.n_categories_nonzero,
        SUM(pc.mi_term_cond_nats)::double precision AS conditional_mi_nats,
        at.p_ageb * SUM(pc.mi_term_cond_nats)::double precision AS weighted_conditional_mi_nats,
        me.h_manzanas_cond_nats,
        ce.h_categories_cond_nats,
        CASE
            WHEN LEAST(me.h_manzanas_cond_nats, ce.h_categories_cond_nats) > 0
            THEN SUM(pc.mi_term_cond_nats) / LEAST(me.h_manzanas_cond_nats, ce.h_categories_cond_nats)
            ELSE NULL
        END::double precision AS nmi_min_cond,
        me.effective_manzanas,
        ce.effective_categories,
        dm.manzana_code AS dominant_manzana_code,
        dm.manzana_label AS dominant_manzana_label,
        dm.dominant_manzana_share
    FROM tmp_pair_cond pc
    JOIN tmp_ageb_tot at
      ON at.city_code = pc.city_code
     AND at.ageb_code = pc.ageb_code
    JOIN manz_entropy me
      ON me.city_code = at.city_code
     AND me.ageb_code = at.ageb_code
    JOIN cat_entropy ce
      ON ce.city_code = at.city_code
     AND ce.ageb_code = at.ageb_code
    JOIN dominant_manz dm
      ON dm.city_code = at.city_code
     AND dm.ageb_code = at.ageb_code
    GROUP BY
        at.city_code, at.city_name, at.state_code, at.ageb_code, at.ageb_label,
        at.ageb_total, at.p_ageb,
        me.n_manzanas_nonzero, ce.n_categories_nonzero,
        me.h_manzanas_cond_nats, ce.h_categories_cond_nats,
        me.effective_manzanas, ce.effective_categories,
        dm.manzana_code, dm.manzana_label, dm.dominant_manzana_share
)
INSERT INTO derived.ageb_spatial_information_within (
    source_file, source_method, family, city_code, city_name, state_code, ageb_code, ageb_label,
    ageb_total, p_ageb, n_manzanas_nonzero, n_categories_nonzero,
    conditional_mi_nats, weighted_conditional_mi_nats,
    h_manzanas_cond_nats, h_categories_cond_nats, nmi_min_cond,
    effective_manzanas, effective_categories, dominant_manzana_code,
    dominant_manzana_label, dominant_manzana_share, notes
)
SELECT
    'raw.population_units|raw.admin_units|raw.denue_establishments',
    '{RESULT_SOURCE_METHOD}',
    family,
    city_code,
    city_name,
    state_code,
    ageb_code,
    ageb_label,
    ageb_total,
    p_ageb,
    n_manzanas_nonzero,
    n_categories_nonzero,
    conditional_mi_nats,
    weighted_conditional_mi_nats,
    h_manzanas_cond_nats,
    h_categories_cond_nats,
    nmi_min_cond,
    effective_manzanas,
    effective_categories,
    dominant_manzana_code,
    dominant_manzana_label,
    dominant_manzana_share,
    ''
FROM ageb_within;

WITH city_within AS (
    SELECT
        city_code,
        MAX(city_name) AS city_name,
        MAX(state_code) AS state_code,
        '{_sql_text(family)}'::text AS family,
        SUM(weighted_conditional_mi_nats)::double precision AS mi_within_ageb_nats,
        COUNT(*)::double precision AS n_ageb_nonzero,
        SUM(n_manzanas_nonzero)::double precision AS n_manzanas_nonzero
    FROM derived.ageb_spatial_information_within
    WHERE source_method = '{RESULT_SOURCE_METHOD}'
      AND city_code = '{_sql_text(city_code)}'
      AND family = '{_sql_text(family)}'
    GROUP BY city_code, family
),
ageb_summary AS (
    SELECT city_code, family, mi_nats, n_units_nonzero
    FROM derived.city_spatial_information_summary
    WHERE source_method = '{SUMMARY_SOURCE_METHOD}'
      AND neighborhood_level = 'ageb'
      AND city_code = '{_sql_text(city_code)}'
      AND family = '{_sql_text(family)}'
),
manz_summary AS (
    SELECT city_code, family, mi_nats, n_units_nonzero
    FROM derived.city_spatial_information_summary
    WHERE source_method = '{SUMMARY_SOURCE_METHOD}'
      AND neighborhood_level = 'manzana'
      AND city_code = '{_sql_text(city_code)}'
      AND family = '{_sql_text(family)}'
)
INSERT INTO derived.city_spatial_information_decomposition (
    source_file, source_method, family, city_code, city_name, state_code,
    mi_manzana_nats, mi_ageb_nats, mi_within_ageb_nats,
    share_between_ageb, share_within_ageb, identity_gap_nats,
    n_ageb_nonzero, n_manzanas_nonzero, notes
)
SELECT
    'derived.city_spatial_information_summary|derived.ageb_spatial_information_within',
    '{RESULT_SOURCE_METHOD}',
    cw.family,
    cw.city_code,
    cw.city_name,
    cw.state_code,
    ms.mi_nats,
    ag.mi_nats,
    cw.mi_within_ageb_nats,
    CASE WHEN ms.mi_nats > 0 THEN ag.mi_nats / ms.mi_nats ELSE NULL END AS share_between_ageb,
    CASE WHEN ms.mi_nats > 0 THEN cw.mi_within_ageb_nats / ms.mi_nats ELSE NULL END AS share_within_ageb,
    ms.mi_nats - ag.mi_nats - cw.mi_within_ageb_nats AS identity_gap_nats,
    cw.n_ageb_nonzero,
    cw.n_manzanas_nonzero,
    ''
FROM city_within cw
JOIN ageb_summary ag
  ON ag.city_code = cw.city_code
 AND ag.family = cw.family
JOIN manz_summary ms
  ON ms.city_code = cw.city_code
 AND ms.family = cw.family;
COMMIT;
"""
    _psql(sql)


def _summary_rows() -> list[dict[str, object]]:
    sql = f"""
    SELECT 'status_success'::text AS metric, COUNT(*)::text AS value
    FROM experiments.spatial_information_decomposition_status
    WHERE source_method = '{RESULT_SOURCE_METHOD}' AND status = 'success'
    UNION ALL
    SELECT 'status_error', COUNT(*)::text
    FROM experiments.spatial_information_decomposition_status
    WHERE source_method = '{RESULT_SOURCE_METHOD}' AND status = 'error'
    UNION ALL
    SELECT 'status_running', COUNT(*)::text
    FROM experiments.spatial_information_decomposition_status
    WHERE source_method = '{RESULT_SOURCE_METHOD}' AND status = 'running'
    UNION ALL
    SELECT 'ageb_within_rows', COUNT(*)::text
    FROM derived.ageb_spatial_information_within
    WHERE source_method = '{RESULT_SOURCE_METHOD}'
    UNION ALL
    SELECT 'city_decomposition_rows', COUNT(*)::text
    FROM derived.city_spatial_information_decomposition
    WHERE source_method = '{RESULT_SOURCE_METHOD}'
    ORDER BY 1;
    """
    rows: list[dict[str, object]] = []
    for line in _psql(sql, capture_output=True).splitlines():
        if not line.strip():
            continue
        metric, value = (line.split("\t", 1) + [""])[:2]
        rows.append({"metric": metric, "value": value})
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Incremental multiscale spatial-information decomposition with status and logs.")
    parser.add_argument("--retry-errors-only", action="store_true", help="Process only city/family pairs currently marked as error.")
    parser.add_argument("--reset", action="store_true", help="Delete previous results/status for this source method before running.")
    parser.add_argument("--limit", type=int, default=0, help="Optional max number of city/family pairs to process.")
    args = parser.parse_args()

    _bootstrap()
    OUTDIR.mkdir(parents=True, exist_ok=True)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    if args.reset:
        _log(f"resetting prior results for {RESULT_SOURCE_METHOD}")
        _reset_results()

    work_rows = _work_rows(args.retry_errors_only)
    if args.limit:
        work_rows = work_rows[: args.limit]
    total = len(work_rows)
    _log(f"worklist ready pairs={total} source_method={RESULT_SOURCE_METHOD}")

    progress_rows: list[dict[str, object]] = []
    completed = 0
    for idx, row in enumerate(work_rows, start=1):
        city_code = row["city_code"]
        city_name = row["city_name"]
        state_code = row["state_code"]
        family = row["family"]

        if not args.retry_errors_only and _marked_success(city_code, family):
            ageb_rows, decomposition_rows = _count_result_rows(city_code, family)
            progress_rows.append(
                {
                    "city_code": city_code,
                    "city_name": city_name,
                    "state_code": state_code,
                    "family": family,
                    "status": "skipped_existing",
                    "ageb_rows": ageb_rows,
                    "decomposition_rows": decomposition_rows,
                    "elapsed_seconds": 0.0,
                }
            )
            _write_csv(OUTDIR / "progress.csv", progress_rows, list(progress_rows[0].keys()))
            _log(f"[{idx}/{total}] skip city={city_code} family={family} ageb_rows={ageb_rows} decomposition_rows={decomposition_rows}")
            continue

        started = time.perf_counter()
        _set_status(
            city_code=city_code,
            city_name=city_name,
            state_code=state_code,
            family=family,
            status="running",
            notes="started",
        )
        try:
            _persist_city_family(city_code, family)
            ageb_rows, decomposition_rows = _count_result_rows(city_code, family)
            elapsed = round(time.perf_counter() - started, 3)
            _set_status(
                city_code=city_code,
                city_name=city_name,
                state_code=state_code,
                family=family,
                status="success",
                ageb_rows=ageb_rows,
                decomposition_rows=decomposition_rows,
                notes=f"elapsed_seconds={elapsed}",
                touch_finished=True,
            )
            progress_rows.append(
                {
                    "city_code": city_code,
                    "city_name": city_name,
                    "state_code": state_code,
                    "family": family,
                    "status": "success",
                    "ageb_rows": ageb_rows,
                    "decomposition_rows": decomposition_rows,
                    "elapsed_seconds": elapsed,
                }
            )
            _write_csv(OUTDIR / "progress.csv", progress_rows, list(progress_rows[0].keys()))
            completed += 1
            _log(
                f"[{idx}/{total}] ok city={city_code} family={family} "
                f"ageb_rows={ageb_rows} decomposition_rows={decomposition_rows} "
                f"elapsed={elapsed}s completed={completed}"
            )
        except Exception as exc:
            elapsed = round(time.perf_counter() - started, 3)
            _set_status(
                city_code=city_code,
                city_name=city_name,
                state_code=state_code,
                family=family,
                status="error",
                error_message=str(exc),
                notes=f"elapsed_seconds={elapsed}",
                touch_finished=True,
            )
            progress_rows.append(
                {
                    "city_code": city_code,
                    "city_name": city_name,
                    "state_code": state_code,
                    "family": family,
                    "status": "error",
                    "ageb_rows": "",
                    "decomposition_rows": "",
                    "elapsed_seconds": elapsed,
                }
            )
            _write_csv(OUTDIR / "progress.csv", progress_rows, list(progress_rows[0].keys()))
            _log(f"[{idx}/{total}] error city={city_code} family={family} elapsed={elapsed}s msg={exc}")

    summary_rows = _summary_rows()
    _write_csv(OUTDIR / "summary.csv", summary_rows, ["metric", "value"])
    report = [
        "# Spatial Information Multiscale Decomposition",
        "",
        "This materializes the identity",
        "",
        "```math",
        "I(M;\\\\Lambda)=I(A;\\\\Lambda)+I(M;\\\\Lambda\\\\mid A)",
        "```",
        "",
        "with `M = manzana`, `A = AGEB`, and economic categories as `\\\\Lambda`.",
        "",
        "Persisted objects:",
        f"- `derived.ageb_spatial_information_within` under `{RESULT_SOURCE_METHOD}`",
        f"- `derived.city_spatial_information_decomposition` under `{RESULT_SOURCE_METHOD}`",
        f"- `experiments.spatial_information_decomposition_status` under `{RESULT_SOURCE_METHOD}`",
        "",
        "Files:",
        f"- [summary.csv]({(OUTDIR / 'summary.csv').resolve()})",
        f"- [progress.csv]({(OUTDIR / 'progress.csv').resolve()})",
        f"- [log]({LOG_PATH.resolve()})",
    ]
    (OUTDIR / "report.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    _log(f"done summary -> {(OUTDIR / 'summary.csv').resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

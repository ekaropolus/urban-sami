#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
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
RESULT_SOURCE_METHOD = "bettencourt_spatial_information_phase1_v1"
FAMILIES = ("scian2", "size_class")


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


def _bootstrap() -> None:
    script = Path(__file__).resolve().parents[1] / "scripts" / "bootstrap_experiment_db.sh"
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


def _summary_query(level: str) -> str:
    families_sql = ", ".join(f"'{x}'" for x in FAMILIES)
    if level == "ageb":
        source_counts = f"""
WITH source_counts AS (
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
),
unit_avail AS (
    SELECT
        city_code,
        MAX(city_name) AS city_name,
        LEFT(city_code, 2) AS state_code,
        COUNT(*)::double precision AS n_units_total
    FROM raw.admin_units
    WHERE level = 'ageb_u'
    GROUP BY city_code
)
"""
    elif level == "manzana":
        size_case = _size_case_sql("d.per_ocu")
        source_counts = f"""
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
),
source_counts AS (
    SELECT city_code, city_name, state_code, unit_code, unit_label, 'scian2'::text AS family, scian2 AS category, COUNT(*)::double precision AS est_count
    FROM denue_clean
    WHERE scian2 <> ''
    GROUP BY city_code, city_name, state_code, unit_code, unit_label, scian2
    UNION ALL
    SELECT city_code, city_name, state_code, unit_code, unit_label, 'size_class'::text AS family, size_class AS category, COUNT(*)::double precision AS est_count
    FROM denue_clean
    WHERE size_class <> '' AND size_class <> 'unknown'
    GROUP BY city_code, city_name, state_code, unit_code, unit_label, size_class
),
unit_avail AS (
    SELECT
        city_code,
        MAX(city_name) AS city_name,
        LEFT(city_code, 2) AS state_code,
        COUNT(*)::double precision AS n_units_total
    FROM raw.population_units
    WHERE level = 'manzana'
    GROUP BY city_code
)
"""
    else:
        raise ValueError(level)

    return f"""
{source_counts},
city_tot AS (
    SELECT city_code, MAX(city_name) AS city_name, MAX(state_code) AS state_code, family,
           SUM(est_count)::double precision AS total_count
    FROM source_counts
    GROUP BY city_code, family
),
unit_tot AS (
    SELECT city_code, family, unit_code, MAX(unit_label) AS unit_label,
           SUM(est_count)::double precision AS unit_total
    FROM source_counts
    GROUP BY city_code, family, unit_code
),
cat_tot AS (
    SELECT city_code, family, category, SUM(est_count)::double precision AS cat_total
    FROM source_counts
    GROUP BY city_code, family, category
),
mi_terms AS (
    SELECT
        s.city_code,
        s.family,
        (s.est_count / ct.total_count) * LN((s.est_count * ct.total_count) / (ut.unit_total * gt.cat_total)) AS mi_term
    FROM source_counts s
    JOIN city_tot ct
      ON ct.city_code = s.city_code
     AND ct.family = s.family
    JOIN unit_tot ut
      ON ut.city_code = s.city_code
     AND ut.family = s.family
     AND ut.unit_code = s.unit_code
    JOIN cat_tot gt
      ON gt.city_code = s.city_code
     AND gt.family = s.family
     AND gt.category = s.category
),
mi_summary AS (
    SELECT city_code, family, SUM(mi_term)::double precision AS mi_nats
    FROM mi_terms
    GROUP BY city_code, family
),
unit_entropy AS (
    SELECT
        u.city_code,
        u.family,
        -SUM((u.unit_total / ct.total_count) * LN(u.unit_total / ct.total_count))::double precision AS h_units_nats,
        COUNT(*)::double precision AS n_units_nonzero
    FROM unit_tot u
    JOIN city_tot ct
      ON ct.city_code = u.city_code
     AND ct.family = u.family
    GROUP BY u.city_code, u.family
),
cat_entropy AS (
    SELECT
        g.city_code,
        g.family,
        -SUM((g.cat_total / ct.total_count) * LN(g.cat_total / ct.total_count))::double precision AS h_categories_nats,
        COUNT(*)::double precision AS n_categories
    FROM cat_tot g
    JOIN city_tot ct
      ON ct.city_code = g.city_code
     AND ct.family = g.family
    GROUP BY g.city_code, g.family
)
SELECT
    '{level}'::text AS level,
    ct.city_code,
    ct.city_name,
    ct.state_code,
    ct.family,
    ROUND(ct.total_count::numeric, 6)::text AS total_count,
    ROUND(COALESCE(ua.n_units_total, 0)::numeric, 6)::text AS n_units_total,
    ROUND(ue.n_units_nonzero::numeric, 6)::text AS n_units_nonzero,
    ROUND(ce.n_categories::numeric, 6)::text AS n_categories,
    ROUND(ms.mi_nats::numeric, 12)::text AS mi_nats,
    ROUND((ms.mi_nats / LN(2.0))::numeric, 12)::text AS mi_bits,
    ROUND(ue.h_units_nats::numeric, 12)::text AS h_units_nats,
    ROUND(ce.h_categories_nats::numeric, 12)::text AS h_categories_nats,
    ROUND(
        CASE
            WHEN LEAST(ue.h_units_nats, ce.h_categories_nats) > 0
            THEN ms.mi_nats / LEAST(ue.h_units_nats, ce.h_categories_nats)
            ELSE NULL
        END::numeric,
        12
    )::text AS nmi_min,
    ROUND(EXP(ue.h_units_nats)::numeric, 6)::text AS effective_units,
    ROUND(EXP(ce.h_categories_nats)::numeric, 6)::text AS effective_categories,
    ROUND(
        CASE
            WHEN COALESCE(ua.n_units_total, 0) > 0
            THEN ue.n_units_nonzero / ua.n_units_total
            ELSE NULL
        END::numeric,
        12
    )::text AS unit_coverage
FROM city_tot ct
JOIN mi_summary ms
  ON ms.city_code = ct.city_code
 AND ms.family = ct.family
JOIN unit_entropy ue
  ON ue.city_code = ct.city_code
 AND ue.family = ct.family
JOIN cat_entropy ce
  ON ce.city_code = ct.city_code
 AND ce.family = ct.family
LEFT JOIN unit_avail ua
  ON ua.city_code = ct.city_code
ORDER BY ct.family, ms.mi_nats DESC, ct.city_code
"""


def _rows_to_typed(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for row in rows:
        out.append(
            {
                "level": row["level"],
                "city_code": row["city_code"],
                "city_name": row["city_name"],
                "state_code": row["state_code"],
                "family": row["family"],
                "total_count": float(row["total_count"]),
                "n_units_total": float(row["n_units_total"]),
                "n_units_nonzero": float(row["n_units_nonzero"]),
                "n_categories": float(row["n_categories"]),
                "mi_nats": float(row["mi_nats"]),
                "mi_bits": float(row["mi_bits"]),
                "h_units_nats": float(row["h_units_nats"]),
                "h_categories_nats": float(row["h_categories_nats"]),
                "nmi_min": float(row["nmi_min"]) if row["nmi_min"] not in {"", None} else None,
                "effective_units": float(row["effective_units"]),
                "effective_categories": float(row["effective_categories"]),
                "unit_coverage": float(row["unit_coverage"]) if row["unit_coverage"] not in {"", None} else None,
            }
        )
    return out


def _overlap_comparison(ageb_rows: list[dict[str, object]], manzana_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    ageb_map = {(r["city_code"], r["family"]): r for r in ageb_rows}
    out: list[dict[str, object]] = []
    for m in manzana_rows:
        key = (m["city_code"], m["family"])
        a = ageb_map.get(key)
        if not a:
            continue
        out.append(
            {
                "city_code": m["city_code"],
                "city_name": m["city_name"],
                "state_code": m["state_code"],
                "family": m["family"],
                "mi_nats_manzana": m["mi_nats"],
                "mi_nats_ageb": a["mi_nats"],
                "nmi_min_manzana": m["nmi_min"],
                "nmi_min_ageb": a["nmi_min"],
                "unit_coverage_manzana": m["unit_coverage"],
                "unit_coverage_ageb": a["unit_coverage"],
                "delta_mi_nats_ageb_minus_manzana": a["mi_nats"] - m["mi_nats"],
                "delta_nmi_min_ageb_minus_manzana": (a["nmi_min"] or 0.0) - (m["nmi_min"] or 0.0),
            }
        )
    out.sort(key=lambda r: (r["family"], r["delta_mi_nats_ageb_minus_manzana"]))
    return out


def _family_stats(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    stats: list[dict[str, object]] = []
    for level in sorted({r["level"] for r in rows}):
        for family in sorted({r["family"] for r in rows if r["level"] == level}):
            subset = [r for r in rows if r["level"] == level and r["family"] == family]
            subset.sort(key=lambda r: r["mi_nats"])
            n = len(subset)
            mean_mi = sum(r["mi_nats"] for r in subset) / n if n else 0.0
            mean_nmi = sum((r["nmi_min"] or 0.0) for r in subset) / n if n else 0.0
            mean_coverage = sum((r["unit_coverage"] or 0.0) for r in subset) / n if n else 0.0
            top = max(subset, key=lambda r: r["mi_nats"]) if subset else None
            stats.append(
                {
                    "level": level,
                    "family": family,
                    "n_cities": n,
                    "mean_mi_nats": mean_mi,
                    "mean_nmi_min": mean_nmi,
                    "mean_unit_coverage": mean_coverage,
                    "top_city_code": top["city_code"] if top else "",
                    "top_city_name": top["city_name"] if top else "",
                    "top_mi_nats": top["mi_nats"] if top else None,
                }
            )
    return stats


def _report(
    outdir: Path,
    stats: list[dict[str, object]],
    overlap: list[dict[str, object]],
    ageb_rows: list[dict[str, object]],
    manzana_rows: list[dict[str, object]],
) -> str:
    lines = [
        "# Spatial Information Phase 1",
        "",
        "This is the first Bettencourt-new-style information experiment using internal economic structure rather than income classes.",
        "",
        "For each city `i`, neighborhood unit `j`, and category `\\ell`, we use:",
        "",
        "```math",
        "p_i(j,\\ell)=\\frac{Y_{ij\\ell}}{\\sum_{j,\\ell}Y_{ij\\ell}}",
        "```",
        "",
        "and compute:",
        "",
        "```math",
        "I_i(J;\\Lambda)=\\sum_{j,\\ell} p_i(j,\\ell)\\log\\frac{p_i(j,\\ell)}{p_i(j)p_i(\\ell)}",
        "```",
        "",
        "with two neighborhood systems:",
        "- `manzana` for all loaded cities",
        "- `AGEB` for the current urban sample",
        "",
        "Files:",
        f"- [manzana_city_information_summary.csv]({(outdir / 'manzana_city_information_summary.csv').resolve()})",
        f"- [ageb_city_information_summary.csv]({(outdir / 'ageb_city_information_summary.csv').resolve()})",
        f"- [multiscale_overlap_comparison.csv]({(outdir / 'multiscale_overlap_comparison.csv').resolve()})",
        f"- [family_stats.csv]({(outdir / 'family_stats.csv').resolve()})",
        "",
    ]
    for row in stats:
        lines.extend(
            [
                f"## {row['level']} · {row['family']}",
                "",
                f"- cities: `{row['n_cities']}`",
                f"- mean `I(J;Λ)` in nats: `{row['mean_mi_nats']:.4f}`",
                f"- mean normalized MI: `{row['mean_nmi_min']:.4f}`",
                f"- mean unit coverage: `{row['mean_unit_coverage']:.4f}`",
                f"- top city by MI: `{row['top_city_name']}` (`{row['top_city_code']}`) with `{row['top_mi_nats']:.4f}`",
                "",
            ]
        )
    for family in FAMILIES:
        subset = [r for r in overlap if r["family"] == family]
        if not subset:
            continue
        mean_delta = sum(r["delta_mi_nats_ageb_minus_manzana"] for r in subset) / len(subset)
        mean_delta_nmi = sum(r["delta_nmi_min_ageb_minus_manzana"] for r in subset) / len(subset)
        lines.extend(
            [
                f"## Overlap comparison · {family}",
                "",
                f"- overlap cities: `{len(subset)}`",
                f"- mean `MI_AGEB - MI_manzana`: `{mean_delta:.4f}`",
                f"- mean `NMI_AGEB - NMI_manzana`: `{mean_delta_nmi:.4f}`",
                "",
            ]
        )
    top_m_scian2 = max((r for r in manzana_rows if r["family"] == "scian2"), key=lambda r: r["mi_nats"])
    top_m_size = max((r for r in manzana_rows if r["family"] == "size_class"), key=lambda r: r["mi_nats"])
    lines.extend(
        [
            "## Quick read",
            "",
            f"- national manzana winner in `scian2`: `{top_m_scian2['city_name']}` with `MI={top_m_scian2['mi_nats']:.4f}`",
            f"- national manzana winner in `size_class`: `{top_m_size['city_name']}` with `MI={top_m_size['mi_nats']:.4f}`",
            "- if `MI` is higher at finer scale, the economic structure is more spatially selected when we look at smaller neighborhoods.",
            "- if normalized MI stays high, that is not just a trivial effect of having more units.",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def _persist_summary(rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    values: list[str] = []
    for row in rows:
        values.append(
            "("
            + ",".join(
                [
                    "'derived.ageb_economic_mix|raw.population_units|raw.denue_establishments'",
                    f"'{RESULT_SOURCE_METHOD}'",
                    f"'{_sql_text(row['level'])}'",
                    f"'{_sql_text(row['family'])}'",
                    f"'{_sql_text(row['city_code'])}'",
                    f"'{_sql_text(row['city_name'])}'",
                    f"'{_sql_text(row['state_code'])}'",
                    str(row["total_count"]),
                    str(row["n_units_total"]),
                    str(row["n_units_nonzero"]),
                    str(row["n_categories"]),
                    str(row["mi_nats"]),
                    str(row["mi_bits"]),
                    str(row["h_units_nats"]),
                    str(row["h_categories_nats"]),
                    "NULL" if row["nmi_min"] is None else str(row["nmi_min"]),
                    str(row["effective_units"]),
                    str(row["effective_categories"]),
                    "NULL" if row["unit_coverage"] is None else str(row["unit_coverage"]),
                    "''",
                ]
            )
            + ")"
        )
    chunks = [values[i : i + 1000] for i in range(0, len(values), 1000)]
    _psql_exec(
        f"""
\\set ON_ERROR_STOP on
BEGIN;
DELETE FROM derived.city_spatial_information_summary
WHERE source_method = '{RESULT_SOURCE_METHOD}';
COMMIT;
"""
    )
    for chunk in chunks:
        _psql_exec(
            f"""
\\set ON_ERROR_STOP on
INSERT INTO derived.city_spatial_information_summary (
    source_file, source_method, neighborhood_level, family, city_code, city_name, state_code,
    total_count, n_units_total, n_units_nonzero, n_categories, mi_nats, mi_bits,
    h_units_nats, h_categories_nats, nmi_min, effective_units, effective_categories,
    unit_coverage, notes
) VALUES
{",".join(chunk)};
"""
        )


def _persist_overlap(rows: list[dict[str, object]]) -> None:
    _psql_exec(
        f"""
\\set ON_ERROR_STOP on
DELETE FROM derived.city_spatial_information_overlap
WHERE source_method = '{RESULT_SOURCE_METHOD}';
"""
    )
    if not rows:
        return
    values: list[str] = []
    for row in rows:
        values.append(
            "("
            + ",".join(
                [
                    "'derived.city_spatial_information_summary'",
                    f"'{RESULT_SOURCE_METHOD}'",
                    f"'{_sql_text(row['family'])}'",
                    f"'{_sql_text(row['city_code'])}'",
                    f"'{_sql_text(row['city_name'])}'",
                    f"'{_sql_text(row['state_code'])}'",
                    str(row["mi_nats_manzana"]),
                    str(row["mi_nats_ageb"]),
                    "NULL" if row["nmi_min_manzana"] is None else str(row["nmi_min_manzana"]),
                    "NULL" if row["nmi_min_ageb"] is None else str(row["nmi_min_ageb"]),
                    "NULL" if row["unit_coverage_manzana"] is None else str(row["unit_coverage_manzana"]),
                    "NULL" if row["unit_coverage_ageb"] is None else str(row["unit_coverage_ageb"]),
                    str(row["delta_mi_nats_ageb_minus_manzana"]),
                    str(row["delta_nmi_min_ageb_minus_manzana"]),
                    "''",
                ]
            )
            + ")"
        )
    for i in range(0, len(values), 1000):
        chunk = values[i : i + 1000]
        _psql_exec(
            f"""
\\set ON_ERROR_STOP on
INSERT INTO derived.city_spatial_information_overlap (
    source_file, source_method, family, city_code, city_name, state_code,
    mi_nats_manzana, mi_nats_ageb, nmi_min_manzana, nmi_min_ageb,
    unit_coverage_manzana, unit_coverage_ageb, delta_mi_nats_ageb_minus_manzana,
    delta_nmi_min_ageb_minus_manzana, notes
) VALUES
{",".join(chunk)};
"""
        )


def main() -> int:
    _bootstrap()
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "spatial-information-phase1-2026-04-24"
    figures = outdir / "figures"
    outdir.mkdir(parents=True, exist_ok=True)

    ageb_rows = _rows_to_typed(_psql_copy(_summary_query("ageb")))
    manzana_rows = _rows_to_typed(_psql_copy(_summary_query("manzana")))
    combined_rows = ageb_rows + manzana_rows
    overlap_rows = _overlap_comparison(ageb_rows, manzana_rows)
    family_stats = _family_stats(combined_rows)

    _persist_summary(combined_rows)
    _persist_overlap(overlap_rows)

    _write_csv(outdir / "ageb_city_information_summary.csv", ageb_rows, list(ageb_rows[0].keys()))
    _write_csv(outdir / "manzana_city_information_summary.csv", manzana_rows, list(manzana_rows[0].keys()))
    _write_csv(outdir / "multiscale_overlap_comparison.csv", overlap_rows, list(overlap_rows[0].keys()))
    _write_csv(outdir / "family_stats.csv", family_stats, list(family_stats[0].keys()))

    (outdir / "report.md").write_text(
        _report(outdir, family_stats, overlap_rows, ageb_rows, manzana_rows),
        encoding="utf-8",
    )
    print(f"[phase1] done -> {outdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
from __future__ import annotations

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

DECOMP_SOURCE_METHOD = "bettencourt_spatial_information_multiscale_v2"
RESULT_SOURCE_METHOD = "bettencourt_spatial_information_regimes_v1"
ROOT = Path(__file__).resolve().parents[1]
OUTDIR = ROOT / "reports" / "spatial-information-regimes-2026-04-25"


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
    script = ROOT / "scripts" / "bootstrap_experiment_db.sh"
    subprocess.run(["bash", str(script)], check=True)


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _persist() -> None:
    sql = f"""
\\set ON_ERROR_STOP on
BEGIN;
DELETE FROM derived.city_spatial_information_regimes
WHERE source_method = '{RESULT_SOURCE_METHOD}';

INSERT INTO derived.city_spatial_information_regimes (
    source_file,
    source_method,
    decomposition_source_method,
    family,
    city_code,
    city_name,
    state_code,
    population,
    est_total,
    mi_manzana_nats,
    mi_ageb_nats,
    mi_within_ageb_nats,
    identity_gap_nats,
    share_between_ageb,
    share_within_ageb,
    share_gap,
    share_between_explained,
    share_within_explained,
    dominant_component_total,
    regime_total_067,
    regime_explained_067,
    notes
)
SELECT
    'derived.city_spatial_information_decomposition|derived.city_network_metrics',
    '{RESULT_SOURCE_METHOD}',
    '{DECOMP_SOURCE_METHOD}',
    d.family,
    d.city_code,
    d.city_name,
    d.state_code,
    n.population,
    n.est_total,
    d.mi_manzana_nats,
    d.mi_ageb_nats,
    d.mi_within_ageb_nats,
    d.identity_gap_nats,
    d.share_between_ageb,
    d.share_within_ageb,
    d.identity_gap_nats / NULLIF(d.mi_manzana_nats, 0) AS share_gap,
    d.mi_ageb_nats / NULLIF(d.mi_ageb_nats + d.mi_within_ageb_nats, 0) AS share_between_explained,
    d.mi_within_ageb_nats / NULLIF(d.mi_ageb_nats + d.mi_within_ageb_nats, 0) AS share_within_explained,
    CASE
        WHEN d.share_between_ageb >= d.share_within_ageb
         AND d.share_between_ageb >= GREATEST(COALESCE(d.identity_gap_nats / NULLIF(d.mi_manzana_nats, 0), 0), 0)
            THEN 'between_ageb'
        WHEN d.share_within_ageb >= d.share_between_ageb
         AND d.share_within_ageb >= GREATEST(COALESCE(d.identity_gap_nats / NULLIF(d.mi_manzana_nats, 0), 0), 0)
            THEN 'within_ageb'
        ELSE 'gap'
    END AS dominant_component_total,
    CASE
        WHEN d.share_between_ageb >= 0.67 THEN 'between_dominant'
        WHEN d.share_within_ageb >= 0.67 THEN 'within_dominant'
        WHEN COALESCE(d.identity_gap_nats / NULLIF(d.mi_manzana_nats, 0), 0) >= 0.67 THEN 'gap_dominant'
        ELSE 'mixed'
    END AS regime_total_067,
    CASE
        WHEN d.mi_ageb_nats / NULLIF(d.mi_ageb_nats + d.mi_within_ageb_nats, 0) >= 0.67 THEN 'between_dominant'
        WHEN d.mi_within_ageb_nats / NULLIF(d.mi_ageb_nats + d.mi_within_ageb_nats, 0) >= 0.67 THEN 'within_dominant'
        ELSE 'mixed'
    END AS regime_explained_067,
    CASE
        WHEN ABS(d.identity_gap_nats / NULLIF(d.mi_manzana_nats, 0)) > 0.10 THEN 'large_identity_gap'
        ELSE ''
    END
FROM derived.city_spatial_information_decomposition d
LEFT JOIN derived.city_network_metrics n
  ON n.source_method = 'osm_drive_municipal_full_v1'
 AND n.city_code = d.city_code
WHERE d.source_method = '{DECOMP_SOURCE_METHOD}';
COMMIT;
"""
    _psql_exec(sql)


def _load_csv(sql: str) -> list[dict[str, object]]:
    rows = _psql_copy(sql)
    typed: list[dict[str, object]] = []
    for row in rows:
        typed_row: dict[str, object] = {}
        for key, value in row.items():
            if value is None:
                typed_row[key] = ""
                continue
            text = value.strip()
            if text == "":
                typed_row[key] = ""
                continue
            try:
                if "." in text or "e" in text.lower() or "-" in text[1:]:
                    typed_row[key] = float(text)
                else:
                    typed_row[key] = int(text)
            except Exception:
                typed_row[key] = text
        typed.append(typed_row)
    return typed


def main() -> int:
    _bootstrap()
    OUTDIR.mkdir(parents=True, exist_ok=True)
    _persist()

    family_summary = _load_csv(
        f"""
        SELECT
            family,
            COUNT(*) AS n_cities,
            ROUND(AVG(share_between_ageb)::numeric, 4) AS mean_share_between,
            ROUND(AVG(share_within_ageb)::numeric, 4) AS mean_share_within,
            ROUND(AVG(share_gap)::numeric, 4) AS mean_share_gap,
            ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY share_between_ageb)::numeric, 4) AS median_share_between,
            ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY share_within_ageb)::numeric, 4) AS median_share_within,
            ROUND(percentile_cont(0.9) WITHIN GROUP (ORDER BY share_between_ageb)::numeric, 4) AS p90_share_between,
            ROUND(percentile_cont(0.9) WITHIN GROUP (ORDER BY share_within_ageb)::numeric, 4) AS p90_share_within,
            ROUND(AVG(share_between_explained)::numeric, 4) AS mean_share_between_explained,
            ROUND(AVG(share_within_explained)::numeric, 4) AS mean_share_within_explained
        FROM derived.city_spatial_information_regimes
        WHERE source_method = '{RESULT_SOURCE_METHOD}'
        GROUP BY family
        ORDER BY family
        """
    )
    _write_csv(OUTDIR / "family_summary.csv", family_summary, list(family_summary[0].keys()))

    regime_counts_total = _load_csv(
        f"""
        SELECT family, regime_total_067, COUNT(*) AS n_cities
        FROM derived.city_spatial_information_regimes
        WHERE source_method = '{RESULT_SOURCE_METHOD}'
        GROUP BY family, regime_total_067
        ORDER BY family, regime_total_067
        """
    )
    _write_csv(OUTDIR / "regime_counts_total.csv", regime_counts_total, list(regime_counts_total[0].keys()))

    regime_counts_explained = _load_csv(
        f"""
        SELECT family, regime_explained_067, COUNT(*) AS n_cities
        FROM derived.city_spatial_information_regimes
        WHERE source_method = '{RESULT_SOURCE_METHOD}'
        GROUP BY family, regime_explained_067
        ORDER BY family, regime_explained_067
        """
    )
    _write_csv(OUTDIR / "regime_counts_explained.csv", regime_counts_explained, list(regime_counts_explained[0].keys()))

    top_between = _load_csv(
        f"""
        WITH ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY family ORDER BY share_between_ageb DESC NULLS LAST, population DESC NULLS LAST, city_code) AS rn
            FROM derived.city_spatial_information_regimes
            WHERE source_method = '{RESULT_SOURCE_METHOD}'
        )
        SELECT family, city_code, city_name, state_code, population, est_total,
               ROUND(share_between_ageb::numeric, 4) AS share_between_ageb,
               ROUND(share_within_ageb::numeric, 4) AS share_within_ageb,
               ROUND(share_gap::numeric, 4) AS share_gap
        FROM ranked
        WHERE rn <= 25
        ORDER BY family, rn
        """
    )
    _write_csv(OUTDIR / "top_between.csv", top_between, list(top_between[0].keys()))

    top_within = _load_csv(
        f"""
        WITH ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY family ORDER BY share_within_ageb DESC NULLS LAST, population DESC NULLS LAST, city_code) AS rn
            FROM derived.city_spatial_information_regimes
            WHERE source_method = '{RESULT_SOURCE_METHOD}'
        )
        SELECT family, city_code, city_name, state_code, population, est_total,
               ROUND(share_between_ageb::numeric, 4) AS share_between_ageb,
               ROUND(share_within_ageb::numeric, 4) AS share_within_ageb,
               ROUND(share_gap::numeric, 4) AS share_gap
        FROM ranked
        WHERE rn <= 25
        ORDER BY family, rn
        """
    )
    _write_csv(OUTDIR / "top_within.csv", top_within, list(top_within[0].keys()))

    top_gap = _load_csv(
        f"""
        WITH ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY family ORDER BY ABS(share_gap) DESC NULLS LAST, population DESC NULLS LAST, city_code) AS rn
            FROM derived.city_spatial_information_regimes
            WHERE source_method = '{RESULT_SOURCE_METHOD}'
        )
        SELECT family, city_code, city_name, state_code, population, est_total,
               ROUND(share_between_ageb::numeric, 4) AS share_between_ageb,
               ROUND(share_within_ageb::numeric, 4) AS share_within_ageb,
               ROUND(share_gap::numeric, 4) AS share_gap
        FROM ranked
        WHERE rn <= 25
        ORDER BY family, rn
        """
    )
    _write_csv(OUTDIR / "top_gap.csv", top_gap, list(top_gap[0].keys()))

    large_city_top_between = _load_csv(
        f"""
        WITH ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY family ORDER BY share_between_ageb DESC NULLS LAST, population DESC NULLS LAST, city_code) AS rn
            FROM derived.city_spatial_information_regimes
            WHERE source_method = '{RESULT_SOURCE_METHOD}'
              AND population >= 100000
        )
        SELECT family, city_code, city_name, state_code, population, est_total,
               ROUND(share_between_ageb::numeric, 4) AS share_between_ageb,
               ROUND(share_within_ageb::numeric, 4) AS share_within_ageb,
               ROUND(share_gap::numeric, 4) AS share_gap
        FROM ranked
        WHERE rn <= 25
        ORDER BY family, rn
        """
    )
    _write_csv(OUTDIR / "large_city_top_between.csv", large_city_top_between, list(large_city_top_between[0].keys()))

    large_city_top_within = _load_csv(
        f"""
        WITH ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY family ORDER BY share_within_ageb DESC NULLS LAST, population DESC NULLS LAST, city_code) AS rn
            FROM derived.city_spatial_information_regimes
            WHERE source_method = '{RESULT_SOURCE_METHOD}'
              AND population >= 100000
        )
        SELECT family, city_code, city_name, state_code, population, est_total,
               ROUND(share_between_ageb::numeric, 4) AS share_between_ageb,
               ROUND(share_within_ageb::numeric, 4) AS share_within_ageb,
               ROUND(share_gap::numeric, 4) AS share_gap
        FROM ranked
        WHERE rn <= 25
        ORDER BY family, rn
        """
    )
    _write_csv(OUTDIR / "large_city_top_within.csv", large_city_top_within, list(large_city_top_within[0].keys()))

    family_map = {row["family"]: row for row in family_summary}
    total_map = {(row["family"], row["regime_total_067"]): row["n_cities"] for row in regime_counts_total}
    explained_map = {(row["family"], row["regime_explained_067"]): row["n_cities"] for row in regime_counts_explained}

    lines = [
        "# Spatial Information Regimes",
        "",
        "This interprets the multiscale information decomposition",
        "",
        "```math",
        "I(M;\\Lambda)=I(A;\\Lambda)+I(M;\\Lambda\\mid A)+\\Delta",
        "```",
        "",
        "where `M = manzana`, `A = AGEB`, and `\\Delta` is the empirical identity gap observed in the persisted outputs.",
        "",
    ]
    for family in ("scian2", "size_class"):
        row = family_map.get(family)
        if not row:
            continue
        lines.extend(
            [
                f"## {family}",
                "",
                f"- cities: `{row['n_cities']}`",
                f"- mean share between AGEB: `{row['mean_share_between']}`",
                f"- mean share within AGEB: `{row['mean_share_within']}`",
                f"- mean gap share: `{row['mean_share_gap']}`",
                f"- median share between AGEB: `{row['median_share_between']}`",
                f"- median share within AGEB: `{row['median_share_within']}`",
                f"- p90 share between AGEB: `{row['p90_share_between']}`",
                f"- p90 share within AGEB: `{row['p90_share_within']}`",
                f"- regime total 0.67 between/mixed/within/gap: "
                f"`{total_map.get((family, 'between_dominant'), 0)}` / "
                f"`{total_map.get((family, 'mixed'), 0)}` / "
                f"`{total_map.get((family, 'within_dominant'), 0)}` / "
                f"`{total_map.get((family, 'gap_dominant'), 0)}`",
                f"- regime explained 0.67 between/mixed/within: "
                f"`{explained_map.get((family, 'between_dominant'), 0)}` / "
                f"`{explained_map.get((family, 'mixed'), 0)}` / "
                f"`{explained_map.get((family, 'within_dominant'), 0)}`",
                "",
            ]
        )
    lines.extend(
        [
            "Files:",
            f"- [family_summary.csv]({(OUTDIR / 'family_summary.csv').resolve()})",
            f"- [regime_counts_total.csv]({(OUTDIR / 'regime_counts_total.csv').resolve()})",
            f"- [regime_counts_explained.csv]({(OUTDIR / 'regime_counts_explained.csv').resolve()})",
            f"- [top_between.csv]({(OUTDIR / 'top_between.csv').resolve()})",
            f"- [top_within.csv]({(OUTDIR / 'top_within.csv').resolve()})",
            f"- [top_gap.csv]({(OUTDIR / 'top_gap.csv').resolve()})",
            f"- [large_city_top_between.csv]({(OUTDIR / 'large_city_top_between.csv').resolve()})",
            f"- [large_city_top_within.csv]({(OUTDIR / 'large_city_top_within.csv').resolve()})",
        ]
    )
    (OUTDIR / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

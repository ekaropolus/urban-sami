#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

from load_manzana_for_ageb_cities import _bootstrap, _copy_city, _psql, _sql_text


SOURCE_METHOD = "inegi_wscatgeo_manzana_full_v1"


def _all_city_rows() -> list[dict[str, str]]:
    out = _psql(
        """
        SELECT city_code, COALESCE(MAX(city_name), '')
        FROM raw.population_units
        WHERE level = 'city'
          AND city_code <> ''
        GROUP BY city_code
        ORDER BY city_code;
        """,
        capture_output=True,
    )
    rows: list[dict[str, str]] = []
    for line in out.splitlines():
        if not line.strip():
            continue
        city_code, city_name = (line.split("\t", 1) + [""])[:2]
        rows.append({"city_code": city_code.strip(), "city_name": city_name.strip()})
    return rows


def _error_city_rows() -> list[dict[str, str]]:
    out = _psql(
        f"""
        SELECT city_code, COALESCE(city_name, '')
        FROM experiments.manzana_load_status
        WHERE source_method = '{SOURCE_METHOD}'
          AND status = 'error'
        ORDER BY city_code;
        """,
        capture_output=True,
    )
    rows: list[dict[str, str]] = []
    for line in out.splitlines():
        if not line.strip():
            continue
        city_code, city_name = (line.split("\t", 1) + [""])[:2]
        rows.append({"city_code": city_code.strip(), "city_name": city_name.strip()})
    return rows


def _city_has_manzana(city_code: str) -> bool:
    out = _psql(
        f"""
        SELECT COUNT(*)
        FROM raw.population_units
        WHERE level = 'manzana'
          AND city_code = '{_sql_text(city_code)}';
        """,
        capture_output=True,
    ).strip()
    try:
        return int(out) > 0
    except Exception:
        return False


def _set_status(
    *,
    city_code: str,
    city_name: str,
    status: str,
    features_seen: int | None = None,
    loaded_rows: int | None = None,
    error_message: str = "",
    notes: str = "",
    touch_finished: bool = False,
) -> None:
    finished_sql = "NOW()" if touch_finished else "NULL"
    _psql(
        f"""
        INSERT INTO experiments.manzana_load_status (
            source_method, city_code, city_name, status, started_at, finished_at,
            features_seen, loaded_rows, error_message, notes
        ) VALUES (
            '{SOURCE_METHOD}',
            '{_sql_text(city_code)}',
            '{_sql_text(city_name)}',
            '{_sql_text(status)}',
            NOW(),
            {finished_sql},
            {('NULL' if features_seen is None else str(int(features_seen)))},
            {('NULL' if loaded_rows is None else str(int(loaded_rows)))},
            '{_sql_text(error_message[:1000])}',
            '{_sql_text(notes[:1000])}'
        )
        ON CONFLICT (source_method, city_code) DO UPDATE SET
            city_name = EXCLUDED.city_name,
            status = EXCLUDED.status,
            features_seen = EXCLUDED.features_seen,
            loaded_rows = EXCLUDED.loaded_rows,
            error_message = EXCLUDED.error_message,
            notes = EXCLUDED.notes,
            finished_at = EXCLUDED.finished_at,
            started_at = CASE
                WHEN EXCLUDED.status = 'running' THEN NOW()
                ELSE experiments.manzana_load_status.started_at
            END;
        """
    )


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description="National resumable manzana loader for all city-level units in urban_sami_exp.")
    parser.add_argument("--refresh-existing", action="store_true", help="Reload cities even if manzana rows already exist.")
    parser.add_argument("--retry-errors-only", action="store_true", help="Process only cities currently marked as error for this source method.")
    parser.add_argument("--limit", type=int, default=0, help="Optional max number of cities to process in this run.")
    args = parser.parse_args()

    _bootstrap()
    outdir = Path(__file__).resolve().parents[1] / "reports" / "manzana-load-national-2026-04-24"
    outdir.mkdir(parents=True, exist_ok=True)

    city_rows = _error_city_rows() if args.retry_errors_only else _all_city_rows()
    rows: list[dict[str, object]] = []
    processed = 0
    for city in city_rows:
        city_code = city["city_code"]
        city_name = city["city_name"]
        if args.limit and processed >= args.limit:
            break
        processed += 1

        if not args.retry_errors_only and not args.refresh_existing and _city_has_manzana(city_code):
            _set_status(
                city_code=city_code,
                city_name=city_name,
                status="success",
                notes="skipped_existing",
                touch_finished=True,
            )
            rows.append(
                {
                    "city_code": city_code,
                    "city_name": city_name,
                    "status": "skipped_existing",
                    "features_seen": "",
                    "manzana_loaded": "",
                    "distinct_parent_agebs": "",
                }
            )
            _write_csv(outdir / "city_load_summary.csv", rows, list(rows[0].keys()))
            print(f"[skip] {city_code} {city_name}")
            continue

        _set_status(city_code=city_code, city_name=city_name, status="running", notes="")
        try:
            result = _copy_city(city_code, refresh=True)
            _set_status(
                city_code=city_code,
                city_name=str(result.get("city_name") or city_name),
                status="success",
                features_seen=int(result.get("features_seen") or 0),
                loaded_rows=int(result.get("manzana_loaded") or 0),
                notes="loaded",
                touch_finished=True,
            )
            rows.append(
                {
                    "city_code": city_code,
                    "city_name": result.get("city_name") or city_name,
                    "status": "loaded",
                    "features_seen": result.get("features_seen") or 0,
                    "manzana_loaded": result.get("manzana_loaded") or 0,
                    "distinct_parent_agebs": result.get("distinct_parent_agebs") or 0,
                }
            )
            _write_csv(outdir / "city_load_summary.csv", rows, list(rows[0].keys()))
            print(f"[ok] {city_code} {result.get('city_name') or city_name} rows={result.get('manzana_loaded')}")
        except Exception as exc:
            _set_status(
                city_code=city_code,
                city_name=city_name,
                status="error",
                error_message=str(exc),
                notes="load_failed",
                touch_finished=True,
            )
            rows.append(
                {
                    "city_code": city_code,
                    "city_name": city_name,
                    "status": "error",
                    "features_seen": "",
                    "manzana_loaded": "",
                    "distinct_parent_agebs": "",
                }
            )
            _write_csv(outdir / "city_load_summary.csv", rows, list(rows[0].keys()))
            print(f"[error] {city_code} {city_name}: {exc}")

    summary_sql = f"""
    SELECT 'manzana_population_rows', count(*)::text FROM raw.population_units WHERE level = 'manzana'
    UNION ALL
    SELECT 'manzana_admin_rows', count(*)::text FROM raw.admin_units WHERE level = 'manzana'
    UNION ALL
    SELECT 'manzana_cities', count(DISTINCT city_code)::text FROM raw.population_units WHERE level = 'manzana'
    UNION ALL
    SELECT 'status_success', count(*)::text FROM experiments.manzana_load_status WHERE source_method = '{SOURCE_METHOD}' AND status = 'success'
    UNION ALL
    SELECT 'status_error', count(*)::text FROM experiments.manzana_load_status WHERE source_method = '{SOURCE_METHOD}' AND status = 'error'
    UNION ALL
    SELECT 'status_running', count(*)::text FROM experiments.manzana_load_status WHERE source_method = '{SOURCE_METHOD}' AND status = 'running';
    """
    summary_rows = []
    for line in _psql(summary_sql, capture_output=True).splitlines():
        if not line.strip():
            continue
        key, value = line.split("\t", 1)
        summary_rows.append({"metric": key, "value": value})
    _write_csv(outdir / "summary.csv", summary_rows, ["metric", "value"])
    report = [
        "# Manzana National Load",
        "",
        f"- source method: `{SOURCE_METHOD}`",
        "",
        "Files:",
        f"- [summary.csv]({(outdir / 'summary.csv').resolve()})",
        f"- [city_load_summary.csv]({(outdir / 'city_load_summary.csv').resolve()})",
    ]
    (outdir / "report.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

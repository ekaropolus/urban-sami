from __future__ import annotations

from pathlib import Path

from urban_sami.io.csvio import write_csv_rows


ASSIGNMENT_COLUMNS = [
    "obs_id",
    "domain_id",
    "unit_id",
]


def write_assignments(rows: list[dict], path: str | Path) -> Path:
    return write_csv_rows(path, fieldnames=ASSIGNMENT_COLUMNS, rows=rows)


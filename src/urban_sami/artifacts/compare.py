from __future__ import annotations

from pathlib import Path

from urban_sami.io.csvio import write_csv_rows


COMPARISON_COLUMNS = [
    "key",
    "column",
    "left_value",
    "right_value",
    "status",
]


def write_comparison_rows(rows: list[dict], path: str | Path) -> Path:
    return write_csv_rows(path, fieldnames=COMPARISON_COLUMNS, rows=rows)


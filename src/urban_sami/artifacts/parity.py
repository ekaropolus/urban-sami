from __future__ import annotations

from pathlib import Path

from urban_sami.io.csvio import write_csv_rows


PARITY_COMPARISON_COLUMNS = [
    "key",
    "column",
    "expected_value",
    "actual_value",
    "abs_diff",
    "status",
]


def write_parity_rows(rows: list[dict], path: str | Path) -> Path:
    return write_csv_rows(path, fieldnames=PARITY_COMPARISON_COLUMNS, rows=rows)

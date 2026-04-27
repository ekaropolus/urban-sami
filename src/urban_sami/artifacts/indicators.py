from __future__ import annotations

from pathlib import Path

from urban_sami.io.csvio import write_csv_rows


INDICATOR_OUTPUT_COLUMNS = [
    "domain_id",
    "unit_id",
    "indicator_key",
    "indicator_value",
    "unit_label",
    "parent_id",
]


def write_indicator_outputs(rows: list[dict], path: str | Path) -> Path:
    return write_csv_rows(path, fieldnames=INDICATOR_OUTPUT_COLUMNS, rows=rows)


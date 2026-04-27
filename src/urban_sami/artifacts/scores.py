from __future__ import annotations

from pathlib import Path

from urban_sami.io.csvio import write_csv_rows


UNIT_SCORE_COLUMNS = [
    "domain_id",
    "indicator_key",
    "scale_basis",
    "fit_method",
    "unit_id",
    "unit_label",
    "parent_id",
    "indicator_value",
    "scale_n",
    "y_expected",
    "epsilon_log",
    "sami",
    "z_residual",
    "alpha",
    "beta",
    "r2",
    "resid_std",
]


def write_unit_scores(rows: list[dict], path: str | Path) -> Path:
    return write_csv_rows(path, fieldnames=UNIT_SCORE_COLUMNS, rows=rows)

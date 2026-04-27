from __future__ import annotations

import csv
from pathlib import Path


SUMMARY_COLUMNS = [
    "n_field",
    "level",
    "filter_mode",
    "fit_method",
    "units",
    "alpha",
    "beta",
    "r2",
    "resid_std",
    "aic",
    "bic",
    "y_min",
    "y_p95",
    "y_max",
    "n_min",
    "n_p05",
    "n_p95",
    "n_max",
]


def read_summary_csv(path: str | Path) -> list[dict[str, str]]:
    with Path(path).open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
    if not rows:
        return []
    missing = [col for col in SUMMARY_COLUMNS if col not in reader.fieldnames]
    if missing:
        raise ValueError(f"summary csv missing required columns: {missing}")
    return [{col: row.get(col, "") for col in SUMMARY_COLUMNS} for row in rows]


def write_summary_csv(rows: list[dict[str, str]], path: str | Path) -> Path:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=SUMMARY_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in SUMMARY_COLUMNS})
    return output

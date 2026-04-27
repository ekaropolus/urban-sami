from __future__ import annotations

from pathlib import Path

from urban_sami.io.csvio import write_csv_rows


MODEL_SUMMARY_COLUMNS = [
    "domain_id",
    "indicator_key",
    "scale_basis",
    "fit_method",
    "units",
    "alpha",
    "alpha_ci95_low",
    "alpha_ci95_high",
    "beta",
    "beta_ci95_low",
    "beta_ci95_high",
    "r2",
    "resid_std",
    "aic",
    "bic",
    "value_min",
    "value_max",
    "n_min",
    "n_max",
]


def write_model_summaries(rows: list[dict], path: str | Path) -> Path:
    return write_csv_rows(path, fieldnames=MODEL_SUMMARY_COLUMNS, rows=rows)

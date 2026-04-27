from __future__ import annotations

import csv
from pathlib import Path

from urban_sami.workflow import load_workflow
from urban_sami.workflow.runner import run_workflow


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _find(rows: list[dict[str, str]], **filters: str) -> dict[str, str]:
    for row in rows:
        if all(row.get(key) == value for key, value in filters.items()):
            return row
    raise AssertionError(f"missing row for filters: {filters}")


def test_indicator_sami_workflow_builds_model_outputs(tmp_path: Path):
    workflow_path = Path("examples/indicator-sami-demo.yaml")
    workflow = load_workflow(workflow_path)
    workflow = type(workflow)(
        version=workflow.version,
        kind=workflow.kind,
        metadata=workflow.metadata,
        data_sources=workflow.data_sources,
        outputs=type(workflow.outputs)(
            base_dir=str(tmp_path),
            write_bundle_zip=True,
            write_report_md=workflow.outputs.write_report_md,
            write_summary_csv=workflow.outputs.write_summary_csv,
        ),
        raw=workflow.raw,
    )
    result = run_workflow(workflow, manifest_path=workflow_path)
    assert result["ok"] is True
    assert result["model_rows"] > 0

    rows = _read_rows(Path(result["output_dir"]) / "model_summaries.csv")
    row = _find(rows, domain_id="admin_ageb", indicator_key="denue_est_count", fit_method="ols")
    assert row["units"] == "2"
    assert row["scale_basis"] == "population"
    assert row["alpha"] != ""
    assert row["beta"] != ""
    score_rows = _read_rows(Path(result["output_dir"]) / "unit_scores.csv")
    score_row = _find(score_rows, domain_id="admin_ageb", indicator_key="denue_est_count", fit_method="ols", unit_id="admin_ageb:001")
    assert score_row["sami"] == score_row["epsilon_log"]
    assert score_row["y_expected"] != ""
    assert score_row["z_residual"] != ""
    auto_row = _find(rows, domain_id="hex_8", indicator_key="denue_est_per_1k_pop", fit_method="auto")
    assert auto_row["r2"] != ""

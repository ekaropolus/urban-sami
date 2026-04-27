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


def test_geojson_indicator_sami_workflow_runs_from_raw_points_and_polygons(tmp_path: Path):
    workflow_path = Path("examples/geojson-indicator-sami-demo.yaml")
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
    assert result["assigned_count"] == 2
    assert result["indicator_rows"] >= 4
    assert result["model_rows"] >= 2

    assignments = _read_rows(Path(result["output_dir"]) / "assignments.csv")
    assert {"obs_id": "p1", "domain_id": "geo_demo", "unit_id": "geo_demo:poly_a"} in assignments
    assert {"obs_id": "p2", "domain_id": "geo_demo", "unit_id": "geo_demo:poly_b"} in assignments

    indicators = _read_rows(Path(result["output_dir"]) / "indicator_outputs.csv")
    count_row = _find(
        indicators,
        domain_id="geo_demo",
        unit_id="geo_demo:poly_a",
        indicator_key="denue_est_count",
    )
    assert count_row["indicator_value"] == "1.0"

    models = _read_rows(Path(result["output_dir"]) / "model_summaries.csv")
    model_row = _find(
        models,
        domain_id="geo_demo",
        indicator_key="denue_est_density_km2",
        fit_method="ols",
    )
    assert model_row["units"] == "2"
    assert model_row["scale_basis"] == "population"
    assert model_row["beta_ci95_low"] != ""
    assert model_row["beta_ci95_high"] != ""
    scores = _read_rows(Path(result["output_dir"]) / "unit_scores.csv")
    score_row = _find(
        scores,
        domain_id="geo_demo",
        indicator_key="denue_est_density_km2",
        fit_method="ols",
        unit_id="geo_demo:poly_a",
    )
    assert score_row["sami"] == score_row["epsilon_log"]
    assert score_row["y_expected"] != ""

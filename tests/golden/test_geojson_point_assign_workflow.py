from __future__ import annotations

import csv
from pathlib import Path

from urban_sami.workflow import load_workflow
from urban_sami.workflow.runner import run_workflow


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def test_geojson_point_assign_workflow_assigns_expected_points(tmp_path: Path):
    workflow_path = Path("examples/geojson-point-assign-demo.yaml")
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
    rows = _read_rows(Path(result["output_dir"]) / "assignments.csv")
    assert {"obs_id": "p1", "domain_id": "geo_demo", "unit_id": "geo_demo:poly_a"} in rows
    assert {"obs_id": "p2", "domain_id": "geo_demo", "unit_id": "geo_demo:poly_b"} in rows


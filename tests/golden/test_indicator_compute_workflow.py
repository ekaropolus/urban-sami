from __future__ import annotations

import csv
from pathlib import Path

from urban_sami.workflow import load_workflow
from urban_sami.workflow.runner import run_workflow


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _find(rows: list[dict[str, str]], *, domain_id: str, unit_id: str, indicator_key: str) -> dict[str, str]:
    for row in rows:
        if row["domain_id"] == domain_id and row["unit_id"] == unit_id and row["indicator_key"] == indicator_key:
            return row
    raise AssertionError(f"missing row: {domain_id} {unit_id} {indicator_key}")


def test_indicator_compute_workflow_builds_generic_outputs(tmp_path: Path):
    workflow_path = Path("examples/indicator-compute-demo.yaml")
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
    rows = _read_rows(Path(result["output_dir"]) / "indicator_outputs.csv")

    row = _find(rows, domain_id="admin_ageb", unit_id="admin_ageb:001", indicator_key="denue_est_count")
    assert row["indicator_value"] == "2.0"
    row = _find(rows, domain_id="admin_ageb", unit_id="admin_ageb:001", indicator_key="denue_est_per_1k_pop")
    assert row["indicator_value"] == "4.0"
    row = _find(rows, domain_id="admin_ageb", unit_id="admin_ageb:001", indicator_key="denue_sector_share_46")
    assert row["indicator_value"] == "0.5"
    row = _find(rows, domain_id="hex_8", unit_id="hex_8:abc", indicator_key="denue_est_density_km2")
    assert row["indicator_value"] == "4.0"
    row = _find(rows, domain_id="geofence_demo", unit_id="geofence_demo:g1", indicator_key="denue_revenue_proxy_daily_mxn")
    assert row["indicator_value"] == "2000.0"


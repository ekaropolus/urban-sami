from __future__ import annotations

import csv
from pathlib import Path

from urban_sami.workflow import load_workflow
from urban_sami.workflow.runner import run_workflow


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def test_polisplexity_state_matrix_parity_workflow_matches_summary(tmp_path: Path):
    workflow_path = Path("examples/polisplexity-state-matrix-parity.yaml")
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
    assert result["summary_rows"] == 5
    assert result["mismatch_count"] == 0

    rows = _read_rows(Path(result["output_dir"]) / "parity_comparison.csv")
    assert rows
    assert all(row["status"] == "match" for row in rows)


def test_polisplexity_ageb_matrix_parity_workflow_matches_summary(tmp_path: Path):
    workflow_path = Path("examples/polisplexity-ageb-matrix-parity.yaml")
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
    assert result["summary_rows"] == 5
    assert result["mismatch_count"] == 0

    rows = _read_rows(Path(result["output_dir"]) / "parity_comparison.csv")
    assert rows
    assert all(row["status"] == "match" for row in rows)


def test_polisplexity_state_run_dir_parity_workflow_matches_summary(tmp_path: Path):
    workflow_path = Path("examples/polisplexity-state-run-dir-parity.yaml")
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
    assert result["summary_rows"] == 5
    assert result["mismatch_count"] == 0

    rows = _read_rows(Path(result["output_dir"]) / "parity_comparison.csv")
    assert rows
    assert all(row["status"] == "match" for row in rows)

from __future__ import annotations

import csv
from pathlib import Path

from urban_sami.workflow import load_workflow
from urban_sami.workflow.runner import run_workflow


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def test_replay_workflow_reproduces_state_summary(tmp_path: Path):
    workflow_path = Path("examples/replay-state-workflow.yaml")
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

    produced = _read_rows(Path(result["output_dir"]) / "summary.csv")
    expected = _read_rows(Path("tests/fixtures/polisplexity_golden/state_population_all_cities/summary.csv"))
    assert produced == expected

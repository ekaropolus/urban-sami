from __future__ import annotations

from pathlib import Path

from urban_sami.workflow import load_workflow, plan_workflow


def test_load_example_workflow():
    workflow = load_workflow(Path("examples/replay-state-workflow.yaml"))
    assert workflow.workflow_id == "mx_state_population_replay"
    plan = plan_workflow(workflow)
    assert plan["kind"] == "summary_matrix_replay"
    assert "summary" in plan["data_sources"]


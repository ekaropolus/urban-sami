from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


def test_cli_validate_example_workflow():
    root = Path(__file__).resolve().parents[2]
    env = dict(os.environ)
    env["PYTHONPATH"] = str(root / "src")
    proc = subprocess.run(
        [sys.executable, "-m", "urban_sami.cli", "validate", "examples/replay-state-workflow.yaml"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(proc.stdout)
    assert payload["ok"] is True
    assert payload["workflow_id"] == "mx_state_population_replay"

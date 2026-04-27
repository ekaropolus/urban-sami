from __future__ import annotations

import argparse
import json
from pathlib import Path

from urban_sami.workflow.loader import load_workflow
from urban_sami.workflow.runner import plan_workflow, run_workflow


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="urban-sami", description="Headless SAMI workflow engine")
    sub = parser.add_subparsers(dest="command", required=True)

    validate = sub.add_parser("validate", help="Validate a workflow manifest")
    validate.add_argument("workflow", type=Path)

    plan = sub.add_parser("plan", help="Print normalized execution plan")
    plan.add_argument("workflow", type=Path)

    run = sub.add_parser("run", help="Execute a workflow")
    run.add_argument("workflow", type=Path)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    workflow = load_workflow(args.workflow)

    if args.command == "validate":
        print(json.dumps({"ok": True, "workflow_id": workflow.metadata.workflow_id}, indent=2))
        return 0

    if args.command == "plan":
        print(json.dumps(plan_workflow(workflow), indent=2))
        return 0

    if args.command == "run":
        print(json.dumps(run_workflow(workflow, manifest_path=args.workflow.resolve()), indent=2))
        return 0

    parser.error(f"unsupported command: {args.command}")
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

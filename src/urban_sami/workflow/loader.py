from __future__ import annotations

from pathlib import Path

import yaml

from urban_sami.workflow.schema import WorkflowDataSource, WorkflowMetadata, WorkflowOutputs, WorkflowSpec


def _require_mapping(obj, *, name: str) -> dict:
    if not isinstance(obj, dict):
        raise ValueError(f"{name} must be a mapping")
    return obj


def load_workflow(path: str | Path) -> WorkflowSpec:
    manifest_path = Path(path).expanduser().resolve()
    raw = yaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
    raw = _require_mapping(raw, name="workflow")

    metadata_raw = _require_mapping(raw.get("metadata") or {}, name="metadata")
    outputs_raw = _require_mapping(raw.get("outputs") or {}, name="outputs")
    sources_raw = _require_mapping(raw.get("data_sources") or {}, name="data_sources")

    metadata = WorkflowMetadata(
        workflow_id=str(metadata_raw.get("workflow_id") or "").strip(),
        title=str(metadata_raw.get("title") or "").strip(),
        description=str(metadata_raw.get("description") or "").strip(),
        country_code=(str(metadata_raw.get("country_code") or "MX").strip().upper() or "MX"),
    )
    if not metadata.workflow_id:
        raise ValueError("metadata.workflow_id is required")
    if not metadata.title:
        raise ValueError("metadata.title is required")

    outputs = WorkflowOutputs(
        base_dir=str(outputs_raw.get("base_dir") or "dist").strip(),
        write_bundle_zip=bool(outputs_raw.get("write_bundle_zip", True)),
        write_report_md=bool(outputs_raw.get("write_report_md", True)),
        write_summary_csv=bool(outputs_raw.get("write_summary_csv", True)),
    )

    data_sources: dict[str, WorkflowDataSource] = {}
    for key, value in sources_raw.items():
        item = _require_mapping(value, name=f"data_sources.{key}")
        data_sources[str(key)] = WorkflowDataSource(
            source_type=str(item.get("source_type") or "").strip(),
            path=str(item.get("path") or "").strip(),
        )
        if not data_sources[str(key)].source_type:
            raise ValueError(f"data_sources.{key}.source_type is required")
        if not data_sources[str(key)].path:
            raise ValueError(f"data_sources.{key}.path is required")

    return WorkflowSpec(
        version=str(raw.get("version") or "0.1").strip(),
        kind=str(raw.get("kind") or "").strip(),
        metadata=metadata,
        data_sources=data_sources,
        outputs=outputs,
        raw=raw,
    )

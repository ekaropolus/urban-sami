from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class WorkflowMetadata:
    workflow_id: str
    title: str
    description: str = ""
    country_code: str = "MX"


@dataclass(frozen=True)
class WorkflowDataSource:
    source_type: str
    path: str


@dataclass(frozen=True)
class WorkflowOutputs:
    base_dir: str
    write_bundle_zip: bool = True
    write_report_md: bool = True
    write_summary_csv: bool = True
    write_figures: bool = True


@dataclass(frozen=True)
class WorkflowSpec:
    version: str
    kind: str
    metadata: WorkflowMetadata
    data_sources: dict[str, WorkflowDataSource] = field(default_factory=dict)
    outputs: WorkflowOutputs = field(default_factory=lambda: WorkflowOutputs(base_dir="dist"))
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def workflow_id(self) -> str:
        return self.metadata.workflow_id

    def output_base_path(self, manifest_path: Path) -> Path:
        base = Path(self.outputs.base_dir)
        if base.is_absolute():
            return base
        return (manifest_path.parent / base).resolve()

    @property
    def geometry_domains(self) -> list[dict[str, Any]]:
        raw = self.raw.get("geometry_domains") or []
        return raw if isinstance(raw, list) else []

    @property
    def indicators(self) -> list[dict[str, Any]]:
        raw = self.raw.get("indicators") or []
        return raw if isinstance(raw, list) else []

    @property
    def models(self) -> dict[str, Any]:
        raw = self.raw.get("models") or {}
        return raw if isinstance(raw, dict) else {}

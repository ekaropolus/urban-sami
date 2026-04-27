from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class GeometryDomain:
    domain_id: str
    domain_type: str
    level: str = ""
    crs: str = "EPSG:4326"
    parameters: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class UnitRecord:
    domain_id: str
    unit_id: str
    unit_label: str = ""
    parent_id: str = ""
    attrs: dict[str, Any] = field(default_factory=dict)


def make_unit_id(domain_id: str, local_code: str) -> str:
    left = str(domain_id or "").strip()
    right = str(local_code or "").strip()
    if not left or not right:
        raise ValueError("domain_id and local_code are required")
    return f"{left}:{right}"


from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


IndicatorFunc = Callable[[dict[str, Any], dict[str, Any] | None], float | None]


@dataclass(frozen=True)
class IndicatorSpec:
    key: str
    family: str
    label: str
    formula: str
    compatible_domain_types: tuple[str, ...] = ("admin", "hex", "square_grid", "voronoi", "isochrone", "geofence")
    assumption_level: str = "observed"
    compute: IndicatorFunc | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class IndicatorRegistry:
    def __init__(self) -> None:
        self._items: dict[str, IndicatorSpec] = {}

    def register(self, spec: IndicatorSpec) -> None:
        if spec.key in self._items:
            raise ValueError(f"indicator already registered: {spec.key}")
        self._items[spec.key] = spec

    def get(self, key: str) -> IndicatorSpec:
        try:
            return self._items[key]
        except KeyError as exc:
            raise KeyError(f"unknown indicator: {key}") from exc

    def keys(self) -> list[str]:
        return sorted(self._items.keys())

    def items(self) -> list[IndicatorSpec]:
        return [self._items[key] for key in self.keys()]


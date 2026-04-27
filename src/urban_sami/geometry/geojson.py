from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class PolygonFeature:
    domain_id: str
    unit_id: str
    unit_label: str
    parent_id: str
    rings: list[list[tuple[float, float]]] = field(default_factory=list)
    attrs: dict[str, Any] = field(default_factory=dict)


def _as_float_pair(item) -> tuple[float, float]:
    if not isinstance(item, (list, tuple)) or len(item) < 2:
        raise ValueError("invalid coordinate")
    return (float(item[0]), float(item[1]))


def _normalize_polygon_coords(coords) -> list[list[tuple[float, float]]]:
    if not isinstance(coords, list) or not coords:
        raise ValueError("polygon coordinates must be a non-empty list")
    rings: list[list[tuple[float, float]]] = []
    for ring in coords:
        if not isinstance(ring, list) or len(ring) < 4:
            continue
        parsed = [_as_float_pair(item) for item in ring]
        rings.append(parsed)
    if not rings:
        raise ValueError("polygon has no valid rings")
    return rings


def _iter_polygon_geometries(geometry: dict) -> list[list[list[tuple[float, float]]]]:
    geom_type = str((geometry or {}).get("type") or "").strip()
    coords = (geometry or {}).get("coordinates")
    if geom_type == "Polygon":
        return [_normalize_polygon_coords(coords)]
    if geom_type == "MultiPolygon":
        if not isinstance(coords, list):
            raise ValueError("multipolygon coordinates must be a list")
        return [_normalize_polygon_coords(polygon) for polygon in coords]
    raise ValueError(f"unsupported geometry type: {geom_type}")


def load_geojson_polygons(
    path: str | Path,
    *,
    domain_id: str,
    unit_id_field: str = "unit_id",
    unit_label_field: str = "unit_label",
    parent_id_field: str = "parent_id",
) -> list[PolygonFeature]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    features = payload.get("features") if isinstance(payload, dict) else None
    if not isinstance(features, list):
        raise ValueError("GeoJSON must be a FeatureCollection with features")

    out: list[PolygonFeature] = []
    for idx, feature in enumerate(features, start=1):
        if not isinstance(feature, dict):
            continue
        geometry = feature.get("geometry") or {}
        properties = feature.get("properties") or {}
        if not isinstance(properties, dict):
            properties = {}
        geometries = _iter_polygon_geometries(geometry)
        raw_unit_id = str(properties.get(unit_id_field) or f"{domain_id}_{idx}").strip()
        raw_label = str(properties.get(unit_label_field) or raw_unit_id).strip()
        raw_parent = str(properties.get(parent_id_field) or "").strip()
        for poly_index, rings in enumerate(geometries, start=1):
            suffix = f"__p{poly_index}" if len(geometries) > 1 else ""
            out.append(
                PolygonFeature(
                    domain_id=domain_id,
                    unit_id=f"{domain_id}:{raw_unit_id}{suffix}",
                    unit_label=raw_label,
                    parent_id=raw_parent,
                    rings=rings,
                    attrs=dict(properties),
                )
            )
    return out


from __future__ import annotations

from dataclasses import dataclass

from urban_sami.geometry.geojson import PolygonFeature


@dataclass(frozen=True)
class PointObservation:
    obs_id: str
    lon: float
    lat: float


def _point_in_ring(point: tuple[float, float], ring: list[tuple[float, float]]) -> bool:
    x, y = point
    inside = False
    n = len(ring)
    if n < 3:
        return False
    for i in range(n):
        x1, y1 = ring[i]
        x2, y2 = ring[(i + 1) % n]
        intersects = ((y1 > y) != (y2 > y))
        if not intersects:
            continue
        denom = (y2 - y1)
        if denom == 0:
            continue
        x_at_y = (x2 - x1) * (y - y1) / denom + x1
        if x < x_at_y:
            inside = not inside
    return inside


def point_in_polygon(point: tuple[float, float], polygon: PolygonFeature) -> bool:
    if not polygon.rings:
        return False
    outer = polygon.rings[0]
    if not _point_in_ring(point, outer):
        return False
    for hole in polygon.rings[1:]:
        if _point_in_ring(point, hole):
            return False
    return True


def assign_points_to_polygons(
    observations: list[PointObservation],
    polygons: list[PolygonFeature],
) -> dict[str, str]:
    out: dict[str, str] = {}
    for obs in observations:
        point = (float(obs.lon), float(obs.lat))
        for polygon in polygons:
            if point_in_polygon(point, polygon):
                out[obs.obs_id] = polygon.unit_id
                break
    return out


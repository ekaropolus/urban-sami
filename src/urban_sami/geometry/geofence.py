from __future__ import annotations

from urban_sami.geometry.base import GeometryDomain


def build_geofence_domain(*, domain_id: str, geofence_id: str, crs: str = "EPSG:4326") -> GeometryDomain:
    return GeometryDomain(
        domain_id=domain_id,
        domain_type="geofence",
        level="geofence",
        crs=crs,
        parameters={"geofence_id": geofence_id},
    )


from __future__ import annotations

from urban_sami.geometry.base import GeometryDomain


def build_admin_domain(*, domain_id: str, level: str, crs: str = "EPSG:4326") -> GeometryDomain:
    return GeometryDomain(
        domain_id=domain_id,
        domain_type="admin",
        level=str(level or "").strip(),
        crs=crs,
        parameters={},
    )


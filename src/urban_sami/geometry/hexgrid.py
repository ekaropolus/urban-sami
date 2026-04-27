from __future__ import annotations

from urban_sami.geometry.base import GeometryDomain


def build_hex_domain(*, domain_id: str, resolution: int, crs: str = "EPSG:4326") -> GeometryDomain:
    return GeometryDomain(
        domain_id=domain_id,
        domain_type="hex",
        level="hex",
        crs=crs,
        parameters={"resolution": int(resolution)},
    )


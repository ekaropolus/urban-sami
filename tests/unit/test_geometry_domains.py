from __future__ import annotations

from urban_sami.geometry import build_admin_domain, build_geofence_domain, build_hex_domain


def test_geometry_backends_build_expected_domain_types():
    admin = build_admin_domain(domain_id="admin_ageb", level="ageb_u")
    hexd = build_hex_domain(domain_id="hex_8", resolution=8)
    geofence = build_geofence_domain(domain_id="gf", geofence_id="g1")
    assert admin.domain_type == "admin"
    assert admin.level == "ageb_u"
    assert hexd.domain_type == "hex"
    assert hexd.parameters["resolution"] == 8
    assert geofence.domain_type == "geofence"
    assert geofence.parameters["geofence_id"] == "g1"


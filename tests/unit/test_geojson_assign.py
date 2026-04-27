from __future__ import annotations

from pathlib import Path

from urban_sami.geometry import PointObservation, assign_points_to_polygons, load_geojson_polygons, point_in_polygon


def test_geojson_loader_and_assignment():
    polygons = load_geojson_polygons(Path("tests/fixtures/geojson/mini_polygons.geojson"), domain_id="geo_demo")
    assert len(polygons) == 2
    point = PointObservation(obs_id="p1", lon=-99.5, lat=20.5)
    assert point_in_polygon((point.lon, point.lat), polygons[0]) is True
    assigned = assign_points_to_polygons([point], polygons)
    assert assigned["p1"] == "geo_demo:poly_a"


from __future__ import annotations

from urban_sami.io.inegi_ageb import parse_ageb_geojson


def test_parse_ageb_geojson_reads_codes_population_and_geometry():
    payload = """
    {
      "type": "FeatureCollection",
      "features": [
        {
          "type": "Feature",
          "properties": {
            "cvegeo": "010010001216A",
            "cve_ent": "01",
            "nom_ent": "Aguascalientes",
            "cve_mun": "001",
            "nom_mun": "Aguascalientes",
            "cve_loc": "0001",
            "pob_total": "2657",
            "pob_masculina": "1213",
            "pob_femenina": "1444",
            "total_viviendas_habitadas": "815",
            "cve_ageb": "216A"
          },
          "geometry": {
            "type": "MultiPolygon",
            "coordinates": []
          }
        }
      ]
    }
    """
    rows = parse_ageb_geojson(payload)
    assert len(rows) == 1
    assert rows[0].unit_code == "010010001216A"
    assert rows[0].city_code == "01001"
    assert rows[0].population_total == 2657
    assert rows[0].households == 815
    assert rows[0].geometry["type"] == "MultiPolygon"

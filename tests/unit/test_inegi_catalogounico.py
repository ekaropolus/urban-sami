from __future__ import annotations

from urban_sami.io.inegi_catalogounico import parse_municipality_payload


def test_parse_municipality_payload_reads_codes_and_population():
    payload = """
    {
      "datos": [
        {
          "cvegeo": "01001",
          "cve_ent": "01",
          "cve_mun": "001",
          "nomgeo": "Aguascalientes",
          "cve_cab": "0001",
          "nom_cab": "Aguascalientes",
          "pob_total": "948990",
          "pob_femenina": "486917",
          "pob_masculina": "462073",
          "total_viviendas_habitadas": "266942"
        }
      ]
    }
    """
    rows = parse_municipality_payload(payload)
    assert len(rows) == 1
    assert rows[0].city_code == "01001"
    assert rows[0].state_code == "01"
    assert rows[0].city_name == "Aguascalientes"
    assert rows[0].population_total == 948990
    assert rows[0].households == 266942

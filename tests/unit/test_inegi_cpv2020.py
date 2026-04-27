from __future__ import annotations

from urban_sami.io.inegi_cpv2020 import parse_state_population_xml


def test_parse_state_population_xml_reads_population_totals():
    xml_text = '<root><row nombre="Zacatecas" Total="1 622 138" Hombres="790 544" Mujeres="831 594" /></root>'
    row = parse_state_population_xml(xml_text, state_code="32")
    assert row.state_code == "32"
    assert row.state_name == "Zacatecas"
    assert row.population_total == 1622138
    assert row.population_male == 790544
    assert row.population_female == 831594

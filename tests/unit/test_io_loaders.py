from __future__ import annotations

from pathlib import Path

from urban_sami.io.loaders import load_observations, load_units


def test_load_units_reads_generic_unit_csv():
    rows = load_units(Path("tests/fixtures/indicator_compute/units.csv"), source_type="csv_units")
    assert len(rows) == 5
    assert rows[0].domain_id == "admin_ageb"
    assert rows[0].population == 500.0


def test_load_observations_reads_denue_aliases():
    rows = load_observations(Path("tests/fixtures/indicator_compute/observations_denue_like.csv"), source_type="denue_csv")
    assert len(rows) == 3
    assert rows[0].domain_id == "admin_ageb"
    assert rows[0].unit_id == "admin_ageb:001"
    assert rows[0].scian_code == "461110"


from __future__ import annotations

from urban_sami.analysis.experiment_pack import UnitDatum, aggregate_to_city, synthetic_bundle_rows


def test_aggregate_to_city_sums_y_and_n():
    rows = [
        UnitDatum(level="manzana", fit_method="ols", unit_code="11020:001", unit_label="a", y=2.0, n=10.0),
        UnitDatum(level="manzana", fit_method="ols", unit_code="11020:002", unit_label="b", y=3.0, n=15.0),
        UnitDatum(level="manzana", fit_method="ols", unit_code="22014:001", unit_label="c", y=5.0, n=20.0),
    ]
    out = aggregate_to_city(rows, level_name="city")
    by_code = {row.unit_code: row for row in out}
    assert by_code["11020"].y == 5.0
    assert by_code["11020"].n == 25.0
    assert by_code["22014"].y == 5.0
    assert by_code["22014"].n == 20.0


def test_synthetic_bundle_rows_is_deterministic_for_seed():
    rows = [
        UnitDatum(level="manzana", fit_method="ols", unit_code=f"11020:{idx:03d}", unit_label=str(idx), y=float(idx), n=float(idx + 10))
        for idx in range(1, 6)
    ]
    left = synthetic_bundle_rows(rows, bundle_size=2, seed=7, level_name="synthetic")
    right = synthetic_bundle_rows(rows, bundle_size=2, seed=7, level_name="synthetic")
    assert [(row.unit_code, row.y, row.n) for row in left] == [(row.unit_code, row.y, row.n) for row in right]

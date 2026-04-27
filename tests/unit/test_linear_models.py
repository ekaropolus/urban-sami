from __future__ import annotations

from urban_sami.analysis.linear_models import compare_nested_models, ols_fit, pearson_corr


def test_ols_fit_recovers_simple_linear_relation():
    x = [[1.0, 0.0], [1.0, 1.0], [1.0, 2.0], [1.0, 3.0]]
    y = [2.0, 5.0, 8.0, 11.0]
    result = ols_fit(x, y)
    assert abs(result.coefficients[0] - 2.0) < 1e-9
    assert abs(result.coefficients[1] - 3.0) < 1e-9
    assert result.r2 > 0.999999


def test_compare_nested_models_detects_better_full_model():
    y = [1.0, 2.0, 9.0, 12.0]
    restricted = ols_fit([[1.0] for _ in y], y)
    full = ols_fit([[1.0, 0.0], [1.0, 0.0], [1.0, 1.0], [1.0, 1.0]], y)
    cmp = compare_nested_models(restricted, full)
    assert cmp.f_stat > 0
    assert cmp.p_value is not None
    assert cmp.p_value < 0.1


def test_pearson_corr_handles_basic_case():
    x = [1.0, 2.0, 3.0, 4.0]
    y = [2.0, 4.0, 6.0, 8.0]
    assert pearson_corr(x, y) > 0.99

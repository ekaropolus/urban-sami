from __future__ import annotations

import math

from urban_sami.modeling import bootstrap_fit_intervals, compute_deviation_score, fit_by_name, fit_ols


def test_fit_ols_recovers_linear_scaling_reasonably():
    n = [100.0, 200.0, 400.0, 800.0, 1600.0]
    y = [10.0, 20.5, 39.0, 81.0, 163.0]
    result = fit_ols(y, n)
    assert 0.95 <= result.beta <= 1.05
    assert result.r2 > 0.99
    assert result.fit_method == "ols"


def test_fit_by_name_supports_all_current_methods():
    n = [100.0, 150.0, 250.0, 400.0, 650.0, 900.0]
    y = [5.0, 7.0, 12.0, 20.0, 31.0, 42.0]
    for method in ("ols", "robust", "poisson", "negbin", "auto"):
        result = fit_by_name(y, n, method)
        assert result.fit_method == method
        assert result.alpha == result.alpha
        assert result.beta == result.beta
        assert result.r2 == result.r2


def test_bootstrap_fit_intervals_return_bounds_for_stable_inputs():
    n = [100.0, 150.0, 250.0, 400.0, 650.0, 900.0]
    y = [5.0, 7.0, 12.0, 20.0, 31.0, 42.0]
    intervals = bootstrap_fit_intervals(y, n, "ols", n_bootstrap=40, seed=7)
    assert intervals["alpha_low"] is not None
    assert intervals["alpha_high"] is not None
    assert intervals["beta_low"] is not None
    assert intervals["beta_high"] is not None
    assert float(intervals["beta_low"]) <= float(intervals["beta_high"])


def test_compute_deviation_score_uses_raw_log_deviation_as_sami():
    score = compute_deviation_score(y_value=120.0, n_value=100.0, alpha=0.0, beta=1.0, residual_std=0.5)
    expected = math.log(120.0 / 100.0)
    assert abs(score.y_expected - 100.0) < 1e-9
    assert abs(score.epsilon_log - expected) < 1e-9
    assert abs(score.sami - expected) < 1e-9
    assert abs(score.z_residual - (expected / 0.5)) < 1e-9

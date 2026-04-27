from __future__ import annotations

import math
from pathlib import Path

from urban_sami.io.csvio import read_csv_rows


SUMMARY_COLUMNS = [
    "n_field",
    "level",
    "filter_mode",
    "fit_method",
    "units",
    "alpha",
    "beta",
    "r2",
    "resid_std",
    "aic",
    "bic",
    "y_min",
    "y_p95",
    "y_max",
    "n_min",
    "n_p05",
    "n_p95",
    "n_max",
]


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _mean(values: list[float]) -> float:
    return sum(values) / float(len(values))


def _stddev(values: list[float], *, ddof: int = 0) -> float:
    if len(values) <= ddof:
        return 0.0
    mu = _mean(values)
    return math.sqrt(sum((value - mu) ** 2 for value in values) / float(len(values) - ddof))


def _percentile(values: list[float], p: float) -> float:
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return ordered[0]
    idx = (len(ordered) - 1) * float(p)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return ordered[lo]
    frac = idx - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def _gaussian_ic_from_residual(residual: list[float], *, k: int = 2) -> tuple[float | None, float | None]:
    n_obs = int(len(residual))
    if n_obs <= 0:
        return (None, None)
    sse = float(sum(value * value for value in residual))
    if sse <= 0:
        return (None, None)
    sigma2 = sse / float(n_obs)
    if sigma2 <= 0:
        return (None, None)
    llf = -0.5 * n_obs * (math.log(2.0 * math.pi * sigma2) + 1.0)
    aic = (2.0 * float(k)) - (2.0 * llf)
    bic = (float(k) * math.log(float(n_obs))) - (2.0 * llf)
    return (aic, bic)


def _alpha_beta_from_yhat(yhat: list[float], n: list[float]) -> tuple[float, float]:
    x = [math.log(float(value)) for value in n]
    z_hat = [math.log(max(1e-300, float(value))) for value in yhat]
    x_bar = _mean(x)
    z_bar = _mean(z_hat)
    var_x = float(sum((value - x_bar) ** 2 for value in x))
    if var_x <= 0:
        raise ValueError("zero variance in log(n)")
    beta = float(sum((xi - x_bar) * (zi - z_bar) for xi, zi in zip(x, z_hat)) / var_x)
    alpha = float(z_bar - (beta * x_bar))
    return alpha, beta


def _r2_from_log_residual(y: list[float], residual: list[float]) -> float:
    z = [math.log(float(value)) for value in y]
    z_bar = _mean(z)
    ss_tot = float(sum((zi - z_bar) ** 2 for zi in z))
    ss_res = float(sum(float(r) ** 2 for r in residual))
    return (1.0 - (ss_res / ss_tot)) if ss_tot > 0 else 0.0


def _poisson_stats(y: list[float], yhat: list[float]) -> tuple[float, float | None, float | None]:
    dev = 0.0
    mu_null_value = _mean(y)
    null_dev = 0.0
    for yi, mui in zip(y, yhat):
        yi_safe = max(0.0, float(yi))
        mui_safe = max(1e-9, float(mui))
        if yi_safe > 0:
            dev += 2.0 * ((yi_safe * math.log(yi_safe / mui_safe)) - (yi_safe - mui_safe))
        else:
            dev += 2.0 * (0.0 - (yi_safe - mui_safe))
        if yi_safe > 0:
            null_dev += 2.0 * ((yi_safe * math.log(yi_safe / mu_null_value)) - (yi_safe - mu_null_value))
        else:
            null_dev += 2.0 * (0.0 - (yi_safe - mu_null_value))
    r2 = float(1.0 - (dev / null_dev)) if null_dev > 0 else 0.0
    llf = 0.0
    for yi, mui in zip(y, yhat):
        mui = max(1e-9, float(mui))
        llf += (float(yi) * math.log(mui)) - mui - math.lgamma(float(yi) + 1.0)
    k = 2.0
    aic = (2.0 * k) - (2.0 * llf)
    bic = (k * math.log(float(len(y)))) - (2.0 * llf)
    return r2, aic, bic


def _negbin_loglike(y: list[float], mu: list[float], alpha: float) -> float:
    if alpha <= 0:
        return float("-inf")
    size = 1.0 / float(alpha)
    total = 0.0
    for yi, mui in zip(y, mu):
        yi = max(0.0, float(yi))
        mui = max(1e-12, float(mui))
        total += math.lgamma(yi + size) - math.lgamma(size) - math.lgamma(yi + 1.0)
        total += size * math.log(size / (size + mui))
        total += yi * math.log(mui / (size + mui))
    return total


def _maximize_log_alpha(y: list[float], mu: list[float]) -> float:
    left = -16.0
    right = 8.0
    phi = (1.0 + math.sqrt(5.0)) / 2.0
    invphi = 1.0 / phi
    invphi2 = invphi * invphi
    c = right - (right - left) * invphi
    d = left + (right - left) * invphi
    fc = _negbin_loglike(y, mu, math.exp(c))
    fd = _negbin_loglike(y, mu, math.exp(d))
    for _ in range(120):
        if abs(right - left) < 1e-8:
            break
        if fc > fd:
            right = d
            d = c
            fd = fc
            c = right - (right - left) * invphi
            fc = _negbin_loglike(y, mu, math.exp(c))
        else:
            left = c
            c = d
            fc = fd
            d = left + (right - left) * invphi
            fd = _negbin_loglike(y, mu, math.exp(d))
    return math.exp((left + right) / 2.0)


def _negbin_stats(y: list[float], yhat: list[float]) -> tuple[float, float | None, float | None]:
    alpha_hat = _maximize_log_alpha(y, yhat)
    llf = _negbin_loglike(y, yhat, alpha_hat)
    mu_null = [_mean(y)] * len(y)
    alpha_null = _maximize_log_alpha(y, mu_null)
    llnull = _negbin_loglike(y, mu_null, alpha_null)
    r2 = float(1.0 - (llf / llnull)) if math.isfinite(llf) and math.isfinite(llnull) and llnull != 0.0 else 0.0
    k = 3.0
    aic = (2.0 * k) - (2.0 * llf)
    bic = (k * math.log(float(len(y)))) - (2.0 * llf)
    return r2, aic, bic


def summarize_exported_units(
    units_csv_path: str | Path,
    *,
    n_field: str,
    level: str,
    filter_mode: str,
    fit_method: str,
) -> dict[str, object]:
    rows = read_csv_rows(units_csv_path)
    y = [_to_float(row.get("y")) for row in rows]
    n = [_to_float(row.get("n")) for row in rows]
    yhat = [_to_float(row.get("yhat")) for row in rows]
    residual = [_to_float(row.get("residual")) for row in rows]
    if not y:
        raise ValueError(f"no unit rows found in {units_csv_path}")

    alpha, beta = _alpha_beta_from_yhat(yhat, n)
    resid_std = _stddev(residual, ddof=1) if len(residual) > 1 else 0.0
    method = str(fit_method or "").strip().lower()
    if method in {"ols", "robust", "auto"}:
        r2 = _r2_from_log_residual(y, residual)
        aic, bic = _gaussian_ic_from_residual(residual, k=2)
    elif method == "poisson":
        r2, aic, bic = _poisson_stats(y, yhat)
    elif method == "negbin":
        r2, aic, bic = _negbin_stats(y, yhat)
    else:
        raise ValueError(f"unsupported fit method: {fit_method}")

    return {
        "n_field": n_field,
        "level": level,
        "filter_mode": filter_mode,
        "fit_method": fit_method,
        "units": int(len(y)),
        "alpha": alpha,
        "beta": beta,
        "r2": r2,
        "resid_std": resid_std,
        "aic": aic if aic is not None else "",
        "bic": bic if bic is not None else "",
        "y_min": min(y),
        "y_p95": _percentile(y, 0.95),
        "y_max": max(y),
        "n_min": min(n),
        "n_p05": _percentile(n, 0.05),
        "n_p95": _percentile(n, 0.95),
        "n_max": max(n),
    }


def unit_export_filename(*, n_field: str, level: str, filter_mode: str, fit_method: str) -> str:
    return f"{n_field}__{level}__{filter_mode}__{fit_method}_units.csv"

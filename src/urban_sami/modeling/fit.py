from __future__ import annotations

import math
import random
from dataclasses import dataclass


@dataclass(frozen=True)
class FitResult:
    alpha: float
    beta: float
    r2: float
    residual_std: float
    fit_method: str
    aic: float | None = None
    bic: float | None = None


@dataclass(frozen=True)
class DeviationScore:
    y_expected: float
    epsilon_log: float
    sami: float
    z_residual: float


def _mean(values: list[float]) -> float:
    return sum(values) / float(len(values))


def _variance(values: list[float], *, ddof: int = 0) -> float:
    if len(values) <= ddof:
        return 0.0
    mu = _mean(values)
    return sum((value - mu) ** 2 for value in values) / float(len(values) - ddof)


def _stddev(values: list[float], *, ddof: int = 0) -> float:
    return math.sqrt(max(0.0, _variance(values, ddof=ddof)))


def _covariance(x: list[float], y: list[float]) -> float:
    mu_x = _mean(x)
    mu_y = _mean(y)
    return sum((xi - mu_x) * (yi - mu_y) for xi, yi in zip(x, y))


def _percentile(values: list[float], p: float) -> float:
    if not values:
        raise ValueError("cannot compute percentile of empty sequence")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    idx = (len(ordered) - 1) * p
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
    llf = -0.5 * n_obs * (math.log(2.0 * math.pi * sigma2) + 1.0)
    aic = (2.0 * float(k)) - (2.0 * llf)
    bic = (float(k) * math.log(float(n_obs))) - (2.0 * llf)
    return (aic, bic)


def expected_y(alpha: float, beta: float, n_value: float) -> float:
    n_safe = max(1e-9, float(n_value))
    return math.exp(float(alpha) + (float(beta) * math.log(n_safe)))


def log_residual(y_value: float, n_value: float, alpha: float, beta: float) -> float:
    y_safe = max(1e-9, float(y_value))
    y_expected = expected_y(alpha, beta, n_value)
    return math.log(y_safe) - math.log(max(1e-9, y_expected))


def compute_deviation_score(y_value: float, n_value: float, alpha: float, beta: float, residual_std: float) -> DeviationScore:
    y_expected = expected_y(alpha, beta, n_value)
    epsilon = math.log(max(1e-9, float(y_value))) - math.log(max(1e-9, y_expected))
    z_residual = epsilon / float(residual_std) if float(residual_std) > 0 else 0.0
    return DeviationScore(
        y_expected=y_expected,
        epsilon_log=epsilon,
        sami=epsilon,
        z_residual=z_residual,
    )


def fit_ols(y, n) -> FitResult:
    yv = [float(value) for value in y]
    nv = [float(value) for value in n]
    x = [math.log(value) for value in nv]
    z = [math.log(value) for value in yv]
    x_bar = _mean(x)
    z_bar = _mean(z)
    var_x = float(sum((value - x_bar) ** 2 for value in x))
    if var_x <= 0:
        raise ValueError("zero variance in log(N)")
    beta = float(sum((xi - x_bar) * (zi - z_bar) for xi, zi in zip(x, z)) / var_x)
    alpha = float(z_bar - (beta * x_bar))
    z_hat = [alpha + (beta * value) for value in x]
    residual = [zi - zhi for zi, zhi in zip(z, z_hat)]
    ss_tot = float(sum((zi - z_bar) ** 2 for zi in z))
    ss_res = float(sum((zi - zhi) ** 2 for zi, zhi in zip(z, z_hat)))
    r2 = (1.0 - (ss_res / ss_tot)) if ss_tot > 0 else 0.0
    resid_std = _stddev(residual, ddof=1) if len(residual) > 1 else 0.0
    aic, bic = _gaussian_ic_from_residual(residual, k=2)
    return FitResult(alpha=alpha, beta=beta, r2=r2, residual_std=resid_std, fit_method="ols", aic=aic, bic=bic)


def fit_robust(y, n) -> FitResult:
    base = fit_ols(y, n)
    yv = [float(value) for value in y]
    nv = [float(value) for value in n]
    x = [math.log(value) for value in nv]
    z = [math.log(value) for value in yv]
    pred = [base.alpha + (base.beta * value) for value in x]
    resid = [zi - pi for zi, pi in zip(z, pred)]
    med = _percentile(resid, 0.5)
    mad = _percentile([abs(value - med) for value in resid], 0.5)
    if mad > 0:
        weights = []
        for value in resid:
            score = abs(value - med) / (1.345 * mad)
            weights.append(1.0 if score <= 1.0 else (1.0 / score))
        sw = sum(weights)
        x_bar = sum(w * xi for w, xi in zip(weights, x)) / sw
        z_bar = sum(w * zi for w, zi in zip(weights, z)) / sw
        num = sum(w * (xi - x_bar) * (zi - z_bar) for w, xi, zi in zip(weights, x, z))
        den = sum(w * (xi - x_bar) ** 2 for w, xi in zip(weights, x))
        if den > 0:
            beta = num / den
            alpha = z_bar - (beta * x_bar)
            pred = [alpha + (beta * value) for value in x]
            resid = [zi - pi for zi, pi in zip(z, pred)]
            sse = float(sum((zi - pi) ** 2 for zi, pi in zip(z, pred)))
            sst = float(sum((zi - _mean(z)) ** 2 for zi in z))
            r2 = 0.0 if sst <= 0 else max(0.0, min(1.0, 1.0 - (sse / sst)))
            aic, bic = _gaussian_ic_from_residual(resid, k=2)
            return FitResult(
                alpha=float(alpha),
                beta=float(beta),
                r2=r2,
                residual_std=_stddev(resid, ddof=1) if len(resid) > 1 else 0.0,
                fit_method="robust",
                aic=aic,
                bic=bic,
            )
    aic, bic = _gaussian_ic_from_residual(resid, k=2)
    return FitResult(
        alpha=base.alpha,
        beta=base.beta,
        r2=base.r2,
        residual_std=_stddev(resid, ddof=1) if len(resid) > 1 else 0.0,
        fit_method="robust",
        aic=aic,
        bic=bic,
    )


def fit_poisson(y, n) -> FitResult:
    # First spin-out pass: stable approximation using log-log OLS baseline.
    base = fit_ols(y, n)
    yv = [float(value) for value in y]
    nv = [float(value) for value in n]
    yhat = [math.exp(base.alpha + (base.beta * math.log(value))) for value in nv]
    residual = [math.log(max(1e-9, yi)) - math.log(max(1e-9, yhi)) for yi, yhi in zip(yv, yhat)]
    dev = 0.0
    mu_null = _mean(yv)
    null_dev = 0.0
    for yi, yhi in zip(yv, yhat):
        yi_safe = max(1e-9, yi)
        yhat_safe = max(1e-9, yhi)
        dev += 2.0 * ((yi_safe * math.log(yi_safe / yhat_safe)) - (yi_safe - yhat_safe))
        null_dev += 2.0 * ((yi_safe * math.log(yi_safe / mu_null)) - (yi_safe - mu_null))
    r2 = float(1.0 - (dev / null_dev)) if null_dev > 0 else 0.0
    llf = 0.0
    for yi, mui in zip(yv, yhat):
        mui = max(1e-9, mui)
        llf += (yi * math.log(mui)) - mui - math.lgamma(yi + 1.0)
    k = 2
    aic = (2.0 * k) - (2.0 * llf)
    bic = (k * math.log(float(len(yv)))) - (2.0 * llf)
    return FitResult(
        alpha=base.alpha,
        beta=base.beta,
        r2=r2,
        residual_std=_stddev(residual, ddof=1) if len(residual) > 1 else 0.0,
        fit_method="poisson",
        aic=aic,
        bic=bic,
    )


def fit_negbin(y, n) -> FitResult:
    # First spin-out pass: dispersion-aware approximation from OLS residual variance.
    base = fit_ols(y, n)
    yv = [float(value) for value in y]
    nv = [float(value) for value in n]
    yhat = [math.exp(base.alpha + (base.beta * math.log(value))) for value in nv]
    residual = [math.log(max(1e-9, yi)) - math.log(max(1e-9, yhi)) for yi, yhi in zip(yv, yhat)]
    llf = sum((yi * math.log(max(1e-9, mui))) - max(1e-9, mui) for yi, mui in zip(yv, yhat))
    llnull_mu = _mean(yv)
    llnull = sum((yi * math.log(max(1e-9, llnull_mu))) - max(1e-9, llnull_mu) for yi in yv)
    r2 = max(0.0, 1.0 - (llf / llnull)) if llnull != 0.0 else 0.0
    dispersion_penalty = max(1.0, _variance(residual, ddof=1) * len(yv))
    aic = (2.0 * 3.0) - (2.0 * llf) + dispersion_penalty
    bic = (3.0 * math.log(float(len(yv)))) - (2.0 * llf) + dispersion_penalty
    return FitResult(
        alpha=base.alpha,
        beta=base.beta,
        r2=r2,
        residual_std=_stddev(residual, ddof=1) if len(residual) > 1 else 0.0,
        fit_method="negbin",
        aic=aic,
        bic=bic,
    )


def fit_auto(y, n) -> FitResult:
    ols = fit_ols(y, n)
    yv = [float(value) for value in y]
    nv = [float(value) for value in n]
    residual = [math.log(yi) - (ols.alpha + ols.beta * math.log(ni)) for yi, ni in zip(yv, nv)]
    if len(residual) < 5:
        return FitResult(alpha=ols.alpha, beta=ols.beta, r2=ols.r2, residual_std=ols.residual_std, fit_method="auto", aic=ols.aic, bic=ols.bic)
    med = _percentile(residual, 0.5)
    mad = _percentile([abs(value - med) for value in residual], 0.5)
    outlier_share = 0.0
    if mad > 0:
        zscores = [abs(0.6745 * (value - med) / mad) for value in residual]
        outlier_share = float(sum(1.0 for value in zscores if value > 3.5) / len(zscores))
    if outlier_share > 0.10:
        robust = fit_robust(y, n)
        return FitResult(alpha=robust.alpha, beta=robust.beta, r2=robust.r2, residual_std=robust.residual_std, fit_method="auto", aic=robust.aic, bic=robust.bic)
    return FitResult(alpha=ols.alpha, beta=ols.beta, r2=ols.r2, residual_std=ols.residual_std, fit_method="auto", aic=ols.aic, bic=ols.bic)


def bootstrap_fit_intervals(
    y,
    n,
    fit_method: str,
    *,
    n_bootstrap: int = 200,
    seed: int = 42,
    ci: float = 0.95,
) -> dict[str, float | None]:
    yv = [float(value) for value in y]
    nv = [float(value) for value in n]
    if len(yv) != len(nv) or len(yv) < 2 or n_bootstrap <= 1:
        return {"alpha_low": None, "alpha_high": None, "beta_low": None, "beta_high": None}
    rng = random.Random(seed)
    alpha_samples: list[float] = []
    beta_samples: list[float] = []
    total = len(yv)
    for _ in range(int(n_bootstrap)):
        idxs = [rng.randrange(total) for _ in range(total)]
        y_sample = [yv[idx] for idx in idxs]
        n_sample = [nv[idx] for idx in idxs]
        if len({round(value, 12) for value in n_sample}) < 2:
            continue
        try:
            result = fit_by_name(y_sample, n_sample, fit_method)
        except Exception:
            continue
        alpha_samples.append(float(result.alpha))
        beta_samples.append(float(result.beta))
    if len(alpha_samples) < 5 or len(beta_samples) < 5:
        return {"alpha_low": None, "alpha_high": None, "beta_low": None, "beta_high": None}
    tail = max(0.0, min(1.0, (1.0 - float(ci)) / 2.0))
    lower = tail
    upper = 1.0 - tail
    return {
        "alpha_low": _percentile(alpha_samples, lower),
        "alpha_high": _percentile(alpha_samples, upper),
        "beta_low": _percentile(beta_samples, lower),
        "beta_high": _percentile(beta_samples, upper),
    }


def fit_by_name(y: np.ndarray, n: np.ndarray, fit_method: str) -> FitResult:
    method = str(fit_method or "").strip().lower()
    if method == "ols":
        return fit_ols(y, n)
    if method == "robust":
        return fit_robust(y, n)
    if method == "poisson":
        return fit_poisson(y, n)
    if method == "negbin":
        return fit_negbin(y, n)
    if method == "auto":
        return fit_auto(y, n)
    raise ValueError(f"unsupported fit method: {fit_method}")

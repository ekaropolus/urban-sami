from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class OLSResult:
    coefficients: list[float]
    stderr: list[float]
    fitted: list[float]
    residuals: list[float]
    rss: float
    tss: float
    r2: float
    adj_r2: float
    n_obs: int
    n_params: int
    df_resid: int
    sigma2: float


@dataclass(frozen=True)
class NestedModelComparison:
    f_stat: float
    df_num: int
    df_den: int
    p_value: float | None
    rss_restricted: float
    rss_full: float


def _transpose(matrix: list[list[float]]) -> list[list[float]]:
    return [list(col) for col in zip(*matrix)]


def _matmul(a: list[list[float]], b: list[list[float]]) -> list[list[float]]:
    b_t = _transpose(b)
    return [[sum(ai * bj for ai, bj in zip(row, col)) for col in b_t] for row in a]


def _matvec(a: list[list[float]], x: list[float]) -> list[float]:
    return [sum(ai * xi for ai, xi in zip(row, x)) for row in a]


def _invert_matrix(matrix: list[list[float]]) -> list[list[float]]:
    n = len(matrix)
    aug = [row[:] + [1.0 if i == j else 0.0 for j in range(n)] for i, row in enumerate(matrix)]
    for col in range(n):
        pivot = max(range(col, n), key=lambda row: abs(aug[row][col]))
        if abs(aug[pivot][col]) < 1e-12:
            raise ValueError("singular matrix")
        if pivot != col:
            aug[col], aug[pivot] = aug[pivot], aug[col]
        pivot_val = aug[col][col]
        aug[col] = [value / pivot_val for value in aug[col]]
        for row in range(n):
            if row == col:
                continue
            factor = aug[row][col]
            if factor == 0.0:
                continue
            aug[row] = [rv - (factor * cv) for rv, cv in zip(aug[row], aug[col])]
    return [row[n:] for row in aug]


def ols_fit(design: list[list[float]], response: list[float]) -> OLSResult:
    if not design or not response or len(design) != len(response):
        raise ValueError("design and response must be non-empty and aligned")
    n_obs = len(response)
    n_params = len(design[0])
    xt = _transpose(design)
    xtx = _matmul(xt, design)
    xtx_inv = _invert_matrix(xtx)
    xty = [sum(xij * yi for xij, yi in zip(col, response)) for col in xt]
    coefficients = _matvec(xtx_inv, xty)
    fitted = [sum(beta * xij for beta, xij in zip(coefficients, row)) for row in design]
    residuals = [yi - yhat for yi, yhat in zip(response, fitted)]
    rss = sum(value * value for value in residuals)
    y_bar = sum(response) / float(n_obs)
    tss = sum((yi - y_bar) ** 2 for yi in response)
    r2 = 0.0 if tss <= 0 else max(0.0, 1.0 - (rss / tss))
    df_resid = n_obs - n_params
    sigma2 = rss / float(df_resid) if df_resid > 0 else 0.0
    cov = [[sigma2 * value for value in row] for row in xtx_inv]
    stderr = [math.sqrt(max(0.0, cov[i][i])) for i in range(n_params)]
    if df_resid > 0 and n_obs > 1:
        adj_r2 = 1.0 - ((rss / df_resid) / (tss / (n_obs - 1))) if tss > 0 else 0.0
    else:
        adj_r2 = r2
    return OLSResult(
        coefficients=coefficients,
        stderr=stderr,
        fitted=fitted,
        residuals=residuals,
        rss=rss,
        tss=tss,
        r2=r2,
        adj_r2=adj_r2,
        n_obs=n_obs,
        n_params=n_params,
        df_resid=df_resid,
        sigma2=sigma2,
    )


def _betacf(a: float, b: float, x: float) -> float:
    max_iter = 200
    eps = 3e-14
    fpmin = 1e-30
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    c = 1.0
    d = 1.0 - (qab * x / qap)
    if abs(d) < fpmin:
        d = fpmin
    d = 1.0 / d
    h = d
    for m in range(1, max_iter + 1):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + (aa * d)
        if abs(d) < fpmin:
            d = fpmin
        c = 1.0 + (aa / c)
        if abs(c) < fpmin:
            c = fpmin
        d = 1.0 / d
        h *= d * c
        aa = -((a + m) * (qab + m) * x) / ((a + m2) * (qap + m2))
        d = 1.0 + (aa * d)
        if abs(d) < fpmin:
            d = fpmin
        c = 1.0 + (aa / c)
        if abs(c) < fpmin:
            c = fpmin
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < eps:
            break
    return h


def _reg_incomplete_beta(a: float, b: float, x: float) -> float:
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    ln_beta = math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
    front = math.exp((a * math.log(x)) + (b * math.log(1.0 - x)) + ln_beta)
    if x < (a + 1.0) / (a + b + 2.0):
        return front * _betacf(a, b, x) / a
    return 1.0 - (front * _betacf(b, a, 1.0 - x) / b)


def _f_sf(f_stat: float, df_num: int, df_den: int) -> float | None:
    if f_stat < 0 or df_num <= 0 or df_den <= 0:
        return None
    x = df_den / (df_den + (df_num * f_stat))
    return _reg_incomplete_beta(df_den / 2.0, df_num / 2.0, x)


def compare_nested_models(restricted: OLSResult, full: OLSResult) -> NestedModelComparison:
    if full.n_params <= restricted.n_params:
        raise ValueError("full model must have more parameters than restricted model")
    df_num = full.n_params - restricted.n_params
    df_den = full.df_resid
    if df_den <= 0:
        raise ValueError("full model must have positive residual degrees of freedom")
    numerator = (restricted.rss - full.rss) / float(df_num)
    denominator = full.rss / float(df_den)
    f_stat = numerator / denominator if denominator > 0 else 0.0
    p_value = _f_sf(f_stat, df_num, df_den)
    return NestedModelComparison(
        f_stat=f_stat,
        df_num=df_num,
        df_den=df_den,
        p_value=p_value,
        rss_restricted=restricted.rss,
        rss_full=full.rss,
    )


def pearson_corr(x: list[float], y: list[float]) -> float:
    if len(x) != len(y) or len(x) < 2:
        return 0.0
    x_bar = sum(x) / float(len(x))
    y_bar = sum(y) / float(len(y))
    sxx = sum((xi - x_bar) ** 2 for xi in x)
    syy = sum((yi - y_bar) ** 2 for yi in y)
    sxy = sum((xi - x_bar) * (yi - y_bar) for xi, yi in zip(x, y))
    if sxx <= 0 or syy <= 0:
        return 0.0
    return sxy / math.sqrt(sxx * syy)

# SAMI National Matrix Report

- Country: `MX`
- Y source: `denue_est_count`
- N fields: `population`
- Levels: `state`
- Fit methods: `ols, robust, poisson, negbin, auto`
- Filter modes: `raw`
- Filters: `min_n=30.0, trim_low_q=0.0, trim_high_q=0.995`
- Generated (UTC): `2026-03-15T22:19:02.103479`
- Experiment slug: `matrix-y-denue_est_count-n-population-lvl-state-fit-ols-robust-poisson-negbin-auto-filt-raw-scope-all-cities`
- Run dir: `/app/data/sami_experiments/mx/matrix-y-denue_est_count-n-population-lvl-state-fit-ols-robust-poisson-negbin-auto-filt-raw-scope-all-cities/20260315_221902`

## Equations

Common scaling definition:
```text
ln(Y_i) = alpha + beta * ln(N_i) + epsilon_i
Yhat_i = exp(alpha + beta * ln(N_i))
residual_i = ln(Y_i) - ln(Yhat_i)
SAMI_i = residual_i / sigma_residual
```

Fit methods:
- `ols`: ordinary least squares in log-log space.
- `poisson`: count GLM (log link) with pseudo-R2 by deviance.
- `robust`: robust log-log linear fit (Huber loss), less sensitive to outliers.
- `negbin`: negative binomial count model (log link), useful when count variance is overdispersed.
- `auto`: starts with OLS; switches to robust when residual outlier share is high.

## Summary Matrix

| N field | Level | Filter | Fit | Units | Alpha | Beta | R2 | Residual STD | AIC | BIC |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| population | state | raw | ols | 32 | -2.506695 | 0.958799 | 0.943435 | 0.173930 | -18.146 | -15.215 |
| population | state | raw | robust | 32 | -2.517972 | 0.959330 | 0.943413 | 0.173931 | -18.134 | -15.203 |
| population | state | raw | poisson | 32 | -2.886312 | 0.984875 | 0.951177 | 0.175000 | 147887.005 | 147889.937 |
| population | state | raw | negbin | 32 | -2.495636 | 0.959053 | 0.114973 | 0.173930 | 739.450 | 743.847 |
| population | state | raw | auto | 32 | -2.506695 | 0.958799 | 0.943435 | 0.173930 | -18.146 | -15.215 |

## Notes
- `raw`: uses all units with `Y>0` and `N>0`.
- `filtered`: applies `N>=min_n` and optional N-quantile trimming.
- Compare runs by same level and same fit before interpreting beta/SAMI shifts.

## Recommended Filter Policy (Auto)

| N field | Level | Fit | Recommended | Reason | ΔR²(filtered-raw) | Keep ratio |
|---|---|---|---|---|---:|---:|
| population | state | auto | raw | filtered mode not present in this run | - | - |
| population | state | negbin | raw | filtered mode not present in this run | - | - |
| population | state | ols | raw | filtered mode not present in this run | - | - |
| population | state | poisson | raw | filtered mode not present in this run | - | - |
| population | state | robust | raw | filtered mode not present in this run | - | - |
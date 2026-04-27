# SAMI National Matrix Report

- Country: `MX`
- Y source: `denue_est_count`
- N fields: `population`
- Levels: `manzana`
- Fit methods: `ols, robust, poisson, negbin, auto`
- Filter modes: `raw`
- Filters: `min_n=30.0, trim_low_q=0.0, trim_high_q=0.995`
- Generated (UTC): `2026-03-15T22:19:05.785609`
- Experiment slug: `matrix-y-denue_est_count-n-population-lvl-manzana-fit-ols-robust-poisson-negbin-auto-filt-raw-scope-cities-19001-14001-14002-19002-14003-plus187`
- Run dir: `/app/data/sami_experiments/mx/matrix-y-denue_est_count-n-population-lvl-manzana-fit-ols-robust-poisson-negbin-auto-filt-raw-scope-cities-19001-14001-14002-19002-14003-plus187/20260315_221905`

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
| population | manzana | raw | ols | 136132 | 0.335485 | 0.220838 | 0.050598 | 0.998516 | 385924.411 | 385944.054 |
| population | manzana | raw | robust | 136132 | 0.157858 | 0.250668 | 0.047545 | 0.999001 | 386361.452 | 386381.095 |
| population | manzana | raw | poisson | 136132 | 1.080824 | 0.185843 | 0.023448 | 0.999183 | 1728025.999 | 1728045.642 |
| population | manzana | raw | negbin | 136132 | 1.395432 | 0.115396 | 0.003307 | 1.004563 | 808917.994 | 808947.458 |
| population | manzana | raw | auto | 136132 | 0.335485 | 0.220838 | 0.050598 | 0.998516 | 385924.411 | 385944.054 |

## Notes
- `raw`: uses all units with `Y>0` and `N>0`.
- `filtered`: applies `N>=min_n` and optional N-quantile trimming.
- Compare runs by same level and same fit before interpreting beta/SAMI shifts.

## Recommended Filter Policy (Auto)

| N field | Level | Fit | Recommended | Reason | ΔR²(filtered-raw) | Keep ratio |
|---|---|---|---|---|---:|---:|
| population | manzana | auto | raw | filtered mode not present in this run | - | - |
| population | manzana | negbin | raw | filtered mode not present in this run | - | - |
| population | manzana | ols | raw | filtered mode not present in this run | - | - |
| population | manzana | poisson | raw | filtered mode not present in this run | - | - |
| population | manzana | robust | raw | filtered mode not present in this run | - | - |
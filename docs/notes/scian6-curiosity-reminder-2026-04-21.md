# SCIAN6 Curiosity Reminder

Date: `2026-04-21`

This note is not the main line of the paper. It is a reminder of why `SCIAN 6-digit` looked scientifically interesting but dangerous at the city level.

## Why Come Back To This

- `SCIAN 6-digit` is where the city-scaling framework starts to hit the limit of category granularity.
- Many 6-digit activities are too narrow, too rare, or too geographically restricted to behave like broad city-system quantities.
- A few 6-digit activities remain strong and interpretable, so the family is not useless; it is mixed.

## Main Numbers To Remember

- total `SCIAN6 Y`: `985`
- `A_strong`: `19`
- `B_usable`: `108`
- `C_exploratory`: `216`
- `D_unfit`: `642`

- median `R²`: `0.245`
- median coverage rate: `0.0628`
- median zero-rate: `0.9372`
- median `n_obs`: `155`
- median share of all establishments: `0.000077`

## Why Fitability Collapses

- the typical 6-digit activity is absent from most cities
- many categories have tiny effective support
- high `R²` can be meaningless when `n_obs` is extremely small
- specialization and presence/absence structure start to dominate over broad size scaling

## Examples To Remember

Strong examples:

- `461110`: `n=2468`, coverage `0.9996`, share `0.1087`, `beta=0.840`, `R²=0.826`
- `812110`: `n=2161`, coverage `0.8753`, share `0.0402`, `beta=1.121`, `R²=0.820`
- `465311`: `n=2277`, coverage `0.9222`, share `0.0224`, `beta=0.962`, `R²=0.801`

Misleading high-`R²` but unfit examples:

- `212292`: `n=2`, coverage `0.0008`, `R²=1.000`
- `523210`: `n=3`, coverage `0.0012`, `R²=0.975`
- `525110`: `n=3`, coverage `0.0012`, `R²=0.878`

## Questions To Revisit Later

1. Which `SCIAN6` categories stay fitable because they are effectively mass-market urban functions rather than niche activities?
2. Is there a systematic threshold in coverage or share below which city-scaling interpretation stops being meaningful?
3. Does `SCIAN6` become more interpretable if we restrict analysis to larger cities only?
4. Does grouping `SCIAN6` by broader economic families recover fit without collapsing all the way back to `SCIAN2`?
5. Can we use `SCIAN6` mainly as a specialization/residual layer instead of a primary scaling layer?

## Current Position

- keep `SCIAN6` in the database and audit outputs
- do not use it as a main theory family for the paper right now
- return later as a side analysis on the limit of granularity in urban scaling

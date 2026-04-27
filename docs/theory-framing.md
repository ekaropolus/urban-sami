# Theory Framing

This note defines the theory language that `urban-sami` should use in reports, figures, and papers.

Primary theoretical references:

- Luís M. A. Bettencourt, *Introduction to Urban Science: Evidence and Theory for Cities as Complex Systems* (MIT Press, 2021)
- Bettencourt course materials repo:
  https://github.com/lmbett/Introduction-to-Urban-Science-Course-Materials
- Bettencourt et al. 2007, PNAS
- Bettencourt, Lobo, Strumsky, West 2010, PLOS One
- Bettencourt 2013, *Science*

## 1. Core Theoretical Object

The theory-native object in urban scaling is the **city**.

Urban scaling theory is about cities as socioeconomic networks embedded in built space and time. In this framing, city size summarizes the scale of interactions, coordination, infrastructure, and spatial costs.

Therefore:

- `city` is the primary theory object
- `state` is **not** the theory-native object
- `AGEB` and `manzana` are **not** automatically assumed to satisfy the same theory

## 2. Core Model

For unit `u`, indicator `Y`, and scale basis `N`:

- `Y = Y0 * N^beta`
- `log(Y) = alpha + beta * log(N) + epsilon`

Where:

- `Y`: observed urban quantity or derived indicator
- `N`: scale variable, usually population
- `alpha = log(Y0)`: normalization constant / intercept
- `beta`: scaling exponent
- `epsilon`: log deviation from the fitted scaling law

Expected value under the fitted law:

- `Y_expected = exp(alpha + beta * log(N))`

Raw log deviation:

- `xi = log(Y / Y_expected) = epsilon`

This raw log deviation is the deviation object used in the scaling literature and is the canonical SAMI quantity in this repository.

## 3. Meaning of Beta

`beta` is a **scaling-regime parameter**, not a unit score and not a generic "importance" statistic.

It is interpreted relative to `1`:

- `beta > 1`: superlinear
- `beta ~= 1`: near-linear / proportional
- `beta < 1`: sublinear

For count-like quantities, the most useful interpretation is:

- `Y / N ~ N^(beta - 1)`

So:

- superlinear: per-capita prevalence rises with size
- near-linear: per-capita prevalence is roughly stable with size
- sublinear: per-capita prevalence falls with size

In urban scaling theory, the key issue is not merely whether a quantity "grows faster", but what kind of urban process it belongs to:

- infrastructure tends to be sublinear
- many socioeconomic outputs tend to be superlinear
- some quantities are near-linear

## 4. Meaning of Alpha

`alpha` sets the vertical position of the scaling relation.

It is useful for computing expected values but usually less central than `beta` for scientific interpretation.

Because `alpha` depends on units and indicator construction, it should not be compared casually across different indicators or incompatible runs.

## 5. Meaning of Residuals and SAMI

Residual:

- `epsilon = log(Y) - log(Y_expected)`

Interpretation:

- `epsilon > 0`: above expected value for size
- `epsilon < 0`: below expected value for size

### 5.1 Canonical SAMI quantity

In this repository, SAMI is the raw log deviation:

- `xi = log(Y / Y_expected)`

### 5.2 Current `urban-sami` operational score

The current `urban-sami` workflow uses the raw log deviation as the canonical SAMI field:

- `sami = xi = epsilon = log(Y / Y_expected)`

It also reports a secondary standardized diagnostic:

- `z_residual = epsilon / sd(epsilon)`

So:

- canonical `urban-sami` SAMI: raw log residual
- secondary diagnostic: standardized residual

To avoid ambiguity in papers and docs:

- use `SAMI` or `raw log deviation`
- use `z_residual` or `standardized residual` for the normalized diagnostic

## 6. Meaning of R2 and Uncertainty

- `R2`: how much variance in `log(Y)` is explained by `log(N)`
- high `R2`: tighter scaling relation
- low `R2`: weaker scaling relation; residual-based interpretation becomes more exploratory

Other required diagnostics:

- `beta_ci_95`: uncertainty for the exponent
- `residual_std`: width of the deviation distribution
- rank stability or resampling stability when rankings are used

If the confidence interval overlaps `1`, strong claims of nonlinearity should be avoided.

## 7. Scale and Geometry as Experiments

The repository should frame levels and supports like this:

### 7.1 City

`city` is the main theory test.

This is where urban scaling is most directly interpretable.

### 7.2 State

`state` is a **coarse aggregation experiment**.

Purpose:

- test what happens when cities are aggregated into larger administrative units
- understand whether apparent scaling strength is amplified by aggregation
- study how category composition behaves under supra-urban aggregation

State is therefore not the main urban-theory object. It is an experiment about aggregation and system-level smoothing.

### 7.3 AGEB and Manzana

`AGEB` and `manzana` are **intra-urban extension experiments**.

Purpose:

- test whether city-scale theory extends inside cities
- identify where the theory weakens
- study how local geometry, land use, accessibility, specialization, sparsity, and discrete counts alter scaling behavior

These levels are not expected to automatically reproduce city exponents.

### 7.4 Alternative Geometries

Hex grids, Voronoi cells, geofences, buffers, and isochrones are **geometry-support experiments**.

Purpose:

- test whether observed scaling and deviations are robust to support definition
- separate substantive urban structure from administrative-boundary artifacts
- compare global and local geometries explicitly

This is central to the research program: not just whether scaling exists, but how it changes across global and local geometries.

## 8. Comparison Rules

Only compare deviation scores directly when all relevant context matches:

- same indicator `Y`
- same scale basis `N`
- same level or support definition
- same model form
- same data vintage / source context

Do not treat scores from different levels as if they were automatically commensurate.

## 9. Reporting Rules For This Repository

When writing reports:

1. state whether the run is:
   - a city theory run
   - a state aggregation experiment
   - an intra-urban extension experiment
   - a geometry-support experiment
2. report `beta` as a scaling regime relative to `1`
3. interpret count-like `Y` through `beta - 1` when useful
4. distinguish raw log deviation from standardized residual
5. avoid causal language unless supported by a separate design

## 10. Paper Framing For Current Project

The current project should be framed as:

- **city**: main urban scaling analysis
- **state**: aggregation experiment to understand coarse-scale emergence
- **AGEB/manzana**: local experiments probing whether and how theory extends inside cities
- **alternative geometries**: experiments on support dependence across global and local geometries

This is the scientific point of the project:

not simply whether scaling exists, but how the theory behaves, weakens, or changes as we move across aggregation levels and geometry definitions.

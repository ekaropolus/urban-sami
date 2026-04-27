# Figure Style Notes for Nature-Type Presentation

Date: `2026-04-22`

These notes set the figure standard for `urban-sami` city results.

Primary source guidance:
- Nature Research figure guide:
  https://research-figure-guide.nature.com/figures/preparing-figures-our-specifications/
- Nature panel and sizing guide:
  https://research-figure-guide.nature.com/figures/building-and-exporting-figure-panels/
- Nature formatting guide:
  https://www.nature.com/nature/for-authors/formatting-guide

## Non-negotiable rules

- Figures must be simple enough to read quickly outside the subfield.
- Use standard fonts only: Helvetica or Arial.
- Axis text and labels should be legible at final size, roughly `5–7 pt`.
- Avoid decorative colour, rainbow palettes, coloured text, and chart junk.
- Use vector output for line art and text.
- Multi-panel figures should be logically connected and alphabetically ordered.
- Panel layouts should minimize wasted white space.

## What we should stop doing

- giant category dumps as heatmaps
- mixing several statistics with incompatible meaning in one axis or one glyph
- overlong figure subtitles that try to replace the legend
- dense label clutter with arbitrary ordering
- “dashboard” graphics that are complete but not interpretable

## What we should do instead

- one figure = one scientific question
- use composite figures only when the panels are logically linked
- separate:
  - fit quality
  - scaling regime
  - residual spread
  - city-level deviations
- keep labels short and move detail into the legend or caption
- prefer direct labels only for a few highlighted items, not all points

## Default visual language for this paper

- background: white
- axes and frame: dark grey
- grid: very light grey, minimal
- text: near-black
- palette:
  - aggregate and baseline: deep blue
  - size families: blue / slate
  - sector families: rust / brick
  - residual spread or deviation emphasis: dark green or charcoal accent
- no coloured text
- no gradients
- no shadows

## Preferred figure types for the city section

1. Aggregate scaling scatter
- log population vs log establishments
- fitted line
- only a handful of annotated outliers

2. Family-level dot plots
- one metric per panel
- for example:
  - median `R²` by family
  - median `beta` by family

3. Sector ranking plots
- ordered `SCIAN2` labels
- one panel for `R²`
- one panel for `beta`

4. Residual spread plot
- retained `Y` ordered by `SAMI` 90% range
- optional companion scatter against `R²`

5. Recurrent deviation profiles
- upper-tail and lower-tail recurrence shown as ranked horizontal bars
- interpreted explicitly as deviation frequency across retained `Y`, not as absolute quality

## Recommended city figure sequence

- `Fig. 1`
  Aggregate and family overview.
- `Fig. 2`
  Retained `SCIAN2` heterogeneity.
- `Fig. 3`
  Recurrent upper-tail and lower-tail city deviations across retained `Y`.

## Practical sizing target

For main text figures:
- prefer double-column width
- max height below full-page limit
- panels should remain readable if reduced

In `urban-sami`, this means the default target should be a compact double-column composite rather than oversized single-purpose diagnostic canvases.

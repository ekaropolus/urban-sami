# urban-sami

Headless research-first SAMI workflow engine.

Current scope in this repository revision:

- workflow manifest loading and validation
- pure fitting kernels for `ols`, `robust`, `poisson`, `negbin`, `auto`
- summary-matrix replay/bundle generation from frozen Polisplexity outputs
- parity workflows that recompute Polisplexity matrix summaries from exported unit tables
- CLI entry point
- golden tests against preserved Polisplexity artifacts

This is the first executable spin-out stage. The eventual goal is to support full geometry, indicator, aggregation, and workflow execution outside Django.

## Theory Framing

Current theory framing for this repo is documented at [docs/theory-framing.md](docs/theory-framing.md).

The short version is:

- `city` is the theory-native urban scaling object
- `state` is an aggregation experiment over cities
- `AGEB` and `manzana` are intra-urban extension experiments
- alternative supports such as hex or geofences are geometry-support experiments

This keeps the repo aligned with urban scaling theory while still using coarse and fine supports as deliberate experiments.

## GitHub Review Map

For community review, the project now includes an explicit monograph-to-code crosswalk:

- [PROJECT_MAP.md](/home/hadox/cmd-center/platforms/research/urban-sami/PROJECT_MAP.md)
- [docs/monograph-script-crosswalk.csv](/home/hadox/cmd-center/platforms/research/urban-sami/docs/monograph-script-crosswalk.csv)

These files connect:

- monograph phases
- executable scripts
- report packs
- persistent database tables

The final integrated monograph is here:

- [main.pdf](manuscript/final-multiscale-monograph-2026-04-25/main.pdf)
- [main.tex](manuscript/final-multiscale-monograph-2026-04-25/main.tex)

Suggested review workflow:

1. open a phase in the monograph
2. locate it in `PROJECT_MAP.md`
3. open the listed script
4. inspect the listed `reports/` pack
5. verify the listed `derived.*` / `experiments.*` tables if the phase is DB-persistent

The default GitHub snapshot is intentionally light:

- tracked: source code, scripts, SQL, monographs, docs, tests
- not tracked by default: `data/`, `reports/`, `logs/`, `dist/`

Those large artifact folders are generated locally from the persistent experiment database and the scripts mapped in `PROJECT_MAP.md`. The exact rerun flow is documented in [docs/REPRODUCIBILITY.md](docs/REPRODUCIBILITY.md).

## Parity Target

The immediate mission is not feature breadth. It is parity.

The test contract is:

1. Run a Polisplexity matrix experiment for a chosen configuration.
2. Feed the exported unit tables into `urban-sami`.
3. Require `urban-sami` to regenerate the same summary outputs within a fixed numeric tolerance.

Parity is currently proven in this repo for preserved `state` and `AGEB` matrix exports, and was also verified against the original live `manzana` export directory.

The practical contract is now:

1. Run a Polisplexity matrix experiment with `--export-units`.
2. Point `urban-sami` at that completed run directory.
3. Require `mismatch_count = 0`.

Examples:

```bash
urban-sami run examples/polisplexity-state-run-dir-parity.yaml
urban-sami run examples/polisplexity-ageb-run-dir-parity-live.yaml
```

## Independent DB

When a local Postgres container is available, `urban-sami` can use a separate experiment database instead of Polisplexity's app database.

Bootstrap script:

```bash
bash scripts/bootstrap_experiment_db.sh
```

This creates `urban_sami_exp` with schemas intended for independent ingestion:

- `raw`
- `staging`
- `derived`
- `experiments`

City-scale street-network persistence can also be materialized into `derived` once the OSM metrics report exists:

```bash
PYTHONPATH=src python3 scripts/load_city_network_metrics.py \
  --metrics-csv reports/city-connectivity-power-laws-2026-04-23/city_connectivity_rows.csv
```

This stores:

- municipal geometry used for the network extraction in `derived.city_network_geoms`
- derived OSM street-network metrics in `derived.city_network_metrics`

To persist the full `drive` network for cities into PostGIS, including nodes and edges:

```bash
PYTHONPATH=src python3 scripts/extract_city_osm_networks_to_db.py \
  --source-method osm_drive_municipal_full_v1 \
  --city-code 14039
```

To run the same extractor nationally, resumably, over all cities in `dist/independent_city_baseline/city_counts.csv`:

```bash
PYTHONPATH=src python3 scripts/extract_city_osm_networks_to_db.py \
  --source-method osm_drive_municipal_full_v1
```

To monitor progress while the national run is active:

```bash
bash scripts/monitor_city_osm_network_extract.sh osm_drive_municipal_full_v1
```

This persists:

- city support polygons in `derived.city_network_geoms`
- derived city-scale OSM metrics in `derived.city_network_metrics`
- city OSM nodes in `derived.city_network_nodes`
- city OSM edges in `derived.city_network_edges`
- extraction progress and failures in `experiments.city_network_extract_status`

## Independent State Baseline

The repo can now run a fully independent state-level baseline using:

- official INEGI CPV 2020 state population
- local DENUE bulk CSV files
- the separate `urban_sami_exp` database

Commands:

```bash
bash scripts/bootstrap_experiment_db.sh
PYTHONPATH=src python3 scripts/fetch_inegi_state_population.py --output data/raw/inegi_cpv2020_state_population.csv
PYTHONPATH=src python3 scripts/load_population_units.py data/raw/inegi_cpv2020_state_population.csv --truncate --level state
PYTHONPATH=src python3 scripts/load_denue_bulk.py --truncate
PYTHONPATH=src python3 scripts/run_independent_state_baseline.py --output-dir dist/independent_state_baseline
```

The resulting report is written under `reports/independent-state-baseline-report-*` when reports are generated locally.

## Independent City Baseline

The repo can also run a fully independent city-level baseline using the official INEGI municipality catalog service.

Commands:

```bash
PYTHONPATH=src python3 scripts/fetch_inegi_city_population.py --output data/raw/inegi_cpv2020_city_population.csv
PYTHONPATH=src python3 scripts/load_population_units.py data/raw/inegi_cpv2020_city_population.csv --level city
PYTHONPATH=src python3 scripts/run_independent_city_baseline.py --output-dir dist/independent_city_baseline
```

The resulting report is written under `reports/independent-city-baseline-report-*` when reports are generated locally.

## Quick Start

```bash
python -m pip install -e .
urban-sami validate examples/replay-state-workflow.yaml
urban-sami run examples/replay-state-workflow.yaml
urban-sami run examples/polisplexity-state-matrix-parity.yaml
```

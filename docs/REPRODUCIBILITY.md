# Reproducibility

This repository is meant to be reviewable on GitHub without forcing every clone to carry multi-gigabyte generated artifacts.

## What is tracked

- `src/`
- `scripts/`
- `sql/`
- `tests/`
- `docs/`
- `manuscript/`
- root research documentation such as `README.md` and `PROJECT_MAP.md`

## What is not tracked by default

- `data/`
- `reports/`
- `logs/`
- `dist/`

These folders are large, regenerated frequently, and in the later phases are largely projections of the persistent experiment database rather than hand-authored inputs.

## Review workflow

1. Start from the final monograph:
   - `manuscript/final-multiscale-monograph-2026-04-25/main.tex`
   - `manuscript/final-multiscale-monograph-2026-04-25/main.pdf`
2. Open `PROJECT_MAP.md`.
3. Find the matching phase.
4. For equation-level review, open `docs/EQUATION_TRACEABILITY.md`.
5. Inspect the listed script or scripts.
6. If the generated report pack is not present locally, rerun it from the listed script.
7. If the phase is DB-persistent, verify the listed `raw.*`, `derived.*`, or `experiments.*` tables.

## Database bootstrap

```bash
bash scripts/bootstrap_experiment_db.sh
```

This creates the independent experiment database and the schemas:

- `raw`
- `staging`
- `derived`
- `experiments`

## National support loading

### City population

```bash
PYTHONPATH=src python3 scripts/fetch_inegi_city_population.py \
  --output data/raw/inegi_cpv2020_city_population.csv

PYTHONPATH=src python3 scripts/load_population_units.py \
  data/raw/inegi_cpv2020_city_population.csv \
  --level city
```

### AGEB support

```bash
PYTHONPATH=src python3 scripts/load_ageb_national.py
```

### Manzana support

```bash
PYTHONPATH=src python3 scripts/load_manzana_national.py
```

## National street-network persistence

```bash
PYTHONPATH=src python3 scripts/extract_city_osm_networks_to_db.py \
  --source-method osm_drive_municipal_full_v1
```

Monitor:

```bash
bash scripts/monitor_city_osm_network_extract.sh osm_drive_municipal_full_v1
```

## National AGEB derived tables

```bash
PYTHONPATH=src python3 scripts/materialize_ageb_derived_tables.py
```

## Phase reruns

Use `PROJECT_MAP.md` as the authoritative crosswalk. Most phase scripts run in the form:

```bash
PYTHONPATH=src python3 scripts/<script_name>.py
```

Examples:

```bash
PYTHONPATH=src python3 scripts/run_spatial_information_phase1.py
PYTHONPATH=src python3 scripts/run_spatial_information_multiscale_decomposition.py
PYTHONPATH=src python3 scripts/run_city_coarse_graining_laws.py
PYTHONPATH=src python3 scripts/run_city_multiscale_synthesis.py
```

## Monograph rebuild

```bash
cd manuscript/final-multiscale-monograph-2026-04-25
bash build.sh
```

## Important review note

The crosswalk intentionally points to report packs even though `reports/` is not tracked by default. This is because the reports are treated as reproducible outputs, not as the canonical source of truth. The canonical source of truth is:

1. the scripts,
2. the persistent database tables they write,
3. the monograph that interprets them.

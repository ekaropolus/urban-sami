#!/usr/bin/env bash
set -euo pipefail

DOCKER_EXE=${DOCKER_EXE:-"/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"}
DB_CONTAINER=${DB_CONTAINER:-"24-polisplexity-core-db-dev"}
POSTGRES_USER=${POSTGRES_USER:-"postgres"}
DB_NAME=${DB_NAME:-"urban_sami_exp"}
SOURCE_METHOD=${1:-"inegi_wscatgeo_manzana_full_v1"}

"${DOCKER_EXE}" exec -i "${DB_CONTAINER}" psql -U "${POSTGRES_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 <<SQL
\pset footer off
\pset tuples_only on
\pset format aligned
SELECT
  (SELECT count(*) FROM raw.population_units WHERE level = 'city') AS total_cities,
  (SELECT count(*) FROM experiments.manzana_load_status WHERE source_method = '${SOURCE_METHOD}' AND status = 'success') AS success,
  (SELECT count(*) FROM experiments.manzana_load_status WHERE source_method = '${SOURCE_METHOD}' AND status = 'error') AS error,
  (SELECT count(*) FROM experiments.manzana_load_status WHERE source_method = '${SOURCE_METHOD}' AND status = 'running') AS running,
  (SELECT count(*) FROM raw.population_units WHERE level = 'manzana') AS manzana_rows,
  (SELECT count(DISTINCT city_code) FROM raw.population_units WHERE level = 'manzana') AS manzana_cities;

SELECT city_code, city_name, status, features_seen, loaded_rows, left(error_message, 120)
FROM experiments.manzana_load_status
WHERE source_method = '${SOURCE_METHOD}'
ORDER BY
  CASE status WHEN 'running' THEN 0 WHEN 'error' THEN 1 ELSE 2 END,
  city_code
LIMIT 20;
SQL

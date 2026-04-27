#!/usr/bin/env bash
set -euo pipefail

DOCKER_EXE=${DOCKER_EXE:-"/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"}
DB_CONTAINER=${DB_CONTAINER:-"24-polisplexity-core-db-dev"}
POSTGRES_USER=${POSTGRES_USER:-"postgres"}
DB_NAME=${DB_NAME:-"urban_sami_exp"}
SOURCE_METHOD=${1:-"osm_drive_municipal_full_v1"}

"${DOCKER_EXE}" exec -i "${DB_CONTAINER}" psql -U "${POSTGRES_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 <<SQL
\pset pager off
\echo 'status counts'
SELECT status, count(*) AS n
FROM experiments.city_network_extract_status
WHERE source_method = '${SOURCE_METHOD}'
GROUP BY status
ORDER BY status;

\echo ''
\echo 'stored rows'
SELECT
  (SELECT count(*) FROM derived.city_network_geoms WHERE source_method = '${SOURCE_METHOD}') AS geoms,
  (SELECT count(*) FROM derived.city_network_metrics WHERE source_method = '${SOURCE_METHOD}') AS metrics,
  (SELECT count(*) FROM derived.city_network_nodes WHERE source_method = '${SOURCE_METHOD}') AS nodes,
  (SELECT count(*) FROM derived.city_network_edges WHERE source_method = '${SOURCE_METHOD}') AS edges;

\echo ''
\echo 'latest cities'
SELECT city_code, city_name, status, n_nodes, n_edges, started_at, finished_at
FROM experiments.city_network_extract_status
WHERE source_method = '${SOURCE_METHOD}'
ORDER BY COALESCE(finished_at, started_at) DESC NULLS LAST
LIMIT 10;
SQL

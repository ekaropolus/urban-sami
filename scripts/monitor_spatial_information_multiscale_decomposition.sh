#!/usr/bin/env bash
set -euo pipefail

DOCKER_EXE=${DOCKER_EXE:-"/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"}
DB_CONTAINER=${DB_CONTAINER:-"24-polisplexity-core-db-dev"}
POSTGRES_USER=${POSTGRES_USER:-"postgres"}
DB_NAME=${DB_NAME:-"urban_sami_exp"}
SOURCE_METHOD=${1:-"bettencourt_spatial_information_multiscale_v2"}
LOG_FILE=${2:-"/home/hadox/cmd-center/platforms/research/urban-sami/logs/spatial_information_multiscale_decomposition_v2.log"}

echo "== status counts =="
"${DOCKER_EXE}" exec -i "${DB_CONTAINER}" psql -U "${POSTGRES_USER}" -d "${DB_NAME}" -AtF $'\t' <<SQL
SELECT status, COUNT(*)
FROM experiments.spatial_information_decomposition_status
WHERE source_method = '${SOURCE_METHOD}'
GROUP BY status
ORDER BY status;
SQL

echo
echo "== running =="
"${DOCKER_EXE}" exec -i "${DB_CONTAINER}" psql -U "${POSTGRES_USER}" -d "${DB_NAME}" -AtF $'\t' <<SQL
SELECT city_code, city_name, family, status, COALESCE(notes, '')
FROM experiments.spatial_information_decomposition_status
WHERE source_method = '${SOURCE_METHOD}'
  AND status = 'running'
ORDER BY started_at DESC
LIMIT 10;
SQL

echo
echo "== result rows =="
"${DOCKER_EXE}" exec -i "${DB_CONTAINER}" psql -U "${POSTGRES_USER}" -d "${DB_NAME}" -AtF $'\t' <<SQL
SELECT 'ageb_within', COUNT(*)
FROM derived.ageb_spatial_information_within
WHERE source_method = '${SOURCE_METHOD}'
UNION ALL
SELECT 'city_decomposition', COUNT(*)
FROM derived.city_spatial_information_decomposition
WHERE source_method = '${SOURCE_METHOD}';
SQL

if [[ -f "${LOG_FILE}" ]]; then
  echo
  echo "== log tail =="
  tail -n 20 "${LOG_FILE}"
fi

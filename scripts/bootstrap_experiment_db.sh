#!/usr/bin/env bash
set -euo pipefail

DOCKER_EXE=${DOCKER_EXE:-"/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"}
DB_CONTAINER=${DB_CONTAINER:-"24-polisplexity-core-db-dev"}
POSTGRES_USER=${POSTGRES_USER:-"postgres"}

SQL_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/sql/bootstrap_experiment_db.sql"

"${DOCKER_EXE}" exec -i "${DB_CONTAINER}" psql -U "${POSTGRES_USER}" -d postgres -Atqc "SELECT 1 FROM pg_database WHERE datname = 'urban_sami_exp';" | grep -q 1 || \
  "${DOCKER_EXE}" exec -i "${DB_CONTAINER}" psql -U "${POSTGRES_USER}" -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE urban_sami_exp;"

"${DOCKER_EXE}" exec -i "${DB_CONTAINER}" psql -U "${POSTGRES_USER}" -d urban_sami_exp -v ON_ERROR_STOP=1 <<SQL
\set ON_ERROR_STOP on
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS derived;
CREATE SCHEMA IF NOT EXISTS experiments;

CREATE TABLE IF NOT EXISTS raw.denue_establishments (
    obs_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    denue_id TEXT NOT NULL DEFAULT '',
    country_code TEXT NOT NULL DEFAULT 'MX',
    state_code TEXT NOT NULL DEFAULT '',
    state_name TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    ageb_code TEXT NOT NULL DEFAULT '',
    manzana_code TEXT NOT NULL DEFAULT '',
    scian_code TEXT NOT NULL DEFAULT '',
    per_ocu TEXT NOT NULL DEFAULT '',
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    geom geometry(Point, 4326)
);
ALTER TABLE raw.denue_establishments ADD COLUMN IF NOT EXISTS denue_id TEXT NOT NULL DEFAULT '';
ALTER TABLE raw.denue_establishments ADD COLUMN IF NOT EXISTS state_code TEXT NOT NULL DEFAULT '';
ALTER TABLE raw.denue_establishments ADD COLUMN IF NOT EXISTS state_name TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS raw_denue_establishments_country_idx ON raw.denue_establishments (country_code);
CREATE INDEX IF NOT EXISTS raw_denue_establishments_state_idx ON raw.denue_establishments (state_code);
CREATE INDEX IF NOT EXISTS raw_denue_establishments_city_idx ON raw.denue_establishments (city_code);
CREATE INDEX IF NOT EXISTS raw_denue_establishments_ageb_idx ON raw.denue_establishments (ageb_code);
CREATE INDEX IF NOT EXISTS raw_denue_establishments_manzana_idx ON raw.denue_establishments (manzana_code);
CREATE INDEX IF NOT EXISTS raw_denue_establishments_geom_gix ON raw.denue_establishments USING GIST (geom);

CREATE TABLE IF NOT EXISTS raw.population_units (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    country_code TEXT NOT NULL DEFAULT 'MX',
    level TEXT NOT NULL DEFAULT '',
    unit_code TEXT NOT NULL DEFAULT '',
    unit_label TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    ageb_code TEXT NOT NULL DEFAULT '',
    manzana_code TEXT NOT NULL DEFAULT '',
    population DOUBLE PRECISION,
    households DOUBLE PRECISION,
    population_female DOUBLE PRECISION,
    population_male DOUBLE PRECISION,
    area_km2 DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS raw_population_units_level_idx ON raw.population_units (level);
CREATE INDEX IF NOT EXISTS raw_population_units_unit_idx ON raw.population_units (unit_code);
CREATE INDEX IF NOT EXISTS raw_population_units_city_idx ON raw.population_units (city_code);

CREATE TABLE IF NOT EXISTS raw.admin_units (
    unit_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    country_code TEXT NOT NULL DEFAULT 'MX',
    level TEXT NOT NULL DEFAULT '',
    unit_code TEXT NOT NULL DEFAULT '',
    unit_label TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    parent_code TEXT NOT NULL DEFAULT '',
    population DOUBLE PRECISION,
    households DOUBLE PRECISION,
    population_female DOUBLE PRECISION,
    population_male DOUBLE PRECISION,
    area_km2 DOUBLE PRECISION,
    geom geometry(MultiPolygon, 4326)
);
CREATE INDEX IF NOT EXISTS raw_admin_units_level_idx ON raw.admin_units (level);
CREATE INDEX IF NOT EXISTS raw_admin_units_code_idx ON raw.admin_units (unit_code);
CREATE INDEX IF NOT EXISTS raw_admin_units_city_idx ON raw.admin_units (city_code);
CREATE INDEX IF NOT EXISTS raw_admin_units_geom_gix ON raw.admin_units USING GIST (geom);

CREATE TABLE IF NOT EXISTS derived.city_network_geoms (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    area_km2 DOUBLE PRECISION,
    perimeter_km DOUBLE PRECISION,
    geom geometry(MultiPolygon, 4326)
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_city_network_geoms_method_city_uidx
    ON derived.city_network_geoms (source_method, city_code);
CREATE INDEX IF NOT EXISTS derived_city_network_geoms_city_idx
    ON derived.city_network_geoms (city_code);
CREATE INDEX IF NOT EXISTS derived_city_network_geoms_geom_gix
    ON derived.city_network_geoms USING GIST (geom);

CREATE TABLE IF NOT EXISTS derived.city_network_metrics (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    population DOUBLE PRECISION,
    occupied_dwellings DOUBLE PRECISION,
    est_total DOUBLE PRECISION,
    city_area_km2 DOUBLE PRECISION,
    city_perimeter_km DOUBLE PRECISION,
    rho_pop DOUBLE PRECISION,
    rho_dwellings DOUBLE PRECISION,
    n_nodes DOUBLE PRECISION,
    n_edges DOUBLE PRECISION,
    intersection_count DOUBLE PRECISION,
    streets_per_node_avg DOUBLE PRECISION,
    street_length_total_km DOUBLE PRECISION,
    street_density_km_per_km2 DOUBLE PRECISION,
    intersection_density_km2 DOUBLE PRECISION,
    edge_length_avg_m DOUBLE PRECISION,
    circuity_avg DOUBLE PRECISION,
    mean_degree DOUBLE PRECISION,
    sum_degree DOUBLE PRECISION,
    boundary_entry_edges DOUBLE PRECISION,
    boundary_entry_edges_per_km DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_city_network_metrics_method_city_uidx
    ON derived.city_network_metrics (source_method, city_code);
CREATE INDEX IF NOT EXISTS derived_city_network_metrics_city_idx
    ON derived.city_network_metrics (city_code);

CREATE TABLE IF NOT EXISTS derived.city_network_nodes (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    node_osmid TEXT NOT NULL DEFAULT '',
    x DOUBLE PRECISION,
    y DOUBLE PRECISION,
    street_count DOUBLE PRECISION,
    degree DOUBLE PRECISION,
    highway TEXT NOT NULL DEFAULT '',
    ref TEXT NOT NULL DEFAULT '',
    geom geometry(Point, 4326)
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_city_network_nodes_method_city_node_uidx
    ON derived.city_network_nodes (source_method, city_code, node_osmid);
CREATE INDEX IF NOT EXISTS derived_city_network_nodes_city_idx
    ON derived.city_network_nodes (city_code);
CREATE INDEX IF NOT EXISTS derived_city_network_nodes_geom_gix
    ON derived.city_network_nodes USING GIST (geom);

CREATE TABLE IF NOT EXISTS derived.city_network_edges (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    u_osmid TEXT NOT NULL DEFAULT '',
    v_osmid TEXT NOT NULL DEFAULT '',
    edge_key INTEGER NOT NULL DEFAULT 0,
    osmid TEXT NOT NULL DEFAULT '',
    highway TEXT NOT NULL DEFAULT '',
    junction TEXT NOT NULL DEFAULT '',
    lanes TEXT NOT NULL DEFAULT '',
    oneway BOOLEAN,
    reversed BOOLEAN,
    length_m DOUBLE PRECISION,
    name TEXT NOT NULL DEFAULT '',
    maxspeed TEXT NOT NULL DEFAULT '',
    tunnel TEXT NOT NULL DEFAULT '',
    ref TEXT NOT NULL DEFAULT '',
    bridge TEXT NOT NULL DEFAULT '',
    access TEXT NOT NULL DEFAULT '',
    width TEXT NOT NULL DEFAULT '',
    geom geometry(Geometry, 4326)
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_city_network_edges_method_city_edge_uidx
    ON derived.city_network_edges (source_method, city_code, u_osmid, v_osmid, edge_key);
CREATE INDEX IF NOT EXISTS derived_city_network_edges_city_idx
    ON derived.city_network_edges (city_code);
CREATE INDEX IF NOT EXISTS derived_city_network_edges_geom_gix
    ON derived.city_network_edges USING GIST (geom);

CREATE TABLE IF NOT EXISTS derived.ageb_economic_mix (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    ageb_code TEXT NOT NULL DEFAULT '',
    ageb_label TEXT NOT NULL DEFAULT '',
    family TEXT NOT NULL DEFAULT '',
    category TEXT NOT NULL DEFAULT '',
    category_label TEXT NOT NULL DEFAULT '',
    est_count DOUBLE PRECISION,
    est_share_within_ageb DOUBLE PRECISION,
    est_share_within_city_family DOUBLE PRECISION,
    total_est_ageb DOUBLE PRECISION,
    total_est_city_family DOUBLE PRECISION,
    population DOUBLE PRECISION,
    occupied_dwellings DOUBLE PRECISION,
    area_km2 DOUBLE PRECISION,
    rho_pop DOUBLE PRECISION,
    rho_dwellings DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_ageb_economic_mix_uidx
    ON derived.ageb_economic_mix (source_method, city_code, ageb_code, family, category);
CREATE INDEX IF NOT EXISTS derived_ageb_economic_mix_city_idx
    ON derived.ageb_economic_mix (city_code);
CREATE INDEX IF NOT EXISTS derived_ageb_economic_mix_ageb_idx
    ON derived.ageb_economic_mix (ageb_code);
CREATE INDEX IF NOT EXISTS derived_ageb_economic_mix_family_idx
    ON derived.ageb_economic_mix (family);

CREATE TABLE IF NOT EXISTS derived.ageb_network_metrics (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    ageb_code TEXT NOT NULL DEFAULT '',
    ageb_label TEXT NOT NULL DEFAULT '',
    population DOUBLE PRECISION,
    occupied_dwellings DOUBLE PRECISION,
    est_total DOUBLE PRECISION,
    ageb_area_km2 DOUBLE PRECISION,
    ageb_perimeter_km DOUBLE PRECISION,
    rho_pop DOUBLE PRECISION,
    rho_dwellings DOUBLE PRECISION,
    n_nodes DOUBLE PRECISION,
    n_edges DOUBLE PRECISION,
    intersection_count DOUBLE PRECISION,
    streets_per_node_avg DOUBLE PRECISION,
    street_length_total_km DOUBLE PRECISION,
    street_density_km_per_km2 DOUBLE PRECISION,
    intersection_density_km2 DOUBLE PRECISION,
    edge_length_avg_m DOUBLE PRECISION,
    circuity_avg DOUBLE PRECISION,
    mean_degree DOUBLE PRECISION,
    sum_degree DOUBLE PRECISION,
    boundary_entry_edges DOUBLE PRECISION,
    boundary_entry_edges_per_km DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_ageb_network_metrics_uidx
    ON derived.ageb_network_metrics (source_method, city_code, ageb_code);
CREATE INDEX IF NOT EXISTS derived_ageb_network_metrics_city_idx
    ON derived.ageb_network_metrics (city_code);
CREATE INDEX IF NOT EXISTS derived_ageb_network_metrics_ageb_idx
    ON derived.ageb_network_metrics (ageb_code);

CREATE TABLE IF NOT EXISTS experiments.city_network_extract_status (
    row_id BIGSERIAL PRIMARY KEY,
    source_method TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    n_nodes INTEGER,
    n_edges INTEGER,
    error_message TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS experiments_city_network_extract_status_method_city_uidx
    ON experiments.city_network_extract_status (source_method, city_code);
CREATE INDEX IF NOT EXISTS experiments_city_network_extract_status_status_idx
    ON experiments.city_network_extract_status (status);

CREATE TABLE IF NOT EXISTS experiments.manzana_load_status (
    row_id BIGSERIAL PRIMARY KEY,
    source_method TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    features_seen INTEGER,
    loaded_rows INTEGER,
    error_message TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS experiments_manzana_load_status_method_city_uidx
    ON experiments.manzana_load_status (source_method, city_code);
CREATE INDEX IF NOT EXISTS experiments_manzana_load_status_status_idx
    ON experiments.manzana_load_status (status);

CREATE TABLE IF NOT EXISTS experiments.ageb_load_status (
    row_id BIGSERIAL PRIMARY KEY,
    source_method TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    features_seen INTEGER,
    loaded_rows INTEGER,
    error_message TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS experiments_ageb_load_status_method_city_uidx
    ON experiments.ageb_load_status (source_method, city_code);
CREATE INDEX IF NOT EXISTS experiments_ageb_load_status_status_idx
    ON experiments.ageb_load_status (status);

CREATE TABLE IF NOT EXISTS experiments.spatial_information_decomposition_status (
    row_id BIGSERIAL PRIMARY KEY,
    source_method TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    family TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    ageb_rows INTEGER,
    decomposition_rows INTEGER,
    error_message TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS experiments_spatial_information_decomposition_status_uidx
    ON experiments.spatial_information_decomposition_status (source_method, family, city_code);
CREATE INDEX IF NOT EXISTS experiments_spatial_information_decomposition_status_status_idx
    ON experiments.spatial_information_decomposition_status (status);

CREATE TABLE IF NOT EXISTS derived.city_spatial_information_summary (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    neighborhood_level TEXT NOT NULL DEFAULT '',
    family TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    total_count DOUBLE PRECISION,
    n_units_total DOUBLE PRECISION,
    n_units_nonzero DOUBLE PRECISION,
    n_categories DOUBLE PRECISION,
    mi_nats DOUBLE PRECISION,
    mi_bits DOUBLE PRECISION,
    h_units_nats DOUBLE PRECISION,
    h_categories_nats DOUBLE PRECISION,
    nmi_min DOUBLE PRECISION,
    effective_units DOUBLE PRECISION,
    effective_categories DOUBLE PRECISION,
    unit_coverage DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_city_spatial_information_summary_uidx
    ON derived.city_spatial_information_summary (source_method, neighborhood_level, family, city_code);
CREATE INDEX IF NOT EXISTS derived_city_spatial_information_summary_city_idx
    ON derived.city_spatial_information_summary (city_code);
CREATE INDEX IF NOT EXISTS derived_city_spatial_information_summary_level_family_idx
    ON derived.city_spatial_information_summary (neighborhood_level, family);

CREATE TABLE IF NOT EXISTS derived.city_spatial_information_overlap (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    family TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    mi_nats_manzana DOUBLE PRECISION,
    mi_nats_ageb DOUBLE PRECISION,
    nmi_min_manzana DOUBLE PRECISION,
    nmi_min_ageb DOUBLE PRECISION,
    unit_coverage_manzana DOUBLE PRECISION,
    unit_coverage_ageb DOUBLE PRECISION,
    delta_mi_nats_ageb_minus_manzana DOUBLE PRECISION,
    delta_nmi_min_ageb_minus_manzana DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_city_spatial_information_overlap_uidx
    ON derived.city_spatial_information_overlap (source_method, family, city_code);
CREATE INDEX IF NOT EXISTS derived_city_spatial_information_overlap_city_idx
    ON derived.city_spatial_information_overlap (city_code);

CREATE TABLE IF NOT EXISTS derived.city_spatial_information_pair_lift (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    neighborhood_level TEXT NOT NULL DEFAULT '',
    family TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    unit_code TEXT NOT NULL DEFAULT '',
    unit_label TEXT NOT NULL DEFAULT '',
    category TEXT NOT NULL DEFAULT '',
    est_count DOUBLE PRECISION,
    total_count DOUBLE PRECISION,
    unit_total DOUBLE PRECISION,
    category_total DOUBLE PRECISION,
    p_joint DOUBLE PRECISION,
    p_unit DOUBLE PRECISION,
    p_category DOUBLE PRECISION,
    share_within_unit DOUBLE PRECISION,
    share_within_category DOUBLE PRECISION,
    lift DOUBLE PRECISION,
    log_lift_nats DOUBLE PRECISION,
    mi_term_nats DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS derived_city_spatial_information_pair_lift_city_idx
    ON derived.city_spatial_information_pair_lift (source_method, neighborhood_level, family, city_code);
CREATE INDEX IF NOT EXISTS derived_city_spatial_information_pair_lift_unit_idx
    ON derived.city_spatial_information_pair_lift (city_code, unit_code);
CREATE INDEX IF NOT EXISTS derived_city_spatial_information_pair_lift_category_idx
    ON derived.city_spatial_information_pair_lift (city_code, category);

CREATE TABLE IF NOT EXISTS derived.city_spatial_information_unit_scores (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    neighborhood_level TEXT NOT NULL DEFAULT '',
    family TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    unit_code TEXT NOT NULL DEFAULT '',
    unit_label TEXT NOT NULL DEFAULT '',
    unit_total DOUBLE PRECISION,
    p_unit DOUBLE PRECISION,
    local_entropy_nats DOUBLE PRECISION,
    effective_categories_in_unit DOUBLE PRECISION,
    kl_to_city_nats DOUBLE PRECISION,
    mi_contribution_nats DOUBLE PRECISION,
    dominant_category TEXT NOT NULL DEFAULT '',
    dominant_category_share DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_city_spatial_information_unit_scores_uidx
    ON derived.city_spatial_information_unit_scores (source_method, neighborhood_level, family, city_code, unit_code);
CREATE INDEX IF NOT EXISTS derived_city_spatial_information_unit_scores_city_idx
    ON derived.city_spatial_information_unit_scores (city_code);

CREATE TABLE IF NOT EXISTS derived.city_spatial_information_category_scores (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    neighborhood_level TEXT NOT NULL DEFAULT '',
    family TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    category TEXT NOT NULL DEFAULT '',
    category_total DOUBLE PRECISION,
    p_category DOUBLE PRECISION,
    localization_entropy_nats DOUBLE PRECISION,
    effective_units_for_category DOUBLE PRECISION,
    kl_to_units_nats DOUBLE PRECISION,
    mi_contribution_nats DOUBLE PRECISION,
    dominant_unit_code TEXT NOT NULL DEFAULT '',
    dominant_unit_label TEXT NOT NULL DEFAULT '',
    dominant_unit_share DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_city_spatial_information_category_scores_uidx
    ON derived.city_spatial_information_category_scores (source_method, neighborhood_level, family, city_code, category);
CREATE INDEX IF NOT EXISTS derived_city_spatial_information_category_scores_city_idx
    ON derived.city_spatial_information_category_scores (city_code);

CREATE TABLE IF NOT EXISTS derived.ageb_spatial_information_within (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    family TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    ageb_code TEXT NOT NULL DEFAULT '',
    ageb_label TEXT NOT NULL DEFAULT '',
    ageb_total DOUBLE PRECISION,
    p_ageb DOUBLE PRECISION,
    n_manzanas_nonzero DOUBLE PRECISION,
    n_categories_nonzero DOUBLE PRECISION,
    conditional_mi_nats DOUBLE PRECISION,
    weighted_conditional_mi_nats DOUBLE PRECISION,
    h_manzanas_cond_nats DOUBLE PRECISION,
    h_categories_cond_nats DOUBLE PRECISION,
    nmi_min_cond DOUBLE PRECISION,
    effective_manzanas DOUBLE PRECISION,
    effective_categories DOUBLE PRECISION,
    dominant_manzana_code TEXT NOT NULL DEFAULT '',
    dominant_manzana_label TEXT NOT NULL DEFAULT '',
    dominant_manzana_share DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_ageb_spatial_information_within_uidx
    ON derived.ageb_spatial_information_within (source_method, family, city_code, ageb_code);
CREATE INDEX IF NOT EXISTS derived_ageb_spatial_information_within_city_idx
    ON derived.ageb_spatial_information_within (city_code);

CREATE TABLE IF NOT EXISTS derived.city_spatial_information_decomposition (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    family TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    mi_manzana_nats DOUBLE PRECISION,
    mi_ageb_nats DOUBLE PRECISION,
    mi_within_ageb_nats DOUBLE PRECISION,
    share_between_ageb DOUBLE PRECISION,
    share_within_ageb DOUBLE PRECISION,
    identity_gap_nats DOUBLE PRECISION,
    n_ageb_nonzero DOUBLE PRECISION,
    n_manzanas_nonzero DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_city_spatial_information_decomposition_uidx
    ON derived.city_spatial_information_decomposition (source_method, family, city_code);
CREATE INDEX IF NOT EXISTS derived_city_spatial_information_decomposition_city_idx
    ON derived.city_spatial_information_decomposition (city_code);

CREATE TABLE IF NOT EXISTS derived.city_spatial_information_regimes (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    decomposition_source_method TEXT NOT NULL DEFAULT '',
    family TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    population DOUBLE PRECISION,
    est_total DOUBLE PRECISION,
    mi_manzana_nats DOUBLE PRECISION,
    mi_ageb_nats DOUBLE PRECISION,
    mi_within_ageb_nats DOUBLE PRECISION,
    identity_gap_nats DOUBLE PRECISION,
    share_between_ageb DOUBLE PRECISION,
    share_within_ageb DOUBLE PRECISION,
    share_gap DOUBLE PRECISION,
    share_between_explained DOUBLE PRECISION,
    share_within_explained DOUBLE PRECISION,
    dominant_component_total TEXT NOT NULL DEFAULT '',
    regime_total_067 TEXT NOT NULL DEFAULT '',
    regime_explained_067 TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_city_spatial_information_regimes_uidx
    ON derived.city_spatial_information_regimes (source_method, family, city_code);
CREATE INDEX IF NOT EXISTS derived_city_spatial_information_regimes_city_idx
    ON derived.city_spatial_information_regimes (city_code);

CREATE TABLE IF NOT EXISTS derived.city_spatial_information_state_features (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    regimes_source_method TEXT NOT NULL DEFAULT '',
    family TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    population DOUBLE PRECISION,
    est_total DOUBLE PRECISION,
    log_est_total DOUBLE PRECISION,
    mi_manzana_nats DOUBLE PRECISION,
    share_within_explained DOUBLE PRECISION,
    share_between_explained DOUBLE PRECISION,
    share_gap DOUBLE PRECISION,
    n_centered DOUBLE PRECISION,
    z_city_area_km2 DOUBLE PRECISION,
    z_street_density_km_per_km2 DOUBLE PRECISION,
    z_mean_degree DOUBLE PRECISION,
    z_circuity_avg DOUBLE PRECISION,
    z_log_mi_manzana DOUBLE PRECISION,
    z_share_within_explained DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_city_spatial_information_state_features_uidx
    ON derived.city_spatial_information_state_features (source_method, family, city_code);
CREATE INDEX IF NOT EXISTS derived_city_spatial_information_state_features_city_idx
    ON derived.city_spatial_information_state_features (city_code);

CREATE TABLE IF NOT EXISTS derived.city_spatial_information_state_model_summary (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    family TEXT NOT NULL DEFAULT '',
    model_id TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    n_obs DOUBLE PRECISION,
    n_params DOUBLE PRECISION,
    r2 DOUBLE PRECISION,
    adj_r2 DOUBLE PRECISION,
    rss DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_city_spatial_information_state_model_summary_uidx
    ON derived.city_spatial_information_state_model_summary (source_method, family, model_id);

CREATE TABLE IF NOT EXISTS derived.city_spatial_information_state_model_coefficients (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    family TEXT NOT NULL DEFAULT '',
    model_id TEXT NOT NULL DEFAULT '',
    term TEXT NOT NULL DEFAULT '',
    coefficient DOUBLE PRECISION,
    stderr DOUBLE PRECISION,
    term_role TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_city_spatial_information_state_model_coefficients_uidx
    ON derived.city_spatial_information_state_model_coefficients (source_method, family, model_id, term);

CREATE TABLE IF NOT EXISTS derived.city_spatial_information_state_city_parameters (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    family TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    population DOUBLE PRECISION,
    est_total DOUBLE PRECISION,
    alpha_eff DOUBLE PRECISION,
    beta_eff DOUBLE PRECISION,
    observed_logE DOUBLE PRECISION,
    predicted_logE DOUBLE PRECISION,
    residual_logE DOUBLE PRECISION,
    z_log_mi_manzana DOUBLE PRECISION,
    z_share_within_explained DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_city_spatial_information_state_city_parameters_uidx
    ON derived.city_spatial_information_state_city_parameters (source_method, family, city_code);
CREATE INDEX IF NOT EXISTS derived_city_spatial_information_state_city_parameters_city_idx
    ON derived.city_spatial_information_state_city_parameters (city_code);

CREATE TABLE IF NOT EXISTS experiments.city_coarse_graining_status (
    row_id BIGSERIAL PRIMARY KEY,
    source_method TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    n_manzana_rows INTEGER,
    n_ageb_rows INTEGER,
    output_rows INTEGER,
    error_message TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS experiments_city_coarse_graining_status_uidx
    ON experiments.city_coarse_graining_status (source_method, city_code);
CREATE INDEX IF NOT EXISTS experiments_city_coarse_graining_status_status_idx
    ON experiments.city_coarse_graining_status (status);

CREATE TABLE IF NOT EXISTS derived.city_coarse_graining_scale_summary (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    support_scale TEXT NOT NULL DEFAULT '',
    scope TEXT NOT NULL DEFAULT '',
    threshold_units DOUBLE PRECISION,
    description TEXT NOT NULL DEFAULT '',
    n_obs DOUBLE PRECISION,
    n_groups DOUBLE PRECISION,
    n_params DOUBLE PRECISION,
    alpha DOUBLE PRECISION,
    beta DOUBLE PRECISION,
    r2 DOUBLE PRECISION,
    adj_r2 DOUBLE PRECISION,
    rss DOUBLE PRECISION,
    resid_std DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_city_coarse_graining_scale_summary_uidx
    ON derived.city_coarse_graining_scale_summary (source_method, support_scale, scope, threshold_units);

CREATE TABLE IF NOT EXISTS derived.city_coarse_graining_unit_fits (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    support_scale TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    population DOUBLE PRECISION,
    est_total DOUBLE PRECISION,
    n_units DOUBLE PRECISION,
    alpha DOUBLE PRECISION,
    beta DOUBLE PRECISION,
    r2 DOUBLE PRECISION,
    adj_r2 DOUBLE PRECISION,
    rss DOUBLE PRECISION,
    resid_std DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_city_coarse_graining_unit_fits_uidx
    ON derived.city_coarse_graining_unit_fits (source_method, support_scale, city_code);
CREATE INDEX IF NOT EXISTS derived_city_coarse_graining_unit_fits_city_idx
    ON derived.city_coarse_graining_unit_fits (city_code);

CREATE TABLE IF NOT EXISTS derived.city_coarse_graining_paths (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    population DOUBLE PRECISION,
    est_total DOUBLE PRECISION,
    n_manzana DOUBLE PRECISION,
    n_ageb DOUBLE PRECISION,
    beta_manzana DOUBLE PRECISION,
    beta_ageb DOUBLE PRECISION,
    delta_beta_ageb_minus_manzana DOUBLE PRECISION,
    r2_manzana DOUBLE PRECISION,
    r2_ageb DOUBLE PRECISION,
    delta_r2_ageb_minus_manzana DOUBLE PRECISION,
    resid_std_manzana DOUBLE PRECISION,
    resid_std_ageb DOUBLE PRECISION,
    delta_resid_std_ageb_minus_manzana DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_city_coarse_graining_paths_uidx
    ON derived.city_coarse_graining_paths (source_method, city_code);
CREATE INDEX IF NOT EXISTS derived_city_coarse_graining_paths_city_idx
    ON derived.city_coarse_graining_paths (city_code);

CREATE TABLE IF NOT EXISTS derived.city_multiscale_synthesis_features (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    coarse_source_method TEXT NOT NULL DEFAULT '',
    regime_source_method TEXT NOT NULL DEFAULT '',
    family TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    threshold_units DOUBLE PRECISION,
    population DOUBLE PRECISION,
    est_total DOUBLE PRECISION,
    n_manzana DOUBLE PRECISION,
    n_ageb DOUBLE PRECISION,
    beta_manzana DOUBLE PRECISION,
    beta_ageb DOUBLE PRECISION,
    delta_beta DOUBLE PRECISION,
    r2_manzana DOUBLE PRECISION,
    r2_ageb DOUBLE PRECISION,
    delta_r2 DOUBLE PRECISION,
    resid_std_manzana DOUBLE PRECISION,
    resid_std_ageb DOUBLE PRECISION,
    delta_resid_std DOUBLE PRECISION,
    mi_manzana_nats DOUBLE PRECISION,
    mi_ageb_nats DOUBLE PRECISION,
    mi_within_ageb_nats DOUBLE PRECISION,
    share_between_explained DOUBLE PRECISION,
    share_within_explained DOUBLE PRECISION,
    share_gap DOUBLE PRECISION,
    n_centered DOUBLE PRECISION,
    z_log_mi_manzana DOUBLE PRECISION,
    z_share_within_explained DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_city_multiscale_synthesis_features_uidx
    ON derived.city_multiscale_synthesis_features (source_method, family, city_code, threshold_units);
CREATE INDEX IF NOT EXISTS derived_city_multiscale_synthesis_features_city_idx
    ON derived.city_multiscale_synthesis_features (city_code);

CREATE TABLE IF NOT EXISTS derived.city_multiscale_synthesis_model_summary (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    family TEXT NOT NULL DEFAULT '',
    outcome TEXT NOT NULL DEFAULT '',
    model_id TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    n_obs DOUBLE PRECISION,
    n_params DOUBLE PRECISION,
    r2 DOUBLE PRECISION,
    adj_r2 DOUBLE PRECISION,
    rss DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_city_multiscale_synthesis_model_summary_uidx
    ON derived.city_multiscale_synthesis_model_summary (source_method, family, outcome, model_id);

CREATE TABLE IF NOT EXISTS derived.city_multiscale_synthesis_model_coefficients (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    family TEXT NOT NULL DEFAULT '',
    outcome TEXT NOT NULL DEFAULT '',
    model_id TEXT NOT NULL DEFAULT '',
    term TEXT NOT NULL DEFAULT '',
    coefficient DOUBLE PRECISION,
    stderr DOUBLE PRECISION,
    term_role TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_city_multiscale_synthesis_model_coefficients_uidx
    ON derived.city_multiscale_synthesis_model_coefficients (source_method, family, outcome, model_id, term);

CREATE TABLE IF NOT EXISTS derived.city_multiscale_synthesis_quadrants (
    row_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT '',
    source_method TEXT NOT NULL DEFAULT '',
    family TEXT NOT NULL DEFAULT '',
    city_code TEXT NOT NULL DEFAULT '',
    city_name TEXT NOT NULL DEFAULT '',
    state_code TEXT NOT NULL DEFAULT '',
    threshold_units DOUBLE PRECISION,
    delta_r2 DOUBLE PRECISION,
    share_within_explained DOUBLE PRECISION,
    delta_r2_centered DOUBLE PRECISION,
    share_within_centered DOUBLE PRECISION,
    quadrant TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS derived_city_multiscale_synthesis_quadrants_uidx
    ON derived.city_multiscale_synthesis_quadrants (source_method, family, city_code, threshold_units);
CREATE INDEX IF NOT EXISTS derived_city_multiscale_synthesis_quadrants_city_idx
    ON derived.city_multiscale_synthesis_quadrants (city_code);

CREATE TABLE IF NOT EXISTS experiments.run_manifest (
    run_id BIGSERIAL PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    params_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    notes TEXT NOT NULL DEFAULT ''
);
SQL

echo "Bootstrapped urban_sami_exp in container ${DB_CONTAINER}"

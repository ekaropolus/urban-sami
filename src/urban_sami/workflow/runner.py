from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from urban_sami.aggregation import DenueObservation, attach_denue_observations, contexts_by_unit, seed_unit_metrics
from urban_sami.artifacts.assignments import write_assignments
from urban_sami.artifacts.indicators import write_indicator_outputs
from urban_sami.artifacts.bundle import write_bundle, write_manifest
from urban_sami.artifacts.compare import write_comparison_rows
from urban_sami.artifacts.figures import figure_slug, write_model_overview_figure, write_scaling_scatter_figure
from urban_sami.artifacts.models import write_model_summaries
from urban_sami.artifacts.parity import write_parity_rows
from urban_sami.artifacts.scores import write_unit_scores
from urban_sami.artifacts.tables import read_summary_csv, write_summary_csv
from urban_sami.geometry import (
    PointObservation,
    UnitRecord,
    assign_points_to_polygons,
    build_admin_domain,
    build_geofence_domain,
    build_hex_domain,
    load_geojson_polygons,
)
from urban_sami.indicators import default_indicator_registry
from urban_sami.io.csvio import read_csv_rows
from urban_sami.io.loaders import GenericObservationRow, load_observations, load_units
from urban_sami.modeling import bootstrap_fit_intervals, compute_deviation_score, fit_by_name
from urban_sami.parity import SUMMARY_COLUMNS, summarize_exported_units, unit_export_filename
from urban_sami.workflow.schema import WorkflowSpec


def plan_workflow(workflow: WorkflowSpec) -> dict:
    return {
        "workflow_id": workflow.metadata.workflow_id,
        "title": workflow.metadata.title,
        "kind": workflow.kind,
        "country_code": workflow.metadata.country_code,
        "data_sources": {
            key: {"source_type": value.source_type, "path": value.path}
            for key, value in workflow.data_sources.items()
        },
        "outputs": {
            "base_dir": workflow.outputs.base_dir,
            "write_bundle_zip": workflow.outputs.write_bundle_zip,
            "write_report_md": workflow.outputs.write_report_md,
            "write_summary_csv": workflow.outputs.write_summary_csv,
            "write_figures": workflow.outputs.write_figures,
        },
        "geometry_domains": workflow.geometry_domains,
        "indicators": workflow.indicators,
    }


def _resolve_source_path(workflow: WorkflowSpec, *, key: str, manifest_path: Path | None = None) -> Path:
    source = workflow.data_sources[key]
    path = Path(source.path)
    if path.is_absolute():
        return path
    base = manifest_path.parent if manifest_path is not None else Path.cwd()
    return (base / path).resolve()


def run_workflow(workflow: WorkflowSpec, *, manifest_path: Path | None = None) -> dict:
    if workflow.kind == "summary_matrix_replay":
        return _run_summary_replay(workflow, manifest_path=manifest_path)
    if workflow.kind == "indicator_compute":
        return _run_indicator_compute(workflow, manifest_path=manifest_path)
    if workflow.kind == "indicator_sami":
        return _run_indicator_sami(workflow, manifest_path=manifest_path)
    if workflow.kind == "compare_csv_baseline":
        return _run_compare_csv_baseline(workflow, manifest_path=manifest_path)
    if workflow.kind == "geojson_point_assign":
        return _run_geojson_point_assign(workflow, manifest_path=manifest_path)
    if workflow.kind == "geojson_indicator_sami":
        return _run_geojson_indicator_sami(workflow, manifest_path=manifest_path)
    if workflow.kind == "polisplexity_matrix_parity":
        return _run_polisplexity_matrix_parity(workflow, manifest_path=manifest_path)
    if workflow.kind == "polisplexity_run_dir_parity":
        return _run_polisplexity_run_dir_parity(workflow, manifest_path=manifest_path)
    raise ValueError(f"unsupported workflow kind: {workflow.kind}")


def _run_summary_replay(workflow: WorkflowSpec, *, manifest_path: Path | None = None) -> dict:
    if "summary" not in workflow.data_sources:
        raise ValueError("summary_matrix_replay requires data_sources.summary")

    output_dir = workflow.output_base_path(manifest_path or Path.cwd()) / workflow.workflow_id
    output_dir.mkdir(parents=True, exist_ok=True)

    summary_src = _resolve_source_path(workflow, key="summary", manifest_path=manifest_path)
    summary_rows = read_summary_csv(summary_src)
    summary_out = write_summary_csv(summary_rows, output_dir / "summary.csv")

    written_files = [summary_out]
    if workflow.outputs.write_figures:
        figure_out = write_model_overview_figure(summary_rows, output_dir / "summary_overview.svg", title=workflow.metadata.title or workflow.workflow_id)
        written_files.append(figure_out)

    if "report" in workflow.data_sources and workflow.outputs.write_report_md:
        report_src = _resolve_source_path(workflow, key="report", manifest_path=manifest_path)
        report_out = output_dir / "report.md"
        report_out.write_text(report_src.read_text(encoding="utf-8"), encoding="utf-8")
        written_files.append(report_out)

    if "request" in workflow.data_sources:
        request_src = _resolve_source_path(workflow, key="request", manifest_path=manifest_path)
        request_out = output_dir / "request.json"
        request_out.write_text(request_src.read_text(encoding="utf-8"), encoding="utf-8")
        written_files.append(request_out)

    metadata = {
        "workflow_id": workflow.workflow_id,
        "kind": workflow.kind,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    manifest_out = write_manifest(output_dir, files=written_files, metadata=metadata)
    written_files.append(manifest_out)

    bundle_out = None
    if workflow.outputs.write_bundle_zip:
        bundle_out = write_bundle(output_dir, files=written_files, bundle_name=f"{workflow.workflow_id}.zip")
        written_files.append(bundle_out)

    return {
        "ok": True,
        "workflow_id": workflow.workflow_id,
        "output_dir": str(output_dir),
        "files": [str(path) for path in written_files],
        "bundle_path": str(bundle_out) if bundle_out is not None else "",
        "summary_rows": int(len(summary_rows)),
    }


def _build_domain(domain_spec: dict) -> object:
    domain_type = str(domain_spec.get("domain_type") or "").strip()
    domain_id = str(domain_spec.get("domain_id") or "").strip()
    if not domain_id:
        raise ValueError("geometry domain requires domain_id")
    if domain_type == "admin":
        return build_admin_domain(domain_id=domain_id, level=str(domain_spec.get("level") or "").strip())
    if domain_type == "hex":
        return build_hex_domain(domain_id=domain_id, resolution=int(domain_spec.get("resolution") or 0))
    if domain_type == "geofence":
        geofence_id = str(domain_spec.get("geofence_id") or domain_id).strip()
        return build_geofence_domain(domain_id=domain_id, geofence_id=geofence_id)
    raise ValueError(f"unsupported geometry domain type: {domain_type}")


def _run_indicator_compute(workflow: WorkflowSpec, *, manifest_path: Path | None = None) -> dict:
    if "units" not in workflow.data_sources:
        raise ValueError("indicator_compute requires data_sources.units")
    if "observations" not in workflow.data_sources:
        raise ValueError("indicator_compute requires data_sources.observations")
    if not workflow.geometry_domains:
        raise ValueError("indicator_compute requires geometry_domains")
    if not workflow.indicators:
        raise ValueError("indicator_compute requires indicators")

    output_dir = workflow.output_base_path(manifest_path or Path.cwd()) / workflow.workflow_id
    output_dir.mkdir(parents=True, exist_ok=True)

    domain_map = {}
    for item in workflow.geometry_domains:
        if not isinstance(item, dict):
            continue
        enabled = item.get("enabled", True)
        if enabled is False:
            continue
        domain = _build_domain(item)
        domain_map[getattr(domain, "domain_id")] = domain

    unit_rows = read_csv_rows(_resolve_source_path(workflow, key="units", manifest_path=manifest_path))
    observation_rows = read_csv_rows(_resolve_source_path(workflow, key="observations", manifest_path=manifest_path))

    units_by_domain: dict[str, list[UnitRecord]] = {}
    for domain_id in domain_map:
        domain_unit_rows = [row for row in unit_rows if str(row.get("domain_id") or "").strip() == domain_id]
        units_by_domain[domain_id] = [
            UnitRecord(
                domain_id=domain_id,
                unit_id=str(row.get("unit_id") or "").strip(),
                unit_label=str(row.get("unit_label") or "").strip(),
                parent_id=str(row.get("parent_id") or "").strip(),
                attrs={
                    "population": float(row.get("population") or 0.0),
                    "households": float(row.get("households") or 0.0),
                    "area_km2": float(row.get("area_km2") or 0.0),
                },
            )
            for row in domain_unit_rows
            if str(row.get("unit_id") or "").strip()
        ]

    observations = [
        DenueObservation(
            unit_id=str(row.get("unit_id") or "").strip(),
            domain_id=str(row.get("domain_id") or "").strip(),
            scian_code=str(row.get("scian_code") or "").strip(),
            per_ocu=str(row.get("per_ocu") or "").strip(),
        )
        for row in observation_rows
        if str(row.get("unit_id") or "").strip() and str(row.get("domain_id") or "").strip()
    ]

    registry = default_indicator_registry()
    output_rows: list[dict] = []
    for domain_id, domain in domain_map.items():
        metrics = seed_unit_metrics(domain=domain, units=units_by_domain.get(domain_id, []))
        domain_observations = [obs for obs in observations if obs.domain_id == domain_id]
        attach_denue_observations(metrics, domain_observations)
        contexts = contexts_by_unit(metrics)
        unit_lookup = {unit.unit_id: unit for unit in units_by_domain.get(domain_id, [])}
        for indicator_spec in workflow.indicators:
            if not isinstance(indicator_spec, dict):
                continue
            if indicator_spec.get("enabled", True) is False:
                continue
            indicator_key = str(indicator_spec.get("key") or "").strip()
            if not indicator_key:
                continue
            spec = registry.get(indicator_key)
            params = indicator_spec.get("params") if isinstance(indicator_spec.get("params"), dict) else None
            for unit_id, context in contexts.items():
                value = spec.compute(context, params) if spec.compute is not None else None
                if value is None:
                    continue
                unit = unit_lookup.get(unit_id)
                output_rows.append(
                    {
                        "domain_id": domain_id,
                        "unit_id": unit_id,
                        "indicator_key": indicator_key,
                        "indicator_value": value,
                        "unit_label": getattr(unit, "unit_label", "") or "",
                        "parent_id": getattr(unit, "parent_id", "") or "",
                    }
                )

    indicators_out = write_indicator_outputs(output_rows, output_dir / "indicator_outputs.csv")
    written_files = [indicators_out]
    metadata = {
        "workflow_id": workflow.workflow_id,
        "kind": workflow.kind,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    manifest_out = write_manifest(output_dir, files=written_files, metadata=metadata)
    written_files.append(manifest_out)
    bundle_out = None
    if workflow.outputs.write_bundle_zip:
        bundle_out = write_bundle(output_dir, files=written_files, bundle_name=f"{workflow.workflow_id}.zip")
        written_files.append(bundle_out)
    return {
        "ok": True,
        "workflow_id": workflow.workflow_id,
        "output_dir": str(output_dir),
        "files": [str(path) for path in written_files],
        "bundle_path": str(bundle_out) if bundle_out is not None else "",
        "indicator_rows": len(output_rows),
    }


def _scale_value(unit: UnitRecord, scale_basis: str) -> float:
    basis = str(scale_basis or "population").strip().lower()
    if basis == "households":
        return float(unit.attrs.get("households") or 0.0)
    return float(unit.attrs.get("population") or 0.0)


def _to_attr_float(value: object) -> float:
    try:
        raw = str(value or "").strip()
        if "," in raw and "." not in raw:
            raw = raw.replace(",", ".")
        return float(raw or 0.0)
    except Exception:
        return 0.0


def _compute_indicator_rows_from_loaded(
    workflow: WorkflowSpec,
    *,
    domain_map: dict[str, object],
    units_by_domain: dict[str, list[UnitRecord]],
    loaded_observations: list[GenericObservationRow],
) -> tuple[list[dict], dict[str, UnitRecord]]:
    unit_lookup: dict[str, UnitRecord] = {}
    for domain_units in units_by_domain.values():
        for unit in domain_units:
            unit_lookup[unit.unit_id] = unit

    observations = [
        DenueObservation(
            unit_id=row.unit_id,
            domain_id=row.domain_id,
            scian_code=row.scian_code,
            per_ocu=row.per_ocu,
        )
        for row in loaded_observations
        if row.domain_id and row.unit_id
    ]

    registry = default_indicator_registry()
    output_rows: list[dict] = []
    for domain_id, domain in domain_map.items():
        metrics = seed_unit_metrics(domain=domain, units=units_by_domain.get(domain_id, []))
        domain_observations = [obs for obs in observations if obs.domain_id == domain_id]
        attach_denue_observations(metrics, domain_observations)
        contexts = contexts_by_unit(metrics)
        local_lookup = {unit.unit_id: unit for unit in units_by_domain.get(domain_id, [])}
        for indicator_spec in workflow.indicators:
            if not isinstance(indicator_spec, dict):
                continue
            if indicator_spec.get("enabled", True) is False:
                continue
            indicator_key = str(indicator_spec.get("key") or "").strip()
            if not indicator_key:
                continue
            spec = registry.get(indicator_key)
            params = indicator_spec.get("params") if isinstance(indicator_spec.get("params"), dict) else None
            scale_basis = str(indicator_spec.get("scale_basis") or "population").strip().lower()
            for unit_id, context in contexts.items():
                value = spec.compute(context, params) if spec.compute is not None else None
                if value is None:
                    continue
                unit = local_lookup.get(unit_id)
                output_rows.append(
                    {
                        "domain_id": domain_id,
                        "unit_id": unit_id,
                        "indicator_key": indicator_key,
                        "indicator_value": value,
                        "unit_label": getattr(unit, "unit_label", "") or "",
                        "parent_id": getattr(unit, "parent_id", "") or "",
                        "scale_basis": scale_basis,
                        "scale_n": _scale_value(unit, scale_basis) if unit is not None else 0.0,
                    }
                )
    return output_rows, unit_lookup


def _compute_model_rows(workflow: WorkflowSpec, indicator_rows: list[dict]) -> list[dict]:
    model_cfg = workflow.models
    fit_methods = model_cfg.get("fit_methods") or ["ols"]
    if not isinstance(fit_methods, list) or not fit_methods:
        fit_methods = ["ols"]
    bootstrap_iterations = int(model_cfg.get("bootstrap_iterations") or 0)
    bootstrap_seed = int(model_cfg.get("bootstrap_seed") or 42)

    grouped: dict[tuple[str, str, str], list[dict]] = {}
    for row in indicator_rows:
        key = (str(row["domain_id"]), str(row["indicator_key"]), str(row.get("scale_basis") or "population"))
        grouped.setdefault(key, []).append(row)

    model_rows: list[dict] = []
    for (domain_id, indicator_key, scale_basis), rows in sorted(grouped.items()):
        filtered_rows = [
            row
            for row in rows
            if float(row.get("indicator_value") or 0.0) > 0 and float(row.get("scale_n") or 0.0) > 0
        ]
        if len(filtered_rows) < 2:
            continue
        y = [float(row["indicator_value"]) for row in filtered_rows]
        n = [float(row["scale_n"]) for row in filtered_rows]
        for fit_method in fit_methods:
            result = fit_by_name(y, n, str(fit_method))
            intervals = bootstrap_fit_intervals(
                y,
                n,
                str(fit_method),
                n_bootstrap=bootstrap_iterations,
                seed=bootstrap_seed,
            ) if bootstrap_iterations > 1 else {"alpha_low": None, "alpha_high": None, "beta_low": None, "beta_high": None}
            model_rows.append(
                {
                    "domain_id": domain_id,
                    "indicator_key": indicator_key,
                    "scale_basis": scale_basis,
                    "fit_method": str(fit_method),
                    "units": len(filtered_rows),
                    "alpha": result.alpha,
                    "alpha_ci95_low": intervals["alpha_low"] if intervals["alpha_low"] is not None else "",
                    "alpha_ci95_high": intervals["alpha_high"] if intervals["alpha_high"] is not None else "",
                    "beta": result.beta,
                    "beta_ci95_low": intervals["beta_low"] if intervals["beta_low"] is not None else "",
                    "beta_ci95_high": intervals["beta_high"] if intervals["beta_high"] is not None else "",
                    "r2": result.r2,
                    "resid_std": result.residual_std,
                    "aic": result.aic if result.aic is not None else "",
                    "bic": result.bic if result.bic is not None else "",
                    "value_min": min(y),
                    "value_max": max(y),
                    "n_min": min(n),
                    "n_max": max(n),
                }
            )
    return model_rows


def _compute_unit_score_rows(indicator_rows: list[dict], model_rows: list[dict]) -> list[dict]:
    grouped_indicator_rows: dict[tuple[str, str, str], list[dict]] = {}
    for row in indicator_rows:
        key = (str(row["domain_id"]), str(row["indicator_key"]), str(row.get("scale_basis") or "population"))
        grouped_indicator_rows.setdefault(key, []).append(row)

    score_rows: list[dict] = []
    for model in model_rows:
        key = (
            str(model["domain_id"]),
            str(model["indicator_key"]),
            str(model.get("scale_basis") or "population"),
        )
        rows = grouped_indicator_rows.get(key, [])
        alpha = _to_attr_float(model.get("alpha"))
        beta = _to_attr_float(model.get("beta"))
        resid_std = _to_attr_float(model.get("resid_std"))
        for row in rows:
            y_value = float(row.get("indicator_value") or 0.0)
            n_value = float(row.get("scale_n") or 0.0)
            if y_value <= 0.0 or n_value <= 0.0:
                continue
            score = compute_deviation_score(y_value, n_value, alpha, beta, resid_std)
            score_rows.append(
                {
                    "domain_id": key[0],
                    "indicator_key": key[1],
                    "scale_basis": key[2],
                    "fit_method": str(model.get("fit_method") or ""),
                    "unit_id": str(row.get("unit_id") or ""),
                    "unit_label": str(row.get("unit_label") or ""),
                    "parent_id": str(row.get("parent_id") or ""),
                    "indicator_value": y_value,
                    "scale_n": n_value,
                    "y_expected": score.y_expected,
                    "epsilon_log": score.epsilon_log,
                    "sami": score.sami,
                    "z_residual": score.z_residual,
                    "alpha": alpha,
                    "beta": beta,
                    "r2": _to_attr_float(model.get("r2")),
                    "resid_std": resid_std,
                }
            )
    return score_rows


def _compute_indicator_rows(
    workflow: WorkflowSpec,
    *,
    manifest_path: Path | None = None,
) -> tuple[list[dict], dict[str, UnitRecord]]:
    domain_map = {}
    for item in workflow.geometry_domains:
        if not isinstance(item, dict):
            continue
        enabled = item.get("enabled", True)
        if enabled is False:
            continue
        domain = _build_domain(item)
        domain_map[getattr(domain, "domain_id")] = domain

    units_source = workflow.data_sources["units"]
    observations_source = workflow.data_sources["observations"]
    loaded_units = load_units(
        _resolve_source_path(workflow, key="units", manifest_path=manifest_path),
        source_type=units_source.source_type,
    )
    loaded_observations = load_observations(
        _resolve_source_path(workflow, key="observations", manifest_path=manifest_path),
        source_type=observations_source.source_type,
    )

    units_by_domain: dict[str, list[UnitRecord]] = {}
    unit_lookup: dict[str, UnitRecord] = {}
    for domain_id in domain_map:
        domain_unit_rows = [row for row in loaded_units if row.domain_id == domain_id]
        domain_units = [
            UnitRecord(
                domain_id=domain_id,
                unit_id=row.unit_id,
                unit_label=row.unit_label,
                parent_id=row.parent_id,
                attrs={
                    "population": row.population,
                    "households": row.households,
                    "area_km2": row.area_km2,
                },
            )
            for row in domain_unit_rows
        ]
        units_by_domain[domain_id] = domain_units
        for unit in domain_units:
            unit_lookup[unit.unit_id] = unit

    return _compute_indicator_rows_from_loaded(
        workflow,
        domain_map=domain_map,
        units_by_domain=units_by_domain,
        loaded_observations=loaded_observations,
    )


def _run_indicator_sami(workflow: WorkflowSpec, *, manifest_path: Path | None = None) -> dict:
    if "units" not in workflow.data_sources:
        raise ValueError("indicator_sami requires data_sources.units")
    if "observations" not in workflow.data_sources:
        raise ValueError("indicator_sami requires data_sources.observations")
    if not workflow.geometry_domains:
        raise ValueError("indicator_sami requires geometry_domains")
    if not workflow.indicators:
        raise ValueError("indicator_sami requires indicators")

    output_dir = workflow.output_base_path(manifest_path or Path.cwd()) / workflow.workflow_id
    output_dir.mkdir(parents=True, exist_ok=True)

    indicator_rows, _unit_lookup = _compute_indicator_rows(workflow, manifest_path=manifest_path)
    indicators_out = write_indicator_outputs(indicator_rows, output_dir / "indicator_outputs.csv")

    model_rows = _compute_model_rows(workflow, indicator_rows)
    unit_score_rows = _compute_unit_score_rows(indicator_rows, model_rows)

    model_out = write_model_summaries(model_rows, output_dir / "model_summaries.csv")
    scores_out = write_unit_scores(unit_score_rows, output_dir / "unit_scores.csv")
    written_files = [indicators_out, model_out, scores_out]
    if workflow.outputs.write_figures:
        overview_out = write_model_overview_figure(model_rows, output_dir / "model_overview.svg", title=workflow.metadata.title or workflow.workflow_id)
        written_files.append(overview_out)
        grouped_indicator_rows: dict[tuple[str, str, str], list[dict]] = {}
        grouped_model_rows: dict[tuple[str, str, str], list[dict]] = {}
        for row in indicator_rows:
            key = (str(row["domain_id"]), str(row["indicator_key"]), str(row.get("scale_basis") or "population"))
            grouped_indicator_rows.setdefault(key, []).append(row)
        for row in model_rows:
            key = (str(row["domain_id"]), str(row["indicator_key"]), str(row.get("scale_basis") or "population"))
            grouped_model_rows.setdefault(key, []).append(row)
        for key, rows in sorted(grouped_indicator_rows.items()):
            models = grouped_model_rows.get(key, [])
            if not models:
                continue
            best = max(models, key=lambda item: _to_attr_float(item.get("r2")))
            domain_id, indicator_key, scale_basis = key
            scatter_out = write_scaling_scatter_figure(
                rows,
                output_dir / f"scatter_{figure_slug(domain_id, indicator_key, scale_basis)}.svg",
                title=f"{domain_id} · {indicator_key}",
                x_key="scale_n",
                y_key="indicator_value",
                fit_alpha=_to_attr_float(best.get("alpha")),
                fit_beta=_to_attr_float(best.get("beta")),
                annotation=f"{best.get('fit_method', 'fit')}  β={_to_attr_float(best.get('beta')):.3f}  R²={_to_attr_float(best.get('r2')):.3f}",
            )
            written_files.append(scatter_out)
    metadata = {
        "workflow_id": workflow.workflow_id,
        "kind": workflow.kind,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    manifest_out = write_manifest(output_dir, files=written_files, metadata=metadata)
    written_files.append(manifest_out)
    bundle_out = None
    if workflow.outputs.write_bundle_zip:
        bundle_out = write_bundle(output_dir, files=written_files, bundle_name=f"{workflow.workflow_id}.zip")
        written_files.append(bundle_out)
    return {
        "ok": True,
        "workflow_id": workflow.workflow_id,
        "output_dir": str(output_dir),
        "files": [str(path) for path in written_files],
        "bundle_path": str(bundle_out) if bundle_out is not None else "",
        "indicator_rows": len(indicator_rows),
        "model_rows": len(model_rows),
        "unit_score_rows": len(unit_score_rows),
    }


def _run_compare_csv_baseline(workflow: WorkflowSpec, *, manifest_path: Path | None = None) -> dict:
    if "left" not in workflow.data_sources or "right" not in workflow.data_sources:
        raise ValueError("compare_csv_baseline requires data_sources.left and data_sources.right")
    output_dir = workflow.output_base_path(manifest_path or Path.cwd()) / workflow.workflow_id
    output_dir.mkdir(parents=True, exist_ok=True)

    left_rows = read_csv_rows(_resolve_source_path(workflow, key="left", manifest_path=manifest_path))
    right_rows = read_csv_rows(_resolve_source_path(workflow, key="right", manifest_path=manifest_path))
    compare_cfg = workflow.raw.get("compare") or {}
    key_columns = list(compare_cfg.get("key_columns") or [])
    value_columns = list(compare_cfg.get("value_columns") or [])
    if not key_columns or not value_columns:
        raise ValueError("compare_csv_baseline requires compare.key_columns and compare.value_columns")

    def make_key(row: dict[str, str]) -> str:
        return "|".join(str(row.get(col, "")).strip() for col in key_columns)

    left_map = {make_key(row): row for row in left_rows}
    right_map = {make_key(row): row for row in right_rows}
    all_keys = sorted(set(left_map.keys()) | set(right_map.keys()))

    comparison_rows: list[dict] = []
    mismatch_count = 0
    for key in all_keys:
        left_row = left_map.get(key, {})
        right_row = right_map.get(key, {})
        for column in value_columns:
            left_value = str(left_row.get(column, ""))
            right_value = str(right_row.get(column, ""))
            status = "match" if left_value == right_value else "mismatch"
            if status == "mismatch":
                mismatch_count += 1
            comparison_rows.append(
                {
                    "key": key,
                    "column": column,
                    "left_value": left_value,
                    "right_value": right_value,
                    "status": status,
                }
            )

    comparison_out = write_comparison_rows(comparison_rows, output_dir / "comparison.csv")
    written_files = [comparison_out]
    metadata = {
        "workflow_id": workflow.workflow_id,
        "kind": workflow.kind,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mismatch_count": mismatch_count,
    }
    manifest_out = write_manifest(output_dir, files=written_files, metadata=metadata)
    written_files.append(manifest_out)
    bundle_out = None
    if workflow.outputs.write_bundle_zip:
        bundle_out = write_bundle(output_dir, files=written_files, bundle_name=f"{workflow.workflow_id}.zip")
        written_files.append(bundle_out)
    return {
        "ok": True,
        "workflow_id": workflow.workflow_id,
        "output_dir": str(output_dir),
        "files": [str(path) for path in written_files],
        "bundle_path": str(bundle_out) if bundle_out is not None else "",
        "comparison_rows": len(comparison_rows),
        "mismatch_count": mismatch_count,
    }


def _run_geojson_point_assign(workflow: WorkflowSpec, *, manifest_path: Path | None = None) -> dict:
    if "polygons" not in workflow.data_sources or "observations" not in workflow.data_sources:
        raise ValueError("geojson_point_assign requires data_sources.polygons and data_sources.observations")
    output_dir = workflow.output_base_path(manifest_path or Path.cwd()) / workflow.workflow_id
    output_dir.mkdir(parents=True, exist_ok=True)

    polygons_source = workflow.data_sources["polygons"]
    observations_source = workflow.data_sources["observations"]
    polygons_path = _resolve_source_path(workflow, key="polygons", manifest_path=manifest_path)
    observations_path = _resolve_source_path(workflow, key="observations", manifest_path=manifest_path)
    domain_cfgs = [item for item in workflow.geometry_domains if isinstance(item, dict) and item.get("domain_type") == "geojson_polygon"]
    if not domain_cfgs:
        raise ValueError("geojson_point_assign requires a geometry_domains entry with domain_type=geojson_polygon")
    domain_cfg = domain_cfgs[0]
    domain_id = str(domain_cfg.get("domain_id") or "").strip()
    if not domain_id:
        raise ValueError("geojson polygon domain requires domain_id")

    polygons = load_geojson_polygons(
        polygons_path,
        domain_id=domain_id,
        unit_id_field=str(domain_cfg.get("unit_id_field") or "unit_id"),
        unit_label_field=str(domain_cfg.get("unit_label_field") or "unit_label"),
        parent_id_field=str(domain_cfg.get("parent_id_field") or "parent_id"),
    )
    loaded_obs = load_observations(observations_path, source_type=observations_source.source_type)
    points = [
        PointObservation(obs_id=row.obs_id, lon=row.lon, lat=row.lat)
        for row in loaded_obs
    ]
    assigned = assign_points_to_polygons(points, polygons)
    assignment_rows = [{"obs_id": obs_id, "domain_id": domain_id, "unit_id": unit_id} for obs_id, unit_id in sorted(assigned.items())]
    assignments_out = write_assignments(assignment_rows, output_dir / "assignments.csv")
    written_files = [assignments_out]
    metadata = {
        "workflow_id": workflow.workflow_id,
        "kind": workflow.kind,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "assigned_count": len(assignment_rows),
    }
    manifest_out = write_manifest(output_dir, files=written_files, metadata=metadata)
    written_files.append(manifest_out)
    bundle_out = None
    if workflow.outputs.write_bundle_zip:
        bundle_out = write_bundle(output_dir, files=written_files, bundle_name=f"{workflow.workflow_id}.zip")
        written_files.append(bundle_out)
    return {
        "ok": True,
        "workflow_id": workflow.workflow_id,
        "output_dir": str(output_dir),
        "files": [str(path) for path in written_files],
        "bundle_path": str(bundle_out) if bundle_out is not None else "",
        "assigned_count": len(assignment_rows),
    }


def _run_geojson_indicator_sami(workflow: WorkflowSpec, *, manifest_path: Path | None = None) -> dict:
    if "polygons" not in workflow.data_sources or "observations" not in workflow.data_sources:
        raise ValueError("geojson_indicator_sami requires data_sources.polygons and data_sources.observations")
    if not workflow.indicators:
        raise ValueError("geojson_indicator_sami requires indicators")

    output_dir = workflow.output_base_path(manifest_path or Path.cwd()) / workflow.workflow_id
    output_dir.mkdir(parents=True, exist_ok=True)

    polygons_path = _resolve_source_path(workflow, key="polygons", manifest_path=manifest_path)
    observations_path = _resolve_source_path(workflow, key="observations", manifest_path=manifest_path)
    observations_source = workflow.data_sources["observations"]
    domain_cfgs = [item for item in workflow.geometry_domains if isinstance(item, dict) and item.get("domain_type") == "geojson_polygon"]
    if not domain_cfgs:
        raise ValueError("geojson_indicator_sami requires a geometry_domains entry with domain_type=geojson_polygon")
    domain_cfg = domain_cfgs[0]
    domain_id = str(domain_cfg.get("domain_id") or "").strip()
    if not domain_id:
        raise ValueError("geojson polygon domain requires domain_id")

    polygons = load_geojson_polygons(
        polygons_path,
        domain_id=domain_id,
        unit_id_field=str(domain_cfg.get("unit_id_field") or "unit_id"),
        unit_label_field=str(domain_cfg.get("unit_label_field") or "unit_label"),
        parent_id_field=str(domain_cfg.get("parent_id_field") or "parent_id"),
    )
    domain_map = {domain_id: _build_domain({"domain_id": domain_id, "domain_type": "geofence", "geofence_id": domain_id})}

    population_field = str(domain_cfg.get("population_field") or "population").strip()
    households_field = str(domain_cfg.get("households_field") or "households").strip()
    area_field = str(domain_cfg.get("area_km2_field") or "area_km2").strip()
    units = [
        UnitRecord(
            domain_id=domain_id,
            unit_id=polygon.unit_id,
            unit_label=polygon.unit_label,
            parent_id=polygon.parent_id,
            attrs={
                "population": _to_attr_float(polygon.attrs.get(population_field)),
                "households": _to_attr_float(polygon.attrs.get(households_field)),
                "area_km2": _to_attr_float(polygon.attrs.get(area_field)),
            },
        )
        for polygon in polygons
    ]
    units_by_domain = {domain_id: units}

    raw_observations = load_observations(observations_path, source_type=observations_source.source_type)
    points = [
        PointObservation(obs_id=row.obs_id, lon=row.lon, lat=row.lat)
        for row in raw_observations
        if row.lon or row.lat
    ]
    assigned = assign_points_to_polygons(points, polygons)
    assigned_observations = [
        GenericObservationRow(
            domain_id=domain_id,
            unit_id=assigned[row.obs_id],
            scian_code=row.scian_code,
            per_ocu=row.per_ocu,
            obs_id=row.obs_id,
            lon=row.lon,
            lat=row.lat,
        )
        for row in raw_observations
        if row.obs_id in assigned
    ]

    assignment_rows = [
        {"obs_id": obs_id, "domain_id": domain_id, "unit_id": unit_id}
        for obs_id, unit_id in sorted(assigned.items())
    ]
    assignments_out = write_assignments(assignment_rows, output_dir / "assignments.csv")

    indicator_rows, _unit_lookup = _compute_indicator_rows_from_loaded(
        workflow,
        domain_map=domain_map,
        units_by_domain=units_by_domain,
        loaded_observations=assigned_observations,
    )
    indicators_out = write_indicator_outputs(indicator_rows, output_dir / "indicator_outputs.csv")
    model_rows = _compute_model_rows(workflow, indicator_rows)
    unit_score_rows = _compute_unit_score_rows(indicator_rows, model_rows)
    models_out = write_model_summaries(model_rows, output_dir / "model_summaries.csv")
    scores_out = write_unit_scores(unit_score_rows, output_dir / "unit_scores.csv")

    written_files = [assignments_out, indicators_out, models_out, scores_out]
    if workflow.outputs.write_figures:
        overview_out = write_model_overview_figure(model_rows, output_dir / "model_overview.svg", title=workflow.metadata.title or workflow.workflow_id)
        written_files.append(overview_out)
        grouped_indicator_rows: dict[tuple[str, str, str], list[dict]] = {}
        grouped_model_rows: dict[tuple[str, str, str], list[dict]] = {}
        for row in indicator_rows:
            key = (str(row["domain_id"]), str(row["indicator_key"]), str(row.get("scale_basis") or "population"))
            grouped_indicator_rows.setdefault(key, []).append(row)
        for row in model_rows:
            key = (str(row["domain_id"]), str(row["indicator_key"]), str(row.get("scale_basis") or "population"))
            grouped_model_rows.setdefault(key, []).append(row)
        for key, rows in sorted(grouped_indicator_rows.items()):
            models = grouped_model_rows.get(key, [])
            if not models:
                continue
            best = max(models, key=lambda item: _to_attr_float(item.get("r2")))
            domain_id, indicator_key, scale_basis = key
            scatter_out = write_scaling_scatter_figure(
                rows,
                output_dir / f"scatter_{figure_slug(domain_id, indicator_key, scale_basis)}.svg",
                title=f"{domain_id} · {indicator_key}",
                x_key="scale_n",
                y_key="indicator_value",
                fit_alpha=_to_attr_float(best.get("alpha")),
                fit_beta=_to_attr_float(best.get("beta")),
                annotation=f"{best.get('fit_method', 'fit')}  β={_to_attr_float(best.get('beta')):.3f}  R²={_to_attr_float(best.get('r2')):.3f}",
            )
            written_files.append(scatter_out)
    metadata = {
        "workflow_id": workflow.workflow_id,
        "kind": workflow.kind,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "assigned_count": len(assignment_rows),
    }
    manifest_out = write_manifest(output_dir, files=written_files, metadata=metadata)
    written_files.append(manifest_out)
    bundle_out = None
    if workflow.outputs.write_bundle_zip:
        bundle_out = write_bundle(output_dir, files=written_files, bundle_name=f"{workflow.workflow_id}.zip")
        written_files.append(bundle_out)

    return {
        "ok": True,
        "workflow_id": workflow.workflow_id,
        "output_dir": str(output_dir),
        "files": [str(path) for path in written_files],
        "bundle_path": str(bundle_out) if bundle_out is not None else "",
        "assigned_count": len(assignment_rows),
        "indicator_rows": len(indicator_rows),
        "model_rows": len(model_rows),
        "unit_score_rows": len(unit_score_rows),
    }


def _run_polisplexity_matrix_parity(workflow: WorkflowSpec, *, manifest_path: Path | None = None) -> dict:
    if "summary" not in workflow.data_sources or "units_dir" not in workflow.data_sources:
        raise ValueError("polisplexity_matrix_parity requires data_sources.summary and data_sources.units_dir")

    output_dir = workflow.output_base_path(manifest_path or Path.cwd()) / workflow.workflow_id
    output_dir.mkdir(parents=True, exist_ok=True)

    summary_path = _resolve_source_path(workflow, key="summary", manifest_path=manifest_path)
    units_dir = _resolve_source_path(workflow, key="units_dir", manifest_path=manifest_path)
    baseline_rows = read_summary_csv(summary_path)
    compare_cfg = workflow.raw.get("compare") or {}
    tolerance = float(compare_cfg.get("numeric_tolerance") or 1e-9)

    computed_rows: list[dict] = []
    comparison_rows: list[dict] = []
    mismatch_count = 0

    for baseline in baseline_rows:
        n_field = str(baseline.get("n_field") or "").strip()
        level = str(baseline.get("level") or "").strip()
        filter_mode = str(baseline.get("filter_mode") or "").strip()
        fit_method = str(baseline.get("fit_method") or "").strip()
        units_file = units_dir / unit_export_filename(
            n_field=n_field,
            level=level,
            filter_mode=filter_mode,
            fit_method=fit_method,
        )
        computed = summarize_exported_units(
            units_file,
            n_field=n_field,
            level=level,
            filter_mode=filter_mode,
            fit_method=fit_method,
        )
        computed_rows.append(computed)
        key = f"{n_field}|{level}|{filter_mode}|{fit_method}"
        for column in SUMMARY_COLUMNS:
            expected_raw = baseline.get(column, "")
            actual_raw = computed.get(column, "")
            status = "match"
            abs_diff = ""
            try:
                expected_num = float(expected_raw)
                actual_num = float(actual_raw)
                abs_diff_value = abs(expected_num - actual_num)
                abs_diff = abs_diff_value
                if abs_diff_value > tolerance:
                    status = "mismatch"
            except Exception:
                if str(expected_raw) != str(actual_raw):
                    status = "mismatch"
            if status == "mismatch":
                mismatch_count += 1
            comparison_rows.append(
                {
                    "key": key,
                    "column": column,
                    "expected_value": expected_raw,
                    "actual_value": actual_raw,
                    "abs_diff": abs_diff,
                    "status": status,
                }
            )

    summary_out = write_summary_csv(computed_rows, output_dir / "summary.csv")
    comparison_out = write_parity_rows(comparison_rows, output_dir / "parity_comparison.csv")
    written_files = [summary_out, comparison_out]
    if workflow.outputs.write_figures:
        figure_out = write_model_overview_figure(computed_rows, output_dir / "summary_overview.svg", title=workflow.metadata.title or workflow.workflow_id)
        written_files.append(figure_out)
    metadata = {
        "workflow_id": workflow.workflow_id,
        "kind": workflow.kind,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mismatch_count": mismatch_count,
        "numeric_tolerance": tolerance,
    }
    manifest_out = write_manifest(output_dir, files=written_files, metadata=metadata)
    written_files.append(manifest_out)
    bundle_out = None
    if workflow.outputs.write_bundle_zip:
        bundle_out = write_bundle(output_dir, files=written_files, bundle_name=f"{workflow.workflow_id}.zip")
        written_files.append(bundle_out)
    return {
        "ok": True,
        "workflow_id": workflow.workflow_id,
        "output_dir": str(output_dir),
        "files": [str(path) for path in written_files],
        "bundle_path": str(bundle_out) if bundle_out is not None else "",
        "summary_rows": len(computed_rows),
        "comparison_rows": len(comparison_rows),
        "mismatch_count": mismatch_count,
    }


def _run_polisplexity_run_dir_parity(workflow: WorkflowSpec, *, manifest_path: Path | None = None) -> dict:
    if "run_dir" not in workflow.data_sources:
        raise ValueError("polisplexity_run_dir_parity requires data_sources.run_dir")

    run_dir = _resolve_source_path(workflow, key="run_dir", manifest_path=manifest_path)
    if not run_dir.exists() or not run_dir.is_dir():
        raise ValueError(f"run_dir does not exist or is not a directory: {run_dir}")

    units_dir = run_dir / "units" if (run_dir / "units").exists() else run_dir

    synthetic = type(workflow)(
        version=workflow.version,
        kind="polisplexity_matrix_parity",
        metadata=workflow.metadata,
        data_sources={
            "summary": type(next(iter(workflow.data_sources.values())))(
                source_type="csv_summary",
                path=str(run_dir / "summary.csv"),
            ),
            "units_dir": type(next(iter(workflow.data_sources.values())))(
                source_type="directory",
                path=str(units_dir),
            ),
        },
        outputs=workflow.outputs,
        raw=workflow.raw,
    )
    result = _run_polisplexity_matrix_parity(synthetic, manifest_path=manifest_path)

    output_dir = Path(result["output_dir"])
    copied_files: list[Path] = []
    for filename in ("request_matrix.json", "details.json", "report.md"):
        src = run_dir / filename
        if not src.exists():
            continue
        dst = output_dir / filename
        dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
        copied_files.append(dst)

    if copied_files:
        metadata = {
            "workflow_id": workflow.workflow_id,
            "kind": workflow.kind,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        manifest_out = write_manifest(output_dir, files=[Path(p) for p in result["files"] if Path(p).exists()] + copied_files, metadata=metadata)
        result["files"] = [path for path in result["files"] if not path.endswith("artifact_manifest.json")]
        result["files"].append(str(manifest_out))
        if workflow.outputs.write_bundle_zip:
            bundle_out = write_bundle(output_dir, files=[Path(p) for p in result["files"] if Path(p).exists()], bundle_name=f"{workflow.workflow_id}.zip")
            result["files"] = [path for path in result["files"] if not path.endswith(".zip")]
            result["files"].append(str(bundle_out))
            result["bundle_path"] = str(bundle_out)
    return result

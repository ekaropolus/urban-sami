from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from urban_sami.geometry.base import GeometryDomain, UnitRecord
from urban_sami.indicators.denue import DenueUnitMetrics


@dataclass(frozen=True)
class DenueObservation:
    unit_id: str
    domain_id: str
    scian_code: str = ""
    per_ocu: str = ""


def seed_unit_metrics(*, domain: GeometryDomain, units: list[UnitRecord]) -> dict[str, DenueUnitMetrics]:
    metrics: dict[str, DenueUnitMetrics] = {}
    for unit in units:
        metrics[unit.unit_id] = DenueUnitMetrics(
            domain_id=domain.domain_id,
            unit_id=unit.unit_id,
            population=float(unit.attrs.get("population") or 0.0),
            households=float(unit.attrs.get("households") or 0.0),
            area_km2=float(unit.attrs.get("area_km2") or 0.0),
            attrs=dict(unit.attrs),
        )
    return metrics


def attach_denue_observations(
    metrics_by_unit: dict[str, DenueUnitMetrics],
    observations: list[DenueObservation],
) -> dict[str, DenueUnitMetrics]:
    from urban_sami.indicators.denue import accumulate_denue_row

    for obs in observations:
        bucket = metrics_by_unit.get(obs.unit_id)
        if bucket is None:
            continue
        accumulate_denue_row(bucket, scian_code=obs.scian_code, per_ocu=obs.per_ocu)
    return metrics_by_unit


def contexts_by_unit(metrics_by_unit: dict[str, DenueUnitMetrics]) -> dict[str, dict[str, Any]]:
    return {unit_id: metrics.to_context() for unit_id, metrics in metrics_by_unit.items()}


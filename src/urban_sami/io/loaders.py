from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from urban_sami.io.csvio import read_csv_rows
from urban_sami.io.normalize import pick


@dataclass(frozen=True)
class GenericUnitRow:
    domain_id: str
    unit_id: str
    unit_label: str
    parent_id: str
    population: float
    households: float
    area_km2: float


@dataclass(frozen=True)
class GenericObservationRow:
    domain_id: str
    unit_id: str
    scian_code: str
    per_ocu: str
    obs_id: str = ""
    lon: float = 0.0
    lat: float = 0.0


def _to_float(value: str | None, default: float = 0.0) -> float:
    try:
        raw = str(value or "").strip()
        if "," in raw and "." not in raw:
            raw = raw.replace(",", ".")
        return float(raw)
    except Exception:
        return float(default)


def load_units(path: str | Path, *, source_type: str) -> list[GenericUnitRow]:
    rows = read_csv_rows(path)
    out: list[GenericUnitRow] = []
    source_type = str(source_type or "").strip().lower()
    if source_type not in {"csv_units", "generic_units_csv"}:
        raise ValueError(f"unsupported unit source_type: {source_type}")
    for row in rows:
        domain_id = pick(row, ("domain_id", "domain", "geometry_domain"))
        unit_id = pick(row, ("unit_id", "unit", "cell_id", "zone_id"))
        if not domain_id or not unit_id:
            continue
        out.append(
            GenericUnitRow(
                domain_id=domain_id,
                unit_id=unit_id,
                unit_label=pick(row, ("unit_label", "label", "name"), ""),
                parent_id=pick(row, ("parent_id", "parent", "city_id", "admin2_code"), ""),
                population=_to_float(pick(row, ("population", "pop", "pobtot"), "0")),
                households=_to_float(pick(row, ("households", "hogares"), "0")),
                area_km2=_to_float(pick(row, ("area_km2", "area", "surface_km2"), "0")),
            )
        )
    return out


def load_observations(path: str | Path, *, source_type: str) -> list[GenericObservationRow]:
    rows = read_csv_rows(path)
    out: list[GenericObservationRow] = []
    source_type = str(source_type or "").strip().lower()
    if source_type not in {"csv_observations", "generic_observations_csv", "denue_csv", "denue_points_csv"}:
        raise ValueError(f"unsupported observation source_type: {source_type}")
    for index, row in enumerate(rows, start=1):
        domain_id = pick(row, ("domain_id", "domain", "geometry_domain"))
        unit_id = pick(row, ("unit_id", "unit", "cell_id", "zone_id"))
        obs_id = pick(row, ("obs_id", "id", "denue_id"), f"obs_{index}")
        lon = _to_float(pick(row, ("lon", "longitude", "longitud"), "0"))
        lat = _to_float(pick(row, ("lat", "latitude", "latitud"), "0"))
        if source_type != "denue_points_csv" and (not domain_id or not unit_id):
            continue
        out.append(
            GenericObservationRow(
                domain_id=domain_id,
                unit_id=unit_id,
                scian_code=pick(row, ("scian_code", "codigo_act", "codigo", "scian"), ""),
                per_ocu=pick(row, ("per_ocu", "personal_ocupado", "tam_estab"), ""),
                obs_id=obs_id,
                lon=lon,
                lat=lat,
            )
        )
    return out

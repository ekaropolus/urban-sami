from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DenueBulkRow:
    source_file: str
    denue_id: str
    country_code: str
    state_code: str
    state_name: str
    city_code: str
    city_name: str
    ageb_code: str
    manzana_code: str
    scian_code: str
    per_ocu: str
    latitude: float | None
    longitude: float | None


def _to_float(text: str | None) -> float | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _strip(value: str | None) -> str:
    return str(value or "").strip()


def iter_denue_bulk_rows(path: str | Path) -> list[DenueBulkRow]:
    source_path = Path(path)
    rows: list[DenueBulkRow] = []
    with source_path.open("r", encoding="latin-1", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            state_code = _strip(row.get("cve_ent"))
            city_mun_code = _strip(row.get("cve_mun"))
            city_code = f"{state_code}{city_mun_code}" if state_code and city_mun_code else ""
            rows.append(
                DenueBulkRow(
                    source_file=source_path.name,
                    denue_id=_strip(row.get("id")),
                    country_code="MX",
                    state_code=state_code,
                    state_name=_strip(row.get("entidad")),
                    city_code=city_code,
                    city_name=_strip(row.get("municipio")),
                    ageb_code=_strip(row.get("ageb")),
                    manzana_code=_strip(row.get("manzana")),
                    scian_code=_strip(row.get("codigo_act")),
                    per_ocu=_strip(row.get("per_ocu")),
                    latitude=_to_float(row.get("latitud")),
                    longitude=_to_float(row.get("longitud")),
                )
            )
    return rows

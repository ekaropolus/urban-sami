from __future__ import annotations

from dataclasses import dataclass
import json
import urllib.request


BASE_MUNICIPALITY_URL = "https://gaia.inegi.org.mx/wscatgeo/v2/mgem/{state_code}"


@dataclass(frozen=True)
class MunicipalityPopulationRow:
    city_code: str
    state_code: str
    city_name: str
    seat_code: str
    seat_name: str
    population_total: int
    population_female: int
    population_male: int
    households: int


def _parse_int(text: str | None) -> int:
    raw = str(text or "").replace(" ", "").replace(",", "").strip()
    return int(raw or "0")


def parse_municipality_payload(payload: str) -> list[MunicipalityPopulationRow]:
    doc = json.loads(payload)
    rows: list[MunicipalityPopulationRow] = []
    for item in doc.get("datos", []):
        rows.append(
            MunicipalityPopulationRow(
                city_code=str(item.get("cvegeo", "")).strip(),
                state_code=str(item.get("cve_ent", "")).strip(),
                city_name=str(item.get("nomgeo", "")).strip(),
                seat_code=str(item.get("cve_cab", "")).strip(),
                seat_name=str(item.get("nom_cab", "")).strip(),
                population_total=_parse_int(item.get("pob_total")),
                population_female=_parse_int(item.get("pob_femenina")),
                population_male=_parse_int(item.get("pob_masculina")),
                households=_parse_int(item.get("total_viviendas_habitadas")),
            )
        )
    return rows


def fetch_municipalities_for_state(state_code: str, *, timeout: int = 30) -> list[MunicipalityPopulationRow]:
    code = f"{int(state_code):02d}"
    url = BASE_MUNICIPALITY_URL.format(state_code=code)
    with urllib.request.urlopen(url, timeout=timeout) as response:
        payload = response.read().decode("utf-8")
    return parse_municipality_payload(payload)


def fetch_all_municipalities(*, timeout: int = 30) -> list[MunicipalityPopulationRow]:
    rows: list[MunicipalityPopulationRow] = []
    for idx in range(1, 33):
        rows.extend(fetch_municipalities_for_state(f"{idx:02d}", timeout=timeout))
    return rows

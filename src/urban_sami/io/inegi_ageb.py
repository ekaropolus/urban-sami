from __future__ import annotations

from dataclasses import dataclass
import json
import urllib.request


BASE_AGEBU_URL = "https://gaia.inegi.org.mx/wscatgeo/v2/geo/agebu/{state_code}/{city_code}"


@dataclass(frozen=True)
class AgebFeatureRow:
    unit_code: str
    state_code: str
    state_name: str
    city_code: str
    city_name: str
    locality_code: str
    ageb_code: str
    population_total: int
    population_female: int
    population_male: int
    households: int
    geometry: dict


def _parse_int(text: str | None) -> int:
    raw = str(text or "").replace(" ", "").replace(",", "").strip()
    return int(raw or "0")


def parse_ageb_geojson(payload: str) -> list[AgebFeatureRow]:
    doc = json.loads(payload)
    rows: list[AgebFeatureRow] = []
    for feature in doc.get("features", []):
        props = feature.get("properties", {}) or {}
        rows.append(
            AgebFeatureRow(
                unit_code=str(props.get("cvegeo", "")).strip(),
                state_code=str(props.get("cve_ent", "")).strip(),
                state_name=str(props.get("nom_ent", "")).strip(),
                city_code=f"{str(props.get('cve_ent', '')).strip()}{str(props.get('cve_mun', '')).strip()}",
                city_name=str(props.get("nom_mun", "")).strip(),
                locality_code=str(props.get("cve_loc", "")).strip(),
                ageb_code=str(props.get("cve_ageb", "")).strip(),
                population_total=_parse_int(props.get("pob_total")),
                population_female=_parse_int(props.get("pob_femenina")),
                population_male=_parse_int(props.get("pob_masculina")),
                households=_parse_int(props.get("total_viviendas_habitadas")),
                geometry=feature.get("geometry", {}) or {},
            )
        )
    return rows


def fetch_ageb_urban_geojson(state_code: str, city_code: str, *, timeout: int = 60) -> dict:
    ent = f"{int(state_code):02d}"
    mun = f"{int(city_code):03d}"
    url = BASE_AGEBU_URL.format(state_code=ent, city_code=mun)
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


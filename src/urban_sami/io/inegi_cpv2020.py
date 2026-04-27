from __future__ import annotations

from dataclasses import dataclass
import urllib.request
import xml.etree.ElementTree as ET


BASE_URL = "https://www.inegi.org.mx/widgets/cpv/2020/recursos/INEGIWgEst_cpv2020_{code}.xml"


@dataclass(frozen=True)
class StatePopulationRow:
    state_code: str
    state_name: str
    population_total: int
    population_male: int
    population_female: int


def _parse_int(text: str | None) -> int:
    raw = str(text or "").replace(" ", "").replace(",", "")
    return int(raw or "0")


def parse_state_population_xml(xml_text: str, *, state_code: str) -> StatePopulationRow:
    root = ET.fromstring(xml_text)
    if not len(root):
        raise ValueError("INEGI CPV 2020 XML payload has no rows")
    attrs = root[0].attrib
    return StatePopulationRow(
        state_code=state_code,
        state_name=str(attrs.get("nombre", "")).strip(),
        population_total=_parse_int(attrs.get("Total")),
        population_male=_parse_int(attrs.get("Hombres")),
        population_female=_parse_int(attrs.get("Mujeres")),
    )


def fetch_state_population_row(state_code: str, *, timeout: int = 30) -> StatePopulationRow:
    code = f"{int(state_code):02d}" if state_code != "00" else "00"
    url = BASE_URL.format(code=code)
    with urllib.request.urlopen(url, timeout=timeout) as response:
        xml_text = response.read().decode("utf-8")
    return parse_state_population_xml(xml_text, state_code=code)


def fetch_all_state_population_rows(*, include_national: bool = False, timeout: int = 30) -> list[StatePopulationRow]:
    codes = ["00"] if include_national else []
    codes.extend(f"{idx:02d}" for idx in range(1, 33))
    return [fetch_state_population_row(code, timeout=timeout) for code in codes]

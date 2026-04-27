#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from urban_sami.io.inegi_ageb import fetch_ageb_urban_geojson


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch official INEGI AGEB urban GeoJSON for selected municipalities")
    parser.add_argument("--city-code", action="append", dest="city_codes", default=[], help="5-digit municipality code, repeatable")
    parser.add_argument("--city-csv", type=Path, default=None, help="Optional CSV with a city_code column")
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw/ageb_u"))
    args = parser.parse_args()

    city_codes = [str(code).strip() for code in args.city_codes if str(code).strip()]
    if args.city_csv:
        with args.city_csv.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                code = str(row.get("city_code", "")).strip()
                if code:
                    city_codes.append(code)
    city_codes = sorted(set(city_codes))
    if not city_codes:
        raise SystemExit("no city codes supplied")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest = []
    for city_code in city_codes:
        state_code = city_code[:2]
        mun_code = city_code[2:]
        payload = fetch_ageb_urban_geojson(state_code, mun_code)
        out = args.output_dir / f"{city_code}.geojson"
        out.write_text(json.dumps(payload), encoding="utf-8")
        manifest.append({"city_code": city_code, "path": str(out.resolve()), "features": len(payload.get("features", []))})

    manifest_path = args.output_dir / "manifest.csv"
    with manifest_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["city_code", "path", "features"])
        writer.writeheader()
        writer.writerows(manifest)
    print(manifest_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

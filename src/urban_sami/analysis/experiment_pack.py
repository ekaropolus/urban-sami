from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass
from pathlib import Path

from urban_sami.io.csvio import read_csv_rows, write_csv_rows
from urban_sami.modeling import fit_by_name


@dataclass(frozen=True)
class UnitDatum:
    level: str
    fit_method: str
    unit_code: str
    unit_label: str
    y: float
    n: float

    @property
    def city_code(self) -> str:
        return str(self.unit_code.split(":", 1)[0]).strip()


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _mean(values: list[float]) -> float:
    return sum(values) / float(len(values)) if values else 0.0


def _variance(values: list[float], *, ddof: int = 0) -> float:
    if len(values) <= ddof:
        return 0.0
    mu = _mean(values)
    return sum((value - mu) ** 2 for value in values) / float(len(values) - ddof)


def _stddev(values: list[float], *, ddof: int = 0) -> float:
    return math.sqrt(max(0.0, _variance(values, ddof=ddof)))


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return ordered[0]
    idx = (len(ordered) - 1) * float(p)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return ordered[lo]
    frac = idx - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def load_units_csv(path: str | Path, *, level: str, fit_method: str) -> list[UnitDatum]:
    out: list[UnitDatum] = []
    for row in read_csv_rows(path):
        y = _to_float(row.get("y"))
        n = _to_float(row.get("n"))
        if y <= 0 or n <= 0:
            continue
        out.append(
            UnitDatum(
                level=level,
                fit_method=fit_method,
                unit_code=str(row.get("unit_code") or "").strip(),
                unit_label=str(row.get("unit_label") or "").strip(),
                y=y,
                n=n,
            )
        )
    return out


def fit_metrics(rows: list[UnitDatum], *, fit_method: str = "ols") -> dict[str, float | int | str]:
    y = [row.y for row in rows]
    n = [row.n for row in rows]
    result = fit_by_name(y, n, fit_method)
    yhat = [math.exp(result.alpha + (result.beta * math.log(value))) for value in n]
    residual = [math.log(yi) - math.log(max(1e-9, yhi)) for yi, yhi in zip(y, yhat)]
    return {
        "fit_method": fit_method,
        "units": len(rows),
        "alpha": float(result.alpha),
        "beta": float(result.beta),
        "r2": float(result.r2),
        "resid_std": float(_stddev(residual, ddof=1) if len(residual) > 1 else 0.0),
        "y_min": min(y) if y else 0.0,
        "y_p95": _percentile(y, 0.95),
        "y_max": max(y) if y else 0.0,
        "n_min": min(n) if n else 0.0,
        "n_p05": _percentile(n, 0.05),
        "n_p95": _percentile(n, 0.95),
        "n_max": max(n) if n else 0.0,
    }


def aggregate_to_city(rows: list[UnitDatum], *, level_name: str) -> list[UnitDatum]:
    grouped: dict[str, dict[str, float | str]] = {}
    for row in rows:
        node = grouped.setdefault(
            row.city_code,
            {
                "unit_code": row.city_code,
                "unit_label": row.city_code,
                "y": 0.0,
                "n": 0.0,
            },
        )
        node["y"] = float(node["y"]) + row.y
        node["n"] = float(node["n"]) + row.n
    return [
        UnitDatum(
            level=level_name,
            fit_method="ols",
            unit_code=str(node["unit_code"]),
            unit_label=str(node["unit_label"]),
            y=float(node["y"]),
            n=float(node["n"]),
        )
        for node in grouped.values()
        if float(node["y"]) > 0 and float(node["n"]) > 0
    ]


def synthetic_bundle_rows(
    rows: list[UnitDatum],
    *,
    bundle_size: int,
    seed: int,
    level_name: str,
) -> list[UnitDatum]:
    rng = random.Random(seed)
    grouped_by_city: dict[str, list[UnitDatum]] = {}
    for row in rows:
        grouped_by_city.setdefault(row.city_code, []).append(row)
    out: list[UnitDatum] = []
    for city_code, city_rows in sorted(grouped_by_city.items()):
        local = list(city_rows)
        rng.shuffle(local)
        for idx in range(0, len(local), bundle_size):
            chunk = local[idx : idx + bundle_size]
            if not chunk:
                continue
            out.append(
                UnitDatum(
                    level=level_name,
                    fit_method="ols",
                    unit_code=f"{city_code}:bundle_{seed}_{idx // bundle_size:05d}",
                    unit_label=f"{city_code} synthetic bundle {idx // bundle_size + 1}",
                    y=sum(item.y for item in chunk),
                    n=sum(item.n for item in chunk),
                )
            )
    return out


def shuffle_y_within_city(rows: list[UnitDatum], *, seed: int, level_name: str) -> list[UnitDatum]:
    rng = random.Random(seed)
    grouped_by_city: dict[str, list[UnitDatum]] = {}
    for row in rows:
        grouped_by_city.setdefault(row.city_code, []).append(row)
    out: list[UnitDatum] = []
    for city_code, city_rows in sorted(grouped_by_city.items()):
        y_values = [row.y for row in city_rows]
        rng.shuffle(y_values)
        for row, y_value in zip(city_rows, y_values):
            out.append(
                UnitDatum(
                    level=level_name,
                    fit_method="ols",
                    unit_code=row.unit_code,
                    unit_label=row.unit_label,
                    y=float(y_value),
                    n=row.n,
                )
            )
    return out


def fit_per_city(rows: list[UnitDatum], *, fit_method: str = "ols", min_units: int = 20) -> list[dict[str, float | str | int]]:
    grouped: dict[str, list[UnitDatum]] = {}
    for row in rows:
        grouped.setdefault(row.city_code, []).append(row)
    out: list[dict[str, float | str | int]] = []
    for city_code, city_rows in sorted(grouped.items()):
        if len(city_rows) < min_units:
            continue
        metrics = fit_metrics(city_rows, fit_method=fit_method)
        metrics["city_code"] = city_code
        out.append(metrics)
    return out


def distribution_audit(rows: list[UnitDatum], *, level: str) -> dict[str, float | int | str]:
    y = [row.y for row in rows]
    n = [row.n for row in rows]
    if not rows:
        return {"level": level}
    y_sorted = sorted(y, reverse=True)
    n_top1 = max(1, int(round(0.01 * len(y_sorted))))
    return {
        "level": level,
        "units": len(rows),
        "cities": len({row.city_code for row in rows}),
        "share_y_eq_1": sum(1 for value in y if value == 1.0) / float(len(y)),
        "share_y_le_3": sum(1 for value in y if value <= 3.0) / float(len(y)),
        "share_n_le_10": sum(1 for value in n if value <= 10.0) / float(len(n)),
        "share_n_le_30": sum(1 for value in n if value <= 30.0) / float(len(n)),
        "y_mean": _mean(y),
        "y_std": _stddev(y, ddof=1) if len(y) > 1 else 0.0,
        "n_mean": _mean(n),
        "n_std": _stddev(n, ddof=1) if len(n) > 1 else 0.0,
        "top_1pct_y_share": sum(y_sorted[:n_top1]) / float(sum(y) or 1.0),
    }


def write_markdown(path: str | Path, text: str) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")
    return target


def write_json(path: str | Path, payload: dict) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return target


def write_rows(path: str | Path, rows: list[dict]) -> Path:
    if not rows:
        return write_csv_rows(path, fieldnames=["empty"], rows=[{"empty": ""}])
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    return write_csv_rows(path, fieldnames=fieldnames, rows=rows)

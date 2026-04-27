from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from urban_sami.indicators.registry import IndicatorRegistry, IndicatorSpec


SIZE_MICRO = "micro"
SIZE_SMALL = "small"
SIZE_MEDIUM = "medium"
SIZE_LARGE = "large"
SIZE_UNKNOWN = "unknown"


@dataclass
class DenueUnitMetrics:
    domain_id: str
    unit_id: str
    est_count: int = 0
    population: float = 0.0
    households: float = 0.0
    area_km2: float = 0.0
    sector_counts: Counter = field(default_factory=Counter)
    size_counts: Counter = field(default_factory=Counter)
    sector_size_counts: Counter = field(default_factory=Counter)
    attrs: dict[str, Any] = field(default_factory=dict)

    def to_context(self) -> dict[str, Any]:
        return {
            "domain_id": self.domain_id,
            "unit_id": self.unit_id,
            "est_count": float(self.est_count),
            "population": float(self.population),
            "households": float(self.households),
            "area_km2": float(self.area_km2),
            "sector_counts": dict(self.sector_counts),
            "size_counts": dict(self.size_counts),
            "sector_size_counts": {f"{sector}|{size}": count for (sector, size), count in self.sector_size_counts.items()},
            "attrs": dict(self.attrs),
        }


def sector_prefix(scian_code: str) -> str:
    digits = "".join(ch for ch in str(scian_code or "") if ch.isdigit())
    return digits[:2] if len(digits) >= 2 else ""


def size_class_from_per_ocu(raw: str | None) -> str:
    text = (raw or "").strip().lower()
    if not text:
        return SIZE_UNKNOWN
    text = (
        text.replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
    )
    digits = [int(x) for x in re.findall(r"\d+", text)]
    compact = " ".join(text.split())
    if "251 y mas" in compact or "251+" in compact or "mas de 250" in compact:
        return SIZE_LARGE
    if any(token in compact for token in ("101 a 250", "101-250", "51 a 100", "51-100")):
        return SIZE_MEDIUM
    if any(token in compact for token in ("31 a 50", "31-50", "11 a 30", "11-30")):
        return SIZE_SMALL
    if any(token in compact for token in ("0 a 5", "1 a 5", "0-5", "1-5", "6 a 10", "6-10", "hasta 10")):
        return SIZE_MICRO
    if digits:
        hi = max(digits)
        lo = min(digits)
        if hi <= 10:
            return SIZE_MICRO
        if hi <= 50:
            return SIZE_SMALL
        if hi <= 250:
            return SIZE_MEDIUM
        if lo >= 251 or hi > 250:
            return SIZE_LARGE
    return SIZE_UNKNOWN


def accumulate_denue_row(bucket: DenueUnitMetrics, *, scian_code: str = "", per_ocu: str = "") -> DenueUnitMetrics:
    bucket.est_count += 1
    sector = sector_prefix(scian_code)
    if sector:
        bucket.sector_counts[sector] += 1
    size = size_class_from_per_ocu(per_ocu)
    bucket.size_counts[size] += 1
    if sector:
        bucket.sector_size_counts[(sector, size)] += 1
    return bucket


def _proxy_defaults(params: dict[str, Any] | None) -> dict[str, Any]:
    base = {
        "default_daily_mxn": 8000.0,
        "sector_daily_mxn": {"46": 7000.0, "54": 12000.0, "72": 10000.0, "default": 8000.0},
        "size_multiplier": {"micro": 1.0, "small": 2.0, "medium": 4.0, "large": 8.0, "default": 1.0},
    }
    params = dict(params or {})
    if isinstance(params.get("sector_daily_mxn"), dict):
        merged = dict(base["sector_daily_mxn"])
        merged.update(params["sector_daily_mxn"])
        base["sector_daily_mxn"] = merged
    if isinstance(params.get("size_multiplier"), dict):
        merged = dict(base["size_multiplier"])
        merged.update(params["size_multiplier"])
        base["size_multiplier"] = merged
    if "default_daily_mxn" in params:
        base["default_daily_mxn"] = float(params["default_daily_mxn"])
    return base


def _compute_denue_est_count(ctx: dict[str, Any], params: dict[str, Any] | None) -> float | None:
    total = float(ctx.get("est_count") or 0.0)
    return total if total > 0 else None


def _compute_denue_est_per_1k_pop(ctx: dict[str, Any], params: dict[str, Any] | None) -> float | None:
    total = float(ctx.get("est_count") or 0.0)
    population = float(ctx.get("population") or 0.0)
    if total <= 0 or population <= 0:
        return None
    return 1000.0 * total / population


def _compute_denue_est_density_km2(ctx: dict[str, Any], params: dict[str, Any] | None) -> float | None:
    total = float(ctx.get("est_count") or 0.0)
    area_km2 = float(ctx.get("area_km2") or 0.0)
    if total <= 0 or area_km2 <= 0:
        return None
    return total / area_km2


def _compute_denue_sector_hhi(ctx: dict[str, Any], params: dict[str, Any] | None) -> float | None:
    total = float(ctx.get("est_count") or 0.0)
    if total <= 0:
        return None
    sector_counts = dict(ctx.get("sector_counts") or {})
    return sum((float(count) / total) ** 2 for count in sector_counts.values())


def _compute_denue_sector_entropy_norm(ctx: dict[str, Any], params: dict[str, Any] | None) -> float | None:
    total = float(ctx.get("est_count") or 0.0)
    if total <= 0:
        return None
    sector_counts = dict(ctx.get("sector_counts") or {})
    probs = [float(count) / total for count in sector_counts.values() if float(count) > 0]
    if not probs:
        return 0.0
    entropy = -sum(p * math.log(p) for p in probs if p > 0)
    if len(probs) <= 1:
        return 0.0
    return entropy / math.log(len(probs))


def _compute_denue_sector_share_46(ctx: dict[str, Any], params: dict[str, Any] | None) -> float | None:
    return _compute_sector_share(ctx, "46")


def _compute_denue_sector_share_54(ctx: dict[str, Any], params: dict[str, Any] | None) -> float | None:
    return _compute_sector_share(ctx, "54")


def _compute_denue_sector_share_72(ctx: dict[str, Any], params: dict[str, Any] | None) -> float | None:
    return _compute_sector_share(ctx, "72")


def _compute_sector_share(ctx: dict[str, Any], sector: str) -> float | None:
    total = float(ctx.get("est_count") or 0.0)
    if total <= 0:
        return None
    sector_counts = dict(ctx.get("sector_counts") or {})
    return float(sector_counts.get(sector, 0)) / total


def _compute_denue_revenue_proxy_daily_mxn(ctx: dict[str, Any], params: dict[str, Any] | None) -> float | None:
    total = float(ctx.get("est_count") or 0.0)
    if total <= 0:
        return None
    cfg = _proxy_defaults(params)
    sector_daily = cfg["sector_daily_mxn"]
    size_mult = cfg["size_multiplier"]
    default_daily = float(cfg["default_daily_mxn"])
    default_sector_daily = float(sector_daily.get("default", default_daily))
    default_size_mult = float(size_mult.get("default", 1.0))
    raw = dict(ctx.get("sector_size_counts") or {})
    value = 0.0
    for combined_key, count in raw.items():
        sector, size = str(combined_key).split("|", 1)
        base = float(sector_daily.get(sector, default_sector_daily))
        mult = float(size_mult.get(size, default_size_mult))
        value += float(count) * base * mult
    return value if value > 0 else None


def default_indicator_registry() -> IndicatorRegistry:
    registry = IndicatorRegistry()
    registry.register(
        IndicatorSpec(
            key="denue_est_count",
            family="structural_count",
            label="DENUE Establishment Count",
            formula="E_u",
            compute=_compute_denue_est_count,
        )
    )
    registry.register(
        IndicatorSpec(
            key="denue_est_per_1k_pop",
            family="intensity",
            label="DENUE Establishments per 1k Population",
            formula="1000 * E_u / P_u",
            compute=_compute_denue_est_per_1k_pop,
        )
    )
    registry.register(
        IndicatorSpec(
            key="denue_est_density_km2",
            family="density",
            label="DENUE Establishment Density (km2)",
            formula="E_u / A_u",
            compute=_compute_denue_est_density_km2,
        )
    )
    registry.register(
        IndicatorSpec(
            key="denue_sector_hhi",
            family="compositional",
            label="DENUE Sector Concentration (HHI)",
            formula="sum_s((E_{u,s}/E_u)^2)",
            compute=_compute_denue_sector_hhi,
        )
    )
    registry.register(
        IndicatorSpec(
            key="denue_sector_entropy_norm",
            family="compositional",
            label="DENUE Sector Diversity (Normalized Entropy)",
            formula="-sum_s(p_{u,s} * ln(p_{u,s})) / ln(S_u)",
            compute=_compute_denue_sector_entropy_norm,
        )
    )
    registry.register(
        IndicatorSpec(
            key="denue_sector_share_46",
            family="compositional",
            label="DENUE Retail Share (SCIAN 46)",
            formula="E_{u,46} / E_u",
            compute=_compute_denue_sector_share_46,
        )
    )
    registry.register(
        IndicatorSpec(
            key="denue_sector_share_54",
            family="compositional",
            label="DENUE Professional Services Share (SCIAN 54)",
            formula="E_{u,54} / E_u",
            compute=_compute_denue_sector_share_54,
        )
    )
    registry.register(
        IndicatorSpec(
            key="denue_sector_share_72",
            family="compositional",
            label="DENUE Accommodation & Food Share (SCIAN 72)",
            formula="E_{u,72} / E_u",
            compute=_compute_denue_sector_share_72,
        )
    )
    registry.register(
        IndicatorSpec(
            key="denue_revenue_proxy_daily_mxn",
            family="proxy_economic",
            label="DENUE Revenue Proxy (MXN/day)",
            formula="sum_{s,z}(E_{u,s,z} * w_{s,z})",
            assumption_level="proxy",
            compute=_compute_denue_revenue_proxy_daily_mxn,
        )
    )
    return registry

from __future__ import annotations

from urban_sami.aggregation import DenueObservation, attach_denue_observations, contexts_by_unit, seed_unit_metrics
from urban_sami.geometry import GeometryDomain, UnitRecord, build_hex_domain, make_unit_id
from urban_sami.indicators import default_indicator_registry, size_class_from_per_ocu


def test_size_class_normalization_covers_denue_bands():
    assert size_class_from_per_ocu("0 a 5 personas") == "micro"
    assert size_class_from_per_ocu("31 a 50 personas") == "small"
    assert size_class_from_per_ocu("101 a 250 personas") == "medium"
    assert size_class_from_per_ocu("251 y mas personas") == "large"


def test_generic_domain_and_indicator_computation_work_for_hex_units():
    domain = build_hex_domain(domain_id="hex_8", resolution=8)
    unit_a = UnitRecord(domain_id=domain.domain_id, unit_id=make_unit_id(domain.domain_id, "abc"), attrs={"population": 500.0, "area_km2": 0.5})
    unit_b = UnitRecord(domain_id=domain.domain_id, unit_id=make_unit_id(domain.domain_id, "def"), attrs={"population": 1000.0, "area_km2": 2.0})
    metrics = seed_unit_metrics(domain=domain, units=[unit_a, unit_b])
    attach_denue_observations(
        metrics,
        [
            DenueObservation(unit_id=unit_a.unit_id, domain_id=domain.domain_id, scian_code="461110", per_ocu="0 a 5 personas"),
            DenueObservation(unit_id=unit_a.unit_id, domain_id=domain.domain_id, scian_code="722515", per_ocu="11 a 30 personas"),
            DenueObservation(unit_id=unit_b.unit_id, domain_id=domain.domain_id, scian_code="541430", per_ocu="251 y mas personas"),
        ],
    )
    contexts = contexts_by_unit(metrics)
    registry = default_indicator_registry()

    est_count_a = registry.get("denue_est_count").compute(contexts[unit_a.unit_id], None)
    per_1k_a = registry.get("denue_est_per_1k_pop").compute(contexts[unit_a.unit_id], None)
    density_a = registry.get("denue_est_density_km2").compute(contexts[unit_a.unit_id], None)
    retail_share_a = registry.get("denue_sector_share_46").compute(contexts[unit_a.unit_id], None)

    assert est_count_a == 2.0
    assert per_1k_a == 4.0
    assert density_a == 4.0
    assert retail_share_a == 0.5


def test_revenue_proxy_uses_sector_and_size_weights():
    domain = GeometryDomain(domain_id="admin_ageb", domain_type="admin", level="ageb_u")
    unit = UnitRecord(domain_id=domain.domain_id, unit_id="admin_ageb:001", attrs={"population": 250.0})
    metrics = seed_unit_metrics(domain=domain, units=[unit])
    attach_denue_observations(
        metrics,
        [
            DenueObservation(unit_id=unit.unit_id, domain_id=domain.domain_id, scian_code="461110", per_ocu="0 a 5 personas"),
            DenueObservation(unit_id=unit.unit_id, domain_id=domain.domain_id, scian_code="541430", per_ocu="251 y mas personas"),
        ],
    )
    ctx = contexts_by_unit(metrics)[unit.unit_id]
    proxy = default_indicator_registry().get("denue_revenue_proxy_daily_mxn").compute(
        ctx,
        {
            "sector_daily_mxn": {"46": 100.0, "54": 200.0, "default": 50.0},
            "size_multiplier": {"micro": 1.0, "large": 10.0, "default": 1.0},
            "default_daily_mxn": 50.0,
        },
    )
    assert proxy == 2100.0


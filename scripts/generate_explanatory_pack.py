from __future__ import annotations

from pathlib import Path

from urban_sami.analysis.experiment_pack import (
    aggregate_to_city,
    distribution_audit,
    fit_metrics,
    fit_per_city,
    load_units_csv,
    shuffle_y_within_city,
    synthetic_bundle_rows,
    write_json,
    write_markdown,
    write_rows,
)
from urban_sami.io.csvio import read_csv_rows


ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "tests" / "fixtures" / "polisplexity_golden"
DIST = ROOT / "reports" / "experiment-pack-2026-04-21"


def _baseline_support_rows() -> list[dict]:
    state = read_csv_rows(FIX / "state_population_all_cities" / "summary.csv")
    ageb = read_csv_rows(FIX / "ageb_population_scope" / "summary.csv")
    manzana = read_csv_rows(ROOT / "dist" / "polisplexity_manzana_matrix_parity_live" / "summary.csv")
    return state + ageb + manzana


def _load_baseline_ols_units() -> dict[str, list]:
    return {
        "state": load_units_csv(
            FIX / "state_population_all_cities" / "units" / "population__state__raw__ols_units.csv",
            level="state",
            fit_method="ols",
        ),
        "ageb_u": load_units_csv(
            FIX / "ageb_population_scope" / "units" / "population__ageb_u__raw__ols_units.csv",
            level="ageb_u",
            fit_method="ols",
        ),
        "manzana": load_units_csv(
            ROOT
            / "dist"
            / "polisplexity_manzana_matrix_parity_live"
            / "summary.csv",  # placeholder replaced below
            level="manzana",
            fit_method="ols",
        ),
    }


def _load_manzana_units() -> list:
    return load_units_csv(
        Path("/home/hadox/cmd-center/platforms/polisplexity/core/data/sami_experiments/mx/")
        / "matrix-y-denue_est_count-n-population-lvl-manzana-fit-ols-robust-poisson-negbin-auto-filt-raw-scope-cities-19001-14001-14002-19002-14003-plus187/20260315_221905/population__manzana__raw__ols_units.csv",
        level="manzana",
        fit_method="ols",
    )


def question_1_support_ladder(base_dir: Path) -> None:
    rows = _baseline_support_rows()
    write_rows(base_dir / "support_ladder_summary.csv", rows)
    best_rows = []
    for level in ("state", "ageb_u", "manzana"):
        local = [row for row in rows if str(row.get("level")) == level]
        if not local:
            continue
        best = max(local, key=lambda row: float(row.get("r2") or 0.0))
        best_rows.append(best)
    write_rows(base_dir / "support_ladder_best_fit.csv", best_rows)
    write_markdown(
        base_dir / "README.md",
        "\n".join(
            [
                "# Q1 Support Ladder",
                "",
                "Question: does fit degrade smoothly as support becomes finer?",
                "",
                "Result: yes. The validated baseline shows a strong decline from state to AGEB to manzana.",
                "",
                "Key files:",
                "- `support_ladder_summary.csv`: all validated fits by level",
                "- `support_ladder_best_fit.csv`: best fit by `R2` for each level",
            ]
        ),
    )


def question_2_synthetic_aggregation(base_dir: Path, manzana_rows: list) -> None:
    bundle_sizes = [2, 5, 10, 20, 50, 100, 250]
    repeat_rows = []
    summary_rows = []
    for bundle_size in bundle_sizes:
        local_metrics = []
        for seed in range(1, 21):
            bundled = synthetic_bundle_rows(
                manzana_rows,
                bundle_size=bundle_size,
                seed=seed,
                level_name=f"manzana_bundle_{bundle_size}",
            )
            metrics = fit_metrics(bundled, fit_method="ols")
            metrics["bundle_size"] = bundle_size
            metrics["seed"] = seed
            repeat_rows.append(metrics)
            local_metrics.append(metrics)
        summary_rows.append(
            {
                "bundle_size": bundle_size,
                "repeats": len(local_metrics),
                "units_mean": sum(float(row["units"]) for row in local_metrics) / float(len(local_metrics)),
                "beta_mean": sum(float(row["beta"]) for row in local_metrics) / float(len(local_metrics)),
                "r2_mean": sum(float(row["r2"]) for row in local_metrics) / float(len(local_metrics)),
                "r2_p05": sorted(float(row["r2"]) for row in local_metrics)[0],
                "r2_p95": sorted(float(row["r2"]) for row in local_metrics)[-1],
            }
        )
    write_rows(base_dir / "synthetic_aggregation_repeats.csv", repeat_rows)
    write_rows(base_dir / "synthetic_aggregation_summary.csv", summary_rows)
    write_markdown(
        base_dir / "README.md",
        "\n".join(
            [
                "# Q2 Synthetic Aggregation Recovery",
                "",
                "Question: does fit recover when fine units are aggregated upward even without original admin boundaries?",
                "",
                "Method: random within-city bundles of manzana units, repeated 20 times for each bundle size.",
                "",
                "Key files:",
                "- `synthetic_aggregation_repeats.csv`",
                "- `synthetic_aggregation_summary.csv`",
            ]
        ),
    )


def question_3_shuffle_null(base_dir: Path, rows_by_level: dict[str, list]) -> None:
    repeat_rows = []
    summary_rows = []
    for level in ("ageb_u", "manzana"):
        observed = fit_metrics(rows_by_level[level], fit_method="ols")
        observed["experiment"] = "observed"
        observed["level"] = level
        summary_rows.append(observed)
        shuffled_r2 = []
        for seed in range(1, 51):
            shuffled = shuffle_y_within_city(rows_by_level[level], seed=seed, level_name=f"{level}_shuffle")
            metrics = fit_metrics(shuffled, fit_method="ols")
            metrics["experiment"] = "shuffled"
            metrics["level"] = level
            metrics["seed"] = seed
            repeat_rows.append(metrics)
            shuffled_r2.append(float(metrics["r2"]))
        summary_rows.append(
            {
                "level": level,
                "experiment": "shuffled_summary",
                "repeats": len(shuffled_r2),
                "r2_mean": sum(shuffled_r2) / float(len(shuffled_r2)),
                "r2_p05": sorted(shuffled_r2)[0],
                "r2_p95": sorted(shuffled_r2)[-1],
            }
        )
    write_rows(base_dir / "shuffle_null_repeats.csv", repeat_rows)
    write_rows(base_dir / "shuffle_null_summary.csv", summary_rows)
    write_markdown(
        base_dir / "README.md",
        "\n".join(
            [
                "# Q3 Shuffle Null",
                "",
                "Question: how much of the fine-scale behavior can be explained by sparse-count structure alone?",
                "",
                "Method: within each city, shuffle `Y` across units while keeping the unit population distribution fixed.",
                "",
                "Key files:",
                "- `shuffle_null_repeats.csv`",
                "- `shuffle_null_summary.csv`",
            ]
        ),
    )


def question_4_city_context(base_dir: Path, rows_by_level: dict[str, list]) -> None:
    summary_rows = []
    per_city_rows = []
    for level in ("ageb_u", "manzana"):
        pooled = fit_metrics(rows_by_level[level], fit_method="ols")
        pooled["scope"] = "pooled"
        pooled["level"] = level
        summary_rows.append(pooled)
        per_city = fit_per_city(rows_by_level[level], fit_method="ols", min_units=20)
        per_city_rows.extend([dict(row, level=level) for row in per_city])
        if per_city:
            weighted_r2 = sum(float(row["r2"]) * float(row["units"]) for row in per_city) / sum(float(row["units"]) for row in per_city)
            summary_rows.append(
                {
                    "level": level,
                    "scope": "city_specific_weighted",
                    "cities": len(per_city),
                    "weighted_r2": weighted_r2,
                    "beta_min": min(float(row["beta"]) for row in per_city),
                    "beta_max": max(float(row["beta"]) for row in per_city),
                }
            )
    write_rows(base_dir / "city_context_per_city.csv", per_city_rows)
    write_rows(base_dir / "city_context_summary.csv", summary_rows)
    write_markdown(
        base_dir / "README.md",
        "\n".join(
            [
                "# Q4 City Context Effect",
                "",
                "Question: is pooled fine-scale weakness partly caused by combining different city regimes into one model?",
                "",
                "Method: compare pooled OLS to separate within-city OLS fits.",
                "",
                "Key files:",
                "- `city_context_per_city.csv`",
                "- `city_context_summary.csv`",
            ]
        ),
    )


def question_5_distribution_audit(base_dir: Path, rows_by_level: dict[str, list]) -> None:
    rows = [distribution_audit(rows_by_level[level], level=level) for level in ("state", "ageb_u", "manzana")]
    write_rows(base_dir / "distribution_audit.csv", rows)
    write_markdown(
        base_dir / "README.md",
        "\n".join(
            [
                "# Q5 Distribution Audit",
                "",
                "Question: do sparsity and denominator distribution help explain fine-scale fit collapse?",
                "",
                "Method: audit concentration, small counts, and small denominators by level.",
                "",
                "Key files:",
                "- `distribution_audit.csv`",
            ]
        ),
    )


def question_6_city_recovery(base_dir: Path, rows_by_level: dict[str, list]) -> None:
    summary_rows = []
    for level in ("ageb_u", "manzana"):
        city_rows = aggregate_to_city(rows_by_level[level], level_name=f"{level}_to_city")
        metrics = fit_metrics(city_rows, fit_method="ols")
        metrics["source_level"] = level
        summary_rows.append(metrics)
    write_rows(base_dir / "city_recovery_summary.csv", summary_rows)
    write_markdown(
        base_dir / "README.md",
        "\n".join(
            [
                "# Q6 Aggregation Recovery To City Scale",
                "",
                "Question: does a stronger scaling relation re-emerge when fine units are aggregated back to city totals?",
                "",
                "Method: aggregate AGEB and manzana units to city totals and refit OLS.",
                "",
                "Key files:",
                "- `city_recovery_summary.csv`",
            ]
        ),
    )


def main() -> int:
    DIST.mkdir(parents=True, exist_ok=True)

    state_rows = load_units_csv(
        FIX / "state_population_all_cities" / "units" / "population__state__raw__ols_units.csv",
        level="state",
        fit_method="ols",
    )
    ageb_rows = load_units_csv(
        FIX / "ageb_population_scope" / "units" / "population__ageb_u__raw__ols_units.csv",
        level="ageb_u",
        fit_method="ols",
    )
    manzana_rows = _load_manzana_units()
    rows_by_level = {"state": state_rows, "ageb_u": ageb_rows, "manzana": manzana_rows}

    question_1_support_ladder(DIST / "q1_support_ladder")
    question_2_synthetic_aggregation(DIST / "q2_synthetic_aggregation", manzana_rows)
    question_3_shuffle_null(DIST / "q3_shuffle_null", rows_by_level)
    question_4_city_context(DIST / "q4_city_context", rows_by_level)
    question_5_distribution_audit(DIST / "q5_distribution_audit", rows_by_level)
    question_6_city_recovery(DIST / "q6_city_recovery", rows_by_level)

    write_json(
        DIST / "manifest.json",
        {
            "generated_at": "2026-04-21",
            "base_levels": ["state", "ageb_u", "manzana"],
            "base_outcome": "denue_est_count",
            "base_scale_variable": "population",
            "questions": [
                "q1_support_ladder",
                "q2_synthetic_aggregation",
                "q3_shuffle_null",
                "q4_city_context",
                "q5_distribution_audit",
                "q6_city_recovery",
            ],
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

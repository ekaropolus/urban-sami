#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
import shutil
from pathlib import Path

from urban_sami.modeling.fit import compute_deviation_score, fit_ols
from run_ageb_unconstrained_subset_search import (
    N_STARTS,
    RANDOM_SEED,
    TARGET_SIZES,
    _components,
    _fetch_features,
    _fetch_touch_edges,
    _improve_subset,
)
from run_city_sami_internal_signature_experiment import _fetch_ageb_city_list, _fetch_ageb_rows
from run_single_city_ageb_experiment import _write_ageb_map


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _city_rows(city_code: str) -> list[dict[str, float | str]]:
    raw_rows = _fetch_ageb_rows(city_code)
    rows = []
    for r in raw_rows:
        population = _safe_float(r["population"])
        est = _safe_float(r["est_count"])
        if population <= 0 or est <= 0:
            continue
        rows.append(
            {
                "unit_code": r["unit_code"],
                "unit_label": r["unit_label"],
                "population": population,
                "est_count": est,
                "area_km2": _safe_float(r["area_km2"]),
                "dist_to_center_km": _safe_float(r["dist_to_center_km"]),
            }
        )
    return rows


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "city-best-local-ageb-subsets-2026-04-22"
    figdir = outdir / "figures"
    statedir = outdir / "by_state"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)
    statedir.mkdir(parents=True, exist_ok=True)

    cities = _fetch_ageb_city_list()
    summary_rows = []
    frontier_rows = []
    member_rows = []
    graph_rows = []

    for city_idx, city in enumerate(cities):
        city_code = city["city_code"]
        city_name = city["city_name"] or city_code
        state_code = city_code[:2]
        rows = _city_rows(city_code)
        if len(rows) < 40:
            continue

        pop = [float(r["population"]) for r in rows]
        est = [float(r["est_count"]) for r in rows]
        city_best = None
        best_indices = None
        target_candidates = [t for t in TARGET_SIZES if t < len(rows) - 5]
        if not target_candidates:
            target_candidates = [max(35, len(rows) - 10)]

        for target in target_candidates:
            best_for_target = None
            idxs_for_target = None
            for start_idx in range(N_STARTS):
                subset_indices, score = _improve_subset(target, pop, est, __import__("random").Random(RANDOM_SEED + city_idx * 1000 + start_idx + target))
                if best_for_target is None or score["adj_r2"] > best_for_target["adj_r2"] + 1e-12 or (
                    abs(score["adj_r2"] - best_for_target["adj_r2"]) <= 1e-12 and score["r2"] > best_for_target["r2"] + 1e-12
                ):
                    best_for_target = score
                    idxs_for_target = subset_indices
            assert best_for_target is not None and idxs_for_target is not None
            frontier_rows.append(
                {
                    "city_code": city_code,
                    "city_name": city_name,
                    "state_code": state_code,
                    "target_size": target,
                    "n_obs": len(idxs_for_target),
                    "beta": best_for_target["beta"],
                    "r2": best_for_target["r2"],
                    "adj_r2": best_for_target["adj_r2"],
                }
            )
            if city_best is None or best_for_target["adj_r2"] > city_best["adj_r2"] + 1e-12 or (
                abs(best_for_target["adj_r2"] - city_best["adj_r2"]) <= 1e-12 and best_for_target["r2"] > city_best["r2"] + 1e-12
            ):
                city_best = {**best_for_target, "target_size": target}
                best_indices = list(idxs_for_target)

        assert city_best is not None and best_indices is not None
        selected = [rows[i] for i in best_indices]
        local_fit = fit_ols([float(r["est_count"]) for r in selected], [float(r["population"]) for r in selected])
        selected_codes = [str(r["unit_code"]) for r in selected]
        edges = _fetch_touch_edges(city_code, selected_codes)
        comps = _components(selected_codes, edges)

        summary_rows.append(
            {
                "state_code": state_code,
                "city_code": city_code,
                "city_name": city_name,
                "ageb_total": len(rows),
                "selected_target_size": city_best["target_size"],
                "selected_ageb_n": len(selected),
                "retention_rate": len(selected) / float(len(rows)),
                "beta_local_subset": local_fit.beta,
                "r2_local_subset": local_fit.r2,
                "adj_r2_local_subset": city_best["adj_r2"],
                "mean_population_selected": sum(float(r["population"]) for r in selected) / len(selected),
                "mean_est_selected": sum(float(r["est_count"]) for r in selected) / len(selected),
                "mean_area_selected": sum(float(r["area_km2"]) for r in selected) / len(selected),
                "mean_dist_selected": sum(float(r["dist_to_center_km"]) for r in selected) / len(selected),
                "component_count": comps["component_count"],
                "largest_component_size": comps["largest_component_size"],
                "largest_component_share": comps["largest_component_share"],
                "edge_count": comps["edge_count"],
            }
        )
        graph_rows.append({"state_code": state_code, "city_code": city_code, "city_name": city_name, **comps})

        value_lookup = {}
        for r in selected:
            score = compute_deviation_score(float(r["est_count"]), float(r["population"]), local_fit.alpha, local_fit.beta, local_fit.residual_std)
            value_lookup[str(r["unit_code"])] = score.sami
            member_rows.append(
                {
                    "state_code": state_code,
                    "city_code": city_code,
                    "city_name": city_name,
                    "selected_target_size": city_best["target_size"],
                    "unit_code": r["unit_code"],
                    "unit_label": r["unit_label"],
                    "population": r["population"],
                    "est_count": r["est_count"],
                    "area_km2": r["area_km2"],
                    "dist_to_center_km": r["dist_to_center_km"],
                    "sami_local_subset": score.sami,
                    "y_expected_local_subset": score.y_expected,
                }
            )

        features = _fetch_features(city_code)
        state_folder = statedir / state_code / f"{city_code}_{city_name.replace('/', '_').replace(' ', '_')}"
        state_folder.mkdir(parents=True, exist_ok=True)
        _write_csv(state_folder / "selected_ageb_members.csv", [m for m in member_rows if m["city_code"] == city_code], list(member_rows[0].keys()))
        _write_ageb_map(
            city_name,
            f"Best local AGEB subset (n={len(selected)}, R²={local_fit.r2:.3f}, β={local_fit.beta:+.3f})",
            features,
            value_lookup,
            state_folder / "selected_ageb_map.svg",
        )

    summary_rows.sort(key=lambda r: (r["state_code"], r["city_code"]))
    frontier_rows.sort(key=lambda r: (r["state_code"], r["city_code"], int(r["target_size"])))
    member_rows.sort(key=lambda r: (r["state_code"], r["city_code"], r["unit_code"]))
    graph_rows.sort(key=lambda r: (r["state_code"], r["city_code"]))

    _write_csv(outdir / "city_best_subset_summary.csv", summary_rows, list(summary_rows[0].keys()))
    _write_csv(outdir / "city_subset_frontier.csv", frontier_rows, list(frontier_rows[0].keys()))
    _write_csv(outdir / "city_selected_ageb_members.csv", member_rows, list(member_rows[0].keys()))
    _write_csv(outdir / "city_selected_subset_graph_stats.csv", graph_rows, list(graph_rows[0].keys()))

    lines = [
        "# Best Local AGEB Subsets By City",
        "",
        "For each city with AGEB loaded, this experiment searches mathematically for the AGEB subset that gives the best local fit. `beta` is estimated within each city from the selected subset itself.",
        "",
        "No national beta is used here.",
        "",
        "## Files",
        f"- [city_best_subset_summary.csv]({(outdir / 'city_best_subset_summary.csv').resolve()})",
        f"- [city_subset_frontier.csv]({(outdir / 'city_subset_frontier.csv').resolve()})",
        f"- [city_selected_ageb_members.csv]({(outdir / 'city_selected_ageb_members.csv').resolve()})",
        f"- [city_selected_subset_graph_stats.csv]({(outdir / 'city_selected_subset_graph_stats.csv').resolve()})",
        "",
        "Each state/city folder under `by_state/` contains:",
        "- `selected_ageb_members.csv`",
        "- `selected_ageb_map.svg`",
    ]
    (outdir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

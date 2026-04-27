#!/usr/bin/env python3
from __future__ import annotations

import csv
import html
import math
import random
import shutil
from collections import Counter, defaultdict
from pathlib import Path

from urban_sami.modeling.fit import fit_ols
from run_single_city_ageb_experiment import _write_ageb_map
from run_ageb_subset_discovery import _fetch_features, _fetch_touch_edges, _components


BG = "#f8f6f1"
PANEL = "#fffdf8"
GRID = "#ddd6c8"
AXIS = "#8b8478"
TEXT = "#1f1f1f"
MUTED = "#625d54"
TEAL = "#0f766e"
BLUE = "#315c80"
RUST = "#b14d3b"
SANS = "Helvetica, Arial, sans-serif"
SERIF = "Georgia, 'Times New Roman', serif"

RANDOM_SEED = 20260422
TARGET_SIZES = [40, 60, 80, 100, 120, 160, 200]
N_STARTS = 24
MAX_STEPS = 500
STALL_LIMIT = 80


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _adj_r2(r2: float, n: int, p: int) -> float:
    df = n - p - 1
    if df <= 0:
        return r2
    return 1.0 - ((1.0 - r2) * (n - 1) / float(df))


def _fmt(v: float, digits: int = 3) -> str:
    return f"{v:.{digits}f}"


def _score(indices: list[int], pop: list[float], est: list[float]) -> dict[str, float]:
    fit = fit_ols([est[i] for i in indices], [pop[i] for i in indices])
    return {
        "beta": float(fit.beta),
        "r2": float(fit.r2),
        "adj_r2": float(_adj_r2(fit.r2, len(indices), 1)),
    }


def _improve_subset(
    target_size: int,
    pop: list[float],
    est: list[float],
    rng: random.Random,
) -> tuple[list[int], dict[str, float]]:
    n_total = len(pop)
    current = sorted(rng.sample(range(n_total), target_size))
    current_set = set(current)
    current_score = _score(current, pop, est)
    best = list(current)
    best_score = dict(current_score)
    stall = 0

    for _ in range(MAX_STEPS):
        if stall >= STALL_LIMIT:
            break
        out_idx = rng.choice(current)
        in_idx = rng.randrange(n_total)
        if in_idx in current_set:
            continue
        trial = [i for i in current if i != out_idx] + [in_idx]
        trial.sort()
        trial_score = _score(trial, pop, est)
        better = (
            trial_score["adj_r2"] > current_score["adj_r2"] + 1e-12
            or (
                abs(trial_score["adj_r2"] - current_score["adj_r2"]) <= 1e-12
                and trial_score["r2"] > current_score["r2"] + 1e-12
            )
        )
        if better:
            current = trial
            current_set = set(current)
            current_score = trial_score
            stall = 0
            if (
                current_score["adj_r2"] > best_score["adj_r2"] + 1e-12
                or (
                    abs(current_score["adj_r2"] - best_score["adj_r2"]) <= 1e-12
                    and current_score["r2"] > best_score["r2"] + 1e-12
                )
            ):
                best = list(current)
                best_score = dict(current_score)
        else:
            stall += 1
    return best, best_score


def _jaccard(a: set[str], b: set[str]) -> float:
    union = len(a | b)
    return len(a & b) / float(union) if union else 0.0


def _svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _write_frontier(path: Path, rows: list[dict[str, object]]) -> Path:
    width = 1040
    height = 620
    left = 88
    right = 40
    top = 96
    bottom = 88
    plot_w = width - left - right
    plot_h = height - top - bottom
    xvals = [float(r["n_obs"]) for r in rows]
    yvals = [float(r["r2"]) for r in rows]
    xmin, xmax = min(xvals), max(xvals)
    ymin, ymax = min(yvals), max(yvals)
    xmin -= 5
    xmax += 5
    ymin = max(0.0, ymin - 0.03)
    ymax += 0.03

    def px(v: float) -> float:
        return left + ((v - xmin) / max(xmax - xmin, 1e-9)) * plot_w

    def py(v: float) -> float:
        return top + plot_h - ((v - ymin) / max(ymax - ymin, 1e-9)) * plot_h

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">Unconstrained subset frontier</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">Best subset found at each target size. No geometry rule was imposed in the optimization itself.</text>',
        f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="none" stroke="{AXIS}"/>',
    ]
    for row in rows:
        x = float(row["n_obs"])
        y = float(row["r2"])
        body.append(f'<circle cx="{px(x):.2f}" cy="{py(y):.2f}" r="5" fill="{TEAL}" fill-opacity="0.85"/>')
        body.append(f'<text x="{px(x)+8:.2f}" y="{py(y)-6:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">n={int(x)} R²={y:.3f}</text>')
    for a, b in zip(rows[:-1], rows[1:]):
        body.append(
            f'<line x1="{px(float(a["n_obs"])):.2f}" y1="{py(float(a["r2"])):.2f}" '
            f'x2="{px(float(b["n_obs"])):.2f}" y2="{py(float(b["r2"])):.2f}" stroke="{BLUE}" stroke-width="1.5"/>'
        )
    return _svg(path, width, height, "".join(body))


def _write_rank(path: Path, rows: list[dict[str, object]]) -> Path:
    width = 1140
    left = 250
    right = 70
    top = 96
    bottom = 78
    row_h = 28
    height = top + len(rows) * row_h + bottom
    vals = [float(r["r2"]) for r in rows]
    xmax = max(vals) * 1.1 if vals else 1.0

    def px(v: float) -> float:
        return left + (v / max(xmax, 1e-9)) * (width - left - right)

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">Best unconstrained subsets</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">Each row is the best subset found for a fixed target size.</text>',
    ]
    for i, row in enumerate(rows):
        y = top + i * row_h
        label = f"target_{int(row['target_size'])} (found n={int(row['n_obs'])})"
        r2 = float(row["r2"])
        beta = float(row["beta"])
        body.append(f'<text x="{left-12}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{html.escape(label)}</text>')
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{px(r2):.2f}" y2="{y:.2f}" stroke="{TEAL}" stroke-width="8"/>')
        body.append(f'<text x="{px(r2)+8:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">R²={r2:.3f} β={beta:+.3f}</text>')
    return _svg(path, width, height, "".join(body))


def _write_overlap_table(path: Path, rows: list[dict[str, object]]) -> Path:
    width = 1220
    col_x = [44, 280, 520, 760, 930, 1060]
    row_h = 28
    top = 106
    height = top + row_h * len(rows) + 50
    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">Overlap among unconstrained subsets</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">If overlaps are low, the optimization is discovering distinct good subsets rather than one trivial answer.</text>',
    ]
    headers = ["subset_a", "subset_b", "jaccard", "shared", "union", "mean_size"]
    for x, h in zip(col_x, headers):
        body.append(f'<text x="{x}" y="{top-18}" font-size="12" font-family="{SANS}" fill="{MUTED}">{h}</text>')
    for i, row in enumerate(rows):
        y = top + i * row_h
        vals = [
            str(row["subset_a"]),
            str(row["subset_b"]),
            f'{float(row["jaccard"]):.3f}',
            str(int(row["shared_agebs"])),
            str(int(row["distinct_union"])),
            str(int(row["mean_size"])),
        ]
        for x, v in zip(col_x, vals):
            body.append(f'<text x="{x}" y="{y}" font-size="12" font-family="{SANS}" fill="{TEXT}">{html.escape(v)}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "ageb-unconstrained-subset-search-guadalajara-2026-04-22"
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    city_code = "14039"
    city_name = "Guadalajara"
    feature_rows = _read_csv(root / "reports" / "ageb-subset-discovery-guadalajara-2026-04-22" / "ageb_feature_table.csv")
    rows = [
        {
            "unit_code": r["unit_code"],
            "unit_label": r["unit_label"],
            "population": float(r["population"]),
            "est_count": float(r["est_count"]),
            "area_km2": float(r["area_km2"]),
            "dist_to_center_km": float(r["dist_to_center_km"]),
            "population_density": float(r["population_density"]),
            "compactness": float(r["compactness"]),
            "neighbor_degree": float(r["neighbor_degree"]),
        }
        for r in feature_rows
        if float(r["population"]) > 0 and float(r["est_count"]) > 0
    ]
    pop = [r["population"] for r in rows]
    est = [r["est_count"] for r in rows]

    rng = random.Random(RANDOM_SEED)
    best_rows = []
    member_rows = []
    best_sets: dict[str, set[str]] = {}
    best_index_sets: dict[str, list[int]] = {}
    for target_size in TARGET_SIZES:
        best_indices = None
        best_metrics = None
        for _ in range(N_STARTS):
            subset_indices, metrics = _improve_subset(target_size, pop, est, rng)
            if best_metrics is None or metrics["adj_r2"] > best_metrics["adj_r2"] + 1e-12 or (
                abs(metrics["adj_r2"] - best_metrics["adj_r2"]) <= 1e-12 and metrics["r2"] > best_metrics["r2"] + 1e-12
            ):
                best_indices = subset_indices
                best_metrics = metrics
        assert best_indices is not None and best_metrics is not None
        subset_name = f"target_{target_size}"
        best_index_sets[subset_name] = list(best_indices)
        best_sets[subset_name] = {rows[i]["unit_code"] for i in best_indices}
        best_rows.append(
            {
                "subset_rule": subset_name,
                "target_size": target_size,
                "n_obs": len(best_indices),
                "beta": best_metrics["beta"],
                "r2": best_metrics["r2"],
                "adj_r2": best_metrics["adj_r2"],
                "mean_population": sum(rows[i]["population"] for i in best_indices) / len(best_indices),
                "mean_est_count": sum(rows[i]["est_count"] for i in best_indices) / len(best_indices),
                "mean_area_km2": sum(rows[i]["area_km2"] for i in best_indices) / len(best_indices),
                "mean_dist_to_center_km": sum(rows[i]["dist_to_center_km"] for i in best_indices) / len(best_indices),
                "mean_density": sum(rows[i]["population_density"] for i in best_indices) / len(best_indices),
                "mean_compactness": sum(rows[i]["compactness"] for i in best_indices) / len(best_indices),
                "mean_neighbor_degree": sum(rows[i]["neighbor_degree"] for i in best_indices) / len(best_indices),
            }
        )
        for idx in best_indices:
            r = rows[idx]
            member_rows.append(
                {
                    "subset_rule": subset_name,
                    "unit_code": r["unit_code"],
                    "unit_label": r["unit_label"],
                    "population": r["population"],
                    "est_count": r["est_count"],
                    "area_km2": r["area_km2"],
                    "dist_to_center_km": r["dist_to_center_km"],
                    "population_density": r["population_density"],
                    "compactness": r["compactness"],
                    "neighbor_degree": r["neighbor_degree"],
                }
            )

    best_rows.sort(key=lambda r: int(r["target_size"]))
    _write_csv(outdir / "best_subsets_by_size.csv", best_rows, list(best_rows[0].keys()))
    _write_csv(outdir / "subset_members_long.csv", member_rows, list(member_rows[0].keys()))

    overlap_rows = []
    names = [str(r["subset_rule"]) for r in best_rows]
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            a = best_sets[names[i]]
            b = best_sets[names[j]]
            overlap_rows.append(
                {
                    "subset_a": names[i],
                    "subset_b": names[j],
                    "jaccard": _jaccard(a, b),
                    "shared_agebs": len(a & b),
                    "distinct_union": len(a | b),
                    "mean_size": round((len(a) + len(b)) / 2),
                }
            )
    overlap_rows.sort(key=lambda r: float(r["jaccard"]), reverse=True)
    _write_csv(outdir / "subset_overlap.csv", overlap_rows, list(overlap_rows[0].keys()))

    appear = Counter()
    for subset in best_sets.values():
        appear.update(subset)
    consensus_codes = {code for code, c in appear.items() if c >= 3}
    consensus_rows = [r for r in rows if r["unit_code"] in consensus_codes]
    consensus_summary_rows = []
    if len(consensus_rows) >= 35:
        fit = fit_ols([r["est_count"] for r in consensus_rows], [r["population"] for r in consensus_rows])
        consensus_summary_rows.append(
            {
                "subset_rule": "consensus_core_ge3",
                "n_obs": len(consensus_rows),
                "beta": fit.beta,
                "r2": fit.r2,
                "adj_r2": _adj_r2(fit.r2, len(consensus_rows), 1),
                "mean_population": sum(r["population"] for r in consensus_rows) / len(consensus_rows),
                "mean_est_count": sum(r["est_count"] for r in consensus_rows) / len(consensus_rows),
                "mean_area_km2": sum(r["area_km2"] for r in consensus_rows) / len(consensus_rows),
                "mean_dist_to_center_km": sum(r["dist_to_center_km"] for r in consensus_rows) / len(consensus_rows),
            }
        )
    consensus_member_rows = []
    for r in rows:
        if appear[r["unit_code"]] > 0:
            consensus_member_rows.append(
                {
                    "unit_code": r["unit_code"],
                    "unit_label": r["unit_label"],
                    "appearance_count": appear[r["unit_code"]],
                    "in_consensus_core": 1 if r["unit_code"] in consensus_codes else 0,
                    "population": r["population"],
                    "est_count": r["est_count"],
                    "area_km2": r["area_km2"],
                    "dist_to_center_km": r["dist_to_center_km"],
                    "population_density": r["population_density"],
                    "compactness": r["compactness"],
                    "neighbor_degree": r["neighbor_degree"],
                }
            )
    consensus_member_rows.sort(key=lambda r: (int(r["appearance_count"]), float(r["est_count"])), reverse=True)
    _write_csv(outdir / "consensus_members.csv", consensus_member_rows, list(consensus_member_rows[0].keys()))
    if consensus_summary_rows:
        _write_csv(outdir / "consensus_summary.csv", consensus_summary_rows, list(consensus_summary_rows[0].keys()))

    features = _fetch_features(city_code)
    graph_rows = []
    map_manifest = []
    for row in best_rows:
        name = str(row["subset_rule"])
        codes = best_sets[name]
        idxs = best_index_sets[name]
        edges = _fetch_touch_edges(city_code, sorted(codes))
        comps = _components(sorted(codes), edges)
        graph_rows.append({"subset_rule": name, **comps})
        fit = fit_ols([rows[i]["est_count"] for i in idxs], [rows[i]["population"] for i in idxs])
        value_lookup = {}
        for i in idxs:
            y = rows[i]["est_count"]
            n = rows[i]["population"]
            y_expected = math.exp(fit.alpha + fit.beta * math.log(max(n, 1e-9)))
            value_lookup[rows[i]["unit_code"]] = math.log(max(y, 1e-9)) - math.log(max(y_expected, 1e-9))
        map_path = _write_ageb_map(
            city_name,
            f"Unconstrained subset {name} (n={int(row['n_obs'])}, R²={float(row['r2']):.3f})",
            features,
            value_lookup,
            figdir / f"{name}_map.svg",
        )
        map_manifest.append({"figure_id": f"{name}_map", "path": str(map_path.resolve()), "description": f"Map for {name}."})
    _write_csv(outdir / "subset_graph_stats.csv", graph_rows, list(graph_rows[0].keys()))

    if consensus_summary_rows:
        fit = fit_ols([r["est_count"] for r in consensus_rows], [r["population"] for r in consensus_rows])
        value_lookup = {}
        for r in consensus_rows:
            y_expected = math.exp(fit.alpha + fit.beta * math.log(max(r["population"], 1e-9)))
            value_lookup[r["unit_code"]] = math.log(max(r["est_count"], 1e-9)) - math.log(max(y_expected, 1e-9))
        cmap = _write_ageb_map(
            city_name,
            f"Consensus core ge3 (n={len(consensus_rows)}, R²={fit.r2:.3f})",
            features,
            value_lookup,
            figdir / "consensus_core_map.svg",
        )
        map_manifest.append({"figure_id": "consensus_core_map", "path": str(cmap.resolve()), "description": "Map for consensus core."})

    fig1 = _write_rank(figdir / "best_subsets_rank.svg", best_rows)
    fig2 = _write_frontier(figdir / "subset_frontier.svg", best_rows)
    fig3 = _write_overlap_table(figdir / "subset_overlap_table.svg", overlap_rows[:18])
    manifest = [
        {"figure_id": "best_subsets_rank", "path": str(fig1.resolve()), "description": "Best subset at each target size."},
        {"figure_id": "subset_frontier", "path": str(fig2.resolve()), "description": "R²-size frontier."},
        {"figure_id": "subset_overlap_table", "path": str(fig3.resolve()), "description": "Overlap among best subsets."},
        *map_manifest,
    ]
    _write_csv(figdir / "figures_manifest.csv", manifest, ["figure_id", "path", "description"])

    lines = [
        "# Unconstrained AGEB Subset Search in Guadalajara",
        "",
        "This search does not impose geometric rules on the optimizer. It picks AGEB subsets directly to maximize fit, while fixing only the target subset size.",
        "",
        "Optimization setup:",
        f"- target sizes = `{TARGET_SIZES}`",
        f"- random starts per size = `{N_STARTS}`",
        f"- local swap iterations max = `{MAX_STEPS}`",
        f"- seed = `{RANDOM_SEED}`",
        "",
        "## Best subsets by size",
    ]
    for row in best_rows:
        lines.append(
            f"- `{row['subset_rule']}`: `n={int(row['n_obs'])}`, `beta={float(row['beta']):+.3f}`, `R²={float(row['r2']):.3f}`, `adjR²={float(row['adj_r2']):.3f}`"
        )
    if consensus_summary_rows:
        c = consensus_summary_rows[0]
        lines.extend(
            [
                "",
                "## Consensus core",
                f"- `n={int(c['n_obs'])}`",
                f"- `beta={float(c['beta']):+.3f}`",
                f"- `R²={float(c['r2']):.3f}`",
                f"- `adjR²={float(c['adj_r2']):.3f}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Files",
            f"- [best_subsets_by_size.csv]({(outdir / 'best_subsets_by_size.csv').resolve()})",
            f"- [subset_members_long.csv]({(outdir / 'subset_members_long.csv').resolve()})",
            f"- [subset_overlap.csv]({(outdir / 'subset_overlap.csv').resolve()})",
            f"- [subset_graph_stats.csv]({(outdir / 'subset_graph_stats.csv').resolve()})",
            f"- [consensus_members.csv]({(outdir / 'consensus_members.csv').resolve()})",
        ]
    )
    if consensus_summary_rows:
        lines.append(f"- [consensus_summary.csv]({(outdir / 'consensus_summary.csv').resolve()})")
    (outdir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

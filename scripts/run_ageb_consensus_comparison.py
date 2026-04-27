#!/usr/bin/env python3
from __future__ import annotations

import csv
import html
import math
import shutil
from collections import defaultdict
from itertools import combinations
from pathlib import Path


BG = "#f8f6f1"
PANEL = "#fffdf8"
GRID = "#ddd6c8"
AXIS = "#8b8478"
TEXT = "#1f1f1f"
MUTED = "#625d54"
TEAL = "#0f766e"
BLUE = "#315c80"
RUST = "#b14d3b"
GOLD = "#b28a2e"
SANS = "Helvetica, Arial, sans-serif"
SERIF = "Georgia, 'Times New Roman', serif"


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


def _mean(values: list[float]) -> float:
    return sum(values) / float(len(values)) if values else 0.0


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mu = _mean(values)
    return math.sqrt(sum((v - mu) ** 2 for v in values) / float(len(values) - 1))


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na <= 0 or nb <= 0:
        return 0.0
    return dot / (na * nb)


def _write_feature_contrast(path: Path, rows: list[dict[str, object]]) -> Path:
    width = 1080
    left = 340
    right = 60
    top = 96
    bottom = 78
    row_h = 28
    height = top + len(rows) * row_h + bottom
    vals = [abs(float(r["z_gap"])) for r in rows]
    xmax = max(vals) * 1.1 if vals else 1.0

    def px(v: float) -> float:
        return left + (abs(v) / max(xmax, 1e-9)) * (width - left - right)

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">Consensus core versus rest: feature contrasts</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">Bars show standardized mean gaps. Positive means the consensus core is higher than the rest.</text>',
    ]
    for i, row in enumerate(rows):
        y = top + i * row_h
        zgap = float(row["z_gap"])
        color = TEAL if zgap >= 0 else RUST
        body.append(f'<text x="{left-12}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{html.escape(str(row["feature"]))}</text>')
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{px(zgap):.2f}" y2="{y:.2f}" stroke="{color}" stroke-width="8"/>')
        body.append(f'<text x="{px(zgap)+8:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">Δz={zgap:+.2f}</text>')
    return _svg(path, width, height, "".join(body))


def _write_enrichment(path: Path, rows: list[dict[str, object]], title: str, subtitle: str) -> Path:
    width = 1180
    left = 360
    right = 60
    top = 96
    bottom = 78
    row_h = 28
    height = top + len(rows) * row_h + bottom
    vals = [abs(float(r["log2_ratio"])) for r in rows]
    xmax = max(vals) * 1.1 if vals else 1.0

    def px(v: float) -> float:
        return left + (abs(v) / max(xmax, 1e-9)) * (width - left - right)

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{html.escape(title)}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">{html.escape(subtitle)}</text>',
    ]
    for i, row in enumerate(rows):
        y = top + i * row_h
        val = float(row["log2_ratio"])
        color = TEAL if val >= 0 else RUST
        label = f"{row['category']} ({float(row['core_share']):.3f} vs {float(row['rest_share']):.3f})"
        body.append(f'<text x="{left-12}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{html.escape(label)}</text>')
        body.append(f'<line x1="{left}" y1="{y:.2f}" x2="{px(val):.2f}" y2="{y:.2f}" stroke="{color}" stroke-width="8"/>')
        body.append(f'<text x="{px(val)+8:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{TEXT}">log2={val:+.2f}</text>')
    return _svg(path, width, height, "".join(body))


def _write_similarity(path: Path, rows: list[dict[str, object]]) -> Path:
    width = 1220
    col_x = [44, 210, 376, 540, 710, 860, 1030]
    row_h = 28
    top = 106
    height = top + row_h * len(rows) + 50
    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">Most similar AGEB pairs inside the consensus core</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">Similarity is cosine over morphology plus composition shares. This is after selection, not before.</text>',
    ]
    headers = ["unit_a", "unit_b", "similarity", "dist_a", "dist_b", "dens_a", "dens_b"]
    for x, h in zip(col_x, headers):
        body.append(f'<text x="{x}" y="{top-18}" font-size="12" font-family="{SANS}" fill="{MUTED}">{h}</text>')
    for i, row in enumerate(rows):
        y = top + i * row_h
        vals = [
            str(row["unit_a"]),
            str(row["unit_b"]),
            f'{float(row["similarity"]):.3f}',
            f'{float(row["dist_a"]):.2f}',
            f'{float(row["dist_b"]):.2f}',
            f'{float(row["dens_a"]):.0f}',
            f'{float(row["dens_b"]):.0f}',
        ]
        for x, v in zip(col_x, vals):
            body.append(f'<text x="{x}" y="{y}" font-size="12" font-family="{SANS}" fill="{TEXT}">{html.escape(v)}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "ageb-consensus-comparison-guadalajara-2026-04-22"
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    feature_rows = _read_csv(root / "reports" / "ageb-subset-discovery-guadalajara-2026-04-22" / "ageb_feature_table.csv")
    consensus_rows = _read_csv(root / "reports" / "ageb-unconstrained-subset-search-guadalajara-2026-04-22" / "consensus_members.csv")
    y_rows = _read_csv(root / "reports" / "ageb-city-native-experiments-guadalajara-2026-04-22" / "ageb_y_unit_counts.csv")

    consensus_map = {r["unit_code"]: int(r["in_consensus_core"]) for r in consensus_rows}
    rows = []
    for r in feature_rows:
        code = r["unit_code"]
        in_core = consensus_map.get(code, 0)
        rows.append(
            {
                "unit_code": code,
                "unit_label": r["unit_label"],
                "in_consensus_core": in_core,
                "population": _safe_float(r["population"]),
                "est_count": _safe_float(r["est_count"]),
                "area_km2": _safe_float(r["area_km2"]),
                "dist_to_center_km": _safe_float(r["dist_to_center_km"]),
                "population_density": _safe_float(r["population_density"]),
                "compactness": _safe_float(r["compactness"]),
                "neighbor_degree": _safe_float(r["neighbor_degree"]),
            }
        )

    core = [r for r in rows if int(r["in_consensus_core"]) == 1]
    rest = [r for r in rows if int(r["in_consensus_core"]) == 0]

    feature_names = [
        "population",
        "est_count",
        "area_km2",
        "dist_to_center_km",
        "population_density",
        "compactness",
        "neighbor_degree",
    ]
    feature_summary = []
    for feat in feature_names:
        core_vals = [float(r[feat]) for r in core]
        rest_vals = [float(r[feat]) for r in rest]
        all_vals = [float(r[feat]) for r in rows]
        denom = _std(all_vals)
        z_gap = (_mean(core_vals) - _mean(rest_vals)) / denom if denom > 0 else 0.0
        feature_summary.append(
            {
                "feature": feat,
                "core_mean": _mean(core_vals),
                "rest_mean": _mean(rest_vals),
                "core_std": _std(core_vals),
                "rest_std": _std(rest_vals),
                "z_gap": z_gap,
            }
        )
    feature_summary.sort(key=lambda r: abs(float(r["z_gap"])), reverse=True)
    _write_csv(outdir / "feature_contrast_summary.csv", feature_summary, list(feature_summary[0].keys()))

    unit_totals: dict[str, float] = defaultdict(float)
    scian2_counts: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    size_counts: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for r in y_rows:
        code = r["unit_code"]
        fam = r["family"]
        cat = r["category"]
        val = _safe_float(r["y_value"])
        if fam == "total" and cat == "all":
            unit_totals[code] = val
        elif fam == "scian2":
            scian2_counts[code][cat] += val
        elif fam == "size_class":
            size_counts[code][cat] += val

    scian2_categories = sorted({cat for d in scian2_counts.values() for cat in d})
    size_categories = sorted({cat for d in size_counts.values() for cat in d})

    def share_stats(count_map: dict[str, dict[str, float]], categories: list[str], label: str) -> list[dict[str, object]]:
        rows_out = []
        for cat in categories:
            core_share_vals = []
            rest_share_vals = []
            for r in core:
                total = max(unit_totals.get(r["unit_code"], 0.0), 1e-9)
                core_share_vals.append(count_map[r["unit_code"]].get(cat, 0.0) / total)
            for r in rest:
                total = max(unit_totals.get(r["unit_code"], 0.0), 1e-9)
                rest_share_vals.append(count_map[r["unit_code"]].get(cat, 0.0) / total)
            core_share = _mean(core_share_vals)
            rest_share = _mean(rest_share_vals)
            log2_ratio = math.log(max(core_share, 1e-9) / max(rest_share, 1e-9), 2)
            rows_out.append(
                {
                    "family": label,
                    "category": cat,
                    "core_share": core_share,
                    "rest_share": rest_share,
                    "difference": core_share - rest_share,
                    "log2_ratio": log2_ratio,
                }
            )
        rows_out.sort(key=lambda r: abs(float(r["log2_ratio"])), reverse=True)
        return rows_out

    scian2_enrichment = share_stats(scian2_counts, scian2_categories, "scian2")
    size_enrichment = share_stats(size_counts, size_categories, "size_class")
    _write_csv(outdir / "scian2_enrichment.csv", scian2_enrichment, list(scian2_enrichment[0].keys()))
    _write_csv(outdir / "size_class_enrichment.csv", size_enrichment, list(size_enrichment[0].keys()))

    all_scian2 = [r for r in scian2_enrichment if float(r["core_share"]) > 0.005 or float(r["rest_share"]) > 0.005]
    all_sizes = [r for r in size_enrichment if float(r["core_share"]) > 0.005 or float(r["rest_share"]) > 0.005]

    all_feature_vectors = {}
    vector_features = ["population", "est_count", "area_km2", "dist_to_center_km", "population_density", "compactness", "neighbor_degree"]
    means = {feat: _mean([float(r[feat]) for r in rows]) for feat in vector_features}
    stds = {feat: max(_std([float(r[feat]) for r in rows]), 1e-9) for feat in vector_features}
    for r in rows:
        code = r["unit_code"]
        vec = []
        for feat in vector_features:
            vec.append((float(r[feat]) - means[feat]) / stds[feat])
        total = max(unit_totals.get(code, 0.0), 1e-9)
        for cat in scian2_categories:
            vec.append(scian2_counts[code].get(cat, 0.0) / total)
        for cat in size_categories:
            vec.append(size_counts[code].get(cat, 0.0) / total)
        all_feature_vectors[code] = vec

    core_lookup = {r["unit_code"]: r for r in core}
    pair_rows = []
    for a, b in combinations([r["unit_code"] for r in core], 2):
        pair_rows.append(
            {
                "unit_a": a,
                "unit_b": b,
                "similarity": _cosine(all_feature_vectors[a], all_feature_vectors[b]),
                "dist_a": core_lookup[a]["dist_to_center_km"],
                "dist_b": core_lookup[b]["dist_to_center_km"],
                "dens_a": core_lookup[a]["population_density"],
                "dens_b": core_lookup[b]["population_density"],
            }
        )
    pair_rows.sort(key=lambda r: float(r["similarity"]), reverse=True)
    top_pairs = pair_rows[:20]
    _write_csv(outdir / "core_similarity_pairs_top20.csv", top_pairs, list(top_pairs[0].keys()))

    core_core_sim = [float(r["similarity"]) for r in pair_rows]
    core_rest_sim = []
    rest_codes = [r["unit_code"] for r in rest]
    for c in [r["unit_code"] for r in core]:
        for rc in rest_codes[:150]:
            core_rest_sim.append(_cosine(all_feature_vectors[c], all_feature_vectors[rc]))
    sim_summary = [
        {"group": "core_core", "mean_similarity": _mean(core_core_sim), "std_similarity": _std(core_core_sim), "n_pairs": len(core_core_sim)},
        {"group": "core_rest_sample", "mean_similarity": _mean(core_rest_sim), "std_similarity": _std(core_rest_sim), "n_pairs": len(core_rest_sim)},
    ]
    _write_csv(outdir / "similarity_summary.csv", sim_summary, list(sim_summary[0].keys()))

    fig1 = _write_feature_contrast(figdir / "feature_contrast.svg", feature_summary)
    fig2 = _write_enrichment(
        figdir / "scian2_enrichment.svg",
        all_scian2[:12],
        "SCIAN2 enrichment in the consensus core",
        "Positive values mean the category has higher share inside the consensus core than in the rest of Guadalajara.",
    )
    fig3 = _write_enrichment(
        figdir / "size_class_enrichment.svg",
        all_sizes[:8],
        "Size-class enrichment in the consensus core",
        "This compares average establishment-size composition inside versus outside the consensus core.",
    )
    fig4 = _write_similarity(figdir / "core_similarity_pairs.svg", top_pairs[:14])
    _write_csv(
        figdir / "figures_manifest.csv",
        [
            {"figure_id": "feature_contrast", "path": str(fig1.resolve()), "description": "Feature gaps between consensus core and rest."},
            {"figure_id": "scian2_enrichment", "path": str(fig2.resolve()), "description": "SCIAN2 enrichment in consensus core."},
            {"figure_id": "size_class_enrichment", "path": str(fig3.resolve()), "description": "Size-class enrichment in consensus core."},
            {"figure_id": "core_similarity_pairs", "path": str(fig4.resolve()), "description": "Most similar core-core pairs."},
        ],
        ["figure_id", "path", "description"],
    )

    lines = [
        "# Consensus Core Versus Rest in Guadalajara AGEB",
        "",
        "This dossier studies what the unconstrained consensus core has in common after selection. It does not use those features to build the core.",
        "",
        "## Core size",
        f"- consensus core AGEB = `{len(core)}`",
        f"- rest AGEB = `{len(rest)}`",
        "",
        "## Strongest feature contrasts",
    ]
    for row in feature_summary[:6]:
        lines.append(
            f"- `{row['feature']}`: core mean `{float(row['core_mean']):.3f}` vs rest `{float(row['rest_mean']):.3f}`, `Δz = {float(row['z_gap']):+.2f}`"
        )
    lines.extend(
        [
            "",
            "## Similarity",
            f"- mean core-core similarity = `{sim_summary[0]['mean_similarity']:.3f}`",
            f"- mean core-rest sampled similarity = `{sim_summary[1]['mean_similarity']:.3f}`",
            "",
            "## Files",
            f"- [feature_contrast_summary.csv]({(outdir / 'feature_contrast_summary.csv').resolve()})",
            f"- [scian2_enrichment.csv]({(outdir / 'scian2_enrichment.csv').resolve()})",
            f"- [size_class_enrichment.csv]({(outdir / 'size_class_enrichment.csv').resolve()})",
            f"- [core_similarity_pairs_top20.csv]({(outdir / 'core_similarity_pairs_top20.csv').resolve()})",
            f"- [similarity_summary.csv]({(outdir / 'similarity_summary.csv').resolve()})",
            "",
            "## Figures",
            f"- [feature_contrast.svg]({fig1.resolve()})",
            f"- [scian2_enrichment.svg]({fig2.resolve()})",
            f"- [size_class_enrichment.svg]({fig3.resolve()})",
            f"- [core_similarity_pairs.svg]({fig4.resolve()})",
        ]
    )
    (outdir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

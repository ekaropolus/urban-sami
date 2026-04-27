#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
import shutil
from collections import defaultdict
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


def _mean(values):
    return sum(values) / float(len(values)) if values else 0.0


def _std(values):
    if len(values) < 2:
        return 0.0
    mu = _mean(values)
    return math.sqrt(sum((v - mu) ** 2 for v in values) / float(len(values) - 1))


def _distance(a, b, feats, means, stds):
    total = 0.0
    for f in feats:
        d = stds[f] if stds[f] > 0 else 1.0
        total += (((float(a[f]) - float(b[f])) / d) ** 2)
    return math.sqrt(total)


def _assign(rows, centroids, feats, means, stds):
    groups = [[] for _ in centroids]
    for r in rows:
        dists = []
        for c in centroids:
            total = 0.0
            for f in feats:
                d = stds[f] if stds[f] > 0 else 1.0
                total += (((float(r[f]) - c[f]) / d) ** 2)
            dists.append(math.sqrt(total))
        idx = min(range(len(dists)), key=lambda i: dists[i])
        groups[idx].append(r)
    return groups


def _recenter(groups, feats):
    centroids = []
    for g in groups:
        centroids.append({f: _mean([float(r[f]) for r in g]) for f in feats})
    return centroids


def _kmeans(rows, feats, k):
    centroids = [{f: float(rows[i][f]) for f in feats} for i in [0, len(rows)//2, len(rows)-1][:k]]
    means = {f: _mean([float(r[f]) for r in rows]) for f in feats}
    stds = {f: max(_std([float(r[f]) for r in rows]), 1e-9) for f in feats}
    for _ in range(25):
        groups = _assign(rows, centroids, feats, means, stds)
        nonempty = [g for g in groups if g]
        centroids = _recenter(nonempty, feats)
        if len(centroids) == len(nonempty):
            groups = nonempty
    return groups, means, stds


def _svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding='utf-8'
    )
    return path


def _write_cluster_table(path: Path, rows):
    width = 1240
    col_x = [44, 130, 340, 500, 660, 830, 1010]
    row_h = 28
    top = 106
    height = top + row_h * len(rows) + 50
    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">Latent classes of internal structure</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">Cities clustered only by internal signature, then inspected against SAMI.</text>',
    ]
    headers = ["class", "city", "SAMI", "m0_r2", "density_gain", "consensus_r2", "consensus_share"]
    for x, h in zip(col_x, headers):
        body.append(f'<text x="{x}" y="{top-18}" font-size="12" font-family="{SANS}" fill="{MUTED}">{h}</text>')
    for i, row in enumerate(rows):
        y = top + i * row_h
        vals = [
            str(row["cluster"]),
            str(row["city_name"]),
            f'{float(row["city_sami"]):+.3f}',
            f'{float(row["ageb_m0_r2"]):.3f}',
            f'{float(row["density_gain"]):.3f}',
            f'{float(row["consensus_r2"]):.3f}',
            f'{float(row["consensus_share"]):.3f}',
        ]
        for x, v in zip(col_x, vals):
            body.append(f'<text x="{x}" y="{y}" font-size="12" font-family="{SANS}" fill="{TEXT}">{v}</text>')
    return _svg(path, width, height, "".join(body))


def _write_pair_table(path: Path, rows):
    width = 1360
    col_x = [44, 250, 500, 760, 970, 1130, 1250]
    row_h = 28
    top = 106
    height = top + row_h * len(rows) + 50
    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        '<text x="44" y="50" font-size="26" font-family="Georgia, \'Times New Roman\', serif" fill="#1f1f1f">Same-SAMI, different-inside city pairs</text>',
        '<text x="44" y="74" font-size="14" font-family="Helvetica, Arial, sans-serif" fill="#625d54">Pairs with very small SAMI distance but large internal-structure distance.</text>',
    ]
    headers = ["city_a", "city_b", "SAMI dist", "internal dist", "class_a", "class_b", "same_class"]
    for x, h in zip(col_x, headers):
        body.append(f'<text x="{x}" y="{top-18}" font-size="12" font-family="{SANS}" fill="{MUTED}">{h}</text>')
    for i, row in enumerate(rows):
        y = top + i * row_h
        vals = [
            str(row["city_name_a"]),
            str(row["city_name_b"]),
            f'{float(row["sami_distance"]):.3f}',
            f'{float(row["internal_distance"]):.3f}',
            str(row["cluster_a"]),
            str(row["cluster_b"]),
            str(int(row["same_cluster"])),
        ]
        for x, v in zip(col_x, vals):
            body.append(f'<text x="{x}" y="{y}" font-size="12" font-family="{SANS}" fill="{TEXT}">{v}</text>')
    return _svg(path, width, height, "".join(body))


def main():
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "city-sami-latent-classes-2026-04-22"
    figdir = outdir / "figures"
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir.mkdir(parents=True, exist_ok=True)

    rows = _read_csv(root / "reports" / "city-sami-vs-internal-structure-2026-04-22" / "city_internal_signatures.csv")
    pair_rows = _read_csv(root / "reports" / "city-sami-vs-internal-structure-2026-04-22" / "city_pair_distances.csv")
    feats = [
        "ageb_m0_beta", "ageb_m0_r2", "density_gain", "distance_gain", "full_gain",
        "subset40_r2", "subset80_r2", "subset120_r2", "consensus_r2", "consensus_beta",
        "consensus_share", "mean_log_density", "std_log_density", "mean_share_81",
        "mean_share_46", "mean_share_54", "mean_share_micro"
    ]
    groups, means, stds = _kmeans(rows, feats, 3)
    cluster_rows = []
    city_cluster = {}
    summary_rows = []
    for idx, g in enumerate(groups, start=1):
        samis = [float(r["city_sami"]) for r in g]
        summary_rows.append(
            {
                "cluster": idx,
                "n_cities": len(g),
                "mean_sami": _mean(samis),
                "std_sami": _std(samis),
                "mean_m0_r2": _mean([float(r["ageb_m0_r2"]) for r in g]),
                "mean_density_gain": _mean([float(r["density_gain"]) for r in g]),
                "mean_consensus_r2": _mean([float(r["consensus_r2"]) for r in g]),
                "mean_consensus_share": _mean([float(r["consensus_share"]) for r in g]),
            }
        )
        for r in sorted(g, key=lambda x: float(x["city_sami"])):
            city_cluster[r["city_code"]] = idx
            cluster_rows.append({**r, "cluster": idx})
    _write_csv(outdir / "cluster_members.csv", cluster_rows, list(cluster_rows[0].keys()))
    _write_csv(outdir / "cluster_summary.csv", summary_rows, list(summary_rows[0].keys()))

    extreme_pairs = []
    for r in pair_rows:
        sdist = float(r["sami_distance"])
        idist = float(r["internal_distance"])
        r["cluster_a"] = city_cluster.get(r["city_code_a"], "")
        r["cluster_b"] = city_cluster.get(r["city_code_b"], "")
        r["same_cluster"] = 1 if r["cluster_a"] == r["cluster_b"] else 0
        if sdist <= 0.03:
            extreme_pairs.append(r)
    extreme_pairs.sort(key=lambda r: float(r["internal_distance"]), reverse=True)
    top_extreme = extreme_pairs[:15]
    _write_csv(outdir / "same_sami_different_inside_pairs.csv", top_extreme, list(top_extreme[0].keys()))

    # within/between cluster internal distance and SAMI spread
    by_cluster_pairs = []
    rows_by_code = {r["city_code"]: r for r in rows}
    for i in range(len(rows)):
        for j in range(i+1, len(rows)):
            a = rows[i]; b = rows[j]
            di = _distance(a, b, feats, means, stds)
            by_cluster_pairs.append(
                {
                    "same_cluster": 1 if city_cluster[a["city_code"]] == city_cluster[b["city_code"]] else 0,
                    "internal_distance": di,
                    "sami_distance": abs(float(a["city_sami"]) - float(b["city_sami"])),
                }
            )
    relation_summary = [
        {
            "group": "same_cluster",
            "mean_internal_distance": _mean([float(r["internal_distance"]) for r in by_cluster_pairs if int(r["same_cluster"]) == 1]),
            "mean_sami_distance": _mean([float(r["sami_distance"]) for r in by_cluster_pairs if int(r["same_cluster"]) == 1]),
        },
        {
            "group": "different_cluster",
            "mean_internal_distance": _mean([float(r["internal_distance"]) for r in by_cluster_pairs if int(r["same_cluster"]) == 0]),
            "mean_sami_distance": _mean([float(r["sami_distance"]) for r in by_cluster_pairs if int(r["same_cluster"]) == 0]),
        },
    ]
    _write_csv(outdir / "cluster_relation_summary.csv", relation_summary, list(relation_summary[0].keys()))

    fig1 = _write_cluster_table(figdir / "cluster_members.svg", cluster_rows)
    fig2 = _write_pair_table(figdir / "same_sami_different_inside_pairs.svg", top_extreme)
    _write_csv(
        figdir / "figures_manifest.csv",
        [
            {"figure_id": "cluster_members", "path": str(fig1.resolve()), "description": "Cluster members by internal structure."},
            {"figure_id": "same_sami_different_inside_pairs", "path": str(fig2.resolve()), "description": "Same-SAMI different-inside pairs."},
        ],
        ["figure_id", "path", "description"],
    )

    lines = [
        "# Latent Classes and Same-SAMI Different-Inside Pairs",
        "",
        "Cities were clustered only by internal signature, not by SAMI.",
        "",
        "## Cluster summaries",
    ]
    for r in summary_rows:
        lines.append(
            f"- cluster `{r['cluster']}`: `n={int(r['n_cities'])}`, `mean SAMI={float(r['mean_sami']):+.3f}`, `mean m0 R²={float(r['mean_m0_r2']):.3f}`, `mean consensus R²={float(r['mean_consensus_r2']):.3f}`"
        )
    lines.extend(
        [
            "",
            "## Files",
            f"- [cluster_members.csv]({(outdir / 'cluster_members.csv').resolve()})",
            f"- [cluster_summary.csv]({(outdir / 'cluster_summary.csv').resolve()})",
            f"- [same_sami_different_inside_pairs.csv]({(outdir / 'same_sami_different_inside_pairs.csv').resolve()})",
            f"- [cluster_relation_summary.csv]({(outdir / 'cluster_relation_summary.csv').resolve()})",
            "",
            "## Figures",
            f"- [cluster_members.svg]({fig1.resolve()})",
            f"- [same_sami_different_inside_pairs.svg]({fig2.resolve()})",
        ]
    )
    (outdir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

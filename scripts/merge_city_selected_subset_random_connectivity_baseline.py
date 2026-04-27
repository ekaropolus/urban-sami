#!/usr/bin/env python3
from __future__ import annotations

import csv
from pathlib import Path


BG = "#f8f6f1"
PANEL = "#fffdf8"
GRID = "#ddd6c8"
TEXT = "#1f1f1f"
MUTED = "#625d54"
TEAL = "#0f766e"
RUST = "#b14d3b"
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


def _svg(path: Path, width: int, height: int, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">{body}</svg>',
        encoding="utf-8",
    )
    return path


def _write_metric_rank(path: Path, rows: list[dict[str, object]], metric: str, title: str) -> Path:
    width = 1180
    left = 270
    right = 70
    top = 96
    bottom = 78
    row_h = 28
    height = top + len(rows) * row_h + bottom
    vals = [abs(float(r[metric])) for r in rows]
    xmax = max(vals) * 1.1 if vals else 1.0

    def x0() -> float:
        return left + (width - left - right) / 2.0

    def px(v: float) -> float:
        return x0() + ((v / max(xmax, 1e-9)) * (width - left - right) / 2.0)

    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{title}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Selected subset against 40 random same-size AGEB subsets within the same city.</text>',
        f'<line x1="{x0():.2f}" y1="{top-16}" x2="{x0():.2f}" y2="{height-bottom+8}" stroke="{GRID}" stroke-width="1.5"/>',
    ]
    for i, row in enumerate(rows):
        y = top + i * row_h
        v = float(row[metric])
        color = TEAL if v >= 0 else RUST
        body.append(f'<text x="{left-12}" y="{y+5:.2f}" text-anchor="end" font-size="12" font-family="{SANS}" fill="{TEXT}">{row["city_name"]}</text>')
        body.append(f'<line x1="{x0():.2f}" y1="{y:.2f}" x2="{px(v):.2f}" y2="{y:.2f}" stroke="{color}" stroke-width="8"/>')
        anchor = "start" if v >= 0 else "end"
        label_x = px(v) + (8 if v >= 0 else -8)
        body.append(f'<text x="{label_x:.2f}" y="{y+4:.2f}" text-anchor="{anchor}" font-size="11" font-family="{SANS}" fill="{TEXT}">{v:.2f}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "city-selected-subset-random-connectivity-baseline-2026-04-23"
    figdir = outdir / "figures"
    shard_dirs = [outdir / "shard_a", outdir / "shard_b", outdir / "shard_c", outdir / "shard_d"]

    selected_rows = []
    random_rows = []
    summary_rows = []
    for shard in shard_dirs:
        selected_rows.extend(_read_csv(shard / "city_selected_subset_metrics.csv"))
        random_rows.extend(_read_csv(shard / "city_random_subset_metrics.csv"))
        summary_rows.extend(_read_csv(shard / "city_selected_vs_random_summary.csv"))

    selected_rows.sort(key=lambda r: (r["state_code"], r["city_code"]))
    random_rows.sort(key=lambda r: (r["state_code"], r["city_code"], int(r["sample_id"])))
    summary_rows.sort(key=lambda r: (r["metric"], r["state_code"], r["city_code"]))

    _write_csv(outdir / "city_selected_subset_metrics.csv", selected_rows, list(selected_rows[0].keys()))
    _write_csv(outdir / "city_random_subset_metrics.csv", random_rows, list(random_rows[0].keys()))
    _write_csv(outdir / "city_selected_vs_random_summary.csv", summary_rows, list(summary_rows[0].keys()))

    metric_signal_rows = []
    for metric in sorted({r["metric"] for r in summary_rows}):
        rows = [r for r in summary_rows if r["metric"] == metric]
        percentiles = sorted(float(r["selected_percentile"]) for r in rows)
        metric_signal_rows.append(
            {
                "metric": metric,
                "n_cities": len(rows),
                "n_selected_above_95pct": sum(float(r["selected_percentile"]) >= 0.95 for r in rows),
                "n_selected_above_90pct": sum(float(r["selected_percentile"]) >= 0.90 for r in rows),
                "n_selected_below_10pct": sum(float(r["selected_percentile"]) <= 0.10 for r in rows),
                "mean_z_vs_random": sum(float(r["z_vs_random"]) for r in rows) / len(rows),
                "median_percentile": percentiles[len(percentiles) // 2],
            }
        )
    _write_csv(outdir / "metric_signal_summary.csv", metric_signal_rows, list(metric_signal_rows[0].keys()))

    fig1 = _write_metric_rank(
        figdir / "boundary_entry_edges_per_km_z.svg",
        sorted((r for r in summary_rows if r["metric"] == "boundary_entry_edges_per_km"), key=lambda r: float(r["z_vs_random"]), reverse=True),
        "z_vs_random",
        "Boundary-entry roads per km: selected subset vs random baseline",
    )
    fig2 = _write_metric_rank(
        figdir / "intersection_density_z.svg",
        sorted((r for r in summary_rows if r["metric"] == "intersection_density_km2"), key=lambda r: float(r["z_vs_random"]), reverse=True),
        "z_vs_random",
        "Intersection density: selected subset vs random baseline",
    )
    fig3 = _write_metric_rank(
        figdir / "street_density_z.svg",
        sorted((r for r in summary_rows if r["metric"] == "street_density_km_per_km2"), key=lambda r: float(r["z_vs_random"]), reverse=True),
        "z_vs_random",
        "Street density: selected subset vs random baseline",
    )
    _write_csv(
        figdir / "figures_manifest.csv",
        [
            {"figure_id": "boundary_entry_edges_per_km_z", "path": str(fig1.resolve()), "description": "Z-score vs random within-city subsets."},
            {"figure_id": "intersection_density_z", "path": str(fig2.resolve()), "description": "Z-score vs random within-city subsets."},
            {"figure_id": "street_density_z", "path": str(fig3.resolve()), "description": "Z-score vs random within-city subsets."},
        ],
        ["figure_id", "path", "description"],
    )

    lines = [
        "# Selected Subsets vs Random Same-Size AGEB Subsets",
        "",
        "Merged result across all 20 cities. Each selected AGEB subset is compared against 40 random same-size subsets drawn from the same city.",
        "",
        "## Files",
        f"- [city_selected_subset_metrics.csv]({(outdir / 'city_selected_subset_metrics.csv').resolve()})",
        f"- [city_random_subset_metrics.csv]({(outdir / 'city_random_subset_metrics.csv').resolve()})",
        f"- [city_selected_vs_random_summary.csv]({(outdir / 'city_selected_vs_random_summary.csv').resolve()})",
        f"- [metric_signal_summary.csv]({(outdir / 'metric_signal_summary.csv').resolve()})",
        "",
        "## Figures",
        f"- [boundary_entry_edges_per_km_z.svg]({fig1.resolve()})",
        f"- [intersection_density_z.svg]({fig2.resolve()})",
        f"- [street_density_z.svg]({fig3.resolve()})",
    ]
    (outdir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

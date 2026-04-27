#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import subprocess
from pathlib import Path

import numpy as np


DOCKER_EXE = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DB_CONTAINER = "24-polisplexity-core-db-dev"
POSTGRES_USER = "postgres"
DB_NAME = "urban_sami_exp"

BG = "#f6f3ec"
PANEL = "#fffdf9"
GRID = "#ddd6c8"
TEXT = "#1f1f1f"
MUTED = "#6b665d"
TEAL = "#0f766e"
RUST = "#b14d3b"
SERIF = "Georgia, 'Times New Roman', serif"
SANS = "Helvetica, Arial, sans-serif"


def _psql(sql: str) -> str:
    cmd = [
        DOCKER_EXE,
        "exec",
        "-i",
        DB_CONTAINER,
        "psql",
        "-U",
        POSTGRES_USER,
        "-d",
        DB_NAME,
        "-At",
        "-F",
        "\t",
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    ]
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return proc.stdout


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


def _slug(text: str) -> str:
    return text.replace("/", "-").replace(" ", "_")


def _query_rows(source_method: str) -> list[dict[str, object]]:
    sql = f"""
    SELECT city_code, city_name, state_code, population::text, city_area_km2::text,
           n_nodes::text, n_edges::text, street_density_km_per_km2::text,
           boundary_entry_edges_per_km::text, mean_degree::text
    FROM derived.city_network_metrics
    WHERE source_method = '{source_method}'
      AND n_nodes IS NOT NULL
      AND n_edges IS NOT NULL
      AND city_area_km2 IS NOT NULL
      AND street_density_km_per_km2 IS NOT NULL
      AND boundary_entry_edges_per_km IS NOT NULL
      AND mean_degree IS NOT NULL
    ORDER BY city_code;
    """
    out = _psql(sql)
    rows: list[dict[str, object]] = []
    for line in out.splitlines():
        city_code, city_name, state_code, population, area, n_nodes, n_edges, street_density, boundary_per_km, mean_degree = line.split("\t")
        rows.append(
            {
                "city_code": city_code,
                "city_name": city_name,
                "state_code": state_code,
                "population": float(population),
                "city_area_km2": float(area),
                "n_nodes": float(n_nodes),
                "n_edges": float(n_edges),
                "street_density_km_per_km2": float(street_density),
                "boundary_entry_edges_per_km": float(boundary_per_km),
                "mean_degree": float(mean_degree),
            }
        )
    return rows


def _feature_matrix(rows: list[dict[str, object]]) -> tuple[np.ndarray, list[str]]:
    feature_names = [
        "log_n_nodes",
        "log_city_area_km2",
        "log_street_density",
        "log_boundary_entry_edges_per_km",
        "mean_degree",
    ]
    X = []
    for row in rows:
        X.append(
            [
                math.log1p(float(row["n_nodes"])),
                math.log1p(float(row["city_area_km2"])),
                math.log1p(float(row["street_density_km_per_km2"])),
                math.log1p(float(row["boundary_entry_edges_per_km"])),
                float(row["mean_degree"]),
            ]
        )
    return np.asarray(X, dtype=float), feature_names


def _standardize(X: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    mu = X.mean(axis=0)
    sigma = X.std(axis=0)
    sigma[sigma == 0] = 1.0
    Z = (X - mu) / sigma
    return Z, mu, sigma


def _kmeans(Z: np.ndarray, k: int, restarts: int = 30, max_iter: int = 200, seed: int = 42) -> tuple[np.ndarray, np.ndarray, float]:
    rng = np.random.default_rng(seed)
    best_labels = None
    best_centers = None
    best_inertia = None
    n = Z.shape[0]
    for _ in range(restarts):
        centers = Z[rng.choice(n, size=k, replace=False)].copy()
        labels = np.zeros(n, dtype=int)
        for _it in range(max_iter):
            d2 = ((Z[:, None, :] - centers[None, :, :]) ** 2).sum(axis=2)
            new_labels = d2.argmin(axis=1)
            if np.array_equal(new_labels, labels):
                break
            labels = new_labels
            new_centers = centers.copy()
            for j in range(k):
                mask = labels == j
                if not mask.any():
                    new_centers[j] = Z[rng.integers(0, n)]
                else:
                    new_centers[j] = Z[mask].mean(axis=0)
            centers = new_centers
        inertia = float(((Z - centers[labels]) ** 2).sum())
        if best_inertia is None or inertia < best_inertia:
            best_inertia = inertia
            best_labels = labels.copy()
            best_centers = centers.copy()
    assert best_labels is not None and best_centers is not None and best_inertia is not None
    return best_labels, best_centers, best_inertia


def _cluster_names(centers: np.ndarray) -> dict[int, str]:
    size = centers[:, 0]
    area = centers[:, 1]
    density = 0.6 * centers[:, 2] + 0.4 * centers[:, 3]
    remaining = set(range(len(centers)))
    names: dict[int, str] = {}

    dense_large = int(np.argmax(size + density))
    names[dense_large] = "Large dense metropolitan"
    remaining.remove(dense_large)

    sparse_large = max(remaining, key=lambda i: size[i] - density[i] + 0.25 * area[i])
    names[sparse_large] = "Large expansive sparse"
    remaining.remove(sparse_large)

    dense_small = max(remaining, key=lambda i: density[i] - size[i])
    names[dense_small] = "Small dense compact"
    remaining.remove(dense_small)

    sparse_small = max(remaining, key=lambda i: -size[i] - density[i] + 0.25 * area[i])
    names[sparse_small] = "Small sparse rural"
    remaining.remove(sparse_small)

    for i in remaining:
        names[i] = "Intermediate mixed"
    return names


def _representatives(rows: list[dict[str, object]], Z: np.ndarray, labels: np.ndarray, centers: np.ndarray) -> dict[int, int]:
    reps: dict[int, int] = {}
    for j in range(centers.shape[0]):
        idx = np.where(labels == j)[0]
        d2 = ((Z[idx] - centers[j]) ** 2).sum(axis=1)
        reps[j] = int(idx[int(np.argmin(d2))])
    return reps


def _summary_svg(path: Path, class_rows: list[dict[str, str]]) -> Path:
    width = 1180
    row_h = 52
    top = 106
    height = top + row_h * len(class_rows) + 48
    body = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="18" y="18" width="{width-36}" height="{height-36}" rx="20" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="42" y="50" font-size="30" font-family="{SERIF}" fill="{TEXT}">City Network Typology</text>',
        f'<text x="42" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">National typology using persisted OSM city networks. One representative city per class.</text>',
    ]
    cols = [
        (42, "Class"),
        (330, "Representative"),
        (620, "Nodes"),
        (720, "Street Density"),
        (880, "Boundary Entries/km"),
        (1060, "Members"),
    ]
    for x, label in cols:
        body.append(f'<text x="{x}" y="{top-18}" font-size="12" font-family="{SANS}" fill="{MUTED}">{label}</text>')
    for i, row in enumerate(class_rows):
        y = top + i * row_h
        body.append(f'<line x1="36" y1="{y+18}" x2="{width-36}" y2="{y+18}" stroke="{GRID}"/>')
        body.append(f'<text x="42" y="{y}" font-size="18" font-family="{SERIF}" fill="{TEXT}">{row["class_label"]}</text>')
        body.append(f'<text x="330" y="{y}" font-size="16" font-family="{SANS}" fill="{TEAL}">{row["representative_city"]}</text>')
        body.append(f'<text x="620" y="{y}" font-size="14" font-family="{SANS}" fill="{TEXT}">{row["rep_n_nodes"]}</text>')
        body.append(f'<text x="720" y="{y}" font-size="14" font-family="{SANS}" fill="{TEXT}">{row["rep_street_density"]}</text>')
        body.append(f'<text x="880" y="{y}" font-size="14" font-family="{SANS}" fill="{TEXT}">{row["rep_boundary_entry"]}</text>')
        body.append(f'<text x="1060" y="{y}" font-size="14" font-family="{SANS}" fill="{TEXT}">{row["n_members"]}</text>')
    return _svg(path, width, height, "".join(body))


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an interpretable typology of persisted city OSM networks and choose one representative city per class.")
    parser.add_argument("--source-method", default="osm_drive_municipal_full_v1")
    parser.add_argument("--k", type=int, default=5)
    parser.add_argument("--output-dir", type=Path, default=Path("reports/city-network-typology-2026-04-24"))
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    out_dir = root / args.output_dir
    fig_dir = out_dir / "figures"
    rep_dir = out_dir / "representatives"
    fig_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)

    rows = _query_rows(args.source_method)
    X, feature_names = _feature_matrix(rows)
    Z, mu, sigma = _standardize(X)
    labels, centers, inertia = _kmeans(Z, k=args.k)
    label_names = _cluster_names(centers)
    reps = _representatives(rows, Z, labels, centers)

    member_rows: list[dict[str, str]] = []
    summary_rows: list[dict[str, str]] = []
    for cluster_id in sorted(set(labels)):
        idx = np.where(labels == cluster_id)[0]
        rep_idx = reps[cluster_id]
        rep = rows[rep_idx]
        class_label = label_names[cluster_id]
        rep_subdir = rep_dir / f'{cluster_id:02d}_{_slug(rep["city_name"])}'
        subprocess.run(
            [
                "python3",
                str(root / "scripts" / "render_city_network_ageb_denue_overlay.py"),
                "--source-method",
                args.source_method,
                "--city-code",
                str(rep["city_code"]),
                "--output-dir",
                str(rep_subdir.relative_to(root)),
            ],
            cwd=str(root),
            check=True,
        )
        summary_rows.append(
            {
                "cluster_id": str(cluster_id),
                "class_label": class_label,
                "n_members": str(len(idx)),
                "representative_city_code": str(rep["city_code"]),
                "representative_city": str(rep["city_name"]),
                "representative_state_code": str(rep["state_code"]),
                "rep_n_nodes": f'{rep["n_nodes"]:.0f}',
                "rep_city_area_km2": f'{rep["city_area_km2"]:.2f}',
                "rep_street_density": f'{rep["street_density_km_per_km2"]:.3f}',
                "rep_boundary_entry": f'{rep["boundary_entry_edges_per_km"]:.3f}',
                "rep_mean_degree": f'{rep["mean_degree"]:.3f}',
                "rep_overlay_path": str((rep_subdir / "figures" / "urban_zoom_overlay.svg").resolve()),
                "rep_denue_path": str((rep_subdir / "figures" / "urban_zoom_denue_emphasis.svg").resolve()),
                "centroid_log_n_nodes_z": f"{centers[cluster_id,0]:+.3f}",
                "centroid_log_area_z": f"{centers[cluster_id,1]:+.3f}",
                "centroid_log_street_density_z": f"{centers[cluster_id,2]:+.3f}",
                "centroid_log_boundary_entry_z": f"{centers[cluster_id,3]:+.3f}",
                "centroid_mean_degree_z": f"{centers[cluster_id,4]:+.3f}",
            }
        )
        for i in idx:
            row = rows[i]
            member_rows.append(
                {
                    "city_code": str(row["city_code"]),
                    "city_name": str(row["city_name"]),
                    "state_code": str(row["state_code"]),
                    "population": f'{row["population"]:.0f}',
                    "city_area_km2": f'{row["city_area_km2"]:.6f}',
                    "n_nodes": f'{row["n_nodes"]:.0f}',
                    "n_edges": f'{row["n_edges"]:.0f}',
                    "street_density_km_per_km2": f'{row["street_density_km_per_km2"]:.6f}',
                    "boundary_entry_edges_per_km": f'{row["boundary_entry_edges_per_km"]:.6f}',
                    "mean_degree": f'{row["mean_degree"]:.6f}',
                    "cluster_id": str(cluster_id),
                    "class_label": class_label,
                    "is_representative": "1" if i == rep_idx else "0",
                }
            )

    summary_rows.sort(key=lambda r: r["class_label"])
    member_rows.sort(key=lambda r: (r["class_label"], r["state_code"], r["city_code"]))

    _write_csv(out_dir / "cluster_summary.csv", summary_rows, list(summary_rows[0].keys()))
    _write_csv(out_dir / "cluster_members.csv", member_rows, list(member_rows[0].keys()))
    _write_csv(
        out_dir / "model_metadata.csv",
        [
            {
                "source_method": args.source_method,
                "k": str(args.k),
                "n_cities": str(len(rows)),
                "features": ",".join(feature_names),
                "inertia": f"{inertia:.6f}",
                "feature_means": ",".join(f"{v:.6f}" for v in mu),
                "feature_stds": ",".join(f"{v:.6f}" for v in sigma),
            }
        ],
        ["source_method", "k", "n_cities", "features", "inertia", "feature_means", "feature_stds"],
    )
    _summary_svg(fig_dir / "class_summary.svg", summary_rows)

    report_lines = [
        "# City Network Typology",
        "",
        f"Source method: `{args.source_method}`",
        f"Cities classified: `{len(rows)}`",
        f"Classes: `{args.k}`",
        "",
        "Files:",
        f"- [cluster_summary.csv]({(out_dir / 'cluster_summary.csv').resolve()})",
        f"- [cluster_members.csv]({(out_dir / 'cluster_members.csv').resolve()})",
        f"- [model_metadata.csv]({(out_dir / 'model_metadata.csv').resolve()})",
        f"- [class_summary.svg]({(fig_dir / 'class_summary.svg').resolve()})",
        "",
        "Representative cities by class:",
    ]
    for row in summary_rows:
        report_lines.extend(
            [
                f'- `{row["class_label"]}`: [{row["representative_city"]}]({row["rep_overlay_path"]})',
                f'  DENUE emphasis: [{row["representative_city"]}]({row["rep_denue_path"]})',
            ]
        )
    (out_dir / "report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

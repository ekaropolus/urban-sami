from __future__ import annotations

from pathlib import Path

from urban_sami.artifacts.figures import (
    write_model_overview_figure,
    write_residual_histogram_figure,
    write_scale_comparison_figure,
    write_scaling_scatter_figure,
)


def test_write_model_overview_figure_creates_svg(tmp_path: Path):
    out = write_model_overview_figure(
        [
            {"fit_method": "ols", "beta": 0.95, "r2": 0.84},
            {"fit_method": "poisson", "beta": 0.96, "r2": 0.91},
        ],
        tmp_path / "overview.svg",
        title="Overview",
    )
    text = out.read_text(encoding="utf-8")
    assert out.exists()
    assert "<svg" in text
    assert "Overview" in text


def test_write_scaling_scatter_figure_creates_svg(tmp_path: Path):
    out = write_scaling_scatter_figure(
        [
            {"population": 100, "est_count": 10},
            {"population": 200, "est_count": 20},
            {"population": 400, "est_count": 39},
        ],
        tmp_path / "scatter.svg",
        title="Scatter",
        x_key="population",
        y_key="est_count",
        fit_alpha=-2.3,
        fit_beta=1.0,
        annotation="ols",
    )
    text = out.read_text(encoding="utf-8")
    assert out.exists()
    assert "<svg" in text
    assert "Scatter" in text


def test_write_residual_histogram_figure_creates_svg(tmp_path: Path):
    out = write_residual_histogram_figure(
        [-0.4, -0.1, 0.0, 0.1, 0.2, 0.35],
        tmp_path / "residuals.svg",
        title="Residuals",
    )
    text = out.read_text(encoding="utf-8")
    assert out.exists()
    assert "<svg" in text
    assert "Residuals" in text


def test_write_scale_comparison_figure_creates_svg(tmp_path: Path):
    out = write_scale_comparison_figure(
        [
            {"level": "state", "fit_method": "poisson", "units": 32, "beta": 0.959, "r2": 0.950},
            {"level": "city", "fit_method": "poisson", "units": 2469, "beta": 0.950, "r2": 0.919},
        ],
        tmp_path / "scale_compare.svg",
        title="Scale compare",
    )
    text = out.read_text(encoding="utf-8")
    assert out.exists()
    assert "<svg" in text
    assert "Scale compare" in text

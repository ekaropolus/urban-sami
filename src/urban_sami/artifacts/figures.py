from __future__ import annotations

import html
import math
import re
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
SERIF = "Georgia, 'Times New Roman', serif"
SANS = "Helvetica, Arial, sans-serif"


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return float(default)


def _safe_slug(text: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", str(text or "").strip().lower()).strip("_")
    return slug or "figure"


def _fmt_num(value: float, digits: int = 3) -> str:
    if abs(value) >= 1000:
        return f"{value:,.0f}"
    if abs(value) >= 100:
        return f"{value:.1f}"
    if abs(value) >= 10:
        return f"{value:.2f}"
    return f"{value:.{digits}f}"


def _fmt_int(value: float) -> str:
    return f"{int(round(value)):,}"


def _svg_document(width: int, height: int, body: str) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img">{body}</svg>'
    )


def _write_svg(path: str | Path, svg_text: str) -> Path:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(svg_text, encoding="utf-8")
    return output


def write_model_overview_figure(rows: list[dict], path: str | Path, *, title: str) -> Path:
    width = 980
    row_h = 34
    top = 102
    bottom = 56
    left = 250
    right = 80
    chart_w = width - left - right
    if not rows:
        svg = _svg_document(
            width,
            180,
            f'<rect width="{width}" height="180" fill="{BG}"/>'
            f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{html.escape(title)}</text>'
            f'<text x="44" y="98" font-size="16" font-family="{SANS}" fill="{MUTED}">No rows available.</text>',
        )
        return _write_svg(path, svg)

    height = top + (len(rows) * row_h) + bottom
    beta_values = [_to_float(row.get("beta")) for row in rows]
    beta_min = min(beta_values + [0.0])
    beta_max = max(beta_values + [1.0])
    if math.isclose(beta_min, beta_max):
        beta_min -= 0.25
        beta_max += 0.25
    beta_pad = (beta_max - beta_min) * 0.08
    beta_min -= beta_pad
    beta_max += beta_pad

    panel_gap = 68
    panel_w = (chart_w - panel_gap) / 2.0
    beta_x0 = left
    beta_x1 = left + panel_w
    r2_x0 = beta_x1 + panel_gap
    r2_x1 = r2_x0 + panel_w

    def beta_to_x(value: float) -> float:
        return beta_x0 + ((value - beta_min) / (beta_max - beta_min)) * panel_w

    def r2_to_x(value: float) -> float:
        return r2_x0 + (max(0.0, min(1.0, value)) * panel_w)

    parts = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{html.escape(title)}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Model coefficients and goodness of fit across candidate estimators</text>',
        f'<text x="{beta_x0}" y="88" font-size="14" font-family="{SANS}" fill="{MUTED}">Scaling exponent β</text>',
        f'<text x="{r2_x0}" y="88" font-size="14" font-family="{SANS}" fill="{MUTED}">Coefficient of determination R²</text>',
    ]

    for tick in range(5):
        value = beta_min + ((beta_max - beta_min) * tick / 4.0)
        x = beta_to_x(value)
        parts.append(f'<line x1="{x:.2f}" y1="{top-8}" x2="{x:.2f}" y2="{height-bottom+8}" stroke="{GRID}" stroke-width="1"/>')
        parts.append(f'<text x="{x:.2f}" y="{height-bottom+30}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">{value:.2f}</text>')

    for tick in range(6):
        value = tick / 5.0
        x = r2_to_x(value)
        parts.append(f'<line x1="{x:.2f}" y1="{top-8}" x2="{x:.2f}" y2="{height-bottom+8}" stroke="{GRID}" stroke-width="1"/>')
        parts.append(f'<text x="{x:.2f}" y="{height-bottom+30}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">{value:.1f}</text>')

    x_one = beta_to_x(1.0)
    parts.append(f'<line x1="{x_one:.2f}" y1="{top-8}" x2="{x_one:.2f}" y2="{height-bottom+8}" stroke="{RUST}" stroke-width="1.5" stroke-dasharray="6,5"/>')
    parts.append(f'<text x="{x_one+6:.2f}" y="{top-18}" font-size="11" font-family="{SANS}" fill="{RUST}">β = 1</text>')

    for idx, row in enumerate(rows):
        y = top + (idx * row_h)
        label = str(row.get("fit_method") or row.get("level") or row.get("indicator_key") or f"row_{idx+1}")
        beta = _to_float(row.get("beta"))
        r2 = _to_float(row.get("r2"))
        beta_low = row.get("beta_ci95_low", "")
        beta_high = row.get("beta_ci95_high", "")
        beta_low_f = _to_float(beta_low, beta)
        beta_high_f = _to_float(beta_high, beta)
        parts.append(f'<text x="{left-14}" y="{y+5}" text-anchor="end" font-size="13" font-family="{SANS}" fill="{TEXT}">{html.escape(label)}</text>')
        parts.append(f'<line x1="{left-6}" y1="{y:.2f}" x2="{r2_x1:.2f}" y2="{y:.2f}" stroke="#f2ede2" stroke-width="1"/>')
        if beta_low != "" and beta_high != "":
            parts.append(f'<line x1="{beta_to_x(beta_low_f):.2f}" y1="{y:.2f}" x2="{beta_to_x(beta_high_f):.2f}" y2="{y:.2f}" stroke="{BLUE}" stroke-width="2.4"/>')
        parts.append(f'<circle cx="{beta_to_x(beta):.2f}" cy="{y:.2f}" r="5.2" fill="{TEAL}"/>')
        parts.append(f'<circle cx="{r2_to_x(r2):.2f}" cy="{y:.2f}" r="5.2" fill="{BLUE}"/>')
        parts.append(f'<text x="{r2_x1+12:.2f}" y="{y+5:.2f}" font-size="11" font-family="{SANS}" fill="{MUTED}">β={beta:.3f}  R²={r2:.3f}</text>')

    return _write_svg(path, _svg_document(width, height, "".join(parts)))


def write_scaling_scatter_figure(
    rows: list[dict],
    path: str | Path,
    *,
    title: str,
    x_key: str,
    y_key: str,
    fit_alpha: float,
    fit_beta: float,
    annotation: str = "",
) -> Path:
    filtered = []
    for row in rows:
        x = _to_float(row.get(x_key), -1.0)
        y = _to_float(row.get(y_key), -1.0)
        if x > 0.0 and y > 0.0:
            filtered.append((x, y))
    width = 980
    height = 760
    left = 110
    right = 52
    top = 90
    bottom = 96
    plot_w = width - left - right
    plot_h = height - top - bottom
    if not filtered:
        svg = _svg_document(
            width,
            height,
            f'<rect width="{width}" height="{height}" fill="{BG}"/>'
            f'<text x="40" y="50" font-size="24" font-family="{SERIF}" fill="{TEXT}">{html.escape(title)}</text>',
        )
        return _write_svg(path, svg)

    xs = [math.log(x) for x, _ in filtered]
    ys = [math.log(y) for _, y in filtered]
    x_min, x_max = min(xs), max(xs)
    y_min, y_max = min(ys), max(ys)
    if math.isclose(x_min, x_max):
        x_min -= 1.0
        x_max += 1.0
    if math.isclose(y_min, y_max):
        y_min -= 1.0
        y_max += 1.0
    x_pad = (x_max - x_min) * 0.05
    y_pad = (y_max - y_min) * 0.05
    x_min -= x_pad
    x_max += x_pad
    y_min -= y_pad
    y_max += y_pad

    def px(log_x: float) -> float:
        return left + ((log_x - x_min) / (x_max - x_min)) * plot_w

    def py(log_y: float) -> float:
        return top + plot_h - ((log_y - y_min) / (y_max - y_min)) * plot_h

    parts = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{html.escape(title)}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Log-log scatter with fitted scaling line</text>',
    ]

    for tick in range(6):
        tx = x_min + ((x_max - x_min) * tick / 5.0)
        x = px(tx)
        parts.append(f'<line x1="{x:.2f}" y1="{top:.2f}" x2="{x:.2f}" y2="{top+plot_h:.2f}" stroke="{GRID}" stroke-width="1"/>')
        parts.append(f'<text x="{x:.2f}" y="{height-44}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">{_fmt_int(math.exp(tx))}</text>')
    for tick in range(6):
        ty = y_min + ((y_max - y_min) * tick / 5.0)
        y = py(ty)
        parts.append(f'<line x1="{left:.2f}" y1="{y:.2f}" x2="{left+plot_w:.2f}" y2="{y:.2f}" stroke="{GRID}" stroke-width="1"/>')
        parts.append(f'<text x="{left-16}" y="{y+4:.2f}" text-anchor="end" font-size="11" font-family="{SANS}" fill="{MUTED}">{_fmt_int(math.exp(ty))}</text>')

    parts.append(f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="none" stroke="{AXIS}" stroke-width="1.2"/>')
    parts.append(f'<text x="{left + plot_w/2:.2f}" y="{height-12}" text-anchor="middle" font-size="13" font-family="{SANS}" fill="{MUTED}">{html.escape(x_key.replace("_", " "))}</text>')
    parts.append(f'<text x="26" y="{top + plot_h/2:.2f}" text-anchor="middle" font-size="13" font-family="{SANS}" fill="{MUTED}" transform="rotate(-90 26 {top + plot_h/2:.2f})">{html.escape(y_key.replace("_", " "))}</text>')

    radius = 1.6 if len(filtered) > 1500 else 2.2
    opacity = 0.18 if len(filtered) > 1500 else 0.30
    for log_x, log_y in zip(xs, ys):
        parts.append(f'<circle cx="{px(log_x):.2f}" cy="{py(log_y):.2f}" r="{radius:.1f}" fill="{BLUE}" fill-opacity="{opacity}"/>')

    line_x0 = x_min
    line_x1 = x_max
    line_y0 = fit_alpha + (fit_beta * line_x0)
    line_y1 = fit_alpha + (fit_beta * line_x1)
    parts.append(
        f'<line x1="{px(line_x0):.2f}" y1="{py(line_y0):.2f}" '
        f'x2="{px(line_x1):.2f}" y2="{py(line_y1):.2f}" stroke="{RUST}" stroke-width="2.8"/>'
    )

    annot = annotation or f"log(Y) = {fit_alpha:.3f} + {fit_beta:.3f} log(N)"
    parts.append(f'<rect x="{left+18}" y="{top+18}" width="330" height="60" rx="10" fill="#fffdfa" stroke="{GRID}"/>')
    parts.append(f'<text x="{left+32}" y="{top+43}" font-size="13" font-family="{SANS}" fill="{TEXT}">{html.escape(annot)}</text>')
    parts.append(f'<text x="{left+32}" y="{top+63}" font-size="12" font-family="{SANS}" fill="{MUTED}">Included units: {_fmt_int(len(filtered))}</text>')

    return _write_svg(path, _svg_document(width, height, "".join(parts)))


def write_residual_histogram_figure(
    residuals: list[float],
    path: str | Path,
    *,
    title: str,
    subtitle: str = "",
    bins: int = 24,
) -> Path:
    values = [float(v) for v in residuals]
    width = 980
    height = 620
    left = 92
    right = 42
    top = 90
    bottom = 86
    plot_w = width - left - right
    plot_h = height - top - bottom
    if not values:
        svg = _svg_document(width, height, f'<rect width="{width}" height="{height}" fill="{BG}"/>')
        return _write_svg(path, svg)

    vmin = min(values)
    vmax = max(values)
    if math.isclose(vmin, vmax):
        vmin -= 1.0
        vmax += 1.0
    pad = (vmax - vmin) * 0.04
    vmin -= pad
    vmax += pad
    step = (vmax - vmin) / float(max(1, bins))
    counts = [0 for _ in range(max(1, bins))]
    for value in values:
        idx = int((value - vmin) / step)
        idx = max(0, min(len(counts) - 1, idx))
        counts[idx] += 1
    cmax = max(counts) or 1

    def px(value: float) -> float:
        return left + ((value - vmin) / (vmax - vmin)) * plot_w

    def py(value: float) -> float:
        return top + plot_h - (value / cmax) * plot_h

    parts = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{html.escape(title)}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">{html.escape(subtitle or "Distribution of log residuals around the fitted scaling relation")}</text>',
    ]

    for tick in range(6):
        xval = vmin + ((vmax - vmin) * tick / 5.0)
        x = px(xval)
        parts.append(f'<line x1="{x:.2f}" y1="{top:.2f}" x2="{x:.2f}" y2="{top+plot_h:.2f}" stroke="{GRID}" stroke-width="1"/>')
        parts.append(f'<text x="{x:.2f}" y="{height-40}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">{xval:.2f}</text>')

    for tick in range(5):
        yval = cmax * tick / 4.0
        y = py(yval)
        parts.append(f'<line x1="{left:.2f}" y1="{y:.2f}" x2="{left+plot_w:.2f}" y2="{y:.2f}" stroke="{GRID}" stroke-width="1"/>')
        parts.append(f'<text x="{left-14}" y="{y+4:.2f}" text-anchor="end" font-size="11" font-family="{SANS}" fill="{MUTED}">{_fmt_int(yval)}</text>')

    parts.append(f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="none" stroke="{AXIS}" stroke-width="1.2"/>')
    parts.append(f'<line x1="{px(0.0):.2f}" y1="{top:.2f}" x2="{px(0.0):.2f}" y2="{top+plot_h:.2f}" stroke="{RUST}" stroke-width="1.6" stroke-dasharray="6,5"/>')

    for idx, count in enumerate(counts):
        x0 = vmin + (idx * step)
        x1 = x0 + step
        parts.append(
            f'<rect x="{px(x0)+0.7:.2f}" y="{py(count):.2f}" width="{max(0.8, px(x1)-px(x0)-1.4):.2f}" '
            f'height="{top+plot_h-py(count):.2f}" fill="{BLUE}" fill-opacity="0.82"/>'
        )

    mean = sum(values) / len(values)
    sd = math.sqrt(sum((v - mean) ** 2 for v in values) / max(1, len(values) - 1))
    parts.append(f'<rect x="{left+18}" y="{top+18}" width="250" height="66" rx="10" fill="#fffdfa" stroke="{GRID}"/>')
    parts.append(f'<text x="{left+32}" y="{top+42}" font-size="13" font-family="{SANS}" fill="{TEXT}">n = {_fmt_int(len(values))}</text>')
    parts.append(f'<text x="{left+32}" y="{top+61}" font-size="12" font-family="{SANS}" fill="{MUTED}">mean = {mean:.3f}</text>')
    parts.append(f'<text x="{left+132}" y="{top+61}" font-size="12" font-family="{SANS}" fill="{MUTED}">sd = {sd:.3f}</text>')

    return _write_svg(path, _svg_document(width, height, "".join(parts)))


def write_scale_comparison_figure(rows: list[dict], path: str | Path, *, title: str) -> Path:
    width = 980
    height = 560
    left = 170
    right = 72
    top = 110
    bottom = 72
    plot_w = width - left - right
    plot_h = height - top - bottom
    if not rows:
        svg = _svg_document(width, height, f'<rect width="{width}" height="{height}" fill="{BG}"/>')
        return _write_svg(path, svg)

    ordered = sorted(rows, key=lambda row: _to_float(row.get("units"), 0.0), reverse=True)
    beta_values = [_to_float(row.get("beta")) for row in ordered]
    beta_min = min(beta_values + [0.0])
    beta_max = max(beta_values + [1.0])
    if math.isclose(beta_min, beta_max):
        beta_min -= 0.25
        beta_max += 0.25
    beta_pad = (beta_max - beta_min) * 0.1
    beta_min -= beta_pad
    beta_max += beta_pad

    r2_x0 = left + (plot_w * 0.58)
    beta_x0 = left
    beta_x1 = r2_x0 - 34
    r2_x1 = left + plot_w
    row_h = plot_h / max(1, len(ordered))

    def beta_to_x(value: float) -> float:
        return beta_x0 + ((value - beta_min) / (beta_max - beta_min)) * (beta_x1 - beta_x0)

    def r2_to_x(value: float) -> float:
        return r2_x0 + max(0.0, min(1.0, value)) * (r2_x1 - r2_x0)

    parts = [
        f'<rect width="{width}" height="{height}" fill="{BG}"/>',
        f'<rect x="20" y="18" width="{width-40}" height="{height-36}" rx="18" fill="{PANEL}" stroke="{GRID}"/>',
        f'<text x="44" y="50" font-size="26" font-family="{SERIF}" fill="{TEXT}">{html.escape(title)}</text>',
        f'<text x="44" y="74" font-size="14" font-family="{SANS}" fill="{MUTED}">Best-fitting model by scale, showing β and R² together</text>',
        f'<text x="{beta_x0}" y="95" font-size="14" font-family="{SANS}" fill="{MUTED}">β</text>',
        f'<text x="{r2_x0}" y="95" font-size="14" font-family="{SANS}" fill="{MUTED}">R²</text>',
    ]

    for tick in range(5):
        val = beta_min + ((beta_max - beta_min) * tick / 4.0)
        x = beta_to_x(val)
        parts.append(f'<line x1="{x:.2f}" y1="{top-10}" x2="{x:.2f}" y2="{height-bottom+6}" stroke="{GRID}" stroke-width="1"/>')
        parts.append(f'<text x="{x:.2f}" y="{height-bottom+28}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">{val:.2f}</text>')
    for tick in range(6):
        val = tick / 5.0
        x = r2_to_x(val)
        parts.append(f'<line x1="{x:.2f}" y1="{top-10}" x2="{x:.2f}" y2="{height-bottom+6}" stroke="{GRID}" stroke-width="1"/>')
        parts.append(f'<text x="{x:.2f}" y="{height-bottom+28}" text-anchor="middle" font-size="11" font-family="{SANS}" fill="{MUTED}">{val:.1f}</text>')

    parts.append(f'<line x1="{beta_to_x(1.0):.2f}" y1="{top-10}" x2="{beta_to_x(1.0):.2f}" y2="{height-bottom+6}" stroke="{RUST}" stroke-width="1.5" stroke-dasharray="6,5"/>')

    for idx, row in enumerate(ordered):
        y = top + ((idx + 0.5) * row_h)
        label = str(row.get("level") or row.get("domain_id") or f"row_{idx+1}")
        beta = _to_float(row.get("beta"))
        r2 = _to_float(row.get("r2"))
        units = _to_float(row.get("units") or row.get("n_obs"))
        fit_method = str(row.get("fit_method") or "")
        parts.append(f'<line x1="{left-10}" y1="{y:.2f}" x2="{r2_x1:.2f}" y2="{y:.2f}" stroke="#f2ede2" stroke-width="1"/>')
        parts.append(f'<text x="{left-16}" y="{y+4:.2f}" text-anchor="end" font-size="14" font-family="{SANS}" fill="{TEXT}">{html.escape(label)}</text>')
        parts.append(f'<circle cx="{beta_to_x(beta):.2f}" cy="{y:.2f}" r="6" fill="{TEAL}"/>')
        parts.append(f'<circle cx="{r2_to_x(r2):.2f}" cy="{y:.2f}" r="6" fill="{BLUE}"/>')
        parts.append(f'<text x="{r2_x1+10:.2f}" y="{y+4:.2f}" font-size="11" font-family="{SANS}" fill="{MUTED}">{fit_method}, n={_fmt_int(units)}</text>')

    return _write_svg(path, _svg_document(width, height, "".join(parts)))


def figure_slug(*parts: str) -> str:
    return _safe_slug("_".join(str(part or "") for part in parts))

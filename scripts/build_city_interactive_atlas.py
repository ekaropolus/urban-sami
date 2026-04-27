#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path
from urllib.request import urlopen


WINDOWS_DOCKER = "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
DOCKER_EXE = os.environ.get("DOCKER_EXE") or shutil.which("docker") or WINDOWS_DOCKER
DB_CONTAINER = os.environ.get("DB_CONTAINER", "24-polisplexity-core-db-dev")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
DB_NAME = os.environ.get("DB_NAME", "urban_sami_exp")

SOURCE_METHOD = "osm_drive_municipal_full_v1"
BOUNDARY_TOL = 0.00015
ROAD_TOL = 0.00005
ROAD_REGEX = "(motorway|trunk|primary|secondary|tertiary)"

LEAFLET_VERSION = "1.9.4"
LEAFLET_CSS_URL = f"https://unpkg.com/leaflet@{LEAFLET_VERSION}/dist/leaflet.css"
LEAFLET_JS_URL = f"https://unpkg.com/leaflet@{LEAFLET_VERSION}/dist/leaflet.js"

STATE_NAMES = {
    "01": "Aguascalientes",
    "02": "Baja California",
    "03": "Baja California Sur",
    "04": "Campeche",
    "05": "Coahuila",
    "06": "Colima",
    "07": "Chiapas",
    "08": "Chihuahua",
    "09": "Ciudad de México",
    "10": "Durango",
    "11": "Guanajuato",
    "12": "Guerrero",
    "13": "Hidalgo",
    "14": "Jalisco",
    "15": "México",
    "16": "Michoacán",
    "17": "Morelos",
    "18": "Nayarit",
    "19": "Nuevo León",
    "20": "Oaxaca",
    "21": "Puebla",
    "22": "Querétaro",
    "23": "Quintana Roo",
    "24": "San Luis Potosí",
    "25": "Sinaloa",
    "26": "Sonora",
    "27": "Tabasco",
    "28": "Tamaulipas",
    "29": "Tlaxcala",
    "30": "Veracruz",
    "31": "Yucatán",
    "32": "Zacatecas",
}


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


def _sql_text(value: object) -> str:
    return str(value).replace("'", "''")


def _download(url: str, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return path
    with urlopen(url, timeout=60) as resp:  # noqa: S310
        path.write_bytes(resp.read())
    return path


def _manifest_rows(limit: int = 0) -> list[dict[str, object]]:
    limit_sql = f"LIMIT {limit}" if limit else ""
    sql = f"""
    WITH city_pop AS (
        SELECT city_code,
               COALESCE(MAX(city_name), '') AS city_name,
               MAX(population) AS population,
               MAX(households) AS occupied_dwellings
        FROM raw.population_units
        WHERE level = 'city'
        GROUP BY city_code
    ),
    city_est AS (
        SELECT city_code, COUNT(*)::bigint AS est_total
        FROM raw.denue_establishments
        WHERE city_code <> ''
        GROUP BY city_code
    ),
    city_manzana AS (
        SELECT city_code, COUNT(*)::bigint AS manzana_count
        FROM raw.population_units
        WHERE level = 'manzana'
        GROUP BY city_code
    ),
    city_ageb AS (
        SELECT city_code, COUNT(*)::bigint AS ageb_count
        FROM raw.admin_units
        WHERE level = 'ageb_u'
        GROUP BY city_code
    )
    SELECT
        m.city_code,
        COALESCE(p.city_name, m.city_name) AS city_name,
        m.state_code,
        COALESCE(p.population, 0)::text,
        COALESCE(p.occupied_dwellings, 0)::text,
        COALESCE(e.est_total, 0)::text,
        COALESCE(z.manzana_count, 0)::text,
        COALESCE(a.ageb_count, 0)::text,
        COALESCE(m.n_nodes, 0)::text,
        COALESCE(m.n_edges, 0)::text,
        COALESCE(m.boundary_entry_edges, 0)::text,
        COALESCE(m.street_density_km_per_km2, 0)::text,
        ROUND(ST_X(ST_PointOnSurface(g.geom))::numeric, 6)::text,
        ROUND(ST_Y(ST_PointOnSurface(g.geom))::numeric, 6)::text,
        ROUND(ST_XMin(g.geom)::numeric, 6)::text,
        ROUND(ST_YMin(g.geom)::numeric, 6)::text,
        ROUND(ST_XMax(g.geom)::numeric, 6)::text,
        ROUND(ST_YMax(g.geom)::numeric, 6)::text
    FROM derived.city_network_metrics m
    JOIN derived.city_network_geoms g
      ON g.source_method = m.source_method
     AND g.city_code = m.city_code
    LEFT JOIN city_pop p
      ON p.city_code = m.city_code
    LEFT JOIN city_est e
      ON e.city_code = m.city_code
    LEFT JOIN city_manzana z
      ON z.city_code = m.city_code
    LEFT JOIN city_ageb a
      ON a.city_code = m.city_code
    WHERE m.source_method = '{SOURCE_METHOD}'
    ORDER BY m.city_code
    {limit_sql};
    """
    rows: list[dict[str, object]] = []
    for line in _psql(sql).splitlines():
        if not line.strip():
            continue
        (
            city_code,
            city_name,
            state_code,
            population,
            occupied_dwellings,
            est_total,
            manzana_count,
            ageb_count,
            n_nodes,
            n_edges,
            boundary_entry_edges,
            street_density,
            lon,
            lat,
            minx,
            miny,
            maxx,
            maxy,
        ) = line.split("\t")
        rows.append(
            {
                "city_code": city_code,
                "city_name": city_name,
                "state_code": state_code,
                "state_name": STATE_NAMES.get(state_code, state_code),
                "population": int(float(population or 0)),
                "occupied_dwellings": int(float(occupied_dwellings or 0)),
                "est_total": int(float(est_total or 0)),
                "manzana_count": int(float(manzana_count or 0)),
                "ageb_count": int(float(ageb_count or 0)),
                "n_nodes": int(float(n_nodes or 0)),
                "n_edges": int(float(n_edges or 0)),
                "boundary_entry_edges": int(float(boundary_entry_edges or 0)),
                "street_density_km_per_km2": float(street_density or 0.0),
                "center": [float(lat), float(lon)],
                "bbox": [[float(miny), float(minx)], [float(maxy), float(maxx)]],
            }
        )
    return rows


def _city_payload(city_code: str) -> dict[str, object]:
    sql = f"""
    WITH boundary AS (
        SELECT ST_SimplifyPreserveTopology(geom, {BOUNDARY_TOL}) AS geom
        FROM derived.city_network_geoms
        WHERE source_method = '{SOURCE_METHOD}'
          AND city_code = '{_sql_text(city_code)}'
    ),
    major_roads AS (
        SELECT ST_LineMerge(
                   ST_UnaryUnion(
                       ST_Collect(
                           ST_Simplify(geom, {ROAD_TOL})
                       )
                   )
               ) AS geom
        FROM derived.city_network_edges
        WHERE source_method = '{SOURCE_METHOD}'
          AND city_code = '{_sql_text(city_code)}'
          AND highway ~ '{ROAD_REGEX}'
    )
    SELECT
        COALESCE(ST_AsGeoJSON((SELECT geom FROM boundary)), ''),
        COALESCE(ST_AsGeoJSON((SELECT geom FROM major_roads)), '');
    """
    line = _psql(sql).strip()
    boundary_json, roads_json = (line.split("\t", 1) + [""])[:2]
    payload: dict[str, object] = {"boundary": None, "roads_major": None}
    if boundary_json:
        payload["boundary"] = json.loads(boundary_json)
    if roads_json:
        payload["roads_major"] = json.loads(roads_json)
    return payload


def _write_json(path: Path, obj: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    return path


def _write_js_assignment(path: Path, global_name: str, obj: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    path.write_text(f"window.{global_name} = {payload};\n", encoding="utf-8")
    return path


def _write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _index_html() -> str:
    return """<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Urban SAMI Interactive Atlas</title>
  <link rel="stylesheet" href="./assets/leaflet.css" />
  <link rel="stylesheet" href="./assets/app.css" />
</head>
<body>
  <div class="layout">
    <aside class="sidebar">
      <div class="sidebar-header">
        <h1>Atlas de Ciudades</h1>
        <p>2478 ciudades · red vial persistida · navegación interactiva</p>
      </div>
      <div class="filters">
        <label class="filter-field">
          <span>Estado</span>
          <select id="state-select" class="select">
            <option value="">Todos los estados</option>
          </select>
        </label>
        <label class="filter-field">
          <span>Ciudad</span>
          <select id="city-select" class="select">
            <option value="">Selecciona una ciudad…</option>
          </select>
        </label>
        <button id="city-open" class="open-btn" type="button">Abrir ciudad</button>
      </div>
      <input id="city-search" class="search" type="search" placeholder="Busca ciudad o código..." />
      <div id="city-count" class="meta-line"></div>
      <div id="city-list" class="city-list"></div>
    </aside>
    <main class="map-shell">
      <div class="toolbar">
        <span class="pill">Municipio</span>
        <span class="pill">Red principal</span>
        <span class="pill">Click para abrir detalle</span>
      </div>
      <div id="map"></div>
    </main>
  </div>
  <script src="./assets/leaflet.js"></script>
  <script src="./data/manifest.js"></script>
  <script src="./assets/index.js"></script>
</body>
</html>
"""


def _city_html() -> str:
    return """<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Urban SAMI City Atlas</title>
  <link rel="stylesheet" href="./assets/leaflet.css" />
  <link rel="stylesheet" href="./assets/app.css" />
</head>
<body>
  <div class="detail-layout">
    <header class="detail-header">
      <div>
        <a class="back-link" href="./index.html">← volver al atlas</a>
        <h1 id="city-title">Ciudad</h1>
        <div id="city-subtitle" class="meta-line"></div>
      </div>
      <div class="stats" id="city-stats"></div>
    </header>
    <div class="detail-main">
      <div id="detail-map"></div>
      <aside class="detail-panel">
        <h2>Capas</h2>
        <p class="meta-line">El mapa carga el contorno municipal y la red vial principal extraída desde la base.</p>
        <ul class="legend">
          <li><span class="swatch boundary"></span> contorno municipal</li>
          <li><span class="swatch roads"></span> red principal extraída</li>
          <li><span class="swatch marker"></span> centro de referencia</li>
        </ul>
        <div id="city-notes" class="notes"></div>
      </aside>
    </div>
  </div>
  <script src="./assets/leaflet.js"></script>
  <script src="./data/manifest.js"></script>
  <script src="./assets/city.js"></script>
</body>
</html>
"""


def _app_css() -> str:
    return """*{box-sizing:border-box}html,body{height:100%;margin:0;font-family:ui-sans-serif,system-ui,-apple-system,sans-serif;background:#f5f1e8;color:#1c1b17}a{color:#0f766e;text-decoration:none}.layout{display:grid;grid-template-columns:360px 1fr;height:100vh}.sidebar{background:#fffdf8;border-right:1px solid #ddd3c0;padding:20px 16px;overflow:auto}.sidebar-header h1{margin:0 0 6px;font-size:28px;line-height:1.1}.sidebar-header p,.meta-line{margin:0;color:#6b665d;font-size:13px;line-height:1.4}.filters{display:grid;grid-template-columns:1fr;gap:10px;margin:16px 0 10px}.filter-field{display:flex;flex-direction:column;gap:6px;font-size:12px;color:#5a5448}.select,.search{width:100%;padding:11px 12px;border-radius:10px;border:1px solid #d8cfbd;background:#fff}.open-btn{padding:11px 12px;border:none;border-radius:10px;background:#0f766e;color:#fff;font-weight:600;cursor:pointer}.open-btn:hover{background:#0b5f59}.city-list{display:flex;flex-direction:column;gap:8px;margin-top:12px}.city-link{display:block;padding:10px 12px;border:1px solid #e3dac8;border-radius:12px;background:#fff;color:#1c1b17}.city-link:hover{border-color:#0f766e;background:#f5fbfa}.city-link strong{display:block;font-size:15px}.city-link span{display:block;color:#6b665d;font-size:12px;margin-top:4px}.map-shell{display:flex;flex-direction:column;min-width:0}.toolbar{display:flex;gap:8px;padding:12px 16px;border-bottom:1px solid #ddd3c0;background:#fffdf8}.pill{display:inline-flex;align-items:center;padding:6px 10px;border-radius:999px;background:#efe8d8;color:#5a5448;font-size:12px}#map,#detail-map{flex:1;min-height:0}.detail-layout{display:flex;flex-direction:column;height:100vh}.detail-header{display:flex;justify-content:space-between;gap:16px;align-items:flex-start;padding:18px 22px;background:#fffdf8;border-bottom:1px solid #ddd3c0}.detail-header h1{margin:6px 0 4px;font-size:32px;line-height:1.1}.back-link{font-size:13px}.stats{display:grid;grid-template-columns:repeat(2,minmax(120px,1fr));gap:10px;min-width:min(460px,50vw)}.stat-card{background:#fff;border:1px solid #e2d8c5;border-radius:14px;padding:10px 12px}.stat-card .label{display:block;font-size:11px;color:#6b665d;text-transform:uppercase;letter-spacing:.03em}.stat-card .value{display:block;font-size:18px;font-weight:700;margin-top:4px}.detail-main{display:grid;grid-template-columns:minmax(0,1fr) 320px;flex:1;min-height:0}.detail-panel{padding:18px;background:#fffdf8;border-left:1px solid #ddd3c0;overflow:auto}.detail-panel h2{margin-top:0}.legend{list-style:none;padding:0;margin:14px 0;display:flex;flex-direction:column;gap:8px}.legend li{display:flex;align-items:center;gap:10px;font-size:14px}.swatch{display:inline-block;width:18px;height:8px;border-radius:999px}.swatch.boundary{background:#c2410c}.swatch.roads{background:#0ea5a4}.swatch.marker{background:#1d4ed8}.notes{font-size:13px;color:#4e4a42;line-height:1.5}@media (max-width: 980px){.layout{grid-template-columns:1fr}.sidebar{height:48vh;border-right:none;border-bottom:1px solid #ddd3c0}.detail-main{grid-template-columns:1fr}.detail-panel{border-left:none;border-top:1px solid #ddd3c0}.stats{grid-template-columns:repeat(2,minmax(100px,1fr));min-width:0}}"""


def _index_js() -> str:
    return """const rows = window.__CITY_MANIFEST__ || [];
const map = L.map('map', {preferCanvas: true, zoomControl: true});
const noBasemap = L.layerGroup();
const osm = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {maxZoom: 19, attribution: '&copy; OpenStreetMap'});
const carto = L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {maxZoom: 20, attribution: '&copy; OpenStreetMap &copy; CARTO'}).addTo(map);
L.control.layers({'Carto claro': carto, 'OSM': osm, 'Sin base': noBasemap}, {}, {collapsed: true}).addTo(map);
map.setView([23.5, -102.0], 5);
const cityLayer = L.layerGroup().addTo(map);
const listEl = document.getElementById('city-list');
const searchEl = document.getElementById('city-search');
const countEl = document.getElementById('city-count');
const stateSelect = document.getElementById('state-select');
const citySelect = document.getElementById('city-select');
const cityOpenBtn = document.getElementById('city-open');

function formatInt(value){return new Intl.NumberFormat('es-MX').format(value || 0);}

function cityPopup(row){
  return `<strong>${row.city_name}</strong><br/>city_code ${row.city_code}<br/>población ${formatInt(row.population)}<br/>establecimientos ${formatInt(row.est_total)}<br/><a href="./city.html?city=${row.city_code}">abrir detalle</a>`;
}

function renderList(rows){
  listEl.innerHTML = rows.map((row)=>`
    <a class="city-link" href="./city.html?city=${row.city_code}">
      <strong>${row.city_name}</strong>
      <span>${row.city_code} · ${row.state_name} · est ${formatInt(row.est_total)}</span>
    </a>
  `).join('');
  countEl.textContent = `${rows.length} ciudades visibles`;
}

function renderStateOptions(rows){
  const states = [...new Map(rows.map((row)=>[row.state_code, row.state_name])).entries()]
    .sort((a,b)=>a[1].localeCompare(b[1], 'es'));
  stateSelect.innerHTML = '<option value=\"\">Todos los estados</option>' +
    states.map(([code, name])=>`<option value=\"${code}\">${name}</option>`).join('');
}

function renderCityOptions(rows){
  citySelect.innerHTML = '<option value=\"\">Selecciona una ciudad…</option>' +
    rows.map((row)=>`<option value=\"${row.city_code}\">${row.city_name} · ${row.state_name}</option>`).join('');
}

function flyToCity(row){
  map.flyTo(row.center, Math.max(map.getZoom(), 10), {duration: 0.8});
}

if (!rows.length){
  countEl.textContent = 'No se pudo cargar el manifiesto local.';
} else {
  renderStateOptions(rows);
  function applyFilters(){
    const q = searchEl.value.trim().toLowerCase();
    const stateCode = stateSelect.value;
    const filtered = rows.filter((row)=>{
      const stateOk = !stateCode || row.state_code === stateCode;
      const textOk = !q ||
        row.city_name.toLowerCase().includes(q) ||
        row.city_code.toLowerCase().includes(q) ||
        row.state_name.toLowerCase().includes(q) ||
        row.state_code.toLowerCase().includes(q);
      return stateOk && textOk;
    });
    cityLayer.clearLayers();
    filtered.forEach((row)=>{
      const marker = L.circleMarker(row.center, {
        radius: Math.max(3, Math.min(10, 3 + Math.log10((row.est_total || 1) + 1))),
        color: '#0f766e',
        weight: 1,
        fillColor: '#14b8a6',
        fillOpacity: 0.5
      }).bindPopup(cityPopup(row));
      marker.on('click', ()=>window.location.href = `./city.html?city=${row.city_code}`);
      marker.addTo(cityLayer);
    });
    renderList(filtered);
    renderCityOptions(filtered);
    if (filtered.length){
      const bounds = L.latLngBounds(filtered.map((r)=>r.center));
      if (bounds.isValid()) map.fitBounds(bounds.pad(0.08));
    }
  }

  rows.forEach((row)=>{
    const marker = L.circleMarker(row.center, {
      radius: Math.max(3, Math.min(10, 3 + Math.log10((row.est_total || 1) + 1))),
      color: '#0f766e',
      weight: 1,
      fillColor: '#14b8a6',
      fillOpacity: 0.5
    }).bindPopup(cityPopup(row));
    marker.on('click', ()=>window.location.href = `./city.html?city=${row.city_code}`);
    marker.addTo(cityLayer);
  });
  const bounds = L.latLngBounds(rows.map((r)=>r.center));
  if (bounds.isValid()) map.fitBounds(bounds.pad(0.08));
  applyFilters();
  searchEl.addEventListener('input', applyFilters);
  stateSelect.addEventListener('change', applyFilters);
  citySelect.addEventListener('change', ()=>{
    const row = rows.find((r)=>r.city_code === citySelect.value);
    if (row) flyToCity(row);
  });
  cityOpenBtn.addEventListener('click', ()=>{
    if (citySelect.value) {
      window.location.href = `./city.html?city=${citySelect.value}`;
    }
  });
}"""


def _city_js() -> str:
    return """const manifest = window.__CITY_MANIFEST__ || [];
const params = new URLSearchParams(window.location.search);
const cityCode = params.get('city') || '';
const map = L.map('detail-map', {preferCanvas: true, zoomControl: true});
const noBasemap = L.layerGroup();
const osm = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {maxZoom: 19, attribution: '&copy; OpenStreetMap'});
const carto = L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {maxZoom: 20, attribution: '&copy; OpenStreetMap &copy; CARTO'}).addTo(map);
const overlays = {};
L.control.layers({'Carto claro': carto, 'OSM': osm, 'Sin base': noBasemap}, overlays, {collapsed: false}).addTo(map);

function formatInt(value){return new Intl.NumberFormat('es-MX').format(value || 0);}
function formatFloat(value){return new Intl.NumberFormat('es-MX', {maximumFractionDigits: 2}).format(value || 0);}

function statCard(label, value){
  return `<div class="stat-card"><span class="label">${label}</span><span class="value">${value}</span></div>`;
}

function asFeature(geometry){
  return geometry ? {type: 'Feature', properties: {}, geometry} : null;
}

function safeGeoJsonLayer(geometry, options, notes, label){
  if (!geometry) return null;
  try {
    return L.geoJSON(asFeature(geometry), options).addTo(map);
  } catch (err) {
    notes.push(`${label}: ${String(err)}`);
    return null;
  }
}

function loadCityPayload(code){
  return new Promise((resolve, reject)=>{
    if (!code) {
      reject(new Error('Falta parámetro city.'));
      return;
    }
    window.__CITY_PAYLOAD__ = undefined;
    const script = document.createElement('script');
    script.src = `./data/cities/${code}.js`;
    script.onload = ()=>resolve(window.__CITY_PAYLOAD__);
    script.onerror = ()=>reject(new Error(`No se pudo cargar ./data/cities/${code}.js`));
    document.body.appendChild(script);
  });
}

loadCityPayload(cityCode).then((payload)=>{
  const row = manifest.find((r)=>r.city_code === cityCode) || payload.meta;
  document.title = `${row.city_name} · Urban SAMI Atlas`;
  document.getElementById('city-title').textContent = row.city_name;
  document.getElementById('city-subtitle').textContent = `city_code ${row.city_code} · ${row.state_name}`;
  document.getElementById('city-stats').innerHTML = [
    statCard('Población', formatInt(row.population)),
    statCard('Establecimientos', formatInt(row.est_total)),
    statCard('Manzanas', formatInt(row.manzana_count)),
    statCard('AGEB', formatInt(row.ageb_count)),
    statCard('Nodos', formatInt(row.n_nodes)),
    statCard('Aristas', formatInt(row.n_edges)),
    statCard('Entradas borde', formatInt(row.boundary_entry_edges)),
    statCard('Street density', formatFloat(row.street_density_km_per_km2))
  ].join('');

  const notes = [];
  if (!row.ageb_count) notes.push('No hay AGEB urbana cargada para esta ciudad en la base actual.');
  notes.push('El contorno municipal viene del soporte persistido y la red vial es la capa extraída desde OSM que quedó en la base.');

  const boundaryLayer = safeGeoJsonLayer(payload.boundary, {
    style: {color:'#c2410c', weight:2, fillOpacity:0}
  }, notes, 'Contorno municipal');
  if (boundaryLayer) overlays['Contorno municipal'] = boundaryLayer;

  const roadsLayer = safeGeoJsonLayer(payload.roads_major, {
    style: {color:'#0ea5a4', weight:2, opacity:0.9}
  }, notes, 'Red principal');
  if (roadsLayer) overlays['Red principal extraída'] = roadsLayer;

  const marker = L.circleMarker(row.center, {
    radius: 6,
    color: '#1d4ed8',
    weight: 1,
    fillColor: '#2563eb',
    fillOpacity: 0.8
  }).bindPopup(row.city_name).addTo(map);
  overlays['Centro de referencia'] = marker;

  if (row.bbox && row.bbox.length === 2) {
    map.fitBounds(row.bbox, {padding: [20, 20]});
  } else if (boundaryLayer && boundaryLayer.getBounds && boundaryLayer.getBounds().isValid()) {
    map.fitBounds(boundaryLayer.getBounds(), {padding: [20, 20]});
  } else {
    map.setView(row.center, 12);
  }
  document.getElementById('city-notes').innerHTML = notes.map((x)=>`<p>${x}</p>`).join('');
}).catch((err)=>{
  document.getElementById('city-title').textContent = 'No se pudo cargar la ciudad';
  document.getElementById('city-notes').innerHTML = `<p>${String(err)}</p>`;
  map.setView([23.5, -102.0], 5);
});"""


def _report(manifest_rows: list[dict[str, object]], outdir: Path) -> str:
    return "\n".join(
        [
            "# City Interactive Atlas",
            "",
            f"- cities: `{len(manifest_rows)}`",
            f"- source_method: `{SOURCE_METHOD}`",
            "",
            "Files:",
            f"- [index.html]({(outdir / 'index.html').resolve()})",
            f"- [city.html]({(outdir / 'city.html').resolve()})",
            f"- [manifest.json]({(outdir / 'data' / 'manifest.json').resolve()})",
            "",
            "Run locally:",
            "```bash",
            f"cd {outdir}",
            "python3 -m http.server 8765",
            "```",
            "",
            "Then open:",
            "- `http://127.0.0.1:8765/index.html`",
        ]
    ) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an interactive HTML atlas for all persisted city OSM networks.")
    parser.add_argument("--limit", type=int, default=0, help="Optional limit for quick builds.")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    outdir = root / "reports" / "city-interactive-atlas-2026-04-24"
    assets = outdir / "assets"
    data_dir = outdir / "data"
    cities_dir = data_dir / "cities"
    outdir.mkdir(parents=True, exist_ok=True)
    assets.mkdir(parents=True, exist_ok=True)
    cities_dir.mkdir(parents=True, exist_ok=True)

    _download(LEAFLET_CSS_URL, assets / "leaflet.css")
    _download(LEAFLET_JS_URL, assets / "leaflet.js")
    _write_text(assets / "app.css", _app_css())
    _write_text(assets / "index.js", _index_js())
    _write_text(assets / "city.js", _city_js())
    _write_text(outdir / "index.html", _index_html())
    _write_text(outdir / "city.html", _city_html())

    manifest_rows = _manifest_rows(limit=args.limit)
    for idx, row in enumerate(manifest_rows, start=1):
        payload = _city_payload(str(row["city_code"]))
        payload["meta"] = row
        _write_json(cities_dir / f"{row['city_code']}.json", payload)
        _write_js_assignment(cities_dir / f"{row['city_code']}.js", "__CITY_PAYLOAD__", payload)
        if idx % 50 == 0 or idx == len(manifest_rows):
            print(f"[atlas] {idx}/{len(manifest_rows)} city payloads written")

    _write_json(data_dir / "manifest.json", manifest_rows)
    _write_js_assignment(data_dir / "manifest.js", "__CITY_MANIFEST__", manifest_rows)
    _write_text(outdir / "report.md", _report(manifest_rows, outdir))
    print(f"[atlas] done -> {outdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

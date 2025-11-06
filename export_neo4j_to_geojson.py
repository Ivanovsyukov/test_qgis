#!/usr/bin/env python3
# export_neo4j_to_geojson.py
# Использование: python3 export_neo4j_to_geojson.py <relation_type> <node_type> <metric_name>
# metric_name: 'leiden_community', 'louvain_community', 'pageRank', 'betweenness'

import json
import os
import sys
import random
import colorsys
from neo4j import GraphDatabase

def distinct_random_color():
    hue = random.random()
    saturation = 0.7 + random.random() * 0.3
    lightness = 0.4 + random.random() * 0.3
    rgb = colorsys.hls_to_rgb(hue, lightness, saturation)
    return '#{:02x}{:02x}{:02x}'.format(
        int(rgb[0] * 255),
        int(rgb[1] * 255),
        int(rgb[2] * 255)
    )

# === Чтение конфигурации ===
CONFIG_PATH = "config.json"

if not os.path.exists(CONFIG_PATH):
    print(f"❌ Конфиг {CONFIG_PATH} не найден. Создайте файл с параметрами подключения.")
    sys.exit(1)

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = json.load(f)

URI = config.get("uri", "bolt://localhost:7687")
USER = config.get("user", "neo4j")
PASSWORD = config.get("password", "neo4j")
DATABASE = config.get("database", "neo4j")
AUTH = (USER, PASSWORD)
DEBUG = config.get("debug", False)

def log_debug(msg):
    if DEBUG:
        print(msg)

# === Проверка аргументов ===
if len(sys.argv) < 4:
    print("Использование: python3 export_neo4j_to_geojson.py <relation_type> <node_type> <metric_name>")
    print("Пример: python3 export_neo4j_to_geojson.py БирскBusRouteSegment БирскBusStop pageRank")
    sys.exit(1)

relation_type = sys.argv[1]
node_type = sys.argv[2]
metric_name = sys.argv[3]  # например, 'leiden_community', 'pageRank' и т.п.

# Определяем тип визуализации
if metric_name in ("leiden_community", "louvain_community"):
    view_mode = "community"
elif metric_name in ("pageRank", "betweenness"):
    view_mode = "centrality"
else:
    print(f"❌ Неизвестная метрика: {metric_name}. Должна быть одна из: "
          "leiden_community, louvain_community, pageRank, betweenness")
    sys.exit(1)

driver = GraphDatabase.driver(URI, auth=AUTH)

def point_to_geojson(point):
    if not point:
        return None
    # Neo4j point: используем x,y (если в вашей БД longitude/latitude — поменяйте)
    return {"type": "Point", "coordinates": [point.x, point.y]}

# === Динамическое построение списка полей для RETURN ===
base_return = [
    "elementId(a) AS id_a",
    "a.name AS name_a",
    "a.location AS loc_a",
    "elementId(b) AS id_b",
    "b.name AS name_b",
    "b.location AS loc_b",
    "r.name AS rel_name",
    "r.duration AS duration",
    "r.route AS route"
]

if view_mode == "community":
    base_return += [
        "a.leiden_community AS leiden_a",
        "a.louvain_community AS louvain_a",
        "b.leiden_community AS leiden_b",
        "b.louvain_community AS louvain_b"
    ]
else:
    base_return += [
        "a.pageRank AS pageRank_a",
        "a.betweenness AS betweenness_a",
        "b.pageRank AS pageRank_b",
        "b.betweenness AS betweenness_b"
    ]

return_clause = ",\n    ".join(base_return)

query = f"""
MATCH (a:`{node_type}`)-[r:`{relation_type}`]->(b:`{node_type}`)
RETURN
    {return_clause}
"""

log_debug("Generated Cypher query:")
log_debug(query)

features_nodes = {}
features_links = []
colors_by_community = {}

# helper: mapping metric_name -> record key names
record_key_a = {
    "leiden_community": "leiden_a",
    "louvain_community": "louvain_a",
    "pageRank": "pageRank_a",
    "betweenness": "betweenness_a"
}
record_key_b = {
    "leiden_community": "leiden_b",
    "louvain_community": "louvain_b",
    "pageRank": "pageRank_b",
    "betweenness": "betweenness_b"
}

with driver.session(database=DATABASE) as session:
    result = session.run(query)
    for record in result:
        # безопасно получаем поля (get вернёт None, если нет)
        id_a = record.get("id_a")
        id_b = record.get("id_b")
        loc_a = record.get("loc_a")
        loc_b = record.get("loc_b")

        # community fields (may be None)
        leiden_a = record.get("leiden_a")
        louvain_a = record.get("louvain_a")
        leiden_b = record.get("leiden_b")
        louvain_b = record.get("louvain_b")

        # centrality fields (may be None)
        pageRank_a = record.get("pageRank_a")
        betweenness_a = record.get("betweenness_a")
        pageRank_b = record.get("pageRank_b")
        betweenness_b = record.get("betweenness_b")

        # --- Цвета для сообществ: используем первую доступную метку (leiden затем louvain) ---
        comm_key_a = leiden_a if leiden_a is not None else louvain_a
        comm_key_b = leiden_b if leiden_b is not None else louvain_b

        if comm_key_a is not None and comm_key_a not in colors_by_community:
            colors_by_community[comm_key_a] = distinct_random_color()
        if comm_key_b is not None and comm_key_b not in colors_by_community:
            colors_by_community[comm_key_b] = distinct_random_color()

        color_a = colors_by_community.get(comm_key_a, "#888888")
        color_b = colors_by_community.get(comm_key_b, "#888888")

        # --- Правильное определение значения метрики (чтобы не перепутать leiden/louvain) ---
        # используем маппинг с _a / _b ключами, если такого ключа нет — пробуем общий (редко нужно)
        metric_val_a = None
        metric_val_b = None
        key_a = record_key_a.get(metric_name)
        key_b = record_key_b.get(metric_name)
        if key_a:
            metric_val_a = record.get(key_a)
        if key_b:
            metric_val_b = record.get(key_b)
        # fallback: если нет явных алиасов, пытаться взять по простому имени
        if metric_val_a is None:
            metric_val_a = record.get(metric_name)
        if metric_val_b is None:
            metric_val_b = record.get(metric_name)

        # --- Узлы: сохраняем только если есть геометрия ---
        if id_a is not None and id_a not in features_nodes and loc_a:
            props_a = {
                "id": id_a,
                "name": record.get("name_a"),
                "leiden_community": leiden_a,
                "louvain_community": louvain_a,
                "pageRank": pageRank_a,
                "betweenness": betweenness_a,
                metric_name: metric_val_a,
                "color": color_a
            }
            popup_a = f"<div style='background:white;color:black;padding:6px;border-radius:4px;font-family:sans-serif;font-size:13px;'><b>{props_a['name']}</b><br>{metric_name}: {props_a.get(metric_name)}</div>"
            props_a["popup"] = popup_a

            features_nodes[id_a] = {
                "type": "Feature",
                "geometry": point_to_geojson(loc_a),
                "properties": props_a
            }

        if id_b is not None and id_b not in features_nodes and loc_b:
            props_b = {
                "id": id_b,
                "name": record.get("name_b"),
                "leiden_community": leiden_b,
                "louvain_community": louvain_b,
                "pageRank": pageRank_b,
                "betweenness": betweenness_b,
                metric_name: metric_val_b,
                "color": color_b
            }
            popup_b = f"<div style='background:white;color:black;padding:6px;border-radius:4px;font-family:sans-serif;font-size:13px;'><b>{props_b['name']}</b><br>{metric_name}: {props_b.get(metric_name)}</div>"
            props_b["popup"] = popup_b

            features_nodes[id_b] = {
                "type": "Feature",
                "geometry": point_to_geojson(loc_b),
                "properties": props_b
            }

        # --- Связи ---
        if loc_a and loc_b:
            features_links.append({
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [loc_a.x, loc_a.y],
                        [loc_b.x, loc_b.y]
                    ]
                },
                "properties": {
                    "name": record.get("rel_name"),
                    "duration": record.get("duration"),
                    "route": record.get("route")
                }
            })

driver.close()

geojson_nodes = {
    "type": "FeatureCollection",
    "features": list(features_nodes.values())
}

geojson_links = {
    "type": "FeatureCollection",
    "features": features_links
}

if DEBUG:
    with open(f"nodes_{node_type}.geojson", "w", encoding="utf-8") as f:
        json.dump(geojson_nodes, f, ensure_ascii=False, indent=2)
    with open(f"links_{relation_type}.geojson", "w", encoding="utf-8") as f:
        json.dump(geojson_links, f, ensure_ascii=False, indent=2)
    print("Colors by community (sample):")
    for k, v in colors_by_community.items():
        print(f"  {k}: {v}")

# === Генерация HTML ===
def generate_leaflet_html_inline(nodes_data, links_data, output_html="map.html", view_mode="community", metric_name="pageRank"):
    nodes_json = json.dumps(nodes_data, ensure_ascii=False)
    links_json = json.dumps(links_data, ensure_ascii=False)

    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>Neo4j Graph Map ({metric_name})</title>
    <meta charset="utf-8">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <style>
        body {{ margin: 0; padding: 0; }}
        #map {{ position: absolute; top: 0; bottom: 0; width: 100%; }}
    </style>
</head>
<body>
    <div id="map"></div>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    {'<script src="https://unpkg.com/leaflet.heat@0.2.0/dist/leaflet-heat.js"></script>' if view_mode=='centrality' else ''}

    <script>
        const nodesData = {nodes_json};
        const linksData = {links_json};
        const metricName = "{metric_name}";
        const viewMode = "{view_mode}";

        // Исправлено: двойные фигурные скобки для f-строки
        const tileUrl = viewMode === "community"
            ? "https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png"
            : "https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png";

        const map = L.map('map').setView([0, 0], 2);
        L.tileLayer(tileUrl, {{ maxZoom: 19 }}).addTo(map);

        if (viewMode === "community") {{
            // Простая отрисовка узлов без кластеризации
            const nodesLayer = L.geoJSON(nodesData, {{
                pointToLayer: (feature, latlng) => {{
                    const color = feature.properties?.color || '#3388ff';
                    const popup = feature.properties?.popup ?? '';
                    return L.circleMarker(latlng, {{
                        radius: 8,
                        color: '#000',
                        weight: 1,
                        fillColor: color,
                        fillOpacity: 0.9
                    }}).bindPopup(popup);
                }}
            }}).addTo(map);

            // Добавляем связи
            L.geoJSON(linksData, {{
                style: () => ({{ color: 'gray', weight: 1.2, opacity: 0.4 }})
            }}).addTo(map);
        }} 
        else {{
            // Тепловая карта
            const heatPoints = [];
            nodesData.features.forEach(f => {{
                const loc = f.geometry?.coordinates;
                const val = parseFloat(f.properties?.[metricName] || 0);
                if (loc && !isNaN(val)) heatPoints.push([loc[1], loc[0], val]);
            }});
            L.heatLayer(heatPoints, {{
                radius: 25,
                blur: 15,
                maxZoom: 10,
                minOpacity: 0.3
            }}).addTo(map);

            // Подписи
            L.geoJSON(nodesData, {{
                pointToLayer: (feature, latlng) => {{
                    const val = feature.properties?.[metricName] ?? 0;
                    const popup = `<b>${{feature.properties.name}}</b><br>${{metricName}}: ${{val}}`;
                    return L.circleMarker(latlng, {{
                        radius: 4,
                        color: '#000',
                        fillColor: '#fff',
                        fillOpacity: 0.6
                    }}).bindPopup(popup);
                }}
            }}).addTo(map);
        }}

        try {{
            const bounds = L.geoJSON(nodesData).getBounds();
            map.fitBounds(bounds, {{ padding: [20, 20] }});
        }} catch (e) {{
            console.warn('fitBounds failed:', e);
        }}
    </script>
</body>
</html>"""

    with open(output_html, "w", encoding="utf-8") as f:
        f.write(html_content)
    log_debug(f"✅ Карта ({metric_name}) сохранена в {output_html}")

# === Генерация карты ===
generate_leaflet_html_inline(geojson_nodes, geojson_links, view_mode=view_mode, metric_name=metric_name)
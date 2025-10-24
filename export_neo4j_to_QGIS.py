import sys
import json
from neo4j import GraphDatabase
import colorsys
import random

def distinct_random_color():
    # Генерируем равномерно распределенные оттенки
    hue = random.random()  # 0.0 - 1.0
    saturation = 0.7 + random.random() * 0.3  # 70-100%
    lightness = 0.4 + random.random() * 0.3  # 40-70%
    
    rgb = colorsys.hls_to_rgb(hue, lightness, saturation)
    return '#{:02x}{:02x}{:02x}'.format(
        int(rgb[0] * 255),
        int(rgb[1] * 255), 
        int(rgb[2] * 255)
    )

# === Настройки подключения ===
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "12345678")
DATABASE = "neo4j"

# === Проверка аргументов ===
if len(sys.argv) < 2:
    print("Использование: python3 export_neo4j_to_QGIS.py <имя города по базе данных>")
    print("Например: python3 export_neo4j_to_QGIS.py Бирск")
    sys.exit(1)

relation_type = sys.argv[1]+"BusRouteSegment"
node_type = sys.argv[1]+"BusStop"

driver = GraphDatabase.driver(URI, auth=AUTH)

def point_to_geojson(point):
    if not point:
        return None
    return {"type": "Point", "coordinates": [point.x, point.y]}

# === Генератор случайного цвета (для разных leiden_community) ===
def random_color():
    return "#{:06x}".format(random.randint(0, 0xFFFFFF))

# === Запрос ===
query = f"""
MATCH (a:`{node_type}`)-[r:`{relation_type}`]->(b:`{node_type}`)
RETURN 
    elementId(a) AS id_a,
    a.name AS name_a,
    a.location AS loc_a,
    a.leiden_community AS leiden_a,
    elementId(b) AS id_b,
    b.name AS name_b,
    b.location AS loc_b,
    b.leiden_community AS leiden_b,
    r.name AS rel_name,
    r.duration AS duration,
    r.route AS route
"""

features_nodes = {}
features_links = []
colors_by_community = {}  # чтобы одинаковые сообщества были одного цвета

with driver.session(database=DATABASE) as session:
    result = session.run(query)
    for record in result:
        id_a = record["id_a"]
        id_b = record["id_b"]
        loc_a = record["loc_a"]
        loc_b = record["loc_b"]

        leiden_a = record["leiden_a"]
        leiden_b = record["leiden_b"]

        # --- Цвет для каждого сообщества ---
        if leiden_a not in colors_by_community:
            colors_by_community[leiden_a] = distinct_random_color()
        if leiden_b not in colors_by_community:
            colors_by_community[leiden_b] = distinct_random_color()

        color_a = colors_by_community[leiden_a]
        color_b = colors_by_community[leiden_b]

        # --- Узлы ---
        if id_a not in features_nodes and loc_a:
            features_nodes[id_a] = {
                "type": "Feature",
                "geometry": point_to_geojson(loc_a),
                "properties": {
                    "id": id_a,
                    "name": record["name_a"],
                    "leiden_community": leiden_a,
                    "color": color_a,
                    "popup": f"<div style='background-color:white; color:black; padding:5px; border-radius:4px; font-family:sans-serif; font-size:12px;'><b>{record['name_a']}</b><br>Community: {leiden_a}</div>"

                }
            }
        if id_b not in features_nodes and loc_b:
            features_nodes[id_b] = {
                "type": "Feature",
                "geometry": point_to_geojson(loc_b),
                "properties": {
                    "id": id_b,
                    "name": record["name_b"],
                    "leiden_community": leiden_b,
                    "color": color_b,
                    "popup": f"<div style='background-color:white; color:black; padding:5px; border-radius:4px; font-family:sans-serif; font-size:12px;'><b>{record['name_b']}</b><br>Community: {leiden_b}</div>"

                }
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
                    "name": record["rel_name"],
                    "duration": record["duration"],
                    "route": record["route"]
                }
            })

driver.close()

# === Два отдельных GeoJSON ===
geojson_nodes = {
    "type": "FeatureCollection",
    "features": list(features_nodes.values())
}

geojson_links = {
    "type": "FeatureCollection",
    "features": features_links
}

file_nodes = f"nodes_{node_type}.geojson"
file_links = f"links_{relation_type}.geojson"

with open(file_nodes, "w", encoding="utf-8") as f:
    json.dump(geojson_nodes, f, ensure_ascii=False, indent=2)

with open(file_links, "w", encoding="utf-8") as f:
    json.dump(geojson_links, f, ensure_ascii=False, indent=2)

print(f"✅ Узлы сохранены в {file_nodes}")
print(f"✅ Связи сохранены в {file_links}")
print("🎨 Цвета сообществ:")
for k, v in colors_by_community.items():
    print(f"  Community {k}: {v}")

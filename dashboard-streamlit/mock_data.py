"""Generador de datos sintéticos para poder ejecutar el dashboard sin conexión a Athena.
Se activa con USE_MOCK_DATA = True en config.py mientras no tengas credenciales AWS a mano.
El formato de columnas replica exactamente los schemas reales (geometrías como WKT string).
"""
import random
from datetime import datetime, timedelta

import pandas as pd
from shapely.geometry import LineString, Point, Polygon

random.seed(42)

DISTRITOS = [
    (1, "Ciutat Vella"), (2, "L'Eixample"), (3, "Extramurs"),
    (4, "Campanar"), (5, "Patraix"), (6, "Jesús"),
]

LAT0, LON0 = 39.4699, -0.3763
STEP = 0.02


def _distrito_polygon(idx):
    row, col = divmod(idx, 3)
    x0 = LON0 + col * STEP * 1.5
    y0 = LAT0 + row * STEP * 1.5
    return Polygon([(x0, y0), (x0 + STEP * 1.4, y0), (x0 + STEP * 1.4, y0 + STEP * 1.4), (x0, y0 + STEP * 1.4)])


def generate_barrios_distritos() -> pd.DataFrame:
    rows = []
    barrio_id = 1
    for d_idx, (distrito_id, nombre_distrito) in enumerate(DISTRITOS):
        distrito_geom = _distrito_polygon(d_idx)
        row, col = divmod(d_idx, 3)
        x0 = LON0 + col * STEP * 1.5
        y0 = LAT0 + row * STEP * 1.5
        for b in range(random.randint(2, 3)):
            offset = b * 0.006
            barrio_geom = Polygon([
                (x0 + offset, y0 + offset), (x0 + offset + 0.012, y0 + offset),
                (x0 + offset + 0.012, y0 + offset + 0.012), (x0 + offset, y0 + offset + 0.012),
            ])
            rows.append({
                "barrio_id": barrio_id,
                "nombre_barrio": f"{nombre_distrito} - Barrio {b + 1}",
                "geometry_barrio": barrio_geom.wkt,
                "distrito_id": distrito_id,
                "nombre_distrito": nombre_distrito,
                "geometry_distrito": distrito_geom.wkt,
            })
            barrio_id += 1
    return pd.DataFrame(rows)


def generate_traffic(n_tramos=25, n_snapshots=3) -> pd.DataFrame:
    rows = []
    now = datetime.utcnow()
    for t in range(1, n_tramos + 1):
        distrito_id = random.choice(DISTRITOS)[0]
        station_id = random.randint(1, 8)
        barrio_id = random.randint(1, 15)  # constante por tramo, no por snapshot
        row, col = divmod(distrito_id - 1, 3)
        base_lat = LAT0 + row * STEP * 1.5 + random.uniform(0.002, 0.02)
        base_lon = LON0 + col * STEP * 1.5 + random.uniform(0.002, 0.02)
        line = LineString([(base_lon, base_lat), (base_lon + 0.004, base_lat + 0.003)])
        for s in range(n_snapshots):
            rows.append({
                "tramo_id": t,
                "nombre_tramo": f"Calle sintética {t}",
                "estado": random.choice([0, 0, 1, 2]),
                "geometry_tramo": line.wkt,
                "barrio_id": barrio_id,
                "distrito_id": distrito_id,
                "station_id": station_id,
                "event_ts": now - timedelta(hours=s * 6),
            })
    return pd.DataFrame(rows)


def generate_pollution(n_stations=8, n_days=10) -> pd.DataFrame:
    rows = []
    now = datetime.utcnow()
    # Caja acotada dentro del núcleo urbano de Valencia, con margen respecto a la costa real
    # (el resto del grid sintético del mock se desplaza siempre hacia el este/mar; aquí lo evitamos
    # a propósito porque son puntos sueltos y se nota mucho si caen en el agua).
    LAT_MIN, LAT_MAX = 39.455, 39.485
    LON_MIN, LON_MAX = -0.400, -0.355
    for st_id in range(1, n_stations + 1):
        lat = random.uniform(LAT_MIN, LAT_MAX)
        lon = random.uniform(LON_MIN, LON_MAX)
        point = Point(lon, lat)
        for d in range(n_days):
            rows.append({
                "station_id": st_id,
                "station_name": f"Estación {st_id}",
                "pm10": round(random.uniform(5, 45), 1),
                "pm25": round(random.uniform(2, 25), 1),
                "so2": round(random.uniform(1, 10), 1),
                "no2": round(random.uniform(5, 40), 1),
                "o3": round(random.uniform(10, 60), 1),
                "wind_kph": round(random.uniform(0, 30), 1),
                "environmental_risk": round(random.uniform(0, 10), 2),
                "geometry_station": point.wkt,
                "event_ts": now - timedelta(days=d),
                "fis_version": "v1",
            })
    return pd.DataFrame(rows)


def generate_mock_route(origin_lat: float, origin_lon: float, dest_lat: float, dest_lon: float) -> dict:
    """Simula la respuesta de la Lambda de rutas con dos líneas (una recta, otra con un
    pequeño desvío) para poder probar la interfaz sin desplegar la Lambda todavía."""
    import math

    def _line_geojson(coords):
        return {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "properties": {},
                "geometry": {"type": "LineString", "coordinates": coords},
            }],
        }

    def _haversine_m(lat1, lon1, lat2, lon2):
        r = 6371000
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dp = math.radians(lat2 - lat1)
        dl = math.radians(lon2 - lon1)
        a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
        return 2 * r * math.asin(math.sqrt(a))

    shortest_coords = [[origin_lon, origin_lat], [dest_lon, dest_lat]]

    mid_lat = (origin_lat + dest_lat) / 2 + 0.003
    mid_lon = (origin_lon + dest_lon) / 2 + 0.003
    smart_coords = [[origin_lon, origin_lat], [mid_lon, mid_lat], [dest_lon, dest_lat]]

    shortest_distance = _haversine_m(origin_lat, origin_lon, dest_lat, dest_lon)
    smart_distance = (
        _haversine_m(origin_lat, origin_lon, mid_lat, mid_lon)
        + _haversine_m(mid_lat, mid_lon, dest_lat, dest_lon)
    )

    return {
        "shortest_route": _line_geojson(shortest_coords),
        "smart_route": _line_geojson(smart_coords),
        "metrics": {
            "shortest_distance_m": round(shortest_distance, 2),
            "smart_distance_m": round(smart_distance, 2),
            "smart_cost": round(smart_distance * 0.8, 2),
            "execution_time_ms": 42.0,
        },
    }


def generate_out_of_service(n=15) -> pd.DataFrame:
    motivos = ["Obras", "Accidente", "Evento", "Mantenimiento"]
    rows = []
    now = datetime.utcnow()
    for i in range(1, n + 1):
        lat = LAT0 + random.uniform(0, 0.09)
        lon = LON0 + random.uniform(0, 0.09)
        line = LineString([(lon, lat), (lon + 0.003, lat + 0.002)])
        distrito_id = random.choice(DISTRITOS)[0]
        barrio_id = random.randint(1, 15)
        estado_actual = random.choice(["Cerrada", "Cerrada", "Abierta"])
        rows.append({
            "tramo_osm_id": i,
            "nombre_tramo": [f"Vía OSM {i}"],
            "estado": estado_actual,
            "tipoincidencia": random.choice(motivos),
            "barrio_id": barrio_id,
            "distrito_id": distrito_id,
            "geometry_tramo": line.wkt,
            "ingestion_ts": now,
            "source_file": "mock.osm",
        })
        # Para 1 de cada 3 tramos, simulamos un snapshot antiguo con estado distinto,
        # para poder comprobar que el dashboard se queda solo con el más reciente.
        if i % 3 == 0:
            rows.append({
                "tramo_osm_id": i,
                "nombre_tramo": [f"Vía OSM {i}"],
                "estado": "Abierta" if estado_actual == "Cerrada" else "Cerrada",
                "tipoincidencia": random.choice(motivos),
                "barrio_id": barrio_id,
                "distrito_id": distrito_id,
                "geometry_tramo": line.wkt,
                "ingestion_ts": now - timedelta(days=5),
                "source_file": "mock.osm",
            })
    return pd.DataFrame(rows)

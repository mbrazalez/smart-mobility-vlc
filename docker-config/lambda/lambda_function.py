import json
import math
import time
import pickle

import boto3
import networkx as nx
import osmnx as ox

BUCKET_NAME = "smart-mobility-vlc"
GRAPH_KEY = "delta/route-calculations/graph.pkl"

# Distancia máxima admisible entre el punto solicitado y el nodo más
# próximo del grafo. Por encima de este valor se entiende que el punto
# cae fuera de la ciudad y se rechaza la petición.
MAX_DIST_NODO_M = 500

# Los tramos cortados reciben un coste prohibitivo en lugar de eliminarse
# del grafo. Este umbral permite detectar que la ruta devuelta atraviesa
# alguno de ellos.
UMBRAL_TRAMO_CORTADO = 1e11

s3 = boto3.client("s3")

_grafo = None
_grafo_etag = None


def cargar_grafo():
    """Devuelve el grafo, recargándolo solo si ha cambiado en el origen.

    El contenedor de ejecución puede permanecer activo durante horas
    mientras el flujo de actualización reescribe el modelo cada pocos
    minutos. Comprobar el identificador de versión del objeto antes de
    cada resolución evita servir rutas calculadas sobre un grafo
    obsoleto, a un coste muy inferior al de la descarga completa.
    """
    global _grafo, _grafo_etag

    etag_actual = s3.head_object(Bucket=BUCKET_NAME, Key=GRAPH_KEY)["ETag"]

    if _grafo is None or etag_actual != _grafo_etag:
        objeto = s3.get_object(Bucket=BUCKET_NAME, Key=GRAPH_KEY)
        _grafo = pickle.loads(objeto["Body"].read())
        _grafo_etag = etag_actual

    return _grafo


# Carga inicial en el arranque del contenedor
G = cargar_grafo()

# Factor mínimo de la función de coste sobre el grafo. Con la formulación
# aditiva adoptada, el peso de toda arista satisface weight >= length, de
# modo que este valor es 1. Se calcula sobre el propio grafo para que la
# heurística siga siendo admisible si la fórmula cambiara.
FACTOR_MINIMO = min(
    (d["weight"] / d["length"]
     for _, _, d in G.edges(data=True)
     if d.get("length", 0) > 0 and d.get("weight") is not None
     and d["weight"] < UMBRAL_TRAMO_CORTADO),
    default=1.0,
)


def haversine_m(lat1, lon1, lat2, lon2):
    """Distancia sobre la esfera terrestre, en metros."""
    radio = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * radio * math.asin(math.sqrt(a))


def heuristica_distancia(graph, destino):
    """Heurística para la ruta más corta, con el coste medido en metros.

    La distancia en línea recta nunca supera la distancia real por la red
    viaria, por lo que la heurística es admisible y A* conserva la
    garantía de optimalidad.
    """
    lat_d = graph.nodes[destino]["y"]
    lon_d = graph.nodes[destino]["x"]

    def h(n, _):
        return haversine_m(graph.nodes[n]["y"], graph.nodes[n]["x"], lat_d, lon_d)

    return h


def heuristica_coste(graph, destino, factor_minimo):
    """Heurística para la ruta óptima, con el coste ponderado.

    Cuando el coste deja de ser una distancia, la haversine en metros no
    es admisible por sí sola: hay que escalarla por el factor mínimo que
    la función de coste puede aplicar a una arista. De este modo la
    estimación sigue siendo una cota inferior del coste restante.

    Con la formulación aditiva empleada el factor mínimo es 1, por lo que
    ambas heurísticas coinciden numéricamente. El escalado se mantiene de
    forma explícita para que la propiedad se conserve si los coeficientes
    de la función de coste se modificaran.
    """
    lat_d = graph.nodes[destino]["y"]
    lon_d = graph.nodes[destino]["x"]

    def h(n, _):
        d = haversine_m(graph.nodes[n]["y"], graph.nodes[n]["x"], lat_d, lon_d)
        return factor_minimo * d

    return h


def metricas_ruta(gdf):
    """Distancia y coste de una ruta, calculados sobre su propia geometría.

    Tomar ambos valores del mismo GeoDataFrame que se devuelve al cliente
    garantiza que las métricas se corresponden con las aristas realmente
    representadas. Calcularlos por separado sobre el grafo permitiría que
    procedieran de aristas paralelas distintas a las dibujadas.
    """
    return float(gdf["length"].sum()), float(gdf["weight"].sum())


def respuesta(codigo, cuerpo):
    return {"statusCode": codigo, "body": json.dumps(cuerpo)}


def lambda_handler(event, context):

    inicio = time.perf_counter()

    try:
        grafo = cargar_grafo()

        origen_lat = event["origin"]["lat"]
        origen_lon = event["origin"]["lon"]
        destino_lat = event["destination"]["lat"]
        destino_lon = event["destination"]["lon"]

        # --- Localización de los nodos más próximos ------------------
        origen, dist_origen = ox.distance.nearest_nodes(
            grafo, X=origen_lon, Y=origen_lat, return_dist=True
        )
        destino, dist_destino = ox.distance.nearest_nodes(
            grafo, X=destino_lon, Y=destino_lat, return_dist=True
        )

        if max(dist_origen, dist_destino) > MAX_DIST_NODO_M:
            return respuesta(400, {
                "message": "El origen o el destino se encuentran fuera del "
                           "área cubierta por el sistema."
            })

        if origen == destino:
            return respuesta(400, {
                "message": "El origen y el destino corresponden al mismo "
                           "punto de la red viaria."
            })

        # --- Resolución de ambas rutas -------------------------------
        ruta_corta = nx.astar_path(
            grafo, origen, destino,
            heuristic=heuristica_distancia(grafo, destino),
            weight="length",
        )

        ruta_optima = nx.astar_path(
            grafo, origen, destino,
            heuristic=heuristica_coste(grafo, destino, FACTOR_MINIMO),
            weight="weight",
        )

        # --- Geometrías ----------------------------------------------
        # El criterio de selección entre aristas paralelas debe coincidir
        # con el empleado por el algoritmo, o la geometría representada no
        # correspondería a la ruta calculada.
        gdf_corta = ox.routing.route_to_gdf(grafo, ruta_corta, weight="length")
        gdf_optima = ox.routing.route_to_gdf(grafo, ruta_optima, weight="weight")

        dist_corta, coste_corta = metricas_ruta(gdf_corta)
        dist_optima, coste_optima = metricas_ruta(gdf_optima)

        # --- Detección de tramos cortados ----------------------------
        atraviesa_corte = bool(
            (gdf_optima["weight"] >= UMBRAL_TRAMO_CORTADO).any()
        )

        transcurrido = (time.perf_counter() - inicio) * 1000

        return respuesta(200, {
            "shortest_route": json.loads(gdf_corta.to_json()),
            "smart_route": json.loads(gdf_optima.to_json()),
            "metrics": {
                "shortest_distance_m": round(dist_corta, 2),
                "shortest_cost": round(coste_corta, 2),
                "smart_distance_m": round(dist_optima, 2),
                "smart_cost": round(coste_optima, 2),
                "execution_time_ms": round(transcurrido, 2),
            },
            "warnings": (
                ["La ruta óptima atraviesa al menos un tramo cortado, al no "
                 "existir alternativa disponible."]
                if atraviesa_corte else []
            ),
        })

    except nx.NetworkXNoPath:
        return respuesta(404, {
            "message": "No existe una ruta entre el origen y el destino."
        })

    except KeyError as e:
        return respuesta(400, {
            "message": f"Falta el campo obligatorio {e} en la petición."
        })

    except Exception:
        # El detalle del error queda registrado en el servicio de trazas;
        # al cliente se le devuelve un mensaje genérico para no exponer
        # información interna del sistema.
        import traceback
        print(traceback.format_exc())
        return respuesta(500, {
            "message": "Se ha producido un error al calcular la ruta."
        })
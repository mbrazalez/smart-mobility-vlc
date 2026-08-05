import json
import time
import pickle
import boto3
import networkx as nx
import osmnx as ox

BUCKET_NAME = "smart-mobility-vlc"
GRAPH_KEY = "delta/route-calculations/graph.pkl"

s3 = boto3.client("s3")

response = s3.get_object(
    Bucket=BUCKET_NAME,
    Key=GRAPH_KEY
)

G = pickle.loads(response["Body"].read())



def route_distance(graph, route):
    """Calcula la distancia total de una ruta."""

    distance = 0.0

    for u, v in zip(route[:-1], route[1:]):

        edge = min(
            graph[u][v].values(),
            key=lambda e: e["length"]
        )

        distance += edge["length"]

    return distance


def route_weight(graph, route):
    """Calcula el peso total de una ruta."""

    weight = 0.0

    for u, v in zip(route[:-1], route[1:]):

        edge = min(
            graph[u][v].values(),
            key=lambda e: e["weight"]
        )

        weight += edge["weight"]

    return weight


def lambda_handler(event, context):

    start = time.perf_counter()

    try:

        origin_lat = event["origin"]["lat"]
        origin_lon = event["origin"]["lon"]

        destination_lat = event["destination"]["lat"]
        destination_lon = event["destination"]["lon"]

        origin = ox.distance.nearest_nodes(
            G,
            X=origin_lon,
            Y=origin_lat
        )

        destination = ox.distance.nearest_nodes(
            G,
            X=destination_lon,
            Y=destination_lat
        )
       
        shortest_route = nx.astar_path(
            G,
            origin,
            destination,
            weight="length"
        )

        smart_route = nx.astar_path(
            G,
            origin,
            destination,
            weight="weight"
        )


        shortest_gdf = ox.routing.route_to_gdf(
            G,
            shortest_route
        )

        smart_gdf = ox.routing.route_to_gdf(
            G,
            smart_route
        )

        shortest_distance = route_distance(
            G,
            shortest_route
        )

        smart_distance = route_distance(
            G,
            smart_route
        )

        smart_weight = route_weight(
            G,
            smart_route
        )

        elapsed = (
            time.perf_counter() - start
        ) * 1000

        return {
            "statusCode": 200,
            "body": json.dumps({

                "shortest_route":
                    json.loads(shortest_gdf.to_json()),

                "smart_route":
                    json.loads(smart_gdf.to_json()),

                "metrics": {

                    "shortest_distance_m":
                        round(shortest_distance, 2),

                    "smart_distance_m":
                        round(smart_distance, 2),

                    "smart_cost":
                        round(smart_weight, 2),

                    "execution_time_ms":
                        round(elapsed, 2)
                }

            })
        }

    except nx.NetworkXNoPath:

        return {
            "statusCode": 404,
            "body": json.dumps({
                "message": "No existe una ruta entre el origen y el destino."
            })
        }

    except Exception as e:

        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": str(e)
            })
        }
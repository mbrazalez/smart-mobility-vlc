"""Cliente para invocar la Lambda de cálculo de rutas (AWS Lambda `invoke` directo, sin API Gateway)."""
import json

import boto3
import streamlit as st

import config
import mock_data


def _lambda_client():
    kwargs = {}
    if config.AWS_PROFILE:
        kwargs["profile_name"] = config.AWS_PROFILE
    session = boto3.Session(**kwargs)
    return session.client("lambda", region_name=config.AWS_REGION)


@st.cache_data(ttl=600, show_spinner="Calculando ruta...")
def calcular_ruta(origin_lat: float, origin_lon: float, dest_lat: float, dest_lon: float) -> dict:
    """Devuelve el 'body' de la respuesta de la Lambda ya parseado:
    {"shortest_route": <geojson>, "smart_route": <geojson>, "metrics": {...}}

    Lanza RuntimeError con un mensaje legible si la Lambda falla o no encuentra ruta.
    """
    if config.USE_MOCK_DATA:
        return mock_data.generate_mock_route(origin_lat, origin_lon, dest_lat, dest_lon)

    payload = {
        "origin": {"lat": origin_lat, "lon": origin_lon},
        "destination": {"lat": dest_lat, "lon": dest_lon},
    }
    client = _lambda_client()
    response = client.invoke(
        FunctionName=config.ROUTE_LAMBDA_NAME,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8"),
    )

    raw = response["Payload"].read()
    result = json.loads(raw)

    # Error a nivel de invocación de Lambda (excepción no controlada, timeout, etc.)
    if response.get("FunctionError"):
        message = result.get("errorMessage", str(result))
        raise RuntimeError(f"Error al ejecutar la Lambda: {message}")

    status_code = result.get("statusCode")
    body = json.loads(result.get("body", "{}"))

    if status_code == 404:
        raise RuntimeError(body.get("message", "No existe una ruta entre el origen y el destino."))
    if status_code != 200:
        raise RuntimeError(body.get("message", f"La Lambda devolvió un error (status {status_code})."))

    return body

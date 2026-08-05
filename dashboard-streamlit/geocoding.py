"""Geocodificación de direcciones con Nominatim (OpenStreetMap), acotada a Valencia.

Nominatim es gratuito y no requiere API key, pero su uso público está limitado a
~1 petición/segundo (https://operations.osmfoundation.org/policies/nominatim/).
Válido para uso académico / bajo tráfico; para producción con muchos usuarios
concurrentes, considera un proveedor de pago (Google Places, Mapbox, Amazon Location
Service) o self-hostear Nominatim.
"""
import requests
import streamlit as st

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
# Bounding box aproximado de Valencia ciudad (lon_min, lat_min, lon_max, lat_max)
VALENCIA_VIEWBOX = "-0.4300,39.5200,-0.3200,39.4200"
USER_AGENT = "tfm-smart-mobility-valencia/1.0 (uso academico)"


@st.cache_data(ttl=3600, show_spinner=False)
def buscar_direcciones(query: str, limit: int = 5) -> list:
    """Devuelve una lista de candidatos: [{"label": str, "lat": float, "lon": float}, ...]
    Lista vacía si no hay resultados o falla la petición (nunca lanza excepción)."""
    if not query or len(query.strip()) < 3:
        return []
    params = {
        "q": query,
        "format": "jsonv2",
        "limit": limit,
        "viewbox": VALENCIA_VIEWBOX,
        "bounded": 1,
        "countrycodes": "es",
        "addressdetails": 1,
    }
    headers = {"User-Agent": USER_AGENT}
    try:
        resp = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=5)
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError):
        return []

    return [
        {"label": item.get("display_name", ""), "lat": float(item["lat"]), "lon": float(item["lon"])}
        for item in data
        if "lat" in item and "lon" in item
    ]

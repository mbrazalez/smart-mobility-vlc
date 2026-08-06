"""Construcción de los mapas Folium."""
import branca.colormap as cm
import folium
import geopandas as gpd
import pandas as pd

import config


def _empty_map():
    m = folium.Map(location=config.VALENCIA_CENTER, zoom_start=config.DEFAULT_ZOOM, tiles=None)
    folium.TileLayer(tiles="cartodbpositron", name="Capas", control=True).add_to(m)
    return m


def _add_estaciones_layer(m: folium.Map, estaciones_gdf: gpd.GeoDataFrame):
    """Capa de puntos con las estaciones de calidad ambiental (pollution-levels-curated)."""
    fg = folium.FeatureGroup(name="Calidad ambiental (estaciones)", show=True)
    if estaciones_gdf.empty:
        fg.add_to(m)
        return

    valores = pd.to_numeric(estaciones_gdf.get("environmental_risk"), errors="coerce").dropna()
    colormap = None
    if not valores.empty:
        vmin, vmax = float(valores.min()), float(valores.max())
        if vmin == vmax:
            vmax = vmin + 1
        colormap = cm.LinearColormap(colors=["#2ecc71", "#f39c12", "#e74c3c"], vmin=vmin, vmax=vmax)
        colormap.caption = "FIS (riesgo ambiental) por estación"
        colormap.add_to(m)

    for _, row in estaciones_gdf.iterrows():
        risk = pd.to_numeric(pd.Series([row.get("environmental_risk")]), errors="coerce").iloc[0]
        color = colormap(risk) if (colormap is not None and pd.notna(risk)) else "#7f8c8d"
        tooltip_html = (
            f"<b>{row.get('station_name', 'Estación')}</b><br>"
            f"FIS (riesgo ambiental): {risk if pd.notna(risk) else 'N/D'}<br>"
            f"PM10: {row.get('pm10', 'N/D')} · PM2.5: {row.get('pm25', 'N/D')}<br>"
            f"NO2: {row.get('no2', 'N/D')} · O3: {row.get('o3', 'N/D')}"
        )
        folium.CircleMarker(
            location=[row.geometry.y, row.geometry.x],
            radius=7,
            color=color,
            weight=2,
            fill=True,
            fill_color=color,
            fill_opacity=0.85,
            tooltip=folium.Tooltip(tooltip_html),
        ).add_to(fg)
    fg.add_to(m)


def build_main_map(
    barrios_gdf: gpd.GeoDataFrame,
    distritos_gdf: gpd.GeoDataFrame,
    tramos_gdf: gpd.GeoDataFrame,
    estaciones_gdf: gpd.GeoDataFrame,
) -> folium.Map:
    m = _empty_map()

    if not distritos_gdf.empty:
        fg = folium.FeatureGroup(name="Distritos", show=True)
        folium.GeoJson(
            distritos_gdf,
            style_function=lambda f: {"color": "#34495e", "weight": 2, "fillOpacity": 0.05},
            tooltip=folium.GeoJsonTooltip(fields=["nombre_distrito"], aliases=["Distrito:"]),
        ).add_to(fg)
        fg.add_to(m)

    if not barrios_gdf.empty:
        fg = folium.FeatureGroup(name="Barrios", show=True)
        folium.GeoJson(
            barrios_gdf,
            style_function=lambda f: {"color": "#2980b9", "weight": 1, "fillOpacity": 0.03},
            tooltip=folium.GeoJsonTooltip(
                fields=["nombre_barrio", "nombre_distrito"], aliases=["Barrio:", "Distrito:"]
            ),
        ).add_to(fg)
        fg.add_to(m)

    if not tramos_gdf.empty:
        fg = folium.FeatureGroup(name="Estado del tráfico", show=True)

        def _style(feature):
            estado = feature["properties"].get("estado")
            color = config.ESTADO_TRAFICO_COLOR.get(estado, "#7f8c8d")
            return {"color": color, "weight": 4, "opacity": 0.85}

        tooltip_fields = ["nombre_tramo", "nombre_distrito", "estado_label", "environmental_risk"]
        tooltip_aliases = ["Tramo:", "Distrito:", "Estado tráfico:", "FIS (riesgo ambiental):"]
        present = [f for f in tooltip_fields if f in tramos_gdf.columns]
        present_aliases = [a for f, a in zip(tooltip_fields, tooltip_aliases) if f in tramos_gdf.columns]

        folium.GeoJson(
            tramos_gdf,
            style_function=_style,
            tooltip=folium.GeoJsonTooltip(fields=present, aliases=present_aliases),
        ).add_to(fg)
        fg.add_to(m)

    if not estaciones_gdf.empty:
        _add_estaciones_layer(m, estaciones_gdf)

    folium.LayerControl(collapsed=False).add_to(m)
    return m


def _iter_coords(geometry):
    t = geometry.get("type")
    coords = geometry.get("coordinates", [])
    if t == "LineString":
        yield from coords
    elif t == "MultiLineString":
        for line in coords:
            yield from line
    elif t == "Point":
        yield coords


def _collect_bounds(*geojsons):
    lats, lons = [], []
    for gj in geojsons:
        if not gj:
            continue
        for feature in gj.get("features", []):
            for lon, lat in _iter_coords(feature.get("geometry", {})):
                lats.append(lat)
                lons.append(lon)
    if not lats:
        return None
    return [[min(lats), min(lons)], [max(lats), max(lons)]]


def build_route_map(origin=None, destination=None, shortest_geojson=None, smart_geojson=None) -> folium.Map:
    """origin/destination son tuplas (lat, lon) o None. Los *_geojson son FeatureCollections
    (el resultado de route_to_gdf(...).to_json() ya parseado con json.loads) o None."""
    center = origin if origin else config.VALENCIA_CENTER
    m = folium.Map(location=center, zoom_start=config.DEFAULT_ZOOM, tiles=None)
    folium.TileLayer(tiles="cartodbpositron", name="Capas", control=True).add_to(m)

    if origin:
        folium.Marker(location=origin, tooltip="Origen", icon=folium.Icon(color="green", icon="play")).add_to(m)
    if destination:
        folium.Marker(location=destination, tooltip="Destino", icon=folium.Icon(color="red", icon="stop")).add_to(m)

    if shortest_geojson:
        folium.GeoJson(
            shortest_geojson,
            name="Ruta más corta (length)",
            style_function=lambda f: {"color": "#3498db", "weight": 5, "opacity": 0.85},
        ).add_to(m)

    if smart_geojson:
        folium.GeoJson(
            smart_geojson,
            name="Ruta óptima (weight)",
            style_function=lambda f: {"color": "#9b59b6", "weight": 5, "opacity": 0.85},
        ).add_to(m)

    if shortest_geojson or smart_geojson:
        folium.LayerControl(collapsed=False).add_to(m)

    bounds = _collect_bounds(shortest_geojson, smart_geojson)
    if bounds:
        m.fit_bounds(bounds)
    elif origin and destination:
        m.fit_bounds([list(origin), list(destination)])

    return m


def build_out_of_service_map(cortes_gdf: gpd.GeoDataFrame) -> folium.Map:
    m = _empty_map()
    if cortes_gdf.empty:
        return m

    fg = folium.FeatureGroup(name="Tramos cortados (OSM)", show=True)
    tooltip_fields = ["nombre_tramo_str", "nombre_distrito", "tipoincidencia"]
    tooltip_aliases = ["Tramo:", "Distrito:", "Motivo del corte:"]
    present = [f for f in tooltip_fields if f in cortes_gdf.columns]
    present_aliases = [a for f, a in zip(tooltip_fields, tooltip_aliases) if f in cortes_gdf.columns]

    folium.GeoJson(
        cortes_gdf,
        style_function=lambda f: {"color": "#c0392b", "weight": 5, "opacity": 0.9},
        tooltip=folium.GeoJsonTooltip(fields=present, aliases=present_aliases),
    ).add_to(fg)
    fg.add_to(m)
    folium.LayerControl(collapsed=False).add_to(m)
    return m

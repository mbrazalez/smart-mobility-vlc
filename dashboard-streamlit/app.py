import streamlit as st
from streamlit_folium import st_folium

import config
import data
import geo
import maps
import charts
import route_api
import geocoding

st.set_page_config(page_title="TFM · Smart Mobility Valencia", layout="wide")
st.markdown(
    "<style>.block-container{padding-top:3rem;padding-bottom:1rem;}</style>",
    unsafe_allow_html=True,
)
st.markdown(
    f"##### 🚦 Smart Mobility Valencia — Dashboard &nbsp;·&nbsp; "
    f"*{'Datos de ejemplo (mock)' if config.USE_MOCK_DATA else 'Datos en vivo desde Athena'} · TFM*"
)

# ---------- Carga de datos ----------
with st.spinner("Cargando datos..."):
    barrios_distritos_df = data.load_barrios_distritos()
    pollution_df = data.load_pollution()
    traffic_df = data.load_traffic()
    out_of_service_df = data.load_out_of_service()

barrios_gdf = geo.get_barrios_gdf(barrios_distritos_df)
distritos_gdf = geo.get_distritos_gdf(barrios_distritos_df)

# Lookup distrito_id -> nombre_distrito (se reutiliza en tráfico, cortes y contaminación
# para no tener que duplicar el nombre del distrito en las tablas gold del catálogo)
distrito_lookup = barrios_distritos_df[["distrito_id", "nombre_distrito"]].drop_duplicates()

# ---------- Última foto de cada entidad (evitamos contar snapshots históricos duplicados) ----------
traffic_latest = data.latest_per_key(traffic_df, ["tramo_id", "barrio_id", "distrito_id"], ts_col="event_ts")
traffic_latest = traffic_latest.merge(distrito_lookup, on="distrito_id", how="left")

pollution_latest = data.latest_per_key(pollution_df, "station_id", ts_col="event_ts")

out_of_service_latest = data.latest_per_key(
    out_of_service_df, ["tramo_osm_id", "barrio_id", "distrito_id"], ts_col="ingestion_ts"
)
out_of_service_latest = out_of_service_latest.merge(distrito_lookup, on="distrito_id", how="left")
# Reconstruimos nombre_tramo_str tras el merge (drop_duplicates/groupby no lo tocan, pero por claridad)
if "nombre_tramo" in out_of_service_latest.columns and "nombre_tramo_str" not in out_of_service_latest.columns:
    out_of_service_latest["nombre_tramo_str"] = out_of_service_latest["nombre_tramo"].apply(
        lambda v: ", ".join(v) if isinstance(v, (list, tuple)) else (v or "")
    )

# Solo las calles actualmente cortadas -> se usan en el mapa de cortes (no en la gráfica
# "abiertas vs cerradas", que necesita ambas categorías para poder comparar)
out_of_service_cerradas = data.solo_cerradas(out_of_service_latest)

# Tabla de correspondencia estación de medición -> distrito
# (se obtiene de traffic-status-gold, que enlaza station_id con distrito_id)
station_to_distrito = (
    traffic_df[["station_id", "distrito_id"]].dropna().drop_duplicates(subset=["station_id"])
    .merge(distrito_lookup, on="distrito_id", how="left")
)

# Tramos de tráfico enriquecidos con el FIS de su estación asociada, para el tooltip del mapa
traffic_enriquecido = traffic_latest.merge(
    pollution_latest[["station_id", "environmental_risk"]], on="station_id", how="left"
)
traffic_enriquecido["estado_label"] = traffic_enriquecido["estado"].map(config.ESTADO_TRAFICO)
traffic_enriquecido["environmental_risk"] = traffic_enriquecido["environmental_risk"].fillna("N/D")
traffic_enriquecido["event_ts"] = traffic_enriquecido["event_ts"].astype(str)
tramos_gdf = geo.wkt_to_gdf(traffic_enriquecido, "geometry_tramo")

cortes_df_for_map = out_of_service_cerradas.copy()
cortes_df_for_map["ingestion_ts"] = cortes_df_for_map["ingestion_ts"].astype(str)
cortes_df_for_map = cortes_df_for_map.fillna("N/D")
cortes_gdf = geo.wkt_to_gdf(cortes_df_for_map, "geometry_tramo")

# Estaciones de calidad ambiental como capa de puntos (para el mapa principal)
estaciones_gdf = geo.wkt_to_gdf(pollution_latest, "geometry_station")

# ---------- Layout ----------
tab_mapa, tab_cortes, tab_stats, tab_rutas = st.tabs(
    ["🗺️ Mapa de tráfico y barrios", "🚧 Cortes de vía (OSM)", "📊 Estadísticas", "🧭 Calcular ruta"]
)

with tab_mapa:
    st.caption("Usa el control de capas del propio mapa (arriba a la derecha) para mostrar u ocultar cada capa.")
    m = maps.build_main_map(barrios_gdf, distritos_gdf, tramos_gdf, estaciones_gdf)
    st_folium(m, width=None, height=config.MAP_HEIGHT, key="mapa_principal")

with tab_cortes:
    st.caption(
        "Fuente: OpenStreetMap, en un mapa aparte porque la segmentación de calles no "
        "coincide con la del ayuntamiento. Solo se muestran los tramos cuyo estado más reciente es 'Cerrada'."
    )
    m2 = maps.build_out_of_service_map(cortes_gdf)
    st_folium(m2, width=None, height=config.MAP_HEIGHT, key="mapa_cortes")

with tab_stats:
    st.subheader("Calles abiertas vs. cerradas")
    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(charts.chart_calles_abiertas_cerradas(out_of_service_latest), use_container_width=True)
    with c2:
        fig_motivos = charts.chart_motivos_cierre(out_of_service_latest)
        if fig_motivos is not None:
            st.plotly_chart(fig_motivos, use_container_width=True)
        else:
            st.info("No hay tramos marcados como cerrados con los criterios actuales de config.ESTADOS_CERRADO.")

    st.subheader("Estado del tráfico por distrito")
    st.plotly_chart(charts.chart_estado_trafico_por_distrito(traffic_latest), use_container_width=True)

    st.subheader("Histórico de calidad ambiental (FIS) por distrito")
    distritos_disponibles = sorted(station_to_distrito["nombre_distrito"].dropna().unique().tolist())
    distritos_sel = st.multiselect(
        "Filtra por distrito (vacío = todos)", options=distritos_disponibles, default=[]
    )
    fig_fis = charts.chart_historico_fis(pollution_df, station_to_distrito, distritos_sel or None)
    if fig_fis is not None:
        st.plotly_chart(fig_fis, use_container_width=True)
    else:
        st.info("No hay datos suficientes para construir el histórico.")

with tab_rutas:
    st.caption(
        "Busca por dirección o haz clic en el mapa (1º clic = origen 📍 verde, 2º = destino 📍 rojo)."
    )

    for key, default in [
        ("route_origin", None), ("route_destination", None),
        ("route_result", None), ("route_last_click", None),
    ]:
        if key not in st.session_state:
            st.session_state[key] = default

    col_origen, col_destino = st.columns(2)

    with col_origen:
        st.text_input("Dirección de origen", key="origin_query", placeholder="p.ej. Calle Colón 1")
        if st.session_state.get("origin_query", "").strip():
            candidatos_o = geocoding.buscar_direcciones(st.session_state.origin_query)
            if candidatos_o:
                opciones_o = {c["label"]: (c["lat"], c["lon"]) for c in candidatos_o}
                sel_o = st.selectbox("Coincidencias en Valencia", options=list(opciones_o.keys()), key="origin_sel")
                if st.button("📍 Usar como origen"):
                    st.session_state.route_origin = opciones_o[sel_o]
                    st.session_state.route_result = None
                    st.rerun()
            else:
                st.caption("Sin coincidencias. Prueba con calle + número.")

    with col_destino:
        st.text_input("Dirección de destino", key="destination_query", placeholder="p.ej. Plaza del Ayuntamiento")
        if st.session_state.get("destination_query", "").strip():
            candidatos_d = geocoding.buscar_direcciones(st.session_state.destination_query)
            if candidatos_d:
                opciones_d = {c["label"]: (c["lat"], c["lon"]) for c in candidatos_d}
                sel_d = st.selectbox("Coincidencias en Valencia", options=list(opciones_d.keys()), key="destination_sel")
                if st.button("📍 Usar como destino"):
                    st.session_state.route_destination = opciones_d[sel_d]
                    st.session_state.route_result = None
                    st.rerun()
            else:
                st.caption("Sin coincidencias. Prueba con calle + número.")

    col_info, col_reset = st.columns([4, 1])
    with col_info:
        origen_txt = (
            f"{st.session_state.route_origin[0]:.5f}, {st.session_state.route_origin[1]:.5f}"
            if st.session_state.route_origin else "— (haz clic en el mapa)"
        )
        destino_txt = (
            f"{st.session_state.route_destination[0]:.5f}, {st.session_state.route_destination[1]:.5f}"
            if st.session_state.route_destination else "— (haz clic en el mapa)"
        )
        st.markdown(f"**Origen:** {origen_txt}  ·  **Destino:** {destino_txt}")


    resultado_previo = st.session_state.route_result
    shortest_geojson = resultado_previo["shortest_route"] if resultado_previo else None
    smart_geojson = resultado_previo["smart_route"] if resultado_previo else None

    m_ruta = maps.build_route_map(
        st.session_state.route_origin, st.session_state.route_destination,
        shortest_geojson, smart_geojson,
    )
    map_state = st_folium(m_ruta, width=None, height=config.ROUTE_MAP_HEIGHT, key="mapa_rutas")

    click = map_state.get("last_clicked") if map_state else None
    if click and click != st.session_state.route_last_click:
        st.session_state.route_last_click = click
        punto = (click["lat"], click["lng"])
        if st.session_state.route_origin is None:
            st.session_state.route_origin = punto
        elif st.session_state.route_destination is None:
            st.session_state.route_destination = punto
        else:
            st.session_state.route_origin = punto
            st.session_state.route_destination = None
        st.session_state.route_result = None
        st.rerun()

    puede_calcular = bool(st.session_state.route_origin and st.session_state.route_destination)
    if st.button("🧭 Calcular ruta", disabled=not puede_calcular, type="primary"):
        try:
            st.session_state.route_result = route_api.calcular_ruta(
                st.session_state.route_origin[0], st.session_state.route_origin[1],
                st.session_state.route_destination[0], st.session_state.route_destination[1],
            )
            st.rerun()
        except RuntimeError as e:
            st.error(str(e))

    if st.session_state.route_result:
        resultado = st.session_state.route_result
        metrics = resultado.get("metrics", {})

        # Avisos devueltos por la Lambda (p. ej. la ruta atraviesa un corte)
        for aviso in resultado.get("warnings", []):
            st.warning(aviso)

        dist_corta = metrics.get("shortest_distance_m", 0)
        dist_optima = metrics.get("smart_distance_m", 0)
        coste_corta = metrics.get("shortest_cost", 0)
        coste_optima = metrics.get("smart_cost", 0)

        # Fila 1: la ruta más corta
        st.markdown("**🔵 Ruta más corta** — minimiza la distancia recorrida")
        c1, c2 = st.columns(2)
        c1.metric("Distancia", f"{dist_corta:,.0f} m")
        c2.metric("Coste ponderado", f"{coste_corta:,.0f}")

        # Fila 2: la ruta óptima, con la diferencia respecto a la anterior
        st.markdown("**🟣 Ruta óptima** — equilibra distancia, calidad del aire, tráfico y cortes")
        c3, c4 = st.columns(2)
        c3.metric(
            "Distancia",
            f"{dist_optima:,.0f} m",
            delta=f"{dist_optima - dist_corta:+,.0f} m",
            delta_color="inverse",
        )
        c4.metric(
            "Coste ponderado",
            f"{coste_optima:,.0f}",
            delta=f"{coste_optima - coste_corta:+,.0f}",
            delta_color="inverse",
        )

        # Lectura conjunta de ambas métricas
        if coste_corta > 0:
            mejora = (coste_corta - coste_optima) / coste_corta
            rodeo = (dist_optima - dist_corta) / dist_corta if dist_corta else 0
            st.caption(
                f"La ruta óptima reduce un {mejora:.1%} el coste ponderado "
                f"a cambio de un {rodeo:+.1%} de distancia adicional."
            )

        st.caption(f"Tiempo de cálculo en Lambda: {metrics.get('execution_time_ms', 0):,.0f} ms")
        
st.sidebar.header("⚙️ Datos")
if st.sidebar.button("🔄 Recargar datos"):
    st.cache_data.clear()
    st.rerun()
st.sidebar.caption(
    f"Barrios: {len(barrios_gdf)} · Distritos: {len(distritos_gdf)} · "
    f"Tramos de tráfico: {len(tramos_gdf)} · Estaciones: {len(estaciones_gdf)} · "
    f"Cortes activos: {len(cortes_gdf)}"
)

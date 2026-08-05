"""Gráficas Plotly para la pestaña de estadísticas.

Todas las funciones esperan que los DataFrames de entrada ya traigan la columna
'nombre_distrito' (se añade en app.py haciendo un join contra barrios_distritos_df,
la tabla que tiene el catálogo de distritos).
"""
import pandas as pd
import plotly.express as px

import config


def chart_calles_abiertas_cerradas(out_df: pd.DataFrame):
    """Nº de tramos por distrito, separados por si están 'cerrados' o no.

    Recibe el conjunto COMPLETO (abiertas + cerradas, ya deduplicado) porque
    esta gráfica necesita ambas categorías para poder compararlas.
    """
    df = out_df.copy()
    df["cerrado"] = df["estado"].astype(str).str.lower().isin([v.lower() for v in config.ESTADOS_CERRADO])
    df["situacion"] = df["cerrado"].map({True: "Cerrada", False: "Abierta"})

    resumen = (
        df.groupby(["nombre_distrito", "situacion"], as_index=False)
        .size()
        .rename(columns={"size": "num_tramos"})
    )
    fig = px.bar(
        resumen, x="nombre_distrito", y="num_tramos", color="situacion",
        barmode="group", title="Calles abiertas vs. cerradas por distrito",
        labels={"nombre_distrito": "Distrito", "num_tramos": "Nº de tramos", "situacion": "Situación"},
    )
    return fig


def chart_motivos_cierre(out_df: pd.DataFrame):
    """Entre las calles cerradas, agrupadas por motivo (tipoincidencia)."""
    df = out_df.copy()
    df["cerrado"] = df["estado"].astype(str).str.lower().isin([v.lower() for v in config.ESTADOS_CERRADO])
    cerradas = df[df["cerrado"]]
    if cerradas.empty:
        return None
    resumen = (
        cerradas.groupby(["nombre_distrito", "tipoincidencia"], as_index=False)
        .size()
        .rename(columns={"size": "num_tramos"})
    )
    fig = px.bar(
        resumen, x="nombre_distrito", y="num_tramos", color="tipoincidencia",
        barmode="stack", title="Motivo de los cortes por distrito",
        labels={"nombre_distrito": "Distrito", "num_tramos": "Nº de tramos", "tipoincidencia": "Motivo"},
    )
    return fig


def chart_estado_trafico_por_distrito(traffic_latest: pd.DataFrame):
    df = traffic_latest.copy()
    df["estado_label"] = df["estado"].map(config.ESTADO_TRAFICO)
    resumen = (
        df.groupby(["nombre_distrito", "estado_label"], as_index=False)
        .size()
        .rename(columns={"size": "num_tramos"})
    )
    fig = px.bar(
        resumen, x="nombre_distrito", y="num_tramos", color="estado_label",
        barmode="group", title="Estado del tráfico por distrito",
        color_discrete_map={
            config.ESTADO_TRAFICO[0]: config.ESTADO_TRAFICO_COLOR[0],
            config.ESTADO_TRAFICO[1]: config.ESTADO_TRAFICO_COLOR[1],
            config.ESTADO_TRAFICO[2]: config.ESTADO_TRAFICO_COLOR[2],
        },
        labels={"nombre_distrito": "Distrito", "num_tramos": "Nº de tramos", "estado_label": "Estado"},
    )
    return fig


def chart_historico_fis(pollution_df: pd.DataFrame, station_to_distrito: pd.DataFrame, distritos_sel=None):
    """station_to_distrito debe traer al menos station_id y nombre_distrito."""
    df = pollution_df.merge(station_to_distrito, on="station_id", how="left")
    df = df.dropna(subset=["nombre_distrito"])
    if distritos_sel:
        df = df[df["nombre_distrito"].isin(distritos_sel)]
    if df.empty:
        return None
    df["fecha"] = pd.to_datetime(df["event_ts"]).dt.date
    resumen = df.groupby(["fecha", "nombre_distrito"], as_index=False)["environmental_risk"].mean()
    fig = px.line(
        resumen, x="fecha", y="environmental_risk", color="nombre_distrito",
        title="Histórico del FIS (riesgo ambiental) medio por distrito",
        labels={"fecha": "Fecha", "environmental_risk": "FIS medio", "nombre_distrito": "Distrito"},
        markers=True,
    )
    return fig

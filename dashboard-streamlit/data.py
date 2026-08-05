"""Capa de acceso a datos: consultas a Athena (Delta Lake) o datos mock, con caché de Streamlit."""
import boto3
import pandas as pd
import streamlit as st
import awswrangler as wr

import config
import mock_data


def _session():
    kwargs = {}
    if config.AWS_PROFILE:
        kwargs["profile_name"] = config.AWS_PROFILE
    if config.AWS_REGION:
        kwargs["region_name"] = config.AWS_REGION
    return boto3.Session(**kwargs)


def _athena_query(table: str) -> pd.DataFrame:
    query = f'SELECT * FROM "{config.ATHENA_DATABASE}"."{table}"'
    return wr.athena.read_sql_query(
        query,
        database=config.ATHENA_DATABASE,
        s3_output=config.ATHENA_S3_OUTPUT,
        ctas_approach=False,
        boto3_session=_session(),
    )


def _require_columns(df: pd.DataFrame, required: list, table: str) -> pd.DataFrame:
    """Falla con un mensaje claro en la UI si faltan columnas esperadas, en vez de un
    KeyError críptico más adelante en el código (p.ej. dentro de un groupby o merge)."""
    missing = [c for c in required if c not in df.columns]
    if missing:
        st.error(
            f"La tabla **{table}** no tiene las columnas esperadas: {missing}.\n\n"
            f"Columnas disponibles ({len(df.columns)}): {list(df.columns)}\n\n"
            "Revisa el nombre exacto con `DESCRIBE` en Athena y ajusta el nombre de columna "
            "en el código (data.py / geo.py / app.py) si difiere."
        )
        st.stop()
    return df


@st.cache_data(ttl=600, show_spinner="Cargando barrios y distritos...")
def load_barrios_distritos() -> pd.DataFrame:
    if config.USE_MOCK_DATA:
        return mock_data.generate_barrios_distritos()
    df = _athena_query(config.TABLE_BARRIOS_DISTRITOS)
    required = ["barrio_id", "nombre_barrio", "geometry_barrio", "distrito_id", "nombre_distrito", "geometry_distrito"]
    return _require_columns(df, required, config.TABLE_BARRIOS_DISTRITOS)


@st.cache_data(ttl=300, show_spinner="Cargando calidad del aire...")
def load_pollution() -> pd.DataFrame:
    if config.USE_MOCK_DATA:
        return mock_data.generate_pollution()
    df = _athena_query(config.TABLE_POLLUTION)
    required = ["station_id", "station_name", "environmental_risk", "geometry_station", "event_ts"]
    return _require_columns(df, required, config.TABLE_POLLUTION)


@st.cache_data(ttl=120, show_spinner="Cargando estado del tráfico...")
def load_traffic() -> pd.DataFrame:
    if config.USE_MOCK_DATA:
        return mock_data.generate_traffic()
    df = _athena_query(config.TABLE_TRAFFIC)
    required = ["tramo_id", "nombre_tramo", "estado", "geometry_tramo", "barrio_id", "distrito_id", "station_id", "event_ts"]
    return _require_columns(df, required, config.TABLE_TRAFFIC)


@st.cache_data(ttl=300, show_spinner="Cargando cortes de vía (OSM)...")
def load_out_of_service() -> pd.DataFrame:
    if config.USE_MOCK_DATA:
        df = mock_data.generate_out_of_service()
    else:
        df = _athena_query(config.TABLE_OUT_OF_SERVICE)
        required = ["tramo_osm_id", "nombre_tramo", "estado", "tipoincidencia", "barrio_id", "distrito_id", "geometry_tramo", "ingestion_ts"]
        df = _require_columns(df, required, config.TABLE_OUT_OF_SERVICE)
    # nombre_tramo llega como array<string> -> lo convertimos a texto para poder mostrarlo
    if "nombre_tramo" in df.columns:
        df["nombre_tramo_str"] = df["nombre_tramo"].apply(
            lambda v: ", ".join(v) if isinstance(v, (list, tuple)) else (v or "")
        )
    return df


def latest_per_key(df: pd.DataFrame, key, ts_col: str = "event_ts") -> pd.DataFrame:
    """Se queda con el registro más reciente de cada 'key'.

    'key' puede ser una columna ("station_id") o una lista de columnas que forman
    el identificador único ("tramo_id", "barrio_id", "distrito_id").
    """
    if df.empty:
        return df
    keys = key if isinstance(key, (list, tuple)) else [key]
    return (
        df.sort_values(ts_col)
        .groupby(keys, as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )


def solo_cerradas(out_of_service_df: pd.DataFrame) -> pd.DataFrame:
    """Filtra out-of-service dejando solo las filas cuyo estado más reciente es 'cerrada'."""
    if out_of_service_df.empty:
        return out_of_service_df
    mask = out_of_service_df["estado"].astype(str).str.lower().isin(
        [v.lower() for v in config.ESTADOS_CERRADO]
    )
    return out_of_service_df[mask].copy()

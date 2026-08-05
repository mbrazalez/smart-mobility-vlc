"""Utilidades para convertir columnas WKT (string) a geometrías / GeoDataFrames."""
import geopandas as gpd
import pandas as pd
from shapely import wkt


def wkt_to_gdf(df: pd.DataFrame, geom_col: str, crs: str = "EPSG:4326") -> gpd.GeoDataFrame:
    """Convierte una columna de texto WKT (POLYGON/LINESTRING/POINT/...) en geometría."""
    df = df.copy()

    def _parse(value):
        if not isinstance(value, str) or not value.strip():
            return None
        try:
            return wkt.loads(value)
        except Exception:
            return None

    geometry = df[geom_col].apply(_parse)
    gdf = gpd.GeoDataFrame(df.drop(columns=[geom_col]), geometry=geometry, crs=crs)
    gdf = gdf[gdf.geometry.notna()].copy()
    return gdf


def get_barrios_gdf(barrios_distritos_df: pd.DataFrame) -> gpd.GeoDataFrame:
    cols = ["barrio_id", "nombre_barrio", "distrito_id", "nombre_distrito", "geometry_barrio"]
    sub = barrios_distritos_df[cols].drop_duplicates(subset=["barrio_id"])
    return wkt_to_gdf(sub, "geometry_barrio")


def get_distritos_gdf(barrios_distritos_df: pd.DataFrame) -> gpd.GeoDataFrame:
    cols = ["distrito_id", "nombre_distrito", "geometry_distrito"]
    sub = barrios_distritos_df[cols].drop_duplicates(subset=["distrito_id"])
    return wkt_to_gdf(sub, "geometry_distrito")

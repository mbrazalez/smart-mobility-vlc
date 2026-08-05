# TFM Smart Mobility Valencia — Dashboard (prototipo)

Dashboard construido con **Streamlit + Folium + Plotly** para visualizar barrios/distritos,
estado del tráfico, calidad del aire y cortes de vía de Valencia a partir del Delta Lake
(consultado vía Amazon Athena).

## 1. Instalación

```bash
python -m venv .venv
source .venv/bin/activate      # En Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

> Nota: `geopandas` depende de GDAL/pyproj/fiona. Si el `pip install` falla en tu sistema
> (típico en Windows), prueba con un entorno conda: `conda install -c conda-forge geopandas`.

## 2. Ejecutar con datos de prueba (sin AWS)

Por defecto `config.py` tiene `USE_MOCK_DATA = True`, así que puedes lanzar el dashboard
inmediatamente para revisar la interfaz con datos sintéticos de Valencia:

```bash
streamlit run app.py
```

## 3. Conectar con Athena (datos reales)

1. Configura tus credenciales de AWS (variables de entorno, `~/.aws/credentials` o un perfil).
2. Asegúrate de tener un bucket S3 donde Athena pueda escribir los resultados de las queries.
3. Edita `config.py`:
   - `USE_MOCK_DATA = False`
   - `ATHENA_S3_OUTPUT = "s3://tu-bucket/tfm-dashboard/"`
   - (opcional) `AWS_PROFILE = "nombre-de-tu-perfil"`
4. Vuelve a lanzar `streamlit run app.py`.

## 4. Estructura

- `app.py` — Layout y orquestación (pestañas: mapa principal, cortes OSM, estadísticas).
- `data.py` — Acceso a datos (Athena o mock) con caché de Streamlit (`st.cache_data`).
- `mock_data.py` — Generador de datos sintéticos para desarrollar sin AWS.
- `geo.py` — Conversión de columnas WKT a GeoDataFrames.
- `maps.py` — Construcción de los mapas Folium (con capas y tooltips).
- `charts.py` — Gráficas Plotly de la pestaña de estadísticas.
- `config.py` — Configuración (tablas, colores, umbrales, flag mock).

## 5. Notas importantes

- **`ESTADOS_CERRADO`** en `config.py`: ajusta esta lista con los valores reales que use tu
  columna `estado` en `out-of-service-streets-gold` (revísalo con
  `SELECT DISTINCT estado FROM "tfm-smart-mobility-db"."out-of-service-streets-gold"`).
- El cruce estación de contaminación → distrito se hace vía `traffic-status-gold`
  (que enlaza `station_id` con `distrito_id`), ya que `pollution-levels-curated` no trae
  `distrito_id` directamente. Si una estación no aparece en ningún tramo de tráfico, no se
  podrá asignar a un distrito con este método (ver README principal / respuesta del chat
  para alternativas).
## 6. Pestaña de cálculo de rutas

- `route_api.py` invoca la Lambda directamente vía `boto3` (`lambda.invoke`, `RequestResponse`), sin API Gateway.
- Con `USE_MOCK_DATA = True` genera una ruta simulada (línea recta + una con un pequeño desvío) para poder probar la interfaz sin tener la Lambda desplegada.
- Con `USE_MOCK_DATA = False`, ajusta `ROUTE_LAMBDA_NAME` en `config.py` con el nombre real de tu función.

**IAM necesario además de crear la Lambda:**

1. El usuario/rol que usa el dashboard necesita `lambda:InvokeFunction` sobre el ARN de la función:
   ```json
   {
     "Sid": "InvokeRouteLambda",
     "Effect": "Allow",
     "Action": "lambda:InvokeFunction",
     "Resource": "arn:aws:lambda:eu-west-3:TU_ACCOUNT_ID:function:smart-mobility-route-calculator"
   }
   ```
2. El *execution role* de la propia Lambda (distinto del anterior) necesita `s3:GetObject` sobre `smart-mobility-vlc/delta/route-calculations/graph.pkl`, más la política gestionada `AWSLambdaBasicExecutionRole` (logs).
3. Configuración de la función: desplegar como **imagen de contenedor** (osmnx/networkx/geopandas no suelen caber en un zip+layer normal), timeout alto (60s+) y memoria generosa (1024–3008 MB) para el cold start de cargar el grafo pickled. No necesita VPC.


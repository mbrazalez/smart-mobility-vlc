"""Configuración global del dashboard."""

# --- Modo de datos ---
# True  -> usa datos sintéticos (mock_data.py), no necesita AWS. Ideal para desarrollar la UI.
# False -> consulta Athena de verdad (rellena ATHENA_S3_OUTPUT más abajo).
USE_MOCK_DATA = False

# --- Athena / AWS (solo si USE_MOCK_DATA = False) ---
ATHENA_DATABASE = "tfm-smart-mobility-db"
# Bucket S3 donde Athena escribe los resultados de las queries (obligatorio en modo real).
ATHENA_S3_OUTPUT = "s3://smart-mobility-vlc/athena-queries/"
# Perfil de AWS a usar (deja None para credenciales por defecto / variables de entorno)
AWS_PROFILE = None
# Región AWS donde están tu bucket de resultados y tu base de datos de Athena/Glue.
# IMPORTANTE: si no coincide con la región real del bucket, Athena falla con
# "Unable to verify/create output bucket".
AWS_REGION = "eu-west-3"

# --- Nombres de tablas ---
TABLE_BARRIOS_DISTRITOS = "barrios-distritos"
TABLE_POLLUTION = "pollution-levels-curated"
TABLE_TRAFFIC = "traffic-status-gold"
TABLE_OUT_OF_SERVICE = "out-of-service-streets-gold"

# --- Mapeos de dominio ---
ESTADO_TRAFICO = {0: "Fluido", 1: "Denso", 2: "Congestionado"}
ESTADO_TRAFICO_COLOR = {0: "#2ecc71", 1: "#f39c12", 2: "#e74c3c"}

# Valores de la columna "estado" de out-of-service-streets-gold que consideras "cerrada".
# Confirmado: la columna usa "Abierta" / "Cerrada". La comparación en charts.py ignora
# mayúsculas/minúsculas, así que basta con este valor.
ESTADOS_CERRADO = ["Cerrada"]

# Centro aproximado de Valencia para inicializar los mapas
VALENCIA_CENTER = (39.4699, -0.3763)
DEFAULT_ZOOM = 13
# Altura en píxeles de los mapas. Si en tu pantalla sigue haciendo falta scroll, baja este valor.
MAP_HEIGHT = 760
# El mapa de "Calcular ruta" tiene más elementos por encima (buscador, info de puntos), así
# que usa una altura más pequeña para que la pestaña quepa entera igualmente.
ROUTE_MAP_HEIGHT = 340

# --- Lambda de cálculo de rutas ---
# Nombre (o ARN) de la función Lambda que calcula rutas. Se invoca directamente vía boto3
# (lambda:InvokeFunction), no a través de API Gateway.
ROUTE_LAMBDA_NAME = "route_calculator"  # AJUSTA con el nombre real de tu función

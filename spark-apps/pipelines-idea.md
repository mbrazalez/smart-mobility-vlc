street-status-app.py:
1. BRONZE: Ingesta Raw
2. SILVER: Filtrar posibles NULLS
3. GOLD: Join tramo con distrito, barrio y niveles de contaminación

AIRFLOW:
Check if Kafka topic exits
if True:
    lanzar job spark
else:
    crear topic > lanzar job spark

check if job spark running OK
if True:
    SNS job successful
else:
    SNS error

in case of error/stop: SNS



pollution-levels-app.py:
1. BRONZE: Ingesta Raw
2. SILVER: Filtrar posibles NULLS
3. GOLD: Agregar valores de WeatherAPI a los missings

cronn cada hora (si pasa por el primer True var para ejecutarlo solo 1 vez?)
check if table exits
if True:
    API call > save result in BDD
else:
    create table > API call > save result in BDD
lanzar job de Spark
if error > SNS
if ends OK > SNS


out-of-service-roads-app.py:
1. BRONZE: Ingesta Raw
2. SILVER: Filtrar posibles NULLS
3. GOLD: Join tramo con distrito, barrio y niveles de contaminación

wait for new file > if new file > call script > update results
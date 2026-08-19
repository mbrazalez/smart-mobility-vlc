import numpy as np
import skfuzzy as fuzz
from skfuzzy import control as ctrl

FIS_VERSION = "v2-eea2024"

####################################################
# INPUTS
####################################################
pm10 = ctrl.Antecedent(np.arange(0, 301, 1), "pm10")
pm25 = ctrl.Antecedent(np.arange(0, 161, 1), "pm25")
wind = ctrl.Antecedent(np.arange(0, 41, 1), "wind")

####################################################
# OUTPUT
####################################################
risk = ctrl.Consequent(np.arange(0, 10.1, 0.1), "environmental_risk")

####################################################
# PM10
####################################################
pm10["good"] = fuzz.zmf(pm10.universe, 10, 20)
pm10["fair"] = fuzz.pimf(pm10.universe, 10, 20, 38, 52)
pm10["moderate"] = fuzz.pimf(pm10.universe, 38, 52, 105, 135)
pm10["poor"] = fuzz.pimf(pm10.universe, 105, 135, 180, 210)
pm10["very_poor"] = fuzz.smf(pm10.universe, 180, 210)

####################################################
# PM25
####################################################
pm25["good"] = fuzz.zmf(pm25.universe, 3, 7)
pm25["fair"] = fuzz.pimf(pm25.universe, 3, 7, 12, 18)
pm25["moderate"] = fuzz.pimf(pm25.universe, 12, 18, 44, 56)
pm25["poor"] = fuzz.pimf(pm25.universe, 44, 56, 82, 98)
pm25["very_poor"] = fuzz.smf(pm25.universe, 82, 98)

####################################################
# WIND
####################################################
wind["low"] = fuzz.trapmf(wind.universe, [0, 0, 6, 12])
wind["medium"] = fuzz.trimf(wind.universe, [8, 18, 28])
wind["high"] = fuzz.trapmf(wind.universe, [22, 30, 40, 40])

####################################################
# OUTPUT
####################################################
risk["very_low"]  = fuzz.trapmf(risk.universe, [0, 0, 1, 2.5])
risk["low"] = fuzz.trimf(risk.universe, [1, 2.5, 4.5])
risk["medium"] = fuzz.trimf(risk.universe, [3.5, 5, 6.5])
risk["high"] = fuzz.trimf(risk.universe, [5.5, 7.5, 9])
risk["very_high"] = fuzz.trapmf(risk.universe, [7.5, 9, 10, 10])
####################################################
# RULES
####################################################
# Orden de severidad de las etiquetas de contaminación
NIVELES_PM = ["good", "fair", "moderate", "poor", "very_poor"]
 
# Nivel de riesgo correspondiente a cada índice de severidad
NIVELES_RIESGO = ["very_low", "low", "medium", "high", "very_high"]
 
# Ajuste que introduce el viento sobre el nivel base
AJUSTE_VIENTO = {
    "low":     +1, # calma: menor dispersión, se agrava el nivel
    "medium":   0, # condiciones neutras
    "high":    -1, # viento intenso: mayor dispersión, se relaja
}
 
 
def _nivel_resultante(idx_pm10, idx_pm25, etiqueta_viento):
    """Determina la etiqueta de salida para una combinación de entradas.
 
    El nivel base lo fija el peor de los dos contaminantes, siguiendo el
    criterio del índice europeo de calidad del aire. El viento lo ajusta
    en un escalón, acotado al rango disponible.
    """
    base = max(idx_pm10, idx_pm25)
    ajustado = base + AJUSTE_VIENTO[etiqueta_viento]
    ajustado = max(0, min(len(NIVELES_RIESGO) - 1, ajustado))
    return NIVELES_RIESGO[ajustado]
 
 
def _construir_reglas():
    reglas = []
    for i, et_pm10 in enumerate(NIVELES_PM):
        for j, et_pm25 in enumerate(NIVELES_PM):
            for et_wind in AJUSTE_VIENTO:
                salida = _nivel_resultante(i, j, et_wind)
                reglas.append(
                    ctrl.Rule(
                        pm10[et_pm10] & pm25[et_pm25] & wind[et_wind],
                        risk[salida],
                    )
                )
    return reglas
 
 
rules = _construir_reglas()
 
 
####################################################
# CONTROL SYSTEM
####################################################
fis = ctrl.ControlSystem(rules)

# Funcion usada en el pipeline como UDF
def compute_environmental_risk(pm10_value, pm25_value, wind_value):

    if (
        pm10_value is None or
        pm25_value is None or
        wind_value is None
    ):
        return None


    simulation = ctrl.ControlSystemSimulation(fis)

    wind_value = min(max(float(wind_value), 0), 40)
    pm10_value = min(max(float(pm10_value), 0), 300)
    pm25_value = min(max(float(pm25_value), 0), 160)

    simulation.input["wind"] = wind_value
    simulation.input["pm10"] = float(pm10_value)
    simulation.input["pm25"] = float(pm25_value)
    simulation.input["wind"] = float(wind_value)

    simulation.compute()

    return float(simulation.output["environmental_risk"])
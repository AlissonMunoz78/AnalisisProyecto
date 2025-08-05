import pandas as pd
import json
import os
from textblob import TextBlob

# Carpeta donde están los archivos JSON en Windows
CARPETA = r"C:\Universidad\Analisis\Proyecto\datos_json"
archivos = [
    "deportes.json",
    "conciertos.json",
    "restaurantes.json",
    "transito.json"
]

def analizar_sentimiento(texto):
    try:
        polaridad = TextBlob(str(texto)).sentiment.polarity
        if polaridad > 0:
            return "Positivo"
        elif polaridad < 0:
            return "Negativo"
        else:
            return "Neutral"
    except:
        return "Error"

for archivo in archivos:
    ruta = os.path.join(CARPETA, archivo)
    if not os.path.exists(ruta):
        print(f" No encontrado: {archivo}")
        continue

    with open(ruta, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not data:
        print(f"Archivo vacío: {archivo}")
        continue
    df = pd.DataFrame(data)

    # Detectar una columna de texto automáticamente
    columna_texto = None
    for col in df.columns:
        if df[col].dtype == 'object':
            columna_texto = col
            break

    if columna_texto is None:
        print(f"No hay columna de texto en {archivo}")
        continue

    # Análisis de sentimiento
    df['sentimiento'] = df[columna_texto].apply(analizar_sentimiento)

    print(f"\n Análisis de sentimientos: {archivo}")
    print(df['sentimiento'].value_counts())
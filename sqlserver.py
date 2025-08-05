import pymssql
import pandas as pd
import json
import os

# Configuración de conexión
conn = pymssql.connect(server='localhost', user='LAPTOP-VN81MUTC/Lis', password='1234', database='DataSql')
cursor = conn.cursor()

# Ruta donde están tus archivos JSON
ruta_base = 'c:/Universidad/Analisis/Proyecto/Datasets'
archivos = ['deportes.json', 'conciertos.json', 'restaurantes.json', 'transito.json']

for archivo in archivos:
    ruta = os.path.join(ruta_base, archivo)
    nombre_tabla = archivo.replace('.json', '')

    # Leer el JSON
    with open(ruta, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Si es lista de dicts
    if isinstance(data, list) and isinstance(data[0], dict):
        columnas = list(data[0].keys())
        # Crear tabla si no existe
        columnas_sql = ', '.join([f"[{col}] TEXT" for col in columnas])
        cursor.execute(f"IF OBJECT_ID('{nombre_tabla}', 'U') IS NOT NULL DROP TABLE {nombre_tabla}")
        cursor.execute(f"CREATE TABLE {nombre_tabla} ({columnas_sql})")

        for fila in data:
            valores = [str(fila.get(col, '')) for col in columnas]
            placeholders = ','.join(['%s'] * len(valores))
            cursor.execute(f"INSERT INTO {nombre_tabla} VALUES ({placeholders})", valores)

        print(f"✅ Tabla {nombre_tabla} insertada correctamente.")
    else:
        print(f"⚠️ Formato no válido en {archivo}.")

conn.commit()
cursor.close()
conn.close()

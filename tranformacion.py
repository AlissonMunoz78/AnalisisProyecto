import os
import pandas as pd
import json
from sqlalchemy import create_engine
from pymongo import MongoClient
import couchdb

# Configuración de conexiones
MYSQL_URL = 'mysql+pymysql://root:root@localhost/datasets'
POSTGRES_URL = 'postgresql://postgres:1234@db.xtzzutseazgrgwabecvw.supabase.co:5432/postgres'
MONGODB_LOCAL_URL = 'mongodb://localhost:27017/'
MONGODB_ATLAS_URL = 'mongodb+srv://Lizzie:1234@cluster0.vchzz3b.mongodb.net/'
COUCHDB_URL = 'http://user:12345@127.0.0.1:5984/'

# Ruta de los archivos CSV
CSV_FOLDER = 'datos_csv'

# Función 1: CSV → JSON (con limpieza)
def csv_a_json(nombre_csv):
    ruta = os.path.join(CSV_FOLDER, nombre_csv)
    df = pd.read_csv(ruta)

    # Limpieza de datos
    df.dropna(how='all', inplace=True)  # Elimina filas completamente vacías
    df.drop_duplicates(inplace=True)    # Elimina duplicados
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]  # Normaliza columnas

    # Intento de convertir columnas que parecen fechas
    for col in df.columns:
        if 'fecha' in col:
            try:
                df[col] = pd.to_datetime(df[col])
            except:
                pass

    # Guardar como JSON
    nombre_sin_ext = os.path.splitext(nombre_csv)[0]
    ruta_json = f"{nombre_sin_ext}.json"
    df.to_json(ruta_json, orient='records', indent=4, force_ascii=False)
    print(f"Convertido {nombre_csv} a {ruta_json} con limpieza")
    return df, nombre_sin_ext, ruta_json

# Función 2: Insertar en MySQL y PostgreSQL
def insertar_mysql_postgres(df, nombre_tabla):
    try:
        mysql_engine = create_engine(MYSQL_URL)
        df.to_sql(nombre_tabla, con=mysql_engine, if_exists='replace', index=False)
        print(f"Insertado en MySQL: {nombre_tabla}")
    except Exception as e:
        print(f"Error MySQL: {e}")
    try:
        postgres_engine = create_engine(POSTGRES_URL)
        df.to_sql(nombre_tabla, con=postgres_engine, if_exists='replace', index=False)
        print(f"Insertado en PostgreSQL: {nombre_tabla}")
    except Exception as e:
        print(f"Error PostgreSQL: {e}")

# Función 3: Migrar a MongoDB Compass y Atlas
def insertar_mongodb(json_file, nombre_tabla):
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    for url, desc in [(MONGODB_LOCAL_URL, "Compass"), (MONGODB_ATLAS_URL, "Atlas")]:
        try:
            client = MongoClient(url)
            db = client['Migracion']
            db[nombre_tabla].drop()
            db[nombre_tabla].insert_many(data)
            print(f"Insertado en MongoDB {desc}: {nombre_tabla}")
        except Exception as e:
            print(f"Error MongoDB {desc}: {e}")

# Función 4: Migrar a CouchDB
def insertar_couchdb(json_file, nombre_tabla):
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        server = couchdb.Server(COUCHDB_URL)
        if nombre_tabla in server:
            del server[nombre_tabla]
        db = server.create(nombre_tabla)
        for doc in data:
            db.save(doc)
        print(f"Insertado en CouchDB: {nombre_tabla}")
    except Exception as e:
        print(f"Error CouchDB: {e}")

# Función 5: JSON → CSV
def json_a_csv(nombre_json):
    nombre_sin_ext = os.path.splitext(nombre_json)[0]
    df = pd.read_json(nombre_json, encoding='utf-8')
    ruta_csv = f"{nombre_sin_ext}_nuevo.csv"
    df.to_csv(ruta_csv, index=False, encoding='utf-8-sig')
    print(f"Convertido {nombre_json} a {ruta_csv}")

# Función principal
def procesar_archivo(nombre_csv):
    df, nombre_tabla, json_file = csv_a_json(nombre_csv)
    insertar_mysql_postgres(df, nombre_tabla)
    insertar_mongodb(json_file, nombre_tabla)
    insertar_couchdb(json_file, nombre_tabla)
    json_a_csv(json_file)

# Ejecutar para todos los archivos
if __name__ == "__main__":
    archivos_csv = [
        "transito.csv",
        "deportes.csv",
        "conciertos.csv",
        "restaurantes.csv",
        "noticias.csv"
    ]
    for archivo in archivos_csv:
        procesar_archivo(archivo)

    print("Proceso completo con limpieza incluida.")
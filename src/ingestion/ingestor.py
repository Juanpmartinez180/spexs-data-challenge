import os
import shutil
import pandas as pd
import psycopg2
import io
from datetime import datetime
from dotenv import load_dotenv
import uuid

load_dotenv()

# Definir rutas de las carpetas
LANDING_ZONE = "data/landing"
ARCHIVE_ZONE = "data/archive"
ingestion_lookup = {
    "cell_phone_trips.csv": "bronze.cell_phone_data",
    "navigation_trips.csv": "bronze.car_navigation_data",
    "app_logs.csv": "bronze.app_usage_data"
}

def get_db_connection():
    """
    Set up DB connection
    """
    return psycopg2.connect(
        host="localhost",
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

def process_new_files():
    """
    Escanear la carpeta Landing, procesar los archivos y archivarlos
    """
    os.makedirs(LANDING_ZONE, exist_ok=True)
    os.makedirs(ARCHIVE_ZONE, exist_ok=True)

    # Listar archivos CSV en la landing zone
    files = [f for f in os.listdir(LANDING_ZONE) if f.endswith('.csv')]
    if not files:
        print("No hay archivos nuevos en 'data/new_datasets'. Pipeline en espera.")
        return
    print(f"Se encontraron {len(files)} archivos nuevos. Iniciando ingesta...")

    for file_name in files:
        input_path = os.path.join(LANDING_ZONE, file_name)
        archive_path = os.path.join(ARCHIVE_ZONE, file_name)
        
        # Determinamos la tabla destino (puedes parametrizar esto por nombre de archivo)
        # Por ahora usaremos la tabla general de bronze
        target_table = ingestion_lookup.get(file_name)

        if target_table:
            print(f"Iniciando ingesta: {file_name} -> {target_table}") 
            success = ingest_and_archive(input_path, target_table, archive_path)
            if success:
                print(f"Archivo {file_name} procesado y movido a 'data/archive'.")
            else:
                print(f"Falló el procesamiento de {file_name}. Se mantiene en landing para revisión.")
        else:
            print(f"El archivo '{file_name}' no está configurado para ingesta. Saltando...")

def ingest_and_archive(file_path: str, table_name: str, archive_path: str):
    """
    Carga un archivo CSV a una tabla específica en el esquema bronze. 
    Luego lo mueve a la zona de archive
    """
    if not os.path.exists(file_path):
        print(f"Archivo no encontrado: {file_path}. Skipping...")
        return

    conn = get_db_connection()
    cursor = conn.cursor()
    start_time = datetime.now()
    log_id = str(uuid.uuid4())
    
    try:
        # Generar logs para la audit table
        cursor.execute("""
            INSERT INTO audit.ingestion_logs (log_id, table_name, source_file, execution_start, status)
            VALUES (%s, %s, %s, %s, 'processing')
        """, (log_id, table_name, file_path, start_time))
        conn.commit()

        total_records = 0
        chunk_size = 100000 

        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            # Agregar campos de metadata
            chunk['extracted_at'] = start_time
            chunk['inserted_at'] = datetime.now()
            chunk['layer'] = 'bronze'
            
            output = io.StringIO()
            chunk.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)

            #Definimos copy statement dinamico
            sql = f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '')"
            cursor.copy_expert(sql, output)
            conn.commit()

            total_records += len(chunk)

        print(f"Finalizado en: {datetime.now() - start_time}\n")

        # Mover el archivo al archive
        shutil.move(file_path, archive_path)

        # Registrar exito en la audit table
        cursor.execute("""
            UPDATE audit.ingestion_logs 
            SET execution_end = %s, status = 'success', records_inserted = %s
            WHERE log_id = %s
        """, (datetime.now(), total_records, log_id))
        conn.commit()
        return True

    except Exception as e:
        conn.rollback()
        # Registrar falla en la audit table
        cursor.execute("""
            UPDATE audit.ingestion_logs 
            SET execution_end = %s, status = 'fail', error_message = %s
            WHERE log_id = %s
        """, (datetime.now(), str(e), log_id))
        print(f"Error en ingesta, procesando {file_path}: {e}")
        conn.commit()
        return False

    finally:
        cursor.close()
        conn.close()
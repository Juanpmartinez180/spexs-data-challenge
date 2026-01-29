import pandas as pd
import psycopg2
import os
import io
from datetime import datetime
from dotenv import load_dotenv
import uuid

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

def ingest_csv_to_bronze(file_path: str, table_name: str):
    """
    Carga un archivo CSV a una tabla específica en el esquema bronze.
    """
    if not os.path.exists(file_path):
        print(f"Archivo no encontrado: {file_path}. Skipping...")
        return

    conn = get_db_connection()
    cursor = conn.cursor()
    
    print(f"Procesando fuente: {file_path} -> Tabla: {table_name}")
    # Generar logs para la audit table
    start_time = datetime.now()
    log_id = str(uuid.uuid4())

    cursor.execute("""
        INSERT INTO audit.ingestion_logs (log_id, table_name, source_file, execution_start, status)
        VALUES (%s, %s, %s, %s, 'processing')
    """, (log_id, table_name, file_path, start_time))
    conn.commit()

    total_records = 0

    try:
        chunk_size = 100000 
        for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
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
            print(f" Chunk {i+1} cargado...")

        print(f"Finalizado en: {datetime.now() - start_time}\n")

        # Registrar exito en la audit table
        cursor.execute("""
            UPDATE audit.ingestion_logs 
            SET execution_end = %s, status = 'success', records_inserted = %s
            WHERE log_id = %s
        """, (datetime.now(), total_records, log_id))

    except Exception as e:
        # Registrar falla en la audit table
        cursor.execute("""
            UPDATE audit.ingestion_logs 
            SET execution_end = %s, status = 'fail', error_message = %s
            WHERE log_id = %s
        """, (datetime.now(), str(e), log_id))
        print(f"Error en ingesta: {e}")
        conn.rollback()
    finally:
        conn.commit()
        cursor.close()
        conn.close()
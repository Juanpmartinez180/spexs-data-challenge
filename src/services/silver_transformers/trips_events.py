import psycopg2
import os
import uuid
from datetime import datetime
from dotenv import load_dotenv
from src.utils.db_settings import get_db_connection

load_dotenv()

def run_silver_transformation():

    bronze_sources = [
        "bronze.cell_phone_data",
        "bronze.car_navigation_data",
        "bronze.app_usage_data"
    ]
    table_name = "silver.trips_enriched"

    conn = get_db_connection()
    cursor = conn.cursor()

    # Audit logs setup

    for source in bronze_sources:
        log_id = str(uuid.uuid4())
        start_time = datetime.now()
        
        print("[Log {log_id}] Iniciando transformación a Capa Silver...")
        
        try:
            # Registrar transacion en la audit table
            cursor.execute("""
                INSERT INTO audit.ingestion_logs (log_id, table_name, source_file, execution_start, status)
                VALUES (%s, %s, %s, %s, 'processing')
            """, (log_id, table_name, f"Step: {source}", start_time))
            conn.commit()
            # Ejecutar transformacion ELT
            sql = f"""
                INSERT INTO silver.trips_events (
                    region, origin_geohash, destination_geohash, departure_time, datasource, similarity_key
                )
                SELECT 
                    region,
                    LEFT(origin_coord, 10) as origin_gh, 
                    LEFT(destination_coord, 10) as dest_gh,
                    datetime::timestamp,
                    datasource,
                    md5(region || LEFT(origin_coord, 10) || LEFT(destination_coord, 10) || date_trunc('hour', datetime::timestamp)::text)
                FROM {source}
                ON CONFLICT (similarity_key) DO NOTHING; -- Evitar duplicados si el proceso se re-ejecuta
            """
            
            cursor.execute(sql)
            records_affected = cursor.rowcount
            conn.commit()
            print(f"Capa Silver actualizada. Fuente: {source}. Con {records_affected} filas procesadas.")

            # Update audit table
            cursor.execute("""
                UPDATE audit.ingestion_logs 
                SET execution_end = %s, status = 'success', records_inserted = %s
                WHERE log_id = %s
            """, (datetime.now(), records_affected, log_id))
            
        except Exception as e:
            conn.rollback()
            error_msg = str(e)
            cursor.execute("""
                UPDATE audit.ingestion_logs 
                SET execution_end = %s, status = 'fail', error_message = %s
                WHERE log_id = %s
            """, (datetime.now(), error_msg, log_id))
            print(f"Error en Silver integrando {source}: {error_msg}")
    conn.commit()
    cursor.close()
    conn.close()
    print("Integración a Silver finalizada.")
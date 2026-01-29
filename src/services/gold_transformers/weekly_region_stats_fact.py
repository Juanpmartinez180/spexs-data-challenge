import psycopg2
import os
import uuid
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def run_gold_transformation():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cursor = conn.cursor()
    log_id = str(uuid.uuid4())
    start_time = datetime.now()
    target_table = "gold.weekly_region_stats_fact"
    source_tables = "silver.trips_events"

    print(f"Cargando {target_table}")

    try:
        # Registrar audit table
        cursor.execute("""
            INSERT INTO audit.ingestion_logs (log_id, table_name, source_file, execution_start, status)
            VALUES (%s, %s, %s, %s, 'processing')
        """, (log_id, target_table, source_tables, start_time))

        # Agregación: Promedio semanal por región
        # Calculamos el total de viajes por semana y dividimos por 7
        sql = f"""
            INSERT INTO {target_table} (region, week_number, year, avg_trips_daily, total_trips)
            SELECT 
                region,
                EXTRACT(WEEK FROM departure_time) as week_number,
                EXTRACT(YEAR FROM departure_time) as year,
                COUNT(*) / 7.0 as avg_trips_daily,
                COUNT(*) as total_trips
            FROM {source_tables}
            GROUP BY region, year, week_number
            ON CONFLICT (region, week_number, year) 
            DO UPDATE SET
                avg_trips_daily = EXCLUDED.avg_trips_daily,
                total_trips = EXCLUDED.total_trips,
                last_updated = CURRENT_TIMESTAMP;
        """
        cursor.execute(sql)
        records = cursor.rowcount

        cursor.execute("""
            UPDATE audit.ingestion_logs 
            SET execution_end = %s, status = 'success', records_inserted = %s
            WHERE log_id = %s
        """, (datetime.now(), records, log_id))
        
        conn.commit()
        print(f"Tabla {target_table} actualizada: {records} filas.")

    except Exception as e:
        conn.rollback()
        error_msg = str(e)
        cursor.execute("""
            UPDATE audit.ingestion_logs 
            SET execution_end = %s, status = 'fail', error_message = %s
            WHERE log_id = %s
        """, (datetime.now(), error_msg, log_id))
        print(f"Error en Gold integrando {source_tables}: {error_msg}")
    finally:
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Integracion a {target_table} finalizada.")
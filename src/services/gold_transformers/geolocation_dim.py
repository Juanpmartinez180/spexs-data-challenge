import psycopg2
import os
import uuid
from datetime import datetime
from dotenv import load_dotenv
from geopy.geocoders import Nominatim
import time

load_dotenv()

def run_transformation_task():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cursor = conn.cursor()
    log_id = str(uuid.uuid4())
    start_time = datetime.now()
    target_table = "gold.geolocation_dim"
    source_tables = "silver.trips_events"
    auxiliary_source_table = "bronze.cell_phone_data"

    print(f"Cargando {target_table}")

    try:
        # Registrar audit table
        cursor.execute("""
            INSERT INTO audit.ingestion_logs (log_id, table_name, source_file, execution_start, status)
            VALUES (%s, %s, %s, %s, 'processing')
        """, (log_id, target_table, source_tables, start_time))

        # Obtener coordenadas del bounding box basandose en los minimos y maximos encontrados en las tablas origen
        bbox_query = f"""
            SELECT 
                region,
                MIN(CAST(split_part(replace(replace(origin_coord, 'POINT (', ''), ')', ''), ' ', 2) AS FLOAT)) as min_lat,
                MIN(CAST(split_part(replace(replace(origin_coord, 'POINT (', ''), ')', ''), ' ', 1) AS FLOAT)) as min_lon,
                MAX(CAST(split_part(replace(replace(origin_coord, 'POINT (', ''), ')', ''), ' ', 2) AS FLOAT)) as max_lat,
                MAX(CAST(split_part(replace(replace(origin_coord, 'POINT (', ''), ')', ''), ' ', 1) AS FLOAT)) as max_lon
            FROM {auxiliary_source_table}
            GROUP BY region
        """
        cursor.execute(bbox_query)
        regions_data = cursor.fetchall()
        records = 0
        for row in regions_data:
            region, min_lat, min_lon, max_lat, max_lon = row
            country = get_country_from_region(region)
            bbox_str = f"{min_lat},{min_lon},{max_lat},{max_lon}"

            # Insertar valores en la tabla final
            sql = f"""
                INSERT INTO {target_table} (region, country, region_bounding_box)
                VALUES (%s, %s, %s)
                ON CONFLICT (region) 
                DO UPDATE SET 
                    region_bounding_box = EXCLUDED.region_bounding_box,
                    country = EXCLUDED.country,
                    last_updated = CURRENT_TIMESTAMP;
                """
            cursor.execute(sql, (region, country, bbox_str))
            records += 1

        # Actualizar audit table
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

def get_country_from_region(region_name):
    """
    Encontrar el pais basandose en la region
    """
    # Initialize the geolocator with a unique user_agent
    geolocator = Nominatim(user_agent="spexs_challenge") 
    
    try:
        # Geocode the region name
        location = geolocator.geocode(region_name)
        
        if location:
            # The location object has an 'address' attribute, which is a full string.
            # A common approach is to parse this string or use the raw data if available.
            
            raw_data = location.raw
            country_name = raw_data.get('address', {}).get('country')
            
            if country_name:
                return country_name
            else:
                # If structured data extraction fails, fall back to parsing the address string
                return location.address.split(',')[-1].strip()
        else:
            return "Region not found"
            
    except Exception as e:
        return f"An error occurred: {e}"
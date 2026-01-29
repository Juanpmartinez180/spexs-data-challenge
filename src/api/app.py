from fastapi import FastAPI, WebSocket
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
app = FastAPI()

def get_db():
    """
    Configurar conexion a la DB
    """
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

@app.get("/stats")
def get_stats(region: str, week: int, year: int):
    """Retorna el promedio semanal pre-calculado."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT wr.region,gd.country, wr.avg_trips_daily, gd.region_bounding_box
        FROM gold.weekly_region_stats_fact wr
        LEFT JOIN gold.geolocation_dim gd ON wr.region = gd.region
        WHERE wr.region = %s AND wr.week_number = %s AND wr.year = %s
    """, (region, week, year))

    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return {"region": region,
        "country":result[1], 
        "weekly_avg": float(result[2]) if result else 'Region o periodo no encontrado.',
        "bounding_box": result[3]
    }

@app.websocket("/ws/status")
async def status_socket(websocket: WebSocket):
    """
    Definir estado del proceso
    """
    await websocket.accept()
    conn = get_db()
    cursor = conn.cursor()
    # ultimo estado de la tabla de auditoría
    cursor.execute(
        "SELECT status, table_name, execution_end FROM audit.ingestion_logs where table_name = 'gold.weekly_region_stats_fact' ORDER BY execution_start DESC LIMIT 1"
        )
    last_status = cursor.fetchone()
    await websocket.send_json({"last_event": last_status[0],
        "table": last_status[1], 
        "last_execution_on": str(last_status[2])}
        )
    await websocket.close()
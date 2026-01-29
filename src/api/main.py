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
        SELECT avg_trips_daily 
        FROM gold.weekly_region_stats_fact
        WHERE region = %s AND week_number = %s AND year = %s
    """, (region, week, year))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return {"region": region, "weekly_avg": float(result[0]) if result else 'Region o periodo no encontrado.'}

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
        "SELECT status, table_name FROM audit.ingestion_logs where table_name = 'gold.weekly_region_stats_fact' ORDER BY execution_start DESC LIMIT 1"
        )
    last_status = cursor.fetchone()
    await websocket.send_json({"last_event": last_status[0], "table": last_status[1]})
    await websocket.close()
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def test_connection():
    try:
        conn = psycopg2.connect(
            host="localhost", # O "db" si el script esta dentro de Docker
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        print("Conexión exitosa a la base de datos.")
        
        cur = conn.cursor()
        cur.execute("SELECT schema_name FROM information_schema.schemata;")
        schemas = [row[0] for row in cur.fetchall()]
        print(f"Esquemas encontrados: {schemas}")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error conectando: {e}")

if __name__ == "__main__":
    test_connection()
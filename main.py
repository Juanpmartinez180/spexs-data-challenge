from fastapi import FastAPI
from src.ingestion.ingestor import ingest_csv_to_bronze

app = FastAPI()

@app.get("/")
def read_root():
    return {"status": "Spexs Data Challenge API is running"}

def run_pipeline():
    # Definición de fuentes y sus destinos
    ingestion_config = [
        {
            "file": "data/cell_phone_trips.csv", 
            "table": "bronze.cell_phone_data"
        },
        {
            "file": "data/navigation_trips.csv", 
            "table": "bronze.car_navigation_data"
        },
        {
            "file": "data/app_logs.csv", 
            "table": "bronze.app_usage_data"
        }
    ]

    print("--- INICIANDO PIPELINE DE DATOS SPEXS ---")
    
    for source in ingestion_config:
        ingest_csv_to_bronze(source["file"], source["table"])

    print("--- PIPELINE FINALIZADO EXITOSAMENTE ---")

if __name__ == "__main__":
    run_pipeline()
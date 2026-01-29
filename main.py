from fastapi import FastAPI
from src.ingestion.ingestor import ingest_csv_to_bronze
from src.services.silver_transformers.trips_events import run_silver_transformation
from src.services.gold_transformers.weekly_region_stats_fact import run_gold_transformation

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
    # Ejecutar Bronze ingestion
    print("--- INICIANDO PIPELINE DE DATOS SPEXS ---")
    
    for source in ingestion_config:
        ingest_csv_to_bronze(source["file"], source["table"])

    print("--- PIPELINE CSV->BRONZE FINALIZADO EXITOSAMENTE ---")

    # Ejecutar Silver ingestion
    run_silver_transformation()

    print("--- PIPELINE BRONZE->SILVER FINALIZADO EXITOSAMENTE ---")

    # Ejecutar Silver ingestion
    run_gold_transformation()

    print("--- PIPELINE SILVER->GOLD FINALIZADO EXITOSAMENTE ---")

if __name__ == "__main__":
    run_pipeline()
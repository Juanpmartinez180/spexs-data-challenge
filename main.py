from fastapi import FastAPI
from src.ingestion.ingestor import process_new_files
from src.services.silver_transformers.trips_events import run_silver_transformation
#from src.services.gold_transformers.weekly_region_stats_fact import run_gold_transformation
from src.services.gold_transformers import weekly_region_stats_fact, events_fact

def run_pipeline():
    print("--- INICIANDO PIPELINE DE DATOS SPEXS ---")

    # Ejecutar Bronze ingestion
    process_new_files()
    print("--- PIPELINE CSV->BRONZE FINALIZADO EXITOSAMENTE ---")

    # Ejecutar Silver ingestion
    run_silver_transformation()
    print("--- PIPELINE BRONZE->SILVER FINALIZADO EXITOSAMENTE ---")

    # Ejecutar Gold ingestion
    #weekly_region_stats_fact.run_transformation_task()
    events_fact.run_transformation_task()

    print("--- PIPELINE SILVER->GOLD FINALIZADO EXITOSAMENTE ---")

if __name__ == "__main__":
    run_pipeline()
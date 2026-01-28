from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def read_root():
    return {"status": "Spexs Data Challenge API is running"}
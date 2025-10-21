# src/serve/app.py
from fastapi import FastAPI
from pydantic import BaseModel
from pathlib import Path
import joblib

app = FastAPI(title="Traffic Congestion Inference")

MODEL_PATH = Path("data/models/production/model.joblib")
clf = joblib.load(MODEL_PATH) if MODEL_PATH.exists() else None

class PredictIn(BaseModel):
    minute_of_day: int
    weekday: int
    distance_m: int
    free_flow_sec: int
    lag1: float
    lag3_mean: float

@app.get("/healthz")
def healthz():
    return {"ok": clf is not None}

@app.post("/predict")
def predict(x: PredictIn):
    if clf is None:
        return {"error": "model not loaded"}
    import numpy as np
    arr = np.array([[x.minute_of_day, x.weekday, x.distance_m, x.free_flow_sec, x.lag1, x.lag3_mean]])
    y = clf.predict(arr)[0]
    return {"congestion_level": int(y)}

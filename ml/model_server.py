"""
FastAPI serving XGBoost fraud model.
Start: uvicorn ml.model_server:app --host 0.0.0.0 --port 8001
"""
import joblib
import numpy as np
from pathlib import Path
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

MODEL_DIR = Path("ml/models")

app = FastAPI(title="Fraud Detection Model Server", version="1.0")

# Load once at startup
model         = joblib.load(MODEL_DIR / "fraud_model.pkl")
amount_scaler = joblib.load(MODEL_DIR / "amount_scaler.pkl")
time_scaler   = joblib.load(MODEL_DIR / "time_scaler.pkl")
THRESHOLD     = 0.5

FEATURE_NAMES = [f"V{i}" for i in range(1, 29)] + ["Amount", "Time"]


class TransactionFeatures(BaseModel):
    V1:float;  V2:float;  V3:float;  V4:float;  V5:float
    V6:float;  V7:float;  V8:float;  V9:float;  V10:float
    V11:float; V12:float; V13:float; V14:float; V15:float
    V16:float; V17:float; V18:float; V19:float; V20:float
    V21:float; V22:float; V23:float; V24:float; V25:float
    V26:float; V27:float; V28:float
    Amount: float = Field(..., ge=0)
    Time:   float = Field(..., ge=0)


class PredictionResult(BaseModel):
    fraud_probability: float
    is_fraud: bool
    model_version: str = "xgboost-v1.0"


@app.get("/health")
def health():
    return {"status": "ok", "model_loaded": model is not None}


@app.post("/predict", response_model=PredictionResult)
def predict(f: TransactionFeatures):
    try:
        # V1-V28 are already PCA-scaled — only scale Amount and Time
        raw = np.array([[getattr(f, feat) for feat in FEATURE_NAMES]])
        raw[0, 28] = float(amount_scaler.transform([[f.Amount]])[0][0])
        raw[0, 29] = float(time_scaler.transform([[f.Time]])[0][0])

        prob = float(model.predict_proba(raw)[0][1])
        return PredictionResult(fraud_probability=prob, is_fraud=prob >= THRESHOLD)
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))

"""
FastAPI serving XGBoost fraud model.
Start: uvicorn ml.model_server:app --host 0.0.0.0 --port 8001

Endpoints:
  GET  /health                      — liveness probe
  POST /predict                     — fraud probability score
  GET  /models                      — list all versioned models
  POST /models/{version}/activate   — rollback to a previous version
  GET  /metrics                     — Prometheus scrape endpoint
"""
import joblib
import json
import os
import numpy as np
import threading
from collections import deque
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter, Gauge

def load_env():
    env_path = Path(".env")
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ.setdefault(key.strip(), val.strip())

load_env()

MODEL_DIR = Path("ml/models")
REGISTRY_PATH = MODEL_DIR / "model_registry.json"

app = FastAPI(title="Fraud Detection Model Server", version="2.0")

# ── Prometheus custom metrics ────────────────────────────────────────────────
fraud_predictions = Counter(
    "fraud_predictions_total",
    "Total predictions by fraud outcome",
    ["is_fraud"],
)
model_info_gauge = Gauge(
    "fraud_model_info",
    "Active model metadata (always=1, labels carry version info)",
    ["version", "auroc"],
)
feature_drift_gauge = Gauge(
    "model_feature_drift_zscore",
    "Feature drift Z-score compared to training baseline (rolling window of 1000)",
    ["feature"]
)
prediction_mean_gauge = Gauge(
    "model_prediction_mean_probability",
    "Rolling average predicted fraud probability (rolling window of 1000)"
)

# Instrument all FastAPI routes automatically (latency histogram, request count)
Instrumentator().instrument(app).expose(app)


# ── Model state (mutable for hot-swap) ───────────────────────────────────────
_state: dict = {}
window_lock = threading.Lock()
feature_window = deque(maxlen=1000)
prediction_window = deque(maxlen=1000)


def _load_version(version: str) -> None:
    """Load a specific versioned model into _state. version is timestamp string."""
    suffix = f"_{version}" if version != "latest" else ""
    if version == "latest":
        model_path  = MODEL_DIR / "fraud_model.pkl"
        scaler_path = MODEL_DIR / "amount_scaler.pkl"
        time_path   = MODEL_DIR / "time_scaler.pkl"
    else:
        model_path  = MODEL_DIR / f"fraud_model_{version}.pkl"
        scaler_path = MODEL_DIR / f"amount_scaler_{version}.pkl"
        time_path   = MODEL_DIR / f"time_scaler_{version}.pkl"

    if not model_path.exists():
        raise FileNotFoundError(f"Model version '{version}' not found at {model_path}")

    loaded_model = joblib.load(model_path)
    loaded_amount = joblib.load(scaler_path)
    loaded_time = joblib.load(time_path)

    global _state
    stats_path = MODEL_DIR / f"baseline_stats_{version}.json" if version != "latest" else MODEL_DIR / "baseline_stats.json"
    
    baseline_mean = {}
    baseline_std = {}
    if stats_path.exists():
        try:
            stats = json.loads(stats_path.read_text())
            baseline_mean = stats.get("mean", {})
            baseline_std = stats.get("std", {})
        except Exception as e:
            print(f"Warning: failed to load baseline stats: {e}")
            
    features_list = [f"V{i}" for i in range(1, 29)] + ["Amount_sc", "Time_sc"]
    for feat in features_list:
        baseline_mean.setdefault(feat, 0.0)
        baseline_std.setdefault(feat, 1.0)
        if baseline_std[feat] <= 1e-6:
            baseline_std[feat] = 1.0
            
    _state = {
        "model": loaded_model,
        "amount_scaler": loaded_amount,
        "time_scaler": loaded_time,
        "version": version,
        "baseline_mean": baseline_mean,
        "baseline_std": baseline_std
    }

    # Determine AUROC from registry
    auroc = "unknown"
    if REGISTRY_PATH.exists():
        registry = json.loads(REGISTRY_PATH.read_text())
        for entry in registry:
            if entry.get("version") == version or version == "latest":
                auroc = str(entry.get("auroc", "unknown"))
                if version == "latest":
                    # get last entry's auroc
                    auroc = str(registry[-1].get("auroc", "unknown"))
                break

    # Update Prometheus gauge
    model_info_gauge.labels(version=version, auroc=auroc).set(1)


@app.on_event("startup")
def startup():
    _load_version("latest")


# ── Schema ────────────────────────────────────────────────────────────────────
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
    model_version: str


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {
        "status": "ok",
        "model_loaded": bool(_state),
        "model_version": _state.get("version", "unknown"),
    }


@app.post("/predict", response_model=PredictionResult)
def predict(f: TransactionFeatures):
    try:
        raw = np.array([[getattr(f, feat) for feat in FEATURE_NAMES]])
        raw[0, 28] = float(_state["amount_scaler"].transform([[f.Amount]])[0][0])
        raw[0, 29] = float(_state["time_scaler"].transform([[f.Time]])[0][0])

        prob = float(_state["model"].predict_proba(raw)[0][1])
        is_fraud = prob >= float(os.getenv("ML_THRESHOLD", "0.5"))

        fraud_predictions.labels(is_fraud=str(is_fraud).lower()).inc()

        # Track features in the rolling window for drift monitoring
        feature_vector = raw[0].copy()
        with window_lock:
            feature_window.append(feature_vector)
            prediction_window.append(prob)
            
            # Compute rolling statistics if we have at least 10 sample predictions
            if len(feature_window) >= 10:
                arr = np.array(feature_window)
                rolling_means = arr.mean(axis=0)
                
                # Expose drift for key features
                # V14 -> index 13, V12 -> index 11, V17 -> index 16, Amount_sc -> index 28
                for key_feat, idx in [("V14", 13), ("V12", 11), ("V17", 16), ("Amount_sc", 28)]:
                    b_mean = _state["baseline_mean"].get(key_feat, 0.0)
                    b_std = _state["baseline_std"].get(key_feat, 1.0)
                    drift_z = abs(rolling_means[idx] - b_mean) / b_std
                    feature_drift_gauge.labels(feature=key_feat).set(drift_z)
                
                # Expose rolling mean of fraud predictions
                prediction_mean_gauge.set(float(np.mean(prediction_window)))

        return PredictionResult(
            fraud_probability=prob,
            is_fraud=is_fraud,
            model_version=_state.get("version", "unknown"),
        )
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))


@app.get("/models")
def list_models():
    """List all versioned models with their AUROC from the registry."""
    if not REGISTRY_PATH.exists():
        return {"active_version": _state.get("version"), "versions": []}
    registry = json.loads(REGISTRY_PATH.read_text())
    return {
        "active_version": _state.get("version"),
        "versions": registry,
    }


@app.post("/models/{version}/activate")
def activate_model(version: str):
    """Hot-swap to a specific model version without restarting the server."""
    try:
        _load_version(version)
        return {"status": "ok", "active_version": version}
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("MODEL_SERVER_PORT", "8001"))
    uvicorn.run(app, host="0.0.0.0", port=port)

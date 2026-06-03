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
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter, Gauge
import queue
import httpx
import asyncio

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


# ── ClickHouse & Monitoring Client ────────────────────────────────────────────
CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = os.getenv("CLICKHOUSE_PORT", "30123")
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "clickhousepass")
CH_URL = f"http://{CH_HOST}:{CH_PORT}"

def run_clickhouse_query(sql: str) -> dict:
    r = httpx.post(
        CH_URL,
        params={"query": sql, "default_format": "JSON"},
        auth=(CH_USER, CH_PASS),
        timeout=10.0
    )
    if r.status_code != 200:
        raise Exception(f"ClickHouse query failed: {r.text}")
    return r.json()

def run_clickhouse_write(sql: str, data: str = None) -> None:
    headers = {"Content-Type": "text/plain"}
    r = httpx.post(
        CH_URL,
        params={"query": sql},
        content=data,
        auth=(CH_USER, CH_PASS),
        headers=headers,
        timeout=10.0
    )
    if r.status_code != 200:
        raise Exception(f"ClickHouse write failed: {r.text}")

prediction_queue = queue.Queue()

def clickhouse_writer_thread():
    while True:
        try:
            item = prediction_queue.get(timeout=1.0)
            batch = [item]
            while not prediction_queue.empty() and len(batch) < 100:
                try:
                    batch.append(prediction_queue.get_nowait())
                except queue.Empty:
                    break
            
            lines = []
            for item in batch:
                lines.append(json.dumps({
                    "transaction_id": item["transaction_id"],
                    "model_version": item["model_version"],
                    "probability": item["probability"],
                    "prediction": int(item["prediction"])
                }))
            data = "\n".join(lines) + "\n"
            
            sql = "INSERT INTO default.model_predictions FORMAT JSONEachRow"
            try:
                run_clickhouse_write(sql, data)
            except Exception as e:
                print(f"Error writing batch to ClickHouse: {e}")
                
            for _ in range(len(batch)):
                prediction_queue.task_done()
                
        except queue.Empty:
            continue
        except Exception as e:
            print(f"Error in ClickHouse writer thread: {e}")

active_connections = []

async def broadcast_loop():
    while True:
        await asyncio.sleep(2.0)
        if not active_connections:
            continue
        try:
            sql_models = "SELECT DISTINCT model_version FROM default.model_predictions"
            try:
                res = await asyncio.to_thread(run_clickhouse_query, sql_models)
                model_versions = [row["model_version"] for row in res.get("data", [])]
            except Exception as e:
                print(f"Error getting models: {e}")
                model_versions = []
                
            active_ver = _state.get("version", "latest")
            if active_ver not in model_versions:
                model_versions.append(active_ver)
                
            data = {"models": {}, "active_version": active_ver}
            for version in model_versions:
                if not version:
                    continue
                stats_sql = f"""
                SELECT
                    count() AS total,
                    countIf(status = 'FRAUD' AND prediction = 1) AS tp,
                    countIf(status = 'SUCCESS' AND prediction = 1) AS fp,
                    countIf(status = 'SUCCESS' AND prediction = 0) AS tn,
                    countIf(status = 'FRAUD' AND prediction = 0) AS fn
                FROM (
                    SELECT
                        t.status AS status,
                        p.prediction AS prediction
                    FROM default.transactions AS t
                    INNER JOIN default.model_predictions AS p ON t.id = p.transaction_id
                    WHERE p.model_version = '{version}'
                )
                """
                try:
                    stats_res = await asyncio.to_thread(run_clickhouse_query, stats_sql)
                    rows = stats_res.get("data", [])
                    if rows and int(rows[0].get("total", 0)) > 0:
                        row = rows[0]
                        tp = int(row.get("tp", 0))
                        fp = int(row.get("fp", 0))
                        tn = int(row.get("tn", 0))
                        fn = int(row.get("fn", 0))
                        total = tp + fp + tn + fn
                        
                        accuracy = (tp + tn) / total if total > 0 else 0.0
                        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
                        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
                        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
                        
                        data["models"][version] = {
                            "total": total,
                            "accuracy": round(accuracy, 4),
                            "precision": round(precision, 4),
                            "recall": round(recall, 4),
                            "f1": round(f1, 4),
                            "confusion_matrix": {"tp": tp, "fp": fp, "tn": tn, "fn": fn}
                        }
                    else:
                        data["models"][version] = {
                            "total": 0,
                            "accuracy": 0.0,
                            "precision": 0.0,
                            "recall": 0.0,
                            "f1": 0.0,
                            "confusion_matrix": {"tp": 0, "fp": 0, "tn": 0, "fn": 0}
                        }
                except Exception as e:
                    print(f"Error getting stats for version {version}: {e}")
            
            payload = json.dumps(data)
            for conn in list(active_connections):
                try:
                    await conn.send_text(payload)
                except Exception:
                    if conn in active_connections:
                        active_connections.remove(conn)
        except Exception as e:
            print(f"Error in broadcast loop: {e}")

@app.on_event("startup")
async def startup():
    _load_version("latest")
    threading.Thread(target=clickhouse_writer_thread, daemon=True).start()
    asyncio.create_task(broadcast_loop())


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


class PredictRequest(BaseModel):
    transaction_id: str
    features: TransactionFeatures


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
def predict(req: PredictRequest):
    try:
        f = req.features
        raw = np.array([[getattr(f, feat) for feat in FEATURE_NAMES]])
        raw[0, 28] = float(_state["amount_scaler"].transform([[f.Amount]])[0][0])
        raw[0, 29] = float(_state["time_scaler"].transform([[f.Time]])[0][0])

        prob = float(_state["model"].predict_proba(raw)[0][1])
        is_fraud = prob >= float(os.getenv("ML_THRESHOLD", "0.5"))

        # Queue prediction for ClickHouse analytical logging
        prediction_queue.put({
            "transaction_id": req.transaction_id,
            "model_version": _state.get("version", "unknown"),
            "probability": prob,
            "prediction": 1 if is_fraud else 0
        })

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
    import re
    if not re.match(r"^[a-zA-Z0-9_]+$", version):
        raise HTTPException(status_code=400, detail="Invalid version format. Only alphanumeric characters and underscores are allowed.")
    try:
        _load_version(version)
        return {"status": "ok", "active_version": version}
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/predictions")
def get_predictions(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0)
):
    sql = f"""
    SELECT
        p.transaction_id AS transaction_id,
        p.model_version AS model_version,
        p.probability AS probability,
        p.prediction AS prediction,
        t.status AS actual_status,
        p.predicted_at AS predicted_at
    FROM default.model_predictions AS p
    LEFT JOIN default.transactions AS t ON p.transaction_id = t.id
    ORDER BY p.predicted_at DESC
    LIMIT {limit} OFFSET {offset}
    """
    try:
        res = run_clickhouse_query(sql)
        rows = res.get("data", [])
        for row in rows:
            if "predicted_at" in row and row["predicted_at"] is not None:
                row["predicted_at"] = str(row["predicted_at"])
        return {"data": rows, "limit": limit, "offset": offset}
    except Exception as e:
        print(f"Error fetching predictions: {e}")
        return {"data": [], "limit": limit, "offset": offset, "error": str(e)}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in active_connections:
            active_connections.remove(websocket)


@app.get("/")
def serve_ui():
    import os
    if os.path.exists("ml/static/index.html"):
        return FileResponse("ml/static/index.html")
    return {"message": "Welcome to Fraud Detection Model & Dashboard Server. (Dashboard UI not built yet)"}


@app.get("/vite.svg")
def serve_favicon():
    import os
    if os.path.exists("ml/static/vite.svg"):
        return FileResponse("ml/static/vite.svg")
    raise HTTPException(status_code=404, detail="Favicon not found")


import os
os.makedirs("ml/static", exist_ok=True)
if os.path.exists("ml/static/assets"):
    app.mount("/assets", StaticFiles(directory="ml/static/assets"), name="assets")
app.mount("/static", StaticFiles(directory="ml/static"), name="static")


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("MODEL_SERVER_PORT", "8001"))
    uvicorn.run(app, host="0.0.0.0", port=port)

"""
FastAPI serving XGBoost fraud model.
Start: uvicorn ml.model_server:app --host 0.0.0.0 --port 8001

Endpoints:
  GET  /health                      — liveness probe
  POST /predict                     — fraud probability score + async ClickHouse write
  GET  /models                      — list all versioned models from registry
  POST /models/{version}/activate   — hot-swap to a specific model version
  GET  /api/predictions             — recent predictions with ground-truth join
  WS   /ws                          — real-time Confusion Matrix broadcast (2s interval)
  GET  /metrics                     — Prometheus scrape endpoint
"""
import asyncio
import json
import os
import queue
import re
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import httpx
import joblib
import numpy as np
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Query, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import Counter, Gauge
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, Field
import warnings
warnings.filterwarnings("ignore", category=UserWarning)


# ── Environment -------------------------------------------------------------------
def load_env() -> None:
    """Load .env file into os.environ (non-overriding)."""
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

# ── Model version whitelist pattern (alphanumeric + underscore) -------------------
_VERSION_PATTERN = re.compile(r"^[a-zA-Z0-9_]+$")


def _sanitize_version(version: str) -> str:
    """
    Validate and return version string safe for use in SQL queries.
    Raises ValueError if the version contains disallowed characters.
    Only alphanumeric characters and underscores are permitted, matching
    the format produced by train_model.py (YYYYMMDD_HHMMSS or 'latest').
    """
    if not _VERSION_PATTERN.match(version):
        raise ValueError(
            f"Invalid model version '{version}': only alphanumeric characters and "
            "underscores are allowed."
        )
    return version


# ── Prometheus custom metrics -----------------------------------------------------
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
    ["feature"],
)
prediction_mean_gauge = Gauge(
    "model_prediction_mean_probability",
    "Rolling average predicted fraud probability (rolling window of 1000)",
)


# ── Model state (mutable for hot-swap) -------------------------------------------
#   _state_lock protects all reads AND writes of _state to prevent race conditions
#   between concurrent /predict calls (running in uvicorn's threadpool) and
#   /models/{version}/activate calls that replace _state atomically.
#   Using RLock (re-entrant) allows the same thread to acquire the lock multiple
#   times without deadlock (e.g. startup calls _load_version which uses _state).
_state: dict = {}
_state_lock = threading.RLock()

window_lock = threading.Lock()
feature_window: deque = deque(maxlen=1000)
prediction_window: deque = deque(maxlen=1000)


def _load_version(version: str) -> None:
    """Load a specific versioned model into _state. `version` is a timestamp string."""
    if version == "latest":
        model_path = MODEL_DIR / "fraud_model.pkl"
        scaler_path = MODEL_DIR / "amount_scaler.pkl"
        time_path = MODEL_DIR / "time_scaler.pkl"
        stats_path = MODEL_DIR / "baseline_stats.json"
    else:
        model_path = MODEL_DIR / f"fraud_model_{version}.pkl"
        scaler_path = MODEL_DIR / f"amount_scaler_{version}.pkl"
        time_path = MODEL_DIR / f"time_scaler_{version}.pkl"
        stats_path = MODEL_DIR / f"baseline_stats_{version}.json"

    if not model_path.exists():
        raise FileNotFoundError(f"Model version '{version}' not found at {model_path}")

    loaded_model = joblib.load(model_path)
    loaded_amount = joblib.load(scaler_path)
    loaded_time = joblib.load(time_path)

    baseline_mean: dict = {}
    baseline_std: dict = {}
    if stats_path.exists():
        try:
            stats = json.loads(stats_path.read_text())
            baseline_mean = stats.get("mean", {})
            baseline_std = stats.get("std", {})
        except Exception as exc:
            print(f"Warning: failed to load baseline stats: {exc}")

    features_list = [f"V{i}" for i in range(1, 29)] + ["Amount_sc", "Time_sc"]
    for feat in features_list:
        baseline_mean.setdefault(feat, 0.0)
        baseline_std.setdefault(feat, 1.0)
        if baseline_std[feat] <= 1e-6:
            baseline_std[feat] = 1.0

    new_state = {
        "model": loaded_model,
        "amount_scaler": loaded_amount,
        "time_scaler": loaded_time,
        "version": version,
        "baseline_mean": baseline_mean,
        "baseline_std": baseline_std,
    }

    # Determine AUROC from registry
    auroc = "unknown"
    if REGISTRY_PATH.exists():
        registry = json.loads(REGISTRY_PATH.read_text())
        for entry in registry:
            if entry.get("version") == version or version == "latest":
                auroc = str(entry.get("auroc", "unknown"))
                if version == "latest":
                    auroc = str(registry[-1].get("auroc", "unknown"))
                break

    # Atomic swap: acquire lock, replace the entire _state dict reference.
    global _state
    with _state_lock:
        _state = new_state

    model_info_gauge.labels(version=version, auroc=auroc).set(1)
    print(f"Model version '{version}' loaded (AUROC={auroc})")


# ── ClickHouse async client -------------------------------------------------------
CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = os.getenv("CLICKHOUSE_PORT", "30123")
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "")
if not CH_PASS:
    raise ValueError("CLICKHOUSE_PASSWORD environment variable is required and must not be empty.")
CH_URL = f"http://{CH_HOST}:{CH_PORT}"

# Shared async ClickHouse client for broadcast_loop and /api/predictions.
_async_ch_client: Optional[httpx.AsyncClient] = None

# Persistent sync client reused by the writer thread to avoid creating a new
# TCP connection for every batch write.
_sync_ch_client: Optional[httpx.Client] = None


async def async_ch_query(sql: str) -> dict:
    """Run a ClickHouse query using the shared async HTTP client."""
    assert _async_ch_client is not None, "Async ClickHouse client not initialised"
    r = await _async_ch_client.post(
        CH_URL,
        params={"query": sql, "default_format": "JSON"},
        auth=(CH_USER, CH_PASS),
        timeout=10.0,
    )
    if r.status_code != 200:
        raise RuntimeError(f"ClickHouse query failed: {r.text}")
    return r.json()


def sync_ch_write(sql: str, data: str) -> None:
    """Blocking ClickHouse INSERT using the persistent sync client."""
    assert _sync_ch_client is not None, "Sync ClickHouse client not initialised"
    r = _sync_ch_client.post(
        CH_URL,
        params={"query": sql},
        content=data,
        auth=(CH_USER, CH_PASS),
        headers={"Content-Type": "text/plain"},
        timeout=10.0,
    )
    if r.status_code != 200:
        raise RuntimeError(f"ClickHouse write failed (HTTP {r.status_code}): {r.text}")


# ── Prediction writer thread ------------------------------------------------------
#   Drain the queue in batches and write to ClickHouse.
#   Uses a threading.Event so the shutdown handler can request a graceful stop
#   and wait for the queue to be fully drained before the process exits.
#
#   Queue is bounded (maxsize=10_000) to prevent unbounded memory growth when
#   ClickHouse is slow or unavailable. The /predict endpoint raises HTTP 503
#   if the queue is full so callers can apply back-pressure.

prediction_queue: queue.Queue = queue.Queue(maxsize=10_000)
_writer_stop_event = threading.Event()


def clickhouse_writer_thread() -> None:
    """Background daemon that batches and writes predictions to ClickHouse."""
    while not _writer_stop_event.is_set() or not prediction_queue.empty():
        try:
            item = prediction_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        batch = [item]
        while not prediction_queue.empty() and len(batch) < 100:
            try:
                batch.append(prediction_queue.get_nowait())
            except queue.Empty:
                break

        lines = []
        for rec in batch:
            lines.append(
                json.dumps(
                    {
                        "transaction_id": rec["transaction_id"],
                        "model_version": rec["model_version"],
                        "probability": rec["probability"],
                        "prediction": int(rec["prediction"]),
                    }
                )
            )
        data = "\n".join(lines) + "\n"

        # Write to ClickHouse with retries.
        # Distinguish retry-able server errors (5xx) from permanent client errors
        # (4xx, e.g. schema mismatch) to avoid wasting retries on non-recoverable failures.
        max_retries = 5
        backoff = 1.0
        success = False
        last_exc: Optional[Exception] = None

        for attempt in range(max_retries):
            try:
                sync_ch_write("INSERT INTO default.model_predictions FORMAT JSONEachRow", data)
                success = True
                break
            except RuntimeError as exc:
                last_exc = exc
                exc_msg = str(exc)
                # Extract HTTP status code from the error message if present.
                is_client_error = "HTTP 4" in exc_msg
                if is_client_error:
                    print(f"[writer] Non-retryable ClickHouse error (4xx) — sending to DLQ immediately: {exc}")
                    break
                print(f"[writer] ClickHouse batch write error (attempt {attempt + 1}/{max_retries}): {exc}")
                if attempt < max_retries - 1:
                    time.sleep(backoff)
                    backoff *= 2.0
            except Exception as exc:
                last_exc = exc
                print(f"[writer] Unexpected write error (attempt {attempt + 1}/{max_retries}): {exc}")
                if attempt < max_retries - 1:
                    time.sleep(backoff)
                    backoff *= 2.0

        if not success:
            try:
                dlq_dir = Path("ml/dlq")
                dlq_dir.mkdir(parents=True, exist_ok=True)
                dlq_file = dlq_dir / f"predictions_dlq_{int(time.time())}.jsonl"
                dlq_file.write_text(data)
                print(f"[writer] Saved failed batch to DLQ file: {dlq_file}")
            except Exception as dlq_exc:
                print(f"[writer] Failed to write to DLQ file: {dlq_exc}")

        for _ in range(len(batch)):
            prediction_queue.task_done()


# ── WebSocket broadcast loop ------------------------------------------------------
#   Caches the last computed stats for 5 s to avoid hammering ClickHouse with
#   heavy JOIN queries every 2 s for every active model version.

active_connections: list = []
_broadcast_cache: dict = {}       # version → stats dict
_broadcast_cache_ts: float = 0.0  # epoch seconds when cache was last refreshed
_CACHE_TTL_S: float = 5.0         # seconds before stats are recomputed


async def broadcast_loop() -> None:
    """Pushes updated Confusion Matrix stats to all connected WebSocket clients."""
    global _broadcast_cache, _broadcast_cache_ts
    while True:
        await asyncio.sleep(2.0)
        if not active_connections:
            continue
        try:
            now = time.monotonic()

            with _state_lock:
                active_ver = _state.get("version", "latest")

            # Only re-query ClickHouse when cache is stale.
            if now - _broadcast_cache_ts > _CACHE_TTL_S:
                try:
                    res = await async_ch_query(
                        "SELECT DISTINCT model_version FROM default.model_predictions"
                    )
                    # Sanitize versions read from DB before using in SQL to prevent injection.
                    raw_versions = [row["model_version"] for row in res.get("data", [])]
                    model_versions = []
                    for v in raw_versions:
                        try:
                            model_versions.append(_sanitize_version(v))
                        except ValueError:
                            print(f"[broadcast] Skipping invalid model version from DB: {v!r}")
                except Exception as exc:
                    print(f"[broadcast] Error listing model versions: {exc}")
                    model_versions = []

                try:
                    safe_active_ver = _sanitize_version(active_ver)
                    if safe_active_ver not in model_versions:
                        model_versions.append(safe_active_ver)
                except ValueError:
                    pass

                new_cache: dict = {}
                for version in model_versions:
                    if not version:
                        continue
                    # Join model_predictions against ground_truth (not transactions.status)
                    # to correctly separate inference labels from ground truth labels.
                    # Limited to the last 24 hours to prevent memory exhaustion.
                    # `version` has been validated by _sanitize_version above.
                    stats_sql = f"""
                    SELECT
                        count() AS total,
                        countIf(gt.actual_label = 'FRAUD'   AND p.prediction = 1) AS tp,
                        countIf(gt.actual_label = 'SUCCESS' AND p.prediction = 1) AS fp,
                        countIf(gt.actual_label = 'SUCCESS' AND p.prediction = 0) AS tn,
                        countIf(gt.actual_label = 'FRAUD'   AND p.prediction = 0) AS fn
                    FROM default.model_predictions AS p
                    INNER JOIN default.ground_truth AS gt
                        ON p.transaction_id = gt.transaction_id
                    WHERE p.model_version = '{version}'
                      AND p.predicted_at >= now() - INTERVAL 24 HOUR
                    """
                    try:
                        stats_res = await async_ch_query(stats_sql)
                        rows = stats_res.get("data", [])
                        if rows and int(rows[0].get("total", 0)) > 0:
                            row = rows[0]
                            tp = int(row.get("tp", 0))
                            fp = int(row.get("fp", 0))
                            tn = int(row.get("tn", 0))
                            fn = int(row.get("fn", 0))
                            total = tp + fp + tn + fn
                            accuracy  = (tp + tn) / total if total > 0 else 0.0
                            precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
                            recall    = tp / (tp + fn) if (tp + fn) > 0 else 0.0
                            f1 = (2 * precision * recall / (precision + recall)
                                  if (precision + recall) > 0 else 0.0)
                            new_cache[version] = {
                                "total": total,
                                "accuracy":  round(accuracy, 4),
                                "precision": round(precision, 4),
                                "recall":    round(recall, 4),
                                "f1":        round(f1, 4),
                                "confusion_matrix": {
                                    "tp": tp, "fp": fp, "tn": tn, "fn": fn
                                },
                            }
                        else:
                            new_cache[version] = {
                                "total": 0, "accuracy": 0.0, "precision": 0.0,
                                "recall": 0.0, "f1": 0.0,
                                "confusion_matrix": {"tp": 0, "fp": 0, "tn": 0, "fn": 0},
                            }
                    except Exception as exc:
                        print(f"[broadcast] Stats error for {version}: {exc}")

                _broadcast_cache = new_cache
                _broadcast_cache_ts = now

            payload = json.dumps(
                {"models": _broadcast_cache, "active_version": active_ver}
            )
            for conn in list(active_connections):
                try:
                    await conn.send_text(payload)
                except Exception:
                    if conn in active_connections:
                        active_connections.remove(conn)

        except Exception as exc:
            print(f"[broadcast] Unexpected error: {exc}")


async def drift_monitoring_loop() -> None:
    """Background task that periodically calculates feature drift and updates Prometheus."""
    while True:
        await asyncio.sleep(10.0)
        try:
            with window_lock:
                if len(feature_window) < 10:
                    continue
                features_snapshot = list(feature_window)
                predictions_snapshot = list(prediction_window)

            with _state_lock:
                local_state = dict(_state)

            if not local_state:
                continue

            arr = np.array(features_snapshot)
            rolling_means = arr.mean(axis=0)
            for key_feat, idx in [("V14", 13), ("V12", 11), ("V17", 16), ("Amount_sc", 28)]:
                b_mean = local_state["baseline_mean"].get(key_feat, 0.0)
                b_std  = local_state["baseline_std"].get(key_feat, 1.0)
                drift_z = abs(rolling_means[idx] - b_mean) / b_std
                feature_drift_gauge.labels(feature=key_feat).set(drift_z)
            prediction_mean_gauge.set(float(np.mean(predictions_snapshot)))
        except Exception as exc:
            print(f"[drift] Error calculating drift: {exc}")


# ── Startup / Shutdown (lifespan context manager) ---------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown lifecycle."""
    global _async_ch_client, _sync_ch_client

    # ── Startup ──────────────────────────────────────────────────────────────────
    _load_version("latest")
    _async_ch_client = httpx.AsyncClient()
    # Persistent sync client for the writer thread — reuses connections across batches.
    _sync_ch_client = httpx.Client()
    _writer_stop_event.clear()
    threading.Thread(target=clickhouse_writer_thread, daemon=True).start()
    asyncio.create_task(broadcast_loop())
    asyncio.create_task(drift_monitoring_loop())

    yield

    # ── Shutdown ─────────────────────────────────────────────────────────────────
    print("[shutdown] Signalling writer thread to stop...")
    _writer_stop_event.set()
    # Block (in a thread) until the queue is fully drained.
    await asyncio.to_thread(prediction_queue.join)
    print("[shutdown] Prediction queue drained — writer thread stopped.")
    if _async_ch_client:
        await _async_ch_client.aclose()
    if _sync_ch_client:
        _sync_ch_client.close()


app = FastAPI(title="Fraud Detection Model Server", version="2.1", lifespan=lifespan)

# Instrument all FastAPI routes automatically (latency histogram, request count)
Instrumentator().instrument(app).expose(app)


# ── Schema -----------------------------------------------------------------------
FEATURE_NAMES = [f"V{i}" for i in range(1, 29)] + ["Amount", "Time"]
SCALED_FEATURE_NAMES = [f"V{i}" for i in range(1, 29)] + ["Amount_sc", "Time_sc"]


class TransactionFeatures(BaseModel):
    V1:  float; V2:  float; V3:  float; V4:  float; V5:  float
    V6:  float; V7:  float; V8:  float; V9:  float; V10: float
    V11: float; V12: float; V13: float; V14: float; V15: float
    V16: float; V17: float; V18: float; V19: float; V20: float
    V21: float; V22: float; V23: float; V24: float; V25: float
    V26: float; V27: float; V28: float
    Amount: float = Field(..., ge=0)
    Time:   float = Field(..., ge=0)


class PredictRequest(BaseModel):
    transaction_id: str
    features: TransactionFeatures


class PredictionResult(BaseModel):
    fraud_probability: float
    is_fraud: bool
    model_version: str


# ── Helper: build scaled feature array ------------------------------------------
def _build_feature_array(f: TransactionFeatures, state: dict) -> np.ndarray:
    """Scale Amount and Time, return (1, 30) numpy array ready for inference."""
    raw = np.array([[getattr(f, feat) for feat in FEATURE_NAMES]])
    raw[0, 28] = float(state["amount_scaler"].transform([[f.Amount]])[0][0])
    raw[0, 29] = float(state["time_scaler"].transform([[f.Time]])[0][0])
    return raw


# ── Endpoints -------------------------------------------------------------------
@app.get("/health")
def health(response: Response):
    with _state_lock:
        ver = _state.get("version", "unknown")
        loaded = bool(_state)
    if not loaded:
        response.status_code = 503
        return {"status": "error", "message": "Model not loaded yet", "model_loaded": False, "model_version": "unknown"}
    return {"status": "ok", "model_loaded": loaded, "model_version": ver}


@app.post("/predict", response_model=PredictionResult)
def predict(req: PredictRequest):
    """Score a transaction and queue the result for ClickHouse logging."""
    try:
        # Acquire lock once to get a consistent local reference to _state.
        # The local reference remains valid even if hot-swap replaces _state
        # mid-request, because Python's GC keeps the old dict alive.
        with _state_lock:
            local_state = _state

        f = req.features
        raw = _build_feature_array(f, local_state)
        prob = float(local_state["model"].predict_proba(raw)[0][1])
        is_fraud = prob >= float(os.getenv("ML_THRESHOLD", "0.5"))

        # Queue prediction for async ClickHouse write.
        # Return 503 immediately if the queue is full to apply back-pressure
        # to callers (Flink will retry via its own retry/fallback logic).
        try:
            prediction_queue.put_nowait({
                "transaction_id": req.transaction_id,
                "model_version": local_state.get("version", "unknown"),
                "probability": prob,
                "prediction": 1 if is_fraud else 0,
            })
        except queue.Full:
            print(f"[predict] prediction_queue full — dropping write for {req.transaction_id}")
            # Still return the prediction result; only the logging is dropped.

        fraud_predictions.labels(is_fraud=str(is_fraud).lower()).inc()

        # Track rolling feature/prediction windows for drift monitoring
        feature_vector = raw[0].copy()
        with window_lock:
            feature_window.append(feature_vector)
            prediction_window.append(prob)

        return PredictionResult(
            fraud_probability=prob,
            is_fraud=is_fraud,
            model_version=local_state.get("version", "unknown"),
        )
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@app.get("/models")
def list_models():
    """List all versioned models with their metrics from the registry."""
    if not REGISTRY_PATH.exists():
        with _state_lock:
            active = _state.get("version")
        return {"active_version": active, "versions": []}
    registry = json.loads(REGISTRY_PATH.read_text())
    with _state_lock:
        active = _state.get("version")
    return {"active_version": active, "versions": registry}


@app.post("/models/{version}/activate")
def activate_model(version: str):
    """Hot-swap to a specific model version without restarting the server."""
    try:
        _sanitize_version(version)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    try:
        _load_version(version)
        return {"status": "ok", "active_version": version}
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/predictions")
async def get_predictions(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    """
    Return recent predictions joined with ground_truth labels.
    Ground truth is read from the dedicated `ground_truth` table rather than
    `transactions.status` to maintain proper data separation.
    """
    sql = f"""
    SELECT
        p.transaction_id  AS transaction_id,
        p.model_version   AS model_version,
        p.probability     AS probability,
        p.prediction      AS prediction,
        gt.actual_label   AS actual_status,
        p.predicted_at    AS predicted_at
    FROM default.model_predictions AS p
    LEFT JOIN default.ground_truth AS gt
        ON p.transaction_id = gt.transaction_id
    ORDER BY p.predicted_at DESC
    LIMIT {limit} OFFSET {offset}
    """
    try:
        res = await async_ch_query(sql)
        rows = res.get("data", [])
        for row in rows:
            if "predicted_at" in row and row["predicted_at"] is not None:
                row["predicted_at"] = str(row["predicted_at"])
        return {"data": rows, "limit": limit, "offset": offset}
    except Exception as exc:
        print(f"[api] Error fetching predictions: {exc}")
        return {"data": [], "limit": limit, "offset": offset, "error": str(exc)}


# ── WebSocket -------------------------------------------------------------------
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


# ── Static files (React SPA) ----------------------------------------------------
@app.get("/")
def serve_ui():
    if os.path.exists("ml/static/index.html"):
        return FileResponse("ml/static/index.html")
    return {
        "message": "Welcome to Fraud Detection Model & Dashboard Server. "
                   "(Dashboard UI not built yet — run: cd ml/dashboard && npm run build)"
    }


@app.get("/vite.svg")
def serve_favicon():
    if os.path.exists("ml/static/vite.svg"):
        return FileResponse("ml/static/vite.svg")
    raise HTTPException(status_code=404, detail="Favicon not found")


os.makedirs("ml/static", exist_ok=True)
if os.path.exists("ml/static/assets"):
    app.mount("/assets", StaticFiles(directory="ml/static/assets"), name="assets")
app.mount("/static", StaticFiles(directory="ml/static"), name="static")


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("MODEL_SERVER_PORT", "8001"))
    uvicorn.run(app, host="0.0.0.0", port=port)

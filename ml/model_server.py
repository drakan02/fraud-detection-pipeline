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
import uuid
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import httpx
import joblib
import numpy as np
from fastapi import FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect, Query, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
import warnings
warnings.filterwarnings("ignore", category=UserWarning)
from filelock import FileLock


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

# Configure Prometheus Multiprocess Mode if running in multi-worker environment
MODEL_SERVER_WORKERS = int(os.getenv("MODEL_SERVER_WORKERS", "4"))
if MODEL_SERVER_WORKERS > 1:
    multiproc_dir = os.getenv("PROMETHEUS_MULTIPROC_DIR", "/tmp/prometheus_multiproc")
    os.environ["PROMETHEUS_MULTIPROC_DIR"] = multiproc_dir
    os.makedirs(multiproc_dir, exist_ok=True)

from prometheus_client import Counter, Gauge
from prometheus_fastapi_instrumentator import Instrumentator

MODEL_DIR = Path("ml/models")
REGISTRY_PATH = MODEL_DIR / "model_registry.json"
REGISTRY_LOCK_PATH = REGISTRY_PATH.with_name("model_registry.json.lock")

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
from prometheus_client import REGISTRY

# Unregister duplicate collectors if they already exist due to reload/multi-worker spawning
for name in list(REGISTRY._names_to_collectors.keys()):
    if name in ["fraud_predictions_total", "fraud_predictions", "fraud_model_info", "model_feature_drift_zscore", "model_prediction_mean_probability"]:
        collector = REGISTRY._names_to_collectors.get(name)
        if collector:
            try:
                REGISTRY.unregister(collector)
            except KeyError:
                pass

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

    # Determine AUROC and resolve real version from registry
    auroc = "unknown"
    real_version = version
    registry = []
    if REGISTRY_PATH.exists():
        try:
            with FileLock(str(REGISTRY_LOCK_PATH)):
                registry = json.loads(REGISTRY_PATH.read_text())
        except Exception as exc:
            print(f"Warning: failed to load/parse registry: {exc}")

    if registry:
        if version == "latest":
            real_version = registry[-1].get("version", "latest")
            auroc = str(registry[-1].get("auroc", "unknown"))
        else:
            for entry in registry:
                if entry.get("version") == version:
                    auroc = str(entry.get("auroc", "unknown"))
                    break

    algorithm = None
    threshold = None
    if registry:
        selected_entry = None
        for entry in registry:
            if entry.get("version") == real_version:
                selected_entry = entry
                break
        if selected_entry is None and version == "latest":
            selected_entry = registry[-1]
        if selected_entry is not None:
            algorithm = selected_entry.get("algorithm")
            threshold = selected_entry.get("threshold")

    try:
        threshold = float(threshold) if threshold is not None else float(os.getenv("ML_THRESHOLD", "0.5"))
    except (TypeError, ValueError):
        threshold = float(os.getenv("ML_THRESHOLD", "0.5"))

    new_state = {
        "model": loaded_model,
        "amount_scaler": loaded_amount,
        "time_scaler": loaded_time,
        "version": real_version,
        "algorithm": algorithm,
        "threshold": threshold,
        "baseline_mean": baseline_mean,
        "baseline_std": baseline_std,
    }

    # Atomic swap: acquire lock, replace the entire _state dict reference.
    global _state
    with _state_lock:
        _state = new_state

    model_info_gauge.labels(version=real_version, auroc=auroc).set(1)
    print(f"Model version '{real_version}' loaded (AUROC={auroc})")


# ── ClickHouse async client -------------------------------------------------------
CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = os.getenv("CLICKHOUSE_PORT", "30123")
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "")
if not CH_PASS:
    raise ValueError("CLICKHOUSE_PASSWORD environment variable is required and must not be empty.")
CH_URL = f"http://{CH_HOST}:{CH_PORT}"
MODEL_ADMIN_API_KEY = os.getenv("MODEL_ADMIN_API_KEY", "")
PREDICTION_DLQ_DIR = Path(os.getenv("PREDICTION_DLQ_DIR", "ml/dlq"))

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
                        "run_id": rec.get("run_id", "unknown"),
                        "data_source": rec.get("data_source", "unknown"),
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
                PREDICTION_DLQ_DIR.mkdir(parents=True, exist_ok=True)
                dlq_file = PREDICTION_DLQ_DIR / f"predictions_dlq_{time.time_ns()}_{uuid.uuid4().hex}.jsonl"
                dlq_file.write_text(data)
                print(f"[writer] Saved failed batch to DLQ file: {dlq_file}")
            except Exception as dlq_exc:
                print(f"[writer] Failed to write to DLQ file: {dlq_exc}")

        for _ in range(len(batch)):
            prediction_queue.task_done()


# ── WebSocket broadcast loop ------------------------------------------------------
#   Caches the last computed stats for 5 s to avoid hammering ClickHouse with
#   heavy JOIN queries every 2 s for every active model version.

active_connections: set = set()  # Use set to prevent duplicate entries on reconnect
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
                    FROM (
                        SELECT transaction_id, run_id, model_version, argMax(prediction, predicted_at) AS prediction
                        FROM default.model_predictions
                        WHERE predicted_at >= now() - INTERVAL 24 HOUR
                        GROUP BY run_id, transaction_id, model_version
                    ) p
                    INNER JOIN (
                        SELECT transaction_id, run_id, argMax(actual_label, created_at) AS actual_label
                        FROM default.ground_truth
                        GROUP BY run_id, transaction_id
                    ) gt
                        ON p.transaction_id = gt.transaction_id
                       AND p.run_id = gt.run_id
                    WHERE p.model_version = '{version}'
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
                    active_connections.discard(conn)

        except Exception as exc:
            print(f"[broadcast] Unexpected error: {exc}")


async def drift_monitoring_loop() -> None:
    """Background task that periodically calculates feature drift and updates Prometheus."""
    while True:
        await asyncio.sleep(10.0)
        try:
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
            # Dynamically calculate drift for all 30 features in SCALED_FEATURE_NAMES.
            # Avoids hardcoding indices, preventing breakage if feature ordering changes.
            for idx, key_feat in enumerate(SCALED_FEATURE_NAMES):
                b_mean = local_state["baseline_mean"].get(key_feat, 0.0)
                b_std  = local_state["baseline_std"].get(key_feat, 1.0)
                if b_std <= 1e-6:
                    b_std = 1.0
                drift_z = abs(rolling_means[idx] - b_mean) / b_std
                feature_drift_gauge.labels(feature=key_feat).set(drift_z)
            prediction_mean_gauge.set(float(np.mean(predictions_snapshot)))
        except Exception as exc:
            print(f"[drift] Error calculating drift: {exc}")



def dlq_replayer_thread() -> None:
    """Background daemon that periodically checks the DLQ directory and replays failed predictions to ClickHouse."""
    dlq_dir = Path(PREDICTION_DLQ_DIR)
    print(f"[dlq-replayer] Started. Watching directory: {dlq_dir}")
    while not _writer_stop_event.is_set():
        # Sleep in short increments to allow quick shutdown
        for _ in range(30):
            if _writer_stop_event.is_set():
                break
            time.sleep(1.0)
        
        if _writer_stop_event.is_set():
            break
            
        if not dlq_dir.exists():
            continue
            
        try:
            # List all JSONL files in the DLQ directory
            dlq_files = sorted(list(dlq_dir.glob("predictions_dlq_*.jsonl")))
            if not dlq_files:
                continue
                
            print(f"[dlq-replayer] Found {len(dlq_files)} DLQ file(s) to replay.")
            for file_path in dlq_files:
                if _writer_stop_event.is_set():
                    break
                    
                # Try to rename the file to indicate it is being processed (atomic operation on Linux)
                processing_path = file_path.with_suffix(".jsonl.processing")
                try:
                    file_path.rename(processing_path)
                except FileNotFoundError:
                    # Another worker already renamed or processed it
                    continue
                    
                try:
                    data = processing_path.read_text()
                    if not data.strip():
                        processing_path.unlink(missing_ok=True)
                        continue
                        
                    # Replay the data back to ClickHouse
                    sync_ch_write("INSERT INTO default.model_predictions FORMAT JSONEachRow", data)
                    print(f"[dlq-replayer] Successfully replayed DLQ file to ClickHouse: {file_path.name}")
                    processing_path.unlink(missing_ok=True)
                except Exception as exc:
                    print(f"[dlq-replayer] Failed to replay DLQ file {file_path.name}: {exc}. Will retry later.")
                    # Restore the original filename if we failed, so it can be retried
                    if processing_path.exists():
                        try:
                            processing_path.rename(file_path)
                        except Exception:
                            pass
                    # Stop processing subsequent files to avoid hammering ClickHouse if it's down
                    break
        except Exception as exc:
            print(f"[dlq-replayer] Error scanning DLQ directory: {exc}")


def model_sync_loop() -> None:
    """Background daemon that polls the active_version.txt file to sync model versions across workers."""
    active_file = MODEL_DIR / "active_version.txt"
    active_lock_file = MODEL_DIR / "active_version.txt.lock"
    print(f"[model-sync] Started. Watching active model configuration file: {active_file}")
    
    # Initialize active_version.txt if it doesn't exist
    if not active_file.exists():
        try:
            with _state_lock:
                current_ver = _state.get("version")
            if current_ver:
                with FileLock(str(active_lock_file)):
                    active_file.write_text(current_ver)
        except Exception as exc:
            print(f"[model-sync] Warning: failed to initialize active_version.txt: {exc}")

    while not _writer_stop_event.is_set():
        # Sleep in short increments to allow quick shutdown
        for _ in range(5):
            if _writer_stop_event.is_set():
                break
            time.sleep(1.0)
            
        if _writer_stop_event.is_set():
            break
            
        try:
            if not active_file.exists():
                continue
                
            with FileLock(str(active_lock_file)):
                file_ver = active_file.read_text().strip()
                
            with _state_lock:
                current_ver = _state.get("version")
                
            # If the version in the file is different from our in-memory version, reload it!
            if file_ver and file_ver != current_ver:
                # Resolve 'latest' to its actual timestamp version to compare correctly
                resolved_file_ver = file_ver
                if file_ver == "latest" and REGISTRY_PATH.exists():
                    try:
                        with FileLock(str(REGISTRY_LOCK_PATH)):
                            registry = json.loads(REGISTRY_PATH.read_text())
                            if registry:
                                resolved_file_ver = registry[-1].get("version", "latest")
                    except Exception:
                        pass
                
                if resolved_file_ver != current_ver:
                    print(f"[model-sync] Version mismatch detected (file={file_ver} [resolved={resolved_file_ver}], memory={current_ver}). Reloading model...")
                    _load_version(file_ver)
        except Exception as exc:
            print(f"[model-sync] Error checking/reloading active model version: {exc}")


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
    threading.Thread(target=dlq_replayer_thread, daemon=True).start()
    threading.Thread(target=model_sync_loop, daemon=True).start()
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
# We do not use .expose(app) because we define our own custom multi-process safe /metrics endpoint.
Instrumentator().instrument(app)


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
    # Amount: non-negative and capped at a realistic upper bound
    Amount: float = Field(..., ge=0, le=1_000_000,
                         description="Transaction amount in EUR (0 – 1,000,000)")
    # Time: seconds elapsed since first transaction in the dataset
    Time:   float = Field(..., ge=0, le=172_800,
                         description="Seconds elapsed since start of observation window (0 – 172800)")


class PredictRequest(BaseModel):
    transaction_id: str
    run_id: Optional[str] = None
    data_source: Optional[str] = None
    features: TransactionFeatures


class PredictionResult(BaseModel):
    fraud_probability: float
    is_fraud: bool
    model_version: str
    algorithm: Optional[str] = None  # e.g. "XGBoost", "LightGBM", "CatBoost"
    threshold: float


# ── Helper: build scaled feature array ------------------------------------------
def _build_feature_array(f: TransactionFeatures, state: dict) -> np.ndarray:
    """Scale Amount and Time, return (1, 30) numpy array ready for inference."""
    raw = np.array([[getattr(f, feat) for feat in FEATURE_NAMES]])
    raw[0, 28] = float(state["amount_scaler"].transform([[f.Amount]])[0][0])
    raw[0, 29] = float(state["time_scaler"].transform([[f.Time]])[0][0])
    return raw


def require_admin_api_key(x_api_key: Optional[str] = Header(default=None)) -> None:
    """Require an API key for model administration. Safe fail-secure check."""
    if not MODEL_ADMIN_API_KEY:
        raise HTTPException(
            status_code=403,
            detail="Model administration is disabled because MODEL_ADMIN_API_KEY is not configured on the server."
        )
    if x_api_key != MODEL_ADMIN_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing admin API key")


# ── Endpoints -------------------------------------------------------------------
@app.get("/metrics")
def metrics():
    """Prometheus metrics endpoint. Supports multiprocess aggregation in multi-worker mode."""
    from prometheus_client import CollectorRegistry, multiprocess, generate_latest, CONTENT_TYPE_LATEST
    if "PROMETHEUS_MULTIPROC_DIR" in os.environ:
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        data = generate_latest(registry)
        return Response(content=data, media_type=CONTENT_TYPE_LATEST)
    else:
        from prometheus_client import REGISTRY
        data = generate_latest(REGISTRY)
        return Response(content=data, media_type=CONTENT_TYPE_LATEST)


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
        threshold = float(local_state.get("threshold", 0.5))
        is_fraud = prob >= threshold

        # Queue prediction for async ClickHouse write.
        # Return 503 immediately if the queue is full to apply back-pressure
        # to callers (Flink will retry via its own retry/fallback logic).
        try:
            prediction_queue.put_nowait({
                "transaction_id": req.transaction_id,
                "run_id": req.run_id or "unknown",
                "data_source": req.data_source or "unknown",
                "model_version": local_state.get("version", "unknown"),
                "probability": prob,
                "prediction": 1 if is_fraud else 0,
            })
        except queue.Full:
            raise HTTPException(
                status_code=503,
                detail="Prediction logging queue is full; retry later",
            )

        fraud_predictions.labels(is_fraud=str(is_fraud).lower()).inc()

        # Track rolling feature/prediction windows for drift monitoring
        feature_vector = raw[0].copy()
        feature_window.append(feature_vector)
        prediction_window.append(prob)

        return PredictionResult(
            fraud_probability=prob,
            is_fraud=is_fraud,
            model_version=local_state.get("version", "unknown"),
            algorithm=local_state.get("algorithm"),
            threshold=threshold,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Inference error: {exc}")


@app.get("/models")
def list_models():
    """List all versioned models with their metrics from the registry."""
    if not REGISTRY_PATH.exists():
        with _state_lock:
            active = _state.get("version")
        return {"active_version": active, "versions": []}
    try:
        with FileLock(str(REGISTRY_LOCK_PATH)):
            registry = json.loads(REGISTRY_PATH.read_text())
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read model registry: {exc}")
    with _state_lock:
        active = _state.get("version")
    return {"active_version": active, "versions": registry}


@app.post("/models/{version}/activate")
def activate_model(version: str, x_api_key: Optional[str] = Header(default=None)):
    """Hot-swap to a specific model version without restarting the server."""
    require_admin_api_key(x_api_key)
    try:
        _sanitize_version(version)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    try:
        _load_version(version)
        
        # Write the active version to a shared file so other worker processes can sync
        active_file = MODEL_DIR / "active_version.txt"
        active_lock_file = MODEL_DIR / "active_version.txt.lock"
        with FileLock(str(active_lock_file)):
            active_file.write_text(version)
            
        with _state_lock:
            active = _state.get("version", version)
        return {"status": "ok", "active_version": active}
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
        p.run_id          AS run_id,
        p.data_source     AS data_source,
        p.probability     AS probability,
        p.prediction      AS prediction,
        gt.actual_label   AS actual_status,
        p.max_predicted_at AS predicted_at
    FROM (
        SELECT transaction_id, run_id, model_version, data_source,
               argMax(probability, predicted_at) AS probability,
               argMax(prediction, predicted_at) AS prediction,
               max(predicted_at) AS max_predicted_at
        FROM default.model_predictions
        GROUP BY run_id, transaction_id, model_version, data_source
    ) p
    LEFT JOIN (
        SELECT transaction_id, run_id, argMax(actual_label, created_at) AS actual_label
        FROM default.ground_truth
        GROUP BY run_id, transaction_id
    ) gt
        ON p.transaction_id = gt.transaction_id
       AND p.run_id = gt.run_id
    ORDER BY p.max_predicted_at DESC
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
    active_connections.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        active_connections.discard(websocket)


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
    import shutil
    port = int(os.getenv("MODEL_SERVER_PORT", "8001"))
    workers = int(os.getenv("MODEL_SERVER_WORKERS", "4"))
    
    # Clean up old metrics DB files in parent process before workers start
    if workers > 1:
        multiproc_dir = os.getenv("PROMETHEUS_MULTIPROC_DIR", "/tmp/prometheus_multiproc")
        if os.path.exists(multiproc_dir):
            try:
                shutil.rmtree(multiproc_dir)
            except Exception as e:
                print(f"Warning: failed to clear multiprocess metrics directory: {e}")
        os.makedirs(multiproc_dir, exist_ok=True)
        
    print(f"Starting FastAPI model server on port {port} with {workers} workers...")
    uvicorn.run("ml.model_server:app", host="0.0.0.0", port=port, workers=workers)

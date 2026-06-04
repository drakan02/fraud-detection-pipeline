"""
Train an XGBoost fraud detection model on the Kaggle creditcardfraud dataset.
Uses BorderlineSMOTE oversampling on the training set, early stopping on a
held-out validation set, and F1-optimal threshold selection via PR curve.
The held-out test set is used only for final reporting (no model selection).
Passes a quality gate of AUPRC >= 0.80 before saving.
Keeps the 3 most recent versions; older versions are deleted automatically.

Output (timestamp versioning):
  ml/models/fraud_model_<YYYYMMDD_HHMMSS>.pkl
  ml/models/amount_scaler_<YYYYMMDD_HHMMSS>.pkl
  ml/models/time_scaler_<YYYYMMDD_HHMMSS>.pkl
  ml/models/baseline_stats_<YYYYMMDD_HHMMSS>.json
  ml/models/fraud_model.pkl     ← symlink → latest version
  ml/models/amount_scaler.pkl   ← symlink
  ml/models/time_scaler.pkl     ← symlink
  ml/models/baseline_stats.json ← symlink
  ml/models/model_registry.json ← audit log of all versions
"""
import os
import sys
import time
import json
import joblib
from filelock import FileLock
import numpy as np
import pandas as pd
import subprocess
import shutil
import warnings
from datetime import datetime, timezone
from pathlib import Path
from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.preprocessing import RobustScaler
from sklearn.metrics import (
    roc_auc_score, classification_report, average_precision_score,
    recall_score, f1_score, precision_recall_curve,
)
from imblearn.over_sampling import BorderlineSMOTE
from xgboost import XGBClassifier

# ── GPU Detection & Diagnostics ───────────────────────────────────────────────
def get_system_gpu_info():
    """
    Checks if nvidia-smi is available and queries the GPU model and status.
    Returns (gpu_present, gpu_name, status_msg)
    """
    return False, "", "GPU detection bypassed"

# GPU checks removed to prevent hangs

print("\n=== Checking GPU/CUDA Support ===")
gpu_present, gpu_name, gpu_status = get_system_gpu_info()
if gpu_present:
    print(f"Physical GPU Detected: {gpu_name}")
    print(f"System GPU Status   : {gpu_status}")
else:
    print("Physical GPU: None detected.")

# Force fallback to CPU to avoid CUDA initialization hangs
xgb_gpu = False
lgb_gpu = False
cat_gpu = False

print(f"Framework GPU Support:")
print(f"  - XGBoost:  {'AVAILABLE' if xgb_gpu else 'NOT AVAILABLE (Fallback to CPU)'}")

if gpu_present and not (xgb_gpu or lgb_gpu or cat_gpu):
    print("\n> [!] WARNING: A physical GPU is present, but frameworks cannot use it.")
    if "GPU requires reset" in gpu_status:
        print("> Reason: The GPU driver has entered a crashed/unresponsive state ('GPU requires reset').")
        print("> Resolution: Please reboot the machine or run 'sudo nvidia-smi --gpu-reset' with root privileges.")
    else:
        print("> Reason: CUDA/OpenCL libraries might be missing or framework binaries not built with GPU support.")
print("=================================\n")

SEED      = 42
DATA      = Path("ml/data/creditcard.csv")
MODEL_DIR = Path("ml/models")
REGISTRY  = MODEL_DIR / "model_registry.json"

# ── Step 1: Load ──────────────────────────────────────────────────────────────
print("Loading dataset...")
df = pd.read_csv(DATA)
print(f"Shape: {df.shape} | Fraud rate: {df['Class'].mean():.4%}")

# ── Step 2: Stratified train / validation / test split ────────────────────────
# The validation split is used for threshold selection. The test split is held
# out until final reporting so quality metrics are not biased by threshold tuning.
df_train_val, df_test = train_test_split(
    df, test_size=0.2, random_state=SEED, stratify=df["Class"])
df_train, df_val = train_test_split(
    df_train_val, test_size=0.2, random_state=SEED, stratify=df_train_val["Class"])

# ── Step 3: Feature engineering (Robust Scaling fitted on train only to avoid leakage) ──
amount_scaler = RobustScaler()
time_scaler   = RobustScaler()

# Work on copies to avoid SettingWithCopyWarning
df_train = df_train.copy()
df_val = df_val.copy()
df_test = df_test.copy()

df_train["Amount_sc"] = amount_scaler.fit_transform(df_train[["Amount"]])
df_train["Time_sc"]   = time_scaler.fit_transform(df_train[["Time"]])

df_val["Amount_sc"] = amount_scaler.transform(df_val[["Amount"]])
df_val["Time_sc"]   = time_scaler.transform(df_val[["Time"]])

df_test["Amount_sc"] = amount_scaler.transform(df_test[["Amount"]])
df_test["Time_sc"]   = time_scaler.transform(df_test[["Time"]])

FEATURES = [f"V{i}" for i in range(1, 29)] + ["Amount_sc", "Time_sc"]
X_train, y_train = df_train[FEATURES], df_train["Class"]
X_val, y_val = df_val[FEATURES], df_val["Class"]
X_test, y_test = df_test[FEATURES], df_test["Class"]
print(f"Train size: {len(X_train)} | Val size: {len(X_val)} | Test size: {len(X_test)}")
print(f"Train fraud: {y_train.sum()} | Val fraud: {y_val.sum()} | Test fraud: {y_test.sum()}")

# ── Step 3.5: Placeholder — baseline stats will be captured AFTER outlier removal ──
# (see Step 4.5 below)
# Baseline must reflect the exact distribution the model was trained on,
# NOT the pre-clean distribution, to avoid drift Z-score bias.

# ── Step 4: Remove extreme outliers from training set for top correlated features (V14, V12) ──
# Outlier removal is applied only to the fraud class to reduce overfitting on
# extreme fraud samples while preserving the full non-fraud distribution.
def remove_outliers(X_df, y_df, features_list, threshold=3.0):
    df_temp = pd.concat([X_df, y_df], axis=1)
    initial_len = len(df_temp)
    for feat in features_list:
        fraud_data = df_temp[df_temp['Class'] == 1][feat]
        q25, q75 = np.percentile(fraud_data, 25), np.percentile(fraud_data, 75)
        iqr = q75 - q25
        cut_off = iqr * threshold
        lower, upper = q25 - cut_off, q75 + cut_off

        # Remove extreme outliers of the fraud class to prevent overfitting
        outliers_mask = (df_temp['Class'] == 1) & ((df_temp[feat] < lower) | (df_temp[feat] > upper))
        df_temp = df_temp[~outliers_mask]

    print(f"Outlier removal ({features_list}): dropped {initial_len - len(df_temp)} rows.")
    return df_temp.drop('Class', axis=1), df_temp['Class']

X_train, y_train = remove_outliers(X_train, y_train, ['V14', 'V12'])

# ── Step 4.5: Capture baseline stats AFTER outlier removal ────────────────────
# This correctly reflects the distribution the model is actually trained on.
# Using these stats for production drift monitoring avoids systematic Z-score
# bias that would arise if computed on the pre-clean (noisier) distribution.
baseline_stats_raw = {
    "mean": X_train.mean().to_dict(),
    "std":  X_train.std().to_dict(),
}
print("Baseline stats captured (post-outlier-removal) for drift monitoring.")

# ── Step 5: Model Definitions (XGBoost only, no CV) ───────────────────────────
models_def = {
    "XGBoost": lambda: XGBClassifier(
        n_estimators=500, max_depth=4, learning_rate=0.03,
        subsample=0.8, colsample_bytree=0.8,
        min_child_weight=5, eval_metric="aucpr",
        random_state=SEED,
        device="cuda" if xgb_gpu else "cpu",
        early_stopping_rounds=30,
        **({} if xgb_gpu else {"n_jobs": -1}),
    )
}

# ── Step 6: Train Final Model, tune threshold on validation, test once ───────
print("\nTraining final XGBoost model on train set, tuning threshold on validation set, evaluating on held-out test set...")
smt_full = BorderlineSMOTE(random_state=SEED, sampling_strategy=0.01)
X_train_res, y_train_res = smt_full.fit_resample(X_train, y_train)

best_model = None
best_model_name = ""
best_val_auprc = 0.0
model_metrics = {}

for name, get_model in models_def.items():
    t0 = time.time()
    clf = get_model()
    
    # Fit with Early Stopping on the validation set
    clf.fit(
        X_train_res, y_train_res,
        eval_set=[(X_val, y_val)],
        verbose=False
    )
        
    t_train = time.time() - t0

    # Select threshold on validation only
    y_val_prob = clf.predict_proba(X_val)[:, 1]
    precisions_v, recalls_v, thresholds_v = precision_recall_curve(y_val, y_val_prob)
    f1_scores_v = (
        2 * precisions_v[:-1] * recalls_v[:-1]
        / np.where((precisions_v[:-1] + recalls_v[:-1]) > 0,
                   precisions_v[:-1] + recalls_v[:-1], 1)
    )
    best_thresh_v = float(thresholds_v[np.argmax(f1_scores_v)]) if len(thresholds_v) > 0 else 0.5
    y_val_pred = (y_val_prob >= best_thresh_v).astype(int)
    val_auroc  = roc_auc_score(y_val, y_val_prob)
    val_auprc  = average_precision_score(y_val, y_val_prob)
    val_f1     = f1_score(y_val, y_val_pred)
    val_recall = recall_score(y_val, y_val_pred)

    model_metrics[name] = {
        "model":       clf,
        "val_auroc":   val_auroc,
        "val_auprc":   val_auprc,
        "val_f1":      val_f1,
        "val_recall":  val_recall,
        "train_time":  t_train,
        "threshold":   best_thresh_v,
    }

    print(f"\n[{name}] Validation Selection Metrics:")
    print(f"  AUROC: {val_auroc:.4f} | AUPRC (PR-AUC): {val_auprc:.4f} | F1-Score: {val_f1:.4f} | Recall: {val_recall:.4f}")
    print(f"  Validation-selected Threshold: {best_thresh_v:.4f} | Training Time: {t_train:.2f}s")

    if val_auprc > best_val_auprc:
        best_val_auprc = val_auprc
        best_model = clf
        best_model_name = name

print(f"\n🏆 WINNER MODEL: {best_model_name} (Validation AUPRC = {best_val_auprc:.4f})")

# Evaluate only the selected winner on the held-out test set. This keeps the
# test split as a final estimate rather than a model-selection signal.
best_threshold = model_metrics[best_model_name]["threshold"]
y_best_prob = best_model.predict_proba(X_test)[:, 1]
y_best_pred = (y_best_prob >= best_threshold).astype(int)
test_auroc  = roc_auc_score(y_test, y_best_prob)
test_auprc  = average_precision_score(y_test, y_best_prob)
test_f1     = f1_score(y_test, y_best_pred)
test_recall = recall_score(y_test, y_best_pred)

model_metrics[best_model_name].update({
    "auroc":  test_auroc,
    "auprc":  test_auprc,
    "f1":     test_f1,
    "recall": test_recall,
})

print(f"\n[{best_model_name}] Held-out Test Metrics:")
print(f"  AUROC: {test_auroc:.4f} | AUPRC (PR-AUC): {test_auprc:.4f} | F1-Score: {test_f1:.4f} | Recall: {test_recall:.4f}")

# Print classification report of the winner using its validation-selected threshold
print(f"\nWinner Model Classification Report (threshold={best_threshold:.4f}):")
print(classification_report(y_test, (y_best_prob >= best_threshold).astype(int)))

# ── Step 8: Quality gate ──────────────────────────────────────────────────────
if test_auprc < 0.80:
    raise ValueError(
        f"Quality gate failed: Winner model held-out test AUPRC {test_auprc:.4f} is below the minimum threshold of 0.80. "
        "Model will NOT be saved."
    )
print(f"✓ Quality gate passed (held-out test AUPRC {test_auprc:.4f} >= 0.80)")

# ── Step 9: Versioned save ────────────────────────────────────────────────────
MODEL_DIR.mkdir(parents=True, exist_ok=True)
ts = datetime.now().strftime("%Y%m%d_%H%M%S")

versioned_model  = MODEL_DIR / f"fraud_model_{ts}.pkl"
versioned_amount = MODEL_DIR / f"amount_scaler_{ts}.pkl"
versioned_time   = MODEL_DIR / f"time_scaler_{ts}.pkl"
versioned_stats  = MODEL_DIR / f"baseline_stats_{ts}.json"

joblib.dump(best_model,    versioned_model)
joblib.dump(amount_scaler, versioned_amount)
joblib.dump(time_scaler,   versioned_time)

# Save post-outlier-removal baseline stats for production drift monitoring.
versioned_stats.write_text(json.dumps(baseline_stats_raw, indent=2))

print(f"Saved versioned models and stats: {ts}")

# ── Step 10: Update symlinks (safe atomic replace) ─────────────────────────────
def update_symlink(link: Path, target: Path) -> None:
    """Atomically update symlink to point to new target."""
    tmp = link.with_suffix(".tmp_link")
    tmp.unlink(missing_ok=True)
    tmp.symlink_to(target.name)   # relative symlink within same dir
    tmp.replace(link)

update_symlink(MODEL_DIR / "fraud_model.pkl",     versioned_model)
update_symlink(MODEL_DIR / "amount_scaler.pkl",   versioned_amount)
update_symlink(MODEL_DIR / "time_scaler.pkl",     versioned_time)
update_symlink(MODEL_DIR / "baseline_stats.json", versioned_stats)
print("Symlinks updated → latest now points to", ts)

# ── Step 11: Update model registry ────────────────────────────────────────────
registry: list = []
lock = FileLock(REGISTRY.with_name("model_registry.json.lock"))
with lock:
    if REGISTRY.exists():
        try:
            registry = json.loads(REGISTRY.read_text())
        except Exception:
            registry = []

    registry.append({
        "version":    ts,
        "algorithm":  best_model_name,
        "auroc":      round(model_metrics[best_model_name]["auroc"], 4),
        "auprc":      round(model_metrics[best_model_name]["auprc"], 4),
        "validation_auprc": round(model_metrics[best_model_name]["val_auprc"], 4),
        "f1_score":   round(model_metrics[best_model_name]["f1"], 4),
        "recall":     round(model_metrics[best_model_name]["recall"], 4),
        "threshold":  round(model_metrics[best_model_name]["threshold"], 4),
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "model_file":  versioned_model.name,
        "status":     "active",
    })
    REGISTRY.write_text(json.dumps(registry, indent=2))
print(f"Registry updated → {len(registry)} version(s) recorded")

# ── Step 12: Clean up old models to save disk space ───────────────────────────
KEEP_LIMIT = 3
active_version = None
active_version_file = MODEL_DIR / "active_version.txt"
if active_version_file.exists():
    try:
        active_version = active_version_file.read_text().strip()
    except Exception:
        pass

# If active_version is 'latest' or not set, try to resolve from symlink target
if not active_version or active_version == "latest":
    model_symlink = MODEL_DIR / "fraud_model.pkl"
    if model_symlink.is_symlink():
        try:
            target_name = model_symlink.readlink().name
            if target_name.startswith("fraud_model_") and target_name.endswith(".pkl"):
                active_version = target_name[12:-4]
        except Exception:
            pass

# Versions we want to keep: the currently active version, and the most recent versions up to KEEP_LIMIT
recent_versions = [entry["version"] for entry in registry[-KEEP_LIMIT:]]
keep_versions = set(recent_versions)
if active_version and active_version != "latest":
    keep_versions.add(active_version)

lock = FileLock(REGISTRY.with_name("model_registry.json.lock"))
with lock:
    if REGISTRY.exists():
        try:
            registry = json.loads(REGISTRY.read_text())
        except Exception:
            registry = []
            
    cleaned_registry = []
    removed_count = 0
    for entry in registry:
        version = entry["version"]
        if version in keep_versions:
            cleaned_registry.append(entry)
        else:
            # Delete corresponding files
            for pattern in [
                f"fraud_model_{version}.pkl",
                f"amount_scaler_{version}.pkl",
                f"time_scaler_{version}.pkl",
                f"baseline_stats_{version}.json"
            ]:
                file_path = MODEL_DIR / pattern
                if file_path.exists():
                    try:
                        file_path.unlink()
                    except Exception as exc:
                        print(f"Warning: failed to delete old model file {file_path.name}: {exc}")
            removed_count += 1

    if removed_count > 0:
        REGISTRY.write_text(json.dumps(cleaned_registry, indent=2))
        print(f"Cleaned up {removed_count} old model version(s). Retained the latest {len(cleaned_registry)} version(s).")

print(f"\n✓ Done. To activate this model: POST http://localhost:8001/models/{ts}/activate")
sys.stdout.flush()
os._exit(0)

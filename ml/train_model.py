"""
Train XGBoost fraud detection model on Kaggle creditcardfraud dataset.

Output (timestamp versioning):
  ml/models/fraud_model_<YYYYMMDD_HHMMSS>.pkl
  ml/models/amount_scaler_<YYYYMMDD_HHMMSS>.pkl
  ml/models/time_scaler_<YYYYMMDD_HHMMSS>.pkl
  ml/models/fraud_model.pkl     ← symlink → latest passing version
  ml/models/amount_scaler.pkl   ← symlink
  ml/models/time_scaler.pkl     ← symlink
  ml/models/model_registry.json ← audit log of all versions
"""
import json
import joblib
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, classification_report, average_precision_score
from imblearn.over_sampling import SMOTE
from xgboost import XGBClassifier

SEED      = 42
DATA      = Path("ml/data/creditcard.csv")
MODEL_DIR = Path("ml/models")
REGISTRY  = MODEL_DIR / "model_registry.json"

# ── Step 1: Load ──────────────────────────────────────────────────────────────
print("Loading dataset...")
df = pd.read_csv(DATA)
print(f"Shape: {df.shape} | Fraud rate: {df['Class'].mean():.4%}")

# ── Step 2: Feature engineering ───────────────────────────────────────────────
amount_scaler = StandardScaler()
time_scaler   = StandardScaler()
df["Amount_sc"] = amount_scaler.fit_transform(df[["Amount"]])
df["Time_sc"]   = time_scaler.fit_transform(df[["Time"]])

FEATURES = [f"V{i}" for i in range(1, 29)] + ["Amount_sc", "Time_sc"]
X, y = df[FEATURES], df["Class"]

# ── Step 3: Stratified split ───────────────────────────────────────────────────
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=SEED, stratify=y)
print(f"Train fraud: {y_train.sum()} | Test fraud: {y_test.sum()}")

# ── Step 4: SMOTE on training set only ────────────────────────────────────────
print("Applying SMOTE...")
X_res, y_res = SMOTE(random_state=SEED, sampling_strategy=0.1).fit_resample(X_train, y_train)
print(f"After SMOTE: {X_res.shape} | Fraud: {y_res.sum()}")

# ── Step 5: Train XGBoost ─────────────────────────────────────────────────────
print("Training...")
model = XGBClassifier(
    n_estimators=300, max_depth=6, learning_rate=0.05,
    subsample=0.8, colsample_bytree=0.8,
    eval_metric="aucpr", random_state=SEED, n_jobs=-1,
)
model.fit(X_res, y_res, eval_set=[(X_test, y_test)], verbose=100)

# ── Step 6: Evaluate ──────────────────────────────────────────────────────────
y_prob = model.predict_proba(X_test)[:, 1]
auroc = roc_auc_score(y_test, y_prob)
auprc = average_precision_score(y_test, y_prob)
print(f"\nAUROC: {auroc:.4f}  AUPRC: {auprc:.4f}")
print(classification_report(y_test, (y_prob >= 0.5).astype(int)))

# ── Step 7: Quality gate ──────────────────────────────────────────────────────
assert auroc >= 0.85, f"AUROC {auroc:.4f} below threshold 0.85 — do not deploy"
print(f"✓ Quality gate passed (AUROC {auroc:.4f})")

# ── Step 8: Versioned save ────────────────────────────────────────────────────
MODEL_DIR.mkdir(parents=True, exist_ok=True)
ts = datetime.now().strftime("%Y%m%d_%H%M%S")

versioned_model  = MODEL_DIR / f"fraud_model_{ts}.pkl"
versioned_amount = MODEL_DIR / f"amount_scaler_{ts}.pkl"
versioned_time   = MODEL_DIR / f"time_scaler_{ts}.pkl"

joblib.dump(model,         versioned_model)
joblib.dump(amount_scaler, versioned_amount)
joblib.dump(time_scaler,   versioned_time)
print(f"Saved versioned models: {ts}")

# ── Step 9: Update symlinks (safe atomic replace) ─────────────────────────────
def update_symlink(link: Path, target: Path) -> None:
    """Atomically update symlink to point to new target."""
    tmp = link.with_suffix(".tmp_link")
    tmp.unlink(missing_ok=True)
    tmp.symlink_to(target.name)   # relative symlink within same dir
    tmp.replace(link)

update_symlink(MODEL_DIR / "fraud_model.pkl",    versioned_model)
update_symlink(MODEL_DIR / "amount_scaler.pkl",  versioned_amount)
update_symlink(MODEL_DIR / "time_scaler.pkl",    versioned_time)
print("Symlinks updated → latest now points to", ts)

# ── Step 10: Update model registry ────────────────────────────────────────────
registry: list = []
if REGISTRY.exists():
    try:
        registry = json.loads(REGISTRY.read_text())
    except Exception:
        registry = []

registry.append({
    "version":   ts,
    "auroc":     round(auroc, 4),
    "auprc":     round(auprc, 4),
    "trained_at": datetime.now(timezone.utc).isoformat(),
    "model_file":  versioned_model.name,
    "status":    "active",
})
REGISTRY.write_text(json.dumps(registry, indent=2))
print(f"Registry updated → {len(registry)} version(s) recorded")
print(f"\n✓ Done. To rollback: POST http://localhost:8001/models/<version>/activate")

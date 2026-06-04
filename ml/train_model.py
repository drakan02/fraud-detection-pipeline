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
from sklearn.preprocessing import RobustScaler
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

# ── Step 2: Stratified split ───────────────────────────────────────────────────
df_train, df_test = train_test_split(
    df, test_size=0.2, random_state=SEED, stratify=df["Class"])

# ── Step 3: Feature engineering (Robust Scaling fitted on train only to avoid leakage) ──
amount_scaler = RobustScaler()
time_scaler   = RobustScaler()

# Work on copies to avoid SettingWithCopyWarning
df_train = df_train.copy()
df_test = df_test.copy()

df_train["Amount_sc"] = amount_scaler.fit_transform(df_train[["Amount"]])
df_train["Time_sc"]   = time_scaler.fit_transform(df_train[["Time"]])

df_test["Amount_sc"] = amount_scaler.transform(df_test[["Amount"]])
df_test["Time_sc"]   = time_scaler.transform(df_test[["Time"]])

FEATURES = [f"V{i}" for i in range(1, 29)] + ["Amount_sc", "Time_sc"]
X_train, y_train = df_train[FEATURES], df_train["Class"]
X_test, y_test = df_test[FEATURES], df_test["Class"]
print(f"Train size: {len(X_train)} | Test size: {len(X_test)}")
print(f"Train fraud: {y_train.sum()} | Test fraud: {y_test.sum()}")

# ── Step 3.5: Save baseline stats BEFORE outlier removal ──────────────────────
# Baseline stats represent the raw training distribution (post-scaling, pre-clean)
# and are used for production feature drift monitoring. Saving them here ensures
# the drift Z-scores are computed against the true data distribution rather than
# the cleaned (outlier-removed) subset, which would understate real drift.
baseline_stats_raw = {
    "mean": X_train.mean().to_dict(),
    "std": X_train.std().to_dict()
}
print("Baseline stats captured (pre-outlier-removal) for drift monitoring.")

# ── Step 4: Remove extreme outliers from training set for top correlated features (V14, V12) ──
# Outlier removal is applied only to the fraud class to reduce overfitting on
# extreme fraud samples while preserving the full non-fraud distribution.
# This asymmetric removal is intentional: we want the model to generalise
# across the legitimate-transaction space while avoiding memorising edge-case fraud.
def remove_outliers(X_df, y_df, features_list, threshold=1.5):
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

# ── Step 5: SMOTE on training set only ────────────────────────────────────────
print("Applying SMOTE...")
X_res, y_res = SMOTE(random_state=SEED, sampling_strategy=0.1).fit_resample(X_train, y_train)
print(f"After SMOTE: {X_res.shape} | Fraud: {y_res.sum()}")

# ── Step 6: Train XGBoost ─────────────────────────────────────────────────────
print("Training...")
ratio = float(y_res.value_counts()[0] / y_res.value_counts()[1])
model = XGBClassifier(
    n_estimators=300, max_depth=6, learning_rate=0.05,
    subsample=0.8, colsample_bytree=0.8,
    scale_pos_weight=ratio,
    eval_metric="aucpr", random_state=SEED, n_jobs=-1,
)
model.fit(X_res, y_res, eval_set=[(X_test, y_test)], verbose=100)

# ── Step 7: Evaluate ──────────────────────────────────────────────────────────
y_prob = model.predict_proba(X_test)[:, 1]
auroc = roc_auc_score(y_test, y_prob)
auprc = average_precision_score(y_test, y_prob)
print(f"\nAUROC: {auroc:.4f}  AUPRC: {auprc:.4f}")
print(classification_report(y_test, (y_prob >= 0.5).astype(int)))

# ── Step 8: Quality gate ──────────────────────────────────────────────────────
# Use an explicit check instead of `assert` so this cannot be bypassed
# by running Python in optimised mode (`python -O`).
if auroc < 0.85:
    raise ValueError(
        f"Quality gate failed: AUROC {auroc:.4f} is below the minimum threshold of 0.85. "
        "Model will NOT be saved. Investigate training data or hyperparameters."
    )
print(f"✓ Quality gate passed (AUROC {auroc:.4f})")

# ── Step 9: Versioned save ────────────────────────────────────────────────────
MODEL_DIR.mkdir(parents=True, exist_ok=True)
ts = datetime.now().strftime("%Y%m%d_%H%M%S")

versioned_model  = MODEL_DIR / f"fraud_model_{ts}.pkl"
versioned_amount = MODEL_DIR / f"amount_scaler_{ts}.pkl"
versioned_time   = MODEL_DIR / f"time_scaler_{ts}.pkl"
versioned_stats  = MODEL_DIR / f"baseline_stats_{ts}.json"

joblib.dump(model,         versioned_model)
joblib.dump(amount_scaler, versioned_amount)
joblib.dump(time_scaler,   versioned_time)

# Save the pre-outlier-removal baseline stats for production drift monitoring.
versioned_stats.write_text(json.dumps(baseline_stats_raw, indent=2))

print(f"Saved versioned models and stats: {ts}")

# ── Step 10: Update symlinks (safe atomic replace) ─────────────────────────────
def update_symlink(link: Path, target: Path) -> None:
    """Atomically update symlink to point to new target."""
    tmp = link.with_suffix(".tmp_link")
    tmp.unlink(missing_ok=True)
    tmp.symlink_to(target.name)   # relative symlink within same dir
    tmp.replace(link)

update_symlink(MODEL_DIR / "fraud_model.pkl",    versioned_model)
update_symlink(MODEL_DIR / "amount_scaler.pkl",  versioned_amount)
update_symlink(MODEL_DIR / "time_scaler.pkl",    versioned_time)
update_symlink(MODEL_DIR / "baseline_stats.json", versioned_stats)
print("Symlinks updated → latest now points to", ts)

# ── Step 11: Update model registry ────────────────────────────────────────────
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

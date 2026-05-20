"""
Train XGBoost fraud detection model on Kaggle creditcardfraud dataset.
Output: ml/models/fraud_model.pkl + ml/models/amount_scaler.pkl
"""
import joblib
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, classification_report, average_precision_score
from imblearn.over_sampling import SMOTE
from xgboost import XGBClassifier

SEED      = 42
DATA      = Path("ml/data/creditcard.csv")
MODEL_DIR = Path("ml/models")

# ── Step 1: Load ──────────────────────────────────────────────────────────
print("Loading dataset...")
df = pd.read_csv(DATA)
print(f"Shape: {df.shape} | Fraud rate: {df['Class'].mean():.4%}")

# ── Step 2: Feature engineering ──────────────────────────────────────────
# V1-V28 are pre-scaled (PCA). Only scale Amount and Time.
amount_scaler = StandardScaler()
time_scaler   = StandardScaler()
df["Amount_sc"] = amount_scaler.fit_transform(df[["Amount"]])
df["Time_sc"]   = time_scaler.fit_transform(df[["Time"]])

FEATURES = [f"V{i}" for i in range(1, 29)] + ["Amount_sc", "Time_sc"]
X, y = df[FEATURES], df["Class"]

# ── Step 3: Stratified split ──────────────────────────────────────────────
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=SEED, stratify=y)
print(f"Train fraud: {y_train.sum()} | Test fraud: {y_test.sum()}")

# ── Step 4: SMOTE on training set only ───────────────────────────────────
print("Applying SMOTE...")
X_res, y_res = SMOTE(random_state=SEED, sampling_strategy=0.1).fit_resample(X_train, y_train)
print(f"After SMOTE: {X_res.shape} | Fraud: {y_res.sum()}")

# ── Step 5: Train XGBoost ─────────────────────────────────────────────────
print("Training...")
model = XGBClassifier(
    n_estimators=300, max_depth=6, learning_rate=0.05,
    subsample=0.8, colsample_bytree=0.8,
    eval_metric="aucpr", random_state=SEED, n_jobs=-1,
)
model.fit(X_res, y_res, eval_set=[(X_test, y_test)], verbose=100)

# ── Step 6: Evaluate ──────────────────────────────────────────────────────
y_prob = model.predict_proba(X_test)[:, 1]
auroc = roc_auc_score(y_test, y_prob)
auprc = average_precision_score(y_test, y_prob)
print(f"\nAUROC: {auroc:.4f}  AUPRC: {auprc:.4f}")
print(classification_report(y_test, (y_prob >= 0.5).astype(int)))

# ── Step 7: Quality gate ─────────────────────────────────────────────────
assert auroc >= 0.85, f"AUROC {auroc:.4f} below threshold 0.85 — do not deploy"
print(f"✓ Quality gate passed (AUROC {auroc:.4f})")

# ── Step 8: Save ─────────────────────────────────────────────────────────
MODEL_DIR.mkdir(parents=True, exist_ok=True)
joblib.dump(model,        MODEL_DIR / "fraud_model.pkl")
joblib.dump(amount_scaler, MODEL_DIR / "amount_scaler.pkl")
joblib.dump(time_scaler,   MODEL_DIR / "time_scaler.pkl")
print(f"Saved to {MODEL_DIR}/")

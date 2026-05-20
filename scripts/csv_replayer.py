#!/usr/bin/env python3
"""
Replays Kaggle creditcard.csv into Kafka topic 'transactions'.

Usage:
  python scripts/csv_replayer.py                    # real-time (1x)
  python scripts/csv_replayer.py --speed 10         # 10x faster
  python scripts/csv_replayer.py --fraud-only        # fraud rows only
  python scripts/csv_replayer.py --speed 50 --limit 2000
"""
import argparse, json, time, uuid
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from kafka import KafkaProducer

KAFKA    = "localhost:9093"
TOPIC    = "transactions"
DATA     = Path("ml/data/creditcard.csv")
COUNTRIES = ["VN","US","SG","JP","GB","DE","FR","AU","TH","MY"]


def args():
    p = argparse.ArgumentParser()
    p.add_argument("--speed",      type=float, default=1.0)
    p.add_argument("--limit",      type=int,   default=None)
    p.add_argument("--fraud-only", action="store_true")
    return p.parse_args()


def to_txn(row, idx: int) -> dict:
    return {
        "id":         str(uuid.uuid4()),
        "userId":     f"user_{idx % 500:04d}",
        "cardNumber": f"4{idx % 500:03d}-xxxx-xxxx-{int(row['Amount']) % 9999:04d}",
        "amount":     round(float(row["Amount"]), 2),
        "currency":   "EUR",
        "merchantId": f"MER-{idx % 1000:04d}",
        "country":    COUNTRIES[idx % len(COUNTRIES)],
        "status":     "FRAUD" if int(row["Class"]) == 1 else "SUCCESS",
        "eventTime":  datetime.now(timezone.utc).isoformat(),
        "mlFeatures": {
            **{f"V{i}": float(row[f"V{i}"]) for i in range(1, 29)},
            "Amount": float(row["Amount"]),
            "Time":   float(row["Time"]),
        },
    }


def main():
    a = args()
    producer = KafkaProducer(
        bootstrap_servers=KAFKA,
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: k.encode(),
    )

    df = pd.read_csv(DATA)
    if a.fraud_only: df = df[df["Class"] == 1]
    if a.limit:      df = df.head(a.limit)

    print(f"Replaying {len(df)} rows at {a.speed}x speed → {TOPIC}\n")
    prev_t, sent, fraud = None, 0, 0

    for idx, row in df.iterrows():
        txn = to_txn(row, idx)
        if prev_t is not None:
            delta = (float(row["Time"]) - prev_t) / a.speed
            if 0 < delta < 5:
                time.sleep(delta)
        prev_t = float(row["Time"])

        producer.send(TOPIC, key=txn["userId"], value=txn)
        sent += 1
        if txn["status"] == "FRAUD":
            fraud += 1
            print(f"[FRAUD] row={idx:6d} | {txn['userId']} | "
                  f"€{txn['amount']:8.2f} | fraud_total={fraud}")
        elif sent % 1000 == 0:
            print(f"[INFO]  sent={sent:6d} | fraud={fraud}")

    producer.flush()
    print(f"\nDone. Sent={sent} Fraud={fraud}")


if __name__ == "__main__":
    main()

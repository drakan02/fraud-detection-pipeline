#!/usr/bin/env python3
"""
Replays Kaggle creditcard.csv into Kafka topic 'transactions'.

Architecture note
-----------------
The `status` field ("FRAUD" | "SUCCESS") is included in the Kafka message so
that Flink can write it to the `ground_truth` table via GroundTruthJdbcSink.
This simulates the separation between inference time (transaction arrives with
features only) and label-arrival time (dispute result known later).

In a real production system the ground-truth label would NOT be available at
transaction time — it would arrive days later from a chargeback / dispute
resolution process and be ingested via a separate pipeline.

Usage:
  python scripts/csv_replayer.py                    # real-time (1x)
  python scripts/csv_replayer.py --speed 10         # 10x faster
  python scripts/csv_replayer.py --fraud-only        # fraud rows only
  python scripts/csv_replayer.py --speed 50 --limit 2000
  python scripts/csv_replayer.py --run-id demo-2     # distinct replay run
"""
import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import pandas as pd
from kafka import KafkaProducer


# Load .env file manually to read ports if they are not in the environment
def load_env() -> None:
    env_path = Path(".env")
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ.setdefault(key.strip(), val.strip())


load_env()

KAFKA_PORT = os.getenv("KAFKA_PORT", "30093")
KAFKA      = os.getenv("KAFKA_BOOTSTRAP", f"localhost:{KAFKA_PORT}")
TOPIC      = os.getenv("TRANSACTIONS_TOPIC", "transactions")
DATA       = Path("ml/data/creditcard.csv")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--speed",      type=float, default=1.0,
                   help="Replay speed multiplier (default: 1x real-time)")
    p.add_argument("--limit",      type=int,   default=None,
                   help="Maximum number of rows to send")
    p.add_argument("--fraud-only", action="store_true",
                   help="Only replay rows where Class == 1 (fraud)")
    p.add_argument("--run-id", default="creditcardfraud-demo",
                   help="Stable replay id. Use a new value to create a distinct transaction stream.")
    return p.parse_args()


def build_transaction(row, idx: int, base_ts: datetime, run_id: str) -> dict:
    """
    Build a Kafka transaction message from a CSV row.

    The `mlFeatures` dict contains the raw PCA features (V1-V28, Amount, Time)
    that the ML model uses for inference. The model does NOT see `status`.

    The `status` field is a ground-truth label derived from the dataset's Class
    column. Flink reads it and writes it to the `ground_truth` ClickHouse table
    via GroundTruthJdbcSink — it is intentionally kept separate from the main
    transaction business fields to reflect real-world architecture.
    """
    # Derive eventTime from CSV Time field (seconds elapsed since first txn),
    # mapped onto a real UTC timeline anchored at base_ts.
    event_ts = datetime.fromtimestamp(
        base_ts.timestamp() + float(row["Time"]),
        tz=timezone.utc,
    )
    stable_id = uuid5(
        NAMESPACE_URL,
        f"{run_id}|{idx}|{float(row['Time']):.6f}|{float(row['Amount']):.2f}|{int(row['Class'])}",
    )
    return {
        "id":         str(stable_id),
        "amount":     round(float(row["Amount"]), 2),
        "runId":      run_id,
        "dataSource": "kaggle-creditcard",
        "rawLabel":   str(int(row["Class"])),
        # Ground-truth label — used ONLY by GroundTruthJdbcSink in Flink.
        # Not available to the ML model at inference time.
        "status":     "FRAUD" if int(row["Class"]) == 1 else "SUCCESS",
        "eventTime":  event_ts.isoformat(),
        "mlFeatures": {
            **{f"V{i}": float(row[f"V{i}"]) for i in range(1, 29)},
            "Amount": float(row["Amount"]),
            "Time":   float(row["Time"]),
        },
    }


def main():
    args = parse_args()
    producer = KafkaProducer(
        bootstrap_servers=KAFKA,
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: k.encode(),
        # Durability: wait for all in-sync replicas to acknowledge
        acks="all",
        # Retry transient broker failures (leader election, network blip)
        retries=5,
        retry_backoff_ms=200,
        # lz4 reduces payload size ~40% with minimal CPU overhead
        compression_type="lz4",
        # Larger batch improves throughput during high-speed replay (--speed > 1)
        batch_size=32 * 1024,
    )

    df = pd.read_csv(DATA)
    if args.fraud_only:
        df = df[df["Class"] == 1]
    if args.limit:
        df = df.head(args.limit)

    # Anchor: treat first row's Time=0 as "now" so the entire 48-hour dataset
    # maps to [now, now + 172792 s] in real UTC.
    base_ts = datetime.now(timezone.utc)
    print(f"Replaying {len(df)} rows at {args.speed}x speed → {TOPIC}")
    print(f"Event-time base : {base_ts.isoformat()}")
    print(f"Event-time range: +{df['Time'].max() / 3600:.1f}h ({df['Time'].max():.0f}s)\n")

    prev_t, sent, fraud = None, 0, 0

    for idx, row in df.iterrows():
        txn = build_transaction(row, idx, base_ts, args.run_id)
        if prev_t is not None:
            delta = (float(row["Time"]) - prev_t) / args.speed
            if 0 < delta < 5:
                time.sleep(delta)
        prev_t = float(row["Time"])

        producer.send(TOPIC, key=txn["id"], value=txn)
        sent += 1
        if txn["status"] == "FRAUD":
            fraud += 1
            print(
                f"[FRAUD] row={idx:6d} | {txn['id'][:8]}... | "
                f"€{txn['amount']:8.2f} | eventTime={txn['eventTime']} | "
                f"fraud_total={fraud}"
            )
        elif sent % 1000 == 0:
            print(f"[INFO]  sent={sent:6d} | fraud={fraud}")

    producer.flush()
    print(f"\nDone. Sent={sent} Fraud={fraud}")


if __name__ == "__main__":
    main()

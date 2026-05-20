# 🛡️ Real-Time Fraud Detection Pipeline v2.1

A complete, production-ready streaming pipeline for detecting credit card fraud in real-time. This project combines the power of **Apache Flink** for Complex Event Processing (CEP) and **Machine Learning** (XGBoost) deployed on FastAPI. It ingests simulated transaction data through **Kafka** and writes fraud alerts directly to **PostgreSQL**.

---

## 🏗️ Architecture & Data Flow

The pipeline operates simultaneously on both rule-based heuristics and an AI-driven probability engine:

1. **Data Ingestion (Simulated):** A Python replayer (`csv_replayer.py`) streams real Kaggle transaction datasets into a Kafka topic (`transactions`).
2. **Dynamic Rule Injection:** Fraud rules (`rules.jsonl`) are injected into a secondary Kafka topic (`fraud-rules`) which are broadcasted across the cluster dynamically without downtime.
3. **Stream Processing Engine (Apache Flink):**
   - **CEP Engine (Complex Event Processing):** Tracks time-windows and sequences (e.g., 3 micro-transactions under 60 seconds -> `P001` Alert; Multiple failed transactions followed by success -> `P003` Alert).
   - **Broadcast State:** Matches real-time streams against the dynamic rule stream for immediate flagging (e.g., High-value transaction in a new location -> `P002` Alert).
   - **Machine Learning (Async I/O):** Flink asynchronously queries the FastAPI server with transaction details. The XGBoost model calculates a fraud probability score.
4. **Data Sinks:** Flink collects all fraud triggers and writes them back into Kafka (`fraud-alerts`) for microservices to consume, and into PostgreSQL (`fraud_alerts` and `transactions` tables) for analytics.

---

## 🛠️ Technology Stack

- **Stream Processing:** Apache Flink 1.20 (Java 17)
- **Message Broker:** Apache Kafka & Confluent ZooKeeper
- **Database:** PostgreSQL 16
- **Machine Learning:** XGBoost, Scikit-Learn, Pandas
- **Model Server:** FastAPI, Uvicorn, Python 3.12
- **Build Tool:** Apache Maven

---

## ⚙️ Prerequisites

Before starting, ensure your system has the following installed:
- **Java 17** and **Maven 3.8+**
- **Python 3.11/3.12** with `uv` or `pip`
- **Docker 24+** (with Docker Compose v2)
- **Apache Flink 1.20** extracted to your local machine (e.g., `~/flink-1.20.0`). *Note: Set `taskmanager.numberOfTaskSlots: 4` in your Flink `config.yaml` to ensure enough parallel execution slots.*
- **Kaggle API Credentials** (`~/.kaggle/kaggle.json`) for downloading the model training dataset.

---

## 🗺️ Port Mapping

| Port  | Service                        | Usage                                     |
|-------|--------------------------------|-------------------------------------------|
| 8081  | Flink Web UI                   | Monitor Job Execution and Backpressure    |
| 9093  | Kafka Broker (External)        | Connect producers/consumers via localhost |
| 2182  | ZooKeeper                      | Cluster management                        |
| 5433  | PostgreSQL                     | Access to `frauddb` (user: `frauduser`)   |
| 8001  | FastAPI ML server              | `/predict` endpoints and health checks    |

---

## 🚀 Startup Order & Execution Guide

Follow these steps precisely to spin up the pipeline. It is recommended to use multiple terminal windows.

### 1. Infrastructure
Spin up the Kafka, Zookeeper, and Postgres containers:
```bash
docker compose up -d
```

### 2. Setup Kafka Topics & Publish Initial Rules
```bash
# Create transactions and fraud-alerts topics
./scripts/create_topics.sh

# Push JSON rules to Kafka for Flink to broadcast
cat rules.jsonl | docker exec -i fraud-kafka \
  kafka-console-producer --bootstrap-server localhost:9092 --topic fraud-rules
```

### 3. Machine Learning Setup & Training
```bash
# Activate Python environment (assuming a virtual environment is used)
source .venv/bin/activate

# Download dataset (requires Kaggle API key)
./ml/download_dataset.sh

# Train model (requires AUROC >= 0.85 to save)
python ml/train_model.py
```

### 4. Run the API Model Server (Terminal A)
Keep this terminal open so Flink can communicate with the model.
```bash
uvicorn ml.model_server:app --host 0.0.0.0 --port 8001
```

### 5. Start Apache Flink (Terminal B)
Start the local Flink cluster.
```bash
export FLINK_HOME=~/flink-1.20.0
$FLINK_HOME/bin/start-cluster.sh
```
*Verify cluster is running by visiting [http://localhost:8081](http://localhost:8081).*

### 6. Compile Java Code and Submit Job (Terminal C)
Compile the Fat JAR (excluding tests for speed) and deploy it to Flink.
```bash
mvn clean package -DskipTests
$FLINK_HOME/bin/flink run -d target/fraud-detection-pipeline-1.0.jar
```

### 7. Trigger the Data Stream (Terminal D)
Run the replayer script to push transactions into Kafka at 5x the normal speed.
```bash
python scripts/csv_replayer.py --speed 5
```

---

## ✅ Validation & Testing

Once the data stream is running, you can verify the output in the PostgreSQL database:

```bash
docker exec -i fraud-postgres psql -U frauduser -d frauddb -c "
SELECT pattern_name, severity, amount, source FROM fraud_alerts LIMIT 5;
"
```
*Expect to see rows triggered by both `source = 'CEP'` and `source = 'ML'`.*

---

## 🧠 Retraining the Model

If you need to tune or retrain the AI model over time:
1. Stop the Model Server (Ctrl+C in Terminal A).
2. Run `python ml/train_model.py`.
3. Restart the Model Server. Flink's Async I/O handles short connectivity loss gracefully, logging warnings instead of crashing.

---

## 🛑 Shutdown Instructions

To gracefully close all connections and terminate the cluster:
```bash
# Stop Flink Cluster
$FLINK_HOME/bin/stop-cluster.sh

# Spin down Docker containers
docker compose down
```

# 🛡️ Real-Time Fraud Detection Pipeline v3.0

A complete, production-ready streaming pipeline for detecting credit card fraud in real-time. This project combines the power of **Apache Flink** for Complex Event Processing (CEP) and **Machine Learning** (XGBoost) deployed on FastAPI. It includes full observability via **Prometheus & Grafana**, dynamic **Model Versioning** with zero-downtime hot-swapping, and robust **Data Validation**.

---

## 🏗️ Architecture & Data Flow

The pipeline operates simultaneously on both rule-based heuristics and an AI-driven probability engine:

1. **Data Ingestion (Simulated):** A Python replayer (`scripts/csv_replayer.py`) streams real Kaggle transaction datasets into a Kafka topic (`transactions`).
2. **Dynamic Rule Injection:** Fraud rules (`rules.jsonl`) are injected into a secondary Kafka topic (`fraud-rules`) which are broadcasted across the cluster dynamically without downtime.
3. **Stream Processing Engine (Apache Flink):**
   - **Data Validation:** Validates incoming Kafka records and increments the `malformedMessages` Prometheus metric for corrupt/missing fields.
   - **CEP Engine (Complex Event Processing):** Tracks time-windows and sequences (e.g., 3 micro-transactions under 60 seconds -> `P001` Alert; Multiple failed transactions followed by success -> `P003` Alert).
   - **Broadcast State:** Matches real-time streams against the dynamic rule stream for immediate flagging (e.g., High-value transaction in a new location -> `P002` Alert).
   - **Machine Learning (Async I/O):** Flink asynchronously queries the FastAPI server with transaction details. The XGBoost model calculates a fraud probability score.
4. **Data Sinks:** Flink collects all fraud triggers and writes them back into Kafka (`fraud-alerts`) and directly into PostgreSQL (`fraud_alerts` and `transactions` tables) for analytics.
5. **Observability:** Prometheus scrapes metrics from both Flink and FastAPI, and Grafana visualizes the real-time throughput, latency, and fraud alerts.

---

## 📂 Project Structure

```text
.
├── docker-compose.yml          # Infrastructure (Kafka, Zookeeper, Postgres, Prometheus, Grafana)
├── .env                        # Environment variables for port configuration
├── init-db/                    # PostgreSQL initialization scripts
│   └── 01_schema.sql           # Schema for `transactions` and `fraud_alerts` tables
├── ml/                         # Machine Learning Module
│   ├── download_dataset.sh     # Script to pull Kaggle Credit Card dataset
│   ├── train_model.py          # XGBoost training script with SMOTE & timestamp versioning
│   ├── model_server.py         # FastAPI application with `/predict` and `/models` endpoints
│   └── models/                 # Model registry (`model_registry.json`) and serialized models (.pkl)
├── monitoring/                 # Observability Module
│   ├── prometheus.yml          # Prometheus scraping configuration
│   └── grafana/                # Grafana dashboards and provisioning configs
├── pom.xml                     # Maven dependencies for the Flink Java application
├── rules.jsonl                 # Static list of rules to broadcast
├── scripts/                    # Utilities
│   ├── create_topics.sh        # Kafka topic creation
│   └── csv_replayer.py         # Transaction simulator pushing to Kafka
└── src/main/java/com/fraud/    # Core Apache Flink Pipeline in Java
    ├── config/                 # Pipeline configuration
    ├── function/               # Flink functions (Async ML Inference, Broadcast Rules)
    ├── model/                  # Data objects (Transaction, FraudAlert, FraudRule)
    ├── pattern/                # Flink CEP Patterns (Rapid Small Txns, Multiple Failed)
    ├── serialization/          # Kafka Deserializers with embedded Validation Metrics
    ├── sink/                   # PostgreSQL JDBC Sinks
    └── FraudDetectionJob.java  # Main Flink Entrypoint
```

---

## 🛠️ Technology Stack

- **Stream Processing:** Apache Flink 1.20 (Java 17)
- **Message Broker:** Apache Kafka 7.6 & Confluent ZooKeeper
- **Database:** PostgreSQL 16
- **Observability:** Prometheus & Grafana
- **Machine Learning:** XGBoost, Scikit-Learn, Pandas
- **Model Server:** FastAPI, Uvicorn, Python 3.12
- **Build Tool:** Apache Maven

---

## ⚙️ Prerequisites

Before starting, ensure your system has the following installed:
- **Java 17** and **Maven 3.8+**
- **Python 3.11/3.12** with `uv` or `pip`
- **Docker 24+** (with Docker Compose v2)
- **Apache Flink 1.20** extracted to your local machine (e.g., `~/flink-1.20.0`). 
  - *Note: Ensure `flink-metrics-prometheus` JAR is placed in your Flink `plugins/metrics-prometheus/` folder.*
  - *Note: Configure `metrics.reporter.prom.port: 9249-9260` in `config.yaml` to avoid port collisions.*
- **Kaggle API Credentials** (`~/.kaggle/kaggle.json`) for downloading the model training dataset.

---

## 🗺️ Port Mapping

*(Ports are configurable via `.env`)*

| Port       | Service                        | Usage                                     |
|------------|--------------------------------|-------------------------------------------|
| 8081       | Flink Web UI                   | Monitor Job Execution and Backpressure    |
| 9093       | Kafka Broker (External)        | Connect producers/consumers via localhost |
| 2182       | ZooKeeper                      | Cluster management                        |
| 5433       | PostgreSQL                     | Access to `frauddb` (user: `frauduser`)   |
| 8001       | FastAPI ML server              | `/predict`, `/models` and `/metrics`      |
| 9090       | Prometheus                     | Scrape target monitoring                  |
| 3000       | Grafana                        | View the Real-Time Fraud Dashboard        |
| 9249-9260  | Flink Prometheus Reporters     | Exposes Flink internal and custom metrics |

---

## 🚀 Startup Order & Execution Guide

Follow these steps precisely to spin up the pipeline. It is recommended to use multiple terminal windows.

### 1. Infrastructure
Spin up the Kafka, Zookeeper, Postgres, Prometheus, and Grafana containers:
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

# Train model (requires AUROC >= 0.85 to save). This creates a timestamped version.
python ml/train_model.py
```

### 4. Run the API Model Server (Terminal A)
Keep this terminal open so Flink can communicate with the model and Prometheus can scrape `/metrics`.
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

## 📊 Monitoring & Observability

Once the data stream is running, you can monitor the pipeline in real-time:

1. **Grafana Dashboard:** Visit `http://localhost:3000`. Navigate to **Dashboards > Fraud Pipeline**.
   - Here you can monitor: Throughput (Records/sec), Model Server Latency, Processed Transactions, and Malformed Messages.
2. **Database Verification:**
   ```bash
   docker exec -i fraud-postgres psql -U frauduser -d frauddb -c "
   SELECT pattern_name, severity, amount, source FROM fraud_alerts LIMIT 5;
   "
   ```

---

## 🧠 Model Versioning & Hot-Swapping

The Machine Learning server tracks a complete history of models in `model_registry.json`.
If you need to tune or retrain the AI model over time:

1. Run `python ml/train_model.py` to train a new model. A new timestamped version will be created.
2. Check available versions:
   ```bash
   curl -s http://localhost:8001/models
   ```
3. **Hot-Swap Model:** To activate a specific version (e.g. rollback) **without restarting Flink**:
   ```bash
   curl -X POST http://localhost:8001/models/<version-id>/activate
   ```
   *The Model Server instantly reloads the specified model into memory, and Flink begins routing predictions through it immediately.*

---

## 🛑 Shutdown Instructions

To gracefully close all connections and terminate the cluster:
```bash
# Stop Flink Cluster
$FLINK_HOME/bin/stop-cluster.sh

# Spin down Docker containers
docker compose down
```

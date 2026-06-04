package com.fraud.config;

public class PipelineConfig {

    // Kafka — host port 9093
    public static final String KAFKA_BOOTSTRAP    = env("KAFKA_BOOTSTRAP",    "localhost:30093");
    public static final String TRANSACTIONS_TOPIC = env("TRANSACTIONS_TOPIC", "transactions");
    public static final String ALERTS_TOPIC       = env("ALERTS_TOPIC",       "fraud-alerts");

    // ClickHouse — host/container port 8123 (HTTP interface)
    public static final String CLICKHOUSE_URL      = env("CLICKHOUSE_URL",      "jdbc:clickhouse://localhost:8123/default");
    public static final String CLICKHOUSE_USER     = env("CLICKHOUSE_USER",     "default");
    public static final String CLICKHOUSE_PASSWORD = env("CLICKHOUSE_PASSWORD", "");

    static {
        if (CLICKHOUSE_PASSWORD.isEmpty()) {
            throw new IllegalStateException("CLICKHOUSE_PASSWORD environment variable is required and must not be empty.");
        }
    }

    // FastAPI model server — port 8001
    // NOTE: The fraud classification threshold (ML_THRESHOLD) is intentionally
    // configured on the ML server side (model_server.py) so it can be adjusted
    // per-model-version without redeploying Flink. Flink trusts the is_fraud
    // boolean returned by the ML server.
    public static final String MODEL_SERVER_URL = env("MODEL_SERVER_URL", "http://localhost:" + env("MODEL_SERVER_PORT", "8001"));

    // Sentinel value stored in FraudAlert.mlProbability when the alert was
    // produced by the rule-based fallback (ML server was unavailable).
    public static final double ML_PROBABILITY_NOT_AVAILABLE = -1.0;

    // Flink
    public static final int PARALLELISM         = Integer.parseInt(env("PARALLELISM",         "2"));
    public static final int CHECKPOINT_INTERVAL = Integer.parseInt(env("CHECKPOINT_INTERVAL", "30000"));
    public static final int ML_ASYNC_TIMEOUT_MS = Integer.parseInt(env("ML_ASYNC_TIMEOUT_MS", "5000"));
    public static final int ML_ASYNC_CAPACITY   = Integer.parseInt(env("ML_ASYNC_CAPACITY",   "100"));
    // CHECKPOINT_STORAGE default points to the PVC mount path defined in flink.yaml
    // (volumeMounts.mountPath = /flink-checkpoints). /tmp was previously the default
    // but is ephemeral and wiped on pod restart, defeating the purpose of checkpointing.
    public static final String CHECKPOINT_STORAGE = env("CHECKPOINT_STORAGE", "file:///flink-checkpoints/fraud-pipeline");

    private static String env(String key, String defaultVal) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : defaultVal;
    }
}

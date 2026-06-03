package com.fraud.config;

public class PipelineConfig {

    // Kafka — host port 9093
    public static final String KAFKA_BOOTSTRAP    = env("KAFKA_BOOTSTRAP",    "localhost:30093");
    public static final String TRANSACTIONS_TOPIC = env("TRANSACTIONS_TOPIC", "transactions");
    public static final String ALERTS_TOPIC       = env("ALERTS_TOPIC",       "fraud-alerts");

    // ClickHouse — host/container port 8123 (HTTP interface)
    public static final String CLICKHOUSE_URL      = env("CLICKHOUSE_URL",      "jdbc:clickhouse://localhost:8123/default");
    public static final String CLICKHOUSE_USER     = env("CLICKHOUSE_USER",     "default");
    public static final String CLICKHOUSE_PASSWORD = env("CLICKHOUSE_PASSWORD", "clickhousepass");

    // FastAPI model server — port 8001
    public static final String MODEL_SERVER_URL   = env("MODEL_SERVER_URL", "http://localhost:" + env("MODEL_SERVER_PORT", "8001"));
    public static final double ML_FRAUD_THRESHOLD = Double.parseDouble(env("ML_THRESHOLD", "0.5"));

    // Flink
    public static final int PARALLELISM          = Integer.parseInt(env("PARALLELISM",          "2"));
    public static final int CHECKPOINT_INTERVAL  = Integer.parseInt(env("CHECKPOINT_INTERVAL",  "30000"));
    public static final int ML_ASYNC_TIMEOUT_MS  = Integer.parseInt(env("ML_ASYNC_TIMEOUT_MS",  "5000"));
    public static final int ML_ASYNC_CAPACITY    = Integer.parseInt(env("ML_ASYNC_CAPACITY",    "100"));
    public static final String CHECKPOINT_STORAGE = env("CHECKPOINT_STORAGE", "file:///tmp/flink-checkpoints/fraud-pipeline");

    private static String env(String key, String defaultVal) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : defaultVal;
    }
}

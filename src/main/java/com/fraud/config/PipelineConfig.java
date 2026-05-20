package com.fraud.config;

public class PipelineConfig {

    // Kafka — host port 9093
    public static final String KAFKA_BOOTSTRAP    = env("KAFKA_BOOTSTRAP",    "localhost:9093");
    public static final String TRANSACTIONS_TOPIC = env("TRANSACTIONS_TOPIC", "transactions");
    public static final String RULES_TOPIC        = env("RULES_TOPIC",        "fraud-rules");
    public static final String ALERTS_TOPIC       = env("ALERTS_TOPIC",       "fraud-alerts");

    // PostgreSQL — host port 5433, container port 5432
    public static final String PG_URL      = env("PG_URL",      "jdbc:postgresql://localhost:5433/frauddb");
    public static final String PG_USER     = env("PG_USER",     "frauduser");
    public static final String PG_PASSWORD = env("PG_PASSWORD", "fraudpass");

    // FastAPI model server — port 8001
    public static final String MODEL_SERVER_URL   = env("MODEL_SERVER_URL",   "http://localhost:8001");
    public static final double ML_FRAUD_THRESHOLD = Double.parseDouble(env("ML_THRESHOLD", "0.5"));

    // Flink
    public static final int PARALLELISM          = Integer.parseInt(env("PARALLELISM",          "2"));
    public static final int CHECKPOINT_INTERVAL  = Integer.parseInt(env("CHECKPOINT_INTERVAL",  "30000"));
    public static final int ML_ASYNC_TIMEOUT_MS  = Integer.parseInt(env("ML_ASYNC_TIMEOUT_MS",  "5000"));
    public static final int ML_ASYNC_CAPACITY    = Integer.parseInt(env("ML_ASYNC_CAPACITY",    "100"));

    private static String env(String key, String defaultVal) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : defaultVal;
    }
}

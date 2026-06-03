package com.fraud.model;

import java.time.Instant;
import java.util.UUID;

/**
 * Represents a fraud alert emitted by the detection pipeline.
 *
 * <p>Alerts can originate from two sources:
 * <ul>
 *   <li>{@code "ML"} — XGBoost model inference via FastAPI.</li>
 *   <li>{@code "RULE_FALLBACK"} — Simple amount threshold rule applied when
 *       the ML server is unreachable after all retries.</li>
 * </ul>
 * Future reserved source: {@code "CEP"} for Flink CEP rule-based detection.</p>
 */
public class FraudAlert {
    public String id;
    public String transactionId;
    public String userId;
    public String patternId;
    public String patternName;
    public String severity;
    public double amount;
    public String description;
    public String source;
    public double mlProbability;
    public Instant detectedAt;

    public FraudAlert() {}

    /** Creates an alert from a successful ML inference result. */
    public static FraudAlert ml(Transaction txn, double probability) {
        FraudAlert a = new FraudAlert();
        a.id = UUID.randomUUID().toString();
        a.transactionId = txn.id;
        a.userId = txn.userId;
        a.patternId = "ML001";
        a.patternName = "XGBoost ML Detection";
        a.severity = probability >= 0.9 ? "CRITICAL" : "HIGH";
        a.amount = txn.amount.doubleValue();
        a.source = "ML";
        a.mlProbability = probability;
        a.description = String.format("XGBoost fraud probability %.1f%%", probability * 100);
        a.detectedAt = Instant.now();
        return a;
    }

    /**
     * Creates an alert from the rule-based fallback when the ML server is
     * unavailable after all retry attempts.
     */
    public static FraudAlert ruleFallback(Transaction txn) {
        FraudAlert a = new FraudAlert();
        a.id = UUID.randomUUID().toString();
        a.transactionId = txn.id;
        a.userId = txn.userId;
        a.patternId = "RULE001";
        a.patternName = "Amount Threshold Rule (ML Fallback)";
        a.severity = "HIGH";
        a.amount = txn.amount.doubleValue();
        a.source = "RULE_FALLBACK";
        a.mlProbability = -1.0;  // -1 signals "not computed by ML"
        a.description = String.format(
            "ML server unavailable — flagged by amount threshold rule (€%.2f > threshold)",
            txn.amount.doubleValue());
        a.detectedAt = Instant.now();
        return a;
    }
}

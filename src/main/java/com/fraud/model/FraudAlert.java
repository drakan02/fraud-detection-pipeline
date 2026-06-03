package com.fraud.model;

import java.time.Instant;
import java.util.UUID;

public class FraudAlert {
    public String id;
    public String transactionId;
    public String userId;
    public String patternId;
    public String patternName;
    public String severity;
    public double amount;
    public String description;
    public String source;           // "ML" (reserved: "CEP" for future rule-based detection)
    public double mlProbability;
    public Instant detectedAt;

    public FraudAlert() {}

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
}

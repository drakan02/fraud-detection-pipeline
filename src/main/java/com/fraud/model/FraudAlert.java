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
    public String source;           // "CEP" | "ML"
    public double mlProbability;    // 0.0 for CEP alerts
    public Instant detectedAt;

    public FraudAlert() {}

    public static FraudAlert cep(Transaction txn, String patternId,
                                  String patternName, String severity, String description) {
        FraudAlert a = new FraudAlert();
        a.id = UUID.randomUUID().toString();
        a.transactionId = txn.id;
        a.userId = txn.userId;
        a.patternId = patternId;
        a.patternName = patternName;
        a.severity = severity;
        a.amount = txn.amount.doubleValue();
        a.description = description;
        a.source = "CEP";
        a.mlProbability = 0.0;
        a.detectedAt = Instant.now();
        return a;
    }

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

package com.fraud.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

/**
 * POJO representing a payment transaction consumed from Kafka.
 *
 * <p>Fields V1-V28, Amount, and Time are raw PCA-anonymised features from the
 * Kaggle creditcard dataset, forwarded directly to the ML inference endpoint.</p>
 *
 * <p><strong>Demo note:</strong> The {@code status} field ("FRAUD" | "SUCCESS")
 * is a ground-truth label derived from the dataset's Class column. It is used
 * exclusively for offline evaluation (written to the {@code ground_truth} table
 * by {@link com.fraud.sink.GroundTruthJdbcSink}). In a real production system
 * this label would not be available at transaction time — it would arrive later
 * via a dispute-resolution process.</p>
 */
public class Transaction {
    public String id;
    public String userId;
    public String cardNumber;
    public BigDecimal amount;
    public String currency;
    public String merchantId;
    public String country;
    /** Ground-truth label for demo evaluation only. NOT stored in transactions table. */
    public String status;
    public Instant eventTime;
    /** PCA features V1-V28 + raw Amount + Time, used as ML inference input. */
    public Map<String, Double> mlFeatures;

    public Transaction() {}

    @Override
    public String toString() {
        return String.format("Transaction{id=%s, userId=%s, amount=%s, status=%s}",
            id, userId, amount, status);
    }
}

package com.fraud.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

public class Transaction {
    public String id;
    public String userId;
    public String cardNumber;
    public BigDecimal amount;
    public String currency;
    public String merchantId;
    public String country;
    public String status;           // "SUCCESS" | "FRAUD"
    public Instant eventTime;
    public Map<String, Double> mlFeatures;  // V1-V28, Time, Amount from replayer

    public Transaction() {}

    @Override
    public String toString() {
        return String.format("Transaction{id=%s, userId=%s, amount=%s, status=%s}",
            id, userId, amount, status);
    }
}

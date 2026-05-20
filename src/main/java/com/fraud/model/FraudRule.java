package com.fraud.model;

import java.io.Serializable;

public class FraudRule implements Serializable {
    public String ruleId;           // "P001" | "P002" | "P003"
    public String ruleName;
    public boolean enabled;
    public double thresholdAmount;
    public int windowSeconds;
    public int minOccurrences;
    public String severity;         // "LOW" | "MEDIUM" | "HIGH" | "CRITICAL"

    public FraudRule() {}
}

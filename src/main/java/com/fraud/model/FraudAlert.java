package com.fraud.model;

import com.fraud.config.PipelineConfig;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

/**
 * Represents a fraud alert emitted by the detection pipeline.
 *
 * <p>Alerts originate from two sources:
 * <ul>
 *   <li>{@code "ML"} — XGBoost model inference via FastAPI.</li>
 *   <li>{@code "RULE_FALLBACK"} — Simple amount threshold rule applied when
 *       the ML server is unreachable after all retries.</li>
 * </ul>
 * </p>
 */
public class FraudAlert {

    private String id;
    private String transactionId;
    private String patternId;
    private String patternName;
    private String severity;
    private double amount;
    private String description;
    private String source;
    private String runId;
    private String modelVersion;
    private double threshold;
    /**
     * ML model fraud probability in [0, 1].
     * Set to {@link PipelineConfig#ML_PROBABILITY_NOT_AVAILABLE} (-1.0)
     * when the alert was produced by the rule-based fallback (ML server unavailable).
     */
    private double mlProbability;
    private Instant detectedAt;

    public FraudAlert() {}

    // ── Getters ────────────────────────────────────────────────────────────────

    public String getId()           { return id; }
    public String getTransactionId(){ return transactionId; }
    public String getPatternId()    { return patternId; }
    public String getPatternName()  { return patternName; }
    public String getSeverity()     { return severity; }
    public double getAmount()       { return amount; }
    public String getDescription()  { return description; }
    public String getSource()       { return source; }
    public String getRunId()        { return runId; }
    public String getModelVersion() { return modelVersion; }
    public double getThreshold()    { return threshold; }
    public double getMlProbability(){ return mlProbability; }
    public Instant getDetectedAt()  { return detectedAt; }

    // ── Setters ────────────────────────────────────────────────────────────────

    public void setId(String id)                   { this.id = id; }
    public void setTransactionId(String tid)        { this.transactionId = tid; }
    public void setPatternId(String pid)            { this.patternId = pid; }
    public void setPatternName(String name)         { this.patternName = name; }
    public void setSeverity(String severity)        { this.severity = severity; }
    public void setAmount(double amount)            { this.amount = amount; }
    public void setDescription(String description)  { this.description = description; }
    public void setSource(String source)            { this.source = source; }
    public void setRunId(String runId)              { this.runId = runId; }
    public void setModelVersion(String modelVersion){ this.modelVersion = modelVersion; }
    public void setThreshold(double threshold)       { this.threshold = threshold; }
    public void setMlProbability(double prob)       { this.mlProbability = prob; }
    public void setDetectedAt(Instant detectedAt)   { this.detectedAt = detectedAt; }

    // ── Factory methods ────────────────────────────────────────────────────────

    /**
     * Creates an alert from a successful ML inference result.
     *
     * @param txn         the source transaction
     * @param probability fraud probability in [0, 1]
     * @param algorithm   the active model algorithm name (e.g. "XGBoost", "LightGBM", "CatBoost")
     * @param modelVersion the active model version returned by the model server
     * @param threshold   the threshold used by the model server for classification
     */
    public static FraudAlert ml(Transaction txn, double probability, String algorithm,
                                String modelVersion, double threshold) {
        FraudAlert a = new FraudAlert();
        String version = modelVersion != null ? modelVersion : "unknown";
        a.id            = deterministicId(txn.getId(), "ML001", version);
        a.transactionId = txn.getId();
        a.patternId     = "ML001";
        a.patternName   = (algorithm != null ? algorithm : "ML") + " Detection";
        a.severity      = probability >= 0.9 ? "CRITICAL" : "HIGH";
        a.amount        = txn.getAmount().doubleValue();
        a.source        = "ML";
        a.runId         = txn.getRunId();
        a.modelVersion  = version;
        a.threshold     = threshold;
        a.mlProbability = probability;
        a.description   = String.format("%s fraud probability %.1f%% (threshold %.4f)",
                            algorithm != null ? algorithm : "ML", probability * 100, threshold);
        a.detectedAt    = Instant.now();
        return a;
    }

    /**
     * Creates an alert from the rule-based fallback when the ML server is
     * unavailable after all retry attempts.
     *
     * <p>{@code mlProbability} is set to
     * {@link PipelineConfig#ML_PROBABILITY_NOT_AVAILABLE} to signal that no
     * ML score was computed.</p>
     */
    public static FraudAlert ruleFallback(Transaction txn) {
        FraudAlert a = new FraudAlert();
        a.id            = deterministicId(txn.getId(), "RULE001", "rule_fallback");
        a.transactionId = txn.getId();
        a.patternId     = "RULE001";
        a.patternName   = "Amount Threshold Rule (ML Fallback)";
        a.severity      = "HIGH";
        a.amount        = txn.getAmount().doubleValue();
        a.source        = "RULE_FALLBACK";
        a.runId         = txn.getRunId();
        a.modelVersion  = "rule_fallback";
        a.threshold     = 2000.0;
        a.mlProbability = PipelineConfig.ML_PROBABILITY_NOT_AVAILABLE;
        a.description   = String.format(
            "ML server unavailable — flagged by amount threshold rule (€%.2f > threshold)",
            txn.getAmount().doubleValue());
        a.detectedAt    = Instant.now();
        return a;
    }

    private static String deterministicId(String transactionId, String patternId, String version) {
        String key = String.join("|",
            transactionId != null ? transactionId : "unknown",
            patternId != null ? patternId : "unknown",
            version != null ? version : "unknown");
        return UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8)).toString();
    }
}

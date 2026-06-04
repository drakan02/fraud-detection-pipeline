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

    private String id;
    private BigDecimal amount;
    /** Ground-truth label for demo evaluation only. NOT stored in transactions table. */
    private String status;
    /** Stable replay/batch identifier used for audit queries and dedup analysis. */
    private String runId;
    /** Data origin, for example "kaggle-creditcard". */
    private String dataSource;
    /** Raw dataset label before mapping to status. Demo-only audit metadata. */
    private String rawLabel;
    private Instant eventTime;
    /** PCA features V1-V28 + raw Amount + Time, used as ML inference input. */
    private Map<String, Double> mlFeatures;

    public Transaction() {}

    public String getId()                       { return id; }
    public void setId(String id)               { this.id = id; }

    public BigDecimal getAmount()               { return amount; }
    public void setAmount(BigDecimal amount)   { this.amount = amount; }

    public String getStatus()                   { return status; }
    public void setStatus(String status)       { this.status = status; }

    public String getRunId()                    { return runId; }
    public void setRunId(String runId)          { this.runId = runId; }

    public String getDataSource()               { return dataSource; }
    public void setDataSource(String dataSource){ this.dataSource = dataSource; }

    public String getRawLabel()                 { return rawLabel; }
    public void setRawLabel(String rawLabel)    { this.rawLabel = rawLabel; }

    public Instant getEventTime()               { return eventTime; }
    public void setEventTime(Instant eventTime){ this.eventTime = eventTime; }

    public Map<String, Double> getMlFeatures()                      { return mlFeatures; }
    public void setMlFeatures(Map<String, Double> mlFeatures)      { this.mlFeatures = mlFeatures; }

    @Override
    public String toString() {
        return String.format("Transaction{id=%s, amount=%s, status=%s, runId=%s}",
            id, amount, status, runId);
    }
}

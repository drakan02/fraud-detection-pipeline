package com.fraud.sink;

import com.fraud.config.PipelineConfig;
import com.fraud.model.FraudAlert;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Timestamp;

public class AlertJdbcSink {

    private static final String SQL =
        "INSERT INTO fraud_alerts " +
        "(id,transaction_id,pattern_id,pattern_name,severity," +
        " amount,description,source,run_id,model_version,threshold,ml_probability,detected_at) " +
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";

    public static SinkFunction<FraudAlert> build() {
        return JdbcSink.sink(
            SQL,
            (stmt, a) -> {
                stmt.setString(1,  a.getId());
                stmt.setString(2,  a.getTransactionId());
                stmt.setString(3,  a.getPatternId());
                stmt.setString(4,  a.getPatternName());
                stmt.setString(5,  a.getSeverity());
                stmt.setDouble(6,  a.getAmount());
                stmt.setString(7,  a.getDescription());
                stmt.setString(8,  a.getSource() != null ? a.getSource() : "ML");
                stmt.setString(9,  a.getRunId() != null && !a.getRunId().isBlank() ? a.getRunId() : "unknown");
                stmt.setString(10, a.getModelVersion() != null ? a.getModelVersion() : "unknown");
                stmt.setDouble(11, a.getThreshold());
                stmt.setDouble(12, a.getMlProbability());
                stmt.setTimestamp(13, Timestamp.from(a.getDetectedAt()));
            },
            JdbcExecutionOptions.builder()
                .withBatchSize(200)
                .withBatchIntervalMs(1000)
                .withMaxRetries(3)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(PipelineConfig.CLICKHOUSE_URL)
                .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                .withUsername(PipelineConfig.CLICKHOUSE_USER)
                .withPassword(PipelineConfig.CLICKHOUSE_PASSWORD)
                .build()
        );
    }
}

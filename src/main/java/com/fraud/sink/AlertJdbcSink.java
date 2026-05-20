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
        "(id,transaction_id,user_id,pattern_id,pattern_name,severity," +
        " amount,description,source,ml_probability,detected_at) " +
        "VALUES (?,?,?,?,?,?,?,?,?,?,?) " +
        "ON CONFLICT (id) DO NOTHING";

    public static SinkFunction<FraudAlert> build() {
        return JdbcSink.sink(
            SQL,
            (stmt, a) -> {
                stmt.setString(1,  a.id);
                stmt.setString(2,  a.transactionId);
                stmt.setString(3,  a.userId);
                stmt.setString(4,  a.patternId);
                stmt.setString(5,  a.patternName);
                stmt.setString(6,  a.severity);
                stmt.setDouble(7,  a.amount);
                stmt.setString(8,  a.description);
                stmt.setString(9,  a.source != null ? a.source : "CEP");
                stmt.setDouble(10, a.mlProbability);
                stmt.setTimestamp(11, Timestamp.from(a.detectedAt));
            },
            JdbcExecutionOptions.builder()
                .withBatchSize(200)
                .withBatchIntervalMs(1000)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(PipelineConfig.PG_URL)
                .withDriverName("org.postgresql.Driver")
                .withUsername(PipelineConfig.PG_USER)
                .withPassword(PipelineConfig.PG_PASSWORD)
                .build()
        );
    }
}

package com.fraud.sink;

import com.fraud.config.PipelineConfig;
import com.fraud.model.Transaction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Timestamp;

/**
 * Writes ground-truth labels (actual_label = FRAUD | SUCCESS) to the
 * {@code default.ground_truth} table in ClickHouse.
 *
 * <p>In a production environment the ground-truth label only becomes
 * available after a dispute-resolution process (chargebacks, manual review).
 * Here we simulate that separation by writing the label to a dedicated table
 * rather than embedding it in the {@code transactions} record, so the
 * Confusion Matrix query can JOIN against {@code ground_truth} instead of
 * reading a label that was co-located with the transaction features.</p>
 */
public class GroundTruthJdbcSink {

    private static final String SQL =
        "INSERT INTO ground_truth (transaction_id, actual_label, raw_label, run_id, data_source) " +
        "VALUES (?,?,?,?,?)";

    public static SinkFunction<Transaction> build() {
        return JdbcSink.sink(
            SQL,
            (stmt, t) -> {
                stmt.setString(1, t.getId());
                // status is "FRAUD" or "SUCCESS" — set by csv_replayer from the
                // Kaggle dataset Class column (ground truth label, demo-only)
                stmt.setString(2, t.getStatus() != null ? t.getStatus() : "SUCCESS");
                stmt.setString(3, t.getRawLabel() != null ? t.getRawLabel() : "");
                stmt.setString(4, t.getRunId() != null && !t.getRunId().isBlank() ? t.getRunId() : "unknown");
                stmt.setString(5, t.getDataSource() != null && !t.getDataSource().isBlank() ? t.getDataSource() : "unknown");
            },
            JdbcExecutionOptions.builder()
                .withBatchSize(500)
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

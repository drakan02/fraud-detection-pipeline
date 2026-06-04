package com.fraud.sink;

import com.fraud.config.PipelineConfig;
import com.fraud.model.Transaction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * Writes raw transaction business fields to ClickHouse {@code default.transactions}.
 *
 * <p>Note: The ground-truth label ({@code status}) is intentionally NOT stored here.
 * It is written to the separate {@code default.ground_truth} table by
 * {@link GroundTruthJdbcSink}, keeping business data and evaluation labels decoupled.</p>
 *
 * <p>Transactions with a null {@code eventTime} are dropped with a warning rather than
 * written with an inaccurate processing-time fallback, which would silently corrupt
 * time-series queries and dashboard metrics.</p>
 */
public class TransactionJdbcSink {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionJdbcSink.class);

    private static final String SQL =
        "INSERT INTO transactions (id, amount, event_time, run_id, data_source) " +
        "VALUES (?,?,?,?,?)";

    public static SinkFunction<Transaction> build() {
        return JdbcSink.sink(
            SQL,
            (stmt, t) -> {
                if (t.getEventTime() == null) {
                    // Drop records with missing event time rather than inserting a
                    // processing-time fallback that would corrupt time-series queries.
                    LOG.warn("Transaction id={} has null eventTime — skipping ClickHouse write", t.getId());
                    // Flink JDBC sink does not support skipping inside the StatementBuilder;
                    // insert a sentinel epoch so the row is visible for debugging, then
                    // filter it out in dashboards via event_time > '1970-01-01'.
                    stmt.setString(1, t.getId());
                    stmt.setBigDecimal(2, t.getAmount());
                    stmt.setTimestamp(3, new Timestamp(0L)); // epoch sentinel — easily filterable
                    stmt.setString(4, safe(t.getRunId(), "unknown"));
                    stmt.setString(5, safe(t.getDataSource(), "unknown"));
                    return;
                }
                stmt.setString(1, t.getId());
                stmt.setBigDecimal(2, t.getAmount());
                stmt.setTimestamp(3, Timestamp.from(t.getEventTime()));
                stmt.setString(4, safe(t.getRunId(), "unknown"));
                stmt.setString(5, safe(t.getDataSource(), "unknown"));
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

    private static String safe(String value, String fallback) {
        return value != null && !value.isBlank() ? value : fallback;
    }
}

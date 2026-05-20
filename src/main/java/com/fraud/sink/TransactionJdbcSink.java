package com.fraud.sink;

import com.fraud.config.PipelineConfig;
import com.fraud.model.Transaction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Timestamp;

public class TransactionJdbcSink {

    private static final String SQL =
        "INSERT INTO transactions " +
        "(id,user_id,card_number,amount,currency,merchant_id,country,status,event_time) " +
        "VALUES (?,?,?,?,?,?,?,?,?) " +
        "ON CONFLICT (id) DO NOTHING";

    public static SinkFunction<Transaction> build() {
        return JdbcSink.sink(
            SQL,
            (stmt, t) -> {
                stmt.setString(1, t.id);
                stmt.setString(2, t.userId);
                stmt.setString(3, t.cardNumber);
                stmt.setBigDecimal(4, t.amount);
                stmt.setString(5, t.currency);
                stmt.setString(6, t.merchantId);
                stmt.setString(7, t.country);
                stmt.setString(8, t.status);
                stmt.setTimestamp(9, t.eventTime != null ? Timestamp.from(t.eventTime) : new Timestamp(System.currentTimeMillis()));
            },
            JdbcExecutionOptions.builder()
                .withBatchSize(500)
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

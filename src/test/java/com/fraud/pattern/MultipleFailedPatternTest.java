package com.fraud.pattern;

import com.fraud.model.Transaction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MultipleFailedPatternTest {

    @Test
    public void testPatternMatch_Valid() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<Transaction> transactions = new ArrayList<>();
        transactions.add(createTransaction("u1", "FRAUD", 1000L));
        transactions.add(createTransaction("u1", "FRAUD", 2000L));
        transactions.add(createTransaction("u1", "SUCCESS", 3000L));

        DataStream<Transaction> stream = env.fromCollection(transactions)
                .assignTimestampsAndWatermarks(
                        org.apache.flink.api.common.eventtime.WatermarkStrategy
                                .<Transaction>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.eventTime.toEpochMilli())
                );

        PatternStream<Transaction> patternStream = CEP.pattern(
                stream.keyBy(t -> t.userId),
                MultipleFailedPattern.build(300)
        );

        TestSink.results.clear();

        patternStream.select((PatternSelectFunction<Transaction, String>) pattern -> "MATCHED")
                .addSink(new TestSink());

        env.execute("Test MultipleFailed");

        assertEquals(1, TestSink.results.size(), "Should match 2 FRAUD followed by 1 SUCCESS");
    }

    @Test
    public void testPatternNoMatch_OnlyOneFraud() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<Transaction> transactions = new ArrayList<>();
        transactions.add(createTransaction("u1", "FRAUD", 1000L));
        transactions.add(createTransaction("u1", "SUCCESS", 2000L));
        transactions.add(createTransaction("u1", "SUCCESS", 3000L));

        DataStream<Transaction> stream = env.fromCollection(transactions)
                .assignTimestampsAndWatermarks(
                        org.apache.flink.api.common.eventtime.WatermarkStrategy
                                .<Transaction>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.eventTime.toEpochMilli())
                );

        PatternStream<Transaction> patternStream = CEP.pattern(
                stream.keyBy(t -> t.userId),
                MultipleFailedPattern.build(300)
        );

        TestSink.results.clear();
        patternStream.select((PatternSelectFunction<Transaction, String>) pattern -> "MATCHED")
                .addSink(new TestSink());

        env.execute("Test MultipleFailed");

        assertTrue(TestSink.results.isEmpty(), "Should not match because there's only 1 FRAUD");
    }

    private Transaction createTransaction(String user, String status, long timestamp) {
        Transaction t = new Transaction();
        t.id = java.util.UUID.randomUUID().toString();
        t.userId = user;
        t.amount = BigDecimal.valueOf(100.0);
        t.eventTime = java.time.Instant.ofEpochMilli(timestamp);
        t.status = status;
        return t;
    }

    public static class TestSink implements SinkFunction<String> {
        public static final List<String> results = new ArrayList<>();

        @Override
        public void invoke(String value, Context context) {
            results.add(value);
        }
    }
}

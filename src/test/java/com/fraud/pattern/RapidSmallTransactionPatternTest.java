package com.fraud.pattern;

import com.fraud.model.FraudRule;
import com.fraud.model.Transaction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RapidSmallTransactionPatternTest {

    private FraudRule rule;

    @BeforeEach
    public void setup() {
        rule = new FraudRule();
        rule.ruleId = "P001";
        rule.ruleName = "RAPID_SMALL_TXN";
        rule.minOccurrences = 3;
        rule.thresholdAmount = 10.0;
        rule.windowSeconds = 60;
    }

    @Test
    public void testPatternMatch_Valid() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<Transaction> transactions = new ArrayList<>();
        transactions.add(createTransaction("u1", 5.0, 1000L));
        transactions.add(createTransaction("u1", 3.0, 2000L));
        transactions.add(createTransaction("u1", 8.0, 3000L));

        DataStream<Transaction> stream = env.fromCollection(transactions)
                .assignTimestampsAndWatermarks(
                        org.apache.flink.api.common.eventtime.WatermarkStrategy
                                .<Transaction>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.eventTime.toEpochMilli())
                );

        PatternStream<Transaction> patternStream = CEP.pattern(
                stream.keyBy(t -> t.cardNumber),
                RapidSmallTransactionPattern.build(rule)
        );

        TestSink.results.clear();

        patternStream.select((PatternSelectFunction<Transaction, String>) pattern -> {
            return "MATCHED";
        }).addSink(new TestSink());

        env.execute("Test CEP");

        assertEquals(1, TestSink.results.size(), "Should have exactly 1 match");
        assertEquals("MATCHED", TestSink.results.get(0));
    }

    @Test
    public void testPatternNoMatch_ExceedsAmount() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<Transaction> transactions = new ArrayList<>();
        transactions.add(createTransaction("u1", 5.0, 1000L));
        transactions.add(createTransaction("u1", 15.0, 2000L)); // Over threshold
        transactions.add(createTransaction("u1", 8.0, 3000L));

        DataStream<Transaction> stream = env.fromCollection(transactions)
                .assignTimestampsAndWatermarks(
                        org.apache.flink.api.common.eventtime.WatermarkStrategy
                                .<Transaction>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.eventTime.toEpochMilli())
                );

        PatternStream<Transaction> patternStream = CEP.pattern(
                stream.keyBy(t -> t.cardNumber),
                RapidSmallTransactionPattern.build(rule)
        );

        TestSink.results.clear();
        patternStream.select((PatternSelectFunction<Transaction, String>) pattern -> "MATCHED")
                .addSink(new TestSink());

        env.execute("Test CEP");

        assertTrue(TestSink.results.isEmpty(), "Should not have matched due to amount exceed");
    }

    private Transaction createTransaction(String card, double amount, long timestamp) {
        Transaction t = new Transaction();
        t.id = java.util.UUID.randomUUID().toString();
        t.cardNumber = card;
        t.amount = BigDecimal.valueOf(amount);
        t.eventTime = java.time.Instant.ofEpochMilli(timestamp);
        t.status = "SUCCESS";
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

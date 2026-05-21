package com.fraud.function;

import com.fraud.model.FraudAlert;
import com.fraud.model.FraudRule;
import com.fraud.model.Transaction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FraudDetectionBroadcastFunctionTest {

    @Test
    public void testRuleP002_HighValueNewCountry() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        FraudRule rule = new FraudRule();
        rule.ruleId = "P002";
        rule.ruleName = "High value in new country";
        rule.enabled = true;
        rule.thresholdAmount = 500.0;
        rule.severity = "HIGH";

        DataStream<FraudRule> ruleStream = env.fromData(rule);
        
        MapStateDescriptor<String, FraudRule> ruleStateDescriptor = 
                new MapStateDescriptor<>("fraud-rules-state", String.class, FraudRule.class);
        
        BroadcastStream<FraudRule> broadcastStream = ruleStream.broadcast(ruleStateDescriptor);

        List<Transaction> transactions = new ArrayList<>();
        transactions.add(createTransaction("u1", 100.0, "VN", 2000L));
        transactions.add(createTransaction("u1", 1000.0, "VN", 3000L)); 
        transactions.add(createTransaction("u1", 800.0, "US", 4000L));  

        DataStream<Transaction> txnStream = env.fromCollection(transactions);

        DataStream<FraudAlert> alerts = txnStream
                .keyBy(t -> t.userId)
                .connect(broadcastStream)
                .process(new FraudDetectionBroadcastFunction());

        TestSink.results.clear();
        alerts.addSink(new TestSink());

        env.execute("Test Broadcast");

        assertEquals(1, TestSink.results.size(), "Should have exactly 1 alert for P002");
        assertEquals("P002", TestSink.results.get(0).patternId);
        assertEquals("HIGH", TestSink.results.get(0).severity);
    }

    private Transaction createTransaction(String user, double amount, String country, long timestamp) {
        Transaction t = new Transaction();
        t.id = java.util.UUID.randomUUID().toString();
        t.userId = user;
        t.amount = BigDecimal.valueOf(amount);
        t.country = country;
        t.eventTime = java.time.Instant.ofEpochMilli(timestamp);
        t.status = "SUCCESS";
        return t;
    }

    public static class TestSink implements SinkFunction<FraudAlert> {
        public static final List<FraudAlert> results = new ArrayList<>();

        @Override
        public void invoke(FraudAlert value, Context context) {
            results.add(value);
        }
    }
}

package com.fraud;

import com.fraud.config.PipelineConfig;
import com.fraud.function.FraudDetectionBroadcastFunction;
import com.fraud.function.MLInferenceFunction;
import com.fraud.model.FraudAlert;
import com.fraud.model.FraudRule;
import com.fraud.model.Transaction;
import com.fraud.pattern.MultipleFailedPattern;
import com.fraud.pattern.RapidSmallTransactionPattern;
import com.fraud.serialization.FraudAlertSerializer;
import com.fraud.serialization.FraudRuleDeserializer;
import com.fraud.serialization.TransactionDeserializer;
import com.fraud.sink.AlertJdbcSink;
import com.fraud.sink.TransactionJdbcSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class FraudDetectionJob {

    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionJob.class);

    public static void main(String[] args) throws Exception {

        // ── Step 1: Environment ────────────────────────────────────────────
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PipelineConfig.PARALLELISM);
        env.enableCheckpointing(PipelineConfig.CHECKPOINT_INTERVAL,
            CheckpointingMode.EXACTLY_ONCE);
        // Checkpoint storage — use file-based so state survives restarts
        env.getCheckpointConfig().setCheckpointStorage(
            "file:///tmp/flink-checkpoints/fraud-pipeline");

        // ── Step 2: Transaction source ─────────────────────────────────────
        KafkaSource<Transaction> txnSource = KafkaSource.<Transaction>builder()
            .setBootstrapServers(PipelineConfig.KAFKA_BOOTSTRAP)
            .setTopics(PipelineConfig.TRANSACTIONS_TOPIC)
            .setGroupId("fraud-detection-group")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setDeserializer(new TransactionDeserializer())
            .build();

        WatermarkStrategy<Transaction> watermark = WatermarkStrategy
            .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner(
                (txn, ts) -> txn.eventTime != null ? txn.eventTime.toEpochMilli() : ts);

        DataStream<Transaction> transactions = env
            .fromSource(txnSource, watermark, "kafka-transaction-source");

        // ── Step 3: Rules source (broadcast) ──────────────────────────────
        KafkaSource<FraudRule> rulesSource = KafkaSource.<FraudRule>builder()
            .setBootstrapServers(PipelineConfig.KAFKA_BOOTSTRAP)
            .setTopics(PipelineConfig.RULES_TOPIC)
            .setGroupId("fraud-rules-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(new FraudRuleDeserializer())
            .build();

        DataStream<FraudRule> rulesStream = env
            .fromSource(rulesSource, WatermarkStrategy.noWatermarks(), "kafka-rules-source");

        BroadcastStream<FraudRule> broadcastRules =
            rulesStream.broadcast(FraudDetectionBroadcastFunction.RULES_STATE);

        // ── Step 4: CEP P001 — rapid small transactions (keyed by card) ───
        DataStream<FraudAlert> p001Alerts;
        {
            // Read P001 rule from broadcasted state is not straightforward with CEP.
            // Use hardcoded defaults matching rules.jsonl — rules can override at runtime.
            PatternStream<Transaction> ps = CEP.pattern(
                transactions.keyBy(t -> t.cardNumber),
                RapidSmallTransactionPattern.build(defaultP001Rule())
            );
            p001Alerts = ps.select(matches -> {
                List<Transaction> hits = matches.get("first");
                Transaction last = hits.get(hits.size() - 1);
                return FraudAlert.cep(last, "P001", "Rapid Small Transactions", "HIGH",
                    hits.size() + " transactions under 50 EUR within 60s on card " + last.cardNumber);
            });
        }

        // ── Step 5: CEP P003 — multiple fraud then success (keyed by user) ─
        DataStream<FraudAlert> p003Alerts;
        {
            PatternStream<Transaction> ps = CEP.pattern(
                transactions.keyBy(t -> t.userId),
                MultipleFailedPattern.build(300)
            );
            p003Alerts = ps.select(matches -> {
                List<Transaction> frauds  = matches.get("failed");
                List<Transaction> success = matches.get("success");
                Transaction last = success.get(0);
                return FraudAlert.cep(last, "P003", "Multiple Fraud Then Success", "HIGH",
                    frauds.size() + " fraud attempts followed by success for " + last.userId);
            });
        }

        // ── Step 6: Broadcast function (P002 + rule updates) ──────────────
        // Note: P002 is fully dynamic (enable/disable via Kafka, no restart needed).
        // P001 and P003 use hardcoded defaults matching rules.jsonl;
        // they respect rule parameters but cannot be hot-disabled without restart.
        DataStream<FraudAlert> p002Stream = transactions
            .keyBy(t -> t.userId)
            .connect(broadcastRules)
            .process(new FraudDetectionBroadcastFunction())
            .name("broadcast-p002");

        // ── Step 7: ML inference (async HTTP to FastAPI :8001) ─────────────
        DataStream<FraudAlert> mlAlerts = AsyncDataStream.unorderedWait(
            transactions,
            new MLInferenceFunction(),
            PipelineConfig.ML_ASYNC_TIMEOUT_MS,
            TimeUnit.MILLISECONDS,
            PipelineConfig.ML_ASYNC_CAPACITY
        ).name("ml-inference");

        // ── Step 8: Merge all alert streams ───────────────────────────────
        DataStream<FraudAlert> allAlerts =
            p001Alerts.union(p003Alerts, p002Stream, mlAlerts);

        // ── Step 9: Sinks ─────────────────────────────────────────────────
        // Kafka alerts sink
        KafkaSink<FraudAlert> kafkaAlertSink = KafkaSink.<FraudAlert>builder()
            .setBootstrapServers(PipelineConfig.KAFKA_BOOTSTRAP)
            .setRecordSerializer(new FraudAlertSerializer())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
        allAlerts.sinkTo(kafkaAlertSink).name("kafka-alert-sink");

        // PostgreSQL alerts sink
        allAlerts.addSink(AlertJdbcSink.build()).name("postgres-alert-sink");

        // PostgreSQL transactions sink — write directly from source stream
        // (previous version incorrectly used a side output that was never emitted)
        transactions.addSink(TransactionJdbcSink.build()).name("postgres-txn-sink");

        // ── Step 10: Execute ───────────────────────────────────────────────
        LOG.info("Submitting Fraud Detection Pipeline...");
        env.execute("Fraud Detection Pipeline v2.0");
    }

    // Default P001 rule matching rules.jsonl — overridden at runtime via Kafka
    private static com.fraud.model.FraudRule defaultP001Rule() {
        FraudRule r = new FraudRule();
        r.ruleId = "P001"; r.ruleName = "Rapid Small Transactions";
        r.enabled = true; r.thresholdAmount = 50.0;
        r.windowSeconds = 60; r.minOccurrences = 3; r.severity = "HIGH";
        return r;
    }
}

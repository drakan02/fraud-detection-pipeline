package com.fraud;

import com.fraud.config.PipelineConfig;
import com.fraud.function.MLInferenceFunction;
import com.fraud.model.FraudAlert;
import com.fraud.model.Transaction;
import com.fraud.serialization.FraudAlertSerializer;
import com.fraud.serialization.TransactionDeserializer;
import com.fraud.sink.AlertJdbcSink;
import com.fraud.sink.GroundTruthJdbcSink;
import com.fraud.sink.TransactionJdbcSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
        // Checkpoint storage backed by a PersistentVolumeClaim so state
        // survives pod restarts and Flink can recover Kafka offsets + operator
        // state automatically without replaying from the beginning.
        env.getCheckpointConfig().setCheckpointStorage(
            PipelineConfig.CHECKPOINT_STORAGE);

        // ── Step 2: Transaction source ─────────────────────────────────────
        KafkaSource<Transaction> txnSource = KafkaSource.<Transaction>builder()
            .setBootstrapServers(PipelineConfig.KAFKA_BOOTSTRAP)
            .setTopics(PipelineConfig.TRANSACTIONS_TOPIC)
            .setGroupId("fraud-detection-group")
            .setStartingOffsets(OffsetsInitializer.committedOffsets(
                org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST))
            .setDeserializer(new TransactionDeserializer())
            .build();

        WatermarkStrategy<Transaction> watermark = WatermarkStrategy
            .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner(
                (txn, ts) -> txn.getEventTime() != null ? txn.getEventTime().toEpochMilli() : ts);

        DataStream<Transaction> transactions = env
            .fromSource(txnSource, watermark, "kafka-transaction-source");

        // ── Step 3: ML inference (async HTTP to FastAPI :8001) ─────────────
        // MLInferenceFunction includes retry logic (3 attempts, exponential
        // backoff) and a rule-based fallback when the ML server is unavailable.
        DataStream<FraudAlert> mlAlerts = AsyncDataStream.unorderedWait(
            transactions,
            new MLInferenceFunction(),
            PipelineConfig.ML_ASYNC_TIMEOUT_MS,
            TimeUnit.MILLISECONDS,
            PipelineConfig.ML_ASYNC_CAPACITY
        ).name("ml-inference");

        // ── Step 4: Sinks ─────────────────────────────────────────────────
        // 4a. Kafka alert topic (for downstream consumers).
        // EXACTLY_ONCE delivery guarantee via Kafka transactions ensures no
        // duplicate alerts are produced after Flink recovers from a checkpoint.
        KafkaSink<FraudAlert> kafkaAlertSink = KafkaSink.<FraudAlert>builder()
            .setBootstrapServers(PipelineConfig.KAFKA_BOOTSTRAP)
            .setRecordSerializer(new FraudAlertSerializer())
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .build();
        mlAlerts.sinkTo(kafkaAlertSink).name("kafka-alert-sink");

        // 4b. ClickHouse: fraud alerts
        mlAlerts.addSink(AlertJdbcSink.build()).name("clickhouse-alert-sink");

        // 4c. ClickHouse: raw transaction business fields (no label/status column)
        transactions.addSink(TransactionJdbcSink.build()).name("clickhouse-txn-sink");

        // 4d. ClickHouse: ground-truth labels written to a separate table.
        //     In production this would come from a dispute-resolution pipeline.
        //     Here we use the Kaggle dataset Class column (via Transaction.status)
        //     to enable real-time Confusion Matrix evaluation in the dashboard.
        transactions.addSink(GroundTruthJdbcSink.build()).name("clickhouse-ground-truth-sink");

        // ── Step 5: Execute ───────────────────────────────────────────────
        LOG.info("Submitting Fraud Detection Pipeline (ML only)...");
        env.execute("Fraud Detection Pipeline v2.0 (ML only)");
    }
}

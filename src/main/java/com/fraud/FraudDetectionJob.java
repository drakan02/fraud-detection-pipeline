package com.fraud;

import com.fraud.config.PipelineConfig;
import com.fraud.function.MLInferenceFunction;
import com.fraud.model.FraudAlert;
import com.fraud.model.Transaction;
import com.fraud.serialization.FraudAlertSerializer;
import com.fraud.serialization.TransactionDeserializer;
import com.fraud.sink.AlertJdbcSink;
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
        // Checkpoint storage — use file-based so state survives restarts
        env.getCheckpointConfig().setCheckpointStorage(
            PipelineConfig.CHECKPOINT_STORAGE);

        // ── Step 2: Transaction source ─────────────────────────────────────
        KafkaSource<Transaction> txnSource = KafkaSource.<Transaction>builder()
            .setBootstrapServers(PipelineConfig.KAFKA_BOOTSTRAP)
            .setTopics(PipelineConfig.TRANSACTIONS_TOPIC)
            .setGroupId("fraud-detection-group")
            .setStartingOffsets(OffsetsInitializer.committedOffsets(org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST))
            .setDeserializer(new TransactionDeserializer())
            .build();

        WatermarkStrategy<Transaction> watermark = WatermarkStrategy
            .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner(
                (txn, ts) -> txn.eventTime != null ? txn.eventTime.toEpochMilli() : ts);

        DataStream<Transaction> transactions = env
            .fromSource(txnSource, watermark, "kafka-transaction-source");

        // ── Step 3: ML inference (async HTTP to FastAPI :8001) ─────────────
        DataStream<FraudAlert> mlAlerts = AsyncDataStream.unorderedWait(
            transactions,
            new MLInferenceFunction(),
            PipelineConfig.ML_ASYNC_TIMEOUT_MS,
            TimeUnit.MILLISECONDS,
            PipelineConfig.ML_ASYNC_CAPACITY
        ).name("ml-inference");

        // ── Step 4: Sinks ─────────────────────────────────────────────────
        KafkaSink<FraudAlert> kafkaAlertSink = KafkaSink.<FraudAlert>builder()
            .setBootstrapServers(PipelineConfig.KAFKA_BOOTSTRAP)
            .setRecordSerializer(new FraudAlertSerializer())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
        mlAlerts.sinkTo(kafkaAlertSink).name("kafka-alert-sink");

        mlAlerts.addSink(AlertJdbcSink.build()).name("clickhouse-alert-sink");

        transactions.addSink(TransactionJdbcSink.build()).name("clickhouse-txn-sink");

        // ── Step 5: Execute ───────────────────────────────────────────────
        LOG.info("Submitting Fraud Detection Pipeline (ML only)...");
        env.execute("Fraud Detection Pipeline v2.0 (ML only)");
    }
}

package com.fraud.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fraud.model.FraudAlert;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fraud.config.PipelineConfig;

/**
 * Serializes {@link FraudAlert} objects to Kafka ProducerRecords.
 *
 * <p>The transaction ID is used as the Kafka message key so that all alerts
 * for the same transaction are routed to the same partition, preserving
 * ordering for downstream consumers.</p>
 *
 * <p>{@link ObjectMapper} is thread-safe for reads after configuration and is
 * shared across calls within the same operator instance (one instance per
 * parallel subtask). A new instance is created in {@link #open} (not a static
 * field) to avoid serialization issues with Flink's distributed checkpointing.</p>
 */
public class FraudAlertSerializer
        implements KafkaRecordSerializationSchema<FraudAlert> {

    private transient ObjectMapper mapper;

    @Override
    public void open(SerializationSchema.InitializationContext ctx,
                     KafkaSinkContext sinkCtx) {
        mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            FraudAlert alert, KafkaSinkContext ctx, Long timestamp) {
        try {
            byte[] key   = (alert.getTransactionId() != null
                            ? alert.getTransactionId() : "unknown").getBytes();
            byte[] value = mapper.writeValueAsBytes(alert);
            return new ProducerRecord<>(PipelineConfig.ALERTS_TOPIC, key, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize FraudAlert", e);
        }
    }
}

package com.fraud.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fraud.model.FraudAlert;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fraud.config.PipelineConfig;

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
            byte[] key   = alert.userId.getBytes();
            byte[] value = mapper.writeValueAsBytes(alert);
            return new ProducerRecord<>(PipelineConfig.ALERTS_TOPIC, key, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize FraudAlert", e);
        }
    }
}

package com.fraud.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fraud.model.FraudRule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FraudRuleDeserializer
        implements KafkaRecordDeserializationSchema<FraudRule> {

    private static final Logger LOG = LoggerFactory.getLogger(FraudRuleDeserializer.class);
    private transient ObjectMapper mapper;

    @Override
    public void open(DeserializationSchema.InitializationContext ctx) {
        mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record,
                            Collector<FraudRule> out) {
        if (record.value() == null) return;
        try {
            FraudRule rule = mapper.readValue(record.value(), FraudRule.class);
            if (rule.ruleId != null) {
                out.collect(rule);
            }
        } catch (Exception e) {
            LOG.warn("Malformed fraud rule dropped: {}", e.getMessage());
        }
    }

    @Override
    public TypeInformation<FraudRule> getProducedType() {
        return TypeInformation.of(FraudRule.class);
    }
}

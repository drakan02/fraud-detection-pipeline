package com.fraud.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fraud.model.Transaction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionDeserializer
        implements KafkaRecordDeserializationSchema<Transaction> {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionDeserializer.class);
    private transient ObjectMapper mapper;

    @Override
    public void open(DeserializationSchema.InitializationContext ctx) {
        mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record,
                            Collector<Transaction> out) {
        if (record.value() == null) return;
        try {
            Transaction txn = mapper.readValue(record.value(), Transaction.class);
            if (txn.id != null && txn.amount != null) {
                out.collect(txn);
            }
        } catch (Exception e) {
            LOG.warn("Malformed transaction dropped: {}", e.getMessage());
        }
    }

    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeInformation.of(Transaction.class);
    }
}

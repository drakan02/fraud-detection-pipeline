package com.fraud.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fraud.model.Transaction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deserializes Kafka records into Transaction objects.
 *
 * <p>Exposes two Flink Counters (visible in Flink Web UI and Prometheus reporter):
 * <ul>
 *   <li>{@code fraud_pipeline.valid_messages_total}</li>
 *   <li>{@code fraud_pipeline.malformed_messages_total}</li>
 * </ul>
 * </p>
 *
 * <p>{@link ObjectMapper} is configured once in {@link #open} and then used
 * read-only across deserialization calls, making it safe for concurrent access
 * within the same operator instance.</p>
 */
public class TransactionDeserializer
        implements KafkaRecordDeserializationSchema<Transaction> {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionDeserializer.class);

    private transient ObjectMapper mapper;
    private transient Counter validCounter;
    private transient Counter malformedCounter;

    @Override
    public void open(DeserializationSchema.InitializationContext ctx) {
        mapper = new ObjectMapper().registerModule(new JavaTimeModule());

        // Register metrics under the operator's MetricGroup
        MetricGroup group = ctx.getMetricGroup()
                               .addGroup("fraud_pipeline");
        validCounter     = group.counter("valid_messages_total");
        malformedCounter = group.counter("malformed_messages_total");

        LOG.info("TransactionDeserializer initialised — metrics registered");
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record,
                            Collector<Transaction> out) {
        if (record.value() == null) {
            malformedCounter.inc();
            LOG.warn("Received null Kafka record — incrementing malformed counter");
            return;
        }
        try {
            Transaction txn = mapper.readValue(record.value(), Transaction.class);
            if (txn.getId() != null && txn.getAmount() != null) {
                if (txn.getEventTime() == null) {
                    long ts = record.timestamp();
                    if (ts <= 0) {
                        ts = System.currentTimeMillis();
                    }
                    txn.setEventTime(java.time.Instant.ofEpochMilli(ts));
                }
                validCounter.inc();
                out.collect(txn);
            } else {
                malformedCounter.inc();
                LOG.warn("Transaction missing required fields (id={}, amount={}) — dropped",
                         txn.getId(), txn.getAmount());
            }
        } catch (Exception e) {
            malformedCounter.inc();
            LOG.warn("Malformed transaction dropped: {}", e.getMessage());
        }
    }

    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeInformation.of(Transaction.class);
    }
}

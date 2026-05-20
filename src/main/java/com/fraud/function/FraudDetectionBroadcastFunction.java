package com.fraud.function;

import com.fraud.model.FraudAlert;
import com.fraud.model.FraudRule;
import com.fraud.model.Transaction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class FraudDetectionBroadcastFunction
        extends KeyedBroadcastProcessFunction<String, Transaction, FraudRule, FraudAlert> {

    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionBroadcastFunction.class);

    public static final MapStateDescriptor<String, FraudRule> RULES_STATE =
        new MapStateDescriptor<>("fraud-rules-state", String.class, FraudRule.class);

    // Per-user state: known countries (for P002)
    private transient ValueState<Set<String>> knownCountriesState;

    @Override
    public void open(Configuration parameters) {
        knownCountriesState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("known-countries-set",
                org.apache.flink.api.common.typeinfo.TypeInformation
                    .of(new org.apache.flink.api.common.typeinfo.TypeHint<Set<String>>() {}))
        );
    }

    @Override
    public void processBroadcastElement(
            FraudRule rule,
            KeyedBroadcastProcessFunction<String, Transaction, FraudRule, FraudAlert>.Context ctx,
            Collector<FraudAlert> out) throws Exception {

        var broadcastState = ctx.getBroadcastState(RULES_STATE);
        if (rule.enabled) {
            broadcastState.put(rule.ruleId, rule);
            LOG.info("Rule loaded: {} ({}) enabled=true", rule.ruleId, rule.ruleName);
        } else {
            broadcastState.remove(rule.ruleId);
            LOG.info("Rule removed: {} enabled=false", rule.ruleId);
        }
    }

    @Override
    public void processElement(
            Transaction txn,
            KeyedBroadcastProcessFunction<String, Transaction, FraudRule, FraudAlert>.ReadOnlyContext ctx,
            Collector<FraudAlert> out) throws Exception {

        var broadcastState = ctx.getBroadcastState(RULES_STATE);
        FraudRule p002 = broadcastState.get("P002");

        // P002: high-value transaction in a country never seen before for this user
        if (p002 != null && txn.country != null
                && txn.amount.doubleValue() > p002.thresholdAmount) {

            Set<String> known = knownCountriesState.value();
            if (known == null) known = new HashSet<>();

            if (!known.contains(txn.country)) {
                out.collect(FraudAlert.cep(txn,
                    "P002", p002.ruleName, p002.severity,
                    String.format("%.2f EUR in new country '%s' for user %s",
                        txn.amount.doubleValue(), txn.country, txn.userId)));
                LOG.info("FRAUD_ALERT | P002 | userId={} | txnId={} | country={}",
                    txn.userId, txn.id, txn.country);
            }
            known.add(txn.country);
            knownCountriesState.update(known);
        } else if (txn.country != null) {
            // Still track country even for low-value transactions
            Set<String> known = knownCountriesState.value();
            if (known == null) known = new HashSet<>();
            known.add(txn.country);
            knownCountriesState.update(known);
        }
    }
}

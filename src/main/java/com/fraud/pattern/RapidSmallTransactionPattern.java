package com.fraud.pattern;

import com.fraud.model.FraudRule;
import com.fraud.model.Transaction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.math.BigDecimal;

public class RapidSmallTransactionPattern {

    /**
     * P001: 3+ transactions under $50 from the same card within 60 seconds.
     * Keyed by: cardNumber
     */
    public static Pattern<Transaction, ?> build(FraudRule rule) {
        return Pattern.<Transaction>begin("first")
            .where(new SimpleCondition<Transaction>() {
                @Override
                public boolean filter(Transaction t) {
                    return t.amount.compareTo(BigDecimal.valueOf(rule.thresholdAmount)) < 0
                        && "SUCCESS".equals(t.status);
                }
            })
            .times(rule.minOccurrences)
            .within(Time.seconds(rule.windowSeconds));
    }
}

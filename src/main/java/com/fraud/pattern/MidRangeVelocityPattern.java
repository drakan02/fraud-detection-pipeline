package com.fraud.pattern;

import com.fraud.model.FraudRule;
import com.fraud.model.Transaction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.math.BigDecimal;

public class MidRangeVelocityPattern {

    /**
     * P004: Mid-Range Velocity.
     * minOccurrences+ transactions between thresholdAmount and thresholdAmountHigh within windowSeconds.
     * Keyed by: cardNumber
     */
    public static Pattern<Transaction, ?> build(FraudRule rule) {
        return Pattern.<Transaction>begin("first")
            .where(new SimpleCondition<Transaction>() {
                @Override
                public boolean filter(Transaction t) {
                    boolean aboveLow = t.amount.compareTo(BigDecimal.valueOf(rule.thresholdAmount)) >= 0;
                    boolean belowHigh = true;
                    if (rule.thresholdAmountHigh != null) {
                        belowHigh = t.amount.compareTo(BigDecimal.valueOf(rule.thresholdAmountHigh)) <= 0;
                    }
                    return aboveLow && belowHigh && "SUCCESS".equals(t.status);
                }
            })
            .times(rule.minOccurrences)
            .within(Time.seconds(rule.windowSeconds));
    }
}

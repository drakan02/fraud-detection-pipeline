package com.fraud.pattern;

import com.fraud.model.Transaction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;

public class MultipleFailedPattern {

    /**
     * P003: 2+ FRAUD transactions followed by 1 SUCCESS within 5 minutes.
     * Keyed by: userId
     * Note: with Kaggle data, status="FRAUD" maps to Class=1 rows.
     */
    public static Pattern<Transaction, ?> build(int windowSeconds) {
        return Pattern.<Transaction>begin("failed")
            .where(new SimpleCondition<Transaction>() {
                @Override
                public boolean filter(Transaction t) {
                    return "FRAUD".equals(t.status);
                }
            })
            .times(2)
            .followedBy("success")
            .where(new SimpleCondition<Transaction>() {
                @Override
                public boolean filter(Transaction t) {
                    return "SUCCESS".equals(t.status);
                }
            })
            .within(Time.seconds(windowSeconds));
    }
}

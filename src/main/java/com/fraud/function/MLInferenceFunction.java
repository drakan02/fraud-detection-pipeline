package com.fraud.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fraud.config.PipelineConfig;
import com.fraud.model.FraudAlert;
import com.fraud.model.Transaction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Async ML inference function that calls the FastAPI /predict endpoint.
 *
 * <p><strong>Retry strategy:</strong> up to {@value MAX_RETRIES} attempts with
 * exponential backoff (100 ms → 200 ms → 400 ms) before falling back.</p>
 *
 * <p><strong>Circuit-breaker / fallback:</strong> If the ML server is
 * unavailable after all retries, a simple rule-based heuristic is applied
 * (Amount > FALLBACK_AMOUNT_THRESHOLD EUR → HIGH-risk alert with
 * {@code source="RULE_FALLBACK"}). This ensures the pipeline never silently
 * drops suspicious large-amount transactions when ML is down.</p>
 */
public class MLInferenceFunction extends RichAsyncFunction<Transaction, FraudAlert> {

    private static final Logger LOG = LoggerFactory.getLogger(MLInferenceFunction.class);
    private static final String ENDPOINT = PipelineConfig.MODEL_SERVER_URL + "/predict";

    /** Amount threshold for the rule-based fallback when ML server is down. */
    private static final double FALLBACK_AMOUNT_THRESHOLD = 2000.0;

    /** Maximum number of HTTP retry attempts per transaction. */
    private static final int MAX_RETRIES = 3;

    /** Base backoff delay in milliseconds (doubles on each retry). */
    private static final long BACKOFF_BASE_MS = 100L;

    private transient CloseableHttpAsyncClient httpClient;
    private transient ObjectMapper mapper;
    private transient ScheduledExecutorService executorService;

    /** Tracks consecutive ML failures for logging/alerting purposes. */
    private transient AtomicInteger consecutiveFailures;

    @Override
    public void open(Configuration parameters) throws Exception {
        ConnectionConfig connectionConfig = ConnectionConfig.custom()
            .setConnectTimeout(Timeout.ofSeconds(4))
            .setSocketTimeout(Timeout.ofSeconds(4))
            .build();

        RequestConfig requestConfig = RequestConfig.custom()
            .setConnectionRequestTimeout(Timeout.ofSeconds(4))
            .setResponseTimeout(Timeout.ofSeconds(4))
            .build();

        PoolingAsyncClientConnectionManager connManager =
            PoolingAsyncClientConnectionManagerBuilder.create()
                .setMaxConnPerRoute(PipelineConfig.ML_ASYNC_CAPACITY)
                .setMaxConnTotal(PipelineConfig.ML_ASYNC_CAPACITY * 2)
                .setDefaultConnectionConfig(connectionConfig)
                .build();

        httpClient = HttpAsyncClients.custom()
            .setConnectionManager(connManager)
            .setDefaultRequestConfig(requestConfig)
            .build();
        httpClient.start();
        executorService = Executors.newSingleThreadScheduledExecutor();
        mapper = new ObjectMapper();
        consecutiveFailures = new AtomicInteger(0);

        LOG.info("MLInferenceFunction ready — endpoint: {} (pool: max/route={}, max/total={})",
            ENDPOINT, PipelineConfig.ML_ASYNC_CAPACITY, PipelineConfig.ML_ASYNC_CAPACITY * 2);
    }

    @Override
    public void asyncInvoke(Transaction txn, ResultFuture<FraudAlert> future) {
        if (txn.mlFeatures == null || txn.mlFeatures.isEmpty()) {
            future.complete(Collections.emptyList());
            return;
        }
        // Start with attempt 0; retry handler will recurse on failure.
        executeWithRetry(txn, future, 0);
    }

    /**
     * Sends an async HTTP POST to /predict. On failure, backs off and retries
     * up to MAX_RETRIES times. After exhausting retries, applies the
     * rule-based fallback.
     */
    private void executeWithRetry(Transaction txn, ResultFuture<FraudAlert> future, int attempt) {
        try {
            Map<String, Object> payload = new java.util.HashMap<>();
            payload.put("transaction_id", txn.id);
            payload.put("features", txn.mlFeatures);
            String body = mapper.writeValueAsString(payload);

            SimpleHttpRequest req = SimpleRequestBuilder.post(ENDPOINT)
                .setHeader("Content-Type", "application/json")
                .setBody(body, ContentType.APPLICATION_JSON)
                .build();

            httpClient.execute(req, new FutureCallback<SimpleHttpResponse>() {
                @Override
                public void completed(SimpleHttpResponse resp) {
                    int statusCode = resp.getCode();
                    if (statusCode != 200) {
                        LOG.error("ML server returned HTTP {} for txn={}: {}",
                            statusCode, txn.id, resp.getBodyText());
                        handleFailure(txn, future, attempt,
                            new RuntimeException("HTTP " + statusCode));
                        return;
                    }
                    try {
                        consecutiveFailures.set(0);  // reset on success
                        Map<?, ?> result = mapper.readValue(resp.getBodyText(), Map.class);
                        double prob = ((Number) result.get("fraud_probability")).doubleValue();
                        boolean isFraud = (Boolean) result.get("is_fraud");
                        if (isFraud) {
                            LOG.info("FRAUD_ALERT | ML001 | userId={} | prob={}",
                                txn.userId, String.format("%.3f", prob));
                            future.complete(Collections.singletonList(
                                FraudAlert.ml(txn, prob)));
                        } else {
                            future.complete(Collections.emptyList());
                        }
                    } catch (Exception e) {
                        LOG.error("ML response parse error txn={}: {}", txn.id, e.getMessage());
                        future.complete(Collections.emptyList());
                    }
                }

                @Override
                public void failed(Exception e) {
                    handleFailure(txn, future, attempt, e);
                }

                @Override
                public void cancelled() {
                    future.complete(Collections.emptyList());
                }
            });
        } catch (Exception e) {
            LOG.error("ML asyncInvoke serialisation error: {}", e.getMessage());
            future.complete(Collections.emptyList());
        }
    }

    /**
     * On a failed attempt: retry with backoff if attempts remain, otherwise
     * apply the rule-based fallback so large transactions are never silently
     * dropped when ML is unavailable.
     */
    private void handleFailure(Transaction txn, ResultFuture<FraudAlert> future,
                               int attempt, Exception cause) {
        int failures = consecutiveFailures.incrementAndGet();
        if (attempt < MAX_RETRIES) {
            long backoffMs = BACKOFF_BASE_MS * (1L << attempt); // 100, 200, 400 ms
            LOG.warn("ML inference failed for txn={} (attempt {}/{}), retrying in {}ms: {}",
                txn.id, attempt + 1, MAX_RETRIES, backoffMs, cause.getMessage());
            executorService.schedule(
                () -> executeWithRetry(txn, future, attempt + 1),
                backoffMs,
                TimeUnit.MILLISECONDS
            );
        } else {
            // All retries exhausted — apply rule-based fallback
            if (failures % 100 == 1) {
                LOG.error("ML server unreachable after {} retries ({} consecutive failures). " +
                    "Applying rule-based fallback for txn={}.", MAX_RETRIES, failures, txn.id);
            }
            future.complete(applyRuleBasedFallback(txn));
        }
    }

    /**
     * Simple rule-based fallback: flag transactions above the amount threshold
     * as HIGH risk with source="RULE_FALLBACK" so they are not silently dropped.
     */
    private java.util.List<FraudAlert> applyRuleBasedFallback(Transaction txn) {
        if (txn.amount != null &&
            txn.amount.compareTo(BigDecimal.valueOf(FALLBACK_AMOUNT_THRESHOLD)) > 0) {
            LOG.warn("RULE_FALLBACK | txn={} | amount={} > threshold={}",
                txn.id, txn.amount, FALLBACK_AMOUNT_THRESHOLD);
            FraudAlert alert = FraudAlert.ruleFallback(txn);
            return Collections.singletonList(alert);
        }
        return Collections.emptyList();
    }

    @Override
    public void timeout(Transaction input, ResultFuture<FraudAlert> resultFuture) throws Exception {
        LOG.warn("Async ML inference timed out ({} ms) for transaction={}. Applying fallback.",
            PipelineConfig.ML_ASYNC_TIMEOUT_MS, input.id);
        resultFuture.complete(applyRuleBasedFallback(input));
    }

    @Override
    public void close() throws Exception {
        if (executorService != null) {
            executorService.shutdown();
        }
        if (httpClient != null) {
            httpClient.close();
        }
    }
}

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

import java.util.Collections;
import java.util.Map;

public class MLInferenceFunction extends RichAsyncFunction<Transaction, FraudAlert> {

    private static final Logger LOG = LoggerFactory.getLogger(MLInferenceFunction.class);
    private static final String ENDPOINT = PipelineConfig.MODEL_SERVER_URL + "/predict";

    private transient CloseableHttpAsyncClient httpClient;
    private transient ObjectMapper mapper;

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

        PoolingAsyncClientConnectionManager connManager = PoolingAsyncClientConnectionManagerBuilder.create()
            .setMaxConnPerRoute(PipelineConfig.ML_ASYNC_CAPACITY)
            .setMaxConnTotal(PipelineConfig.ML_ASYNC_CAPACITY * 2)
            .setDefaultConnectionConfig(connectionConfig)
            .build();

        httpClient = HttpAsyncClients.custom()
            .setConnectionManager(connManager)
            .setDefaultRequestConfig(requestConfig)
            .build();
        httpClient.start();
        mapper = new ObjectMapper();
        LOG.info("MLInferenceFunction ready — endpoint: {} (connection pool configured: max per route {}, max total {})",
            ENDPOINT, PipelineConfig.ML_ASYNC_CAPACITY, PipelineConfig.ML_ASYNC_CAPACITY * 2);
    }

    @Override
    public void asyncInvoke(Transaction txn, ResultFuture<FraudAlert> future) {
        // Skip if no ML features (e.g. injected test events)
        if (txn.mlFeatures == null || txn.mlFeatures.isEmpty()) {
            future.complete(Collections.emptyList());
            return;
        }
        try {
            String body = mapper.writeValueAsString(txn.mlFeatures);
            SimpleHttpRequest req = SimpleRequestBuilder.post(ENDPOINT)
                .setHeader("Content-Type", "application/json")
                .setBody(body, ContentType.APPLICATION_JSON)
                .build();

            httpClient.execute(req, new FutureCallback<SimpleHttpResponse>() {
                @Override
                public void completed(SimpleHttpResponse resp) {
                    try {
                        Map<?, ?> result = mapper.readValue(resp.getBodyText(), Map.class);
                        double prob = ((Number) result.get("fraud_probability")).doubleValue();
                        if (prob >= PipelineConfig.ML_FRAUD_THRESHOLD) {
                            LOG.info("FRAUD_ALERT | ML001 | userId={} | prob={}",
                                txn.userId, String.format("%.3f", prob));
                            future.complete(Collections.singletonList(FraudAlert.ml(txn, prob)));
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
                    // Model server down — do NOT fail the Flink task, just skip ML check
                    LOG.warn("ML inference unavailable for txn={}: {}", txn.id, e.getMessage());
                    future.complete(Collections.emptyList());
                }

                @Override
                public void cancelled() {
                    future.complete(Collections.emptyList());
                }
            });
        } catch (Exception e) {
            LOG.error("ML asyncInvoke error: {}", e.getMessage());
            future.complete(Collections.emptyList());
        }
    }

    @Override
    public void close() throws Exception {
        if (httpClient != null) httpClient.close();
    }
}

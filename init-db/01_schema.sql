CREATE TABLE IF NOT EXISTS transactions (
    id              VARCHAR(36)    PRIMARY KEY,
    user_id         VARCHAR(50)    NOT NULL,
    card_number     VARCHAR(30)    NOT NULL,
    amount          DECIMAL(12,2)  NOT NULL,
    currency        CHAR(3)        NOT NULL,
    merchant_id     VARCHAR(50),
    country         CHAR(2),
    status          VARCHAR(20)    NOT NULL,
    event_time      TIMESTAMP      NOT NULL,
    ingested_at     TIMESTAMP      DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    id              VARCHAR(36)    PRIMARY KEY,
    transaction_id  VARCHAR(36)    NOT NULL,
    user_id         VARCHAR(50)    NOT NULL,
    pattern_id      VARCHAR(10)    NOT NULL,
    pattern_name    VARCHAR(100)   NOT NULL,
    severity        VARCHAR(10)    NOT NULL,
    amount          DECIMAL(12,2)  NOT NULL,
    description     TEXT,
    source          VARCHAR(10)    NOT NULL DEFAULT 'CEP',
    ml_probability  DECIMAL(6,4),
    detected_at     TIMESTAMP      DEFAULT NOW()
);

CREATE INDEX idx_fraud_alerts_user     ON fraud_alerts(user_id);
CREATE INDEX idx_fraud_alerts_time     ON fraud_alerts(detected_at);
CREATE INDEX idx_fraud_alerts_source   ON fraud_alerts(source);
CREATE INDEX idx_txn_user              ON transactions(user_id);
CREATE INDEX idx_txn_event_time        ON transactions(event_time);

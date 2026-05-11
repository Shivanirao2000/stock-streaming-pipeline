CREATE TABLE IF NOT EXISTS stock_prices (
    id          SERIAL PRIMARY KEY,
    ticker      TEXT            NOT NULL,
    price       FLOAT           NOT NULL,
    rolling_avg FLOAT,
    ts          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sp_ticker_ts ON stock_prices (ticker, ts DESC);

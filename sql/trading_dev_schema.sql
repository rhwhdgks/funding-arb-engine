-- trading_dev minimal schema draft
-- 목적:
-- 1) trade_history를 체결 truth source로 단순화
-- 2) snapshot 계열은 bucket_start_ms 기반으로 정리
-- 3) 모든 시각은 UTC epoch milliseconds(BIGINT)로만 저장
-- 4) 운영상 보류된 order_events / pair_ledger는 포함하지 않음

CREATE TABLE IF NOT EXISTS balance_snapshot (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    account_label VARCHAR(16) NOT NULL DEFAULT 'test',
    logic VARCHAR(64) NOT NULL DEFAULT 'funding fee',
    exchange_name VARCHAR(32) NOT NULL,
    bucket_start_ms BIGINT UNSIGNED NOT NULL,
    desktop_time_ms BIGINT UNSIGNED NOT NULL,
    server_time_ms BIGINT UNSIGNED NULL,

    tracked_symbols_json JSON NOT NULL,
    symbol_balances_json JSON NOT NULL,

    cash_usdt DECIMAL(28, 12) NOT NULL DEFAULT 0,
    available_cash_usdt DECIMAL(28, 12) NOT NULL DEFAULT 0,
    locked_cash_usdt DECIMAL(28, 12) NOT NULL DEFAULT 0,
    total_equity_usdt DECIMAL(28, 12) NOT NULL DEFAULT 0,
    unrealized_pnl_usdt DECIMAL(28, 12) NOT NULL DEFAULT 0,

    raw_json LONGTEXT NULL,

    PRIMARY KEY (id),
    UNIQUE KEY uq_balance_snapshot_context_exchange_bucket (account_label, logic, exchange_name, bucket_start_ms),
    KEY idx_balance_snapshot_bucket (bucket_start_ms),
    KEY idx_balance_snapshot_context_bucket (account_label, logic, bucket_start_ms),
    KEY idx_balance_snapshot_exchange_desktop (exchange_name, desktop_time_ms),
    KEY idx_balance_snapshot_exchange_server (exchange_name, server_time_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS order_history (
    account_label VARCHAR(16) NOT NULL DEFAULT 'test',
    logic VARCHAR(64) NOT NULL DEFAULT 'funding fee',
    uuid VARCHAR(128) NOT NULL,
    exchange_name VARCHAR(32) NOT NULL,
    symbol VARCHAR(32) NOT NULL,

    order_id VARCHAR(128) NULL,
    client_order_id VARCHAR(128) NULL,

    side VARCHAR(8) NOT NULL,
    position_side VARCHAR(16) NULL,
    reduce_only TINYINT(1) NOT NULL DEFAULT 0,
    order_type VARCHAR(16) NOT NULL,

    price DECIMAL(28, 12) NULL,
    qty DECIMAL(38, 18) NOT NULL,
    executed_qty DECIMAL(38, 18) NOT NULL DEFAULT 0,
    avg_price DECIMAL(28, 12) NULL,

    status VARCHAR(32) NOT NULL,

    order_start BIGINT UNSIGNED NOT NULL,
    order_end BIGINT UNSIGNED NOT NULL,
    desktop_time_ms BIGINT UNSIGNED NOT NULL,
    server_time_ms BIGINT UNSIGNED NULL,

    raw_json LONGTEXT NULL,

    PRIMARY KEY (account_label, logic, uuid),
    KEY idx_order_history_context_time (account_label, logic, order_start),
    KEY idx_order_history_exchange_symbol_last_seen (exchange_name, symbol, order_end),
    KEY idx_order_history_exchange_client_order_id (exchange_name, client_order_id),
    KEY idx_order_history_exchange_order_id (exchange_name, order_id),
    KEY idx_order_history_symbol_status (exchange_name, symbol, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS trade_history (
    account_label VARCHAR(16) NOT NULL DEFAULT 'test',
    logic VARCHAR(64) NOT NULL DEFAULT 'funding fee',
    uuid VARCHAR(128) NOT NULL,
    exchange_name VARCHAR(32) NOT NULL,
    symbol VARCHAR(32) NOT NULL,

    order_uuid VARCHAR(128) NULL,
    order_id VARCHAR(128) NULL,
    client_order_id VARCHAR(128) NULL,
    trade_id VARCHAR(128) NULL,

    side VARCHAR(8) NOT NULL,
    role VARCHAR(16) NULL,
    position_side VARCHAR(16) NULL,

    price DECIMAL(28, 12) NOT NULL,
    qty DECIMAL(38, 18) NOT NULL,
    quote_qty DECIMAL(28, 12) NULL,

    fee DECIMAL(28, 12) NULL,
    fee_ccy VARCHAR(16) NULL,
    realized_pnl_usdt DECIMAL(28, 12) NULL,

    desktop_time_ms BIGINT UNSIGNED NOT NULL,
    server_time_ms BIGINT UNSIGNED NULL,

    raw_json LONGTEXT NULL,

    PRIMARY KEY (account_label, logic, uuid),
    KEY idx_trade_history_context_time (account_label, logic, desktop_time_ms),
    KEY idx_trade_history_exchange_symbol_server_time (exchange_name, symbol, server_time_ms),
    KEY idx_trade_history_exchange_symbol_desktop_time (exchange_name, symbol, desktop_time_ms),
    KEY idx_trade_history_order_uuid (order_uuid),
    KEY idx_trade_history_exchange_order_id (exchange_name, order_id),
    KEY idx_trade_history_exchange_client_order_id (exchange_name, client_order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS symbol_price (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    exchange_name VARCHAR(32) NOT NULL,
    symbol VARCHAR(32) NOT NULL,

    bucket_start_ms BIGINT UNSIGNED NOT NULL,
    desktop_time_ms BIGINT UNSIGNED NOT NULL,
    server_time_ms BIGINT UNSIGNED NULL,

    best_bid DECIMAL(28, 12) NOT NULL,
    best_ask DECIMAL(28, 12) NOT NULL,

    bid DECIMAL(28, 12) NULL,
    ask DECIMAL(28, 12) NULL,

    PRIMARY KEY (id),
    UNIQUE KEY uq_symbol_price_exchange_symbol_bucket_notional (
        exchange_name,
        symbol,
        bucket_start_ms
    ),
    KEY idx_symbol_price_exchange_symbol_bucket (exchange_name, symbol, bucket_start_ms),
    KEY idx_symbol_price_exchange_symbol_desktop (exchange_name, symbol, desktop_time_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS funding_rate (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    exchange_name VARCHAR(32) NOT NULL,
    symbol VARCHAR(32) NOT NULL,

    bucket_start_ms BIGINT UNSIGNED NOT NULL,
    desktop_time_ms BIGINT UNSIGNED NOT NULL,
    server_time_ms BIGINT UNSIGNED NULL,

    funding_rate DECIMAL(18, 10) NOT NULL,
    next_funding_time_ms BIGINT UNSIGNED NULL,

    raw_json LONGTEXT NULL,

    PRIMARY KEY (id),
    UNIQUE KEY uq_funding_rate_exchange_symbol_bucket (exchange_name, symbol, bucket_start_ms),
    KEY idx_funding_rate_exchange_symbol_bucket (exchange_name, symbol, bucket_start_ms),
    KEY idx_funding_rate_exchange_symbol_next_time (exchange_name, symbol, next_funding_time_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS fx (
    bucket_start_ms BIGINT UNSIGNED NOT NULL,
    rate DECIMAL(18, 6) NOT NULL,
    PRIMARY KEY (bucket_start_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS cashflow_ledger (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    movement_key VARCHAR(128) NOT NULL,

    flow_type VARCHAR(32) NOT NULL,
    status VARCHAR(16) NOT NULL,
    reconcile_status VARCHAR(16) NOT NULL DEFAULT 'pending',

    asset_code VARCHAR(16) NOT NULL,
    amount DECIMAL(28, 12) NOT NULL,
    fee_amount DECIMAL(28, 12) NULL,

    exchange_name VARCHAR(32) NULL,
    from_exchange_name VARCHAR(32) NULL,
    to_exchange_name VARCHAR(32) NULL,

    channel VARCHAR(16) NOT NULL,
    network VARCHAR(32) NULL,
    txid VARCHAR(255) NULL,

    event_time_ms BIGINT UNSIGNED NOT NULL,
    memo VARCHAR(255) NULL,
    raw_json LONGTEXT NULL,

    created_at_ms BIGINT UNSIGNED NOT NULL,
    updated_at_ms BIGINT UNSIGNED NOT NULL,

    PRIMARY KEY (id),
    UNIQUE KEY uq_cashflow_movement_key (movement_key),
    KEY idx_cashflow_event_time (event_time_ms),
    KEY idx_cashflow_exchange_time (exchange_name, event_time_ms),
    KEY idx_cashflow_from_to_time (from_exchange_name, to_exchange_name, event_time_ms),
    KEY idx_cashflow_asset_time (asset_code, event_time_ms),
    KEY idx_cashflow_status (status, reconcile_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

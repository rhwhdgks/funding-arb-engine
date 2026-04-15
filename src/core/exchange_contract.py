REQUIRED_ACCOUNT_SNAPSHOT_KEYS = (
    "exchange",
    "total_balance",
    "leverage",
    "delta_leverage",
    "asset_dict",
)

REQUIRED_SYMBOL_SNAPSHOT_KEYS = (
    "ticker",
    "orderbook",
    "tick",
    "funding",
    "position",
    "health",
    "open_orders",
)

REQUIRED_ORDER_KEYS = (
    "ticker",
    "symbol",
    "uuid",
    "order_id",
    "client_order_id",
    "side",
    "price",
    "qty",
    "qty_contract",
    "left_qty",
    "status",
    "reduce_only",
    "timestamp",
    "raw",
)

REQUIRED_TRADE_KEYS = (
    "symbol",
    "ticker",
    "trade_id",
    "order_id",
    "side",
    "price",
    "qty",
    "qty_contract",
    "quote_qty",
    "timestamp",
    "create_time",
    "raw",
)


def missing_keys(row: dict, required_keys: tuple[str, ...]) -> list[str]:
    return [key for key in required_keys if key not in row]

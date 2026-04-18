from decimal import Decimal, ROUND_DOWN, ROUND_UP

# 매수 시
def floor_by_tick(value, tick_size):
    val = Decimal(str(value))
    tick = Decimal(str(tick_size))
    floored = (val / tick).to_integral_value(rounding=ROUND_DOWN) * tick
    return float(floored)

# 매도 시
def ceil_by_tick(value, tick_size):
    val = Decimal(str(value))
    tick = Decimal(str(tick_size))
    ceiled = (val / tick).to_integral_value(rounding=ROUND_UP) * tick
    return float(ceiled)
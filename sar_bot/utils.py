from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR


DECIMAL_0 = Decimal("0")


def d(val) -> Decimal:
    if isinstance(val, Decimal):
        return val
    return Decimal(str(val))


def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    q = (value / step).to_integral_value(rounding=ROUND_FLOOR)
    return q * step


def ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    q = (value / step).to_integral_value(rounding=ROUND_CEILING)
    return q * step


def to_str(value: Decimal) -> str:
    # Binance accepts decimal strings; avoid scientific notation.
    s = format(value, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    if s == "-0":
        s = "0"
    return s


def parse_bool(val: str | None, default: bool = False) -> bool:
    if val is None:
        return default
    v = val.strip().lower()
    if v in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


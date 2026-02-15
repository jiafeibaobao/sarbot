from decimal import Decimal

from sar_bot.utils import ceil_to_step, floor_to_step, to_str


def test_floor_ceil_step():
    step = Decimal("0.01")
    assert floor_to_step(Decimal("1.234"), step) == Decimal("1.23")
    assert ceil_to_step(Decimal("1.234"), step) == Decimal("1.24")


def test_to_str():
    assert to_str(Decimal("1.2300")) == "1.23"
    assert to_str(Decimal("1.000")) == "1"
    assert to_str(Decimal("0")) == "0"


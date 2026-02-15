from decimal import Decimal

from sar_bot.state import StateStore


def test_sma_seed_and_roll():
    state = StateStore(["BTCUSDT"], sma_period=3)
    state.seed_closes("BTCUSDT", [Decimal("1"), Decimal("2"), Decimal("3")])
    st = state.get("BTCUSDT")
    assert st.sma == Decimal("2")

    sma = state.append_close("BTCUSDT", Decimal("4"), close_time_ms=1)
    # window becomes [2,3,4], sma = 3
    assert sma == Decimal("3")
    st = state.get("BTCUSDT")
    assert list(st.closes) == [Decimal("2"), Decimal("3"), Decimal("4")]


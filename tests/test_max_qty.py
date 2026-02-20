from decimal import Decimal

from sar_bot.execution_manager import ExecutionManager
from sar_bot.models import SymbolFilters
from sar_web.server import parse_filters


def test_split_qty_by_max_basic():
    mgr = ExecutionManager.__new__(ExecutionManager)
    filters = SymbolFilters(
        tick_size=Decimal("0.1"),
        step_size=Decimal("1"),
        min_qty=Decimal("1"),
        max_qty=Decimal("100"),
    )

    chunks = mgr._split_qty_by_max(Decimal("250"), filters=filters)
    assert chunks == [Decimal("100"), Decimal("100"), Decimal("50")]


def test_split_qty_by_max_no_max():
    mgr = ExecutionManager.__new__(ExecutionManager)
    filters = SymbolFilters(
        tick_size=Decimal("0.1"),
        step_size=Decimal("0.1"),
        min_qty=Decimal("0.1"),
        max_qty=None,
    )

    chunks = mgr._split_qty_by_max(Decimal("1.27"), filters=filters)
    assert chunks == [Decimal("1.2")]


def test_parse_filters_extracts_max_qty():
    info = {
        "symbols": [
            {
                "symbol": "INITUSDT",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                    {"filterType": "LOT_SIZE", "stepSize": "1", "minQty": "1", "maxQty": "500000"},
                    {
                        "filterType": "MARKET_LOT_SIZE",
                        "stepSize": "1",
                        "minQty": "10",
                        "maxQty": "300000",
                    },
                ],
            }
        ]
    }

    out = parse_filters(info, ["INITUSDT"])
    f = out["INITUSDT"]
    assert f.min_qty == Decimal("10")
    assert f.step_size == Decimal("1")
    assert f.max_qty == Decimal("300000")

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from decimal import Decimal
from collections import deque

from sar_bot.models import Side, SymbolFilters
from sar_bot.utils import DECIMAL_0


@dataclass(slots=True)
class SymbolState:
    filters: SymbolFilters | None = None
    closes: deque[Decimal] = field(default_factory=lambda: deque())
    closes_sum: Decimal = DECIMAL_0
    sma: Decimal | None = None
    last_close: Decimal | None = None
    last_kline_close_time_ms: int | None = None

    position_amt: Decimal = DECIMAL_0  # one-way: >0 long, <0 short
    active_stop_order_id: int | None = None
    active_stop_client_order_id: str | None = None
    stop_mode: str | None = None  # FLIP | CLOSE_ONLY
    active_stop_price: Decimal | None = None
    active_stop_side: str | None = None
    last_flip_ts: float | None = None

    paused: bool = False
    pause_reason: str | None = None
    pending_reentry_side: Side | None = None
    pending_reentry_qty: Decimal | None = None


class StateStore:
    def __init__(self, symbols: list[str], sma_period: int):
        self._lock = threading.RLock()
        self._symbols = {s: SymbolState(closes=deque(maxlen=sma_period)) for s in symbols}
        self.dual_side_position: bool | None = None

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def symbols(self) -> list[str]:
        with self._lock:
            return list(self._symbols.keys())

    def get(self, symbol: str) -> SymbolState:
        with self._lock:
            return self._symbols[symbol]

    def set_filters(self, symbol: str, filters: SymbolFilters) -> None:
        with self._lock:
            self._symbols[symbol].filters = filters

    def seed_closes(self, symbol: str, closes: list[Decimal]) -> None:
        with self._lock:
            st = self._symbols[symbol]
            st.closes.clear()
            st.closes_sum = DECIMAL_0
            for c in closes:
                st.closes.append(c)
                st.closes_sum += c
            if len(st.closes) == st.closes.maxlen:
                st.sma = st.closes_sum / Decimal(len(st.closes))

    def append_close(self, symbol: str, close: Decimal, close_time_ms: int) -> Decimal | None:
        with self._lock:
            st = self._symbols[symbol]
            # Deduplicate
            if st.last_kline_close_time_ms is not None and close_time_ms <= st.last_kline_close_time_ms:
                return st.sma
            st.last_kline_close_time_ms = close_time_ms
            st.last_close = close

            if len(st.closes) == st.closes.maxlen:
                oldest = st.closes.popleft()
                st.closes_sum -= oldest
            st.closes.append(close)
            st.closes_sum += close

            if len(st.closes) < (st.closes.maxlen or 0):
                st.sma = None
            else:
                st.sma = st.closes_sum / Decimal(len(st.closes))
            return st.sma

    def set_position_amt(self, symbol: str, amt: Decimal) -> None:
        with self._lock:
            self._symbols[symbol].position_amt = amt

    def set_paused(self, symbol: str, paused: bool, reason: str | None = None) -> None:
        with self._lock:
            st = self._symbols[symbol]
            st.paused = paused
            st.pause_reason = reason if paused else None

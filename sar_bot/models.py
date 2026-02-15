from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal


@dataclass(frozen=True, slots=True)
class KlineClosedEvent:
    symbol: str
    interval: str
    close_time_ms: int
    close: Decimal


@dataclass(frozen=True, slots=True)
class UserDataEvent:
    event_type: str
    payload: dict[str, Any]


TaskType = Literal[
    "INIT_SYMBOL",
    "CANCEL_STALE_ORDERS",
    "OPEN_INITIAL",
    "UPDATE_STOP",
    "MARKET_FLIP",
    "OPEN_AFTER_CLOSE",
    "RECONCILE",
]


@dataclass(frozen=True, slots=True)
class ExecutionTask:
    task_type: TaskType
    symbol: str
    data: dict[str, Any]


Side = Literal["LONG", "SHORT", "FLAT"]


@dataclass(frozen=True, slots=True)
class SymbolFilters:
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal


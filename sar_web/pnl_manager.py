from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
from decimal import Decimal


@dataclass(slots=True)
class PnlSnapshot:
    equity: Decimal
    wallet_balance: Decimal
    available_balance: Decimal
    unrealized_pnl: Decimal
    daily_realized: Decimal
    total_realized: Decimal
    yield_pct: Decimal


class PnLManager:
    """
    轻量 PnL 追踪（不扫全量历史成交）：
    - Total realized: 当前 walletBalance - 启动时 walletBalance
    - Daily realized: 当前 walletBalance - 当日 00:00 基准 walletBalance
    - Unrealized: 直接取账户接口 totalUnrealizedProfit

    注意：walletBalance 会包含手续费/资金费等影响，这对“资金看板”更贴近真实资金变化。
    """

    def __init__(self):
        self._start_wallet: Decimal | None = None
        self._daily_date: date | None = None
        self._daily_start_wallet: Decimal | None = None

    def update(
        self,
        *,
        now: datetime,
        wallet_balance: Decimal,
        available_balance: Decimal,
        unrealized_pnl: Decimal,
    ) -> PnlSnapshot:
        if self._start_wallet is None:
            self._start_wallet = wallet_balance

        if self._daily_date != now.date():
            self._daily_date = now.date()
            self._daily_start_wallet = wallet_balance

        start_wallet = self._start_wallet or Decimal("0")
        daily_start = self._daily_start_wallet or wallet_balance

        total_realized = wallet_balance - start_wallet
        daily_realized = wallet_balance - daily_start
        equity = wallet_balance + unrealized_pnl

        yield_pct = Decimal("0")
        if start_wallet > 0:
            yield_pct = (total_realized / start_wallet) * Decimal("100")

        return PnlSnapshot(
            equity=equity,
            wallet_balance=wallet_balance,
            available_balance=available_balance,
            unrealized_pnl=unrealized_pnl,
            daily_realized=daily_realized,
            total_realized=total_realized,
            yield_pct=yield_pct,
        )


__all__ = ["PnLManager", "PnlSnapshot"]


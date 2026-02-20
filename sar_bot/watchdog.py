from __future__ import annotations

import logging
import queue
import threading
import time
from decimal import Decimal

from sar_bot.binance_service import BinanceService, ClientError, ServerError
from sar_bot.models import ExecutionTask
from sar_bot.state import StateStore
from sar_bot.utils import d


class Watchdog(threading.Thread):
    """
    每 60s 强制对齐真实持仓（positionRisk）到本地状态：
    - 发现不一致：以交易所为准重置本地 state，并触发 RECONCILE/UPDATE_STOP
    - 如果发现 bot stop 丢失：触发 UPDATE_STOP
    """

    def __init__(
        self,
        *,
        service: BinanceService,
        state: StateStore,
        exec_queue: "queue.Queue[ExecutionTask]",
        symbols: list[str],
        client_order_id_prefix: str,
        interval_s: int,
        shutdown: threading.Event,
    ):
        super().__init__(name="Watchdog", daemon=True)
        self._log = logging.getLogger(self.name)
        self._svc = service
        self._state = state
        self._exec_q = exec_queue
        self._symbols = symbols
        self._prefix = client_order_id_prefix
        self._interval_s = interval_s
        self._shutdown = shutdown

    def run(self) -> None:
        self._log.info("Watchdog started (interval=%ss)", self._interval_s)
        while not self._shutdown.is_set():
            start = time.time()
            try:
                self._tick()
            except Exception:
                self._log.exception("Watchdog tick failed")
            elapsed = time.time() - start
            sleep_s = max(1.0, float(self._interval_s) - elapsed)
            self._shutdown.wait(timeout=sleep_s)

    def _tick(self) -> None:
        try:
            risks = self._svc.get_position_risk()
        except (ClientError, ServerError) as e:
            self._log.warning("watchdog get_position_risk failed: %s", getattr(e, "error_message", e))
            return
        except Exception as e:
            self._log.warning("watchdog get_position_risk unexpected error: %s", e)
            return
        risk_map: dict[str, Decimal] = {}
        for r in risks:
            sym = str(r.get("symbol", "")).upper()
            if sym in self._symbols:
                risk_map[sym] = d(r.get("positionAmt", "0"))

        for sym in self._symbols:
            exch_amt = risk_map.get(sym, Decimal("0"))
            with self._state.lock:
                st = self._state.get(sym)
                local_amt = st.position_amt
                paused = st.paused

            if paused:
                continue

            if local_amt != exch_amt:
                self._log.critical(
                    "STATE MISMATCH %s local=%s exch=%s -> reset to exchange truth",
                    sym,
                    local_amt,
                    exch_amt,
                )
                self._state.set_position_amt(sym, exch_amt)
                self._enqueue(ExecutionTask("RECONCILE", sym, {"reason": "watchdog_mismatch"}))
                continue

            # Ensure bot stop exists (cheap check via open orders)
            try:
                open_orders = self._svc.get_open_orders(sym)
            except (ClientError, ServerError) as e:
                self._log.warning("get_open_orders failed for %s: %s", sym, getattr(e, "error_message", e))
                continue
            except Exception as e:
                self._log.warning("get_open_orders unexpected error for %s: %s", sym, e)
                continue

            has_bot_stop = False
            prefix = f"{self._prefix}-{sym}-"
            for o in open_orders:
                cid = str(o.get("clientOrderId", ""))
                if cid.startswith(prefix):
                    has_bot_stop = True
                    break

            if not has_bot_stop:
                self._log.warning("No bot STOP found for %s; enqueue UPDATE_STOP", sym)
                self._enqueue(ExecutionTask("UPDATE_STOP", sym, {"reason": "watchdog_no_stop"}))

    def _enqueue(self, task: ExecutionTask) -> None:
        try:
            self._exec_q.put_nowait(task)
        except queue.Full:
            self._log.error("exec_queue full; dropping task %s %s", task.task_type, task.symbol)


__all__ = ["Watchdog"]

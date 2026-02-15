from __future__ import annotations

import logging
import queue
import threading
from decimal import Decimal

from sar_bot.models import ExecutionTask, KlineClosedEvent
from sar_bot.state import StateStore


class StrategyEngine(threading.Thread):
    """
    只负责：
    - 消费 1m 收盘 K 线事件
    - 更新 rolling SMA
    - 产出 UPDATE_STOP 任务给执行线程
    """

    def __init__(
        self,
        *,
        state: StateStore,
        market_queue: "queue.Queue[KlineClosedEvent]",
        exec_queue: "queue.Queue[ExecutionTask]",
        shutdown: threading.Event,
    ):
        super().__init__(name="StrategyEngine", daemon=True)
        self._log = logging.getLogger(self.name)
        self._state = state
        self._market_q = market_queue
        self._exec_q = exec_queue
        self._shutdown = shutdown

    def run(self) -> None:
        self._log.info("StrategyEngine started")
        while not self._shutdown.is_set():
            try:
                evt = self._market_q.get(timeout=1)
            except queue.Empty:
                continue
            try:
                self._on_kline_closed(evt)
            except Exception:
                self._log.exception("Strategy error on %s", evt)

    def _on_kline_closed(self, evt: KlineClosedEvent) -> None:
        sma = self._state.append_close(evt.symbol, evt.close, evt.close_time_ms)
        if sma is None:
            self._log.debug("%s SMA not ready yet", evt.symbol)
            return
        self._log.debug("%s close=%s sma=%s", evt.symbol, evt.close, sma)
        task = ExecutionTask(
            task_type="UPDATE_STOP",
            symbol=evt.symbol,
            data={
                "close": evt.close,
                "sma": sma,
                "close_time_ms": evt.close_time_ms,
            },
        )
        try:
            self._exec_q.put_nowait(task)
        except queue.Full:
            self._log.warning("exec_queue full; dropping UPDATE_STOP for %s", evt.symbol)


__all__ = ["StrategyEngine"]


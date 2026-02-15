from __future__ import annotations

import json
import logging
import queue
import threading
import time
from decimal import Decimal
from typing import Any

from sar_bot.binance_service import BinanceService, ClientError, ServerError
from sar_bot.config import Config
from sar_bot.models import ExecutionTask, KlineClosedEvent, UserDataEvent
from sar_bot.state import StateStore
from sar_bot.utils import d


class UserDataProcessor(threading.Thread):
    """
    处理用户数据流事件：
    - 更新本地 position_amt
    - STOP 成交后立即触发 UPDATE_STOP / OPEN_AFTER_CLOSE
    """

    def __init__(
        self,
        *,
        config: Config,
        state: StateStore,
        user_queue: "queue.Queue[UserDataEvent]",
        exec_queue: "queue.Queue[ExecutionTask]",
        shutdown: threading.Event,
    ):
        super().__init__(name="UserDataProcessor", daemon=True)
        self._log = logging.getLogger(self.name)
        self._cfg = config
        self._state = state
        self._user_q = user_queue
        self._exec_q = exec_queue
        self._shutdown = shutdown

    def run(self) -> None:
        self._log.info("UserDataProcessor started")
        while not self._shutdown.is_set():
            try:
                evt = self._user_q.get(timeout=1)
            except queue.Empty:
                continue
            try:
                self._handle(evt)
            except Exception:
                self._log.exception("UserDataProcessor error: %s", evt.event_type)

    def _handle(self, evt: UserDataEvent) -> None:
        et = evt.event_type
        p = evt.payload
        if et == "ACCOUNT_UPDATE":
            self._on_account_update(p)
        elif et == "ORDER_TRADE_UPDATE":
            self._on_order_trade_update(p)
        elif et == "listenKeyExpired":
            # supervisor will handle restart; just log here.
            self._log.warning("listenKeyExpired event received")
        else:
            return

    def _on_account_update(self, payload: dict[str, Any]) -> None:
        a = payload.get("a") or {}
        positions = a.get("P") or []
        if not isinstance(positions, list):
            return
        for pos in positions:
            sym = str(pos.get("s", "")).upper()
            if sym not in self._cfg.symbols:
                continue
            amt = d(pos.get("pa", "0"))
            self._state.set_position_amt(sym, amt)

    def _on_order_trade_update(self, payload: dict[str, Any]) -> None:
        o = payload.get("o") or {}
        sym = str(o.get("s", "")).upper()
        if sym not in self._cfg.symbols:
            return
        status = str(o.get("X", ""))
        order_type = str(o.get("o", ""))
        client_id = str(o.get("c", ""))
        if status != "FILLED":
            return

        prefix = f"{self._cfg.client_order_id_prefix}-{sym}-"
        if not client_id.startswith(prefix):
            # Not ours; still position may have changed (manual action). Watchdog will reconcile.
            return

        self._log.info("%s order FILLED type=%s cid=%s", sym, order_type, client_id)

        # If we were using CLOSE_ONLY stop, re-open immediately with pending qty/side.
        if order_type == "STOP_MARKET":
            with self._state.lock:
                st = self._state.get(sym)
                pending_side = st.pending_reentry_side
                pending_qty = st.pending_reentry_qty
                stop_mode = st.stop_mode
                if stop_mode == "CLOSE_ONLY" and pending_side and pending_qty:
                    st.pending_reentry_side = None
                    st.pending_reentry_qty = None
                    task = ExecutionTask(
                        "OPEN_AFTER_CLOSE",
                        sym,
                        {"side": pending_side, "qty": pending_qty},
                    )
                    self._enqueue(task)
                    return

        # Default: refresh stop quickly.
        self._enqueue(ExecutionTask("UPDATE_STOP", sym, {"reason": "order_filled"}))

    def _enqueue(self, task: ExecutionTask) -> None:
        try:
            self._exec_q.put_nowait(task)
        except queue.Full:
            self._log.error("exec_queue full; dropping %s %s", task.task_type, task.symbol)


class WsSupervisor(threading.Thread):
    """
    管理 market/user 两个 WS client：
    - 断线/错误自动重连（在 supervisor 线程，不在 callback 内做重连）
    - 定期 renew listenKey
    """

    def __init__(
        self,
        *,
        config: Config,
        service: BinanceService,
        state: StateStore,
        symbols: list[str],
        market_queue: "queue.Queue[KlineClosedEvent]",
        user_queue: "queue.Queue[UserDataEvent]",
        shutdown: threading.Event,
    ):
        super().__init__(name="WsSupervisor", daemon=True)
        self._log = logging.getLogger(self.name)
        self._cfg = config
        self._svc = service
        self._state = state
        self._symbols = symbols
        self._market_q = market_queue
        self._user_q = user_queue
        self._shutdown = shutdown

        self._market_ws = None
        self._user_ws = None
        self._listen_key: str | None = None

        self._market_down = threading.Event()
        self._user_down = threading.Event()

    def run(self) -> None:
        self._log.info("WsSupervisor started")
        self._start_all()
        next_renew = time.time() + float(self._cfg.listen_key_renew_s)

        backoff_s = 1.0
        while not self._shutdown.is_set():
            if self._market_down.is_set():
                self._log.warning("Market WS down; restarting")
                self._market_down.clear()
                self._restart_market_ws()
                backoff_s = 1.0

            if self._user_down.is_set():
                self._log.warning("User WS down; restarting")
                self._user_down.clear()
                self._restart_user_ws()
                backoff_s = 1.0

            now = time.time()
            if self._listen_key and now >= next_renew:
                ok = self._renew_listen_key()
                if not ok:
                    self._restart_user_ws(new_listen_key=True)
                next_renew = now + float(self._cfg.listen_key_renew_s)

            self._shutdown.wait(timeout=1)

        self._stop_all()
        self._log.info("WsSupervisor stopped")

    # ===== Start/Stop =====
    def _start_all(self) -> None:
        self._restart_market_ws()
        self._restart_user_ws(new_listen_key=True)

    def _stop_all(self) -> None:
        self._stop_ws(self._market_ws, "market")
        self._market_ws = None
        self._stop_ws(self._user_ws, "user")
        self._user_ws = None
        if self._listen_key:
            try:
                self._svc.close_listen_key(self._listen_key)
            except Exception:
                pass
            self._listen_key = None

    def _stop_ws(self, ws, tag: str) -> None:
        if ws is None:
            return
        try:
            ws.stop()
        except Exception as e:
            self._log.debug("stop %s ws failed: %s", tag, e)

    # ===== Callbacks =====
    def _on_market_message(self, _ws: Any, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        data = msg.get("data", msg)
        if not isinstance(data, dict):
            return
        if data.get("e") != "kline":
            return
        k = data.get("k") or {}
        if not isinstance(k, dict):
            return
        if not k.get("x"):
            return  # only closed candles
        sym = str(k.get("s", "")).upper()
        if sym not in self._cfg.symbols:
            return
        interval = str(k.get("i", ""))
        close_time = int(k.get("T", 0))
        close = d(k.get("c", "0"))
        evt = KlineClosedEvent(symbol=sym, interval=interval, close_time_ms=close_time, close=close)
        try:
            self._market_q.put_nowait(evt)
        except queue.Full:
            self._log.warning("market_queue full; dropping kline for %s", sym)

    def _on_user_message(self, _ws: Any, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        if not isinstance(msg, dict):
            return
        et = str(msg.get("e", ""))
        if not et:
            return
        evt = UserDataEvent(event_type=et, payload=msg)
        try:
            self._user_q.put_nowait(evt)
        except queue.Full:
            self._log.warning("user_queue full; dropping event %s", et)

    def _on_market_error(self, _ws: Any, err: Any) -> None:
        self._log.warning("Market WS error: %s", err)
        self._market_down.set()

    def _on_market_close(self, *_args: Any) -> None:
        self._log.warning("Market WS closed")
        self._market_down.set()

    def _on_user_error(self, _ws: Any, err: Any) -> None:
        self._log.warning("User WS error: %s", err)
        self._user_down.set()

    def _on_user_close(self, *_args: Any) -> None:
        self._log.warning("User WS closed")
        self._user_down.set()

    # ===== Restart logic =====
    def _restart_market_ws(self) -> None:
        self._stop_ws(self._market_ws, "market")
        self._market_ws = self._svc.create_market_ws(
            on_message=self._on_market_message,
            on_close=self._on_market_close,
            on_error=self._on_market_error,
            is_combined=True,
        )
        # subscribe streams
        for sym in self._symbols:
            self._market_ws.kline(symbol=sym, interval=self._cfg.kline_interval)
        self._log.info("Market WS subscribed: %s", ",".join(self._symbols))

    def _restart_user_ws(self, *, new_listen_key: bool = False) -> None:
        self._stop_ws(self._user_ws, "user")
        if new_listen_key or not self._listen_key:
            self._listen_key = self._create_listen_key()
        if not self._listen_key:
            self._log.error("Failed to create listenKey; user WS not started")
            return
        self._user_ws = self._svc.create_user_ws(
            on_message=self._on_user_message,
            on_close=self._on_user_close,
            on_error=self._on_user_error,
        )
        self._user_ws.user_data(listen_key=self._listen_key)
        self._log.info("User WS started")

    def _create_listen_key(self) -> str | None:
        try:
            resp = self._svc.new_listen_key()
            lk = str(resp.get("listenKey", ""))
            if lk:
                self._log.info("listenKey created")
                return lk
        except (ClientError, ServerError) as e:
            self._log.error("new_listen_key failed: %s", getattr(e, "error_message", e))
        return None

    def _renew_listen_key(self) -> bool:
        if not self._listen_key:
            return False
        try:
            self._svc.renew_listen_key(self._listen_key)
            self._log.debug("listenKey renewed")
            return True
        except (ClientError, ServerError) as e:
            self._log.warning("renew_listen_key failed: %s", getattr(e, "error_message", e))
            return False


__all__ = ["WsSupervisor", "UserDataProcessor"]

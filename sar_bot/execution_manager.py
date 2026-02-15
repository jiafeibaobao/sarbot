from __future__ import annotations

import logging
import queue
import threading
import time
from decimal import Decimal
from typing import Any
from collections.abc import Callable

from sar_bot.binance_service import BinanceService, ClientError, ServerError
from sar_bot.config import Config, SymbolConfig
from sar_bot.models import ExecutionTask, Side
from sar_bot.runtime_control import RuntimeControl
from sar_bot.state import StateStore
from sar_bot.utils import DECIMAL_0, ceil_to_step, d, floor_to_step, to_str


class ExecutionManager(threading.Thread):
    """
    所有 REST 下单/撤单/查询 都在这个线程执行，避免阻塞 WebSocket 回调线程。
    """

    def __init__(
        self,
        *,
        config: Config,
        service: BinanceService,
        state: StateStore,
        runtime: RuntimeControl,
        symbol_cfg_provider: Callable[[str], SymbolConfig] | None = None,
        exec_queue: "queue.Queue[ExecutionTask]",
        shutdown: threading.Event,
    ):
        super().__init__(name="ExecutionManager", daemon=True)
        self._log = logging.getLogger(self.name)
        self._cfg = config
        self._svc = service
        self._state = state
        self._rt = runtime
        self._symcfg_provider = symbol_cfg_provider or (lambda s: self._cfg.symbols[s])
        self._q = exec_queue
        self._shutdown = shutdown
        self._symbol_locks: dict[str, threading.Lock] = {
            s: threading.Lock() for s in self._cfg.symbols.keys()
        }

    def run(self) -> None:
        self._log.info("ExecutionManager started (running=%s)", self._rt.is_running())
        while not self._shutdown.is_set():
            try:
                task = self._q.get(timeout=1)
            except queue.Empty:
                continue
            try:
                self._handle_task(task)
            except Exception:
                self._log.exception("Execution task failed: %s", task)

    def _handle_task(self, task: ExecutionTask) -> None:
        sym = task.symbol
        lock = self._symbol_locks.get(sym) or threading.Lock()
        with lock:
            if task.task_type == "INIT_SYMBOL":
                self._init_symbol(sym)
            elif task.task_type == "CANCEL_STALE_ORDERS":
                self._cancel_stale_orders(sym)
            elif task.task_type == "OPEN_INITIAL":
                self._open_initial(sym)
            elif task.task_type == "UPDATE_STOP":
                self._update_stop(sym)
            elif task.task_type == "MARKET_FLIP":
                self._market_flip(sym)
            elif task.task_type == "OPEN_AFTER_CLOSE":
                self._open_after_close(sym, task.data)
            elif task.task_type == "RECONCILE":
                self._reconcile(sym, task.data)
            else:
                self._log.warning("Unknown task_type=%s", task.task_type)

    # ===== Helpers =====
    def _client_order_id(self, symbol: str, tag: str) -> str:
        # Binance clientOrderId max length is limited; keep it short.
        ts = int(time.time() * 1000) % 1_000_000_000
        base = f"{self._cfg.client_order_id_prefix}-{symbol}-{tag}-{ts}"
        return base[:36]

    def _symbol_cfg(self, symbol: str) -> SymbolConfig:
        return self._symcfg_provider(symbol)

    def _is_paused(self, symbol: str) -> bool:
        with self._state.lock:
            return self._state.get(symbol).paused

    def _ensure_trading_enabled(self, action: str, symbol: str) -> bool:
        if not self._rt.is_running():
            self._log.info("[PAUSED] %s %s (trading paused)", action, symbol)
            return False
        if self._is_paused(symbol):
            self._log.warning("Symbol paused; skip %s %s", action, symbol)
            return False
        return True

    def _ensure_protective_allowed(self, action: str, symbol: str) -> bool:
        # Protective actions are allowed even when bot is paused.
        if self._is_paused(symbol):
            self._log.warning("Symbol paused; skip %s %s", action, symbol)
            return False
        return True

    def _available_balance_usdt(self) -> Decimal | None:
        try:
            acct = self._svc.account()
        except (ClientError, ServerError) as e:
            self._log.warning("account() failed: %s", getattr(e, "error_message", e))
            return None

        if "availableBalance" in acct:
            return d(acct.get("availableBalance", "0"))

        assets = acct.get("assets")
        if isinstance(assets, list):
            for a in assets:
                if str(a.get("asset", "")).upper() == "USDT":
                    if "availableBalance" in a:
                        return d(a.get("availableBalance", "0"))
        return None

    def _current_trigger_price(self, symbol: str, fallback: Decimal) -> Decimal:
        """
        Price used by STOP_MARKET trigger, aligned with workingType.
        """
        try:
            if self._cfg.working_type == "MARK_PRICE":
                mp = self._svc.rest.mark_price(symbol=symbol)
                if isinstance(mp, dict) and "markPrice" in mp:
                    return d(mp["markPrice"])
            else:
                tp = self._svc.rest.ticker_price(symbol=symbol)
                if isinstance(tp, dict) and "price" in tp:
                    return d(tp["price"])
        except Exception:
            pass
        return fallback

    def _market_close_only(self, symbol: str) -> None:
        if self._is_paused(symbol):
            return
        with self._state.lock:
            st = self._state.get(symbol)
            filters = st.filters
            pos_amt = st.position_amt
        if filters is None or pos_amt == DECIMAL_0:
            return
        qty = floor_to_step(abs(pos_amt), filters.step_size)
        if qty < filters.min_qty:
            self._log.error("%s market_close_only qty %s < minQty %s", symbol, qty, filters.min_qty)
            self._state.set_paused(symbol, True)
            return
        close_side = "SELL" if pos_amt > 0 else "BUY"
        cid = self._client_order_id(symbol, "CLOSE")
        self._log.warning("%s MARKET_CLOSE_ONLY side=%s qty=%s (pos=%s)", symbol, close_side, qty, pos_amt)
        try:
            self._new_order_safe(
                symbol,
                side=close_side,
                order_type="MARKET",
                protective_ok=True,
                quantity=to_str(qty),
                reduceOnly="true",
                newClientOrderId=cid,
            )
        except ClientError as e:
            self._log.error("%s market_close_only failed: %s (%s)", symbol, e.error_message, e.error_code)
            return
        self._sync_position_amt(symbol)

    def _sync_position_amt(self, symbol: str) -> Decimal:
        risks = self._svc.get_position_risk(symbol=symbol)
        if not risks:
            amt = DECIMAL_0
        else:
            # one-way mode: single row per symbol
            amt = d(risks[0].get("positionAmt", "0"))
        self._state.set_position_amt(symbol, amt)
        return amt

    def _cancel_order_safe(self, symbol: str, order_id: int | None = None, client_id: str | None = None) -> None:
        if not self._ensure_protective_allowed("cancel_order", symbol):
            return
        try:
            self._svc.cancel_order(symbol=symbol, order_id=order_id, orig_client_order_id=client_id)
        except ClientError as e:
            # -2011: Unknown order sent.
            if e.error_code in (-2011,):
                return
            self._log.warning("cancel_order failed %s: %s (%s)", symbol, e.error_message, e.error_code)
        except ServerError as e:
            self._log.warning("cancel_order server error %s: %s", symbol, e)

    def _new_order_safe(self, symbol: str, side: str, order_type: str, *, protective_ok: bool = False, **kwargs) -> dict[str, Any] | None:
        if self._rt.is_running():
            allowed = self._ensure_protective_allowed("new_order", symbol)
        else:
            allowed = protective_ok and self._ensure_protective_allowed("new_order(protective)", symbol)
        if not allowed:
            self._log.info("[SKIP] new_order %s side=%s type=%s kwargs=%s", symbol, side, order_type, kwargs)
            return None
        for attempt in range(1, self._cfg.order_retry_max_attempts + 1):
            try:
                return self._svc.new_order(symbol=symbol, side=side, order_type=order_type, **kwargs)
            except ClientError as e:
                # Some client errors can be transient (e.g., small timestamp drift).
                if e.error_code == -1021 and attempt < self._cfg.order_retry_max_attempts:
                    backoff = self._cfg.order_retry_backoff_s * (2 ** (attempt - 1))
                    self._log.warning(
                        "%s timestamp error (-1021) (attempt %s/%s): %s; sleep %.2fs",
                        symbol,
                        attempt,
                        self._cfg.order_retry_max_attempts,
                        e.error_message,
                        backoff,
                    )
                    time.sleep(backoff)
                    continue
                # Other non-retryable cases bubble up to caller for fallback logic.
                raise
            except ServerError as e:
                if attempt >= self._cfg.order_retry_max_attempts:
                    raise
                backoff = self._cfg.order_retry_backoff_s * (2 ** (attempt - 1))
                self._log.warning("ServerError placing order %s (attempt %s/%s): %s; sleep %.2fs",
                                  symbol, attempt, self._cfg.order_retry_max_attempts, e, backoff)
                time.sleep(backoff)
        return None

    # ===== Tasks =====
    def _init_symbol(self, symbol: str) -> None:
        if not self._cfg.apply_exchange_settings:
            return
        if not self._ensure_trading_enabled("init_symbol", symbol):
            return

        symcfg = self._symbol_cfg(symbol)
        try:
            self._svc.change_margin_type(symbol, symcfg.margin_type)
            self._log.info("%s marginType set to %s", symbol, symcfg.margin_type)
        except ClientError as e:
            # -4046: No need to change margin type.
            if e.error_code != -4046:
                self._log.warning("%s change_margin_type failed: %s (%s)", symbol, e.error_message, e.error_code)
        except ServerError as e:
            self._log.warning("%s change_margin_type server error: %s", symbol, e)

        try:
            self._svc.change_leverage(symbol, symcfg.leverage)
            self._log.info("%s leverage set to %sx", symbol, symcfg.leverage)
        except ClientError as e:
            # -4048: No need to change leverage.
            if e.error_code != -4048:
                self._log.warning("%s change_leverage failed: %s (%s)", symbol, e.error_message, e.error_code)
        except ServerError as e:
            self._log.warning("%s change_leverage server error: %s", symbol, e)

    def _cancel_stale_orders(self, symbol: str) -> None:
        if not self._ensure_protective_allowed("cancel_stale_orders", symbol):
            return
        prefix = f"{self._cfg.client_order_id_prefix}-{symbol}-"
        try:
            orders = self._svc.get_open_orders(symbol)
        except (ClientError, ServerError) as e:
            self._log.warning("get_open_orders failed %s: %s", symbol, getattr(e, "error_message", e))
            return
        for o in orders:
            cid = str(o.get("clientOrderId", ""))
            if cid.startswith(prefix):
                oid = int(o.get("orderId"))
                self._log.info("Cancel stale order %s %s (%s)", symbol, oid, cid)
                self._cancel_order_safe(symbol, order_id=oid)

    def _open_initial(self, symbol: str) -> None:
        if self._is_paused(symbol):
            return
        if not self._rt.is_running():
            return

        with self._state.lock:
            st = self._state.get(symbol)
            sma = st.sma
            last_close = st.last_close
            filters = st.filters
            pos_amt = st.position_amt
        if pos_amt != DECIMAL_0:
            return
        if sma is None or last_close is None or filters is None:
            self._log.info("%s open_initial skipped (sma/price/filters not ready)", symbol)
            return

        symcfg = self._symbol_cfg(symbol)
        if not symcfg.enabled:
            return
        if symcfg.notional_usdt <= 0:
            self._log.error("%s notional_usdt must be >0", symbol)
            self._state.set_paused(symbol, True)
            return

        side: Side = "LONG" if last_close >= sma else "SHORT"
        order_side = "BUY" if side == "LONG" else "SELL"

        qty = symcfg.notional_usdt / last_close
        qty = floor_to_step(qty, filters.step_size)
        if qty < filters.min_qty:
            self._log.error("%s computed qty %s < minQty %s", symbol, qty, filters.min_qty)
            self._state.set_paused(symbol, True)
            return

        cid = self._client_order_id(symbol, f"OPEN-{side}")
        self._log.info("%s OPEN_INITIAL %s qty=%s (notional=%s close=%s sma=%s)", symbol, side, qty, symcfg.notional_usdt, last_close, sma)
        try:
            self._new_order_safe(
                symbol,
                side=order_side,
                order_type="MARKET",
                protective_ok=False,
                quantity=to_str(qty),
                newClientOrderId=cid,
            )
        except ClientError as e:
            self._log.error("%s open_initial failed: %s (%s)", symbol, e.error_message, e.error_code)
            if e.error_code == -1013:
                self._state.set_paused(symbol, True)
            return

        # sync position for subsequent stop sizing
        self._sync_position_amt(symbol)

    def _update_stop(self, symbol: str) -> None:
        if self._is_paused(symbol):
            return

        with self._state.lock:
            st = self._state.get(symbol)
            filters = st.filters
            sma = st.sma
            last_close = st.last_close
            pos_amt = st.position_amt
            active_stop_id = st.active_stop_order_id
            active_stop_cid = st.active_stop_client_order_id
            active_stop_price = st.active_stop_price
            active_stop_side = st.active_stop_side
            active_stop_mode = st.stop_mode

        if filters is None or sma is None or last_close is None:
            return

        if pos_amt == DECIMAL_0 and self._cfg.always_in and self._rt.is_running():
            self._open_initial(symbol)
            with self._state.lock:
                pos_amt = self._state.get(symbol).position_amt

        if pos_amt == DECIMAL_0:
            return

        symcfg = self._symbol_cfg(symbol)
        stop_side = "SELL" if pos_amt > 0 else "BUY"
        # Use SMA as stopPrice, quantized by tickSize.
        if stop_side == "SELL":
            stop_price = floor_to_step(sma, filters.tick_size)
        else:
            stop_price = ceil_to_step(sma, filters.tick_size)

        trigger_price = self._current_trigger_price(symbol, fallback=last_close)
        if stop_side == "SELL":
            immediate = trigger_price <= stop_price
        else:
            immediate = trigger_price >= stop_price

        if stop_price <= 0:
            self._log.warning("%s invalid stop_price=%s", symbol, stop_price)
            return

        # If stop would immediately trigger, we are already on the wrong side.
        # - running + enabled: flip
        # - paused/disabled: close only (no new positions)
        if immediate:
            self._log.warning(
                "%s stop would immediately trigger (workingType=%s trigger=%s sma=%s)",
                symbol,
                self._cfg.working_type,
                trigger_price,
                sma,
            )
            if self._rt.is_running() and symcfg.enabled:
                self._market_flip(symbol)
            else:
                self._market_close_only(symbol)
            return

        flip_qty = abs(pos_amt) * Decimal("2")
        flip_qty = floor_to_step(flip_qty, filters.step_size)
        if flip_qty < filters.min_qty:
            self._log.error("%s flip_qty %s < minQty %s", symbol, flip_qty, filters.min_qty)
            self._state.set_paused(symbol, True)
            return

        # Force CLOSE_ONLY in paused mode or disabled-symbol mode (no new positions).
        close_only = False
        if not self._rt.is_running():
            close_only = True
        if not symcfg.enabled:
            close_only = True
        avail = self._available_balance_usdt()
        if avail is not None:
            est_notional = abs(pos_amt) * last_close
            est_margin = est_notional / Decimal(max(symcfg.leverage, 1))
            if avail < (est_margin * Decimal("1.10")):
                close_only = True
                self._log.warning("%s low balance avail=%s est_margin=%s -> CLOSE_ONLY stop", symbol, avail, est_margin)

        cid = self._client_order_id(symbol, "STOP")

        params: dict[str, Any] = {
            "stopPrice": to_str(stop_price),
            "workingType": self._cfg.working_type,
            "priceProtect": "TRUE" if self._cfg.price_protect else "FALSE",
            "newClientOrderId": cid,
        }

        mode = "FLIP"
        if close_only:
            mode = "CLOSE_ONLY"
            params["closePosition"] = "true"
        else:
            params["quantity"] = to_str(flip_qty)

        # Avoid churn if stop is already correct.
        if (
            active_stop_id is not None
            and active_stop_price == stop_price
            and active_stop_side == stop_side
            and active_stop_mode == mode
        ):
            self._log.debug("%s stop unchanged; skip replace", symbol)
            return

        # Cancel previous bot stop (best-effort) right before placing the replacement.
        if active_stop_id is not None:
            self._cancel_order_safe(symbol, order_id=active_stop_id)
        elif active_stop_cid:
            self._cancel_order_safe(symbol, client_id=active_stop_cid)

        self._log.info(
            "%s PLACE_STOP mode=%s side=%s stopPrice=%s qty=%s pos=%s sma=%s close=%s",
            symbol,
            mode,
            stop_side,
            stop_price,
            params.get("quantity", "closePosition=true"),
            pos_amt,
            sma,
            last_close,
        )

        try:
            resp = self._new_order_safe(symbol, side=stop_side, order_type="STOP_MARKET", protective_ok=True, **params)
        except ClientError as e:
            if e.error_code == -2021:
                self._log.warning("%s STOP would trigger immediately (exchange)", symbol)
                if self._rt.is_running() and symcfg.enabled:
                    self._market_flip(symbol)
                else:
                    self._market_close_only(symbol)
                return
            if e.error_code == -2010 and not close_only:
                self._log.warning("%s insufficient margin for FLIP stop -> retry CLOSE_ONLY", symbol)
                # Retry as close-only
                params.pop("quantity", None)
                params["closePosition"] = "true"
                try:
                    resp = self._new_order_safe(symbol, side=stop_side, order_type="STOP_MARKET", protective_ok=True, **params)
                    mode = "CLOSE_ONLY"
                except ClientError as e2:
                    self._log.error("%s CLOSE_ONLY stop failed: %s (%s)", symbol, e2.error_message, e2.error_code)
                    return
            else:
                self._log.error("%s place stop failed: %s (%s)", symbol, e.error_message, e.error_code)
                if e.error_code == -1013:
                    self._state.set_paused(symbol, True)
                return

        if resp and "orderId" in resp:
            with self._state.lock:
                st = self._state.get(symbol)
                st.active_stop_order_id = int(resp["orderId"])
                st.active_stop_client_order_id = cid
                st.stop_mode = mode
                st.active_stop_price = stop_price
                st.active_stop_side = stop_side
                if mode == "CLOSE_ONLY":
                    # After stop closes, re-enter opposite with same qty (approx = abs(pos_amt)).
                    st.pending_reentry_qty = abs(pos_amt)
                    st.pending_reentry_side = "SHORT" if pos_amt > 0 else "LONG"

    def _market_flip(self, symbol: str) -> None:
        if self._is_paused(symbol):
            return
        if not self._rt.is_running():
            return
        symcfg = self._symbol_cfg(symbol)
        if not symcfg.enabled:
            return

        with self._state.lock:
            st = self._state.get(symbol)
            filters = st.filters
            pos_amt = st.position_amt
            last_close = st.last_close
            last_flip_ts = st.last_flip_ts
        if filters is None or last_close is None or pos_amt == DECIMAL_0:
            return

        now = time.time()
        if last_flip_ts is not None and (now - last_flip_ts) < 2.0:
            self._log.warning("%s flip cooldown active; skip MARKET_FLIP", symbol)
            return
        with self._state.lock:
            self._state.get(symbol).last_flip_ts = now

        side = "SELL" if pos_amt > 0 else "BUY"
        qty = abs(pos_amt)
        qty = floor_to_step(qty, filters.step_size)
        if qty < filters.min_qty:
            self._log.error("%s market_flip qty %s < minQty %s", symbol, qty, filters.min_qty)
            self._state.set_paused(symbol, True)
            return

        flip_qty = floor_to_step(qty * Decimal("2"), filters.step_size)
        cid = self._client_order_id(symbol, "MFLIP")

        # Try atomic flip with a single MARKET order (qty*2). If insufficient margin, fallback to close then open.
        self._log.info("%s MARKET_FLIP atomic qty=%s (pos=%s)", symbol, flip_qty, pos_amt)
        try:
            self._new_order_safe(
                symbol,
                side=side,
                order_type="MARKET",
                protective_ok=False,
                quantity=to_str(flip_qty),
                newClientOrderId=cid,
            )
        except ClientError as e:
            if e.error_code == -2010:
                self._log.warning("%s atomic flip rejected (-2010) -> close then open", symbol)
                self._market_close_then_open(symbol, qty, last_close)
            else:
                self._log.error("%s market_flip failed: %s (%s)", symbol, e.error_message, e.error_code)
                if e.error_code == -1013:
                    self._state.set_paused(symbol, True)
                return

        self._sync_position_amt(symbol)
        # After flip, rebuild stop immediately.
        self._update_stop(symbol)

    def _market_close_then_open(self, symbol: str, qty: Decimal, last_close: Decimal) -> None:
        if self._is_paused(symbol):
            return
        with self._state.lock:
            pos_amt = self._state.get(symbol).position_amt
        if pos_amt == DECIMAL_0:
            return

        close_side = "SELL" if pos_amt > 0 else "BUY"
        open_side = "BUY" if pos_amt > 0 else "SELL"
        close_cid = self._client_order_id(symbol, "MCLOSE")
        open_cid = self._client_order_id(symbol, "MOPEN")

        self._log.info("%s MARKET_CLOSE qty=%s", symbol, qty)
        self._new_order_safe(
            symbol,
            side=close_side,
            order_type="MARKET",
            protective_ok=True,
            quantity=to_str(qty),
            reduceOnly="true",
            newClientOrderId=close_cid,
        )
        self._sync_position_amt(symbol)

        if not self._rt.is_running():
            return

        self._log.info("%s MARKET_OPEN opposite qty=%s", symbol, qty)
        self._new_order_safe(
            symbol,
            side=open_side,
            order_type="MARKET",
            protective_ok=False,
            quantity=to_str(qty),
            newClientOrderId=open_cid,
        )
        self._sync_position_amt(symbol)

    def _open_after_close(self, symbol: str, data: dict[str, Any]) -> None:
        if self._is_paused(symbol):
            return
        if not self._rt.is_running():
            return
        side: Side = data.get("side")
        qty: Decimal = data.get("qty")
        if side not in ("LONG", "SHORT") or not isinstance(qty, Decimal):
            return
        with self._state.lock:
            filters = self._state.get(symbol).filters
        if filters is None:
            return
        qty = floor_to_step(qty, filters.step_size)
        if qty < filters.min_qty:
            self._log.error("%s open_after_close qty %s < minQty %s", symbol, qty, filters.min_qty)
            self._state.set_paused(symbol, True)
            return
        order_side = "BUY" if side == "LONG" else "SELL"
        cid = self._client_order_id(symbol, f"REOPEN-{side}")
        self._log.info("%s REOPEN %s qty=%s", symbol, side, qty)
        try:
            self._new_order_safe(
                symbol,
                side=order_side,
                order_type="MARKET",
                protective_ok=False,
                quantity=to_str(qty),
                newClientOrderId=cid,
            )
        except ClientError as e:
            self._log.error("%s reopen failed: %s (%s)", symbol, e.error_message, e.error_code)
            return
        self._sync_position_amt(symbol)
        self._update_stop(symbol)

    def _reconcile(self, symbol: str, data: dict[str, Any]) -> None:
        if self._is_paused(symbol):
            return
        # Refresh position, cancel stale bot stops, then rebuild stop/entry.
        self._sync_position_amt(symbol)
        self._cancel_stale_orders(symbol)
        self._update_stop(symbol)


__all__ = ["ExecutionManager"]

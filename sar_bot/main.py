from __future__ import annotations

import argparse
import logging
import os
import queue
import sys
import threading
import time
from decimal import Decimal

from sar_bot.binance_service import BinanceService
from sar_bot.config import Config
from sar_bot.execution_manager import ExecutionManager
from sar_bot.models import ExecutionTask, KlineClosedEvent, SymbolFilters, UserDataEvent
from sar_bot.runtime_control import RuntimeControl
from sar_bot.state import StateStore
from sar_bot.strategy_engine import StrategyEngine
from sar_bot.utils import d, parse_bool
from sar_bot.watchdog import Watchdog
from sar_bot.ws_supervisor import UserDataProcessor, WsSupervisor


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def parse_filters(exchange_info: dict, symbols: list[str]) -> dict[str, SymbolFilters]:
    out: dict[str, SymbolFilters] = {}
    syms = exchange_info.get("symbols") or []
    if not isinstance(syms, list):
        return out
    wanted = set(symbols)
    for s in syms:
        sym = str(s.get("symbol", "")).upper()
        if sym not in wanted:
            continue
        filters = s.get("filters") or []
        tick_size = None
        step_size = None
        min_qty = None
        if isinstance(filters, list):
            for f in filters:
                if not isinstance(f, dict):
                    continue
                ft = f.get("filterType")
                if ft == "PRICE_FILTER":
                    tick_size = d(f.get("tickSize", "0"))
                elif ft == "LOT_SIZE":
                    step_size = d(f.get("stepSize", "0"))
                    min_qty = d(f.get("minQty", "0"))
                elif ft == "MARKET_LOT_SIZE":
                    # For safety, enforce at least market minQty/stepSize if larger.
                    ms = d(f.get("stepSize", "0"))
                    mq = d(f.get("minQty", "0"))
                    if step_size is None or ms > step_size:
                        step_size = ms
                    if min_qty is None or mq > min_qty:
                        min_qty = mq
        if not tick_size or not step_size or min_qty is None:
            continue
        out[sym] = SymbolFilters(tick_size=tick_size, step_size=step_size, min_qty=min_qty)
    return out


def seed_sma(service: BinanceService, state: StateStore, symbols: list[str], interval: str, sma_period: int) -> None:
    for sym in symbols:
        kl = service.klines(symbol=sym, interval=interval, limit=max(100, sma_period + 5))
        closes = [d(k[4]) for k in kl if len(k) > 6][-sma_period:]
        if len(closes) < sma_period:
            raise RuntimeError(f"{sym} not enough klines to seed SMA (got {len(closes)})")
        state.seed_closes(sym, closes)
        last = kl[-1]
        close_time_ms = int(last[6])
        last_close = d(last[4])
        with state.lock:
            st = state.get(sym)
            st.last_kline_close_time_ms = close_time_ms
            st.last_close = last_close


def init_positions(service: BinanceService, state: StateStore, symbols: list[str]) -> None:
    risks = service.get_position_risk()
    risk_map: dict[str, Decimal] = {}
    for r in risks:
        sym = str(r.get("symbol", "")).upper()
        if sym in symbols:
            risk_map[sym] = d(r.get("positionAmt", "0"))
    for sym in symbols:
        state.set_position_amt(sym, risk_map.get(sym, Decimal("0")))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.json")
    args = parser.parse_args(argv)

    cfg = Config.load(args.config)
    setup_logging(cfg.log_level)
    log = logging.getLogger("main")

    symbols = [s for s, sc in cfg.symbols.items() if sc.enabled]
    if not symbols:
        log.error("No enabled symbols in config")
        return 2

    if cfg.trading_enabled:
        log.warning("TRADING ENABLED (testnet recommended). Set SAR_TRADING_ENABLED=false to dry-run.")
    else:
        log.warning("DRY-RUN mode (no orders will be sent). Set SAR_TRADING_ENABLED=true to enable.")

    # Safety: refuse to trade on non-testnet unless explicitly allowed.
    if cfg.trading_enabled:
        is_testnet = "testnet" in cfg.rest_base_url.lower()
        allow_mainnet = parse_bool(os.environ.get("SAR_ALLOW_MAINNET"), default=False)
        if not is_testnet and not allow_mainnet:
            log.error("Refusing to trade on non-testnet rest_base_url=%s (set SAR_ALLOW_MAINNET=true to override)", cfg.rest_base_url)
            return 2

    service = BinanceService(cfg)
    state = StateStore(symbols, cfg.sma_period)

    # Detect position mode (hedge vs one-way).
    try:
        pm = service.get_position_mode()
        dual = bool(pm.get("dualSidePosition"))
        state.dual_side_position = dual
        if dual and not cfg.support_hedge_mode:
            log.error("Account is in Hedge Mode (dualSidePosition=true); support_hedge_mode=false -> pause all symbols")
            for sym in symbols:
                state.set_paused(sym, True)
    except Exception as e:
        log.warning("get_position_mode failed: %s", e)

    # Load exchange filters
    info = service.exchange_info()
    filt_map = parse_filters(info, symbols)
    for sym in symbols:
        if sym not in filt_map:
            log.error("Missing filters for %s in exchangeInfo", sym)
            return 2
        state.set_filters(sym, filt_map[sym])

    # Seed SMA and initial state
    seed_sma(service, state, symbols, cfg.kline_interval, cfg.sma_period)
    init_positions(service, state, symbols)

    shutdown = threading.Event()
    market_q: "queue.Queue[KlineClosedEvent]" = queue.Queue(maxsize=10000)
    user_q: "queue.Queue[UserDataEvent]" = queue.Queue(maxsize=10000)
    exec_q: "queue.Queue[ExecutionTask]" = queue.Queue(maxsize=10000)

    rt = RuntimeControl(running=cfg.trading_enabled)
    exec_mgr = ExecutionManager(config=cfg, service=service, state=state, runtime=rt, exec_queue=exec_q, shutdown=shutdown)
    strat = StrategyEngine(state=state, market_queue=market_q, exec_queue=exec_q, shutdown=shutdown)
    user_proc = UserDataProcessor(config=cfg, state=state, user_queue=user_q, exec_queue=exec_q, shutdown=shutdown)
    watchdog = Watchdog(
        service=service,
        state=state,
        exec_queue=exec_q,
        symbols=symbols,
        client_order_id_prefix=cfg.client_order_id_prefix,
        interval_s=cfg.watchdog_interval_s,
        shutdown=shutdown,
    )
    ws = WsSupervisor(config=cfg, service=service, state=state, symbols=symbols, market_queue=market_q, user_queue=user_q, shutdown=shutdown)

    exec_mgr.start()
    strat.start()
    user_proc.start()
    watchdog.start()
    ws.start()

    # Initial actions per symbol
    for sym in symbols:
        exec_q.put(ExecutionTask("INIT_SYMBOL", sym, {}))
        exec_q.put(ExecutionTask("CANCEL_STALE_ORDERS", sym, {}))
        exec_q.put(ExecutionTask("UPDATE_STOP", sym, {"reason": "startup"}))

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutdown requested")
        shutdown.set()

    # give threads time to stop
    for t in (ws, watchdog, user_proc, strat, exec_mgr):
        t.join(timeout=10)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

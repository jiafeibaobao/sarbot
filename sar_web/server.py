from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import psutil
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from sar_bot.binance_service import BinanceService, ClientError, ServerError
from sar_bot.config import Config
from sar_bot.execution_manager import ExecutionManager
from sar_bot.models import ExecutionTask, SymbolFilters
from sar_bot.runtime_control import RuntimeControl
from sar_bot.state import StateStore
from sar_bot.strategy_engine import StrategyEngine
from sar_bot.utils import d, parse_bool
from sar_bot.watchdog import Watchdog
from sar_bot.ws_supervisor import UserDataProcessor, WsSupervisor
from sar_web.pnl_manager import PnLManager
from sar_web.settings_store import SettingsStore


log = logging.getLogger("sar_web")


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


@dataclass(slots=True)
class MetricsCache:
    prices: dict[str, Decimal] = field(default_factory=dict)
    position_risk: dict[str, dict[str, Any]] = field(default_factory=dict)
    wallet_balance: Decimal = Decimal("0")
    available_balance: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    margin_balance: Decimal = Decimal("0")
    api_latency_ms: int | None = None
    cpu_pct: float = 0.0
    ram_pct: float = 0.0
    disk_read_bps: float = 0.0
    disk_write_bps: float = 0.0


class AppContext:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.symbols = [s for s, sc in cfg.symbols.items() if sc.enabled]
        if not self.symbols:
            raise RuntimeError("No enabled symbols in config.json")
        self.settings = SettingsStore(
            symbols=self.symbols,
            initial=cfg.symbols,
            primary_symbol=self.symbols[0],
        )

        self.shutdown_thread = asyncio.Event()
        self.shutdown_bot = None  # threading.Event, created after bot init

        self.runtime = RuntimeControl(running=cfg.trading_enabled)
        self.pnl = PnLManager()

        self.service = BinanceService(cfg)
        self.state = StateStore(self.symbols, cfg.sma_period)

        self.market_q = None
        self.user_q = None
        self.exec_q = None

        self.threads: list[Any] = []
        self.cache = MetricsCache()
        self.cache_lock = asyncio.Lock()

        self.started_at = time.time()
        self._disk_prev = psutil.disk_io_counters()
        self._disk_prev_ts = time.time()

    def uptime_s(self) -> int:
        return int(time.time() - self.started_at)


ctx: AppContext | None = None


app = FastAPI(title="SAR Dashboard")

static_dir = Path(__file__).resolve().parent.parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/")
def index():
    p = static_dir / "index.html"
    if p.exists():
        return FileResponse(str(p))
    return JSONResponse({"ok": True, "msg": "static/index.html not found"}, status_code=404)


@app.post("/api/toggle")
async def api_toggle():
    assert ctx is not None
    running = ctx.runtime.toggle()
    # When pausing, push CLOSE_ONLY stops (no flips). When resuming, rebuild flip stops (and always-in entry).
    for sym in ctx.symbols:
        try:
            ctx.exec_q.put_nowait(ExecutionTask("UPDATE_STOP", sym, {"reason": "toggle"}))
        except Exception:
            pass
    return {"running": running}

@app.get("/api/settings")
async def api_get_settings():
    assert ctx is not None
    snap = ctx.settings.snapshot()
    snap["running"] = ctx.runtime.is_running()
    return snap


@app.post("/api/settings")
async def api_update_settings(payload: dict[str, Any]):
    assert ctx is not None
    changed: list[str] = []

    primary = payload.get("primary_symbol")
    if primary:
        ctx.settings.set_primary_symbol(str(primary))

    symbols = payload.get("symbols") or {}
    if isinstance(symbols, dict):
        for sym, sdata in symbols.items():
            if not isinstance(sdata, dict):
                continue
            sym_u = str(sym).upper()
            try:
                before = ctx.settings.get_symbol_config(sym_u)
            except Exception:
                continue
            after = ctx.settings.update_symbol(
                sym_u,
                enabled=sdata.get("enabled"),
                notional_usdt=sdata.get("notional_usdt"),
                leverage=sdata.get("leverage"),
            )
            if before != after:
                changed.append(sym_u)
                # Apply leverage settings and refresh stop/entry logic.
                try:
                    ctx.exec_q.put_nowait(ExecutionTask("INIT_SYMBOL", sym_u, {}))
                except Exception:
                    pass
                try:
                    ctx.exec_q.put_nowait(ExecutionTask("UPDATE_STOP", sym_u, {"reason": "settings_update"}))
                except Exception:
                    pass

    snap = ctx.settings.snapshot()
    snap["running"] = ctx.runtime.is_running()
    snap["changed"] = changed
    return snap


@app.get("/api/status")
async def api_status():
    assert ctx is not None
    return {"running": ctx.runtime.is_running(), "symbols": ctx.symbols}


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            assert ctx is not None
            payload = await build_dashboard_payload()
            await websocket.send_text(json.dumps(payload, ensure_ascii=False))
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        return


async def build_dashboard_payload() -> dict[str, Any]:
    assert ctx is not None
    sym = ctx.settings.get_primary_symbol()
    now = datetime.now(timezone.utc)

    with ctx.state.lock:
        st = ctx.state.get(sym)
        sma = st.sma
        pos_amt = st.position_amt
        entry_price = None
        stop_active = st.active_stop_order_id is not None
        stop_price = st.active_stop_price
        stop_mode = st.stop_mode

    async with ctx.cache_lock:
        cache = ctx.cache
        price = cache.prices.get(sym)
        pr = cache.position_risk.get(sym, {})
        wallet = cache.wallet_balance
        avail = cache.available_balance
        unreal = cache.unrealized_pnl
        margin_bal = cache.margin_balance
        latency = cache.api_latency_ms
        cpu = cache.cpu_pct
        ram = cache.ram_pct
        disk_r = cache.disk_read_bps
        disk_w = cache.disk_write_bps

    if entry_price is None:
        # Prefer exchange truth if available.
        ep = pr.get("entryPrice")
        if ep is not None:
            try:
                entry_price = d(ep)
            except Exception:
                entry_price = None

    pnl_snap = ctx.pnl.update(
        now=now,
        wallet_balance=wallet,
        available_balance=avail,
        unrealized_pnl=unreal,
    )

    side = "FLAT"
    if pos_amt > 0:
        side = "LONG"
    elif pos_amt < 0:
        side = "SHORT"

    dist_pct = None
    if price is not None and sma is not None and price != 0:
        dist_pct = (abs(price - sma) / price) * Decimal("100")

    roe_pct = None
    if entry_price and entry_price > 0 and pos_amt != 0:
        lev = pr.get("leverage")
        try:
            leverage = int(lev) if lev is not None else 10
        except Exception:
            leverage = 10
        init_margin = (abs(pos_amt) * entry_price) / Decimal(max(leverage, 1))
        if init_margin > 0:
            roe_pct = (unreal / init_margin) * Decimal("100")

    used_ratio = None
    if margin_bal and margin_bal > 0:
        used_ratio = float(max(Decimal("0"), (margin_bal - avail) / margin_bal) * Decimal("100"))

    return {
        "ts": int(time.time() * 1000),
        "server_time": now.isoformat(),
        "uptime_s": ctx.uptime_s(),
        "running": ctx.runtime.is_running(),
        "api_latency_ms": latency,
        "account": {
            "equity": float(pnl_snap.equity),
            "available": float(pnl_snap.available_balance),
            "used_ratio_pct": used_ratio,
        },
        "pnl": {
            "unrealized": float(pnl_snap.unrealized_pnl),
            "daily_realized": float(pnl_snap.daily_realized),
            "total_realized": float(pnl_snap.total_realized),
            "yield_pct": float(pnl_snap.yield_pct),
            "roe_pct": float(roe_pct) if roe_pct is not None else None,
        },
        "symbol": sym,
        "settings": ctx.settings.snapshot(),
        "price": float(price) if price is not None else None,
        "sma": float(sma) if sma is not None else None,
        "dist_pct": float(dist_pct) if dist_pct is not None else None,
        "position": {
            "side": side,
            "amt": float(abs(pos_amt)),
            "entry_price": float(entry_price) if entry_price is not None else None,
            "liq_price": float(d(pr["liquidationPrice"])) if "liquidationPrice" in pr else None,
            "stop": {
                "active": bool(stop_active),
                "mode": stop_mode,
                "stop_price": float(stop_price) if stop_price is not None else None,
            },
        },
        "vps": {
            "cpu_pct": float(cpu),
            "ram_pct": float(ram),
            "disk_read_bps": float(disk_r),
            "disk_write_bps": float(disk_w),
        },
    }


def _fetch_account_sync() -> dict[str, Decimal]:
    assert ctx is not None
    acct = ctx.service.account()
    wallet = d(acct.get("totalWalletBalance", "0"))
    avail = d(acct.get("availableBalance", "0"))
    unreal = d(acct.get("totalUnrealizedProfit", "0"))
    margin_bal = d(acct.get("totalMarginBalance", "0"))
    return {"wallet": wallet, "avail": avail, "unreal": unreal, "margin_bal": margin_bal}


def _fetch_position_risk_sync() -> dict[str, dict[str, Any]]:
    assert ctx is not None
    risks = ctx.service.get_position_risk()
    out: dict[str, dict[str, Any]] = {}
    for r in risks:
        sym = str(r.get("symbol", "")).upper()
        if sym in ctx.symbols:
            out[sym] = r
    return out


def _fetch_prices_sync() -> dict[str, Decimal]:
    assert ctx is not None
    out: dict[str, Decimal] = {}
    for sym in ctx.symbols:
        t = ctx.service.rest.ticker_price(symbol=sym)
        if isinstance(t, dict) and "price" in t:
            out[sym] = d(t["price"])
    return out


def _fetch_latency_sync() -> int:
    assert ctx is not None
    t0 = time.perf_counter()
    _ = ctx.service.rest.time()
    return int((time.perf_counter() - t0) * 1000)


async def metrics_poller():
    assert ctx is not None
    last_prices = 0.0
    last_account = 0.0
    last_risk = 0.0
    last_latency = 0.0
    while not ctx.shutdown_thread.is_set():
        now = time.time()
        # psutil (fast)
        cpu = psutil.cpu_percent(interval=None)
        ram = psutil.virtual_memory().percent
        disk = psutil.disk_io_counters()
        dt = max(0.1, now - ctx._disk_prev_ts)
        read_bps = (disk.read_bytes - ctx._disk_prev.read_bytes) / dt
        write_bps = (disk.write_bytes - ctx._disk_prev.write_bytes) / dt
        ctx._disk_prev = disk
        ctx._disk_prev_ts = now

        async with ctx.cache_lock:
            ctx.cache.cpu_pct = float(cpu)
            ctx.cache.ram_pct = float(ram)
            ctx.cache.disk_read_bps = float(read_bps)
            ctx.cache.disk_write_bps = float(write_bps)

        jobs = []
        if now - last_prices >= 0.5:
            last_prices = now
            jobs.append(("prices", asyncio.to_thread(_fetch_prices_sync)))
        if now - last_account >= 1.0:
            last_account = now
            jobs.append(("account", asyncio.to_thread(_fetch_account_sync)))
        if now - last_risk >= 1.0:
            last_risk = now
            jobs.append(("risk", asyncio.to_thread(_fetch_position_risk_sync)))
        if now - last_latency >= 5.0:
            last_latency = now
            jobs.append(("latency", asyncio.to_thread(_fetch_latency_sync)))

        if jobs:
            results = await asyncio.gather(*(j[1] for j in jobs), return_exceptions=True)
            async with ctx.cache_lock:
                for (name, _job), res in zip(jobs, results):
                    if isinstance(res, Exception):
                        log.debug("metrics %s failed: %s", name, res)
                        continue
                    if name == "prices":
                        ctx.cache.prices = res
                    elif name == "account":
                        ctx.cache.wallet_balance = res["wallet"]
                        ctx.cache.available_balance = res["avail"]
                        ctx.cache.unrealized_pnl = res["unreal"]
                        ctx.cache.margin_balance = res["margin_bal"]
                    elif name == "risk":
                        ctx.cache.position_risk = res
                    elif name == "latency":
                        ctx.cache.api_latency_ms = int(res)

        await asyncio.sleep(0.1)


def start_bot_threads(
    cfg: Config,
    rt: RuntimeControl,
    service: BinanceService,
    state: StateStore,
    *,
    symbol_cfg_provider,
) -> tuple[Any, Any]:
    import queue
    import threading

    shutdown = threading.Event()
    market_q: "queue.Queue[Any]" = queue.Queue(maxsize=10000)
    user_q: "queue.Queue[Any]" = queue.Queue(maxsize=10000)
    exec_q: "queue.Queue[Any]" = queue.Queue(maxsize=10000)

    exec_mgr = ExecutionManager(
        config=cfg,
        service=service,
        state=state,
        runtime=rt,
        symbol_cfg_provider=symbol_cfg_provider,
        exec_queue=exec_q,
        shutdown=shutdown,
    )
    strat = StrategyEngine(state=state, market_queue=market_q, exec_queue=exec_q, shutdown=shutdown)
    user_proc = UserDataProcessor(config=cfg, state=state, user_queue=user_q, exec_queue=exec_q, shutdown=shutdown)
    watchdog = Watchdog(
        service=service,
        state=state,
        exec_queue=exec_q,
        symbols=[s for s, sc in cfg.symbols.items() if sc.enabled],
        client_order_id_prefix=cfg.client_order_id_prefix,
        interval_s=cfg.watchdog_interval_s,
        shutdown=shutdown,
    )
    ws = WsSupervisor(
        config=cfg,
        service=service,
        state=state,
        symbols=[s for s, sc in cfg.symbols.items() if sc.enabled],
        market_queue=market_q,
        user_queue=user_q,
        shutdown=shutdown,
    )

    for t in (exec_mgr, strat, user_proc, watchdog, ws):
        t.start()

    # Initial tasks
    symbols = [s for s, sc in cfg.symbols.items() if sc.enabled]
    for sym in symbols:
        exec_q.put(ExecutionTask("INIT_SYMBOL", sym, {}))
        exec_q.put(ExecutionTask("CANCEL_STALE_ORDERS", sym, {}))
        exec_q.put(ExecutionTask("UPDATE_STOP", sym, {"reason": "startup"}))

    return shutdown, exec_q


@app.on_event("startup")
async def on_startup():
    global ctx
    cfg_path = os.environ.get("SAR_CONFIG", "config.json")
    cfg = Config.load(cfg_path)
    setup_logging(cfg.log_level)

    # Safety: refuse to trade on non-testnet unless explicitly allowed.
    if cfg.trading_enabled:
        is_testnet = "testnet" in cfg.rest_base_url.lower()
        allow_mainnet = parse_bool(os.environ.get("SAR_ALLOW_MAINNET"), default=False)
        if not is_testnet and not allow_mainnet:
            raise RuntimeError(f"Refusing to trade on non-testnet rest_base_url={cfg.rest_base_url}")

    ctx = AppContext(cfg)
    assert ctx is not None

    # Exchange filters + SMA seeding + initial positions
    info = ctx.service.exchange_info()
    filt_map = parse_filters(info, ctx.symbols)
    for sym in ctx.symbols:
        if sym not in filt_map:
            raise RuntimeError(f"Missing filters for {sym} in exchangeInfo")
        ctx.state.set_filters(sym, filt_map[sym])
    seed_sma(ctx.service, ctx.state, ctx.symbols, cfg.kline_interval, cfg.sma_period)
    init_positions(ctx.service, ctx.state, ctx.symbols)

    # Start bot threads (strategy loop) in background
    shutdown_bot, exec_q = start_bot_threads(
        cfg,
        ctx.runtime,
        ctx.service,
        ctx.state,
        symbol_cfg_provider=ctx.settings.get_symbol_config,
    )
    ctx.shutdown_bot = shutdown_bot
    ctx.exec_q = exec_q

    # Start asyncio poller
    asyncio.create_task(metrics_poller())
    log.info("Startup complete (symbols=%s)", ",".join(ctx.symbols))


@app.on_event("shutdown")
async def on_shutdown():
    global ctx
    if ctx is None:
        return
    ctx.shutdown_thread.set()
    if ctx.shutdown_bot is not None:
        ctx.shutdown_bot.set()
    ctx = None


__all__ = ["app"]

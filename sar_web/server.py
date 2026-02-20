from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import psutil
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from sar_bot.binance_service import BinanceService, ClientError, ServerError
from sar_bot.config import Config, SymbolConfig
from sar_bot.execution_manager import ExecutionManager
from sar_bot.models import ExecutionTask, SymbolFilters
from sar_bot.runtime_control import RuntimeControl
from sar_bot.state import StateStore
from sar_bot.strategy_engine import StrategyEngine
from sar_bot.utils import d, floor_to_step, parse_bool, to_str
from sar_bot.watchdog import Watchdog
from sar_bot.ws_supervisor import UserDataProcessor, WsSupervisor
from sar_web.pnl_manager import PnLManager
from sar_web.settings_store import SettingsStore


log = logging.getLogger("sar_web")
SYMBOL_RE = re.compile(r"^[A-Z0-9]+USDT$")


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
        max_qty = None
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
                    mx = d(f.get("maxQty", "0"))
                    if mx > 0:
                        max_qty = mx if max_qty is None else min(max_qty, mx)
                elif ft == "MARKET_LOT_SIZE":
                    ms = d(f.get("stepSize", "0"))
                    mq = d(f.get("minQty", "0"))
                    mx = d(f.get("maxQty", "0"))
                    if step_size is None or ms > step_size:
                        step_size = ms
                    if min_qty is None or mq > min_qty:
                        min_qty = mq
                    if mx > 0:
                        max_qty = mx if max_qty is None else min(max_qty, mx)
        if not tick_size or not step_size or min_qty is None:
            continue
        out[sym] = SymbolFilters(
            tick_size=tick_size,
            step_size=step_size,
            min_qty=min_qty,
            max_qty=max_qty,
        )
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
    market_stats: dict[str, dict[str, Any]] = field(default_factory=dict)
    wallet_balance: Decimal = Decimal("0")
    available_balance: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    margin_balance: Decimal = Decimal("0")
    api_latency_ms: int | None = None
    cpu_pct: float = 0.0
    ram_pct: float = 0.0
    disk_read_bps: float = 0.0
    disk_write_bps: float = 0.0
    ticker_24hr: list[dict[str, Any]] | None = None
    ticker_24hr_ts: float = 0.0
    exchange_symbols: dict[str, dict[str, Any]] = field(default_factory=dict)
    exchange_symbols_ts: float = 0.0


class AppContext:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.symbols = [s for s, sc in cfg.symbols.items() if sc.enabled]
        if not self.symbols:
            raise RuntimeError("No enabled symbols in config.json")
        default_template = next(iter(cfg.symbols.values()))
        self.settings = SettingsStore(
            symbols=self.symbols,
            initial=cfg.symbols,
            primary_symbol=self.symbols[0],
            default_template=default_template,
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
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

static_dir = Path(__file__).resolve().parent.parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

def _safe_decimal(val: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        return d(val)
    except Exception:
        return default


def _safe_decimal_opt(val: Any) -> Decimal | None:
    try:
        return d(val)
    except Exception:
        return None


def _is_ui_symbol(sym: str) -> bool:
    s = str(sym or "").upper()
    return bool(s) and s.isascii() and bool(SYMBOL_RE.fullmatch(s))


async def get_24hr_tickers_cached(max_age_s: float = 10.0) -> list[dict[str, Any]]:
    """
    Cache /fapi/v1/ticker/24hr in-memory to avoid hitting REST on every UI click.
    """
    assert ctx is not None
    now = time.time()
    async with ctx.cache_lock:
        if ctx.cache.ticker_24hr is not None and (now - ctx.cache.ticker_24hr_ts) < max_age_s:
            return ctx.cache.ticker_24hr

    try:
        res = await asyncio.to_thread(ctx.service.rest.ticker_24hr_price_change)
        if isinstance(res, dict):
            tickers = [res]
        elif isinstance(res, list):
            tickers = [t for t in res if isinstance(t, dict)]
        else:
            tickers = []
    except (ClientError, ServerError) as e:
        log.warning("ticker_24hr_price_change failed: %s", e)
        tickers = []
    except Exception as e:
        log.warning("ticker_24hr_price_change failed: %s", e)
        tickers = []

    async with ctx.cache_lock:
        ctx.cache.ticker_24hr = tickers
        ctx.cache.ticker_24hr_ts = now
    return tickers


async def get_exchange_symbols_cached(max_age_s: float = 300.0) -> dict[str, dict[str, Any]]:
    """
    Cache exchangeInfo symbol metadata so we can filter out non-tradable symbols (e.g. SETTLING).
    """
    assert ctx is not None
    now = time.time()
    async with ctx.cache_lock:
        if ctx.cache.exchange_symbols and (now - ctx.cache.exchange_symbols_ts) < max_age_s:
            return ctx.cache.exchange_symbols

    try:
        info = await asyncio.to_thread(ctx.service.exchange_info)
        raw = info.get("symbols") or []
        mp: dict[str, dict[str, Any]] = {}
        if isinstance(raw, list):
            for s in raw:
                if not isinstance(s, dict):
                    continue
                sym = str(s.get("symbol", "")).upper()
                if sym:
                    mp[sym] = s
    except Exception as e:
        log.warning("exchange_info cache refresh failed: %s", e)
        mp = {}

    async with ctx.cache_lock:
        if mp:
            ctx.cache.exchange_symbols = mp
            ctx.cache.exchange_symbols_ts = now
        return ctx.cache.exchange_symbols


@app.get("/")
def index():
    p = static_dir / "index.html"
    if p.exists():
        return FileResponse(
            str(p),
            headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
        )
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
    snap["active_symbols"] = ctx.symbols
    tmpl = ctx.settings.default_template()
    snap["defaults"] = {
        "enabled": True,
        "notional_usdt": float(tmpl.notional_usdt),
        "leverage": int(tmpl.leverage),
        "margin_type": tmpl.margin_type,
    }
    return snap


@app.post("/api/settings")
async def api_update_settings(payload: dict[str, Any]):
    assert ctx is not None
    changed: list[str] = []

    primary = payload.get("primary_symbol")
    primary_u: str | None = None
    if primary:
        primary_u = str(primary).upper()
        if not _is_ui_symbol(primary_u):
            raise HTTPException(status_code=400, detail=f"Invalid symbol: {primary_u}")
        ctx.settings.ensure_symbol(primary_u)
        ctx.settings.set_primary_symbol(primary_u)

    symbols = payload.get("symbols") or {}
    if isinstance(symbols, dict):
        for sym, sdata in symbols.items():
            if not isinstance(sdata, dict):
                continue
            sym_u = str(sym).upper()
            if not _is_ui_symbol(sym_u):
                continue
            ctx.settings.ensure_symbol(sym_u)
            before = ctx.settings.get_symbol_config(sym_u)
            after = ctx.settings.update_symbol(
                sym_u,
                enabled=sdata.get("enabled"),
                notional_usdt=sdata.get("notional_usdt"),
                leverage=sdata.get("leverage"),
            )
            if before != after:
                changed.append(sym_u)
                # Apply leverage settings and refresh stop/entry logic.
                if ctx.exec_q is not None and sym_u in set(ctx.symbols):
                    try:
                        ctx.exec_q.put_nowait(ExecutionTask("INIT_SYMBOL", sym_u, {}))
                    except Exception:
                        pass
                    try:
                        ctx.exec_q.put_nowait(ExecutionTask("UPDATE_STOP", sym_u, {"reason": "settings_update"}))
                    except Exception:
                        pass

    # If primary symbol changed, restart bot to trade that symbol (single-symbol mode).
    if primary_u:
        if ctx.symbols != [primary_u]:
            try:
                await switch_active_symbols([primary_u], primary_symbol=primary_u, cleanup_removed=True, reason="primary_change")
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))

    snap = ctx.settings.snapshot()
    snap["running"] = ctx.runtime.is_running()
    snap["active_symbols"] = ctx.symbols
    tmpl = ctx.settings.default_template()
    snap["defaults"] = {
        "enabled": True,
        "notional_usdt": float(tmpl.notional_usdt),
        "leverage": int(tmpl.leverage),
        "margin_type": tmpl.margin_type,
    }
    snap["changed"] = changed
    return snap


@app.get("/api/rankings")
async def api_rankings(
    kind: str = Query("gainers", pattern="^(gainers|losers)$"),
    limit: int = Query(20, ge=1, le=200),
    universe: str = Query("all", pattern="^(configured|all)$"),
    quote: str = Query("USDT", pattern="^(USDT)$"),
    perpetual_only: bool = Query(True),
):
    """
    Simple rankings for symbol selection:
    - kind=gainers: sort by 24h priceChangePercent desc
    - kind=losers:  sort by 24h priceChangePercent asc
    """
    assert ctx is not None
    allowed: set[str] | None = None
    if universe == "configured":
        allowed = set(ctx.settings.symbols())

    tickers = await get_24hr_tickers_cached()
    exch = await get_exchange_symbols_cached()
    items: list[dict[str, Any]] = []
    quote_u = quote.upper()
    for t in tickers:
        sym = str(t.get("symbol", "")).upper()
        if not sym:
            continue
        if not _is_ui_symbol(sym):
            continue
        if allowed is not None and sym not in allowed:
            continue
        meta = exch.get(sym)
        if meta is not None:
            if meta.get("status") != "TRADING":
                continue
            if quote_u and str(meta.get("quoteAsset", "")).upper() != quote_u:
                continue
            if perpetual_only and str(meta.get("contractType", "")).upper() != "PERPETUAL":
                continue
        else:
            # If exchangeInfo is available, skip unknown symbols (safety).
            if exch:
                continue
            if quote_u and not sym.endswith(quote_u):
                continue
            if perpetual_only and "_" in sym:
                continue
        pct = _safe_decimal_opt(t.get("priceChangePercent", "0"))
        if pct is None:
            continue
        last_price = _safe_decimal(t.get("lastPrice", "0"))
        items.append(
            {
                "symbol": sym,
                "change_pct": float(pct),
                "last_price": float(last_price),
            }
        )

    items.sort(key=lambda x: x["change_pct"], reverse=(kind == "gainers"))
    items = items[: int(limit)]
    return {"kind": kind, "limit": int(limit), "universe": universe, "items": items}


@app.get("/api/status")
async def api_status():
    assert ctx is not None
    return {"running": ctx.runtime.is_running(), "symbols": ctx.symbols}


@app.get("/api/dashboard")
async def api_dashboard():
    assert ctx is not None
    return await build_dashboard_payload()


@app.post("/api/client-log")
async def api_client_log(payload: dict[str, Any]):
    msg = str(payload.get("message", ""))[:500]
    ts = str(payload.get("ts", ""))
    log.warning("CLIENT_LOG ts=%s msg=%s", ts, msg)
    return {"ok": True}


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
    # During symbol switch we may briefly have a primary symbol not yet present in the state store.
    if not ctx.symbols:
        raise RuntimeError("No active symbols")
    if sym not in set(ctx.symbols):
        sym = ctx.symbols[0]
    now = datetime.now(timezone.utc)

    with ctx.state.lock:
        st = ctx.state.get(sym)
        sma = st.sma
        pos_amt = st.position_amt
        entry_price = None
        paused = st.paused
        pause_reason = st.pause_reason
        stop_active = st.active_stop_order_id is not None
        stop_price = st.active_stop_price
        stop_mode = st.stop_mode

    async with ctx.cache_lock:
        cache = ctx.cache
        price = cache.prices.get(sym)
        pr = cache.position_risk.get(sym, {})
        market_snap = cache.market_stats.get(sym, {})
        symbol_meta = cache.exchange_symbols.get(sym, {})
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

    mark_price = _safe_decimal_opt(market_snap.get("mark_price"))
    index_price = _safe_decimal_opt(market_snap.get("index_price"))
    funding_rate = _safe_decimal_opt(market_snap.get("funding_rate"))
    change_24h_pct = _safe_decimal_opt(market_snap.get("change_24h_pct"))
    high_24h = _safe_decimal_opt(market_snap.get("high_24h"))
    low_24h = _safe_decimal_opt(market_snap.get("low_24h"))
    volume_24h = _safe_decimal_opt(market_snap.get("volume_24h"))
    quote_volume_24h = _safe_decimal_opt(market_snap.get("quote_volume_24h"))
    weighted_avg_price_24h = _safe_decimal_opt(market_snap.get("weighted_avg_price_24h"))
    open_interest = _safe_decimal_opt(market_snap.get("open_interest"))
    estimated_settle_price = _safe_decimal_opt(market_snap.get("estimated_settle_price"))
    next_funding_ms = market_snap.get("next_funding_time_ms")
    if next_funding_ms is not None:
        try:
            next_funding_ms = int(next_funding_ms)
        except Exception:
            next_funding_ms = None
    next_funding_countdown_s = None
    if next_funding_ms is not None:
        next_funding_countdown_s = max(0, int((next_funding_ms / 1000) - time.time()))

    open_interest_usdt = None
    oi_ref_price = mark_price or price
    if open_interest is not None and oi_ref_price is not None:
        open_interest_usdt = open_interest * oi_ref_price

    price_basis = None
    spread_bps = None
    if mark_price is not None and price is not None and mark_price != 0:
        price_basis = price - mark_price
        spread_bps = (price_basis / mark_price) * Decimal("10000")

    leverage_now = None
    if "leverage" in pr:
        try:
            leverage_now = int(pr.get("leverage"))
        except Exception:
            leverage_now = None
    margin_type = None
    if "marginType" in pr:
        try:
            margin_type = str(pr.get("marginType", "")).upper()
        except Exception:
            margin_type = None
    position_notional = _safe_decimal_opt(pr.get("notional"))
    isolated_wallet = _safe_decimal_opt(pr.get("isolatedWallet"))
    symbol_unrealized = _safe_decimal_opt(pr.get("unRealizedProfit"))

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
            "paused": bool(paused),
            "pause_reason": pause_reason,
            "leverage": leverage_now,
            "margin_type": margin_type,
            "notional": float(abs(position_notional)) if position_notional is not None else None,
            "isolated_wallet": float(isolated_wallet) if isolated_wallet is not None else None,
            "symbol_unrealized": float(symbol_unrealized) if symbol_unrealized is not None else None,
            "entry_price": float(entry_price) if entry_price is not None else None,
            "liq_price": float(d(pr["liquidationPrice"])) if "liquidationPrice" in pr else None,
            "stop": {
                "active": bool(stop_active),
                "mode": stop_mode,
                "stop_price": float(stop_price) if stop_price is not None else None,
            },
        },
        "market": {
            "mark_price": float(mark_price) if mark_price is not None else None,
            "index_price": float(index_price) if index_price is not None else None,
            "estimated_settle_price": float(estimated_settle_price) if estimated_settle_price is not None else None,
            "funding_rate": float(funding_rate) if funding_rate is not None else None,
            "funding_rate_pct": float(funding_rate * Decimal("100")) if funding_rate is not None else None,
            "next_funding_time_ms": next_funding_ms,
            "next_funding_countdown_s": next_funding_countdown_s,
            "change_24h_pct": float(change_24h_pct) if change_24h_pct is not None else None,
            "high_24h": float(high_24h) if high_24h is not None else None,
            "low_24h": float(low_24h) if low_24h is not None else None,
            "volume_24h": float(volume_24h) if volume_24h is not None else None,
            "quote_volume_24h": float(quote_volume_24h) if quote_volume_24h is not None else None,
            "weighted_avg_price_24h": float(weighted_avg_price_24h) if weighted_avg_price_24h is not None else None,
            "open_interest": float(open_interest) if open_interest is not None else None,
            "open_interest_usdt": float(open_interest_usdt) if open_interest_usdt is not None else None,
            "price_basis": float(price_basis) if price_basis is not None else None,
            "spread_bps": float(spread_bps) if spread_bps is not None else None,
            "contract_type": str(symbol_meta.get("contractType", "")).upper() if symbol_meta else None,
            "symbol_status": str(symbol_meta.get("status", "")).upper() if symbol_meta else None,
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


def _fetch_market_stats_sync() -> dict[str, dict[str, Any]]:
    """
    Pull Binance futures market fields commonly shown on exchange UIs:
    mark/index price, funding, 24h stats, and open interest.
    """
    assert ctx is not None
    out: dict[str, dict[str, Any]] = {}

    for sym in ctx.symbols:
        snap: dict[str, Any] = {}

        try:
            mp = ctx.service.rest.mark_price(symbol=sym)
            if isinstance(mp, dict):
                mark_price = _safe_decimal_opt(mp.get("markPrice"))
                index_price = _safe_decimal_opt(mp.get("indexPrice"))
                funding_rate = _safe_decimal_opt(mp.get("lastFundingRate"))
                settle_price = _safe_decimal_opt(mp.get("estimatedSettlePrice"))
                next_funding = mp.get("nextFundingTime")
                ts = mp.get("time")

                if mark_price is not None:
                    snap["mark_price"] = mark_price
                if index_price is not None:
                    snap["index_price"] = index_price
                if funding_rate is not None:
                    snap["funding_rate"] = funding_rate
                if settle_price is not None:
                    snap["estimated_settle_price"] = settle_price
                if next_funding is not None:
                    snap["next_funding_time_ms"] = int(next_funding)
                if ts is not None:
                    snap["mark_time_ms"] = int(ts)
        except Exception:
            pass

        try:
            t24 = ctx.service.rest.ticker_24hr_price_change(symbol=sym)
            if isinstance(t24, dict):
                pct = _safe_decimal_opt(t24.get("priceChangePercent"))
                high = _safe_decimal_opt(t24.get("highPrice"))
                low = _safe_decimal_opt(t24.get("lowPrice"))
                vol = _safe_decimal_opt(t24.get("volume"))
                qvol = _safe_decimal_opt(t24.get("quoteVolume"))
                wavg = _safe_decimal_opt(t24.get("weightedAvgPrice"))
                open_time = t24.get("openTime")
                close_time = t24.get("closeTime")

                if pct is not None:
                    snap["change_24h_pct"] = pct
                if high is not None:
                    snap["high_24h"] = high
                if low is not None:
                    snap["low_24h"] = low
                if vol is not None:
                    snap["volume_24h"] = vol
                if qvol is not None:
                    snap["quote_volume_24h"] = qvol
                if wavg is not None:
                    snap["weighted_avg_price_24h"] = wavg
                if open_time is not None:
                    snap["open_time_ms_24h"] = int(open_time)
                if close_time is not None:
                    snap["close_time_ms_24h"] = int(close_time)
        except Exception:
            pass

        try:
            oi = ctx.service.rest.open_interest(symbol=sym)
            if isinstance(oi, dict):
                open_interest = _safe_decimal_opt(oi.get("openInterest"))
                oi_ts = oi.get("time")
                if open_interest is not None:
                    snap["open_interest"] = open_interest
                if oi_ts is not None:
                    snap["open_interest_time_ms"] = int(oi_ts)
        except Exception:
            pass

        out[sym] = snap

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
    last_market = 0.0
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
        if now - last_market >= 3.0:
            last_market = now
            jobs.append(("market", asyncio.to_thread(_fetch_market_stats_sync)))
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
                    elif name == "market":
                        ctx.cache.market_stats = res
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
) -> tuple[Any, Any, list[Any]]:
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

    threads = [exec_mgr, strat, user_proc, watchdog, ws]
    for t in threads:
        t.start()

    # Initial tasks
    symbols = [s for s, sc in cfg.symbols.items() if sc.enabled]
    for sym in symbols:
        exec_q.put(ExecutionTask("INIT_SYMBOL", sym, {}))
        exec_q.put(ExecutionTask("CANCEL_STALE_ORDERS", sym, {}))
        exec_q.put(ExecutionTask("UPDATE_STOP", sym, {"reason": "startup"}))

    return shutdown, exec_q, threads


def clone_cfg_with_symbols(cfg: Config, symbols: dict[str, SymbolConfig]) -> Config:
    # Config is frozen; create a new instance to run bot threads on a different symbol set.
    return Config(
        api_key=cfg.api_key,
        api_secret=cfg.api_secret,
        rest_base_url=cfg.rest_base_url,
        ws_stream_url=cfg.ws_stream_url,
        recv_window_ms=cfg.recv_window_ms,
        kline_interval=cfg.kline_interval,
        sma_period=cfg.sma_period,
        working_type=cfg.working_type,
        watchdog_interval_s=cfg.watchdog_interval_s,
        listen_key_renew_s=cfg.listen_key_renew_s,
        apply_exchange_settings=cfg.apply_exchange_settings,
        price_protect=cfg.price_protect,
        always_in=cfg.always_in,
        auto_reenter_after_forced_flat=cfg.auto_reenter_after_forced_flat,
        client_order_id_prefix=cfg.client_order_id_prefix,
        trading_enabled=cfg.trading_enabled,
        support_hedge_mode=cfg.support_hedge_mode,
        order_retry_max_attempts=cfg.order_retry_max_attempts,
        order_retry_backoff_s=cfg.order_retry_backoff_s,
        symbols=symbols,
        log_level=cfg.log_level,
    )


def _cleanup_symbol_sync(symbol: str, *, state: StateStore, client_order_id_prefix: str) -> None:
    """
    Best-effort cleanup to avoid leaving unmanaged exposure when switching symbols:
    - cancel SAR orders on the symbol
    - close any existing one-way position with reduceOnly MARKET
    """
    assert ctx is not None
    sym = symbol.upper()
    prefix = f"{client_order_id_prefix}-{sym}-"

    # 1) Cancel our open orders (STOP / etc.)
    try:
        orders = ctx.service.get_open_orders(sym)
    except Exception:
        orders = []
    for o in orders:
        try:
            cid = str(o.get("clientOrderId", ""))
            if not cid.startswith(prefix):
                continue
            oid = int(o.get("orderId"))
            ctx.service.cancel_order(symbol=sym, order_id=oid)
        except Exception:
            continue

    # 2) Close any existing position
    try:
        risks = ctx.service.get_position_risk(symbol=sym)
        pos_amt = d(risks[0].get("positionAmt", "0")) if risks else Decimal("0")
    except Exception:
        pos_amt = Decimal("0")
    if pos_amt == Decimal("0"):
        return

    with state.lock:
        st = state.get(sym)
        filters = st.filters
    if filters is None:
        try:
            info = ctx.service.exchange_info()
            fm = parse_filters(info, [sym])
            filters = fm.get(sym)
        except Exception:
            filters = None
    if filters is None:
        return

    qty = floor_to_step(abs(pos_amt), filters.step_size)
    if qty < filters.min_qty:
        return
    side = "SELL" if pos_amt > 0 else "BUY"
    ts = int(time.time() * 1000) % 1_000_000_000
    cid = f"{client_order_id_prefix}-{sym}-FLAT-{ts}"[:36]
    try:
        ctx.service.new_order(
            symbol=sym,
            side=side,
            order_type="MARKET",
            quantity=to_str(qty),
            reduceOnly="true",
            newClientOrderId=cid,
        )
    except Exception:
        return


async def switch_active_symbols(
    symbols: list[str],
    *,
    primary_symbol: str | None = None,
    cleanup_removed: bool = True,
    reason: str = "",
) -> None:
    """
    Switch bot to trade ONLY the given symbols (currently, dashboard expects 1 symbol, but supports list).
    This will restart bot threads, re-seed SMA, and resubscribe market WS.
    """
    assert ctx is not None
    syms = [str(s).upper() for s in symbols if str(s).strip()]
    if not syms:
        raise ValueError("symbols must be non-empty")
    bad = [s for s in syms if not _is_ui_symbol(s)]
    if bad:
        raise ValueError(f"Invalid symbols: {','.join(bad)}")
    # keep order but remove duplicates
    seen = set()
    syms = [s for s in syms if not (s in seen or seen.add(s))]

    prim = (primary_symbol or syms[0]).upper()
    ctx.settings.ensure_symbol(prim)
    for s in syms:
        ctx.settings.ensure_symbol(s)
    ctx.settings.set_primary_symbol(prim)

    old_syms = ctx.symbols[:]
    removed = [s for s in old_syms if s not in set(syms)]

    # Cleanup removed symbols before detaching.
    if cleanup_removed and removed:
        old_state = ctx.state
        await asyncio.to_thread(
            lambda: [_cleanup_symbol_sync(s, state=old_state, client_order_id_prefix=ctx.cfg.client_order_id_prefix) for s in removed]
        )

    # Stop current bot threads
    if ctx.shutdown_bot is not None:
        try:
            ctx.shutdown_bot.set()
        except Exception:
            pass
        threads = ctx.threads[:] if ctx.threads else []

        def _join_all():
            for t in threads:
                try:
                    t.join(timeout=5)
                except Exception:
                    continue

        await asyncio.to_thread(_join_all)

    # Replace active symbol set
    ctx.symbols = syms

    # New state store for active symbols
    state = StateStore(ctx.symbols, ctx.cfg.sma_period)
    ctx.state = state

    # Filters + SMA seed + initial positions (blocking REST; run in background threads)
    info = await asyncio.to_thread(ctx.service.exchange_info)
    sym_meta: dict[str, dict[str, Any]] = {}
    raw_syms = info.get("symbols") or []
    if isinstance(raw_syms, list):
        for s in raw_syms:
            if not isinstance(s, dict):
                continue
            sym = str(s.get("symbol", "")).upper()
            if sym:
                sym_meta[sym] = s
    for sym in ctx.symbols:
        meta = sym_meta.get(sym)
        if not meta:
            raise ValueError(f"Unsupported symbol: {sym}")
        status = str(meta.get("status", "")).upper()
        if status != "TRADING":
            raise ValueError(f"{sym} status={status} not TRADING")
        # Enforce USD-M perpetual USDT contracts only.
        ct = str(meta.get("contractType", "")).upper()
        qa = str(meta.get("quoteAsset", "")).upper()
        if ct != "PERPETUAL":
            raise ValueError(f"{sym} contractType={ct} (expected PERPETUAL)")
        if qa != "USDT":
            raise ValueError(f"{sym} quoteAsset={qa} (expected USDT)")
    filt_map = parse_filters(info, ctx.symbols)
    for sym in ctx.symbols:
        if sym not in filt_map:
            raise RuntimeError(f"Missing filters for {sym} in exchangeInfo")
        state.set_filters(sym, filt_map[sym])

    await asyncio.to_thread(seed_sma, ctx.service, state, ctx.symbols, ctx.cfg.kline_interval, ctx.cfg.sma_period)
    await asyncio.to_thread(init_positions, ctx.service, state, ctx.symbols)

    # Start new bot threads on this symbol set
    sym_cfgs = {s: ctx.settings.get_symbol_config(s) for s in ctx.symbols}
    bot_cfg = clone_cfg_with_symbols(ctx.cfg, sym_cfgs)
    shutdown_bot, exec_q, threads = start_bot_threads(
        bot_cfg,
        ctx.runtime,
        ctx.service,
        state,
        symbol_cfg_provider=ctx.settings.get_symbol_config,
    )
    ctx.shutdown_bot = shutdown_bot
    ctx.exec_q = exec_q
    ctx.threads = threads
    log.info("Switched active symbols to %s (primary=%s) reason=%s", ",".join(ctx.symbols), prim, reason)


async def _startup_app() -> None:
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

    # Default trading symbol: top gainer (USDT perpetual) from 24h rankings.
    default_sym = ctx.symbols[0]
    try:
        exch = await get_exchange_symbols_cached(max_age_s=0.0)
        tickers = await get_24hr_tickers_cached(max_age_s=0.0)
        best_sym = None
        best_pct: Decimal | None = None
        for t in tickers:
            sym = str(t.get("symbol", "")).upper()
            if not _is_ui_symbol(sym):
                continue
            meta = exch.get(sym)
            if not meta:
                continue
            if str(meta.get("status", "")).upper() != "TRADING":
                continue
            if str(meta.get("contractType", "")).upper() != "PERPETUAL":
                continue
            if str(meta.get("quoteAsset", "")).upper() != "USDT":
                continue
            pct = _safe_decimal_opt(t.get("priceChangePercent", "0"))
            if pct is None:
                continue
            if best_pct is None or pct > best_pct:
                best_pct = pct
                best_sym = sym
        if best_sym:
            default_sym = best_sym
            log.info(
                "Default symbol set by rankings: %s (24h=%.2f%%)",
                best_sym,
                float(best_pct) if best_pct is not None else 0.0,
            )
    except Exception as e:
        log.warning("Default symbol ranking failed: %s", e)

    # Start bot threads (single-symbol mode) in background
    await switch_active_symbols([default_sym], primary_symbol=default_sym, cleanup_removed=False, reason="startup")

    # Start asyncio poller
    asyncio.create_task(metrics_poller())
    log.info("Startup complete (symbols=%s)", ",".join(ctx.symbols))


async def _shutdown_app() -> None:
    global ctx
    if ctx is None:
        return
    ctx.shutdown_thread.set()
    if ctx.shutdown_bot is not None:
        ctx.shutdown_bot.set()
    ctx = None


@asynccontextmanager
async def app_lifespan(_app: FastAPI):
    await _startup_app()
    try:
        yield
    finally:
        await _shutdown_app()


app.router.lifespan_context = app_lifespan


__all__ = ["app"]

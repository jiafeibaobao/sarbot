from __future__ import annotations

import json
import os
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from sar_bot.utils import d, parse_bool


@dataclass(frozen=True, slots=True)
class SymbolConfig:
    enabled: bool
    notional_usdt: Decimal
    leverage: int
    margin_type: str


@dataclass(frozen=True, slots=True)
class Config:
    api_key: str
    api_secret: str

    rest_base_url: str
    ws_stream_url: str  # base only; SDK will append /ws or /stream

    recv_window_ms: int
    kline_interval: str
    sma_period: int
    working_type: str  # MARK_PRICE | CONTRACT_PRICE

    watchdog_interval_s: int
    listen_key_renew_s: int

    apply_exchange_settings: bool
    price_protect: bool

    always_in: bool
    auto_reenter_after_forced_flat: bool

    client_order_id_prefix: str

    trading_enabled: bool
    support_hedge_mode: bool

    order_retry_max_attempts: int
    order_retry_backoff_s: float

    symbols: dict[str, SymbolConfig]

    log_level: str

    @staticmethod
    def _normalize_ws_base(url: str) -> str:
        u = url.rstrip("/")
        if u.endswith("/ws"):
            u = u[: -len("/ws")]
        if u.endswith("/stream"):
            u = u[: -len("/stream")]
        return u.rstrip("/")

    @classmethod
    def load(cls, path: str | os.PathLike[str]) -> "Config":
        data = json.loads(Path(path).read_text(encoding="utf-8"))

        api_key = os.environ.get("BINANCE_API_KEY") or data.get("api_key") or ""
        api_secret = os.environ.get("BINANCE_API_SECRET") or data.get("api_secret") or ""
        if not api_key or not api_secret:
            raise RuntimeError("Missing BINANCE_API_KEY / BINANCE_API_SECRET (testnet keys).")

        rest_base_url = str(data["rest_base_url"]).rstrip("/")
        ws_stream_url = cls._normalize_ws_base(str(data["ws_stream_url"]))

        log_level = os.environ.get("SAR_LOG_LEVEL") or data.get("log_level") or "INFO"

        # Trading kill-switch: env var can override config.
        cfg_trading = bool(data.get("trading_enabled", False))
        env_override = os.environ.get("SAR_TRADING_ENABLED")
        if env_override is None:
            trading_enabled = cfg_trading
        else:
            trading_enabled = parse_bool(env_override, default=cfg_trading)

        symbols: dict[str, SymbolConfig] = {}
        raw_symbols = data.get("symbols") or {}
        if not isinstance(raw_symbols, dict) or not raw_symbols:
            raise RuntimeError("Config 'symbols' must be a non-empty object.")
        for sym, sdata in raw_symbols.items():
            if not isinstance(sdata, dict):
                continue
            symbols[str(sym).upper()] = SymbolConfig(
                enabled=bool(sdata.get("enabled", True)),
                notional_usdt=d(sdata.get("notional_usdt", "0")),
                leverage=int(sdata.get("leverage", 1)),
                margin_type=str(sdata.get("margin_type", "ISOLATED")).upper(),
            )

        return cls(
            api_key=api_key,
            api_secret=api_secret,
            rest_base_url=rest_base_url,
            ws_stream_url=ws_stream_url,
            recv_window_ms=int(data.get("recv_window_ms", 10000)),
            kline_interval=str(data.get("kline_interval", "1m")),
            sma_period=int(data.get("sma_period", 20)),
            working_type=str(data.get("working_type", "MARK_PRICE")).upper(),
            watchdog_interval_s=int(data.get("watchdog_interval_s", 60)),
            listen_key_renew_s=int(data.get("listen_key_renew_s", 1800)),
            apply_exchange_settings=bool(data.get("apply_exchange_settings", True)),
            price_protect=bool(data.get("price_protect", True)),
            always_in=bool(data.get("always_in", True)),
            auto_reenter_after_forced_flat=bool(
                data.get("auto_reenter_after_forced_flat", True)
            ),
            client_order_id_prefix=str(data.get("client_order_id_prefix", "SAR")).upper(),
            trading_enabled=trading_enabled,
            support_hedge_mode=bool(data.get("support_hedge_mode", False)),
            order_retry_max_attempts=int(data.get("order_retry_max_attempts", 5)),
            order_retry_backoff_s=float(data.get("order_retry_backoff_s", 0.5)),
            symbols=symbols,
            log_level=str(log_level).upper(),
        )


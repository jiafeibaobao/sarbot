from __future__ import annotations

import threading
from decimal import Decimal
from typing import Any

from sar_bot.config import SymbolConfig
from sar_bot.utils import d


class SettingsStore:
    """
    Thread-safe runtime settings for symbols.

    Scope:
    - Only symbols that were enabled at startup are supported (no dynamic WS subscribe yet).
    - Changes affect future orders immediately via ExecutionManager's symbol_cfg_provider.
    """

    def __init__(
        self,
        *,
        symbols: list[str],
        initial: dict[str, SymbolConfig],
        primary_symbol: str,
        default_template: SymbolConfig,
    ):
        self._lock = threading.Lock()
        self._symbols = symbols[:]  # stable ordering
        self._cfg: dict[str, SymbolConfig] = {s: initial[s] for s in symbols if s in initial}
        self._primary = primary_symbol
        self._default = default_template

    def symbols(self) -> list[str]:
        with self._lock:
            return self._symbols[:]

    def default_template(self) -> SymbolConfig:
        with self._lock:
            return self._default

    def ensure_symbol(self, symbol: str) -> SymbolConfig:
        sym = symbol.upper()
        with self._lock:
            if sym in self._cfg:
                return self._cfg[sym]
            tmpl = self._default
            cfg = SymbolConfig(
                enabled=True,
                notional_usdt=tmpl.notional_usdt,
                leverage=tmpl.leverage,
                margin_type=tmpl.margin_type,
            )
            self._cfg[sym] = cfg
            if sym not in self._symbols:
                self._symbols.append(sym)
            return cfg

    def get_primary_symbol(self) -> str:
        with self._lock:
            return self._primary

    def set_primary_symbol(self, symbol: str) -> str:
        sym = symbol.upper()
        with self._lock:
            if sym not in self._cfg:
                raise ValueError(f"Unsupported symbol: {sym}")
            self._primary = sym
            return self._primary

    def get_symbol_config(self, symbol: str) -> SymbolConfig:
        sym = symbol.upper()
        with self._lock:
            return self._cfg[sym]

    def update_symbol(
        self,
        symbol: str,
        *,
        enabled: bool | None = None,
        notional_usdt: Decimal | str | float | None = None,
        leverage: int | None = None,
    ) -> SymbolConfig:
        sym = symbol.upper()
        with self._lock:
            if sym not in self._cfg:
                tmpl = self._default
                self._cfg[sym] = SymbolConfig(
                    enabled=True,
                    notional_usdt=tmpl.notional_usdt,
                    leverage=tmpl.leverage,
                    margin_type=tmpl.margin_type,
                )
                if sym not in self._symbols:
                    self._symbols.append(sym)
            cur = self._cfg[sym]
            en = cur.enabled if enabled is None else bool(enabled)
            notional = cur.notional_usdt if notional_usdt is None else d(notional_usdt)
            lev = cur.leverage if leverage is None else int(leverage)
            # margin_type is fixed for now
            nxt = SymbolConfig(
                enabled=en,
                notional_usdt=notional,
                leverage=lev,
                margin_type=cur.margin_type,
            )
            self._cfg[sym] = nxt
            return nxt

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "primary_symbol": self._primary,
                "symbols": {
                    sym: {
                        "enabled": cfg.enabled,
                        "notional_usdt": float(cfg.notional_usdt),
                        "leverage": int(cfg.leverage),
                        "margin_type": cfg.margin_type,
                    }
                    for sym, cfg in self._cfg.items()
                },
            }


__all__ = ["SettingsStore"]

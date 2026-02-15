from __future__ import annotations

import logging
from typing import Any, Callable

from binance.error import ClientError, ServerError
from binance.um_futures import UMFutures
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient

from sar_bot.config import Config


class BinanceService:
    def __init__(self, config: Config):
        self._cfg = config
        self._log = logging.getLogger(self.__class__.__name__)
        self._rest = UMFutures(
            key=config.api_key,
            secret=config.api_secret,
            base_url=config.rest_base_url,
            timeout=10,
        )

    @property
    def rest(self) -> UMFutures:
        return self._rest

    def signed(self, **kwargs) -> dict[str, Any]:
        # The connector sets timestamp internally; we only ensure recvWindow is present.
        out = dict(kwargs)
        out.setdefault("recvWindow", int(self._cfg.recv_window_ms))
        return out

    # ===== REST (public) =====
    def exchange_info(self) -> dict[str, Any]:
        return self._rest.exchange_info()

    def klines(self, symbol: str, interval: str, limit: int) -> list[list[Any]]:
        return self._rest.klines(symbol=symbol, interval=interval, limit=limit)

    # ===== REST (signed) =====
    def get_position_mode(self) -> dict[str, Any]:
        return self._rest.get_position_mode(**self.signed())

    def get_position_risk(self, symbol: str | None = None) -> list[dict[str, Any]]:
        kw = self.signed()
        if symbol:
            kw["symbol"] = symbol
        return self._rest.get_position_risk(**kw)

    def account(self) -> dict[str, Any]:
        return self._rest.account(**self.signed())

    def get_open_orders(self, symbol: str) -> list[dict[str, Any]]:
        # NOTE: In this connector, "get_open_orders" maps to /openOrder (single order)
        # and requires orderId/origClientOrderId. We want /openOrders (list), which is
        # implemented as get_orders().
        return self._rest.get_orders(symbol=symbol, **self.signed())

    def cancel_order(self, symbol: str, order_id: int | None = None, orig_client_order_id: str | None = None) -> dict[str, Any]:
        return self._rest.cancel_order(
            symbol=symbol, orderId=order_id, origClientOrderId=orig_client_order_id, **self.signed()
        )

    def new_order(self, symbol: str, side: str, order_type: str, **kwargs) -> dict[str, Any]:
        return self._rest.new_order(
            symbol=symbol, side=side, type=order_type, **self.signed(**kwargs)
        )

    def change_leverage(self, symbol: str, leverage: int) -> dict[str, Any]:
        return self._rest.change_leverage(symbol=symbol, leverage=leverage, **self.signed())

    def change_margin_type(self, symbol: str, margin_type: str) -> dict[str, Any]:
        return self._rest.change_margin_type(symbol=symbol, marginType=margin_type, **self.signed())

    def new_listen_key(self) -> dict[str, Any]:
        # data stream endpoints are not signed and do not accept recvWindow
        return self._rest.new_listen_key()

    def renew_listen_key(self, listen_key: str) -> dict[str, Any]:
        return self._rest.renew_listen_key(listenKey=listen_key)

    def close_listen_key(self, listen_key: str) -> dict[str, Any]:
        return self._rest.close_listen_key(listenKey=listen_key)

    # ===== WebSocket =====
    def create_market_ws(
        self,
        on_message: Callable[[str], None],
        on_open: Callable[[], None] | None = None,
        on_close: Callable[[Any], None] | None = None,
        on_error: Callable[[Any], None] | None = None,
        is_combined: bool = True,
    ) -> UMFuturesWebsocketClient:
        self._log.info("Creating market WS (combined=%s, base=%s)", is_combined, self._cfg.ws_stream_url)
        return UMFuturesWebsocketClient(
            stream_url=self._cfg.ws_stream_url,
            on_message=on_message,
            on_open=on_open,
            on_close=on_close,
            on_error=on_error,
            is_combined=is_combined,
        )

    def create_user_ws(
        self,
        on_message: Callable[[str], None],
        on_open: Callable[[], None] | None = None,
        on_close: Callable[[Any], None] | None = None,
        on_error: Callable[[Any], None] | None = None,
    ) -> UMFuturesWebsocketClient:
        self._log.info("Creating user WS (base=%s)", self._cfg.ws_stream_url)
        return UMFuturesWebsocketClient(
            stream_url=self._cfg.ws_stream_url,
            on_message=on_message,
            on_open=on_open,
            on_close=on_close,
            on_error=on_error,
            is_combined=False,
        )


__all__ = ["BinanceService", "ClientError", "ServerError"]

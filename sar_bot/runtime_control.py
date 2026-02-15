from __future__ import annotations

import threading


class RuntimeControl:
    """
    Runtime kill-switch for trading.

    - running=True: allow all orders (open/flip/close)
    - running=False: allow only protective actions (cancel orders, reduceOnly/closePosition orders)
    """

    def __init__(self, running: bool):
        self._lock = threading.Lock()
        self._running = bool(running)

    def is_running(self) -> bool:
        with self._lock:
            return self._running

    def set_running(self, running: bool) -> bool:
        with self._lock:
            self._running = bool(running)
            return self._running

    def toggle(self) -> bool:
        with self._lock:
            self._running = not self._running
            return self._running


__all__ = ["RuntimeControl"]


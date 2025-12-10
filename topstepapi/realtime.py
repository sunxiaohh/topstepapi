from __future__ import annotations

import logging
import threading
import time
from typing import Callable, List, Optional, Sequence, Set, Tuple

from signalrcore.hub_connection_builder import HubConnectionBuilder


class RealTimeClient:
    """SignalR client for Topstep user hub with robust reconnect logic."""

    _DEFAULT_BACKOFF: Tuple[int, ...] = (2, 5, 10, 30)

    def __init__(
        self,
        token: str,
        hub: str = "user",
        token_provider: Optional[Callable[[], str]] = None,
        logger: Optional[logging.Logger] = None,
        reconnect_backoff: Optional[Sequence[int]] = None,
    ) -> None:
        self.logger = logger or logging.getLogger("topstep.realtime")
        self.logger.setLevel(self.logger.level or logging.INFO)
        self._token_provider = token_provider
        self.token = token
        # Follow ProjectX example: https://rtc.thefuturesdesk.projectx.com/hubs/user?access_token=TOKEN
        self.base_url = "https://rtc.thefuturesdesk.projectx.com/hubs"
        self.hub = hub
        self.hub_url = ""

        self._subscribed_accounts = False
        self._subscribed_orders_accounts: Set[str] = set()
        self._subscribed_positions_accounts: Set[str] = set()
        self._subscribed_trades_accounts: Set[str] = set()

        self._event_handler_specs: List[Tuple[str, Callable]] = []
        self._disconnect_handlers: List[Callable[[], None]] = []
        self._reconnect_handlers: List[Callable[[], None]] = []

        self._connection_lock = threading.RLock()
        self._is_connected = threading.Event()
        self._stop_event = threading.Event()
        self._reconnect_lock = threading.Lock()
        self._reconnect_thread: Optional[threading.Thread] = None
        self._reconnect_backoff: Tuple[int, ...] = (
            tuple(reconnect_backoff) if reconnect_backoff else self._DEFAULT_BACKOFF
        )

        self.connection = None
        self._build_connection()

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------
    def _build_connection(self) -> None:
        token = self._token_provider() if self._token_provider else self.token
        if not token:
            raise ValueError("No API token available for realtime connection")
        self.token = token
        # Matching the example: https://rtc.thefuturesdesk.projectx.com/hubs/{hub}?access_token=TOKEN
        self.hub_url = f"{self.base_url}/{self.hub}?access_token={token}"

        builder = HubConnectionBuilder().with_url(
            self.hub_url,
            options={
                "verify_ssl": True,
                "skip_negotiation": True,  # per ProjectX example
                "transport": "WebSockets",
                "access_token_factory": lambda: token,
            },
        )
        builder = builder.with_automatic_reconnect(
            {
                "type": "raw",
                "keep_alive_interval": 15,
                "reconnect_interval": 5,
                "max_reconnect_attempts": 0,
            }
        )
        connection = builder.build()
        connection.on_open(self._on_open)
        connection.on_close(self._on_close)
        connection.on_error(self._on_error)

        for event, handler in self._event_handler_specs:
            connection.on(event, handler)

        self.connection = connection

    def start(self) -> bool:
        self.logger.info("Starting realtime connection to %s", self.hub_url)
        self._stop_event.clear()
        with self._connection_lock:
            if self.connection is None:
                self._build_connection()
            self._is_connected.clear()
            try:
                self.connection.start()
            except Exception as exc:  # pragma: no cover - network dependent
                self.logger.exception("Failed to start realtime connection: %s", exc)
                self._schedule_reconnect(f"start_error:{exc}", immediate=True)
                return False
        if self._is_connected.wait(timeout=5):
            self.logger.info("Realtime connection established")
            return True
        self.logger.warning("Realtime connection start pending; watchdog will monitor")
        return False

    def stop(self) -> None:
        self.logger.info("Stopping realtime connection")
        self._stop_event.set()
        with self._connection_lock:
            try:
                if self.connection:
                    self.connection.stop()
            except Exception as exc:  # pragma: no cover - network dependent
                self.logger.warning("Error stopping realtime connection: %s", exc)
            finally:
                self._is_connected.clear()
        self._join_reconnect_thread()

    def _on_open(self) -> None:
        self.logger.info("Realtime connection opened")
        self._is_connected.set()
        with self._reconnect_lock:
            self._reconnect_thread = None
        self._resubscribe_all()
        for handler in list(self._reconnect_handlers):
            try:
                handler()
            except Exception as exc:  # pragma: no cover
                self.logger.exception("Realtime reconnect handler raised: %s", exc)

    def _on_close(self, args=None) -> None:
        if self._stop_event.is_set():
            self.logger.info("Realtime connection closed (stop requested)")
        else:
            self.logger.warning("Realtime connection closed: %s", args)
        self._is_connected.clear()
        for handler in list(self._disconnect_handlers):
            try:
                handler()
            except Exception as exc:  # pragma: no cover
                self.logger.exception("Realtime disconnect handler raised: %s", exc)
        if not self._stop_event.is_set():
            self._schedule_reconnect(f"close:{args}")

    def _on_error(self, error) -> None:
        self.logger.error("Realtime connection error: %s", error)
        if not self._stop_event.is_set():
            self._schedule_reconnect(f"error:{error}")

    # ------------------------------------------------------------------
    # Reconnect handling
    # ------------------------------------------------------------------
    def _schedule_reconnect(self, reason: str, immediate: bool = False) -> None:
        if self._stop_event.is_set():
            return
        with self._reconnect_lock:
            if self._reconnect_thread and self._reconnect_thread.is_alive():
                return
            self._reconnect_thread = threading.Thread(
                target=self._reconnect_loop,
                args=(reason, immediate),
                daemon=True,
            )
            self._reconnect_thread.start()

    def _reconnect_loop(self, reason: str, immediate: bool) -> None:
        attempt = 0
        backoff = (0,) + self._reconnect_backoff if immediate else self._reconnect_backoff
        while not self._stop_event.is_set():
            delay = backoff[min(attempt, len(backoff) - 1)]
            if delay:
                self.logger.info("Realtime reconnect attempt %s in %ss (%s)", attempt + 1, delay, reason)
                time.sleep(delay)
            else:
                self.logger.info("Realtime reconnect attempt %s immediately (%s)", attempt + 1, reason)
            attempt += 1
            if self._stop_event.is_set():
                break
            try:
                with self._connection_lock:
                    self._is_connected.clear()
                    try:
                        if self.connection:
                            self.connection.stop()
                    except Exception:
                        pass
                    self._build_connection()
                    self.connection.start()
                if self._is_connected.wait(timeout=5):
                    self.logger.info("Realtime reconnect succeeded")
                    return
                self.logger.warning("Realtime reconnect attempt timed out")
            except Exception as exc:
                self.logger.exception("Realtime reconnect failed: %s", exc)
        self.logger.info("Realtime reconnect loop exiting (stop=%s)", self._stop_event.is_set())

    def _join_reconnect_thread(self) -> None:
        with self._reconnect_lock:
            thread = self._reconnect_thread
            self._reconnect_thread = None
        if thread and thread.is_alive():
            thread.join(timeout=1)

    def force_reconnect(self, reason: str = "manual") -> None:
        self.logger.warning("Force realtime reconnect requested (%s)", reason)
        self._schedule_reconnect(reason, immediate=True)

    # ------------------------------------------------------------------
    # Subscription management
    # ------------------------------------------------------------------
    def _resubscribe_all(self) -> None:
        if self._subscribed_accounts:
            self._send("SubscribeAccounts", [])
        for account_id in list(self._subscribed_orders_accounts):
            self._send("SubscribeOrders", [account_id])
        for account_id in list(self._subscribed_positions_accounts):
            self._send("SubscribePositions", [account_id])
        for account_id in list(self._subscribed_trades_accounts):
            self._send("SubscribeTrades", [account_id])

    def _send(self, method: str, args) -> bool:
        try:
            self.connection.send(method, args)
            return True
        except Exception as exc:
            self.logger.exception("Realtime send failed (%s): %s", method, exc)
            return False

    def subscribe_accounts(self) -> None:
        if self._send("SubscribeAccounts", []):
            self._subscribed_accounts = True
            self.logger.info("Subscribed to account updates")

    def subscribe_orders(self, account_id: str) -> None:
        if self._send("SubscribeOrders", [account_id]):
            self._subscribed_orders_accounts.add(account_id)
            self.logger.info("Subscribed to order updates for %s", account_id)

    def subscribe_positions(self, account_id: str) -> None:
        if self._send("SubscribePositions", [account_id]):
            self._subscribed_positions_accounts.add(account_id)
            self.logger.info("Subscribed to position updates for %s", account_id)

    def subscribe_trades(self, account_id: str) -> None:
        if self._send("SubscribeTrades", [account_id]):
            self._subscribed_trades_accounts.add(account_id)
            self.logger.info("Subscribed to trade updates for %s", account_id)

    def unsubscribe_accounts(self) -> None:
        if self._send("UnsubscribeAccounts", []):
            self._subscribed_accounts = False

    def unsubscribe_orders(self, account_id: str) -> None:
        if self._send("UnsubscribeOrders", [account_id]):
            self._subscribed_orders_accounts.discard(account_id)

    def unsubscribe_positions(self, account_id: str) -> None:
        if self._send("UnsubscribePositions", [account_id]):
            self._subscribed_positions_accounts.discard(account_id)

    def unsubscribe_trades(self, account_id: str) -> None:
        if self._send("UnsubscribeTrades", [account_id]):
            self._subscribed_trades_accounts.discard(account_id)

    def unsubscribe_all(self) -> None:
        if self._subscribed_accounts:
            self.unsubscribe_accounts()
        for account_id in list(self._subscribed_orders_accounts):
            self.unsubscribe_orders(account_id)
        for account_id in list(self._subscribed_positions_accounts):
            self.unsubscribe_positions(account_id)
        for account_id in list(self._subscribed_trades_accounts):
            self.unsubscribe_trades(account_id)

    # ------------------------------------------------------------------
    # Event registration
    # ------------------------------------------------------------------
    def _register_event(self, event: str, handler) -> None:
        self._event_handler_specs.append((event, handler))
        if self.connection:
            self.connection.on(event, handler)

    def on_account_update(self, handler):
        self._register_event("GatewayUserAccount", handler)
        return self

    def on_order_update(self, handler):
        self._register_event("GatewayUserOrder", handler)
        return self

    def on_position_update(self, handler):
        self._register_event("GatewayUserPosition", handler)
        return self

    def on_trade_update(self, handler):
        self._register_event("GatewayUserTrade", handler)
        return self

    def on_disconnect(self, handler: Callable[[], None]):
        self._disconnect_handlers.append(handler)
        return self

    def on_reconnect(self, handler: Callable[[], None]):
        self._reconnect_handlers.append(handler)
        return self

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def is_connected(self) -> bool:
        return self._is_connected.is_set()

    def wait_for_connection(self, timeout: float = 10.0) -> bool:
        return self._is_connected.wait(timeout=timeout)

    def get_connection_state(self) -> str:
        if not self.connection:
            return "Disconnected"
        try:
            transport = getattr(self.connection, "transport", None)
            state = getattr(transport, "state", None)
            value = getattr(state, "value", None)
            if value == 0:
                return "Connecting"
            if value == 1:
                return "Connected"
            if value == 2:
                return "Disconnected"
            return f"Unknown({value})"
        except Exception:  # pragma: no cover
            return "Unknown"

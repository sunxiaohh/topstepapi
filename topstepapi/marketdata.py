from __future__ import annotations

import logging
import threading
import time
from typing import Callable, List, Optional, Sequence, Set, Tuple

from signalrcore.hub_connection_builder import HubConnectionBuilder

# NOTE: the websocket transport is selected automatically when skip_negotiation=True,
# but we keep the import so downstream callers can override if desired.
from signalrcore.transport.websockets.websocket_transport import WebsocketTransport  # noqa: F401


class MarketDataClient:
    """SignalR wrapper around Topstep's market data hub with resilient reconnects."""

    _DEFAULT_BACKOFF: Tuple[int, ...] = (2, 5, 10, 30)

    def __init__(
        self,
        token: str,
        token_provider: Optional[Callable[[], str]] = None,
        logger: Optional[logging.Logger] = None,
        reconnect_backoff: Optional[Sequence[int]] = None,
    ) -> None:
        self.logger = logger or logging.getLogger("topstep.marketdata")
        self.logger.setLevel(self.logger.level or logging.INFO)
        self._token_provider = token_provider
        self.token = token
        self.base_url = "wss://rtc.thefuturesdesk.projectx.com/hubs/market"
        self.hub_url = ""

        self._subscribed_quotes: Set[str] = set()
        self._subscribed_trades: Set[str] = set()
        self._subscribed_depth: Set[str] = set()

        self._disconnect_handlers: List[Callable[[], None]] = []
        self._reconnect_handlers: List[Callable[[], None]] = []

        # Event handler specs store (event_names, handler)
        self._event_handler_specs: List[Tuple[Tuple[str, ...], Callable]] = []

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

    # ---------------------------------------------------------------------
    # Connection lifecycle
    # ---------------------------------------------------------------------
    def _build_connection(self) -> None:
        token = self._token_provider() if self._token_provider else self.token
        if not token:
            raise ValueError("No API token available for market data connection")
        self.token = token
        self.hub_url = f"{self.base_url}?access_token={token}"

        builder = HubConnectionBuilder().with_url(
            self.hub_url,
            options={
                "verify_ssl": True,
                "skip_negotiation": True,
                "headers": {"Authorization": f"Bearer {token}"},
                "transport": "WebSockets",
            },
        )
        builder = builder.with_automatic_reconnect(
            {
                "type": "raw",
                "keep_alive_interval": 15,
                "reconnect_interval": 5,
                "max_reconnect_attempts": 0,  # we manage retries ourselves
            }
        )
        connection = builder.build()
        connection.on_open(self._on_open)
        connection.on_close(self._on_close)
        connection.on_error(self._on_error)

        # re-register user handlers on the new connection
        for events, handler in self._event_handler_specs:
            for event in events:
                connection.on(event, handler)

        self.connection = connection

    def start(self) -> bool:
        self.logger.info("Starting market data connection to %s", self.hub_url)
        self._stop_event.clear()
        with self._connection_lock:
            if self.connection is None:
                self._build_connection()
            self._is_connected.clear()
            try:
                self.connection.start()
            except Exception as exc:  # pragma: no cover - network dependent
                self.logger.exception("Failed to start market data connection: %s", exc)
                self._schedule_reconnect(f"start_error:{exc}", immediate=True)
                return False
        if self._is_connected.wait(timeout=5):
            self.logger.info("Market data connection established")
            return True
        self.logger.warning("Market data connection start pending; watchdog will monitor")
        return False

    def stop(self) -> None:
        self.logger.info("Stopping market data connection")
        self._stop_event.set()
        with self._connection_lock:
            try:
                if self.connection:
                    self.connection.stop()
            except Exception as exc:  # pragma: no cover - network dependent
                self.logger.warning("Error stopping market data connection: %s", exc)
            finally:
                self._is_connected.clear()
        self._join_reconnect_thread()

    def _on_open(self) -> None:
        self.logger.info("Market data connection opened")
        self._is_connected.set()
        with self._reconnect_lock:
            self._reconnect_thread = None
        if self._subscribed_quotes or self._subscribed_trades or self._subscribed_depth:
            self.logger.info("Re-subscribing to %s quotes, %s trades, %s depth",
                             len(self._subscribed_quotes),
                             len(self._subscribed_trades),
                             len(self._subscribed_depth))
            self._resubscribe_all()
        for handler in list(self._reconnect_handlers):
            try:
                handler()
            except Exception as exc:  # pragma: no cover - user handler
                self.logger.exception("Reconnect handler raised: %s", exc)

    def _on_close(self, args=None) -> None:
        if self._stop_event.is_set():
            self.logger.info("Market data connection closed (stop requested)")
        else:
            self.logger.warning("Market data connection closed: %s", args)
        self._is_connected.clear()
        for handler in list(self._disconnect_handlers):
            try:
                handler()
            except Exception as exc:  # pragma: no cover - user handler
                self.logger.exception("Disconnect handler raised: %s", exc)
        if not self._stop_event.is_set():
            self._schedule_reconnect(f"close:{args}")

    def _on_error(self, error) -> None:
        self.logger.error("Market data connection error: %s", error)
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
                self.logger.info("Reconnect attempt %s in %ss (%s)", attempt + 1, delay, reason)
                time.sleep(delay)
            else:
                self.logger.info("Reconnect attempt %s immediately (%s)", attempt + 1, reason)
            attempt += 1
            if self._stop_event.is_set():
                break
            try:
                with self._connection_lock:
                    self._is_connected.clear()
                    try:
                        if self.connection:
                            self.connection.stop()
                    except Exception:  # pragma: no cover - best effort cleanup
                        pass
                    self._build_connection()
                    self.connection.start()
                if self._is_connected.wait(timeout=5):
                    self.logger.info("Market data reconnect succeeded")
                    return
                self.logger.warning("Market data reconnect attempt timed out")
            except Exception as exc:  # pragma: no cover - network dependent
                self.logger.exception("Market data reconnect failed: %s", exc)
        self.logger.info("Reconnect loop exiting (stop=%s)", self._stop_event.is_set())

    def _join_reconnect_thread(self) -> None:
        with self._reconnect_lock:
            thread = self._reconnect_thread
            self._reconnect_thread = None
        if thread and thread.is_alive():
            thread.join(timeout=1)

    def force_reconnect(self, reason: str = "manual") -> None:
        self.logger.warning("Force reconnect requested (%s)", reason)
        self._schedule_reconnect(reason, immediate=True)

    # ------------------------------------------------------------------
    # Subscription helpers
    # ------------------------------------------------------------------
    def _resubscribe_all(self) -> None:
        for contract_id in list(self._subscribed_quotes):
            self._send("SubscribeContractQuotes", [contract_id])
        for contract_id in list(self._subscribed_trades):
            self._send("SubscribeContractTrades", [contract_id])
        for contract_id in list(self._subscribed_depth):
            self._send("SubscribeContractMarketDepth", [contract_id])

    def _send(self, method: str, args) -> bool:
        try:
            self.connection.send(method, args)
            return True
        except Exception as exc:  # pragma: no cover - network dependent
            self.logger.exception("SignalR send failed (%s): %s", method, exc)
            return False

    # ------------------------------------------------------------------
    # Public API – subscriptions
    # ------------------------------------------------------------------
    def subscribe_contract_quotes(self, contract_id: str) -> bool:
        ok = self._send("SubscribeContractQuotes", [contract_id])
        if ok:
            self._subscribed_quotes.add(contract_id)
            self.logger.info("Subscribed to quotes for %s", contract_id)
        return ok

    def subscribe_contract_trades(self, contract_id: str) -> bool:
        ok = self._send("SubscribeContractTrades", [contract_id])
        if ok:
            self._subscribed_trades.add(contract_id)
            self.logger.info("Subscribed to trades for %s", contract_id)
        return ok

    def subscribe_contract_market_depth(self, contract_id: str) -> bool:
        ok = self._send("SubscribeContractMarketDepth", [contract_id])
        if ok:
            self._subscribed_depth.add(contract_id)
            self.logger.info("Subscribed to depth for %s", contract_id)
        return ok

    def subscribe_contract_all(self, contract_id: str) -> bool:
        return all(
            (
                self.subscribe_contract_quotes(contract_id),
                self.subscribe_contract_trades(contract_id),
                self.subscribe_contract_market_depth(contract_id),
            )
        )

    def unsubscribe_contract_quotes(self, contract_id: str) -> bool:
        ok = self._send("UnsubscribeContractQuotes", [contract_id])
        if ok:
            self._subscribed_quotes.discard(contract_id)
        return ok

    def unsubscribe_contract_trades(self, contract_id: str) -> bool:
        ok = self._send("UnsubscribeContractTrades", [contract_id])
        if ok:
            self._subscribed_trades.discard(contract_id)
        return ok

    def unsubscribe_contract_market_depth(self, contract_id: str) -> bool:
        ok = self._send("UnsubscribeContractMarketDepth", [contract_id])
        if ok:
            self._subscribed_depth.discard(contract_id)
        return ok

    def unsubscribe_contract_all(self, contract_id: str) -> bool:
        return all(
            (
                self.unsubscribe_contract_quotes(contract_id),
                self.unsubscribe_contract_trades(contract_id),
                self.unsubscribe_contract_market_depth(contract_id),
            )
        )

    def unsubscribe_all(self) -> None:
        for contract_id in list(self._subscribed_quotes):
            self.unsubscribe_contract_quotes(contract_id)
        for contract_id in list(self._subscribed_trades):
            self.unsubscribe_contract_trades(contract_id)
        for contract_id in list(self._subscribed_depth):
            self.unsubscribe_contract_market_depth(contract_id)

    # ------------------------------------------------------------------
    # Event registration
    # ------------------------------------------------------------------
    def _register_event(self, events: Sequence[str], handler) -> None:
        spec = (tuple(events), handler)
        self._event_handler_specs.append(spec)
        if self.connection:
            for event in events:
                self.connection.on(event, handler)

    def on_quote_update(self, handler):
        self._register_event(
            ("GatewayQuote", "GatewayContractQuote", "ContractQuote", "MarketQuote"),
            handler,
        )
        return self

    def on_trade_update(self, handler):
        self._register_event(
            ("GatewayTrade", "GatewayContractTrade", "ContractTrade", "MarketTrade"),
            handler,
        )
        return self

    def on_depth_update(self, handler):
        self._register_event(
            (
                "GatewayDepth",
                "GatewayContractMarketDepth",
                "ContractMarketDepth",
                "MarketDepth",
            ),
            handler,
        )
        return self

    # ------------------------------------------------------------------
    # External hooks & helpers
    # ------------------------------------------------------------------
    def on_disconnect(self, handler: Callable[[], None]):
        self._disconnect_handlers.append(handler)
        return self

    def on_reconnect(self, handler: Callable[[], None]):
        self._reconnect_handlers.append(handler)
        return self

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
            if state is None:
                return "Unknown"
            value = getattr(state, "value", None)
            if value == 0:
                return "Connecting"
            if value == 1:
                return "Connected"
            if value == 2:
                return "Disconnected"
            return f"Unknown({value})"
        except Exception:  # pragma: no cover - defensive
            return "Unknown"

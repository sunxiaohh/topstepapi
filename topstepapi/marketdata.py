"""
MARKET DATA CLIENT – RESILIENT VERSION (v1.1)
===========================================

Changes in **v1.1**
------------------
* Replaced deprecated `on_reconnected` with `on_open` (fires on initial
  connect *and* every automatic reconnect in `signalrcore`).
* `_on_open` now handles the (re‑)subscription logic.
* Keeps automatic JWT refresh & background thread from v1.0.
* No other public API changes – drop‑in replacement.

Requirements
------------
```bash
pip install signalrcore requests
```

Quick Usage
-----------
```python
from marketdata_client_fixed import MarketDataClient

TOKEN = "<initial JWT>"
CONTRACT = "CON.F.US.EP.U25"

md = MarketDataClient(TOKEN)
md.on_quote_update(lambda *a: print("Quote", a))
md.on_depth_update(lambda *a: print("Depth", a))

md.start()
md.subscribe_contract_all(CONTRACT)

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    md.stop()
```
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Set

import requests
from signalrcore.hub_connection_builder import HubConnectionBuilder
from signalrcore.transport.websockets.websocket_transport import WebsocketTransport

__all__ = ["MarketDataClient"]


class MarketDataClient:
    """Project X (TopStep) market‑hub client with:
    * automatic reconnect (SignalR raw mode)
    * JWT refresh (< 24 h hard expiry)
    * automatic re‑subscription on every `on_open`
    """

    # ------------------------------------------------------------------ cfg
    _WS_BASE = "wss://rtc.thefuturesdesk.projectx.com/hubs/market"
    _API_BASE = "https://gateway.thefuturesdesk.projectx.com"

    _KEEP_ALIVE = 30          # s – ping/pong
    _RECONNECT_INTERVAL = 10  # s between reconnect attempts
    _MAX_RECONNECTS = 10

    _TOKEN_REFRESH_MARGIN = 23 * 3600  # refresh 1 h before hard expiry
    _TOKEN_POLL = 600                  # s between refresh checks (10 min)

    # ---------------------------------------------------------------------
    def __init__(self, token: str):
        self._token: str = token
        self._token_obtained = time.time()
        self._token_lock = threading.Lock()

        # subscription state (used for re‑sub on reconnect)
        self._sub_quotes: Set[str] = set()
        self._sub_trades: Set[str] = set()
        self._sub_depth: Set[str] = set()

        self._running = False  # controls background token thread
        self._build_connection()

    # --------------------------------------------------------- connection
    def _build_connection(self):
        """Create a *new* HubConnection with the current JWT."""
        self._hub_url = f"{self._WS_BASE}?access_token={self._token}"

        self.connection = (
            HubConnectionBuilder()
            .with_url(
                self._hub_url,
                options={
                    "verify_ssl": True,
                    "skip_negotiation": True,
                    "headers": {"Authorization": f"Bearer {self._token}"},
                },
            )
            .configure_logging(logging.INFO, socket_trace=False)
            .with_automatic_reconnect(
                {
                    "type": "raw",
                    "keep_alive_interval": self._KEEP_ALIVE,
                    "reconnect_interval": self._RECONNECT_INTERVAL,
                    "max_reconnect_attempts": self._MAX_RECONNECTS,
                }
            )
            .build()
        )

        # lifecycle hooks – `on_open` fires on first connect AND on every
        # automatic reconnect in signalrcore ≥0.10.0
        self.connection.on_open(self._on_open)
        self.connection.on_close(self._on_close)

    # ---------------------------------------------- lifecycle callbacks
    def _on_open(self):  # noqa: D401  – simple hook
        logging.info("🔌 Connection opened – issuing (re)subscriptions …")
        self._resubscribe_all()

    def _on_close(self):  # noqa: ANN001
        logging.warning("⚠️  Market hub closed – waiting for auto‑reconnect …")

    # --------------------------------------------------- token management
    def _token_refresh_loop(self):
        while self._running:
            time.sleep(self._TOKEN_POLL)
            if time.time() - self._token_obtained > self._TOKEN_REFRESH_MARGIN:
                try:
                    self._refresh_token()
                except Exception as exc:  # noqa: BLE001
                    logging.error("Token refresh failed: %s", exc, exc_info=True)

    def _refresh_token(self):
        """Refresh JWT via `/api/Auth/validate`, rebuild connection."""
        with self._token_lock:
            url = f"{self._API_BASE}/api/Auth/validate"
            res = requests.post(url, headers={"Authorization": f"Bearer {self._token}"}, timeout=10)
            res.raise_for_status()
            new_token = res.json().get("token") or res.json().get("access_token")
            if not new_token:
                raise RuntimeError("validate API did not return a token field")

            self._token = new_token
            self._token_obtained = time.time()
            logging.info("🔑 JWT refreshed – rebuilding connection …")

            # restart websocket with fresh token
            if self.connection.transport.state == WebsocketTransport.state.connected:
                self.connection.stop()
            self._build_connection()
            self.start()  # reconnect & auto‑resub through on_open

    # -------------------------------------------------------- public API
    def start(self) -> bool:
        """Open the hub connection and launch background token thread."""
        try:
            logging.info("Connecting → %s", self._hub_url)
            self.connection.start()
            if not self._running:
                self._running = True
                threading.Thread(target=self._token_refresh_loop, daemon=True).start()
            logging.info("✅ Market hub connected")
            return True
        except Exception as exc:  # noqa: BLE001
            logging.error("Connection failed: %s", exc, exc_info=True)
            return False

    def stop(self):
        """Close hub and stop all background work."""
        self._running = False
        self.unsubscribe_all()
        try:
            self.connection.stop()
        finally:
            logging.info("🛑 Market hub stopped")

    # ------------------------------------------------------ subscriptions
    def subscribe_contract_quotes(self, cid: str):
        self.connection.send("SubscribeContractQuotes", [cid])
        self._sub_quotes.add(cid)

    def subscribe_contract_trades(self, cid: str):
        self.connection.send("SubscribeContractTrades", [cid])
        self._sub_trades.add(cid)

    def subscribe_contract_market_depth(self, cid: str):
        self.connection.send("SubscribeContractMarketDepth", [cid])
        self._sub_depth.add(cid)

    def subscribe_contract_all(self, cid: str):
        self.subscribe_contract_quotes(cid)
        self.subscribe_contract_trades(cid)
        self.subscribe_contract_market_depth(cid)

    # ---- unsubscribe helpers -------------------------------------------
    def unsubscribe_contract_quotes(self, cid: str):
        self.connection.send("UnsubscribeContractQuotes", [cid])
        self._sub_quotes.discard(cid)

    def unsubscribe_contract_trades(self, cid: str):
        self.connection.send("UnsubscribeContractTrades", [cid])
        self._sub_trades.discard(cid)

    def unsubscribe_contract_market_depth(self, cid: str):
        self.connection.send("UnsubscribeContractMarketDepth", [cid])
        self._sub_depth.discard(cid)

    def unsubscribe_contract_all(self, cid: str):
        self.unsubscribe_contract_quotes(cid)
        self.unsubscribe_contract_trades(cid)
        self.unsubscribe_contract_market_depth(cid)

    def unsubscribe_all(self):
        for cid in list(self._sub_quotes):
            self.unsubscribe_contract_quotes(cid)
        for cid in list(self._sub_trades):
            self.unsubscribe_contract_trades(cid)
        for cid in list(self._sub_depth):
            self.unsubscribe_contract_market_depth(cid)

    # --------------------------------------------------------- event hooks
    def on_quote_update(self, handler: Callable):
        self.connection.on("GatewayQuote", handler)

    def on_trade_update(self, handler: Callable):
        self.connection.on("GatewayTrade", handler)

    def on_depth_update(self, handler: Callable):
        self.connection.on("GatewayDepth", handler)

    # ----------------------------------------------------- diagnostics
    def is_connected(self) -> bool:
        return self.connection.transport.state == WebsocketTransport.state.connected

    def get_all_subscribed_contracts(self):
        return list(self._sub_quotes | self._sub_trades | self._sub_depth)

    # ----------------------------------------------------- internals
    def _resubscribe_all(self):
        for cid in self._sub_quotes:
            self.connection.send("SubscribeContractQuotes", [cid])
        for cid in self._sub_trades:
            self.connection.send("SubscribeContractTrades", [cid])
        for cid in self._sub_depth:
            self.connection.send("SubscribeContractMarketDepth", [cid])

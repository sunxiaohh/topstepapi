from signalrcore.hub_connection_builder import HubConnectionBuilder
from signalrcore.protocol.json_hub_protocol import JsonHubProtocol
from signalrcore.transport.websockets.websocket_transport import WebsocketTransport
import logging
import threading
import time

class MarketDataClient:
    def __init__(self, token: str):
        # Use the market hub URL from your reference
        base_url = "wss://rtc.thefuturesdesk.projectx.com/hubs/"
        self.hub_url = f"{base_url}market?access_token={token}"
        self.token = token
        
        # Custom handlers
        self._disconnect_handlers = []
        self._reconnect_handlers = []
        
        # Build connection with proper configuration
        self.connection = HubConnectionBuilder()\
            .with_url(self.hub_url, options={
                "verify_ssl": True,
                "skip_negotiation": True,
                "headers": {
                    "Authorization": f"Bearer {self.token}"
                }
            })\
            .configure_logging(logging.WARNING)\
            .with_automatic_reconnect({
                "type": "raw",
                "keep_alive_interval": 30,
                "reconnect_interval": 5,
                "max_reconnect_attempts": 10
            })\
            .build()
        
        # Set up reconnection handlers
        self.connection.on_open(self._on_open)
        self.connection.on_close(self._on_close)
        self.connection.on_error(self._on_error)
        
        # Store subscription state for reconnection
        self._subscribed_quotes = set()  # Contract IDs subscribed to quotes
        self._subscribed_trades = set()  # Contract IDs subscribed to trades
        self._subscribed_depth = set()   # Contract IDs subscribed to market depth
        
        # Connection state management
        self._is_connected = threading.Event()
        self._connection_lock = threading.Lock()

    def _on_open(self):
        """Handle connection opened"""
        print("✅ Market Data Connection Opened")
        self._is_connected.set()
        
        # Re-subscribe to all previous subscriptions
        if self._subscribed_quotes or self._subscribed_trades or self._subscribed_depth:
            print("🔄 Re-subscribing to previous subscriptions...")
            self._resubscribe_all()
        
        # Call custom reconnect handlers
        for handler in self._reconnect_handlers:
            try:
                handler()
            except Exception as e:
                print(f"Error in reconnect handler: {e}")

    def _on_close(self):
        """Handle connection closed"""
        print("⚠️ Market Data Connection Closed")
        self._is_connected.clear()
        
        # Call custom disconnect handlers
        for handler in self._disconnect_handlers:
            try:
                handler()
            except Exception as e:
                print(f"Error in disconnect handler: {e}")

    def _on_error(self, error):
        """Handle connection error"""
        print(f"❌ Market Data Connection Error: {error}")

    def _resubscribe_all(self):
        """Re-subscribe to all previous subscriptions after reconnection"""
        # Re-subscribe to quotes
        for contract_id in list(self._subscribed_quotes):
            try:
                print(f"  → Re-subscribing to quotes for {contract_id}")
                self.connection.send("SubscribeContractQuotes", [contract_id])
            except Exception as e:
                print(f"    ❌ Failed: {e}")
        
        # Re-subscribe to trades
        for contract_id in list(self._subscribed_trades):
            try:
                print(f"  → Re-subscribing to trades for {contract_id}")
                self.connection.send("SubscribeContractTrades", [contract_id])
            except Exception as e:
                print(f"    ❌ Failed: {e}")
        
        # Re-subscribe to depth
        for contract_id in list(self._subscribed_depth):
            try:
                print(f"  → Re-subscribing to market depth for {contract_id}")
                self.connection.send("SubscribeContractMarketDepth", [contract_id])
            except Exception as e:
                print(f"    ❌ Failed: {e}")

    def on_disconnect(self, handler):
        """Register a custom disconnect handler"""
        self._disconnect_handlers.append(handler)
        return self

    def on_reconnect(self, handler):
        """Register a custom reconnect handler"""
        self._reconnect_handlers.append(handler)
        return self

    def start(self):
        """Start the market data connection"""
        try:
            with self._connection_lock:
                print(f"🔌 Connecting to Market Data Hub...")
                print(f"   URL: {self.hub_url[:50]}...")
                self.connection.start()
                
                # Wait for connection to be established (max 5 seconds)
                if self._is_connected.wait(timeout=5):
                    print("✅ Market data connection started successfully")
                    return True
                else:
                    print("⚠️ Connection started but not confirmed within timeout")
                    return True  # Connection might still be establishing
                    
        except Exception as e:
            print(f"❌ Failed to start market data connection: {e}")
            return False

    def stop(self):
        """Stop the market data connection"""
        try:
            with self._connection_lock:
                # Clear all handlers first
                self._disconnect_handlers.clear()
                self._reconnect_handlers.clear()
                
                # Unsubscribe from all before stopping
                self.unsubscribe_all()
                
                # Stop connection
                self.connection.stop()
                self._is_connected.clear()
                print("✅ Market data connection stopped")
        except Exception as e:
            print(f"⚠️ Error stopping market data connection: {e}")

    def subscribe_contract_quotes(self, contract_id: str):
        """Subscribe to quote updates for specific contract"""
        try:
            self.connection.send("SubscribeContractQuotes", [contract_id])
            self._subscribed_quotes.add(contract_id)
            print(f"✅ Subscribed to quotes for contract {contract_id}")
            return True
        except Exception as e:
            print(f"❌ Failed to subscribe to quotes for contract {contract_id}: {e}")
            return False

    def subscribe_contract_trades(self, contract_id: str):
        """Subscribe to trade updates for specific contract"""
        try:
            self.connection.send("SubscribeContractTrades", [contract_id])
            self._subscribed_trades.add(contract_id)
            print(f"✅ Subscribed to trades for contract {contract_id}")
            return True
        except Exception as e:
            print(f"❌ Failed to subscribe to trades for contract {contract_id}: {e}")
            return False

    def subscribe_contract_market_depth(self, contract_id: str):
        """Subscribe to market depth updates for specific contract"""
        try:
            self.connection.send("SubscribeContractMarketDepth", [contract_id])
            self._subscribed_depth.add(contract_id)
            print(f"✅ Subscribed to market depth for contract {contract_id}")
            return True
        except Exception as e:
            print(f"❌ Failed to subscribe to market depth for contract {contract_id}: {e}")
            return False

    def subscribe_contract_all(self, contract_id: str):
        """Subscribe to all market data for a contract (quotes, trades, depth)"""
        results = []
        results.append(self.subscribe_contract_quotes(contract_id))
        results.append(self.subscribe_contract_trades(contract_id))
        results.append(self.subscribe_contract_market_depth(contract_id))
        return all(results)

    def unsubscribe_contract_quotes(self, contract_id: str):
        """Unsubscribe from quote updates for specific contract"""
        try:
            self.connection.send("UnsubscribeContractQuotes", [contract_id])
            self._subscribed_quotes.discard(contract_id)
            print(f"✅ Unsubscribed from quotes for contract {contract_id}")
            return True
        except Exception as e:
            print(f"❌ Failed to unsubscribe from quotes for contract {contract_id}: {e}")
            return False

    def unsubscribe_contract_trades(self, contract_id: str):
        """Unsubscribe from trade updates for specific contract"""
        try:
            self.connection.send("UnsubscribeContractTrades", [contract_id])
            self._subscribed_trades.discard(contract_id)
            print(f"✅ Unsubscribed from trades for contract {contract_id}")
            return True
        except Exception as e:
            print(f"❌ Failed to unsubscribe from trades for contract {contract_id}: {e}")
            return False

    def unsubscribe_contract_market_depth(self, contract_id: str):
        """Unsubscribe from market depth updates for specific contract"""
        try:
            self.connection.send("UnsubscribeContractMarketDepth", [contract_id])
            self._subscribed_depth.discard(contract_id)
            print(f"✅ Unsubscribed from market depth for contract {contract_id}")
            return True
        except Exception as e:
            print(f"❌ Failed to unsubscribe from market depth for contract {contract_id}: {e}")
            return False

    def unsubscribe_contract_all(self, contract_id: str):
        """Unsubscribe from all market data for a contract"""
        results = []
        results.append(self.unsubscribe_contract_quotes(contract_id))
        results.append(self.unsubscribe_contract_trades(contract_id))
        results.append(self.unsubscribe_contract_market_depth(contract_id))
        return all(results)

    def unsubscribe_all(self):
        """Unsubscribe from all market data updates"""
        # Unsubscribe from all contracts
        for contract_id in list(self._subscribed_quotes):
            self.unsubscribe_contract_quotes(contract_id)
        
        for contract_id in list(self._subscribed_trades):
            self.unsubscribe_contract_trades(contract_id)
        
        for contract_id in list(self._subscribed_depth):
            self.unsubscribe_contract_market_depth(contract_id)

    def on_quote_update(self, handler):
        """Register handler for quote updates - matches JS 'GatewayQuote'"""
        try:
            self.connection.on("GatewayQuote", handler)
            print("✅ Quote update handler registered")
            return self
        except Exception as e:
            print(f"❌ Failed to register quote handler: {e}")
            return self

    def on_trade_update(self, handler):
        """Register handler for trade updates - matches JS 'GatewayTrade'"""
        try:
            self.connection.on("GatewayTrade", handler)
            print("✅ Trade update handler registered")
            return self
        except Exception as e:
            print(f"❌ Failed to register trade handler: {e}")
            return self

    def on_depth_update(self, handler):
        """Register handler for market depth updates - matches JS 'GatewayDepth'"""
        try:
            self.connection.on("GatewayDepth", handler)
            print("✅ Market depth update handler registered")
            return self
        except Exception as e:
            print(f"❌ Failed to register depth handler: {e}")
            return self

    def is_connected(self):
        """Check if connection is active"""
        return self._is_connected.is_set()

    def wait_for_connection(self, timeout=10):
        """Wait for connection to be established"""
        return self._is_connected.wait(timeout=timeout)

    def get_connection_state(self):
        """Get current connection state for debugging"""
        try:
            state = "Unknown"
            state_raw = None
            
            if hasattr(self.connection, 'transport') and hasattr(self.connection.transport, 'state'):
                state_raw = self.connection.transport.state
                
                if hasattr(state_raw, 'value'):
                    if state_raw.value == 1:
                        state = "Connected"
                    elif state_raw.value == 0:
                        state = "Connecting"
                    elif state_raw.value == 2:
                        state = "Disconnected"
                    else:
                        state = f"Unknown({state_raw.value})"
                else:
                    state_str = str(state_raw)
                    if 'connected: 1' in state_str:
                        state = "Connected"
                    elif 'connecting: 0' in state_str:
                        state = "Connecting"
                    elif 'disconnected: 2' in state_str:
                        state = "Disconnected"
                    else:
                        state = state_str
            
            return {
                "state": state,
                "state_raw": str(state_raw) if state_raw else None,
                "is_connected": self.is_connected(),
                "url": self.hub_url[:50] + "...",
                "transport": "WebSocket Secure (wss://)",
                "skip_negotiation": True,
                "verify_ssl": True,
                "subscriptions": {
                    "quotes_contracts": list(self._subscribed_quotes),
                    "trades_contracts": list(self._subscribed_trades),
                    "depth_contracts": list(self._subscribed_depth),
                    "total_subscriptions": len(self._subscribed_quotes) + len(self._subscribed_trades) + len(self._subscribed_depth)
                }
            }
        except Exception as e:
            return {"error": str(e), "is_connected": self.is_connected()}

    def get_subscribed_contracts(self):
        """Get list of contracts currently subscribed to"""
        return {
            "quotes": list(self._subscribed_quotes),
            "trades": list(self._subscribed_trades),
            "depth": list(self._subscribed_depth)
        }

    def get_all_subscribed_contracts(self):
        """Get unique list of all contracts with any subscription"""
        all_contracts = set()
        all_contracts.update(self._subscribed_quotes)
        all_contracts.update(self._subscribed_trades)
        all_contracts.update(self._subscribed_depth)
        return list(all_contracts)

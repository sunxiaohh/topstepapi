from signalrcore.hub_connection_builder import HubConnectionBuilder
from signalrcore.protocol.json_hub_protocol import JsonHubProtocol
from signalrcore.transport.websockets.websocket_transport import WebsocketTransport
import logging

class MarketDataClient:
    def __init__(self, token: str):
        # Use the market hub URL from your reference
        base_url = "wss://rtc.thefuturesdesk.projectx.com/hubs/"
        self.hub_url = f"{base_url}market?access_token={token}"
        self.token = token
        
        # Build connection with proper configuration
        self.connection = HubConnectionBuilder()\
            .with_url(self.hub_url, options={
                "verify_ssl": True,
                "skip_negotiation": True,
                "headers": {
                    "Authorization": f"Bearer {self.token}"
                }
            })\
            .configure_logging(logging.DEBUG, socket_trace=True)\
            .with_automatic_reconnect({
                "type": "raw",
                "keep_alive_interval": 30,
                "reconnect_interval": 10,
                "max_reconnect_attempts": 10
            })\
            .build()
        
        # Set up reconnection handler to re-subscribe
        self.connection.on("reconnect", self._on_reconnected)
        
        # Store subscription state for reconnection
        self._subscribed_quotes = set()  # Contract IDs subscribed to quotes
        self._subscribed_trades = set()  # Contract IDs subscribed to trades
        self._subscribed_depth = set()   # Contract IDs subscribed to market depth

    def _on_reconnected(self, connection_id):
        """Handle reconnection by re-subscribing to all previous subscriptions"""
        print(f"Market Data Connection Reconnected with ID: {connection_id}")
        
        # Re-subscribe to all previous subscriptions
        for contract_id in self._subscribed_quotes:
            print(f"Re-subscribing to quotes for contract {contract_id}...")
            try:
                self.connection.send("SubscribeContractQuotes", [contract_id])
            except Exception as e:
                print(f"Error re-subscribing to quotes: {e}")
        
        for contract_id in self._subscribed_trades:
            print(f"Re-subscribing to trades for contract {contract_id}...")
            try:
                self.connection.send("SubscribeContractTrades", [contract_id])
            except Exception as e:
                print(f"Error re-subscribing to trades: {e}")
        
        for contract_id in self._subscribed_depth:
            print(f"Re-subscribing to market depth for contract {contract_id}...")
            try:
                self.connection.send("SubscribeContractMarketDepth", [contract_id])
            except Exception as e:
                print(f"Error re-subscribing to market depth: {e}")

    def start(self):
        """Start the market data connection"""
        try:
            print(f"🔌 Connecting to Market Data: {self.hub_url}")
            print("⚙️ Using WebSocket Secure (wss://) with skipNegotiation=True")
            self.connection.start()
            print("✅ Market data connection started successfully")
            return True
        except Exception as e:
            print(f"❌ Failed to start market data connection: {e}")
            print(f"Error details: {str(e)}")
            return False

    def stop(self):
        """Stop the market data connection"""
        try:
            # Unsubscribe from all before stopping
            self.unsubscribe_all()
            self.connection.stop()
            print("✅ Market data connection stopped")
        except Exception as e:
            print(f"⚠️ Error stopping market data connection: {e}")

    def subscribe_contract_quotes(self, contract_id: str):
        """Subscribe to quote updates for specific contract"""
        try:
            self.connection.send("SubscribeContractQuotes", [contract_id])
            self._subscribed_quotes.add(contract_id)
            print(f"✅ Subscribed to quotes for contract {contract_id}")
        except Exception as e:
            print(f"❌ Failed to subscribe to quotes for contract {contract_id}: {e}")

    def subscribe_contract_trades(self, contract_id: str):
        """Subscribe to trade updates for specific contract"""
        try:
            self.connection.send("SubscribeContractTrades", [contract_id])
            self._subscribed_trades.add(contract_id)
            print(f"✅ Subscribed to trades for contract {contract_id}")
        except Exception as e:
            print(f"❌ Failed to subscribe to trades for contract {contract_id}: {e}")

    def subscribe_contract_market_depth(self, contract_id: str):
        """Subscribe to market depth updates for specific contract"""
        try:
            self.connection.send("SubscribeContractMarketDepth", [contract_id])
            self._subscribed_depth.add(contract_id)
            print(f"✅ Subscribed to market depth for contract {contract_id}")
        except Exception as e:
            print(f"❌ Failed to subscribe to market depth for contract {contract_id}: {e}")

    def subscribe_contract_all(self, contract_id: str):
        """Subscribe to all market data for a contract (quotes, trades, depth)"""
        self.subscribe_contract_quotes(contract_id)
        self.subscribe_contract_trades(contract_id)
        self.subscribe_contract_market_depth(contract_id)

    def unsubscribe_contract_quotes(self, contract_id: str):
        """Unsubscribe from quote updates for specific contract"""
        try:
            self.connection.send("UnsubscribeContractQuotes", [contract_id])
            self._subscribed_quotes.discard(contract_id)
            print(f"✅ Unsubscribed from quotes for contract {contract_id}")
        except Exception as e:
            print(f"❌ Failed to unsubscribe from quotes for contract {contract_id}: {e}")

    def unsubscribe_contract_trades(self, contract_id: str):
        """Unsubscribe from trade updates for specific contract"""
        try:
            self.connection.send("UnsubscribeContractTrades", [contract_id])
            self._subscribed_trades.discard(contract_id)
            print(f"✅ Unsubscribed from trades for contract {contract_id}")
        except Exception as e:
            print(f"❌ Failed to unsubscribe from trades for contract {contract_id}: {e}")

    def unsubscribe_contract_market_depth(self, contract_id: str):
        """Unsubscribe from market depth updates for specific contract"""
        try:
            self.connection.send("UnsubscribeContractMarketDepth", [contract_id])
            self._subscribed_depth.discard(contract_id)
            print(f"✅ Unsubscribed from market depth for contract {contract_id}")
        except Exception as e:
            print(f"❌ Failed to unsubscribe from market depth for contract {contract_id}: {e}")

    def unsubscribe_contract_all(self, contract_id: str):
        """Unsubscribe from all market data for a contract"""
        self.unsubscribe_contract_quotes(contract_id)
        self.unsubscribe_contract_trades(contract_id)
        self.unsubscribe_contract_market_depth(contract_id)

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
        except Exception as e:
            print(f"❌ Failed to register quote handler: {e}")

    def on_trade_update(self, handler):
        """Register handler for trade updates - matches JS 'GatewayTrade'"""
        try:
            self.connection.on("GatewayTrade", handler)
            print("✅ Trade update handler registered")
        except Exception as e:
            print(f"❌ Failed to register trade handler: {e}")

    def on_depth_update(self, handler):
        """Register handler for market depth updates - matches JS 'GatewayDepth'"""
        try:
            self.connection.on("GatewayDepth", handler)
            print("✅ Market depth update handler registered")
        except Exception as e:
            print(f"❌ Failed to register depth handler: {e}")

    def is_connected(self):
        """Check if connection is active"""
        try:
            if not hasattr(self.connection, 'transport') or not hasattr(self.connection.transport, 'state'):
                return False
            
            state = self.connection.transport.state
            return (hasattr(state, 'value') and state.value == 1) or str(state).endswith('connected: 1>')
        except Exception as e:
            print(f"Error checking market data connection state: {e}")
            return False

    def get_connection_state(self):
        """Get current connection state for debugging"""
        try:
            state = "Unknown"
            state_raw = None
            is_connected = False
            
            if hasattr(self.connection, 'transport') and hasattr(self.connection.transport, 'state'):
                state_raw = self.connection.transport.state
                
                if hasattr(state_raw, 'value'):
                    if state_raw.value == 1:
                        state = "Connected"
                        is_connected = True
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
                        is_connected = True
                    elif 'connecting: 0' in state_str:
                        state = "Connecting"
                    elif 'disconnected: 2' in state_str:
                        state = "Disconnected"
                    else:
                        state = state_str
            
            return {
                "state": state,
                "state_raw": str(state_raw),
                "is_connected": is_connected,
                "url": self.hub_url,
                "transport": "WebSocket Secure (wss://)",
                "skip_negotiation": True,
                "verify_ssl": True,
                "subscriptions": {
                    "quotes_contracts": list(self._subscribed_quotes),
                    "trades_contracts": list(self._subscribed_trades),
                    "depth_contracts": list(self._subscribed_depth)
                }
            }
        except Exception as e:
            return {"error": str(e), "raw_error": repr(e)}

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

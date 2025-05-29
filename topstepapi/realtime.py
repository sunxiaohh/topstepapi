from sign```python
from signalrcore.hub_connection_builder import HubConnectionBuilder
from signalrcore.protocol.json_hub_protocol import JsonHubProtocol
import logging

class RealTimeClient:
    def __init__(self, token: str, hub: str = "user"):
        # Use the correct production URL from the JavaScript code
        base_url = "https://rtc.topstepx.com/hubs/"
        self.hub_url = f"{base_url}{hub}"
        self.token = token
        
        # Build connection with improved settings based on JS code
        self.connection = HubConnectionBuilder()\
            .with_url(self.hub_url, options={
                "access_token_factory": lambda: self.token,
                "headers": {
                    "Authorization": f"Bearer {self.token}"
                }
            })\
            .with_automatic_reconnect({
                "type": "raw",
                "keep_alive_interval": 30,
                "reconnect_interval": 10,
                "max_reconnect_attempts": 10
            })\
            .build()
        
        # Set up reconnection handler to re-subscribe
        self.connection.on_reconnect = self._on_reconnected
        
        # Store subscription state for reconnection (account_id will be stored when subscribing)
        self._subscribed_accounts = False
        self._subscribed_orders_accounts = set()  # Can subscribe to multiple accounts
        self._subscribed_positions_accounts = set()
        self._subscribed_trades_accounts = set()

    def _on_reconnected(self, connection_id):
        """Handle reconnection by re-subscribing to all previous subscriptions"""
        print(f"RTC Connection Reconnected with ID: {connection_id}")
        
        # Re-subscribe to all previous subscriptions
        if self._subscribed_accounts:
            print("Re-subscribing to accounts...")
            self.connection.send("SubscribeAccounts", [])
        
        for account_id in self._subscribed_orders_accounts:
            print(f"Re-subscribing to orders for account {account_id}...")
            self.connection.send("SubscribeOrders", [account_id])
        
        for account_id in self._subscribed_positions_accounts:
            print(f"Re-subscribing to positions for account {account_id}...")
            self.connection.send("SubscribePositions", [account_id])
        
        for account_id in self._subscribed_trades_accounts:
            print(f"Re-subscribing to trades for account {account_id}...")
            self.connection.send("SubscribeTrades", [account_id])

    def start(self):
        """Start the real-time connection"""
        try:
            self.connection.start()
            print("✅ Real-time connection started successfully")
            return True
        except Exception as e:
            print(f"❌ Failed to start real-time connection: {e}")
            return False

    def stop(self):
        """Stop the real-time connection"""
        try:
            # Unsubscribe from all before stopping
            self.unsubscribe_all()
            self.connection.stop()
            print("✅ Real-time connection stopped")
        except Exception as e:
            print(f"⚠️ Error stopping connection: {e}")

    def subscribe_accounts(self):
        """Subscribe to account updates"""
        try:
            self.connection.send("SubscribeAccounts", [])
            self._subscribed_accounts = True
            print("✅ Subscribed to account updates")
        except Exception as e:
            print(f"❌ Failed to subscribe to accounts: {e}")

    def subscribe_orders(self, account_id):
        """Subscribe to order updates for specific account"""
        try:
            self.connection.send("SubscribeOrders", [account_id])
            self._subscribed_orders_accounts.add(account_id)
            print(f"✅ Subscribed to order updates for account {account_id}")
        except Exception as e:
            print(f"❌ Failed to subscribe to orders for account {account_id}: {e}")

    def subscribe_positions(self, account_id):
        """Subscribe to position updates for specific account"""
        try:
            self.connection.send("SubscribePositions", [account_id])
            self._subscribed_positions_accounts.add(account_id)
            print(f"✅ Subscribed to position updates for account {account_id}")
        except Exception as e:
            print(f"❌ Failed to subscribe to positions for account {account_id}: {e}")

    def subscribe_trades(self, account_id):
        """Subscribe to trade updates for specific account"""
        try:
            self.connection.send("SubscribeTrades", [account_id])
            self._subscribed_trades_accounts.add(account_id)
            print(f"✅ Subscribed to trade updates for account {account_id}")
        except Exception as e:
            print(f"❌ Failed to subscribe to trades for account {account_id}: {e}")

    def unsubscribe_accounts(self):
        """Unsubscribe from account updates"""
        try:
            self.connection.send("UnsubscribeAccounts", [])
            self._subscribed_accounts = False
            print("✅ Unsubscribed from account updates")
        except Exception as e:
            print(f"❌ Failed to unsubscribe from accounts: {e}")

    def unsubscribe_orders(self, account_id):
        """Unsubscribe from order updates for specific account"""
        try:
            self.connection.send("UnsubscribeOrders", [account_id])
            self._subscribed_orders_accounts.discard(account_id)
            print(f"✅ Unsubscribed from order updates for account {account_id}")
        except Exception as e:
            print(f"❌ Failed to unsubscribe from orders for account {account_id}: {e}")

    def unsubscribe_positions(self, account_id):
        """Unsubscribe from position updates for specific account"""
        try:
            self.connection.send("UnsubscribePositions", [account_id])
            self._subscribed_positions_accounts.discard(account_id)
            print(f"✅ Unsubscribed from position updates for account {account_id}")
        except Exception as e:
            print(f"❌ Failed to unsubscribe from positions for account {account_id}: {e}")

    def unsubscribe_trades(self, account_id):
        """Unsubscribe from trade updates for specific account"""
        try:
            self.connection.send("UnsubscribeTrades", [account_id])
            self._subscribed_trades_accounts.discard(account_id)
            print(f"✅ Unsubscribed from trade updates for account {account_id}")
        except Exception as e:
            print(f"❌ Failed to unsubscribe from trades for account {account_id}: {e}")

    def unsubscribe_all(self):
        """Unsubscribe from all updates"""
        if self._subscribed_accounts:
            self.unsubscribe_accounts()
        
        # Unsubscribe from all account-specific subscriptions
        for account_id in list(self._subscribed_orders_accounts):
            self.unsubscribe_orders(account_id)
        
        for account_id in list(self._subscribed_positions_accounts):
            self.unsubscribe_positions(account_id)
        
        for account_id in list(self._subscribed_trades_accounts):
            self.unsubscribe_trades(account_id)

    def on_account_update(self, handler):
        """Register handler for account updates - matches JS 'GatewayUserAccount'"""
        try:
            self.connection.on("GatewayUserAccount", handler)
            print("✅ Account update handler registered")
        except Exception as e:
            print(f"❌ Failed to register account handler: {e}")

    def on_order_update(self, handler):
        """Register handler for order updates - matches JS 'GatewayUserOrder'"""
        try:
            self.connection.on("GatewayUserOrder", handler)
            print("✅ Order update handler registered")
        except Exception as e:
            print(f"❌ Failed to register order handler: {e}")

    def on_position_update(self, handler):
        """Register handler for position updates - matches JS 'GatewayUserPosition'"""
        try:
            self.connection.on("GatewayUserPosition", handler)
            print("✅ Position update handler registered")
        except Exception as e:
            print(f"❌ Failed to register position handler: {e}")

    def on_trade_update(self, handler):
        """Register handler for trade updates - matches JS 'GatewayUserTrade'"""
        try:
            self.connection.on("GatewayUserTrade", handler)
            print("✅ Trade update handler registered")
        except Exception as e:
            print(f"❌ Failed to register trade handler: {e}")

    def is_connected(self):
        """Check if connection is active"""
        try:
            return self.connection.transport.state == "Connected"
        except:
            return False

    def get_connection_state(self):
        """Get current connection state for debugging"""
        try:
            return {
                "state": self.connection.transport.state,
                "url": self.hub_url,
                "subscriptions": {
                    "accounts": self._subscribed_accounts,
                    "orders_accounts": list(self._subscribed_orders_accounts),
                    "positions_accounts": list(self._subscribed_positions_accounts),
                    "trades_accounts": list(self._subscribed_trades_accounts)
                }
            }
        except Exception as e:
            return {"error": str(e)}

    def get_subscribed_accounts(self):
        """Get list of accounts currently subscribed to"""
        return {
            "accounts_general": self._subscribed_accounts,
            "orders": list(self._subscribed_orders_accounts),
            "positions": list(self._subscribed_positions_accounts),
            "trades": list(self._subscribed_trades_accounts)
        }

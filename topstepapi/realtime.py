#!/usr/bin/env python3
"""
TopStep Trading Activity Logger
Creates trading activity and records real-time streaming data from TopstepAPI
Uses the official TopstepAPI functions as documented
"""

import json
import time
import logging
from datetime import datetime
from topstepapi import TopstepClient

# Configure logging to suppress SignalR warnings
logging.getLogger("SignalRCoreClient").setLevel(logging.ERROR)

class TopstepTradingLogger:
    def __init__(self, username: str, api_key: str, account_id: int):
        """Initialize the TopstepAPI client and logger"""
        self.client = TopstepClient(username, api_key)
        self.account_id = account_id
        self.log_file = f"topstep_realtime_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        self.data_log = []
        self.placed_orders = []  # Keep track of orders for cleanup
        
    def log_realtime_event(self, event_type: str, data):
        """Log real-time events with detailed analysis"""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type,
            "data": data,
            "data_type": type(data).__name__,
            "data_keys": list(data.keys()) if isinstance(data, dict) else None,
            "data_length": len(data) if isinstance(data, (list, dict)) else None
        }
        
        self.data_log.append(log_entry)
        
        # Print to console
        print(f"\n🔥 REAL-TIME EVENT: {event_type}")
        print(f"⏰ Time: {log_entry['timestamp']}")
        print(f"📊 Data Type: {log_entry['data_type']}")
        if log_entry['data_keys']:
            print(f"🔑 Keys: {log_entry['data_keys']}")
        print("📋 Raw Data:")
        print(json.dumps(data, indent=2, default=str))
        print("=" * 80)
        
        # Save to file
        self._save_to_file()
    
    def _save_to_file(self):
        """Save log data to JSON file"""
        try:
            with open(self.log_file, 'w') as f:
                json.dump(self.data_log, f, indent=2, default=str)
        except Exception as e:
            print(f"❌ Error saving to file: {e}")
    
    def setup_realtime_handlers(self):
        """Set up real-time event handlers as per documentation"""
        def on_account_update(data):
            self.log_realtime_event("ACCOUNT_UPDATE", data)
        
        def on_order_update(data):
            self.log_realtime_event("ORDER_UPDATE", data)
        
        def on_position_update(data):
            self.log_realtime_event("POSITION_UPDATE", data)
        
        def on_trade_update(data):
            self.log_realtime_event("TRADE_UPDATE", data)
        
        # Register event handlers as shown in documentation
        self.client.realtime.on_account_update(on_account_update)
        self.client.realtime.on_order_update(on_order_update)
        self.client.realtime.on_position_update(on_position_update)
        self.client.realtime.on_trade_update(on_trade_update)
        
        print("✅ Real-time event handlers registered")
    
    def start_realtime_connection(self):
        """Start real-time connection with improved settings"""
        try:
            # Register handlers FIRST before starting connection
            self.setup_realtime_handlers()
            
            # Start the connection using the improved client
            success = self.client.realtime.start()
            if not success:
                print("❌ Failed to start real-time connection")
                return False
            
            # Wait for connection to stabilize
            import time
            time.sleep(3)
            
            # Check connection state
            state = self.client.realtime.get_connection_state()
            print(f"🔍 Connection state: {state}")
            
            # Subscribe to all updates with delays
            print("📡 Setting up subscriptions...")
            
            self.client.realtime.subscribe_accounts()
            time.sleep(1)
            
            self.client.realtime.subscribe_orders(account_id=self.account_id)
            time.sleep(1)
            
            self.client.realtime.subscribe_positions(account_id=self.account_id)
            time.sleep(1)
            
            self.client.realtime.subscribe_trades(account_id=self.account_id)
            time.sleep(1)
            
            print("✅ All real-time subscriptions completed")
            return True
            
        except Exception as e:
            print(f"❌ Real-time connection failed: {e}")
            return False
    
    def get_test_contract(self):
        """Find a suitable contract for testing"""
        try:
            # Search for NQ contracts as shown in documentation
            print("🔍 Searching for NQ contracts...")
            contracts = self.client.contract.search_contracts("NQ")
            
            if contracts:
                # Find an active contract
                for contract in contracts:
                    if contract.get('isActive', False):
                        contract_id = contract.get('contractId')
                        print(f"✅ Found active NQ contract: {contract_id}")
                        return contract_id
            
            # Fallback to ES contracts
            print("🔍 Searching for ES contracts...")
            contracts = self.client.contract.search_contracts("ES")
            
            if contracts:
                for contract in contracts:
                    if contract.get('isActive', False):
                        contract_id = contract.get('contractId')
                        print(f"✅ Found active ES contract: {contract_id}")
                        return contract_id
            
            print("❌ No suitable contracts found")
            return None
            
        except Exception as e:
            print(f"❌ Error searching contracts: {e}")
            return None
    
    def create_trading_activity(self):
        """Create REAL trading activity using market orders (simulation account)"""
        print("\n📈 CREATING REAL TRADING ACTIVITY (SIMULATION)")
        print("=" * 60)
        print("🎯 Using MARKET ORDERS for maximum real-time events!")
        
        # Get a test contract
        contract_id = self.get_test_contract()
        if not contract_id:
            return False
        
        try:
            # 1. Place a MARKET BUY order (will execute immediately)
            print("\n1️⃣ Placing MARKET BUY order...")
            market_buy_id = self.client.order.place_order(
                account_id=self.account_id,
                contract_id=contract_id,
                type=2,  # Market order
                side=1,  # Buy
                size=1
            )
            
            if market_buy_id:
                print(f"✅ Market buy order executed with ID: {market_buy_id}")
                time.sleep(5)  # Wait for execution and real-time events
            
            # 2. Place a LIMIT SELL order (to close position later)
            print("\n2️⃣ Placing LIMIT SELL order...")
            limit_sell_id = self.client.order.place_order(
                account_id=self.account_id,
                contract_id=contract_id,
                type=1,  # Limit order
                side=2,  # Sell
                size=1,
                limit_price=99999  # High price to avoid immediate execution
            )
            
            if limit_sell_id:
                print(f"✅ Limit sell order placed with ID: {limit_sell_id}")
                self.placed_orders.append(limit_sell_id)
                time.sleep(3)  # Wait for real-time event
            
            # 3. Modify the limit sell order
            if limit_sell_id:
                print("\n3️⃣ Modifying limit sell order...")
                self.client.order.modify_order(
                    account_id=self.account_id,
                    order_id=limit_sell_id,
                    size=1,
                    limit_price=88888  # Different high price
                )
                print("✅ Sell order modified")
                time.sleep(3)  # Wait for real-time event
            
            # 4. Place another MARKET order (opposite direction)
            print("\n4️⃣ Placing MARKET SELL order...")
            market_sell_id = self.client.order.place_order(
                account_id=self.account_id,
                contract_id=contract_id,
                type=2,  # Market order
                side=2,  # Sell
                size=1
            )
            
            if market_sell_id:
                print(f"✅ Market sell order executed with ID: {market_sell_id}")
                time.sleep(5)  # Wait for execution and real-time events
            
            # 5. Cancel the remaining limit order
            if limit_sell_id:
                print("\n5️⃣ Cancelling remaining limit order...")
                self.client.order.cancel_order(
                    account_id=self.account_id,
                    order_id=limit_sell_id
                )
                print("✅ Limit order cancelled")
                time.sleep(3)  # Wait for real-time event
            
            # 6. Check if we have any open positions and close them
            print("\n6️⃣ Checking and closing any open positions...")
            positions = self.client.position.search_open_positions(account_id=self.account_id)
            
            for position in positions:
                pos_contract_id = position.get('contractId')
                pos_size = position.get('size', 0)
                
                if pos_contract_id == contract_id and pos_size != 0:
                    print(f"🔄 Closing position: {pos_size} contracts of {pos_contract_id}")
                    self.client.position.close_position(
                        account_id=self.account_id,
                        contract_id=pos_contract_id
                    )
                    print(f"✅ Position closed via market order")
                    time.sleep(3)  # Wait for real-time event
            
            print("\n🎉 Trading activity sequence completed!")
            print("This should have generated multiple real-time events:")
            print("• Order placement events")
            print("• Order execution/fill events") 
            print("• Position updates")
            print("• Trade confirmations")
            print("• Order modifications")
            print("• Order cancellations")
            
            return True
            
        except Exception as e:
            print(f"❌ Error creating trading activity: {e}")
            print(f"Error details: {str(e)}")
            return False
    
    def check_current_state(self):
        """Check current account state using API functions"""
        print("\n📊 CHECKING CURRENT ACCOUNT STATE")
        print("=" * 40)
        
        try:
            # Search for open orders
            open_orders = self.client.order.search_open_orders(account_id=self.account_id)
            print(f"📋 Open orders: {len(open_orders)}")
            for order in open_orders[:3]:  # Show first 3
                print(f"  - Order ID: {order.get('orderId')}, Status: {order.get('status')}")
            
            # Search for open positions
            positions = self.client.position.search_open_positions(account_id=self.account_id)
            print(f"📈 Open positions: {len(positions)}")
            for pos in positions[:3]:  # Show first 3
                print(f"  - Contract: {pos.get('contractId')}, Size: {pos.get('size')}")
            
            # Search recent trades
            from datetime import datetime, timedelta
            start_time = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
            trades = self.client.trade.search_trades(
                account_id=self.account_id,
                start_timestamp=start_time
            )
            print(f"🔄 Recent trades (last hour): {len(trades)}")
            
        except Exception as e:
            print(f"❌ Error checking account state: {e}")
    
    def cleanup_test_orders(self):
        """Cancel any remaining test orders"""
        print("\n🧹 CLEANING UP TEST ORDERS")
        print("=" * 30)
        
        for order_id in self.placed_orders:
            try:
                self.client.order.cancel_order(
                    account_id=self.account_id,
                    order_id=order_id
                )
                print(f"✅ Cancelled order {order_id}")
            except Exception as e:
                print(f"⚠️ Could not cancel order {order_id}: {e}")
    
    def run_session(self, duration_minutes: int = 5):
        """Run a complete trading and logging session"""
        print("🚀 TOPSTEP REAL TRADING ACTIVITY LOGGER")
        print("=" * 70)
        print(f"📅 Session started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"👤 Account ID: {self.account_id}")
        print(f"📁 Log file: {self.log_file}")
        print(f"⏱️ Duration: {duration_minutes} minutes")
        print("🎯 SIMULATION ACCOUNT - Using REAL market orders!")
        print("=" * 70)
        
        # Check initial state
        self.check_current_state()
        
        # Set up real-time handlers
        self.setup_realtime_handlers()
        
        # Start real-time connection
        realtime_connected = self.start_realtime_connection()
        
        if not realtime_connected:
            print("⚠️ Continuing without real-time connection...")
        
        # Create trading activity
        activity_created = self.create_trading_activity()
        
        if activity_created:
            print(f"\n⏰ MONITORING SESSION ({duration_minutes} minutes)")
            print("Listening for real-time events...")
            print("Press Ctrl+C to stop early")
            print("-" * 40)
            
            # Monitor for specified duration
            end_time = time.time() + (duration_minutes * 60)
            last_status = time.time()
            
            try:
                while time.time() < end_time:
                    time.sleep(1)
                    
                    # Status update every 30 seconds
                    if time.time() - last_status > 30:
                        remaining = int((end_time - time.time()) / 60)
                        events = len(self.data_log)
                        print(f"📊 Status: {events} events captured, {remaining} minutes remaining")
                        last_status = time.time()
                        
            except KeyboardInterrupt:
                print("\n⏹️ Session stopped by user")
        
        # Cleanup
        try:
            self.client.realtime.stop()
            print("✅ Real-time connection stopped")
        except:
            pass
        
        self.cleanup_test_orders()
        self.generate_final_report()
    
    def generate_final_report(self):
        """Generate comprehensive final report"""
        print("\n" + "=" * 80)
        print("📊 FINAL SESSION REPORT")
        print("=" * 80)
        
        if not self.data_log:
            print("❌ NO REAL-TIME EVENTS CAPTURED")
            print("\nPossible reasons:")
            print("• Real-time connection failed to establish")
            print("• Events are not being triggered by order activities")
            print("• SignalR hub connection issues")
            print("• API permissions or configuration problems")
            print("\nRecommendations:")
            print("• Check network connectivity and firewall settings")
            print("• Verify account permissions for real-time data")
            print("• Try running during active market hours")
            print("• Contact TopStep support for SignalR troubleshooting")
            
        else:
            print(f"🎉 SUCCESS! Captured {len(self.data_log)} real-time events")
            
            # Event breakdown
            event_counts = {}
            for entry in self.data_log:
                event_type = entry["event_type"]
                event_counts[event_type] = event_counts.get(event_type, 0) + 1
            
            print(f"\n📈 Event Summary:")
            for event_type, count in event_counts.items():
                print(f"  {event_type}: {count} events")
            
            # Show data structure insights
            print(f"\n🔍 Data Structure Analysis:")
            for entry in self.data_log[:3]:  # Show first few examples
                print(f"\n{entry['event_type']} structure:")
                if entry['data_keys']:
                    print(f"  Keys: {entry['data_keys']}")
                print(f"  Sample data: {json.dumps(entry['data'], indent=4, default=str)[:300]}...")
        
        print(f"\n📁 Complete log saved to: {self.log_file}")
        print("=" * 80)


def test_api_connectivity(username: str, api_key: str, account_id: int):
    """Test basic API connectivity before starting session"""
    print("🔧 TESTING API CONNECTIVITY")
    print("=" * 30)
    
    try:
        client = TopstepClient(username, api_key)
        
        # Test account access
        accounts = client.account.search_accounts()
        print(f"✅ Retrieved {len(accounts)} accounts")
        
        # Verify account ID
        account_ids = [acc.get('id') for acc in accounts]
        if account_id in account_ids:
            print(f"✅ Account ID {account_id} verified")
        else:
            print(f"❌ Account ID {account_id} not found")
            print(f"Available accounts: {account_ids}")
            return False
        
        # Test contract search
        contracts = client.contract.search_contracts("ES")
        print(f"✅ Contract search working ({len(contracts)} ES contracts found)")
        
        return True
        
    except Exception as e:
        print(f"❌ API connectivity test failed: {e}")
        return False


def main():
    """Main execution function"""
    # Configuration - Update with your actual credentials
    USERNAME = "your_username"
    API_KEY = "your_api_key" 
    ACCOUNT_ID = 7914587
    DURATION_MINUTES = 5  # Longer duration to capture all events
    
    print("🏛️ TOPSTEP REAL MARKET TRADING LOGGER")
    print("🎯 SIMULATION ACCOUNT - MARKET ORDERS")
    print("=" * 60)
    
    # Validate configuration
    if USERNAME == "your_username" or API_KEY == "your_api_key":
        print("⚠️ Please update USERNAME and API_KEY variables")
        print("Set your actual TopStep credentials in the script")
        return
    
    # Test API connectivity
    if not test_api_connectivity(USERNAME, API_KEY, ACCOUNT_ID):
        print("❌ API connectivity test failed - cannot proceed")
        return
    
    print("\n" + "=" * 80)
    
    # Run the trading session
    logger = TopstepTradingLogger(USERNAME, API_KEY, ACCOUNT_ID)
    logger.run_session(DURATION_MINUTES)


if __name__ == "__main__":
    main()

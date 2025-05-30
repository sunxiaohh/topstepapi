#!/usr/bin/env python3
"""
Simple OCO (One-Cancels-Other) Order Management System
Built on TopStep API with Real-time Monitoring
"""

import json
import time
from datetime import datetime, timezone
from topstepapi import TopstepClient

class SimpleOCOManager:
    def __init__(self, username: str, api_key: str, account_id: int):
        self.client = TopstepClient(username, api_key)
        self.account_id = account_id
        
        # OCO tracking
        self.oco_pairs = {}  # entry_order_id -> {'stop_order_id': X, 'target_order_id': Y, 'status': 'active'}
        self.oco_order_lookup = {}  # order_id -> {'entry_id': X, 'type': 'stop'/'target'}
        self.order_events = []
        self.position_events = []
        
        # OCO settings
        self.stop_distance = 6  # points
        self.target_distance = 6  # points
        
    def get_current_price(self, contract_id: str) -> float:
        """Get latest 1-minute bar close price as market reference"""
        try:
            # Get current time in UTC
            end_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            
            # Get latest 1-minute bar
            bars = self.client.history.retrieve_bars(
                contract_id=contract_id,
                live=False,
                start_time="2024-12-01T00:00:00Z",  # Start from recent date
                end_time=end_time,
                unit=2,  # minute
                unit_number=1,
                limit=1,  # Just get the latest bar
                include_partial_bar=False
            )
            
            if bars and len(bars) > 0:
                latest_bar = bars[-1]  # Get the most recent bar
                close_price = latest_bar['c']
                print(f"📊 Latest 1-min close price: ${close_price}")
                return float(close_price)
            else:
                print("⚠️ No price data available")
                return None
                
        except Exception as e:
            print(f"❌ Error getting current price: {e}")
            return None
    
    def create_oco_order(self, contract_id: str, side: int = 0, size: int = 1):
        """
        Create OCO order set: Entry + Stop + Target
        side: 0=Buy, 1=Sell
        """
        print(f"\n🎯 CREATING OCO ORDER SET")
        print("=" * 40)
        
        # Step 1: Get current market price
        market_price = self.get_current_price(contract_id)
        if not market_price:
            print("❌ Cannot create OCO without market price")
            return None
        
        # Step 2: Calculate stop and target levels
        if side == 0:  # Buy entry (going long)
            stop_price = market_price - self.stop_distance
            target_price = market_price + self.target_distance
            bracket_side = 1  # Sell (Ask) to close long position
        else:  # Sell entry (going short)
            stop_price = market_price + self.stop_distance  
            target_price = market_price - self.target_distance
            bracket_side = 0  # Buy (Bid) to close short position
        
        print(f"📈 Market Price: ${market_price}")
        print(f"🛑 Stop Loss: ${stop_price}")
        print(f"🎯 Target: ${target_price}")
        
        try:
            # Step 3: Place entry order (market order)
            print("1️⃣ Placing entry order...")
            entry_order_id = self.client.order.place_order(
                account_id=self.account_id,
                contract_id=contract_id,
                type=2,  # Market order
                side=side,
                size=size,
                custom_tag="OCO_ENTRY"
            )
            
            if not entry_order_id:
                print("❌ Failed to place entry order")
                return None
            
            print(f"✅ Entry order placed: {entry_order_id}")
            time.sleep(1)  # Small delay
            
            # Step 4: Place stop-loss order (linked)
            print("2️⃣ Placing stop-loss order...")
            # For long position: Stop order below market that triggers when price goes DOWN
            # For short position: Stop order above market that triggers when price goes UP
            stop_order_id = self.client.order.place_order(
                account_id=self.account_id,
                contract_id=contract_id,
                type=4,  # Stop order
                side=bracket_side,  # 1=Sell for long, 0=Buy for short
                size=size,
                stop_price=stop_price,
                linked_order_id=entry_order_id,
                custom_tag="OCO_STOP"
            )
            
            if not stop_order_id:
                print("❌ Failed to place stop order")
                return None
            
            print(f"✅ Stop order placed: {stop_order_id}")
            time.sleep(1)  # Small delay
            
            # Step 5: Place take-profit order (linked)
            print("3️⃣ Placing take-profit order...")
            # For long position: Limit SELL order above market
            # For short position: Limit BUY order below market  
            target_order_id = self.client.order.place_order(
                account_id=self.account_id,
                contract_id=contract_id,
                type=1,  # Limit order
                side=bracket_side,  # 1=Sell for long, 0=Buy for short
                size=size,
                limit_price=target_price,  # Use limit_price for limit orders
                linked_order_id=entry_order_id,
                custom_tag="OCO_TARGET"
            )
            
            if not target_order_id:
                print("❌ Failed to place target order")
                return None
            
            print(f"✅ Target order placed: {target_order_id}")
            
            # Step 6: Register OCO pair for monitoring
            oco_data = {
                'entry_order_id': entry_order_id,
                'stop_order_id': stop_order_id,
                'target_order_id': target_order_id,
                'contract_id': contract_id,
                'market_price': market_price,
                'stop_price': stop_price,
                'target_price': target_price,
                'status': 'active',
                'created_time': datetime.now().isoformat()
            }
            
            self.oco_pairs[entry_order_id] = oco_data
            
            # Create lookup table for fast OCO detection
            self.oco_order_lookup[str(stop_order_id)] = {
                'entry_id': entry_order_id,
                'type': 'stop',
                'partner_id': str(target_order_id)
            }
            self.oco_order_lookup[str(target_order_id)] = {
                'entry_id': entry_order_id,
                'type': 'target',
                'partner_id': str(stop_order_id)
            }
            
            print(f"🎉 OCO ORDER SET CREATED SUCCESSFULLY!")
            print(f"📋 Entry: {entry_order_id}")
            print(f"🛑 Stop: {stop_order_id}")
            print(f"🎯 Target: {target_order_id}")
            
            return oco_data
            
        except Exception as e:
            print(f"❌ Error creating OCO orders: {e}")
            return None
    
    def on_order_update(self, data):
        """Handle order updates and OCO logic"""
        # Parse order data (same as your existing code)
        order_data = data
        if isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], dict) and 'data' in data[0]:
                order_data = data[0]['data']
            else:
                order_data = data[0]
        elif isinstance(data, dict) and 'data' in data:
            order_data = data['data']
        
        order_id = str(order_data.get('id'))  # Convert to string for consistency
        status_code = order_data.get('status')
        status = self.decode_order_status(status_code)
        custom_tag = order_data.get('customTag', '')
        filled_size = order_data.get('fillVolume', 0)
        
        # Log the order event
        event = {
            'timestamp': datetime.now().isoformat(),
            'order_id': order_id,
            'status': status,
            'custom_tag': custom_tag,
            'filled_size': filled_size,
            'raw_data': data
        }
        self.order_events.append(event)
        
        print(f"\n📋 ORDER UPDATE: {order_id} -> {status} ({custom_tag}) Fill: {filled_size}")
        
        # OCO Logic: Check by order ID lookup (more reliable than custom tags)
        if filled_size > 0 and order_id in self.oco_order_lookup:
            oco_info = self.oco_order_lookup[order_id]
            print(f"🔥 OCO BRACKET ORDER FILLED: {order_id} ({oco_info['type']}) with {filled_size} contracts")
            self.handle_oco_fill_by_lookup(order_id, oco_info)
        elif order_id in self.oco_order_lookup:
            oco_info = self.oco_order_lookup[order_id]
            print(f"📝 OCO bracket order status: {order_id} ({oco_info['type']}) -> {status}")
        
        # Fallback: Also check custom tags if available
        elif filled_size > 0 and custom_tag and ('OCO_STOP' in custom_tag or 'OCO_TARGET' in custom_tag):
            print(f"🔥 BRACKET ORDER FILLED (by tag): {order_id} with {filled_size} contracts")
            self.handle_oco_fill(order_id, custom_tag)
    
    def handle_oco_fill_by_lookup(self, filled_order_id: str, oco_info: dict):
        """Handle OCO fill using lookup table (more reliable)"""
        entry_id = oco_info['entry_id']
        order_type = oco_info['type']
        partner_id = oco_info['partner_id']
        
        print(f"\n🎯 OCO FILL DETECTED: {filled_order_id} ({order_type})")
        print(f"🔗 Entry: {entry_id}, Partner: {partner_id}")
        
        # Get the OCO pair data
        if entry_id not in self.oco_pairs:
            print(f"⚠️ OCO pair not found for entry {entry_id}")
            return
        
        oco_data = self.oco_pairs[entry_id]
        
        if oco_data['status'] != 'active':
            print(f"⚠️ OCO pair already completed: {oco_data['status']}")
            return
        
        if order_type == 'stop':
            # Stop order filled, cancel target
            print("🛑 STOP LOSS HIT - Cancelling target order...")
            if self.cancel_order(partner_id):
                oco_data['status'] = 'stop_hit'
                oco_data['completed_time'] = datetime.now().isoformat()
                print("✅ OCO completed - Stop loss executed")
        
        elif order_type == 'target':
            # Target order filled, cancel stop
            print("🎯 TARGET HIT - Cancelling stop order...")
            if self.cancel_order(partner_id):
                oco_data['status'] = 'target_hit'
                oco_data['completed_time'] = datetime.now().isoformat()
                print("✅ OCO completed - Target profit taken")
        
    def cleanup_orders(self):
        """Cancel any remaining test orders"""
        print("\n🧹 Cleaning up OCO orders...")
        try:
            open_orders = self.client.order.search_open_orders(self.account_id)
            cleaned = 0
            
            for order in open_orders:
                custom_tag = order.get('customTag', '')
                order_id = order.get('orderId')
                
                if 'OCO_' in custom_tag:
                    try:
                        self.client.order.cancel_order(self.account_id, order_id)
                        print(f"✅ Cancelled OCO order: {order_id} ({custom_tag})")
                        cleaned += 1
                        time.sleep(1)
                    except Exception as e:
                        print(f"⚠️ Could not cancel {order_id}: {e}")
            
            if cleaned == 0:
                print("ℹ️  No OCO orders found to clean up")
            else:
                print(f"✅ Cleaned up {cleaned} OCO orders")
                
        except Exception as e:
            print(f"⚠️ Cleanup error: {e}")
        
        # Also cleanup completed OCO state
        self.cleanup_completed_ocos()
    
    def cancel_order(self, order_id: str):
        """Cancel an order"""
        try:
            success = self.client.order.cancel_order(self.account_id, order_id)
            if success:
                print(f"✅ Order cancelled: {order_id}")
            else:
                print(f"⚠️ Cancel may have failed: {order_id}")
            return success
        except Exception as e:
            print(f"❌ Error cancelling order {order_id}: {e}")
            return False
    
    def decode_order_status(self, status_code):
        """Decode TopStep order status codes"""
        status_map = {
            1: "New",
            2: "Working", 
            3: "Filled",
            4: "PartiallyFilled",
            5: "Cancelled",
            6: "Rejected",
            7: "Expired",
            8: "Modified"
        }
        return status_map.get(status_code, f"Unknown({status_code})")
    
    def on_position_update(self, data):
        """Handle position updates"""
        position_data = data
        if isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], dict) and 'data' in data[0]:
                position_data = data[0]['data']
            else:
                position_data = data[0]
        elif isinstance(data, dict) and 'data' in data:
            position_data = data['data']
        
        event = {
            'timestamp': datetime.now().isoformat(),
            'symbol': position_data.get('symbol', 'Unknown'),
            'size': position_data.get('size', 0),
            'avg_price': position_data.get('avgPrice'),
            'unrealized_pnl': position_data.get('unrealizedPnL'),
            'raw_data': data
        }
        
        self.position_events.append(event)
        print(f"📊 POSITION UPDATE: {event['symbol']} Size={event['size']} P&L=${event['unrealized_pnl']}")
    
    def setup_realtime(self):
        """Setup real-time monitoring"""
        self.client.realtime.on_order_update(self.on_order_update)
        self.client.realtime.on_position_update(self.on_position_update)
        
        if not self.client.realtime.start():
            print("❌ Failed to start real-time connection")
            return False
        
        time.sleep(3)
        
        if not self.client.realtime.is_connected():
            print("❌ Real-time connection not active")
            return False
        
        # Subscribe to updates
        self.client.realtime.subscribe_orders(self.account_id)
        self.client.realtime.subscribe_positions(self.account_id)
        
        print("✅ Real-time monitoring active")
        return True
    
    def show_oco_status(self):
        """Show current OCO pairs status"""
        print(f"\n📊 OCO PAIRS STATUS")
        print("=" * 30)
        
        if not self.oco_pairs:
            print("No OCO pairs created yet")
            return
        
        for entry_id, oco_data in self.oco_pairs.items():
            status = oco_data['status']
            print(f"Entry: {entry_id}")
            print(f"  Status: {status}")
            print(f"  Market: ${oco_data['market_price']}")
            print(f"  Stop: ${oco_data['stop_price']} (ID: {oco_data['stop_order_id']})")
            print(f"  Target: ${oco_data['target_price']} (ID: {oco_data['target_order_id']})")
            print(f"  Created: {oco_data['created_time']}")
            if 'completed_time' in oco_data:
                print(f"  Completed: {oco_data['completed_time']}")
            print()
        
        # Also show recent order events for debugging
        print("📋 Recent Order Events:")
        for event in self.order_events[-5:]:  # Show last 5 events
            print(f"  {event['order_id']}: {event['status']} (Fill: {event['filled_size']}) - {event['custom_tag']}")
        
        # Show OCO lookup table
        print("🔍 OCO Order Lookup:")
        for order_id, info in self.oco_order_lookup.items():
            print(f"  {order_id}: {info['type']} -> partner: {info['partner_id']}")
        print()
    
    def reset_oco_state(self):
        """Reset OCO state for clean slate"""
        print("🧹 Resetting OCO state...")
        old_pairs = len(self.oco_pairs)
        old_lookups = len(self.oco_order_lookup)
        
        self.oco_pairs.clear()
        self.oco_order_lookup.clear()
        
        print(f"✅ Cleared {old_pairs} old OCO pairs and {old_lookups} lookup entries")
    
    def cleanup_completed_ocos(self):
        """Remove completed OCO pairs from memory"""
        completed = []
        for entry_id, oco_data in list(self.oco_pairs.items()):
            if oco_data['status'] != 'active':
                completed.append(entry_id)
                # Remove from lookup table too
                stop_id = str(oco_data['stop_order_id'])
                target_id = str(oco_data['target_order_id'])
                self.oco_order_lookup.pop(stop_id, None)
                self.oco_order_lookup.pop(target_id, None)
        
        for entry_id in completed:
            del self.oco_pairs[entry_id]
        
        if completed:
            print(f"🧹 Cleaned up {len(completed)} completed OCO pairs")
        else:
            print("ℹ️  No completed OCO pairs to clean up")
    
    def save_oco_data(self):
        """Save OCO data for analysis"""
        oco_summary = {
            'timestamp': datetime.now().isoformat(),
            'account_id': self.account_id,
            'oco_pairs': self.oco_pairs,
            'order_events': self.order_events[-20:],  # Last 20 events
            'position_events': self.position_events[-10:],  # Last 10 events
            'settings': {
                'stop_distance': self.stop_distance,
                'target_distance': self.target_distance
            }
        }
        
        filename = f"oco_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(oco_summary, f, indent=2, default=str)
        
        print(f"💾 OCO data saved to: {filename}")
        return filename


def main():
    """Main OCO testing function"""
    # Your credentials
    USERNAME = "xiaosun666"
    API_KEY = "MmP9cmQB8Zra4UVLQnQUipyIeBDObzyLmu/NZdkwHUA="
    ACCOUNT_ID = 7914587
    CONTRACT_ID = "CON.F.US.MNQ.M25"  # Micro E-mini Nasdaq
    
    print("🎯 SIMPLE OCO ORDER MANAGEMENT SYSTEM")
    print("=" * 50)
    
    # Create OCO manager
    oco_manager = SimpleOCOManager(USERNAME, API_KEY, ACCOUNT_ID)
    
    # Setup real-time monitoring
    if not oco_manager.setup_realtime():
        print("❌ Failed to setup real-time monitoring")
        return
    
    try:
        # Show menu
        while True:
            print("\n🎛️  OCO MANAGEMENT MENU")
            print("1. Create Long OCO (Buy + Stop/Target)")
            print("2. Create Short OCO (Sell + Stop/Target)")
            print("3. Show OCO Status")
            print("4. Cleanup Orders & Reset State")
            print("5. Reset OCO State Only")
            print("6. Exit")
            
            choice = input("\nEnter choice (1-6): ").strip()
            
            if choice == '1':
                print("\n🔼 Creating LONG OCO order...")
                oco_manager.create_oco_order(CONTRACT_ID, side=0)  # Buy
                
            elif choice == '2':
                print("\n🔽 Creating SHORT OCO order...")
                oco_manager.create_oco_order(CONTRACT_ID, side=1)  # Sell
                
            elif choice == '3':
                oco_manager.show_oco_status()
                
            elif choice == '4':
                oco_manager.cleanup_orders()
                
            elif choice == '5':
                oco_manager.reset_oco_state()
                
            elif choice == '6':
                print("\n👋 Exiting...")
                break
                
            else:
                print("❌ Invalid choice")
            
            # Brief pause for real-time events
            time.sleep(2)
    
    except KeyboardInterrupt:
        print("\n⏹️ Interrupted by user")
    
    finally:
        # Cleanup and save data
        oco_manager.cleanup_orders()
        oco_manager.save_oco_data()
        
        # Stop real-time connection
        try:
            oco_manager.client.realtime.stop()
        except:
            pass
        
        print("\n✅ OCO Manager shutdown complete!")


if __name__ == "__main__":
    main()

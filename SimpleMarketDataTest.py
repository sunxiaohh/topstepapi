#!/usr/bin/env python3
"""
Simple Market Data Stream Test
Just to see what the real-time market data looks like
Based on the SimpleOCO pattern but focused on market data exploration
"""

import json
import time
from datetime import datetime
from topstepapi import TopstepClient

class SimpleMarketDataTest:
    def __init__(self, username: str, api_key: str):
        self.client = TopstepClient(username, api_key)
        
        # Data collection
        self.quote_samples = []
        self.trade_samples = []
        self.depth_samples = []
        
        # Settings
        self.max_samples = 10  # Keep last 10 of each type
        self.start_time = datetime.now()
        
    def log_with_timestamp(self, message):
        """Add timestamp to all messages"""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"[{timestamp}] {message}")
        
    def on_quote_update(self, data):
        """Handle market quote updates"""
        self.log_with_timestamp("📊 QUOTE UPDATE RECEIVED")
        
        # Parse the data (same pattern as SimpleOCO)
        quote_data = data
        if isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], dict) and 'data' in data[0]:
                quote_data = data[0]['data']
            else:
                quote_data = data[0]
        elif isinstance(data, dict) and 'data' in data:
            quote_data = data['data']
        
        print(f"   Raw data type: {type(data)}")
        print(f"   Parsed data type: {type(quote_data)}")
        print(f"   Raw data: {data}")
        
        # Try to extract common fields
        if isinstance(quote_data, dict):
            print("   📋 Quote Fields:")
            for key, value in quote_data.items():
                print(f"      {key}: {value} ({type(value).__name__})")
        
        # Store sample
        sample = {
            'timestamp': datetime.now().isoformat(),
            'raw_data': data,
            'parsed_data': quote_data,
            'data_type': type(quote_data).__name__
        }
        self.quote_samples.append(sample)
        
        # Keep only recent samples
        if len(self.quote_samples) > self.max_samples:
            self.quote_samples = self.quote_samples[-self.max_samples:]
        
        print("   " + "-" * 50)
        
    def on_trade_update(self, data):
        """Handle market trade updates"""
        self.log_with_timestamp("💰 TRADE UPDATE RECEIVED")
        
        # Parse the data (same pattern as SimpleOCO)
        trade_data = data
        if isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], dict) and 'data' in data[0]:
                trade_data = data[0]['data']
            else:
                trade_data = data[0]
        elif isinstance(data, dict) and 'data' in data:
            trade_data = data['data']
        
        print(f"   Raw data type: {type(data)}")
        print(f"   Parsed data type: {type(trade_data)}")
        print(f"   Raw data: {data}")
        
        # Try to extract common fields
        if isinstance(trade_data, dict):
            print("   📋 Trade Fields:")
            for key, value in trade_data.items():
                print(f"      {key}: {value} ({type(value).__name__})")
        
        # Store sample
        sample = {
            'timestamp': datetime.now().isoformat(),
            'raw_data': data,
            'parsed_data': trade_data,
            'data_type': type(trade_data).__name__
        }
        self.trade_samples.append(sample)
        
        # Keep only recent samples
        if len(self.trade_samples) > self.max_samples:
            self.trade_samples = self.trade_samples[-self.max_samples:]
        
        print("   " + "-" * 50)
        
    def on_depth_update(self, data):
        """Handle market depth updates"""
        self.log_with_timestamp("📈 DEPTH UPDATE RECEIVED")
        
        # Parse the data (same pattern as SimpleOCO)
        depth_data = data
        if isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], dict) and 'data' in data[0]:
                depth_data = data[0]['data']
            else:
                depth_data = data[0]
        elif isinstance(data, dict) and 'data' in data:
            depth_data = data['data']
        
        print(f"   Raw data type: {type(data)}")
        print(f"   Parsed data type: {type(depth_data)}")
        
        # Don't print full raw data for depth (can be large)
        print(f"   Raw data length: {len(str(data))} characters")
        
        # Try to extract common fields
        if isinstance(depth_data, dict):
            print("   📋 Depth Fields:")
            for key, value in depth_data.items():
                if isinstance(value, (list, dict)) and len(str(value)) > 100:
                    print(f"      {key}: {type(value).__name__} with {len(value) if hasattr(value, '__len__') else '?'} items")
                else:
                    print(f"      {key}: {value} ({type(value).__name__})")
        
        # Store sample (but don't store the full raw data to save memory)
        sample = {
            'timestamp': datetime.now().isoformat(),
            'raw_data_type': type(data).__name__,
            'raw_data_length': len(str(data)),
            'parsed_data': depth_data,
            'data_type': type(depth_data).__name__
        }
        self.depth_samples.append(sample)
        
        # Keep only recent samples
        if len(self.depth_samples) > self.max_samples:
            self.depth_samples = self.depth_samples[-self.max_samples:]
        
        print("   " + "-" * 50)
    
    def setup_market_data_stream(self, contract_id: str):
        """Setup market data streaming (following SimpleOCO pattern)"""
        self.log_with_timestamp("🔌 Setting up market data stream...")
        
        # Check if the client has market data methods
        if not hasattr(self.client, 'market_data'):
            print("❌ Client doesn't have market_data attribute")
            print("Available client attributes:")
            for attr in dir(self.client):
                if not attr.startswith('_'):
                    print(f"   - {attr}")
            return False
        
        # Set up event handlers
        try:
            self.client.market_data.on_quote_update(self.on_quote_update)
            self.log_with_timestamp("✅ Quote handler registered")
        except Exception as e:
            print(f"⚠️ Could not register quote handler: {e}")
        
        try:
            self.client.market_data.on_trade_update(self.on_trade_update)
            self.log_with_timestamp("✅ Trade handler registered")
        except Exception as e:
            print(f"⚠️ Could not register trade handler: {e}")
        
        try:
            self.client.market_data.on_depth_update(self.on_depth_update)
            self.log_with_timestamp("✅ Depth handler registered")
        except Exception as e:
            print(f"⚠️ Could not register depth handler: {e}")
        
        # Start the connection
        try:
            if not self.client.market_data.start():
                print("❌ Failed to start market data connection")
                return False
            
            self.log_with_timestamp("✅ Market data connection started")
        except Exception as e:
            print(f"❌ Error starting market data connection: {e}")
            return False
        
        # Wait for connection
        self.log_with_timestamp("⏳ Waiting for connection...")
        time.sleep(3)
        
        # Check connection status
        try:
            if not self.client.market_data.is_connected():
                print("❌ Market data connection not active")
                return False
            
            self.log_with_timestamp("✅ Market data connection is active")
        except Exception as e:
            print(f"⚠️ Could not check connection status: {e}")
        
        # Subscribe to data
        self.log_with_timestamp(f"📡 Subscribing to market data for {contract_id}...")
        
        try:
            # Try subscribing to quotes
            self.client.market_data.subscribe_contract_quotes(contract_id)
            self.log_with_timestamp("✅ Subscribed to quotes")
            time.sleep(1)
        except Exception as e:
            print(f"⚠️ Could not subscribe to quotes: {e}")
        
        try:
            # Try subscribing to trades
            self.client.market_data.subscribe_contract_trades(contract_id)
            self.log_with_timestamp("✅ Subscribed to trades")
            time.sleep(1)
        except Exception as e:
            print(f"⚠️ Could not subscribe to trades: {e}")
        
        try:
            # Try subscribing to depth
            self.client.market_data.subscribe_contract_market_depth(contract_id)
            self.log_with_timestamp("✅ Subscribed to market depth")
            time.sleep(1)
        except Exception as e:
            print(f"⚠️ Could not subscribe to market depth: {e}")
        
        return True
    
    def show_summary(self):
        """Show data collection summary"""
        runtime = (datetime.now() - self.start_time).total_seconds()
        
        print("\n" + "=" * 60)
        self.log_with_timestamp("📊 MARKET DATA TEST SUMMARY")
        print("=" * 60)
        print(f"Runtime: {runtime:.1f} seconds")
        print(f"Quote updates: {len(self.quote_samples)}")
        print(f"Trade updates: {len(self.trade_samples)}")
        print(f"Depth updates: {len(self.depth_samples)}")
        
        # Show sample structures
        if self.quote_samples:
            print(f"\n📊 Latest Quote Sample:")
            latest = self.quote_samples[-1]
            print(f"   Data type: {latest['data_type']}")
            if isinstance(latest['parsed_data'], dict):
                print(f"   Keys: {list(latest['parsed_data'].keys())}")
        
        if self.trade_samples:
            print(f"\n💰 Latest Trade Sample:")
            latest = self.trade_samples[-1]
            print(f"   Data type: {latest['data_type']}")
            if isinstance(latest['parsed_data'], dict):
                print(f"   Keys: {list(latest['parsed_data'].keys())}")
        
        if self.depth_samples:
            print(f"\n📈 Latest Depth Sample:")
            latest = self.depth_samples[-1]
            print(f"   Data type: {latest['data_type']}")
            if isinstance(latest['parsed_data'], dict):
                print(f"   Keys: {list(latest['parsed_data'].keys())}")
    
    def save_samples(self):
        """Save collected samples for analysis"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        data = {
            'timestamp': datetime.now().isoformat(),
            'runtime_seconds': (datetime.now() - self.start_time).total_seconds(),
            'quote_samples': self.quote_samples,
            'trade_samples': self.trade_samples,
            'depth_samples': self.depth_samples
        }
        
        filename = f"market_data_test_{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        print(f"💾 Test data saved to: {filename}")
        return filename
    
    def cleanup(self, contract_id: str):
        """Cleanup market data subscriptions"""
        self.log_with_timestamp("🧹 Cleaning up market data subscriptions...")
        
        try:
            if hasattr(self.client, 'market_data'):
                self.client.market_data.unsubscribe_contract_all(contract_id)
                self.log_with_timestamp("✅ Unsubscribed from all market data")
                
                time.sleep(1)
                
                self.client.market_data.stop()
                self.log_with_timestamp("✅ Market data connection stopped")
        except Exception as e:
            print(f"⚠️ Cleanup error: {e}")


def main():
    """Main test function"""
    # Your credentials (same as SimpleOCO)
    USERNAME = "xiaosun666"
    API_KEY = "MmP9cmQB8Zra4UVLQnQUipyIeBDObzyLmu/NZdkwHUA="
    CONTRACT_ID = "CON.F.US.MNQ.M25"  # Same contract as SimpleOCO
    
    print("🔍 SIMPLE MARKET DATA STREAM TEST")
    print("=" * 50)
    print(f"Contract: {CONTRACT_ID}")
    print(f"Test Duration: 30 seconds")
    print("=" * 50)
    
    # Create test instance
    test = SimpleMarketDataTest(USERNAME, API_KEY)
    
    try:
        # Setup market data streaming
        if not test.setup_market_data_stream(CONTRACT_ID):
            print("❌ Failed to setup market data stream")
            return
        
        # Listen for data
        test.log_with_timestamp("🎧 Listening for market data...")
        test.log_with_timestamp("You should see data updates below:")
        print()
        
        # Monitor for 30 seconds
        for i in range(30):
            time.sleep(1)
            
            # Show progress every 10 seconds
            if (i + 1) % 10 == 0:
                test.log_with_timestamp(f"Progress: {i+1}/30 seconds")
                test.log_with_timestamp(f"Data so far - Quotes: {len(test.quote_samples)}, Trades: {len(test.trade_samples)}, Depth: {len(test.depth_samples)}")
                print()
    
    except KeyboardInterrupt:
        test.log_with_timestamp("Test interrupted by user")
    except Exception as e:
        test.log_with_timestamp(f"Test error: {e}")
        print(f"Exception details: {repr(e)}")
    
    finally:
        # Cleanup and summarize
        test.cleanup(CONTRACT_ID)
        test.show_summary()
        test.save_samples()
        
        print("\n🎯 NEXT STEPS:")
        print("1. Review the data structures shown above")
        print("2. Check the saved JSON file for detailed analysis")
        print("3. Look for price/volume/time fields we can use")
        print("4. Share the results so we can design the trading strategy!")


if __name__ == "__main__":
    main()

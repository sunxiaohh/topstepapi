#!/usr/bin/env python3
"""
Enhanced Market Data Stream Test
Fixed parsing logic to properly extract market depth details
"""

import json
import time
from datetime import datetime
from topstepapi import TopstepClient

class EnhancedMarketDataTest:
    def __init__(self, username: str, api_key: str):
        self.client = TopstepClient(username, api_key)
        
        # Data collection
        self.quote_samples = []
        self.trade_samples = []
        self.depth_samples = []
        
        # Settings
        self.max_samples = 10
        self.start_time = datetime.now()
        
    def log_with_timestamp(self, message):
        """Add timestamp to all messages"""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"[{timestamp}] {message}")
    
    def parse_market_data(self, data):
        """
        FIXED: Properly parse the market data structure
        Data comes as: [contract_id, actual_data_object]
        """
        if isinstance(data, list) and len(data) >= 2:
            contract_id = data[0]  # "CON.F.US.MNQ.M25"
            actual_data = data[1]  # The real data object
            return contract_id, actual_data
        elif isinstance(data, dict):
            # Sometimes data might come as a dict directly
            return None, data
        else:
            return None, data
    
    def on_quote_update(self, data):
        """Handle market quote updates - FIXED PARSING"""
        self.log_with_timestamp("📊 QUOTE UPDATE RECEIVED")
        
        # Use the fixed parsing logic
        contract_id, quote_data = self.parse_market_data(data)
        
        print(f"   Contract ID: {contract_id}")
        print(f"   Data type: {type(quote_data)}")
        print(f"   Raw data: {data}")
        
        # Extract quote details
        if isinstance(quote_data, dict):
            print("   📋 Quote Details:")
            for key, value in quote_data.items():
                print(f"      {key}: {value} ({type(value).__name__})")
                
            # Highlight key trading data
            if 'bestBid' in quote_data and 'bestAsk' in quote_data:
                spread = quote_data['bestAsk'] - quote_data['bestBid']
                print(f"   💰 Spread: {spread} ({quote_data['bestBid']} - {quote_data['bestAsk']})")
        
        # Store sample with corrected data
        sample = {
            'timestamp': datetime.now().isoformat(),
            'contract_id': contract_id,
            'quote_data': quote_data,
            'data_type': type(quote_data).__name__
        }
        self.quote_samples.append(sample)
        
        if len(self.quote_samples) > self.max_samples:
            self.quote_samples = self.quote_samples[-self.max_samples:]
        
        print("   " + "-" * 50)
        
    def on_trade_update(self, data):
        """Handle market trade updates - FIXED PARSING"""
        self.log_with_timestamp("💰 TRADE UPDATE RECEIVED")
        
        # Use the fixed parsing logic
        contract_id, trade_data = self.parse_market_data(data)
        
        print(f"   Contract ID: {contract_id}")
        print(f"   Data type: {type(trade_data)}")
        
        # Extract trade details
        if isinstance(trade_data, list):
            print(f"   📋 Trade List with {len(trade_data)} trades:")
            for i, trade in enumerate(trade_data[:3]):  # Show first 3 trades
                if isinstance(trade, dict):
                    price = trade.get('price', 'N/A')
                    volume = trade.get('volume', 'N/A')
                    trade_type = trade.get('type', 'N/A')  # 0=buy, 1=sell typically
                    timestamp = trade.get('timestamp', 'N/A')
                    print(f"      Trade {i+1}: {price} x {volume} (type: {trade_type}) @ {timestamp}")
            
            if len(trade_data) > 3:
                print(f"      ... and {len(trade_data) - 3} more trades")
        
        elif isinstance(trade_data, dict):
            print("   📋 Single Trade Details:")
            for key, value in trade_data.items():
                print(f"      {key}: {value} ({type(value).__name__})")
        
        # Store sample with corrected data
        sample = {
            'timestamp': datetime.now().isoformat(),
            'contract_id': contract_id,
            'trade_data': trade_data,
            'data_type': type(trade_data).__name__,
            'trade_count': len(trade_data) if isinstance(trade_data, list) else 1
        }
        self.trade_samples.append(sample)
        
        if len(self.trade_samples) > self.max_samples:
            self.trade_samples = self.trade_samples[-self.max_samples:]
        
        print("   " + "-" * 50)
        
    def on_depth_update(self, data):
        """Handle market depth updates - FIXED PARSING FOR DEPTH DATA"""
        self.log_with_timestamp("📈 DEPTH UPDATE RECEIVED")
        
        # Use the fixed parsing logic
        contract_id, depth_data = self.parse_market_data(data)
        
        print(f"   Contract ID: {contract_id}")
        print(f"   Data type: {type(depth_data)}")
        print(f"   Raw data length: {len(str(data))} characters")
        
        # DETAILED DEPTH ANALYSIS
        if isinstance(depth_data, dict):
            print("   📋 Depth Object Keys:")
            for key, value in depth_data.items():
                if isinstance(value, (list, dict)):
                    print(f"      {key}: {type(value).__name__} with {len(value) if hasattr(value, '__len__') else '?'} items")
                else:
                    print(f"      {key}: {value} ({type(value).__name__})")
            
            # Look for common depth fields
            bid_levels = depth_data.get('bids', depth_data.get('bidLevels', []))
            ask_levels = depth_data.get('asks', depth_data.get('askLevels', []))
            
            if bid_levels:
                print(f"   📊 BID LEVELS ({len(bid_levels)} levels):")
                for i, level in enumerate(bid_levels[:5]):  # Show top 5
                    if isinstance(level, dict):
                        price = level.get('price', level.get('p', 'N/A'))
                        size = level.get('size', level.get('quantity', level.get('q', 'N/A')))
                        print(f"      Level {i+1}: {price} x {size}")
                    else:
                        print(f"      Level {i+1}: {level}")
            
            if ask_levels:
                print(f"   📊 ASK LEVELS ({len(ask_levels)} levels):")
                for i, level in enumerate(ask_levels[:5]):  # Show top 5
                    if isinstance(level, dict):
                        price = level.get('price', level.get('p', 'N/A'))
                        size = level.get('size', level.get('quantity', level.get('q', 'N/A')))
                        print(f"      Level {i+1}: {price} x {size}")
                    else:
                        print(f"      Level {i+1}: {level}")
        
        elif isinstance(depth_data, list):
            print(f"   📋 Depth List with {len(depth_data)} items:")
            for i, item in enumerate(depth_data[:3]):  # Show first 3 items
                print(f"      Item {i+1}: {type(item).__name__}")
                if isinstance(item, dict):
                    for key, value in list(item.items())[:5]:  # Show first 5 keys
                        print(f"         {key}: {value}")
        
        # Store sample with detailed depth data
        sample = {
            'timestamp': datetime.now().isoformat(),
            'contract_id': contract_id,
            'depth_data': depth_data,
            'data_type': type(depth_data).__name__,
            'raw_data_length': len(str(data))
        }
        
        # Add depth analysis
        if isinstance(depth_data, dict):
            sample['bid_levels'] = len(depth_data.get('bids', depth_data.get('bidLevels', [])))
            sample['ask_levels'] = len(depth_data.get('asks', depth_data.get('askLevels', [])))
            sample['depth_keys'] = list(depth_data.keys())
        
        self.depth_samples.append(sample)
        
        if len(self.depth_samples) > self.max_samples:
            self.depth_samples = self.depth_samples[-self.max_samples:]
        
        print("   " + "-" * 50)
    
    def setup_market_data_stream(self, contract_id: str):
        """Setup market data streaming"""
        self.log_with_timestamp("🔌 Setting up market data stream...")
        
        if not hasattr(self.client, 'market_data'):
            print("❌ Client doesn't have market_data attribute")
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
            self.client.market_data.subscribe_contract_quotes(contract_id)
            self.log_with_timestamp("✅ Subscribed to quotes")
            time.sleep(1)
        except Exception as e:
            print(f"⚠️ Could not subscribe to quotes: {e}")
        
        try:
            self.client.market_data.subscribe_contract_trades(contract_id)
            self.log_with_timestamp("✅ Subscribed to trades")
            time.sleep(1)
        except Exception as e:
            print(f"⚠️ Could not subscribe to trades: {e}")
        
        try:
            self.client.market_data.subscribe_contract_market_depth(contract_id)
            self.log_with_timestamp("✅ Subscribed to market depth")
            time.sleep(1)
        except Exception as e:
            print(f"⚠️ Could not subscribe to market depth: {e}")
        
        return True
    
    def show_summary(self):
        """Show enhanced data collection summary"""
        runtime = (datetime.now() - self.start_time).total_seconds()
        
        print("\n" + "=" * 60)
        self.log_with_timestamp("📊 ENHANCED MARKET DATA TEST SUMMARY")
        print("=" * 60)
        print(f"Runtime: {runtime:.1f} seconds")
        print(f"Quote updates: {len(self.quote_samples)}")
        print(f"Trade updates: {len(self.trade_samples)}")
        print(f"Depth updates: {len(self.depth_samples)}")
        
        # Enhanced sample analysis
        if self.quote_samples:
            print(f"\n📊 Latest Quote Analysis:")
            latest = self.quote_samples[-1]
            print(f"   Data type: {latest['data_type']}")
            if isinstance(latest['quote_data'], dict):
                print(f"   Available fields: {list(latest['quote_data'].keys())}")
                if 'bestBid' in latest['quote_data']:
                    print(f"   Best Bid/Ask: {latest['quote_data']['bestBid']}/{latest['quote_data']['bestAsk']}")
        
        if self.trade_samples:
            print(f"\n💰 Latest Trade Analysis:")
            latest = self.trade_samples[-1]
            print(f"   Data type: {latest['data_type']}")
            print(f"   Trade count: {latest.get('trade_count', 'N/A')}")
        
        if self.depth_samples:
            print(f"\n📈 Latest Depth Analysis:")
            latest = self.depth_samples[-1]
            print(f"   Data type: {latest['data_type']}")
            print(f"   Raw data length: {latest['raw_data_length']} chars")
            if 'depth_keys' in latest:
                print(f"   Depth keys: {latest['depth_keys']}")
            if 'bid_levels' in latest:
                print(f"   Bid levels: {latest['bid_levels']}")
            if 'ask_levels' in latest:
                print(f"   Ask levels: {latest['ask_levels']}")
    
    def save_samples(self):
        """Save collected samples with enhanced structure"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        data = {
            'timestamp': datetime.now().isoformat(),
            'runtime_seconds': (datetime.now() - self.start_time).total_seconds(),
            'summary': {
                'quote_count': len(self.quote_samples),
                'trade_count': len(self.trade_samples),
                'depth_count': len(self.depth_samples)
            },
            'quote_samples': self.quote_samples,
            'trade_samples': self.trade_samples,
            'depth_samples': self.depth_samples
        }
        
        filename = f"enhanced_market_data_{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        print(f"💾 Enhanced test data saved to: {filename}")
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
    USERNAME = "xiaosun666@gmail.com"
    API_KEY = "QDPiLtiqkfz6lwFhgiabIv/oX1iEUUaEWNU7r36M37k="
    CONTRACT_ID = "CON.F.US.MNQ.M25"
    
    print("🔍 ENHANCED MARKET DATA STREAM TEST")
    print("=" * 50)
    print(f"Contract: {CONTRACT_ID}")
    print(f"🆕 FIXED: Proper data parsing to access depth details!")
    print("=" * 50)
    
    # Create enhanced test instance
    test = EnhancedMarketDataTest(USERNAME, API_KEY)
    
    try:
        # Setup market data streaming
        if not test.setup_market_data_stream(CONTRACT_ID):
            print("❌ Failed to setup market data stream")
            return
        
        # Listen for data
        test.log_with_timestamp("🎧 Listening for market data with enhanced parsing...")
        print()
        
        # Monitor for 30 seconds
        for i in range(30):
            time.sleep(1)
            
            # Show progress every 10 seconds
            if (i + 1) % 10 == 0:
                test.log_with_timestamp(f"Progress: {i+1}/30 seconds")
                test.log_with_timestamp(f"Data collected - Q:{len(test.quote_samples)}, T:{len(test.trade_samples)}, D:{len(test.depth_samples)}")
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
        
        print("\n🎯 WHAT'S DIFFERENT:")
        print("✅ Fixed data parsing - now extracts actual data objects")
        print("✅ Enhanced depth analysis - shows bid/ask levels")
        print("✅ Better trade analysis - shows individual trade details")
        print("✅ Improved logging with more market context")
        print("\n📊 You should now see the actual market depth details!")


if __name__ == "__main__":
    main()

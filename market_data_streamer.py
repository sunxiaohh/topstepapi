#!/usr/bin/env python3
"""
Market Data Streaming System with Database Storage
High-performance data capture for microstructure analysis
"""

import json
import time
import signal
import logging
import threading
import gc
import psutil
import os
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor

from topstepapi import TopstepClient
from market_data_db import MarketDataDatabase, DatabaseConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/market_data_streamer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class StreamingConfig:
    """Configuration for market data streaming"""
    username: str
    api_key: str
    contract_id: str
    session_id: str = field(default_factory=lambda: f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    
    # Performance settings
    enable_quotes: bool = True
    enable_trades: bool = True
    enable_depth: bool = True
    
    # Memory management
    memory_check_interval: int = 30  # seconds
    max_memory_usage_mb: int = 2048  # 2GB limit
    gc_threshold: int = 10000  # Force GC after N operations
    
    # Connection settings
    reconnect_attempts: int = 5
    reconnect_delay: int = 10  # seconds
    connection_timeout: int = 30
    
    # Data validation
    validate_data: bool = True
    log_invalid_data: bool = True
    
    # Statistics
    stats_interval: int = 60  # seconds

@dataclass
class StreamingStats:
    """Statistics for streaming performance"""
    start_time: datetime = field(default_factory=datetime.now)
    quotes_received: int = 0
    trades_received: int = 0
    depth_received: int = 0
    total_records: int = 0
    invalid_records: int = 0
    connection_errors: int = 0
    last_data_time: Optional[datetime] = None
    memory_usage_mb: float = 0.0
    operations_count: int = 0

class MarketDataStreamer:
    """
    High-performance market data streaming system
    Captures real-time data and stores to database with memory management
    """
    
    def __init__(self, streaming_config: StreamingConfig, db_config: DatabaseConfig):
        self.config = streaming_config
        self.db_config = db_config
        
        # Initialize components
        self.client = None
        self.database = None
        self.stats = StreamingStats()
        
        # Threading and control
        self.running = False
        self.shutdown_flag = threading.Event()
        self.stats_thread = None
        self.memory_monitor_thread = None
        self.executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="streamer")
        
        # Connection state
        self.connected = False
        self.reconnect_count = 0
        
        # Data callbacks
        self.custom_callbacks = {
            'quote': [],
            'trade': [],
            'depth': []
        }
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"Market Data Streamer initialized for session: {self.config.session_id}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
    
    def add_custom_callback(self, data_type: str, callback: Callable):
        """Add custom callback for data processing"""
        if data_type in self.custom_callbacks:
            self.custom_callbacks[data_type].append(callback)
            logger.info(f"Added custom {data_type} callback")
    
    def start(self) -> bool:
        """Start the streaming system"""
        try:
            logger.info("Starting Market Data Streaming System...")
            
            # Initialize database
            self.database = MarketDataDatabase(self.db_config)
            self.database.start_session(self.config.session_id, self.config.contract_id)
            
            # Initialize TopStep client
            self.client = TopstepClient(self.config.username, self.config.api_key)
            
            # Setup market data streaming
            if not self._setup_market_data_stream():
                logger.error("Failed to setup market data stream")
                return False
            
            # Start monitoring threads
            self._start_monitoring_threads()
            
            self.running = True
            self.stats.start_time = datetime.now()
            
            logger.info("✅ Market Data Streaming System started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start streaming system: {e}")
            self.stop()
            return False
    
    def _setup_market_data_stream(self) -> bool:
        """Setup market data streaming with retry logic"""
        for attempt in range(self.config.reconnect_attempts):
            try:
                logger.info(f"Setting up market data stream (attempt {attempt + 1}/{self.config.reconnect_attempts})")
                
                if not hasattr(self.client, 'market_data'):
                    logger.error("Client doesn't have market_data attribute")
                    return False
                
                # Register event handlers
                if self.config.enable_quotes:
                    self.client.market_data.on_quote_update(self._on_quote_update)
                    logger.info("✅ Quote handler registered")
                
                if self.config.enable_trades:
                    self.client.market_data.on_trade_update(self._on_trade_update)
                    logger.info("✅ Trade handler registered")
                
                if self.config.enable_depth:
                    self.client.market_data.on_depth_update(self._on_depth_update)
                    logger.info("✅ Depth handler registered")
                
                # Start connection
                if not self.client.market_data.start():
                    raise Exception("Failed to start market data connection")
                
                # Wait for connection
                logger.info("⏳ Waiting for connection...")
                time.sleep(3)
                
                # Verify connection
                if not self.client.market_data.is_connected():
                    raise Exception("Market data connection not active")
                
                logger.info("✅ Market data connection established")
                
                # Subscribe to data feeds
                self._subscribe_to_feeds()
                
                self.connected = True
                return True
                
            except Exception as e:
                logger.error(f"Connection attempt {attempt + 1} failed: {e}")
                self.stats.connection_errors += 1
                
                if attempt < self.config.reconnect_attempts - 1:
                    logger.info(f"Retrying in {self.config.reconnect_delay} seconds...")
                    time.sleep(self.config.reconnect_delay)
                else:
                    logger.error("All connection attempts failed")
                    return False
        
        return False
    
    def _subscribe_to_feeds(self):
        """Subscribe to market data feeds"""
        try:
            if self.config.enable_quotes:
                self.client.market_data.subscribe_contract_quotes(self.config.contract_id)
                logger.info("✅ Subscribed to quotes")
                time.sleep(0.5)
            
            if self.config.enable_trades:
                self.client.market_data.subscribe_contract_trades(self.config.contract_id)
                logger.info("✅ Subscribed to trades")
                time.sleep(0.5)
            
            if self.config.enable_depth:
                self.client.market_data.subscribe_contract_market_depth(self.config.contract_id)
                logger.info("✅ Subscribed to market depth")
                time.sleep(0.5)
                
        except Exception as e:
            logger.error(f"Failed to subscribe to feeds: {e}")
            raise
    
    def _start_monitoring_threads(self):
        """Start background monitoring threads"""
        # Statistics thread
        self.stats_thread = threading.Thread(
            target=self._stats_monitor, 
            daemon=True, 
            name="stats_monitor"
        )
        self.stats_thread.start()
        
        # Memory monitor thread
        self.memory_monitor_thread = threading.Thread(
            target=self._memory_monitor, 
            daemon=True, 
            name="memory_monitor"
        )
        self.memory_monitor_thread.start()
        
        logger.info("✅ Monitoring threads started")
    
    def _parse_market_data(self, data):
        """Parse market data structure"""
        if isinstance(data, list) and len(data) >= 2:
            return data[0], data[1]  # contract_id, actual_data
        elif isinstance(data, dict):
            return None, data
        else:
            return None, data
    
    def _validate_quote_data(self, quote_data: Dict) -> bool:
        """Validate quote data structure"""
        if not self.config.validate_data:
            return True
            
        required_fields = ['symbol']
        for field in required_fields:
            if field not in quote_data:
                if self.config.log_invalid_data:
                    logger.warning(f"Invalid quote data - missing {field}: {quote_data}")
                return False
        return True
    
    def _validate_trade_data(self, trade_data: List) -> bool:
        """Validate trade data structure"""
        if not self.config.validate_data:
            return True
            
        if not isinstance(trade_data, list):
            if self.config.log_invalid_data:
                logger.warning(f"Invalid trade data - not a list: {trade_data}")
            return False
            
        for trade in trade_data:
            if not isinstance(trade, dict):
                if self.config.log_invalid_data:
                    logger.warning(f"Invalid trade entry - not a dict: {trade}")
                return False
                
            required_fields = ['price', 'volume']
            for field in required_fields:
                if field not in trade:
                    if self.config.log_invalid_data:
                        logger.warning(f"Invalid trade data - missing {field}: {trade}")
                    return False
        return True
    
    def _validate_depth_data(self, depth_data: List) -> bool:
        """Validate depth data structure"""
        if not self.config.validate_data:
            return True
            
        if not isinstance(depth_data, list):
            if self.config.log_invalid_data:
                logger.warning(f"Invalid depth data - not a list: {depth_data}")
            return False
            
        for entry in depth_data:
            if not isinstance(entry, dict):
                if self.config.log_invalid_data:
                    logger.warning(f"Invalid depth entry - not a dict: {entry}")
                return False
                
            required_fields = ['price', 'volume', 'type']
            for field in required_fields:
                if field not in entry:
                    if self.config.log_invalid_data:
                        logger.warning(f"Invalid depth data - missing {field}: {entry}")
                    return False
        return True
    
    def _on_quote_update(self, data):
        """Handle quote updates with error handling and validation"""
        try:
            received_time = datetime.now(timezone.utc)
            self.stats.last_data_time = received_time
            self.stats.operations_count += 1
            
            # Parse data
            contract_id, quote_data = self._parse_market_data(data)
            if contract_id is None:
                contract_id = self.config.contract_id
            
            # Validate data
            if not self._validate_quote_data(quote_data):
                self.stats.invalid_records += 1
                return
            
            # Store to database
            self.database.store_quote_data(contract_id, quote_data, received_time)
            self.stats.quotes_received += 1
            self.stats.total_records += 1
            
            # Execute custom callbacks
            for callback in self.custom_callbacks['quote']:
                self.executor.submit(callback, contract_id, quote_data, received_time)
            
            # Memory management
            self._check_memory_management()
            
        except Exception as e:
            logger.error(f"Error processing quote update: {e}")
            self.stats.invalid_records += 1
    
    def _on_trade_update(self, data):
        """Handle trade updates with error handling and validation"""
        try:
            received_time = datetime.now(timezone.utc)
            self.stats.last_data_time = received_time
            self.stats.operations_count += 1
            
            # Parse data
            contract_id, trade_data = self._parse_market_data(data)
            if contract_id is None:
                contract_id = self.config.contract_id
            
            # Validate data
            if not self._validate_trade_data(trade_data):
                self.stats.invalid_records += 1
                return
            
            # Store to database
            self.database.store_trade_data(contract_id, trade_data, received_time)
            self.stats.trades_received += 1
            self.stats.total_records += len(trade_data) if isinstance(trade_data, list) else 1
            
            # Execute custom callbacks
            for callback in self.custom_callbacks['trade']:
                self.executor.submit(callback, contract_id, trade_data, received_time)
            
            # Memory management
            self._check_memory_management()
            
        except Exception as e:
            logger.error(f"Error processing trade update: {e}")
            self.stats.invalid_records += 1
    
    def _on_depth_update(self, data):
        """Handle depth updates with error handling and validation"""
        try:
            received_time = datetime.now(timezone.utc)
            self.stats.last_data_time = received_time
            self.stats.operations_count += 1
            
            # Parse data
            contract_id, depth_data = self._parse_market_data(data)
            if contract_id is None:
                contract_id = self.config.contract_id
            
            # Validate data
            if not self._validate_depth_data(depth_data):
                self.stats.invalid_records += 1
                return
            
            # Store to database
            self.database.store_depth_data(contract_id, depth_data, received_time)
            self.stats.depth_received += 1
            self.stats.total_records += len(depth_data) if isinstance(depth_data, list) else 1
            
            # Execute custom callbacks
            for callback in self.custom_callbacks['depth']:
                self.executor.submit(callback, contract_id, depth_data, received_time)
            
            # Memory management
            self._check_memory_management()
            
        except Exception as e:
            logger.error(f"Error processing depth update: {e}")
            self.stats.invalid_records += 1
    
    def _check_memory_management(self):
        """Check if memory management is needed"""
        if self.stats.operations_count % self.config.gc_threshold == 0:
            gc.collect()
            logger.debug(f"Garbage collection triggered after {self.stats.operations_count} operations")
    
    def _stats_monitor(self):
        """Background thread for logging statistics"""
        while self.running and not self.shutdown_flag.is_set():
            try:
                # Calculate rates
                runtime = (datetime.now() - self.stats.start_time).total_seconds()
                if runtime > 0:
                    quotes_per_sec = self.stats.quotes_received / runtime
                    trades_per_sec = self.stats.trades_received / runtime
                    depth_per_sec = self.stats.depth_received / runtime
                    total_per_sec = self.stats.total_records / runtime
                    
                    # Get queue size
                    queue_size = self.database.get_queue_size() if self.database else 0
                    
                    logger.info(
                        f"📊 STATS - Runtime: {runtime:.0f}s | "
                        f"Q: {self.stats.quotes_received}({quotes_per_sec:.1f}/s) | "
                        f"T: {self.stats.trades_received}({trades_per_sec:.1f}/s) | "
                        f"D: {self.stats.depth_received}({depth_per_sec:.1f}/s) | "
                        f"Total: {self.stats.total_records}({total_per_sec:.1f}/s) | "
                        f"Queue: {queue_size} | "
                        f"Memory: {self.stats.memory_usage_mb:.1f}MB | "
                        f"Errors: {self.stats.invalid_records}"
                    )
                
                time.sleep(self.config.stats_interval)
                
            except Exception as e:
                logger.error(f"Stats monitor error: {e}")
                time.sleep(5)
    
    def _memory_monitor(self):
        """Background thread for monitoring memory usage"""
        process = psutil.Process(os.getpid())
        
        while self.running and not self.shutdown_flag.is_set():
            try:
                # Get memory usage
                memory_info = process.memory_info()
                memory_mb = memory_info.rss / 1024 / 1024
                self.stats.memory_usage_mb = memory_mb
                
                # Check if memory limit exceeded
                if memory_mb > self.config.max_memory_usage_mb:
                    logger.warning(f"Memory usage high: {memory_mb:.1f}MB > {self.config.max_memory_usage_mb}MB")
                    
                    # Force garbage collection
                    gc.collect()
                    
                    # Check again
                    memory_info = process.memory_info()
                    memory_mb_after = memory_info.rss / 1024 / 1024
                    
                    if memory_mb_after > self.config.max_memory_usage_mb:
                        logger.error(f"Memory usage still high after GC: {memory_mb_after:.1f}MB")
                        # Could implement more aggressive memory management here
                
                time.sleep(self.config.memory_check_interval)
                
            except Exception as e:
                logger.error(f"Memory monitor error: {e}")
                time.sleep(10)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current streaming statistics"""
        runtime = (datetime.now() - self.stats.start_time).total_seconds()
        
        return {
            'session_id': self.config.session_id,
            'runtime_seconds': runtime,
            'quotes_received': self.stats.quotes_received,
            'trades_received': self.stats.trades_received,
            'depth_received': self.stats.depth_received,
            'total_records': self.stats.total_records,
            'invalid_records': self.stats.invalid_records,
            'connection_errors': self.stats.connection_errors,
            'memory_usage_mb': self.stats.memory_usage_mb,
            'last_data_time': self.stats.last_data_time.isoformat() if self.stats.last_data_time else None,
            'queue_size': self.database.get_queue_size() if self.database else 0,
            'rates': {
                'quotes_per_sec': self.stats.quotes_received / runtime if runtime > 0 else 0,
                'trades_per_sec': self.stats.trades_received / runtime if runtime > 0 else 0,
                'depth_per_sec': self.stats.depth_received / runtime if runtime > 0 else 0,
                'total_per_sec': self.stats.total_records / runtime if runtime > 0 else 0
            }
        }
    
    def run(self, duration_seconds: Optional[int] = None):
        """Run the streaming system for specified duration"""
        if not self.running:
            if not self.start():
                return False
        
        try:
            if duration_seconds:
                logger.info(f"🎧 Streaming for {duration_seconds} seconds...")
                self.shutdown_flag.wait(timeout=duration_seconds)
            else:
                logger.info("🎧 Streaming indefinitely (Ctrl+C to stop)...")
                self.shutdown_flag.wait()
                
        except KeyboardInterrupt:
            logger.info("Streaming interrupted by user")
        
        finally:
            self.stop()
        
        return True
    
    def stop(self):
        """Stop the streaming system gracefully"""
        if not self.running:
            return
        
        logger.info("🛑 Stopping Market Data Streaming System...")
        self.running = False
        self.shutdown_flag.set()
        
        # Stop market data streaming
        if self.client and hasattr(self.client, 'market_data'):
            try:
                self.client.market_data.unsubscribe_contract_all(self.config.contract_id)
                time.sleep(1)
                self.client.market_data.stop()
                logger.info("✅ Market data streaming stopped")
            except Exception as e:
                logger.error(f"Error stopping market data stream: {e}")
        
        # Wait for monitoring threads
        if self.stats_thread and self.stats_thread.is_alive():
            self.stats_thread.join(timeout=5)
        
        if self.memory_monitor_thread and self.memory_monitor_thread.is_alive():
            self.memory_monitor_thread.join(timeout=5)
        
        # Shutdown executor
        self.executor.shutdown(wait=True, timeout=10)
        
        # Close database
        if self.database:
            self.database.end_session(self.config.session_id)
            self.database.close()
        
        # Final stats
        final_stats = self.get_stats()
        logger.info(f"📊 Final Stats: {json.dumps(final_stats, indent=2, default=str)}")
        
        logger.info("✅ Market Data Streaming System stopped")

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

# Convenience function for quick setup
def create_streamer(username: str, api_key: str, contract_id: str, 
                   db_path: str = "market_data.db") -> MarketDataStreamer:
    """Create a market data streamer with default configuration"""
    
    streaming_config = StreamingConfig(
        username=username,
        api_key=api_key,
        contract_id=contract_id
    )
    
    db_config = DatabaseConfig(
        db_type='sqlite',
        db_path=db_path,
        batch_size=500,
        batch_timeout=3
    )
    
    return MarketDataStreamer(streaming_config, db_config)
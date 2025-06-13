#!/usr/bin/env python3
"""
Market Data Database Module
Robust database system for storing streaming market microstructure data
Optimized for high-frequency data ingestion and microstructure analysis
"""

import sqlite3
import json
import logging
import threading
import queue
import time
import os
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Union
from contextlib import contextmanager
from dataclasses import dataclass
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class DatabaseConfig:
    """Database configuration settings"""
    db_type: str = 'sqlite'  # 'sqlite' or 'postgresql'
    db_path: str = 'market_data.db'
    host: str = 'localhost'
    port: int = 5432
    database: str = 'market_data'
    username: str = 'postgres'
    password: str = 'password'
    batch_size: int = 1000
    batch_timeout: int = 5  # seconds
    enable_wal: bool = True  # WAL mode for SQLite
    memory_cache_size: int = 100000  # Number of records to cache in memory

class MarketDataDatabase:
    """
    High-performance database for storing streaming market data
    Supports both SQLite and PostgreSQL with batched inserts and memory management
    """
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
        self.batch_queue = queue.Queue()
        self.batch_worker_running = False
        self.batch_worker_thread = None
        self.lock = threading.Lock()
        
        # Initialize database
        self._initialize_database()
        self._start_batch_worker()
        
    def _initialize_database(self):
        """Initialize database connection and create tables"""
        try:
            if self.config.db_type == 'sqlite':
                self._initialize_sqlite()
            elif self.config.db_type == 'postgresql':
                self._initialize_postgresql()
            else:
                raise ValueError(f"Unsupported database type: {self.config.db_type}")
                
            self._create_tables()
            self._create_indexes()
            logger.info(f"Database initialized successfully: {self.config.db_type}")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def _initialize_sqlite(self):
        """Initialize SQLite database with optimizations"""
        os.makedirs(os.path.dirname(self.config.db_path) if os.path.dirname(self.config.db_path) else '.', exist_ok=True)
        
        self.connection = sqlite3.connect(
            self.config.db_path,
            check_same_thread=False,
            timeout=30.0
        )
        
        # Enable WAL mode for better concurrency
        if self.config.enable_wal:
            self.connection.execute("PRAGMA journal_mode=WAL")
        
        # Performance optimizations
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA cache_size=10000")
        self.connection.execute("PRAGMA temp_store=MEMORY")
        self.connection.execute("PRAGMA mmap_size=268435456")  # 256MB
        
    def _initialize_postgresql(self):
        """Initialize PostgreSQL database connection"""
        try:
            import psycopg2
        except ImportError:
            raise ImportError("psycopg2 is required for PostgreSQL support. Install with: pip install psycopg2-binary")
        
        self.connection = psycopg2.connect(
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
            user=self.config.username,
            password=self.config.password
        )
        self.connection.autocommit = False
        
    def _create_tables(self):
        """Create database tables for market data"""
        
        # Quotes table - stores best bid/ask data
        quotes_table = """
        CREATE TABLE IF NOT EXISTS quotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_timestamp TIMESTAMP NOT NULL,
            exchange_timestamp TIMESTAMP,
            contract_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            best_bid REAL,
            best_ask REAL,
            last_price REAL,
            change_amount REAL,
            change_percent REAL,
            volume INTEGER,
            last_updated TIMESTAMP,
            raw_data TEXT
        )
        """
        
        # Trades table - stores individual trade data
        trades_table = """
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_timestamp TIMESTAMP NOT NULL,
            exchange_timestamp TIMESTAMP,
            contract_id TEXT NOT NULL,
            symbol_id TEXT NOT NULL,
            price REAL NOT NULL,
            volume INTEGER NOT NULL,
            trade_type INTEGER,
            is_buy BOOLEAN,
            trade_sequence INTEGER,
            raw_data TEXT
        )
        """
        
        # Market depth table - stores order book data
        depth_table = """
        CREATE TABLE IF NOT EXISTS market_depth (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_timestamp TIMESTAMP NOT NULL,
            exchange_timestamp TIMESTAMP,
            contract_id TEXT NOT NULL,
            price REAL NOT NULL,
            volume INTEGER NOT NULL,
            current_volume INTEGER,
            depth_type INTEGER NOT NULL,
            side TEXT NOT NULL,
            level_rank INTEGER,
            raw_data TEXT
        )
        """
        
        # Statistics table for data monitoring
        stats_table = """
        CREATE TABLE IF NOT EXISTS ingestion_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TIMESTAMP NOT NULL,
            data_type TEXT NOT NULL,
            records_count INTEGER NOT NULL,
            batch_processing_time REAL,
            memory_usage_mb REAL
        )
        """
        
        # Sessions table to track data collection periods
        sessions_table = """
        CREATE TABLE IF NOT EXISTS data_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT UNIQUE NOT NULL,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            contract_id TEXT NOT NULL,
            status TEXT NOT NULL,
            quotes_count INTEGER DEFAULT 0,
            trades_count INTEGER DEFAULT 0,
            depth_count INTEGER DEFAULT 0
        )
        """
        
        with self.connection:
            cursor = self.connection.cursor()
            cursor.execute(quotes_table)
            cursor.execute(trades_table)
            cursor.execute(depth_table)
            cursor.execute(stats_table)
            cursor.execute(sessions_table)
            
    def _create_indexes(self):
        """Create indexes for query performance"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_quotes_timestamp ON quotes(received_timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_quotes_contract ON quotes(contract_id)",
            "CREATE INDEX IF NOT EXISTS idx_quotes_symbol ON quotes(symbol)",
            "CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(received_timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_trades_contract ON trades(contract_id)",
            "CREATE INDEX IF NOT EXISTS idx_trades_price ON trades(price)",
            "CREATE INDEX IF NOT EXISTS idx_depth_timestamp ON market_depth(received_timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_depth_contract ON market_depth(contract_id)",
            "CREATE INDEX IF NOT EXISTS idx_depth_price ON market_depth(price)",
            "CREATE INDEX IF NOT EXISTS idx_depth_side ON market_depth(side)",
            "CREATE INDEX IF NOT EXISTS idx_sessions_contract ON data_sessions(contract_id)",
            "CREATE INDEX IF NOT EXISTS idx_sessions_start_time ON data_sessions(start_time)"
        ]
        
        with self.connection:
            cursor = self.connection.cursor()
            for index in indexes:
                try:
                    cursor.execute(index)
                except Exception as e:
                    logger.warning(f"Failed to create index: {e}")
    
    def _start_batch_worker(self):
        """Start background thread for batch processing"""
        self.batch_worker_running = True
        self.batch_worker_thread = threading.Thread(target=self._batch_worker, daemon=True)
        self.batch_worker_thread.start()
        logger.info("Batch worker thread started")
    
    def _batch_worker(self):
        """Background worker for batched database inserts"""
        batch_quotes = []
        batch_trades = []
        batch_depth = []
        last_batch_time = time.time()
        
        while self.batch_worker_running:
            try:
                # Get item from queue with timeout
                try:
                    item = self.batch_queue.get(timeout=1.0)
                except queue.Empty:
                    # Check if we should flush batches due to timeout
                    if time.time() - last_batch_time > self.config.batch_timeout:
                        self._flush_batches(batch_quotes, batch_trades, batch_depth)
                        batch_quotes, batch_trades, batch_depth = [], [], []
                        last_batch_time = time.time()
                    continue
                
                # Add item to appropriate batch
                data_type, data = item
                if data_type == 'quote':
                    batch_quotes.append(data)
                elif data_type == 'trade':
                    batch_trades.append(data)
                elif data_type == 'depth':
                    batch_depth.append(data)
                
                # Flush batches if they reach batch size
                if (len(batch_quotes) >= self.config.batch_size or 
                    len(batch_trades) >= self.config.batch_size or 
                    len(batch_depth) >= self.config.batch_size):
                    
                    self._flush_batches(batch_quotes, batch_trades, batch_depth)
                    batch_quotes, batch_trades, batch_depth = [], [], []
                    last_batch_time = time.time()
                
                self.batch_queue.task_done()
                
            except Exception as e:
                logger.error(f"Batch worker error: {e}")
                time.sleep(1)
        
        # Final flush when shutting down
        self._flush_batches(batch_quotes, batch_trades, batch_depth)
        logger.info("Batch worker thread stopped")
    
    def _flush_batches(self, quotes: List, trades: List, depth: List):
        """Flush batched data to database"""
        start_time = time.time()
        
        try:
            with self.lock:
                cursor = self.connection.cursor()
                
                # Insert quotes
                if quotes:
                    cursor.executemany("""
                        INSERT INTO quotes (
                            received_timestamp, exchange_timestamp, contract_id, symbol,
                            best_bid, best_ask, last_price, change_amount, change_percent,
                            volume, last_updated, raw_data
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, quotes)
                    logger.debug(f"Inserted {len(quotes)} quote records")
                
                # Insert trades
                if trades:
                    cursor.executemany("""
                        INSERT INTO trades (
                            received_timestamp, exchange_timestamp, contract_id, symbol_id,
                            price, volume, trade_type, is_buy, trade_sequence, raw_data
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, trades)
                    logger.debug(f"Inserted {len(trades)} trade records")
                
                # Insert depth
                if depth:
                    cursor.executemany("""
                        INSERT INTO market_depth (
                            received_timestamp, exchange_timestamp, contract_id, price,
                            volume, current_volume, depth_type, side, level_rank, raw_data
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, depth)
                    logger.debug(f"Inserted {len(depth)} depth records")
                
                self.connection.commit()
                
                # Log performance stats
                processing_time = time.time() - start_time
                total_records = len(quotes) + len(trades) + len(depth)
                
                if total_records > 0:
                    logger.info(f"Batch flush: {total_records} records in {processing_time:.3f}s "
                              f"({total_records/processing_time:.0f} records/sec)")
                
        except Exception as e:
            logger.error(f"Failed to flush batches: {e}")
            self.connection.rollback()
    
    def store_quote_data(self, contract_id: str, quote_data: Dict[str, Any], received_time: Optional[datetime] = None):
        """Store quote data asynchronously"""
        if received_time is None:
            received_time = datetime.now(timezone.utc)
        
        try:
            # Parse exchange timestamp
            exchange_timestamp = None
            if 'timestamp' in quote_data:
                exchange_timestamp = self._parse_timestamp(quote_data['timestamp'])
            elif 'lastUpdated' in quote_data:
                exchange_timestamp = self._parse_timestamp(quote_data['lastUpdated'])
            
            # Prepare data tuple
            data = (
                received_time.isoformat(),
                exchange_timestamp.isoformat() if exchange_timestamp else None,
                contract_id,
                quote_data.get('symbol', ''),
                quote_data.get('bestBid'),
                quote_data.get('bestAsk'),
                quote_data.get('lastPrice'),
                quote_data.get('change'),
                quote_data.get('changePercent'),
                quote_data.get('volume'),
                exchange_timestamp.isoformat() if exchange_timestamp else None,
                json.dumps(quote_data)
            )
            
            # Add to batch queue
            self.batch_queue.put(('quote', data))
            
        except Exception as e:
            logger.error(f"Error storing quote data: {e}")
    
    def store_trade_data(self, contract_id: str, trade_list: List[Dict[str, Any]], received_time: Optional[datetime] = None):
        """Store trade data asynchronously"""
        if received_time is None:
            received_time = datetime.now(timezone.utc)
        
        try:
            for i, trade in enumerate(trade_list):
                # Parse exchange timestamp
                exchange_timestamp = None
                if 'timestamp' in trade:
                    exchange_timestamp = self._parse_timestamp(trade['timestamp'])
                
                # Determine if it's a buy (type 0) or sell (type 1)
                trade_type = trade.get('type', 0)
                is_buy = trade_type == 0
                
                # Prepare data tuple
                data = (
                    received_time.isoformat(),
                    exchange_timestamp.isoformat() if exchange_timestamp else None,
                    contract_id,
                    trade.get('symbolId', ''),
                    trade.get('price', 0.0),
                    trade.get('volume', 0),
                    trade_type,
                    is_buy,
                    i,  # trade sequence within this batch
                    json.dumps(trade)
                )
                
                # Add to batch queue
                self.batch_queue.put(('trade', data))
                
        except Exception as e:
            logger.error(f"Error storing trade data: {e}")
    
    def store_depth_data(self, contract_id: str, depth_list: List[Dict[str, Any]], received_time: Optional[datetime] = None):
        """Store market depth data asynchronously"""
        if received_time is None:
            received_time = datetime.now(timezone.utc)
        
        try:
            for i, depth_entry in enumerate(depth_list):
                # Parse exchange timestamp
                exchange_timestamp = None
                if 'timestamp' in depth_entry:
                    exchange_timestamp = self._parse_timestamp(depth_entry['timestamp'])
                
                # Determine side based on type (3=ask, 4=bid, 5=trade)
                depth_type = depth_entry.get('type', 0)
                side = 'ask' if depth_type == 3 else 'bid' if depth_type == 4 else 'trade'
                
                # Prepare data tuple
                data = (
                    received_time.isoformat(),
                    exchange_timestamp.isoformat() if exchange_timestamp else None,
                    contract_id,
                    depth_entry.get('price', 0.0),
                    depth_entry.get('volume', 0),
                    depth_entry.get('currentVolume', 0),
                    depth_type,
                    side,
                    i,  # level rank
                    json.dumps(depth_entry)
                )
                
                # Add to batch queue
                self.batch_queue.put(('depth', data))
                
        except Exception as e:
            logger.error(f"Error storing depth data: {e}")
    
    def _parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """Parse various timestamp formats"""
        try:
            # Handle ISO format with timezone
            if '+' in timestamp_str or timestamp_str.endswith('Z'):
                return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                return datetime.fromisoformat(timestamp_str)
        except:
            return None
    
    def start_session(self, session_id: str, contract_id: str) -> str:
        """Start a new data collection session"""
        try:
            with self.connection:
                cursor = self.connection.cursor()
                cursor.execute("""
                    INSERT INTO data_sessions (session_id, start_time, contract_id, status)
                    VALUES (?, ?, ?, ?)
                """, (session_id, datetime.now(timezone.utc).isoformat(), contract_id, 'active'))
            
            logger.info(f"Started data session: {session_id}")
            return session_id
            
        except Exception as e:
            logger.error(f"Failed to start session: {e}")
            raise
    
    def end_session(self, session_id: str):
        """End a data collection session"""
        try:
            with self.connection:
                cursor = self.connection.cursor()
                
                # Get session stats
                cursor.execute("""
                    SELECT contract_id, start_time FROM data_sessions 
                    WHERE session_id = ?
                """, (session_id,))
                result = cursor.fetchone()
                
                if result:
                    contract_id, start_time = result
                    start_dt = datetime.fromisoformat(start_time)
                    
                    # Count records for this session
                    cursor.execute("""
                        SELECT COUNT(*) FROM quotes 
                        WHERE contract_id = ? AND received_timestamp >= ?
                    """, (contract_id, start_time))
                    quotes_count = cursor.fetchone()[0]
                    
                    cursor.execute("""
                        SELECT COUNT(*) FROM trades 
                        WHERE contract_id = ? AND received_timestamp >= ?
                    """, (contract_id, start_time))
                    trades_count = cursor.fetchone()[0]
                    
                    cursor.execute("""
                        SELECT COUNT(*) FROM market_depth 
                        WHERE contract_id = ? AND received_timestamp >= ?
                    """, (contract_id, start_time))
                    depth_count = cursor.fetchone()[0]
                    
                    # Update session
                    cursor.execute("""
                        UPDATE data_sessions 
                        SET end_time = ?, status = ?, quotes_count = ?, trades_count = ?, depth_count = ?
                        WHERE session_id = ?
                    """, (datetime.now(timezone.utc).isoformat(), 'completed', 
                         quotes_count, trades_count, depth_count, session_id))
                    
                    logger.info(f"Ended session {session_id}: Q:{quotes_count}, T:{trades_count}, D:{depth_count}")
            
        except Exception as e:
            logger.error(f"Failed to end session: {e}")
    
    def get_session_stats(self, session_id: str) -> Dict[str, Any]:
        """Get statistics for a data session"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT * FROM data_sessions WHERE session_id = ?
            """, (session_id,))
            
            result = cursor.fetchone()
            if result:
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, result))
            return {}
            
        except Exception as e:
            logger.error(f"Failed to get session stats: {e}")
            return {}
    
    def get_data_range(self, contract_id: str, start_time: datetime, end_time: datetime, 
                      data_types: List[str] = None) -> Dict[str, pd.DataFrame]:
        """Retrieve data for analysis"""
        if data_types is None:
            data_types = ['quotes', 'trades', 'depth']
        
        results = {}
        
        try:
            for data_type in data_types:
                if data_type == 'quotes':
                    query = """
                        SELECT * FROM quotes 
                        WHERE contract_id = ? AND received_timestamp BETWEEN ? AND ?
                        ORDER BY received_timestamp
                    """
                elif data_type == 'trades':
                    query = """
                        SELECT * FROM trades 
                        WHERE contract_id = ? AND received_timestamp BETWEEN ? AND ?
                        ORDER BY received_timestamp
                    """
                elif data_type == 'depth':
                    query = """
                        SELECT * FROM market_depth 
                        WHERE contract_id = ? AND received_timestamp BETWEEN ? AND ?
                        ORDER BY received_timestamp, price
                    """
                else:
                    continue
                
                df = pd.read_sql_query(
                    query, 
                    self.connection, 
                    params=[contract_id, start_time.isoformat(), end_time.isoformat()]
                )
                
                if not df.empty:
                    df['received_timestamp'] = pd.to_datetime(df['received_timestamp'])
                    if 'exchange_timestamp' in df.columns:
                        df['exchange_timestamp'] = pd.to_datetime(df['exchange_timestamp'])
                
                results[data_type] = df
                logger.info(f"Retrieved {len(df)} {data_type} records")
        
        except Exception as e:
            logger.error(f"Failed to retrieve data: {e}")
        
        return results
    
    def get_queue_size(self) -> int:
        """Get current batch queue size"""
        return self.batch_queue.qsize()
    
    def close(self):
        """Close database connection and cleanup"""
        logger.info("Shutting down database...")
        
        # Stop batch worker
        self.batch_worker_running = False
        if self.batch_worker_thread:
            self.batch_worker_thread.join(timeout=10)
        
        # Wait for queue to empty
        if not self.batch_queue.empty():
            logger.info(f"Waiting for {self.batch_queue.qsize()} items to be processed...")
            self.batch_queue.join()
        
        # Close connection
        if self.connection:
            self.connection.close()
        
        logger.info("Database closed successfully")

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
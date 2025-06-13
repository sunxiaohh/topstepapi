# Market Data Streaming and Database System

## Overview

This system provides a robust, high-performance solution for capturing, storing, and analyzing real-time market microstructure data from TopStep. It's designed specifically for developing microstructure order flow analysis strategies.

## System Components

### 1. Database Layer (`market_data_db.py`)
- **High-performance database** with SQLite and PostgreSQL support
- **Batched inserts** to handle high-frequency data without memory issues
- **Automatic schema creation** for quotes, trades, and market depth
- **Memory management** with configurable limits and garbage collection
- **Session tracking** for organizing data collection periods

### 2. Streaming System (`market_data_streamer.py`)
- **Real-time data capture** from TopStep market data feeds
- **Memory-efficient processing** with configurable thresholds
- **Error handling and reconnection** logic for reliability
- **Performance monitoring** with real-time statistics
- **Data validation** to ensure data quality

### 3. Analysis Tools (`market_data_analyzer.py`)
- **Microstructure metrics** calculation (spreads, volatility, order flow)
- **Data export** capabilities (CSV, Parquet, JSON)
- **Visualization tools** for market data analysis
- **Session comparison** and historical analysis

### 4. Main Interface (`run_market_data_capture.py`)
- **Command-line interface** for easy operation
- **Multiple operation modes** (stream, analyze, export, list)
- **Configuration options** for all system parameters

## Quick Start

### 1. Basic Data Capture
```bash
# Activate virtual environment
source venv/bin/activate

# Start streaming with your credentials
python3 run_market_data_capture.py \
    --username "xiaosun666" \
    --api-key "MmP9cmQB8Zra4UVLQnQUipyIeBDObzyLmu/NZdkwHUA=" \
    --contract "CON.F.US.MNQ.M25" \
    --duration 300  # 5 minutes
```

### 2. List Captured Sessions
```bash
python3 run_market_data_capture.py --list-sessions
```

### 3. Analyze a Session
```bash
python3 run_market_data_capture.py \
    --analyze-session "session_20250612_230745" \
    --db-path "market_data.db"
```

### 4. Export Data for Analysis
```bash
python3 run_market_data_capture.py \
    --export-session "session_20250612_230745" \
    --export-format "csv"
```

## Database Schema

### Quotes Table
- **received_timestamp**: When data was received
- **exchange_timestamp**: Exchange timestamp
- **contract_id**: Contract identifier
- **symbol**: Trading symbol
- **best_bid/best_ask**: Top of book prices
- **last_price**: Last traded price
- **volume**: Cumulative volume
- **raw_data**: Original JSON data

### Trades Table
- **received_timestamp**: When trade was received
- **exchange_timestamp**: Trade execution time
- **contract_id**: Contract identifier
- **price**: Trade price
- **volume**: Trade size
- **trade_type**: Buy (0) or Sell (1)
- **raw_data**: Original JSON data

### Market Depth Table
- **received_timestamp**: When depth update was received
- **exchange_timestamp**: Exchange timestamp
- **contract_id**: Contract identifier
- **price**: Price level
- **volume**: Volume at level
- **depth_type**: Update type (3=ask, 4=bid, 5=trade)
- **side**: bid/ask/trade
- **raw_data**: Original JSON data

## Advanced Usage

### Custom Analysis Scripts
```python
from market_data_analyzer import MarketDataAnalyzer
from market_data_db import DatabaseConfig

# Load analyzer
config = DatabaseConfig(db_type='sqlite', db_path='market_data.db')
analyzer = MarketDataAnalyzer(config)

# Load session data
data = analyzer.load_session_data('session_20250612_230745')

# Calculate metrics
metrics = analyzer.calculate_microstructure_metrics(data)

# Create visualizations
analyzer.plot_session_overview('session_20250612_230745', 'analysis.png')
```

### High-Frequency Streaming
```python
from market_data_streamer import MarketDataStreamer, StreamingConfig
from market_data_db import DatabaseConfig

# Configure for high-frequency capture
streaming_config = StreamingConfig(
    username="your_username",
    api_key="your_api_key",
    contract_id="CON.F.US.MNQ.M25",
    memory_check_interval=10,  # Check memory every 10 seconds
    max_memory_usage_mb=4096,  # 4GB limit
    gc_threshold=5000         # Garbage collect every 5000 operations
)

db_config = DatabaseConfig(
    batch_size=1000,          # Larger batches for better performance
    batch_timeout=2           # Faster flush for real-time analysis
)

# Run streaming
with MarketDataStreamer(streaming_config, db_config) as streamer:
    streamer.run(duration_seconds=3600)  # 1 hour
```

## Performance Optimization

### Memory Management
- **Batched database inserts** prevent memory buildup
- **Automatic garbage collection** at configurable intervals
- **Memory monitoring** with alerts and limits
- **Queue size monitoring** to prevent overflow

### Database Performance
- **WAL mode** for SQLite improves concurrency
- **Optimized indexes** for time-series queries
- **Compression options** for long-term storage
- **PostgreSQL support** for larger datasets

### System Monitoring
- **Real-time statistics** (records/second, memory usage)
- **Error tracking** and logging
- **Connection monitoring** with automatic reconnection
- **Performance metrics** collection

## Data Analysis Features

### Microstructure Metrics
- **Bid-ask spreads** (absolute and basis points)
- **Price volatility** (realized and implied)
- **Order flow analysis** (buy/sell ratios, volume imbalance)
- **Market depth** (liquidity analysis)
- **Price impact** (trade impact on quotes)

### Statistical Analysis
- **Volume distribution** analysis
- **Trade size clustering**
- **Spread dynamics**
- **Liquidity provision patterns**
- **High-frequency correlations**

### Visualization Tools
- **Price and spread charts**
- **Trade volume scatter plots**
- **Market depth heatmaps**
- **Order flow visualizations**
- **Performance dashboards**

## Configuration Options

### Streaming Configuration
```python
StreamingConfig(
    enable_quotes=True,           # Enable quote streaming
    enable_trades=True,           # Enable trade streaming
    enable_depth=True,            # Enable depth streaming
    memory_check_interval=30,     # Memory check frequency (seconds)
    max_memory_usage_mb=2048,     # Memory limit (MB)
    reconnect_attempts=5,         # Connection retry attempts
    validate_data=True,           # Enable data validation
    stats_interval=60             # Statistics logging interval
)
```

### Database Configuration
```python
DatabaseConfig(
    db_type='sqlite',             # 'sqlite' or 'postgresql'
    db_path='market_data.db',     # Database file path
    batch_size=1000,              # Records per batch
    batch_timeout=5,              # Batch timeout (seconds)
    enable_wal=True               # Enable WAL mode (SQLite)
)
```

## Best Practices

### For High-Frequency Data
1. **Use larger batch sizes** (1000+) for better performance
2. **Monitor memory usage** and set appropriate limits
3. **Enable WAL mode** for SQLite or use PostgreSQL for very high volumes
4. **Regular garbage collection** to prevent memory leaks

### For Analysis
1. **Export to Parquet** for faster analysis of large datasets
2. **Use time-based filtering** to focus on specific periods
3. **Aggregate data** for longer-term analysis
4. **Create indexes** on frequently queried columns

### For Production
1. **Set up log rotation** to manage log file sizes
2. **Monitor disk space** for database growth
3. **Implement backup strategies** for important data
4. **Use PostgreSQL** for multi-user environments

## Troubleshooting

### Connection Issues
- Check TopStep API credentials
- Verify network connectivity
- Review logs for specific error messages
- Ensure contract ID is valid

### Memory Issues
- Reduce batch size or timeout
- Lower memory limit threshold
- Increase garbage collection frequency
- Monitor system resources

### Performance Issues
- Check database indexes
- Optimize batch parameters
- Monitor queue sizes
- Consider PostgreSQL for large volumes

## Files Created

1. **`market_data_db.py`** - Database layer with high-performance storage
2. **`market_data_streamer.py`** - Real-time streaming system
3. **`market_data_analyzer.py`** - Analysis and visualization tools
4. **`run_market_data_capture.py`** - Main command-line interface
5. **`requirements.txt`** - Updated package dependencies

## Next Steps for Strategy Development

With this system in place, you can now:

1. **Collect extensive microstructure data** over various market conditions
2. **Analyze order flow patterns** and market maker behavior
3. **Develop predictive models** using the captured data
4. **Backtest strategies** using historical microstructure data
5. **Implement real-time strategy signals** based on the streaming data

The system is designed to handle continuous data collection without memory issues, making it suitable for long-term data gathering for strategy development.
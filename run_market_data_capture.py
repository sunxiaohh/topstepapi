#!/usr/bin/env python3
"""
Market Data Capture - Main Execution Script
Run this script to start capturing streaming market data
"""

import os
import sys
import argparse
import json
import signal
from datetime import datetime

# Add current directory to path to import our modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from market_data_streamer import MarketDataStreamer, StreamingConfig, create_streamer
from market_data_db import DatabaseConfig
from market_data_analyzer import MarketDataAnalyzer, quick_analysis, list_sessions

def main():
    parser = argparse.ArgumentParser(description='Market Data Capture System')
    parser.add_argument('--username', required=True, help='TopStep username')
    parser.add_argument('--api-key', required=True, help='TopStep API key')
    parser.add_argument('--contract', default='CON.F.US.MNQ.M25', help='Contract ID to stream')
    parser.add_argument('--duration', type=int, help='Duration in seconds (default: run indefinitely)')
    parser.add_argument('--db-path', default='market_data.db', help='Database file path')
    parser.add_argument('--session-id', help='Custom session ID')
    parser.add_argument('--batch-size', type=int, default=500, help='Database batch size')
    parser.add_argument('--memory-limit', type=int, default=2048, help='Memory limit in MB')
    parser.add_argument('--no-quotes', action='store_true', help='Disable quote streaming')
    parser.add_argument('--no-trades', action='store_true', help='Disable trade streaming')
    parser.add_argument('--no-depth', action='store_true', help='Disable depth streaming')
    parser.add_argument('--list-sessions', action='store_true', help='List existing sessions')
    parser.add_argument('--analyze-session', help='Analyze specific session')
    parser.add_argument('--export-session', help='Export session data')
    parser.add_argument('--export-format', choices=['csv', 'parquet', 'json'], default='csv', help='Export format')
    
    args = parser.parse_args()
    
    # Create logs directory
    os.makedirs('logs', exist_ok=True)
    
    # List sessions mode
    if args.list_sessions:
        print("📋 Available Data Sessions:")
        print("=" * 50)
        sessions = list_sessions(args.db_path)
        if not sessions.empty:
            for _, session in sessions.iterrows():
                print(f"Session: {session['session_id']}")
                print(f"  Start: {session['start_time']}")
                print(f"  Duration: {session.get('duration_minutes', 'N/A'):.1f} minutes")
                print(f"  Contract: {session['contract_id']}")
                print(f"  Data: Q:{session['quotes_count']}, T:{session['trades_count']}, D:{session['depth_count']}")
                print(f"  Status: {session['status']}")
                print()
        else:
            print("No sessions found.")
        return
    
    # Analysis mode
    if args.analyze_session:
        print(f"📊 Analyzing Session: {args.analyze_session}")
        print("=" * 50)
        
        report = quick_analysis(args.analyze_session, args.db_path)
        if 'error' in report:
            print(f"❌ Error: {report['error']}")
            return
        
        # Print summary
        print(f"Session ID: {report['session_id']}")
        print(f"Generated: {report['generated_at']}")
        
        if 'session_metadata' in report:
            meta = report['session_metadata']
            print(f"Duration: {meta.get('duration_minutes', 'N/A'):.1f} minutes")
            print(f"Contract: {meta.get('contract_id', 'N/A')}")
        
        print("\nData Summary:")
        summary = report['data_summary']
        print(f"  Quotes: {summary['quotes_count']:,}")
        print(f"  Trades: {summary['trades_count']:,}")
        print(f"  Depth Updates: {summary['depth_count']:,}")
        
        # Print key metrics
        metrics = report.get('microstructure_metrics', {})
        
        if 'spread_analysis' in metrics:
            spread = metrics['spread_analysis']
            print(f"\nSpread Analysis:")
            print(f"  Average Spread: {spread.get('avg_spread', 0):.4f}")
            print(f"  Average Spread (bps): {spread.get('avg_spread_bps', 0):.2f}")
            print(f"  Spread Range: {spread.get('min_spread', 0):.4f} - {spread.get('max_spread', 0):.4f}")
        
        if 'trade_analysis' in metrics:
            trades = metrics['trade_analysis']
            print(f"\nTrade Analysis:")
            print(f"  Total Volume: {trades.get('total_volume', 0):,}")
            print(f"  Average Trade Size: {trades.get('avg_trade_size', 0):.1f}")
            print(f"  Price Range: {trades.get('max_trade_size', 0) - trades.get('min_trade_size', 0)}")
        
        if 'order_flow' in metrics:
            flow = metrics['order_flow']
            print(f"\nOrder Flow:")
            print(f"  Buy/Sell Ratio: {flow.get('buy_ratio', 0):.2f}")
            print(f"  Volume Imbalance: {flow.get('volume_imbalance', 0):,}")
        
        # Save detailed report
        report_file = f"{args.analyze_session}_analysis_report.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        print(f"\n💾 Detailed report saved to: {report_file}")
        
        return
    
    # Export mode
    if args.export_session:
        print(f"📤 Exporting Session: {args.export_session}")
        
        db_config = DatabaseConfig(db_type='sqlite', db_path=args.db_path)
        analyzer = MarketDataAnalyzer(db_config)
        
        exported = analyzer.export_session_data(
            args.export_session, 
            args.export_format, 
            'exports'
        )
        
        if exported:
            print("✅ Export completed:")
            for data_type, filepath in exported.items():
                print(f"  {data_type}: {filepath}")
        else:
            print("❌ Export failed")
        
        analyzer.close()
        return
    
    # Streaming mode (default)
    print("🚀 Starting Market Data Capture System")
    print("=" * 50)
    print(f"Contract: {args.contract}")
    print(f"Database: {args.db_path}")
    print(f"Duration: {'Indefinite' if not args.duration else f'{args.duration} seconds'}")
    print(f"Batch Size: {args.batch_size}")
    print(f"Memory Limit: {args.memory_limit} MB")
    print()
    
    # Configuration
    streaming_config = StreamingConfig(
        username=args.username,
        api_key=args.api_key,
        contract_id=args.contract,
        session_id=args.session_id or f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        enable_quotes=not args.no_quotes,
        enable_trades=not args.no_trades,
        enable_depth=not args.no_depth,
        max_memory_usage_mb=args.memory_limit
    )
    
    db_config = DatabaseConfig(
        db_type='sqlite',
        db_path=args.db_path,
        batch_size=args.batch_size,
        batch_timeout=3
    )
    
    # Create and run streamer
    streamer = MarketDataStreamer(streaming_config, db_config)
    
    try:
        print(f"📡 Starting streaming session: {streaming_config.session_id}")
        success = streamer.run(args.duration)
        
        if success:
            print("✅ Streaming completed successfully")
        else:
            print("❌ Streaming failed")
            
    except KeyboardInterrupt:
        print("\n🛑 Streaming interrupted by user")
    except Exception as e:
        print(f"❌ Streaming error: {e}")
    
    finally:
        # Print final statistics
        stats = streamer.get_stats()
        print("\n📊 Final Statistics:")
        print(f"  Runtime: {stats['runtime_seconds']:.1f} seconds")
        print(f"  Total Records: {stats['total_records']:,}")
        print(f"  Quotes: {stats['quotes_received']:,}")
        print(f"  Trades: {stats['trades_received']:,}")
        print(f"  Depth Updates: {stats['depth_received']:,}")
        print(f"  Invalid Records: {stats['invalid_records']:,}")
        print(f"  Memory Used: {stats['memory_usage_mb']:.1f} MB")
        
        if stats['runtime_seconds'] > 0:
            print(f"  Rate: {stats['rates']['total_per_sec']:.1f} records/sec")
        
        print(f"\n💾 Data saved to database: {args.db_path}")
        print(f"📋 Session ID: {streaming_config.session_id}")
        
        # Quick analysis tip
        print(f"\n💡 To analyze this session:")
        print(f"   python run_market_data_capture.py --analyze-session {streaming_config.session_id} --db-path {args.db_path}")

if __name__ == "__main__":
    main()
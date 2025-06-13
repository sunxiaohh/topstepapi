#!/usr/bin/env python3
"""
Monitor ongoing data collection
"""

import sqlite3
import time
import os
from datetime import datetime

def get_data_counts(db_path="market_data.db"):
    """Get current data counts"""
    if not os.path.exists(db_path):
        return None
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM quotes")
            quotes = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM trades") 
            trades = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM market_depth")
            depth = cursor.fetchone()[0]
            
            # Get latest timestamp
            cursor.execute("SELECT MAX(received_timestamp) FROM quotes")
            last_quote_time = cursor.fetchone()[0]
            
            return {
                'quotes': quotes,
                'trades': trades, 
                'depth': depth,
                'total': quotes + trades + depth,
                'last_update': last_quote_time
            }
    except Exception as e:
        print(f"Error reading database: {e}")
        return None

def get_file_size(db_path="market_data.db"):
    """Get database file size in MB"""
    if os.path.exists(db_path):
        size_bytes = os.path.getsize(db_path)
        return size_bytes / (1024 * 1024)
    return 0

def main():
    print("📊 Market Data Collection Monitor")
    print("=" * 50)
    print("Press Ctrl+C to stop monitoring\n")
    
    start_time = datetime.now()
    previous_counts = None
    
    try:
        while True:
            current_counts = get_data_counts()
            file_size = get_file_size()
            
            if current_counts:
                runtime = (datetime.now() - start_time).total_seconds()
                
                # Calculate rates
                if previous_counts:
                    time_diff = 10  # monitoring every 10 seconds
                    quote_rate = (current_counts['quotes'] - previous_counts['quotes']) / time_diff
                    trade_rate = (current_counts['trades'] - previous_counts['trades']) / time_diff
                    depth_rate = (current_counts['depth'] - previous_counts['depth']) / time_diff
                    total_rate = (current_counts['total'] - previous_counts['total']) / time_diff
                else:
                    quote_rate = trade_rate = depth_rate = total_rate = 0
                
                # Display current status
                print(f"\r🕐 Runtime: {runtime:.0f}s | "
                      f"📊 Total: {current_counts['total']:,} | "
                      f"Q: {current_counts['quotes']:,} ({quote_rate:.1f}/s) | "
                      f"T: {current_counts['trades']:,} ({trade_rate:.1f}/s) | "
                      f"D: {current_counts['depth']:,} ({depth_rate:.1f}/s) | "
                      f"💾 {file_size:.1f}MB | "
                      f"📈 {total_rate:.1f} rec/s", end="", flush=True)
                
                previous_counts = current_counts
            else:
                print("\r❌ No data found - check if streaming is running", end="", flush=True)
            
            time.sleep(10)
            
    except KeyboardInterrupt:
        print(f"\n\n📋 Final Summary:")
        if current_counts:
            print(f"  Total Records: {current_counts['total']:,}")
            print(f"  Quotes: {current_counts['quotes']:,}")
            print(f"  Trades: {current_counts['trades']:,}")
            print(f"  Depth Updates: {current_counts['depth']:,}")
            print(f"  Database Size: {file_size:.1f} MB")
            print(f"  Last Update: {current_counts['last_update']}")
        print("\nMonitoring stopped.")

if __name__ == "__main__":
    main()
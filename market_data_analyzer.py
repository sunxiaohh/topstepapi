#!/usr/bin/env python3
"""
Market Data Analysis Module
Tools for analyzing stored microstructure data for trading strategies
"""

import pandas as pd
import numpy as np
import sqlite3
import logging
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import warnings
warnings.filterwarnings('ignore')

from market_data_db import MarketDataDatabase, DatabaseConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class AnalysisConfig:
    """Configuration for market data analysis"""
    timeframe_seconds: int = 60  # Default 1-minute analysis
    min_trade_size: int = 1
    max_spread_bps: float = 10.0  # 10 basis points
    depth_levels: int = 10
    volume_buckets: List[int] = None
    
    def __post_init__(self):
        if self.volume_buckets is None:
            self.volume_buckets = [1, 5, 10, 25, 50, 100]

class MarketDataAnalyzer:
    """
    Comprehensive market microstructure analysis tools
    """
    
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.database = MarketDataDatabase(db_config)
        self.analysis_config = AnalysisConfig()
        
    def get_sessions(self) -> pd.DataFrame:
        """Get all data collection sessions"""
        try:
            query = """
                SELECT session_id, start_time, end_time, contract_id, status,
                       quotes_count, trades_count, depth_count
                FROM data_sessions
                ORDER BY start_time DESC
            """
            
            with sqlite3.connect(self.db_config.db_path) as conn:
                df = pd.read_sql_query(query, conn)
                df['start_time'] = pd.to_datetime(df['start_time'])
                df['end_time'] = pd.to_datetime(df['end_time'])
                df['duration_minutes'] = (df['end_time'] - df['start_time']).dt.total_seconds() / 60
                
            return df
            
        except Exception as e:
            logger.error(f"Error getting sessions: {e}")
            return pd.DataFrame()
    
    def load_session_data(self, session_id: str) -> Dict[str, pd.DataFrame]:
        """Load all data for a specific session"""
        try:
            # Get session info
            session_query = """
                SELECT start_time, end_time, contract_id 
                FROM data_sessions 
                WHERE session_id = ?
            """
            
            with sqlite3.connect(self.db_config.db_path) as conn:
                session_info = pd.read_sql_query(session_query, conn, params=[session_id])
                
                if session_info.empty:
                    logger.error(f"Session {session_id} not found")
                    return {}
                
                start_time = pd.to_datetime(session_info.iloc[0]['start_time'])
                end_time = pd.to_datetime(session_info.iloc[0]['end_time']) 
                contract_id = session_info.iloc[0]['contract_id']
                
                # Load all data types
                data = {}
                
                # Quotes
                quotes_query = """
                    SELECT * FROM quotes 
                    WHERE contract_id = ? AND received_timestamp BETWEEN ? AND ?
                    ORDER BY received_timestamp
                """
                data['quotes'] = pd.read_sql_query(
                    quotes_query, conn, 
                    params=[contract_id, start_time.isoformat(), end_time.isoformat()]
                )
                
                # Trades
                trades_query = """
                    SELECT * FROM trades 
                    WHERE contract_id = ? AND received_timestamp BETWEEN ? AND ?
                    ORDER BY received_timestamp
                """
                data['trades'] = pd.read_sql_query(
                    trades_query, conn,
                    params=[contract_id, start_time.isoformat(), end_time.isoformat()]
                )
                
                # Market depth
                depth_query = """
                    SELECT * FROM market_depth 
                    WHERE contract_id = ? AND received_timestamp BETWEEN ? AND ?
                    ORDER BY received_timestamp, price
                """
                data['depth'] = pd.read_sql_query(
                    depth_query, conn,
                    params=[contract_id, start_time.isoformat(), end_time.isoformat()]
                )
                
                # Convert timestamps
                for key in data:
                    if not data[key].empty:
                        data[key]['received_timestamp'] = pd.to_datetime(data[key]['received_timestamp'])
                        if 'exchange_timestamp' in data[key].columns:
                            data[key]['exchange_timestamp'] = pd.to_datetime(data[key]['exchange_timestamp'])
                
                logger.info(f"Loaded session {session_id}: "
                          f"Q:{len(data['quotes'])}, T:{len(data['trades'])}, D:{len(data['depth'])}")
                
                return data
                
        except Exception as e:
            logger.error(f"Error loading session data: {e}")
            return {}
    
    def calculate_microstructure_metrics(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Calculate comprehensive microstructure metrics"""
        metrics = {}
        
        try:
            quotes_df = data.get('quotes', pd.DataFrame())
            trades_df = data.get('trades', pd.DataFrame())
            depth_df = data.get('depth', pd.DataFrame())
            
            # Basic statistics
            metrics['data_counts'] = {
                'quotes': len(quotes_df),
                'trades': len(trades_df),
                'depth_updates': len(depth_df)
            }
            
            if not quotes_df.empty:
                metrics.update(self._analyze_quotes(quotes_df))
            
            if not trades_df.empty:
                metrics.update(self._analyze_trades(trades_df))
            
            if not depth_df.empty:
                metrics.update(self._analyze_depth(depth_df))
            
            # Cross-data analysis
            if not quotes_df.empty and not trades_df.empty:
                metrics.update(self._analyze_price_impact(quotes_df, trades_df))
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error calculating metrics: {e}")
            return {}
    
    def _analyze_quotes(self, quotes_df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze quote data for spread and volatility metrics"""
        metrics = {}
        
        try:
            # Filter valid quotes
            valid_quotes = quotes_df.dropna(subset=['best_bid', 'best_ask'])
            
            if valid_quotes.empty:
                return {'quote_analysis': 'No valid quotes found'}
            
            # Calculate spreads
            valid_quotes = valid_quotes.copy()
            valid_quotes['spread'] = valid_quotes['best_ask'] - valid_quotes['best_bid']
            valid_quotes['mid_price'] = (valid_quotes['best_bid'] + valid_quotes['best_ask']) / 2
            valid_quotes['spread_bps'] = (valid_quotes['spread'] / valid_quotes['mid_price']) * 10000
            
            # Spread statistics
            metrics['spread_analysis'] = {
                'avg_spread': float(valid_quotes['spread'].mean()),
                'median_spread': float(valid_quotes['spread'].median()),
                'min_spread': float(valid_quotes['spread'].min()),
                'max_spread': float(valid_quotes['spread'].max()),
                'avg_spread_bps': float(valid_quotes['spread_bps'].mean()),
                'spread_volatility': float(valid_quotes['spread'].std())
            }
            
            # Mid-price analysis
            if len(valid_quotes) > 1:
                valid_quotes['mid_price_change'] = valid_quotes['mid_price'].diff()
                valid_quotes['mid_price_return'] = valid_quotes['mid_price'].pct_change()
                
                metrics['price_analysis'] = {
                    'price_volatility': float(valid_quotes['mid_price_return'].std() * np.sqrt(252 * 24 * 60)),  # Annualized
                    'avg_price_change': float(valid_quotes['mid_price_change'].mean()),
                    'max_price_move': float(abs(valid_quotes['mid_price_change']).max()),
                    'price_range': float(valid_quotes['mid_price'].max() - valid_quotes['mid_price'].min())
                }
            
            # Quote frequency
            if len(valid_quotes) > 1:
                time_diff = (valid_quotes['received_timestamp'].iloc[-1] - 
                           valid_quotes['received_timestamp'].iloc[0]).total_seconds()
                metrics['quote_frequency'] = {
                    'quotes_per_second': len(valid_quotes) / time_diff if time_diff > 0 else 0,
                    'avg_time_between_quotes': time_diff / len(valid_quotes) if len(valid_quotes) > 0 else 0
                }
            
        except Exception as e:
            logger.error(f"Error analyzing quotes: {e}")
            metrics['quote_analysis_error'] = str(e)
        
        return metrics
    
    def _analyze_trades(self, trades_df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze trade data for execution patterns"""
        metrics = {}
        
        try:
            # Basic trade statistics
            metrics['trade_analysis'] = {
                'total_trades': len(trades_df),
                'total_volume': int(trades_df['volume'].sum()),
                'avg_trade_size': float(trades_df['volume'].mean()),
                'median_trade_size': float(trades_df['volume'].median()),
                'max_trade_size': int(trades_df['volume'].max()),
                'min_trade_size': int(trades_df['volume'].min())
            }
            
            # Price analysis
            if not trades_df.empty:
                metrics['trade_price_analysis'] = {
                    'avg_price': float(trades_df['price'].mean()),
                    'price_range': float(trades_df['price'].max() - trades_df['price'].min()),
                    'weighted_avg_price': float((trades_df['price'] * trades_df['volume']).sum() / trades_df['volume'].sum())
                }
            
            # Buy/sell analysis
            if 'is_buy' in trades_df.columns:
                buy_trades = trades_df[trades_df['is_buy'] == True]
                sell_trades = trades_df[trades_df['is_buy'] == False]
                
                metrics['order_flow'] = {
                    'buy_trades': len(buy_trades),
                    'sell_trades': len(sell_trades),
                    'buy_volume': int(buy_trades['volume'].sum()) if not buy_trades.empty else 0,
                    'sell_volume': int(sell_trades['volume'].sum()) if not sell_trades.empty else 0,
                    'buy_ratio': len(buy_trades) / len(trades_df) if len(trades_df) > 0 else 0,
                    'volume_imbalance': (int(buy_trades['volume'].sum()) - int(sell_trades['volume'].sum())) if not buy_trades.empty and not sell_trades.empty else 0
                }
            
            # Volume distribution
            volume_dist = trades_df['volume'].value_counts().sort_index()
            metrics['volume_distribution'] = {
                'unique_sizes': len(volume_dist),
                'most_common_size': int(volume_dist.index[0]) if not volume_dist.empty else 0,
                'most_common_size_count': int(volume_dist.iloc[0]) if not volume_dist.empty else 0
            }
            
            # Time analysis
            if len(trades_df) > 1:
                trades_df_sorted = trades_df.sort_values('received_timestamp')
                time_diff = (trades_df_sorted['received_timestamp'].iloc[-1] - 
                           trades_df_sorted['received_timestamp'].iloc[0]).total_seconds()
                
                metrics['trade_timing'] = {
                    'trades_per_second': len(trades_df) / time_diff if time_diff > 0 else 0,
                    'avg_time_between_trades': time_diff / len(trades_df) if len(trades_df) > 0 else 0
                }
            
        except Exception as e:
            logger.error(f"Error analyzing trades: {e}")
            metrics['trade_analysis_error'] = str(e)
        
        return metrics
    
    def _analyze_depth(self, depth_df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze market depth for liquidity metrics"""
        metrics = {}
        
        try:
            # Basic depth statistics
            metrics['depth_analysis'] = {
                'total_depth_updates': len(depth_df),
                'unique_price_levels': depth_df['price'].nunique(),
                'avg_depth_volume': float(depth_df['volume'].mean()),
                'max_depth_volume': int(depth_df['volume'].max()),
                'total_depth_volume': int(depth_df['volume'].sum())
            }
            
            # Side analysis
            if 'side' in depth_df.columns:
                bid_depth = depth_df[depth_df['side'] == 'bid']
                ask_depth = depth_df[depth_df['side'] == 'ask']
                
                metrics['liquidity_analysis'] = {
                    'bid_updates': len(bid_depth),
                    'ask_updates': len(ask_depth),
                    'avg_bid_volume': float(bid_depth['volume'].mean()) if not bid_depth.empty else 0,
                    'avg_ask_volume': float(ask_depth['volume'].mean()) if not ask_depth.empty else 0,
                    'total_bid_volume': int(bid_depth['volume'].sum()) if not bid_depth.empty else 0,
                    'total_ask_volume': int(ask_depth['volume'].sum()) if not ask_depth.empty else 0
                }
            
            # Depth type analysis
            if 'depth_type' in depth_df.columns:
                type_dist = depth_df['depth_type'].value_counts()
                metrics['depth_types'] = {
                    'type_3_count': int(type_dist.get(3, 0)),  # Ask updates
                    'type_4_count': int(type_dist.get(4, 0)),  # Bid updates  
                    'type_5_count': int(type_dist.get(5, 0))   # Trade updates
                }
            
        except Exception as e:
            logger.error(f"Error analyzing depth: {e}")
            metrics['depth_analysis_error'] = str(e)
        
        return metrics
    
    def _analyze_price_impact(self, quotes_df: pd.DataFrame, trades_df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze price impact of trades"""
        metrics = {}
        
        try:
            # Merge quotes and trades by timestamp for impact analysis
            quotes_clean = quotes_df.dropna(subset=['best_bid', 'best_ask']).copy()
            quotes_clean['mid_price'] = (quotes_clean['best_bid'] + quotes_clean['best_ask']) / 2
            
            # Simple impact analysis - price change around trades
            trade_impacts = []
            
            for _, trade in trades_df.iterrows():
                trade_time = trade['received_timestamp']
                trade_price = trade['price']
                
                # Find closest quotes before and after trade
                before_quotes = quotes_clean[quotes_clean['received_timestamp'] <= trade_time]
                after_quotes = quotes_clean[quotes_clean['received_timestamp'] > trade_time]
                
                if not before_quotes.empty and not after_quotes.empty:
                    before_price = before_quotes.iloc[-1]['mid_price']
                    after_price = after_quotes.iloc[0]['mid_price']
                    
                    impact = after_price - before_price
                    relative_impact = impact / before_price if before_price > 0 else 0
                    
                    trade_impacts.append({
                        'trade_price': trade_price,
                        'before_mid': before_price,
                        'after_mid': after_price,
                        'impact': impact,
                        'relative_impact': relative_impact,
                        'trade_volume': trade['volume']
                    })
            
            if trade_impacts:
                impacts_df = pd.DataFrame(trade_impacts)
                
                metrics['price_impact'] = {
                    'avg_impact': float(impacts_df['impact'].mean()),
                    'avg_relative_impact_bps': float(impacts_df['relative_impact'].mean() * 10000),
                    'positive_impact_ratio': float((impacts_df['impact'] > 0).mean()),
                    'max_impact': float(impacts_df['impact'].max()),
                    'min_impact': float(impacts_df['impact'].min()),
                    'impact_volatility': float(impacts_df['impact'].std())
                }
            
        except Exception as e:
            logger.error(f"Error analyzing price impact: {e}")
            metrics['price_impact_error'] = str(e)
        
        return metrics
    
    def create_session_report(self, session_id: str, save_path: Optional[str] = None) -> Dict[str, Any]:
        """Create comprehensive analysis report for a session"""
        try:
            logger.info(f"Creating analysis report for session: {session_id}")
            
            # Load data
            data = self.load_session_data(session_id)
            if not data:
                return {'error': 'No data found for session'}
            
            # Calculate metrics
            metrics = self.calculate_microstructure_metrics(data)
            
            # Session metadata
            sessions_df = self.get_sessions()
            session_info = sessions_df[sessions_df['session_id'] == session_id]
            
            if not session_info.empty:
                session_meta = session_info.iloc[0].to_dict()
            else:
                session_meta = {}
            
            # Compile report
            report = {
                'session_id': session_id,
                'generated_at': datetime.now().isoformat(),
                'session_metadata': session_meta,
                'data_summary': {
                    'quotes_count': len(data.get('quotes', [])),
                    'trades_count': len(data.get('trades', [])),
                    'depth_count': len(data.get('depth', []))
                },
                'microstructure_metrics': metrics
            }
            
            # Save report if path provided
            if save_path:
                import json
                with open(save_path, 'w') as f:
                    json.dump(report, f, indent=2, default=str)
                logger.info(f"Report saved to: {save_path}")
            
            return report
            
        except Exception as e:
            logger.error(f"Error creating session report: {e}")
            return {'error': str(e)}
    
    def plot_session_overview(self, session_id: str, save_path: Optional[str] = None):
        """Create visualization plots for session data"""
        try:
            data = self.load_session_data(session_id)
            if not data:
                logger.error("No data to plot")
                return
            
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            fig.suptitle(f'Market Data Overview - Session {session_id}', fontsize=16)
            
            # Plot 1: Price and spread over time
            if not data['quotes'].empty:
                quotes_df = data['quotes'].dropna(subset=['best_bid', 'best_ask'])
                if not quotes_df.empty:
                    quotes_df['mid_price'] = (quotes_df['best_bid'] + quotes_df['best_ask']) / 2
                    quotes_df['spread'] = quotes_df['best_ask'] - quotes_df['best_bid']
                    
                    ax1 = axes[0, 0]
                    ax1.plot(quotes_df['received_timestamp'], quotes_df['mid_price'], label='Mid Price', alpha=0.7)
                    ax1.set_title('Mid Price Over Time')
                    ax1.set_ylabel('Price')
                    ax1.tick_params(axis='x', rotation=45)
                    
                    ax2 = ax1.twinx()
                    ax2.plot(quotes_df['received_timestamp'], quotes_df['spread'], color='red', alpha=0.5, label='Spread')
                    ax2.set_ylabel('Spread', color='red')
            
            # Plot 2: Trade volume and frequency
            if not data['trades'].empty:
                trades_df = data['trades']
                
                ax3 = axes[0, 1]
                ax3.scatter(trades_df['received_timestamp'], trades_df['price'], 
                           s=trades_df['volume']*2, alpha=0.6, c=trades_df['volume'], cmap='viridis')
                ax3.set_title('Trades (Size = Volume, Color = Volume)')
                ax3.set_ylabel('Price')
                ax3.tick_params(axis='x', rotation=45)
            
            # Plot 3: Volume distribution
            if not data['trades'].empty:
                ax4 = axes[1, 0]
                trades_df['volume'].hist(bins=20, ax=ax4, alpha=0.7)
                ax4.set_title('Trade Volume Distribution')
                ax4.set_xlabel('Volume')
                ax4.set_ylabel('Frequency')
            
            # Plot 4: Market depth visualization  
            if not data['depth'].empty:
                depth_df = data['depth']
                
                ax5 = axes[1, 1]
                if 'side' in depth_df.columns:
                    bid_data = depth_df[depth_df['side'] == 'bid']
                    ask_data = depth_df[depth_df['side'] == 'ask']
                    
                    if not bid_data.empty:
                        ax5.scatter(bid_data['price'], bid_data['volume'], 
                                  c='green', alpha=0.5, label='Bids', s=20)
                    if not ask_data.empty:
                        ax5.scatter(ask_data['price'], ask_data['volume'], 
                                  c='red', alpha=0.5, label='Asks', s=20)
                    
                    ax5.set_title('Market Depth')
                    ax5.set_xlabel('Price')
                    ax5.set_ylabel('Volume')
                    ax5.legend()
            
            plt.tight_layout()
            
            if save_path:
                plt.savefig(save_path, dpi=300, bbox_inches='tight')
                logger.info(f"Plot saved to: {save_path}")
            
            plt.show()
            
        except Exception as e:
            logger.error(f"Error creating plots: {e}")
    
    def export_session_data(self, session_id: str, export_format: str = 'csv', 
                          output_dir: str = 'exports') -> Dict[str, str]:
        """Export session data to files"""
        try:
            import os
            os.makedirs(output_dir, exist_ok=True)
            
            data = self.load_session_data(session_id)
            exported_files = {}
            
            for data_type, df in data.items():
                if df.empty:
                    continue
                
                filename = f"{session_id}_{data_type}.{export_format}"
                filepath = os.path.join(output_dir, filename)
                
                if export_format == 'csv':
                    df.to_csv(filepath, index=False)
                elif export_format == 'parquet':
                    df.to_parquet(filepath, index=False)
                elif export_format == 'json':
                    df.to_json(filepath, orient='records', date_format='iso')
                
                exported_files[data_type] = filepath
                logger.info(f"Exported {data_type} to {filepath}")
            
            return exported_files
            
        except Exception as e:
            logger.error(f"Error exporting data: {e}")
            return {}
    
    def close(self):
        """Close database connection"""
        if self.database:
            self.database.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

# Convenience functions
def quick_analysis(session_id: str, db_path: str = "market_data.db") -> Dict[str, Any]:
    """Quick analysis of a session"""
    db_config = DatabaseConfig(db_type='sqlite', db_path=db_path)
    
    with MarketDataAnalyzer(db_config) as analyzer:
        return analyzer.create_session_report(session_id)

def list_sessions(db_path: str = "market_data.db") -> pd.DataFrame:
    """List all available sessions"""
    db_config = DatabaseConfig(db_type='sqlite', db_path=db_path)
    
    with MarketDataAnalyzer(db_config) as analyzer:
        return analyzer.get_sessions()
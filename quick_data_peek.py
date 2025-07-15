# -*- coding: utf-8 -*-
"""
Created on Fri Jul 11 12:11:54 2025

@author: edfit
"""

import pandas as pd
import sqlite3
import os

def quick_data_peek():
    """Quick exploration of the ABS database."""
    
    # Find the most recent database file
    db_files = [f for f in os.listdir() if f.startswith('abs_performance_data_') and f.endswith('.db')]
    
    if not db_files:
        print("No ABS database files found in current directory.")
        print("Available files:")
        for file in os.listdir():
            if file.endswith('.db'):
                print(f"  - {file}")
        return
    
    # Use the most recent database file
    db_file = sorted(db_files)[-1]
    print(f"Using database: {db_file}")
    print("=" * 50)
    
    # Connect to database
    conn = sqlite3.connect(db_file)
    
    try:
        # See all tables
        print("📊 AVAILABLE TABLES:")
        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
        table_list = tables['name'].tolist()
        print(table_list)
        print()
        
        # Quick look at each table
        for table_name in table_list:
            print(f"🔍 TABLE: {table_name.upper()}")
            
            # Get row count
            count_query = f"SELECT COUNT(*) as count FROM {table_name}"
            row_count = pd.read_sql(count_query, conn)['count'].iloc[0]
            
            # Get sample data
            sample_query = f"SELECT * FROM {table_name} LIMIT 3"
            sample_data = pd.read_sql(sample_query, conn)
            
            print(f"   Rows: {row_count}")
            print(f"   Columns: {len(sample_data.columns)}")
            print(f"   Fields: {', '.join(sample_data.columns)}")
            
            if len(sample_data) > 0:
                print("   Sample data:")
                for idx, row in sample_data.iterrows():
                    print(f"     Row {idx + 1}: {dict(row)}")
            print()
        
        # Detailed look at performance metrics
        print("🎯 PERFORMANCE METRICS DETAILED VIEW:")
        perf_query = "SELECT * FROM performance_metrics LIMIT 5"
        perf_data = pd.read_sql(perf_query, conn)
        
        print("Columns in performance_metrics:")
        for i, col in enumerate(perf_data.columns):
            print(f"  {i+1}. {col}")
        
        print("\nFirst 5 records:")
        print(perf_data.to_string(index=False))
        print()
        
        # Check deal distribution
        print("🏢 DEAL DISTRIBUTION:")
        deal_query = """
        SELECT issuer_name, COUNT(*) as deal_count 
        FROM deal_master 
        GROUP BY issuer_name
        """
        deal_dist = pd.read_sql(deal_query, conn)
        print(deal_dist.to_string(index=False))
        print()
        
        # Check historical data types
        print("📈 HISTORICAL DATA TYPES:")
        hist_query = """
        SELECT metric_type, COUNT(*) as record_count 
        FROM historical_performance 
        GROUP BY metric_type
        ORDER BY record_count DESC
        """
        hist_types = pd.read_sql(hist_query, conn)
        print(hist_types.to_string(index=False))
        
    except Exception as e:
        print(f"Error exploring database: {e}")
    
    finally:
        conn.close()
    
    print("\n✅ Quick peek completed!")
    print(f"Database location: {os.path.abspath(db_file)}")

if __name__ == "__main__":
    print("ABS DATA QUICK PEEK")
    print("=" * 30)
    quick_data_peek()
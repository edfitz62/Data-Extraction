# -*- coding: utf-8 -*-
"""
Created on Sat Jul 12 14:29:20 2025

@author: edfit
"""


import pandas as pd
import numpy as np
import pyodbc
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class SimpleAccessDB:
    """
    Simple Access database manager for ABS data
    """
    
    def __init__(self, db_path):
        self.db_path = db_path
        self.connection_string = None
        self._setup_connection()
    
    def _setup_connection(self):
        """Setup connection to Access database"""
        if not os.path.exists(self.db_path):
            print(f"❌ Database file not found: {self.db_path}")
            return False
        
        # Try different Access drivers
        drivers = [
            'Microsoft Access Driver (*.mdb, *.accdb)',
            'Microsoft Office 16.0 Access Database Engine OLE DB Provider',
            'Microsoft Access Driver (*.mdb)',
        ]
        
        for driver in drivers:
            try:
                conn_str = f'DRIVER={{{driver}}};DBQ={self.db_path};'
                conn = pyodbc.connect(conn_str)
                conn.close()
                self.connection_string = conn_str
                print(f"✅ Connected to Access database using: {driver}")
                return True
            except Exception as e:
                print(f"⚠️  Driver '{driver}' failed: {str(e)[:100]}...")
                continue
        
        print("❌ No Access driver found. Please install Microsoft Access Database Engine.")
        print("Download from: https://www.microsoft.com/en-us/download/details.aspx?id=54920")
        return False
    
    def test_connection(self):
        """Test database connection and show existing tables"""
        if not self.connection_string:
            return False
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Get existing tables
            tables = []
            for row in cursor.tables(tableType='TABLE'):
                tables.append(row.table_name)
            
            print(f"📋 Existing tables in database: {tables}")
            
            # If tables exist, show sample data
            if tables:
                for table in tables[:3]:  # Show first 3 tables
                    try:
                        sample = pd.read_sql(f"SELECT TOP 3 * FROM [{table}]", conn)
                        print(f"\n📊 Sample data from {table}:")
                        print(sample.to_string())
                    except Exception as e:
                        print(f"⚠️  Couldn't read {table}: {e}")
            
            conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            return False
    
    def create_abs_tables(self):
        """Create ABS-specific tables if they don't exist"""
        if not self.connection_string:
            return False
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Create main deals table
            cursor.execute('''
                CREATE TABLE ABS_Deals (
                    ID AUTOINCREMENT PRIMARY KEY,
                    DealName TEXT(255),
                    IssueDate DATETIME,
                    RatingAgency TEXT(50),
                    Sector TEXT(100),
                    DealSize CURRENCY,
                    ClassAAdvanceRate DOUBLE,
                    InitialOC DOUBLE,
                    ExpectedCNLLow DOUBLE,
                    ExpectedCNLHigh DOUBLE,
                    ReserveAccount DOUBLE,
                    AvgSeasoning INTEGER,
                    TopObligorConc DOUBLE,
                    CreatedDate DATETIME,
                    SourceFile TEXT(255)
                )
            ''')
            print("✅ Created ABS_Deals table")
            
            # Create stress test results table
            cursor.execute('''
                CREATE TABLE StressTestResults (
                    ID AUTOINCREMENT PRIMARY KEY,
                    DealID INTEGER,
                    TestDate DATETIME,
                    Scenario TEXT(50),
                    StressedLoss DOUBLE,
                    AdequacyRatio DOUBLE,
                    Status TEXT(20)
                )
            ''')
            print("✅ Created StressTestResults table")
            
            # Create Monte Carlo results table
            cursor.execute('''
                CREATE TABLE MonteCarloResults (
                    ID AUTOINCREMENT PRIMARY KEY,
                    DealID INTEGER,
                    TestDate DATETIME,
                    Iterations INTEGER,
                    BreachProbability DOUBLE,
                    AvgShortfall DOUBLE,
                    MaxShortfall DOUBLE
                )
            ''')
            print("✅ Created MonteCarloResults table")
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            print(f"ℹ️  Tables may already exist or error occurred: {e}")
            return True  # Continue anyway
    
    def insert_deal(self, deal_data):
        """Insert new deal into database"""
        if not self.connection_string:
            return None
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Insert deal
            insert_sql = '''
                INSERT INTO ABS_Deals 
                (DealName, IssueDate, RatingAgency, Sector, DealSize, 
                 ClassAAdvanceRate, InitialOC, ExpectedCNLLow, ExpectedCNLHigh,
                 ReserveAccount, AvgSeasoning, TopObligorConc, CreatedDate, SourceFile)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            values = (
                deal_data.get('deal_name'),
                deal_data.get('issue_date'),
                deal_data.get('rating_agency'),
                deal_data.get('sector'),
                float(deal_data.get('deal_size', 0)),
                float(deal_data.get('class_a_advance_rate', 0)),
                float(deal_data.get('initial_oc', 0)),
                float(deal_data.get('expected_cnl_low', 0)),
                float(deal_data.get('expected_cnl_high', 0)),
                float(deal_data.get('reserve_account', 0)),
                int(deal_data.get('avg_seasoning', 0)),
                float(deal_data.get('top_obligor_conc', 0)),
                datetime.now(),
                deal_data.get('source_file', 'Manual Entry')
            )
            
            cursor.execute(insert_sql, values)
            
            # Get the ID of inserted record
            cursor.execute("SELECT @@IDENTITY")
            deal_id = cursor.fetchone()[0]
            
            conn.commit()
            conn.close()
            
            print(f"✅ Inserted deal: {deal_data.get('deal_name')} (ID: {deal_id})")
            return deal_id
            
        except Exception as e:
            print(f"❌ Error inserting deal: {e}")
            return None
    
    def get_all_deals(self):
        """Get all deals from database"""
        if not self.connection_string:
            return pd.DataFrame()
        
        try:
            conn = pyodbc.connect(self.connection_string)
            df = pd.read_sql("SELECT * FROM ABS_Deals ORDER BY CreatedDate DESC", conn)
            conn.close()
            return df
        except Exception as e:
            print(f"❌ Error retrieving deals: {e}")
            return pd.DataFrame()
    
    def save_stress_test(self, deal_id, scenario, results_df):
        """Save stress test results"""
        if not self.connection_string:
            return
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            for _, row in results_df.iterrows():
                cursor.execute('''
                    INSERT INTO StressTestResults 
                    (DealID, TestDate, Scenario, StressedLoss, AdequacyRatio, Status)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    deal_id,
                    datetime.now(),
                    scenario,
                    row.get('stressed_loss', 0),
                    row.get('adequacy_ratio', 0),
                    row.get('status', 'Unknown')
                ))
            
            conn.commit()
            conn.close()
            print(f"✅ Saved stress test results for deal ID: {deal_id}")
            
        except Exception as e:
            print(f"❌ Error saving stress test: {e}")
    
    def save_monte_carlo(self, deal_id, iterations, results):
        """Save Monte Carlo results"""
        if not self.connection_string:
            return
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO MonteCarloResults 
                (DealID, TestDate, Iterations, BreachProbability, AvgShortfall, MaxShortfall)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                deal_id,
                datetime.now(),
                iterations,
                results.get('breach_probability', 0),
                results.get('avg_shortfall', 0),
                results.get('max_shortfall', 0)
            ))
            
            conn.commit()
            conn.close()
            print(f"✅ Saved Monte Carlo results for deal ID: {deal_id}")
            
        except Exception as e:
            print(f"❌ Error saving Monte Carlo: {e}")


class SimpleABSPlatform:
    """
    Simplified ABS Platform with Access database integration
    """
    
    def __init__(self, access_db_path):
        print(f"🔗 Connecting to Access database: {access_db_path}")
        self.db = SimpleAccessDB(access_db_path)
        
        if self.db.connection_string:
            self.db.test_connection()
            self.db.create_abs_tables()
            self.deal_database = self.db.get_all_deals()
            print(f"📊 Loaded {len(self.deal_database)} existing deals")
        else:
            self.deal_database = pd.DataFrame()
            print("⚠️  Running without database connection")
    
    def add_deal(self, deal_data):
        """Add new deal"""
        # Add to database
        deal_id = self.db.insert_deal(deal_data)
        
        if deal_id:
            # Refresh local data
            self.deal_database = self.db.get_all_deals()
            
            # Check for alerts
            alerts = self._check_alerts(deal_data)
            for alert in alerts:
                print(f"🚨 ALERT: {alert}")
        
        return deal_id
    
    def _check_alerts(self, deal_data):
        """Simple deviation checking"""
        alerts = []
        
        if len(self.deal_database) <= 1:
            return ["First deal in database - no benchmarks available"]
        
        # Find similar deals in same sector
        sector_deals = self.deal_database[
            self.deal_database['Sector'] == deal_data['sector']
        ]
        
        if len(sector_deals) > 1:
            avg_advance = sector_deals['ClassAAdvanceRate'].mean()
            avg_oc = sector_deals['InitialOC'].mean()
            
            if abs(deal_data['class_a_advance_rate'] - avg_advance) > 5:
                alerts.append(f"Advance rate ({deal_data['class_a_advance_rate']:.1f}%) differs from sector average ({avg_advance:.1f}%)")
            
            if abs(deal_data['initial_oc'] - avg_oc) > 2:
                alerts.append(f"Initial OC ({deal_data['initial_oc']:.1f}%) differs from sector average ({avg_oc:.1f}%)")
        
        return alerts
    
    def run_stress_test(self, scenario='moderate'):
        """Run stress test on all deals"""
        if len(self.deal_database) == 0:
            print("❌ No deals in database")
            return None
        
        multipliers = {
            'mild': 1.2,
            'moderate': 1.5,
            'severe': 2.0
        }
        
        loss_mult = multipliers.get(scenario, 1.5)
        
        # Calculate stressed metrics
        results = []
        for _, deal in self.deal_database.iterrows():
            stressed_loss = deal['ExpectedCNLLow'] * loss_mult
            total_enhancement = deal['InitialOC'] + deal['ReserveAccount']
            adequacy_ratio = total_enhancement / stressed_loss
            
            if adequacy_ratio >= 1.5:
                status = 'Strong'
            elif adequacy_ratio >= 1.2:
                status = 'Adequate'
            elif adequacy_ratio >= 1.0:
                status = 'Weak'
            else:
                status = 'Critical'
            
            results.append({
                'deal_id': deal['ID'],
                'deal_name': deal['DealName'],
                'stressed_loss': stressed_loss,
                'adequacy_ratio': adequacy_ratio,
                'status': status
            })
            
            # Save to database
            self.db.save_stress_test(deal['ID'], scenario, pd.DataFrame([{
                'stressed_loss': stressed_loss,
                'adequacy_ratio': adequacy_ratio,
                'status': status
            }]))
        
        results_df = pd.DataFrame(results)
        
        # Show summary
        critical = len(results_df[results_df['status'] == 'Critical'])
        weak = len(results_df[results_df['status'] == 'Weak'])
        
        print(f"\n📊 STRESS TEST RESULTS - {scenario.upper()}")
        print("=" * 40)
        print(f"💀 Critical Deals: {critical}")
        print(f"⚠️  Weak Deals: {weak}")
        print(f"✅ Total Deals Tested: {len(results_df)}")
        
        if critical > 0 or weak > 0:
            print("\n🚨 DEALS AT RISK:")
            at_risk = results_df[results_df['status'].isin(['Critical', 'Weak'])]
            print(at_risk[['deal_name', 'adequacy_ratio', 'status']].to_string(index=False))
        
        return results_df
    
    def monte_carlo_simulation(self, deal_name, iterations=10000):
        """Run Monte Carlo simulation"""
        deal = self.deal_database[self.deal_database['DealName'] == deal_name]
        
        if len(deal) == 0:
            print(f"❌ Deal '{deal_name}' not found")
            return None
        
        deal = deal.iloc[0]
        
        # Simple Monte Carlo simulation
        np.random.seed(42)
        loss_factors = np.random.normal(1, 0.3, iterations)  # 30% volatility
        simulated_losses = deal['ExpectedCNLLow'] * np.maximum(0.1, loss_factors)
        enhancement = deal['InitialOC'] + deal['ReserveAccount']
        
        breaches = simulated_losses > enhancement
        shortfalls = np.maximum(0, simulated_losses - enhancement)
        
        results = {
            'breach_probability': np.mean(breaches) * 100,
            'avg_shortfall': np.mean(shortfalls),
            'max_shortfall': np.max(shortfalls)
        }
        
        # Save to database
        self.db.save_monte_carlo(deal['ID'], iterations, results)
        
        print(f"\n🎲 MONTE CARLO - {deal_name}")
        print("=" * 40)
        print(f"🔢 Simulations: {iterations:,}")
        print(f"💥 Breach Probability: {results['breach_probability']:.1f}%")
        print(f"📉 Avg Shortfall: {results['avg_shortfall']:.2f}%")
        print(f"📊 Max Shortfall: {results['max_shortfall']:.2f}%")
        
        return results
    
    def show_database_summary(self):
        """Show summary of database contents"""
        if len(self.deal_database) == 0:
            print("📭 Database is empty")
            return
        
        print(f"\n📊 DATABASE SUMMARY")
        print("=" * 30)
        print(f"Total Deals: {len(self.deal_database)}")
        print(f"Total Volume: ${self.deal_database['DealSize'].sum():.0f}M")
        print(f"Avg Advance Rate: {self.deal_database['ClassAAdvanceRate'].mean():.1f}%")
        print(f"Avg Initial OC: {self.deal_database['InitialOC'].mean():.1f}%")
        
        print(f"\nSECTOR BREAKDOWN:")
        sector_summary = self.deal_database.groupby('Sector').agg({
            'DealSize': ['count', 'sum'],
            'ClassAAdvanceRate': 'mean'
        }).round(1)
        print(sector_summary)
        
        print(f"\nRECENT DEALS:")
        recent = self.deal_database.head(3)[['DealName', 'Sector', 'DealSize', 'CreatedDate']]
        print(recent.to_string(index=False))


def quick_start_access():
    """Quick start function for Access integration"""
    db_path = r"C:\Users\edfit\OneDrive\Documents\ABS_Performance_Data.db.accdb"
    
    print("🚀 Starting ABS Platform with Access Database")
    print("=" * 50)
    
    platform = SimpleABSPlatform(db_path)
    
    if platform.db.connection_string:
        print(f"""
✅ ACCESS INTEGRATION SUCCESSFUL!
================================

Your database: {db_path}
Existing deals: {len(platform.deal_database)}

QUICK START COMMANDS:
--------------------

# Add a sample deal
platform.add_deal({
    'deal_name': 'Sample Deal 2025-1',
    'issue_date': '2025-01-15',
    'rating_agency': 'KBRA',
    'sector': 'Equipment ABS',
    'deal_size': 250.0,
    'class_a_advance_rate': 75.0,
    'initial_oc': 12.0,
    'expected_cnl_low': 2.8,
    'expected_cnl_high': 3.2,
    'reserve_account': 1.5,
    'avg_seasoning': 20,
    'top_obligor_conc': 1.2
})

# Run analyses
platform.run_stress_test('severe')
platform.monte_carlo_simulation('Sample Deal 2025-1')
platform.show_database_summary()

Ready to use! 🎉
        """)
    else:
        print("""
❌ ACCESS CONNECTION FAILED
===========================

Possible solutions:
1. Install Microsoft Access Database Engine:
   https://www.microsoft.com/en-us/download/details.aspx?id=54920

2. Check if database file is accessible:
   - Not open in Access
   - Has write permissions
   - File path is correct

3. Try alternative: Use SQLite instead
        """)
    
    return platform

if __name__ == "__main__":
    platform = quick_start_access()
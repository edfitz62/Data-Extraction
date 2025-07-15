# -*- coding: utf-8 -*-
"""
Created on Sat Jul 12 14:23:48 2025

@author: edfit
"""

"""
Access Database Integration for ABS Analysis Platform
Connects to existing Access database for data persistence
"""

import pandas as pd
import numpy as np
import pyodbc
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class AccessDBManager:
    """
    Manages connection and operations with Access database
    """
    
    def __init__(self, db_path):
        self.db_path = db_path
        self.connection_string = self._build_connection_string()
        self.table_schemas = self._define_table_schemas()
        self._ensure_tables_exist()
    
    def _build_connection_string(self):
        """Build connection string for Access database"""
        # Check if file exists
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Access database not found: {self.db_path}")
        
        # Try different Access drivers
        drivers = [
            'Microsoft Access Driver (*.mdb, *.accdb)',
            'Microsoft Access Driver (*.mdb)',
            'Microsoft Office 16.0 Access Database Engine OLE DB Provider'
        ]
        
        for driver in drivers:
            try:
                conn_str = f'DRIVER={{{driver}}};DBQ={self.db_path};'
                # Test connection
                conn = pyodbc.connect(conn_str)
                conn.close()
                print(f"✅ Connected using driver: {driver}")
                return conn_str
            except:
                continue
        
        raise Exception("❌ No suitable Access driver found. Install Microsoft Access Database Engine.")
    
    def _define_table_schemas(self):
        """Define table schemas for ABS data"""
        return {
            'abs_deals': {
                'id': 'AUTOINCREMENT PRIMARY KEY',
                'deal_name': 'TEXT(255) NOT NULL',
                'issue_date': 'DATETIME',
                'rating_agency': 'TEXT(50)',
                'sector': 'TEXT(100)',
                'deal_size': 'CURRENCY',
                'class_a_advance_rate': 'DOUBLE',
                'initial_oc': 'DOUBLE',
                'expected_cnl_low': 'DOUBLE',
                'expected_cnl_high': 'DOUBLE',
                'reserve_account': 'DOUBLE',
                'avg_seasoning': 'INTEGER',
                'top_obligor_conc': 'DOUBLE',
                'created_date': 'DATETIME DEFAULT NOW()',
                'source_file': 'TEXT(255)',
                'extraction_method': 'TEXT(50)'
            },
            'stress_test_results': {
                'id': 'AUTOINCREMENT PRIMARY KEY',
                'deal_id': 'INTEGER',
                'test_date': 'DATETIME DEFAULT NOW()',
                'scenario': 'TEXT(50)',
                'loss_multiplier': 'DOUBLE',
                'stressed_loss': 'DOUBLE',
                'adequacy_ratio': 'DOUBLE',
                'status': 'TEXT(20)'
            },
            'monte_carlo_results': {
                'id': 'AUTOINCREMENT PRIMARY KEY',
                'deal_id': 'INTEGER',
                'test_date': 'DATETIME DEFAULT NOW()',
                'iterations': 'INTEGER',
                'breach_probability': 'DOUBLE',
                'avg_shortfall': 'DOUBLE',
                'max_shortfall': 'DOUBLE',
                'percentile_95_loss': 'DOUBLE'
            },
            'integration_assessments': {
                'id': 'AUTOINCREMENT PRIMARY KEY',
                'deal_name': 'TEXT(255)',
                'assessment_date': 'DATETIME DEFAULT NOW()',
                'integration_type': 'TEXT(100)',
                'duration_days': 'INTEGER',
                'staff_retention': 'DOUBLE',
                'systems_compatibility': 'TEXT(50)',
                'overall_risk_score': 'DOUBLE',
                'risk_level': 'TEXT(20)',
                'critical_factors': 'MEMO'
            }
        }
    
    def _ensure_tables_exist(self):
        """Create tables if they don't exist"""
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Get existing tables
            existing_tables = [row.table_name.lower() for row in cursor.tables(tableType='TABLE')]
            
            for table_name, schema in self.table_schemas.items():
                if table_name.lower() not in existing_tables:
                    self._create_table(cursor, table_name, schema)
                    print(f"✅ Created table: {table_name}")
                else:
                    print(f"📋 Table exists: {table_name}")
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"❌ Error ensuring tables exist: {e}")
    
    def _create_table(self, cursor, table_name, schema):
        """Create a table with given schema"""
        columns = []
        for col_name, col_type in schema.items():
            columns.append(f"{col_name} {col_type}")
        
        create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
        cursor.execute(create_sql)
    
    def insert_deal(self, deal_data):
        """Insert new deal into database"""
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Prepare data
            insert_data = {
                'deal_name': deal_data.get('deal_name'),
                'issue_date': pd.to_datetime(deal_data.get('issue_date')),
                'rating_agency': deal_data.get('rating_agency'),
                'sector': deal_data.get('sector'),
                'deal_size': float(deal_data.get('deal_size', 0)),
                'class_a_advance_rate': float(deal_data.get('class_a_advance_rate', 0)),
                'initial_oc': float(deal_data.get('initial_oc', 0)),
                'expected_cnl_low': float(deal_data.get('expected_cnl_low', 0)),
                'expected_cnl_high': float(deal_data.get('expected_cnl_high', 0)),
                'reserve_account': float(deal_data.get('reserve_account', 0)),
                'avg_seasoning': int(deal_data.get('avg_seasoning', 0)),
                'top_obligor_conc': float(deal_data.get('top_obligor_conc', 0)),
                'source_file': deal_data.get('source_file', 'Manual Entry'),
                'extraction_method': deal_data.get('extraction_method', 'Manual')
            }
            
            # Build INSERT query
            columns = list(insert_data.keys())
            placeholders = ', '.join(['?' for _ in columns])
            sql = f"INSERT INTO abs_deals ({', '.join(columns)}) VALUES ({placeholders})"
            
            # Execute
            cursor.execute(sql, list(insert_data.values()))
            deal_id = cursor.execute("SELECT @@IDENTITY").fetchone()[0]
            
            conn.commit()
            conn.close()
            
            print(f"✅ Inserted deal: {deal_data.get('deal_name')} (ID: {deal_id})")
            return deal_id
            
        except Exception as e:
            print(f"❌ Error inserting deal: {e}")
            return None
    
    def get_all_deals(self):
        """Retrieve all deals from database"""
        try:
            conn = pyodbc.connect(self.connection_string)
            df = pd.read_sql("SELECT * FROM abs_deals ORDER BY created_date DESC", conn)
            conn.close()
            return df
        except Exception as e:
            print(f"❌ Error retrieving deals: {e}")
            return pd.DataFrame()
    
    def insert_stress_test_result(self, deal_id, scenario, results):
        """Insert stress test results"""
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            for _, row in results.iterrows():
                insert_data = (
                    deal_id,
                    scenario,
                    row.get('loss_multiplier', 1.0),
                    row['stressed_loss'],
                    row['adequacy_ratio'],
                    row['status']
                )
                
                sql = """INSERT INTO stress_test_results 
                        (deal_id, scenario, loss_multiplier, stressed_loss, adequacy_ratio, status) 
                        VALUES (?, ?, ?, ?, ?, ?)"""
                
                cursor.execute(sql, insert_data)
            
            conn.commit()
            conn.close()
            print(f"✅ Inserted stress test results for deal ID: {deal_id}")
            
        except Exception as e:
            print(f"❌ Error inserting stress test results: {e}")
    
    def insert_monte_carlo_result(self, deal_id, iterations, results):
        """Insert Monte Carlo simulation results"""
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            insert_data = (
                deal_id,
                iterations,
                results['breach_probability'],
                results['avg_shortfall'],
                results['max_shortfall'],
                results['percentile_95_loss']
            )
            
            sql = """INSERT INTO monte_carlo_results 
                    (deal_id, iterations, breach_probability, avg_shortfall, max_shortfall, percentile_95_loss) 
                    VALUES (?, ?, ?, ?, ?, ?)"""
            
            cursor.execute(sql, insert_data)
            conn.commit()
            conn.close()
            
            print(f"✅ Inserted Monte Carlo results for deal ID: {deal_id}")
            
        except Exception as e:
            print(f"❌ Error inserting Monte Carlo results: {e}")
    
    def insert_integration_assessment(self, assessment_data):
        """Insert integration risk assessment"""
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            insert_data = (
                assessment_data['deal_name'],
                assessment_data['integration_type'],
                assessment_data.get('duration_days', 90),
                assessment_data.get('staff_retention', 85),
                assessment_data.get('systems_compatibility', 'unknown'),
                assessment_data['overall_risk'],
                assessment_data['risk_level'],
                ', '.join(assessment_data.get('critical_factors', []))
            )
            
            sql = """INSERT INTO integration_assessments 
                    (deal_name, integration_type, duration_days, staff_retention, 
                     systems_compatibility, overall_risk_score, risk_level, critical_factors) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
            
            cursor.execute(sql, insert_data)
            conn.commit()
            conn.close()
            
            print(f"✅ Inserted integration assessment for: {assessment_data['deal_name']}")
            
        except Exception as e:
            print(f"❌ Error inserting integration assessment: {e}")
    
    def get_deal_by_name(self, deal_name):
        """Get specific deal by name"""
        try:
            conn = pyodbc.connect(self.connection_string)
            df = pd.read_sql("SELECT * FROM abs_deals WHERE deal_name = ?", conn, params=[deal_name])
            conn.close()
            return df.iloc[0] if len(df) > 0 else None
        except Exception as e:
            print(f"❌ Error retrieving deal: {e}")
            return None
    
    def generate_database_report(self):
        """Generate comprehensive database report"""
        try:
            conn = pyodbc.connect(self.connection_string)
            
            # Get counts from each table
            deals_count = pd.read_sql("SELECT COUNT(*) as count FROM abs_deals", conn).iloc[0]['count']
            stress_count = pd.read_sql("SELECT COUNT(*) as count FROM stress_test_results", conn).iloc[0]['count']
            monte_count = pd.read_sql("SELECT COUNT(*) as count FROM monte_carlo_results", conn).iloc[0]['count']
            assessment_count = pd.read_sql("SELECT COUNT(*) as count FROM integration_assessments", conn).iloc[0]['count']
            
            # Get sector breakdown
            sector_breakdown = pd.read_sql("""
                SELECT sector, COUNT(*) as deal_count, SUM(deal_size) as total_volume 
                FROM abs_deals 
                GROUP BY sector
            """, conn)
            
            # Get recent activity
            recent_deals = pd.read_sql("""
                SELECT deal_name, sector, deal_size, created_date 
                FROM abs_deals 
                ORDER BY created_date DESC 
                LIMIT 5
            """, conn)
            
            conn.close()
            
            print(f"""
📊 ABS DATABASE REPORT
====================
📈 Total Deals: {deals_count}
🧪 Stress Tests: {stress_count}
🎲 Monte Carlo Runs: {monte_count}
⚠️  Risk Assessments: {assessment_count}

📋 SECTOR BREAKDOWN:
{sector_breakdown.to_string(index=False)}

🕒 RECENT DEALS:
{recent_deals.to_string(index=False)}
            """)
            
            return {
                'deals_count': deals_count,
                'stress_count': stress_count,
                'monte_count': monte_count,
                'assessment_count': assessment_count,
                'sector_breakdown': sector_breakdown,
                'recent_deals': recent_deals
            }
            
        except Exception as e:
            print(f"❌ Error generating report: {e}")
            return None
    
    def backup_to_excel(self, backup_path):
        """Backup database to Excel file"""
        try:
            conn = pyodbc.connect(self.connection_string)
            
            with pd.ExcelWriter(backup_path, engine='openpyxl') as writer:
                # Export each table
                for table_name in self.table_schemas.keys():
                    try:
                        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
                        df.to_excel(writer, sheet_name=table_name, index=False)
                        print(f"✅ Exported {table_name}: {len(df)} records")
                    except:
                        print(f"⚠️  Skipped {table_name} (no data or error)")
            
            conn.close()
            print(f"💾 Database backed up to: {backup_path}")
            
        except Exception as e:
            print(f"❌ Error backing up database: {e}")


# Enhanced ABS Platform with Access Database Integration
class AccessIntegratedABSPlatform:
    """
    ABS Platform integrated with Access database
    """
    
    def __init__(self, access_db_path):
        self.db_manager = AccessDBManager(access_db_path)
        # Load existing deals from database
        self.deal_database = self.db_manager.get_all_deals()
        print(f"📊 Loaded {len(self.deal_database)} deals from Access database")
    
    def add_deal(self, deal_data):
        """Add deal to both memory and Access database"""
        # Insert into Access database
        deal_id = self.db_manager.insert_deal(deal_data)
        
        if deal_id:
            # Add to memory (with database ID)
            deal_data['id'] = deal_id
            new_deal_df = pd.DataFrame([deal_data])
            self.deal_database = pd.concat([self.deal_database, new_deal_df], ignore_index=True)
            
            # Check for alerts
            alerts = self.check_deviation_alerts(deal_data)
            for alert in alerts:
                print(f"🚨 ALERT: {alert}")
        
        return deal_id
    
    def check_deviation_alerts(self, deal_data):
        """Check for deviations (same as before)"""
        alerts = []
        if len(self.deal_database) <= 1:
            return ["First deal in database - no benchmarks available"]
        
        sector_deals = self.deal_database[
            (self.deal_database['sector'] == deal_data['sector']) & 
            (self.deal_database['deal_name'] != deal_data['deal_name'])
        ]
        
        if len(sector_deals) == 0:
            return [f"First deal in {deal_data['sector']} sector"]
        
        # Calculate averages and check deviations
        avg_advance_rate = sector_deals['class_a_advance_rate'].mean()
        avg_oc = sector_deals['initial_oc'].mean()
        avg_cnl = sector_deals['expected_cnl_low'].mean()
        
        if abs(deal_data['class_a_advance_rate'] - avg_advance_rate) > 5.0:
            alerts.append(f"Class A advance rate ({deal_data['class_a_advance_rate']:.1f}%) deviates from sector avg ({avg_advance_rate:.1f}%)")
        
        if abs(deal_data['initial_oc'] - avg_oc) > 2.0:
            alerts.append(f"Initial OC ({deal_data['initial_oc']:.1f}%) deviates from sector avg ({avg_oc:.1f}%)")
        
        if abs(deal_data['expected_cnl_low'] - avg_cnl) > 1.0:
            alerts.append(f"Expected CNL ({deal_data['expected_cnl_low']:.1f}%) deviates from sector avg ({avg_cnl:.1f}%)")
        
        return alerts
    
    def run_stress_test(self, scenario='moderate', custom_loss_multiplier=1.5):
        """Run stress test and save results to database"""
        multipliers = {
            'mild': {'loss': 1.2, 'recovery': 0.95},
            'moderate': {'loss': 1.5, 'recovery': 0.85},
            'severe': {'loss': 2.0, 'recovery': 0.75},
            'custom': {'loss': custom_loss_multiplier, 'recovery': 0.85}
        }
        
        mult = multipliers[scenario]
        
        # Calculate stressed metrics
        stressed_deals = self.deal_database.copy()
        stressed_deals['loss_multiplier'] = mult['loss']
        stressed_deals['stressed_loss'] = stressed_deals['expected_cnl_low'] * mult['loss']
        stressed_deals['total_enhancement'] = stressed_deals['initial_oc'] + stressed_deals['reserve_account']
        stressed_deals['adequacy_ratio'] = stressed_deals['total_enhancement'] / stressed_deals['stressed_loss']
        stressed_deals['status'] = stressed_deals['adequacy_ratio'].apply(
            lambda x: 'Strong' if x >= 1.5 else 'Adequate' if x >= 1.2 else 'Weak' if x >= 1.0 else 'Critical'
        )
        
        # Save results to database
        for _, row in stressed_deals.iterrows():
            if 'id' in row and pd.notna(row['id']):
                self.db_manager.insert_stress_test_result(
                    int(row['id']), scenario, pd.DataFrame([row])
                )
        
        # Display results
        critical_deals = stressed_deals[stressed_deals['status'] == 'Critical']
        weak_deals = stressed_deals[stressed_deals['status'] == 'Weak']
        
        print(f"\n📊 STRESS TEST RESULTS - {scenario.upper()} SCENARIO")
        print("=" * 50)
        print(f"💀 Critical Deals: {len(critical_deals)}")
        print(f"⚠️  Weak Deals: {len(weak_deals)}")
        print(f"💰 Volume at Risk: ${(critical_deals['deal_size'].sum() + weak_deals['deal_size'].sum()):.0f}M")
        
        return stressed_deals
    
    def monte_carlo_simulation(self, deal_name, iterations=10000, loss_volatility=0.3, recovery_volatility=0.15):
        """Run Monte Carlo simulation and save results"""
        deal = self.deal_database[self.deal_database['deal_name'] == deal_name]
        if len(deal) == 0:
            print(f"❌ Deal '{deal_name}' not found")
            return None
        
        deal = deal.iloc[0]
        
        # Generate random scenarios
        np.random.seed(42)
        loss_factors = np.random.normal(1, loss_volatility, iterations)
        recovery_factors = np.random.normal(1, recovery_volatility, iterations)
        
        # Calculate outcomes
        simulated_losses = deal['expected_cnl_low'] * np.maximum(0.1, loss_factors)
        enhancement = deal['initial_oc'] + deal['reserve_account']
        breaches = simulated_losses > enhancement
        shortfalls = np.maximum(0, simulated_losses - enhancement)
        
        # Results
        results = {
            'breach_probability': np.mean(breaches) * 100,
            'avg_shortfall': np.mean(shortfalls),
            'max_shortfall': np.max(shortfalls),
            'percentile_95_loss': np.percentile(simulated_losses, 95)
        }
        
        # Save to database
        if 'id' in deal and pd.notna(deal['id']):
            self.db_manager.insert_monte_carlo_result(int(deal['id']), iterations, results)
        
        print(f"\n🎲 MONTE CARLO SIMULATION - {deal_name}")
        print("=" * 50)
        print(f"🔢 Simulations: {iterations:,}")
        print(f"💥 Breach Probability: {results['breach_probability']:.1f}%")
        print(f"📉 Average Shortfall: {results['avg_shortfall']:.2f}%")
        print(f"📊 Maximum Shortfall: {results['max_shortfall']:.2f}%")
        print(f"📈 95th Percentile Loss: {results['percentile_95_loss']:.2f}%")
        
        return results
    
    def generate_report(self):
        """Generate comprehensive report"""
        print("\n📋 ABS ANALYSIS PLATFORM REPORT")
        print("=" * 50)
        
        # Database report
        db_report = self.db_manager.generate_database_report()
        
        # Analysis summary
        if len(self.deal_database) > 0:
            total_volume = self.deal_database['deal_size'].sum()
            avg_advance_rate = self.deal_database['class_a_advance_rate'].mean()
            avg_oc = self.deal_database['initial_oc'].mean()
            
            print(f"""
📈 ANALYSIS SUMMARY:
Total Portfolio Volume: ${total_volume:.0f}M
Average Advance Rate: {avg_advance_rate:.1f}%
Average Initial OC: {avg_oc:.1f}%
            """)
    
    def backup_database(self, backup_path=None):
        """Backup database to Excel"""
        if backup_path is None:
            backup_path = f"ABS_Database_Backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        self.db_manager.backup_to_excel(backup_path)


# Quick setup function for Access integration
def setup_access_integration(access_db_path):
    """Quick setup for Access database integration"""
    try:
        platform = AccessIntegratedABSPlatform(access_db_path)
        print(f"""
✅ ACCESS DATABASE INTEGRATION READY!
=====================================

Database: {access_db_path}
Loaded Deals: {len(platform.deal_database)}

Quick Start Commands:
---------------------
# Add a new deal
platform.add_deal({
    'deal_name': 'Test Deal 2025-1',
    'issue_date': '2025-01-15',
    'rating_agency': 'KBRA',
    'sector': 'Equipment ABS',
    'deal_size': 300.0,
    'class_a_advance_rate': 75.0,
    'initial_oc': 15.0,
    'expected_cnl_low': 3.0,
    'expected_cnl_high': 4.0,
    'reserve_account': 2.0,
    'avg_seasoning': 18,
    'top_obligor_conc': 1.5
})

# Run analyses (automatically saved to database)
platform.run_stress_test('severe')
platform.monte_carlo_simulation('Deal Name', 10000)

# Generate reports
platform.generate_report()
platform.backup_database()
        """)
        
        return platform
        
    except Exception as e:
        print(f"❌ Error setting up Access integration: {e}")
        print("\nTroubleshooting:")
        print("1. Install Microsoft Access Database Engine")
        print("2. Check database file permissions")
        print("3. Ensure database file is not open in Access")
        return None

if __name__ == "__main__":
    # Example usage
    db_path = r"C:\Users\edfit\OneDrive\Documents\ABS_Performance_Data.db.accdb"
    platform = setup_access_integration(db_path)
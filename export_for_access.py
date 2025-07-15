import pandas as pd
import sqlite3
import os

def export_csv_for_access():
    """Export all tables as CSV files optimized for Access import."""
    
    # Find database file
    db_files = [f for f in os.listdir() if f.startswith('abs_performance_data_') and f.endswith('.db')]
    if not db_files:
        print("No database files found.")
        return
    
    db_file = sorted(db_files)[-1]
    print(f"Exporting from: {db_file}")
    
    # Create output directory
    output_dir = "ABS_Data_for_Access"
    os.makedirs(output_dir, exist_ok=True)
    
    # Connect to database
    conn = sqlite3.connect(db_file)
    
    try:
        # Get tables
        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
        
        print(f"\n📊 Exporting {len(tables)} tables to CSV...")
        print("=" * 50)
        
        for table_name in tables['name']:
            # Read data
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            
            # Clean data for Access
            df = clean_for_access(df)
            
            # Export to CSV
            csv_file = os.path.join(output_dir, f"{table_name}.csv")
            df.to_csv(csv_file, index=False)
            
            print(f"✓ {table_name}: {len(df)} rows → {csv_file}")
        
        # Create import instructions
        create_access_import_instructions(output_dir, tables['name'].tolist())
        
        print(f"\n🎉 All files exported to: {os.path.abspath(output_dir)}")
        
    finally:
        conn.close()

def clean_for_access(df):
    """Clean DataFrame for optimal Access import."""
    
    # Handle date columns
    date_cols = [col for col in df.columns if 'date' in col.lower()]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%m/%d/%Y')
    
    # Handle null values
    df = df.fillna('')
    
    # Ensure text fields aren't too long for Access
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str[:255]  # Access text field limit
    
    return df

def create_access_import_instructions(output_dir, table_names):
    """Create step-by-step import instructions for Access."""
    
    instructions = f"""
ABS DATA IMPORT INSTRUCTIONS FOR MICROSOFT ACCESS
================================================

📁 CSV Files Location: {os.path.abspath(output_dir)}

🚀 STEP-BY-STEP IMPORT PROCESS:

1. OPEN MICROSOFT ACCESS
   - Launch Microsoft Access
   - Create a new blank database (File > New > Blank Database)
   - Name it "ABS_Performance_Data.accdb"

2. IMPORT EACH TABLE
   For each CSV file, repeat these steps:

   a) Go to: External Data tab > New Data Source > From File > Text File
   
   b) Browse to: {os.path.abspath(output_dir)}
   
   c) Select the CSV file and click "Import the source data into a new table"
   
   d) Follow the Import Text Wizard:
      - Choose "Delimited" 
      - Choose "Comma" as delimiter
      - Check "First Row Contains Field Names"
      - Set appropriate data types for each field
      - Choose a primary key (or let Access add one)
      - Name the table (use the filename without .csv)

3. TABLES TO IMPORT:
"""
    
    for i, table in enumerate(table_names, 1):
        instructions += f"   {i}. {table}.csv → Table: {table}\n"
    
    instructions += f"""

4. RECOMMENDED DATA TYPES BY TABLE:

   deal_master:
   - issuer_name: Short Text
   - deal_short_name: Short Text  
   - securitization_date: Date/Time
   - current_collateral_balance: Number (Double)
   - months_seasoned: Number (Integer)

   performance_metrics:
   - All rate fields (cnl_rate, delinquency_*): Number (Double)
   - deal_short_name: Short Text

   historical_performance:
   - metric_value: Number (Double)
   - period_offset: Short Text
   - metric_type: Short Text

5. AFTER IMPORT:
   - Create relationships between tables using deal_short_name
   - Set up queries for analysis
   - Create reports and dashboards

📞 NEED HELP?
   - Access Help: Press F1 in Access
   - CSV files are in: {os.path.abspath(output_dir)}

"""
    
    # Save instructions (with UTF-8 encoding to handle special characters)
    instructions_file = os.path.join(output_dir, "IMPORT_INSTRUCTIONS.txt")
    with open(instructions_file, 'w', encoding='utf-8') as f:
        f.write(instructions)
    
    print(f"📋 Import instructions saved: {instructions_file}")

if __name__ == "__main__":
    export_csv_for_access()
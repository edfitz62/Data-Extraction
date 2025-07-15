# -*- coding: utf-8 -*-
"""
Created on Fri Jul 11 11:53:33 2025

@author: edfit
"""

#!/usr/bin/env python3
"""
ABS Data Extraction Runner Script

This script orchestrates the complete ABS data extraction, validation, and analysis process.
Run this script to extract data from the Excel surveillance dashboards and create
structured dataframes for database storage.

Usage:
    python run_abs_extraction.py

Requirements:
    - Both Excel files should be in the same directory as this script
    - Install required packages: pip install pandas openpyxl matplotlib seaborn sqlalchemy
"""

import os
import sys
from datetime import datetime
import logging


# Add your OneDrive directory where the scripts are located
sys.path.append(r"C:\Users\edfit\OneDrive - Whitehall Partners\Data Extraction")



# Now you can import the ABS modules
from abs_data_extractor import ABSDataExtractor
from abs_data_utilities import ABSDataValidator, ABSDataAnalyzer


try:
    from abs_data_extractor import ABSDataExtractor
    from abs_data_utilities import ABSDataValidator, ABSDataAnalyzer, create_database_export
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Make sure abs_data_extractor.py and abs_data_utilities.py are in the same directory")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'abs_extraction_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def check_file_exists(file_path: str) -> bool:
    """Check if file exists and log appropriate message."""
    if os.path.exists(file_path):
        logger.info(f"Found file: {file_path}")
        return True
    else:
        logger.error(f"File not found: {file_path}")
        return False

def main():
    """Main execution function."""
    print("=" * 60)
    print("ABS PERFORMANCE DATA EXTRACTION SYSTEM")
    print("=" * 60)
    print()
    
    # Define file paths
    marlette_file = "Marlette Funding Trust Comprehensive Surveillance Dashboard (1).xlsx"
    carvana_file = "Carvana Auto Receivables Trust Comprehensive Surveillance Dashboard.xlsx"
    
    # Check if files exist
    print("Checking for input files...")
    if not check_file_exists(marlette_file):
        print(f"Please ensure '{marlette_file}' is in the current directory")
        return False
    
    if not check_file_exists(carvana_file):
        print(f"Please ensure '{carvana_file}' is in the current directory")
        return False
    
    print("✓ All input files found")
    print()
    
    try:
        # STEP 1: Extract data
        print("STEP 1: Extracting data from Excel files...")
        print("-" * 40)
        
        extractor = ABSDataExtractor()
        dataframes = extractor.extract_from_files(marlette_file, carvana_file)
        
        if not dataframes:
            logger.error("No data was extracted from the files")
            return False
        
        print(f"✓ Successfully extracted {len(dataframes)} dataframes")
        
        # Display extraction summary
        total_rows = sum(len(df) for df in dataframes.values())
        print(f"✓ Total rows extracted: {total_rows}")
        
        for name, df in dataframes.items():
            print(f"  - {name}: {len(df)} rows, {len(df.columns)} columns")
        
        print()
        
        # STEP 2: Validate data
        print("STEP 2: Validating data quality...")
        print("-" * 40)
        
        validator = ABSDataValidator(dataframes)
        validation_results = validator.run_all_validations()
        
        # Print validation summary
        validation_report = validator.generate_validation_report()
        print(validation_report)
        print()
        
        # STEP 3: Save extracted data
        print("STEP 3: Saving extracted data...")
        print("-" * 40)
        
        # Save to Excel
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        excel_output = f"abs_extracted_data_{timestamp}.xlsx"
        
        with pd.ExcelWriter(excel_output, engine='openpyxl') as writer:
            for name, df in dataframes.items():
                sheet_name = name[:31]  # Excel sheet name limit
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"✓ Data saved to Excel: {excel_output}")
        
        # Save to SQLite database
        try:
            db_file = f"abs_performance_data_{timestamp}.db"
            create_database_export(dataframes, f'sqlite:///{db_file}')
            print(f"✓ Data exported to database: {db_file}")
        except Exception as e:
            logger.warning(f"Database export failed: {e}")
            print("⚠ Database export failed (continuing without database)")
        
        print()
        
        # STEP 4: Generate analysis (optional)
        print("STEP 4: Generating analysis and visualizations...")
        print("-" * 40)
        
        try:
            analyzer = ABSDataAnalyzer(dataframes)
            
            # Generate summary statistics
            summaries = analyzer.generate_summary_statistics()
            
            print("Summary Statistics Generated:")
            for name in summaries.keys():
                print(f"  - {name}")
            
            # Create deal comparison report
            comparison_report = analyzer.create_deal_comparison_report()
            if not comparison_report.empty:
                comparison_file = f"deal_comparison_report_{timestamp}.csv"
                comparison_report.to_csv(comparison_file, index=False)
                print(f"✓ Deal comparison report saved: {comparison_file}")
            
            # Note about visualizations
            print("📊 To generate charts, call:")
            print("   analyzer.create_performance_dashboard()")
            print("   analyzer.create_historical_trends()")
            
        except Exception as e:
            logger.warning(f"Analysis generation failed: {e}")
            print("⚠ Analysis generation failed (data extraction was successful)")
        
        print()
        
        # STEP 5: Summary
        print("STEP 5: Extraction Summary")
        print("-" * 40)
        
        print("✅ EXTRACTION COMPLETED SUCCESSFULLY")
        print()
        print("Data extracted and validated from:")
        print(f"  - {marlette_file}")
        print(f"  - {carvana_file}")
        print()
        print("Output files created:")
        print(f"  - {excel_output} (Excel format)")
        if 'db_file' in locals():
            print(f"  - {db_file} (SQLite database)")
        print()
        print("Dataframes created:")
        for name, df in dataframes.items():
            print(f"  - {name}: {len(df)} records")
        
        print()
        print("🎉 Ready for analysis and reporting!")
        
        return True
        
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        print(f"❌ EXTRACTION FAILED: {str(e)}")
        return False

def interactive_mode():
    """Run in interactive mode for debugging and exploration."""
    print("Entering interactive mode...")
    print("You can now explore the data using the following variables:")
    print("  - dataframes: Dictionary of all extracted dataframes")
    print("  - validator: Data validation utility")
    print("  - analyzer: Data analysis utility")
    print()
    
    # Make variables available globally for interactive use
    import pandas as pd
    global dataframes, validator, analyzer
    
    # Extract data
    extractor = ABSDataExtractor()
    dataframes = extractor.extract_from_files(
        "Octane Receivables Trust Comprehensive Surveillance Dashboard.xlsx",
        "Carvana Auto Receivables Trust Comprehensive Surveillance Dashboard.xlsx"
    )
    
    validator = ABSDataValidator(dataframes)
    analyzer = ABSDataAnalyzer(dataframes)
    
    print("Example commands:")
    print("  dataframes.keys()  # List all dataframes")
    print("  dataframes['deal_master'].head()  # View deal master data")
    print("  validator.run_all_validations()  # Run validation")
    print("  analyzer.create_performance_dashboard()  # Create charts")

if __name__ == "__main__":
    import pandas as pd
    
    # Check if running in interactive mode
    if len(sys.argv) > 1 and sys.argv[1] == "--interactive":
        interactive_mode()
    else:
        success = main()
        if not success:
            sys.exit(1)
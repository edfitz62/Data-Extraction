# -*- coding: utf-8 -*-
"""
Created on Sun Jul 13 12:23:31 2025

@author: edfit
"""

# -*- coding: utf-8 -*-
"""
Complete ABS Analysis System - Fixed Version
User-friendly interface for document processing and deal management
Run this file to start the complete system
"""

import sys
import os
import warnings
warnings.filterwarnings('ignore')

# Fix pandas/numpy compatibility issue
def fix_pandas_numpy_compatibility():
    """Fix common pandas/numpy compatibility issues"""
    try:
        import numpy
        print(f"✅ NumPy version: {numpy.__version__}")
    except ImportError:
        print("❌ NumPy not found - installing...")
        os.system("pip install numpy")
    
    try:
        # Try importing pandas
        import pandas as pd
        print(f"✅ Pandas version: {pd.__version__}")
        return True
    except Exception as e:
        if "numpy.dtype size changed" in str(e) or "binary incompatibility" in str(e):
            print("🔧 Fixing pandas/numpy compatibility issue...")
            print("   This may take a few minutes...")
            
            # Reinstall both numpy and pandas with compatible versions
            os.system("pip uninstall -y pandas numpy")
            os.system("pip install numpy==1.24.3")
            os.system("pip install pandas==2.0.3")
            
            try:
                import pandas as pd
                print("✅ Pandas/NumPy compatibility fixed!")
                return True
            except Exception as e2:
                print(f"❌ Still having issues: {e2}")
                print("   Trying alternative fix...")
                os.system("pip install --upgrade --force-reinstall pandas numpy")
                try:
                    import pandas as pd
                    return True
                except:
                    return False
        else:
            print(f"❌ Pandas import error: {e}")
            return False

# Try to fix pandas issues before importing other modules
print("🔧 Checking dependencies...")
pandas_working = fix_pandas_numpy_compatibility()

if not pandas_working:
    print("""
❌ DEPENDENCY ISSUE DETECTED
============================

There's a compatibility issue with pandas/numpy in your environment.

QUICK FIXES TO TRY:
1. Open Anaconda Prompt as Administrator and run:
   conda update pandas numpy

2. Or try this command:
   pip install --upgrade --force-reinstall pandas numpy

3. Restart this script after the update

4. If still having issues, create a new conda environment:
   conda create -n abs_env python=3.9 pandas numpy flask
   conda activate abs_env
   
Then run this script again.
""")
    input("Press Enter to exit...")
    sys.exit(1)

# Now import the rest of the modules
try:
    from flask import Flask, render_template_string, request, jsonify, redirect, url_for, flash
    from flask_cors import CORS
    import pandas as pd
    import numpy as np
    import json
    from datetime import datetime
    import threading
    import webbrowser
    import argparse
    import traceback
    print("✅ All Flask dependencies loaded successfully")
except ImportError as e:
    print(f"❌ Missing Flask dependencies: {e}")
    print("Installing Flask components...")
    os.system("pip install flask flask-cors")
    try:
        from flask import Flask, render_template_string, request, jsonify
        from flask_cors import CORS
        print("✅ Flask installed successfully")
    except:
        print("❌ Flask installation failed")
        sys.exit(1)

# Try to import pyodbc for database functionality
try:
    import pyodbc
    PYODBC_AVAILABLE = True
    print("✅ Database functionality available")
except ImportError:
    PYODBC_AVAILABLE = False
    print("⚠️  pyodbc not available - database features disabled")
    print("   To enable: pip install pyodbc")

# Try to import document parser
try:
    from enhanced_document_parser_with_db import (
        initialize_extractor, 
        extract_and_display_enhanced, 
        process_folder_enhanced,
        quick_database_test
    )
    PARSER_AVAILABLE = True
    print("✅ Document parser imported successfully")
except ImportError as e:
    PARSER_AVAILABLE = False
    print(f"⚠️  Document parser not available: {e}")
    print("   Make sure enhanced_document_parser_with_db.py is in the same folder")

class SimpleABSSystem:
    """Simplified ABS System that works even with limited dependencies"""
    
    def __init__(self, db_path):
        self.db_path = db_path
        self.app = Flask(__name__)
        CORS(self.app)
        self.app.secret_key = 'abs_system_secret_key_2025'
        
        # Initialize document parser if available
        self.extractor = None
        if PARSER_AVAILABLE:
            try:
                self.extractor = initialize_extractor(db_path)
                print("✅ Document parser initialized")
            except Exception as e:
                print(f"⚠️  Document parser initialization failed: {e}")
        
        # Initialize database connection if available
        self.connection_string = None
        if PYODBC_AVAILABLE:
            self.connection_string = self._setup_database_connection()
        
        # Setup all routes
        self.setup_routes()
    
    def _setup_database_connection(self):
        """Setup database connection"""
        if not os.path.exists(self.db_path):
            print(f"❌ Database file not found: {self.db_path}")
            return None
        
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
                print(f"✅ Connected to database using: {driver}")
                return conn_str
            except Exception:
                continue
        
        print("❌ No Access driver found")
        return None
    
    def get_db_connection(self):
        """Get database connection"""
        if not self.connection_string or not PYODBC_AVAILABLE:
            return None
        try:
            return pyodbc.connect(self.connection_string)
        except Exception as e:
            print(f"Database connection error: {e}")
            return None
    
    def setup_routes(self):
        """Setup all web routes"""
        
        @self.app.route('/')
        def index():
            """Main dashboard"""
            return render_template_string(self.get_simple_html_template())
        
        @self.app.route('/api/system-status')
        def system_status():
            """Get system status"""
            return jsonify({
                'database_available': PYODBC_AVAILABLE and self.connection_string is not None,
                'parser_available': PARSER_AVAILABLE,
                'pandas_available': True,  # If we got here, pandas is working
                'flask_available': True
            })
        
        @self.app.route('/api/deals', methods=['GET'])
        def get_deals():
            """Get all deals"""
            if not PYODBC_AVAILABLE or not self.connection_string:
                return jsonify([])
            
            conn = self.get_db_connection()
            if not conn:
                return jsonify([])
            
            try:
                # Get main deals
                deals_df = pd.read_sql("""
                    SELECT ID, DealName, IssueDate, RatingAgency, Sector, DealSize, 
                           ClassAAdvanceRate, InitialOC, ExpectedCNLLow, ExpectedCNLHigh,
                           ReserveAccount, AvgSeasoning, TopObligorConc, 
                           OriginalPoolBalance, CreatedDate, SourceFile, ConfidenceScore
                    FROM ABS_Deals ORDER BY CreatedDate DESC
                """, conn)
                
                conn.close()
                
                # Convert to list of dictionaries
                deals = []
                for _, deal in deals_df.iterrows():
                    deal_dict = {
                        'id': int(deal['ID']),
                        'dealName': str(deal['DealName']) if pd.notna(deal['DealName']) else '',
                        'issueDate': deal['IssueDate'].strftime('%Y-%m-%d') if pd.notna(deal['IssueDate']) else '',
                        'ratingAgency': str(deal['RatingAgency']) if pd.notna(deal['RatingAgency']) else '',
                        'sector': str(deal['Sector']) if pd.notna(deal['Sector']) else '',
                        'dealSize': float(deal['DealSize']) if pd.notna(deal['DealSize']) else 0,
                        'classAAdvanceRate': float(deal['ClassAAdvanceRate']) if pd.notna(deal['ClassAAdvanceRate']) else 0,
                        'initialOC': float(deal['InitialOC']) if pd.notna(deal['InitialOC']) else 0,
                        'expectedCNLLow': float(deal['ExpectedCNLLow']) if pd.notna(deal['ExpectedCNLLow']) else 0,
                        'expectedCNLHigh': float(deal['ExpectedCNLHigh']) if pd.notna(deal['ExpectedCNLHigh']) else 0,
                        'reserveAccount': float(deal['ReserveAccount']) if pd.notna(deal['ReserveAccount']) else 0,
                        'avgSeasoning': int(deal['AvgSeasoning']) if pd.notna(deal['AvgSeasoning']) else 0,
                        'topObligorConc': float(deal['TopObligorConc']) if pd.notna(deal['TopObligorConc']) else 0,
                        'originalPoolBalance': float(deal['OriginalPoolBalance']) if pd.notna(deal['OriginalPoolBalance']) else 0,
                        'createdDate': deal['CreatedDate'].strftime('%Y-%m-%d %H:%M') if pd.notna(deal['CreatedDate']) else '',
                        'sourceFile': str(deal['SourceFile']) if pd.notna(deal['SourceFile']) else '',
                        'confidenceScore': int(deal['ConfidenceScore']) if pd.notna(deal['ConfidenceScore']) else 0
                    }
                    deals.append(deal_dict)
                
                return jsonify(deals)
                
            except Exception as e:
                print(f"Error retrieving deals: {e}")
                return jsonify([])
        
        @self.app.route('/api/deals', methods=['POST'])
        def add_deal():
            """Add new deal manually"""
            if not PYODBC_AVAILABLE or not self.connection_string:
                return jsonify({'success': False, 'message': 'Database not available'}), 500
            
            try:
                deal_data = request.json
                conn = self.get_db_connection()
                if not conn:
                    return jsonify({'success': False, 'message': 'Database connection failed'}), 500
                
                cursor = conn.cursor()
                
                # Parse issue date
                issue_date = None
                if deal_data.get('issueDate'):
                    try:
                        issue_date = datetime.strptime(deal_data['issueDate'], '%Y-%m-%d')
                    except:
                        pass
                
                # Insert deal
                insert_sql = '''
                    INSERT INTO ABS_Deals 
                    (DealName, IssueDate, RatingAgency, Sector, DealSize, 
                     ClassAAdvanceRate, InitialOC, ExpectedCNLLow, ExpectedCNLHigh,
                     ReserveAccount, AvgSeasoning, TopObligorConc, OriginalPoolBalance,
                     CreatedDate, SourceFile, ConfidenceScore)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''
                
                values = (
                    deal_data.get('dealName', ''),
                    issue_date,
                    deal_data.get('ratingAgency', ''),
                    deal_data.get('sector', ''),
                    float(deal_data.get('dealSize', 0)),
                    float(deal_data.get('classAAdvanceRate', 0)),
                    float(deal_data.get('initialOC', 0)),
                    float(deal_data.get('expectedCNLLow', 0)),
                    float(deal_data.get('expectedCNLHigh', 0)),
                    float(deal_data.get('reserveAccount', 0)),
                    int(deal_data.get('avgSeasoning', 0)),
                    float(deal_data.get('topObligorConc', 0)),
                    float(deal_data.get('originalPoolBalance', 0)),
                    datetime.now(),
                    'Manual Entry',
                    100  # Manual entries have 100% confidence
                )
                
                cursor.execute(insert_sql, values)
                cursor.execute("SELECT @@IDENTITY")
                deal_id = cursor.fetchone()[0]
                
                conn.commit()
                conn.close()
                
                return jsonify({'success': True, 'id': deal_id, 'message': 'Deal added successfully'})
                
            except Exception as e:
                return jsonify({'success': False, 'message': str(e)}), 500
        
        @self.app.route('/api/process-folder', methods=['POST'])
        def process_folder():
            """Process documents in a folder"""
            if not PARSER_AVAILABLE:
                return jsonify({'success': False, 'message': 'Document parser not available'}), 500
            
            try:
                data = request.json
                folder_path = data.get('folderPath', '').strip()
                
                if not folder_path:
                    return jsonify({'success': False, 'message': 'Please provide a folder path'}), 400
                
                if not os.path.exists(folder_path):
                    return jsonify({'success': False, 'message': f'Folder not found: {folder_path}'}), 400
                
                # Process the folder
                results = process_folder_enhanced(
                    folder_path, 
                    export_excel=True, 
                    save_to_db=True
                )
                
                successful = [r for r in results if 'error' not in r]
                failed = [r for r in results if 'error' in r]
                
                return jsonify({
                    'success': True,
                    'message': f'Processed {len(results)} files',
                    'successful': len(successful),
                    'failed': len(failed),
                    'failedFiles': [r.get('error', 'Unknown error') for r in failed]
                })
                
            except Exception as e:
                return jsonify({'success': False, 'message': str(e)}), 500
        
        @self.app.route('/api/process-file', methods=['POST'])
        def process_single_file():
            """Process a single document file"""
            if not PARSER_AVAILABLE:
                return jsonify({'success': False, 'message': 'Document parser not available'}), 500
            
            try:
                data = request.json
                file_path = data.get('filePath', '').strip()
                
                if not file_path:
                    return jsonify({'success': False, 'message': 'Please provide a file path'}), 400
                
                if not os.path.exists(file_path):
                    return jsonify({'success': False, 'message': f'File not found: {file_path}'}), 400
                
                # Process the file
                result = extract_and_display_enhanced(file_path, save_to_db=True)
                
                if 'error' in result:
                    return jsonify({'success': False, 'message': result['error']}), 400
                
                return jsonify({
                    'success': True,
                    'message': f'Successfully processed: {os.path.basename(file_path)}',
                    'dealName': result.get('deal_name', 'Unknown'),
                    'confidenceScore': result.get('confidence_score', 0),
                    'noteClasses': len(result.get('note_classes', [])),
                    'databaseId': result.get('database_id')
                })
                
            except Exception as e:
                return jsonify({'success': False, 'message': str(e)}), 500
    
    def get_simple_html_template(self):
        """Return simplified HTML template that works with basic dependencies"""
        return '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ABS Analysis System</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            color: #333;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }
        
        .header {
            text-align: center;
            margin-bottom: 30px;
            color: white;
        }
        
        .header h1 {
            font-size: 2.5rem;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        
        .status-bar {
            background: rgba(255,255,255,0.1);
            backdrop-filter: blur(10px);
            border-radius: 10px;
            padding: 15px;
            margin-bottom: 20px;
            color: white;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 10px;
        }
        
        .status-item {
            display: flex;
            align-items: center;
            gap: 5px;
        }
        
        .status-indicator {
            width: 10px;
            height: 10px;
            border-radius: 50%;
            background: #4caf50;
        }
        
        .status-indicator.error { background: #f44336; }
        
        .tabs {
            display: flex;
            background: rgba(255,255,255,0.1);
            backdrop-filter: blur(10px);
            border-radius: 15px;
            padding: 10px;
            margin-bottom: 20px;
            flex-wrap: wrap;
            gap: 10px;
        }
        
        .tab-button {
            flex: 1;
            min-width: 200px;
            padding: 15px 20px;
            background: rgba(255,255,255,0.2);
            border: none;
            border-radius: 10px;
            color: white;
            cursor: pointer;
            transition: all 0.3s ease;
            font-weight: 600;
        }
        
        .tab-button:hover {
            background: rgba(255,255,255,0.3);
            transform: translateY(-2px);
        }
        
        .tab-button.active {
            background: rgba(255,255,255,0.9);
            color: #333;
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
        }
        
        .tab-content {
            display: none;
            background: rgba(255,255,255,0.95);
            backdrop-filter: blur(10px);
            border-radius: 15px;
            padding: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }
        
        .tab-content.active {
            display: block;
            animation: fadeIn 0.5s ease-in;
        }
        
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(20px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        .form-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .form-group {
            display: flex;
            flex-direction: column;
        }
        
        .form-group label {
            font-weight: 600;
            margin-bottom: 8px;
            color: #555;
        }
        
        .form-group input, .form-group select {
            padding: 12px;
            border: 2px solid #e1e5e9;
            border-radius: 8px;
            font-size: 14px;
            transition: border-color 0.3s ease;
        }
        
        .form-group input:focus, .form-group select:focus {
            outline: none;
            border-color: #667eea;
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
        }
        
        .btn {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            padding: 15px 30px;
            border-radius: 10px;
            cursor: pointer;
            font-weight: 600;
            transition: all 0.3s ease;
            margin: 10px;
        }
        
        .btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
        }
        
        .btn-success { background: linear-gradient(135deg, #51cf66 0%, #40c057 100%); }
        .btn-info { background: linear-gradient(135deg, #339af0 0%, #228be6 100%); }
        
        .processing-section {
            background: #f8f9fa;
            border-radius: 10px;
            padding: 25px;
            margin-bottom: 30px;
            border: 2px solid #e9ecef;
        }
        
        .processing-section h3 {
            color: #495057;
            margin-bottom: 15px;
        }
        
        .path-input {
            width: 100%;
            margin-bottom: 15px;
        }
        
        .alert {
            padding: 15px;
            border-radius: 8px;
            margin: 10px 0;
            font-weight: 600;
        }
        
        .alert-success {
            background: #e8f5e8;
            border: 1px solid #c8e6c9;
            color: #2e7d32;
        }
        
        .alert-error {
            background: #ffebee;
            border: 1px solid #ffcdd2;
            color: #c62828;
        }
        
        .alert-info {
            background: #e3f2fd;
            border: 1px solid #bbdefb;
            color: #1976d2;
        }
        
        .help-text {
            font-size: 12px;
            color: #666;
            margin-top: 5px;
            font-style: italic;
        }
        
        .data-table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            background: white;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
        }
        
        .data-table th, .data-table td {
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid #e1e5e9;
        }
        
        .data-table th {
            background: #667eea;
            color: white;
            font-weight: 600;
        }
        
        .data-table tr:hover { background: #f8f9fa; }
        
        .loading {
            display: none;
            text-align: center;
            padding: 20px;
            color: #666;
        }
        
        .spinner {
            border: 4px solid #f3f3f3;
            border-top: 4px solid #667eea;
            border-radius: 50%;
            width: 40px;
            height: 40px;
            animation: spin 1s linear infinite;
            margin: 0 auto 10px;
        }
        
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏦 ABS Analysis System</h1>
            <p>Document Processing & Deal Management Platform</p>
        </div>
        
        <div class="status-bar" id="statusBar">
            <div class="status-item">
                <div class="status-indicator" id="dbIndicator"></div>
                <span id="dbStatus">Checking database...</span>
            </div>
            <div class="status-item">
                <div class="status-indicator" id="parserIndicator"></div>
                <span id="parserStatus">Checking parser...</span>
            </div>
            <div class="status-item">
                <span id="dealCount">Loading...</span>
            </div>
        </div>
        
        <div class="tabs">
            <button class="tab-button active" onclick="showTab('document-processing')">📄 Document Processing</button>
            <button class="tab-button" onclick="showTab('manual-entry')">✍️ Manual Entry</button>
            <button class="tab-button" onclick="showTab('setup')">⚙️ System Setup</button>
        </div>
        
        <!-- Document Processing Tab -->
        <div id="document-processing" class="tab-content active">
            <h2>📄 Automatic Document Processing</h2>
            <p>Process New Issue Reports automatically. Data will be saved to your database.</p>
            
            <div class="processing-section">
                <h3>📁 Process Entire Folder</h3>
                <p>Enter the full path to a folder containing PDF or Word documents.</p>
                
                <div class="form-group">
                    <label for="folderPath">Folder Path</label>
                    <input type="text" id="folderPath" class="path-input" 
                           placeholder="e.g., C:\\Users\\YourName\\Documents\\Reports"
                           value="C:\\Users\\edfit\\OneDrive - Whitehall Partners\\Data Extraction\\New_Issue_Reports">
                    <div class="help-text">💡 Copy the folder path from Windows Explorer address bar</div>
                </div>
                
                <button class="btn btn-success" onclick="processFolderDocuments()">
                    🚀 Process All Documents
                </button>
            </div>
            
            <div class="processing-section">
                <h3>📄 Process Single File</h3>
                <p>Enter the full path to a specific document.</p>
                
                <div class="form-group">
                    <label for="filePath">File Path</label>
                    <input type="text" id="filePath" class="path-input" 
                           placeholder="e.g., C:\\Users\\YourName\\Documents\\Report.pdf">
                    <div class="help-text">💡 Right-click file → Properties to get full path</div>
                </div>
                
                <button class="btn btn-info" onclick="processSingleFile()">
                    📊 Process File
                </button>
            </div>
            
            <div id="processingAlerts"></div>
        </div>
        
        <!-- Manual Entry Tab -->
        <div id="manual-entry" class="tab-content">
            <h2>✍️ Manual Deal Entry</h2>
            
            <form id="dealForm">
                <div class="form-grid">
                    <div class="form-group">
                        <label>Deal Name *</label>
                        <input type="text" id="dealName" required>
                    </div>
                    <div class="form-group">
                        <label>Issue Date *</label>
                        <input type="date" id="issueDate" required>
                    </div>
                    <div class="form-group">
                        <label>Rating Agency *</label>
                        <select id="ratingAgency" required>
                            <option value="">Select Agency</option>
                            <option value="KBRA">KBRA</option>
                            <option value="Moody's">Moody's</option>
                            <option value="S&P">S&P</option>
                            <option value="Fitch">Fitch</option>
                            <option value="DBRS">DBRS</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label>Sector *</label>
                        <select id="sector" required>
                            <option value="">Select Sector</option>
                            <option value="Equipment ABS">Equipment ABS</option>
                            <option value="Small Ticket Leasing">Small Ticket Leasing</option>
                            <option value="Working Capital">Working Capital</option>
                            <option value="Auto ABS">Auto ABS</option>
                            <option value="Consumer Loans">Consumer Loans</option>
                            <option value="Credit Card">Credit Card</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label>Deal Size ($MM) *</label>
                        <input type="number" id="dealSize" step="0.01" required>
                    </div>
                    <div class="form-group">
                        <label>Pool Balance ($MM)</label>
                        <input type="number" id="originalPoolBalance" step="0.01">
                    </div>
                    <div class="form-group">
                        <label>Class A Advance Rate (%) *</label>
                        <input type="number" id="classAAdvanceRate" step="0.01" required>
                    </div>
                    <div class="form-group">
                        <label>Initial OC (%) *</label>
                        <input type="number" id="initialOC" step="0.01" required>
                    </div>
                    <div class="form-group">
                        <label>Expected CNL Low (%) *</label>
                        <input type="number" id="expectedCNLLow" step="0.01" required>
                    </div>
                    <div class="form-group">
                        <label>Expected CNL High (%)</label>
                        <input type="number" id="expectedCNLHigh" step="0.01">
                    </div>
                    <div class="form-group">
                        <label>Reserve Account (%) *</label>
                        <input type="number" id="reserveAccount" step="0.01" required>
                    </div>
                    <div class="form-group">
                        <label>Avg Seasoning (months) *</label>
                        <input type="number" id="avgSeasoning" required>
                    </div>
                    <div class="form-group">
                        <label>Top Obligor Conc (%) *</label>
                        <input type="number" id="topObligorConc" step="0.01" required>
                    </div>
                </div>
                
                <button type="submit" class="btn btn-success">💾 Save Deal</button>
                <button type="button" class="btn" onclick="clearForm()">🔄 Clear</button>
            </form>
            
            <div id="dealAlerts"></div>
            
            <div id="recentDeals">
                <h3>📋 Recent Deals</h3>
                <div class="loading" id="loadingDeals">
                    <div class="spinner"></div>
                    <p>Loading deals...</p>
                </div>
                <table class="data-table" id="dealsTable" style="display: none;">
                    <thead>
                        <tr>
                            <th>Deal Name</th>
                            <th>Sector</th>
                            <th>Size ($MM)</th>
                            <th>Advance Rate</th>
                            <th>Source</th>
                            <th>Date Added</th>
                        </tr>
                    </thead>
                    <tbody id="dealsTableBody"></tbody>
                </table>
            </div>
        </div>
        
        <!-- Setup Tab -->
        <div id="setup" class="tab-content">
            <h2>⚙️ System Setup & Troubleshooting</h2>
            
            <div class="processing-section">
                <h3>📊 System Status</h3>
                <div id="systemStatusDetails">
                    <div class="loading">
                        <div class="spinner"></div>
                        <p>Checking system status...</p>
                    </div>
                </div>
            </div>
            
            <div class="processing-section">
                <h3>🔧 Quick Fixes</h3>
                
                <h4>Database Issues:</h4>
                <ul>
                    <li>Install Microsoft Access Database Engine: <a href="https://www.microsoft.com/en-us/download/details.aspx?id=54920" target="_blank">Download Here</a></li>
                    <li>Check database file path and permissions</li>
                    <li>Ensure database is not open in Access</li>
                </ul>
                
                <h4>Document Parser Issues:</h4>
                <ul>
                    <li>Ensure enhanced_document_parser_with_db.py is in the same folder</li>
                    <li>Install document processing libraries: <code>pip install PyMuPDF PyPDF2 python-docx</code></li>
                </ul>
                
                <h4>Python Environment Issues:</h4>
                <ul>
                    <li>Update pandas/numpy: <code>pip install --upgrade pandas numpy</code></li>
                    <li>Install Flask: <code>pip install flask flask-cors</code></li>
                    <li>Install pyodbc: <code>pip install pyodbc</code></li>
                </ul>
            </div>
        </div>
    </div>

    <script>
        let systemStatus = {};
        
        document.addEventListener('DOMContentLoaded', function() {
            checkSystemStatus();
            loadDeals();
        });
        
        async function checkSystemStatus() {
            try {
                const response = await fetch('/api/system-status');
                systemStatus = await response.json();
                
                updateStatusIndicators();
                displaySystemDetails();
                
            } catch (error) {
                console.error('Status check failed:', error);
                updateStatusIndicators(true);
            }
        }
        
        function updateStatusIndicators(error = false) {
            const dbIndicator = document.getElementById('dbIndicator');
            const dbStatus = document.getElementById('dbStatus');
            const parserIndicator = document.getElementById('parserIndicator');
            const parserStatus = document.getElementById('parserStatus');
            
            if (error) {
                dbIndicator.classList.add('error');
                dbStatus.textContent = 'Connection Error';
                parserIndicator.classList.add('error');
                parserStatus.textContent = 'Connection Error';
                return;
            }
            
            if (systemStatus.database_available) {
                dbIndicator.classList.remove('error');
                dbStatus.textContent = 'Database Connected';
            } else {
                dbIndicator.classList.add('error');
                dbStatus.textContent = 'Database Unavailable';
            }
            
            if (systemStatus.parser_available) {
                parserIndicator.classList.remove('error');
                parserStatus.textContent = 'Parser Ready';
            } else {
                parserIndicator.classList.add('error');
                parserStatus.textContent = 'Parser Unavailable';
            }
        }
        
        function displaySystemDetails() {
            const container = document.getElementById('systemStatusDetails');
            
            let html = '<h4>Component Status:</h4><ul>';
            html += `<li>Database: ${systemStatus.database_available ? '✅ Available' : '❌ Not Available'}</li>`;
            html += `<li>Document Parser: ${systemStatus.parser_available ? '✅ Available' : '❌ Not Available'}</li>`;
            html += `<li>Pandas: ${systemStatus.pandas_available ? '✅ Working' : '❌ Issues'}</li>`;
            html += `<li>Flask: ${systemStatus.flask_available ? '✅ Working' : '❌ Issues'}</li>`;
            html += '</ul>';
            
            if (!systemStatus.database_available) {
                html += '<div class="alert alert-error">Database not available. Manual entry disabled.</div>';
            }
            
            if (!systemStatus.parser_available) {
                html += '<div class="alert alert-error">Document parser not available. Document processing disabled.</div>';
            }
            
            container.innerHTML = html;
        }
        
        function showTab(tabName) {
            document.querySelectorAll('.tab-content').forEach(content => {
                content.classList.remove('active');
            });
            
            document.querySelectorAll('.tab-button').forEach(button => {
                button.classList.remove('active');
            });
            
            document.getElementById(tabName).classList.add('active');
            event.target.classList.add('active');
        }
        
        async function processFolderDocuments() {
            const folderPath = document.getElementById('folderPath').value.trim();
            
            if (!folderPath) {
                showAlert('Please enter a folder path', 'error', 'processingAlerts');
                return;
            }
            
            if (!systemStatus.parser_available) {
                showAlert('Document parser not available. Check Setup tab.', 'error', 'processingAlerts');
                return;
            }
            
            showAlert('Processing folder... Please wait.', 'info', 'processingAlerts');
            
            try {
                const response = await fetch('/api/process-folder', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ folderPath })
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showAlert(`✅ Success! ${result.successful} files processed, ${result.failed} failed.`, 'success', 'processingAlerts');
                    loadDeals();
                } else {
                    showAlert(`Error: ${result.message}`, 'error', 'processingAlerts');
                }
                
            } catch (error) {
                showAlert(`Error: ${error.message}`, 'error', 'processingAlerts');
            }
        }
        
        async function processSingleFile() {
            const filePath = document.getElementById('filePath').value.trim();
            
            if (!filePath) {
                showAlert('Please enter a file path', 'error', 'processingAlerts');
                return;
            }
            
            if (!systemStatus.parser_available) {
                showAlert('Document parser not available. Check Setup tab.', 'error', 'processingAlerts');
                return;
            }
            
            showAlert('Processing file...', 'info', 'processingAlerts');
            
            try {
                const response = await fetch('/api/process-file', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ filePath })
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showAlert(`✅ Success! ${result.dealName} (${result.confidenceScore}% confidence)`, 'success', 'processingAlerts');
                    document.getElementById('filePath').value = '';
                    loadDeals();
                } else {
                    showAlert(`Error: ${result.message}`, 'error', 'processingAlerts');
                }
                
            } catch (error) {
                showAlert(`Error: ${error.message}`, 'error', 'processingAlerts');
            }
        }
        
        // Deal form submission
        document.getElementById('dealForm').addEventListener('submit', async function(e) {
            e.preventDefault();
            
            if (!systemStatus.database_available) {
                showAlert('Database not available. Cannot save deal.', 'error', 'dealAlerts');
                return;
            }
            
            const dealData = {
                dealName: document.getElementById('dealName').value,
                issueDate: document.getElementById('issueDate').value,
                ratingAgency: document.getElementById('ratingAgency').value,
                sector: document.getElementById('sector').value,
                dealSize: parseFloat(document.getElementById('dealSize').value),
                originalPoolBalance: parseFloat(document.getElementById('originalPoolBalance').value) || 0,
                classAAdvanceRate: parseFloat(document.getElementById('classAAdvanceRate').value),
                initialOC: parseFloat(document.getElementById('initialOC').value),
                expectedCNLLow: parseFloat(document.getElementById('expectedCNLLow').value),
                expectedCNLHigh: parseFloat(document.getElementById('expectedCNLHigh').value) || 
                                 parseFloat(document.getElementById('expectedCNLLow').value),
                reserveAccount: parseFloat(document.getElementById('reserveAccount').value),
                avgSeasoning: parseInt(document.getElementById('avgSeasoning').value),
                topObligorConc: parseFloat(document.getElementById('topObligorConc').value)
            };
            
            try {
                const response = await fetch('/api/deals', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(dealData)
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showAlert('Deal saved successfully! 🎉', 'success', 'dealAlerts');
                    clearForm();
                    loadDeals();
                } else {
                    showAlert('Error: ' + result.message, 'error', 'dealAlerts');
                }
            } catch (error) {
                showAlert('Error: ' + error.message, 'error', 'dealAlerts');
            }
        });
        
        function clearForm() {
            document.getElementById('dealForm').reset();
        }
        
        async function loadDeals() {
            if (!systemStatus.database_available) {
                document.getElementById('dealCount').textContent = 'Database not available';
                return;
            }
            
            document.getElementById('loadingDeals').style.display = 'block';
            document.getElementById('dealsTable').style.display = 'none';
            
            try {
                const response = await fetch('/api/deals');
                const deals = await response.json();
                
                document.getElementById('dealCount').textContent = `${deals.length} deals loaded`;
                
                const tbody = document.getElementById('dealsTableBody');
                tbody.innerHTML = '';
                
                deals.forEach(deal => {
                    const row = tbody.insertRow();
                    row.innerHTML = `
                        <td><strong>${deal.dealName}</strong></td>
                        <td>${deal.sector}</td>
                        <td>$${deal.dealSize.toFixed(0)}M</td>
                        <td>${deal.classAAdvanceRate.toFixed(1)}%</td>
                        <td>${deal.sourceFile || 'Manual'}</td>
                        <td>${deal.createdDate}</td>
                    `;
                });
                
                document.getElementById('loadingDeals').style.display = 'none';
                document.getElementById('dealsTable').style.display = 'table';
                
            } catch (error) {
                showAlert('Error loading deals: ' + error.message, 'error', 'dealAlerts');
                document.getElementById('loadingDeals').style.display = 'none';
                document.getElementById('dealCount').textContent = 'Error loading deals';
            }
        }
        
        function showAlert(message, type, containerId = 'dealAlerts') {
            const alertDiv = document.createElement('div');
            alertDiv.className = `alert alert-${type}`;
            alertDiv.textContent = message;
            
            const container = document.getElementById(containerId);
            container.appendChild(alertDiv);
            
            setTimeout(() => alertDiv.remove(), 8000);
        }
    </script>
</body>
</html>
        '''
    
    def run(self, host='127.0.0.1', port=5000, debug=False):
        """Start the ABS system"""
        print(f"🚀 Starting ABS Analysis System (Fixed Version)")
        print(f"🌐 Web Interface: http://{host}:{port}")
        print(f"📊 Database: {'✅ Available' if PYODBC_AVAILABLE and self.connection_string else '❌ Not Available'}")
        print(f"📄 Parser: {'✅ Available' if PARSER_AVAILABLE else '❌ Not Available'}")
        print("🛑 Press Ctrl+C to stop")
        print("=" * 50)
        
        if not debug:
            threading.Timer(1.5, lambda: webbrowser.open(f'http://{host}:{port}')).start()
        
        self.app.run(host=host, port=port, debug=debug)

def main():
    """Main function with command line support"""
    if len(sys.argv) > 1:
        parser = argparse.ArgumentParser(description='ABS Analysis System')
        parser.add_argument('--process-folder', type=str, help='Process documents in folder')
        parser.add_argument('--process-file', type=str, help='Process single file')
        parser.add_argument('--db', type=str, 
                           default=r"C:\Users\edfit\OneDrive\Documents\ABS_Performance_Data.db.accdb",
                           help='Database path')
        parser.add_argument('--port', type=int, default=5000, help='Port')
        parser.add_argument('--host', type=str, default='127.0.0.1', help='Host')
        
        args = parser.parse_args()
        
        if args.process_folder and PARSER_AVAILABLE:
            print(f"📁 Processing folder: {args.process_folder}")
            extractor = initialize_extractor(args.db)
            results = process_folder_enhanced(args.process_folder, True, True)
            print(f"✅ Completed processing")
            return
        elif args.process_file and PARSER_AVAILABLE:
            print(f"📄 Processing file: {args.process_file}")
            extractor = initialize_extractor(args.db)
            result = extract_and_display_enhanced(args.process_file, True)
            return
        else:
            system = SimpleABSSystem(args.db)
            system.run(args.host, args.port)
    else:
        # Default web interface
        db_path = r"C:\Users\edfit\OneDrive\Documents\ABS_Performance_Data.db.accdb"
        system = SimpleABSSystem(db_path)
        system.run()

if __name__ == "__main__":
    main()
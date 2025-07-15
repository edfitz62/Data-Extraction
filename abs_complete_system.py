# -*- coding: utf-8 -*-
"""
Created on Sun Jul 13 12:12:19 2025

@author: edfit
"""

# -*- coding: utf-8 -*-
"""
Complete ABS Analysis System
User-friendly interface for document processing and deal management
Run this file to start the complete system
"""

from flask import Flask, render_template_string, request, jsonify, redirect, url_for, flash
from flask_cors import CORS
import pandas as pd
import numpy as np
import pyodbc
import json
import plotly
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import threading
import webbrowser
import warnings
import os
import sys
import argparse
import traceback
warnings.filterwarnings('ignore')

# Import our enhanced document parser
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

class CompleteABSSystem:
    """Complete ABS System with Web UI and Document Processing"""
    
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
        
        # Initialize database connection
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
        if not self.connection_string:
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
            return render_template_string(self.get_complete_html_template())
        
        @self.app.route('/api/deals', methods=['GET'])
        def get_deals():
            """Get all deals with note classes"""
            conn = self.get_db_connection()
            if not conn:
                return jsonify([])
            
            try:
                # Get main deals
                deals_df = pd.read_sql("""
                    SELECT ID, DealName, IssueDate, RatingAgency, Sector, DealSize, 
                           ClassAAdvanceRate, InitialOC, ExpectedCNLLow, ExpectedCNLHigh,
                           ReserveAccount, AvgSeasoning, TopObligorConc, OriginalPoolBalance,
                           CreatedDate, SourceFile, ConfidenceScore
                    FROM ABS_Deals ORDER BY CreatedDate DESC
                """, conn)
                
                # Get note classes
                note_classes_df = pd.read_sql("""
                    SELECT DealID, Class, AmountMillions, InterestRate, Rating, Maturity
                    FROM NoteClasses ORDER BY DealID, Class
                """, conn)
                
                conn.close()
                
                # Combine deals with their note classes
                deals = []
                for _, deal in deals_df.iterrows():
                    deal_note_classes = note_classes_df[note_classes_df['DealID'] == deal['ID']]
                    
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
                        'confidenceScore': int(deal['ConfidenceScore']) if pd.notna(deal['ConfidenceScore']) else 0,
                        'noteClasses': []
                    }
                    
                    # Add note classes
                    for _, note_class in deal_note_classes.iterrows():
                        note_dict = {
                            'class': str(note_class['Class']) if pd.notna(note_class['Class']) else '',
                            'amountMillions': float(note_class['AmountMillions']) if pd.notna(note_class['AmountMillions']) else 0,
                            'interestRate': float(note_class['InterestRate']) if pd.notna(note_class['InterestRate']) else 0,
                            'rating': str(note_class['Rating']) if pd.notna(note_class['Rating']) else '',
                            'maturity': str(note_class['Maturity']) if pd.notna(note_class['Maturity']) else ''
                        }
                        deal_dict['noteClasses'].append(note_dict)
                    
                    deals.append(deal_dict)
                
                return jsonify(deals)
                
            except Exception as e:
                print(f"Error retrieving deals: {e}")
                return jsonify([])
        
        @self.app.route('/api/deals', methods=['POST'])
        def add_deal():
            """Add new deal manually"""
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
            try:
                data = request.json
                folder_path = data.get('folderPath', '').strip()
                
                if not folder_path:
                    return jsonify({'success': False, 'message': 'Please provide a folder path'}), 400
                
                if not os.path.exists(folder_path):
                    return jsonify({'success': False, 'message': f'Folder not found: {folder_path}'}), 400
                
                if not PARSER_AVAILABLE or not self.extractor:
                    return jsonify({'success': False, 'message': 'Document parser not available'}), 500
                
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
            try:
                data = request.json
                file_path = data.get('filePath', '').strip()
                
                if not file_path:
                    return jsonify({'success': False, 'message': 'Please provide a file path'}), 400
                
                if not os.path.exists(file_path):
                    return jsonify({'success': False, 'message': f'File not found: {file_path}'}), 400
                
                if not PARSER_AVAILABLE or not self.extractor:
                    return jsonify({'success': False, 'message': 'Document parser not available'}), 500
                
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
        
        @self.app.route('/api/stress-test', methods=['POST'])
        def stress_test():
            """Run stress test"""
            try:
                data = request.json
                scenario = data.get('scenario', 'moderate')
                
                conn = self.get_db_connection()
                if not conn:
                    return jsonify({'error': 'Database connection failed'}), 500
                
                deals_df = pd.read_sql("SELECT * FROM ABS_Deals", conn)
                conn.close()
                
                if len(deals_df) == 0:
                    return jsonify({'error': 'No deals found in database'})
                
                multipliers = {'mild': 1.2, 'moderate': 1.5, 'severe': 2.0}
                loss_mult = multipliers.get(scenario, 1.5)
                
                results = []
                for _, deal in deals_df.iterrows():
                    stressed_loss = float(deal['ExpectedCNLLow']) * loss_mult
                    total_enhancement = float(deal['InitialOC']) + float(deal['ReserveAccount'])
                    adequacy_ratio = total_enhancement / stressed_loss if stressed_loss > 0 else 999
                    
                    if adequacy_ratio >= 1.5:
                        status = 'Strong'
                    elif adequacy_ratio >= 1.2:
                        status = 'Adequate'
                    elif adequacy_ratio >= 1.0:
                        status = 'Weak'
                    else:
                        status = 'Critical'
                    
                    results.append({
                        'dealName': deal['DealName'],
                        'stressedLoss': round(stressed_loss, 2),
                        'adequacyRatio': round(adequacy_ratio, 2),
                        'status': status,
                        'dealSize': float(deal['DealSize'])
                    })
                
                # Summary
                critical = len([r for r in results if r['status'] == 'Critical'])
                weak = len([r for r in results if r['status'] == 'Weak'])
                volume_at_risk = sum([r['dealSize'] for r in results if r['status'] in ['Critical', 'Weak']])
                
                return jsonify({
                    'scenario': scenario,
                    'results': results,
                    'summary': {
                        'critical': critical,
                        'weak': weak,
                        'total_at_risk': critical + weak,
                        'volume_at_risk': round(volume_at_risk, 0)
                    }
                })
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/database-status')
        def database_status():
            """Get database status and statistics"""
            try:
                conn = self.get_db_connection()
                if not conn:
                    return jsonify({'connected': False, 'message': 'Database connection failed'})
                
                cursor = conn.cursor()
                
                # Get deals count
                cursor.execute("SELECT COUNT(*) FROM ABS_Deals")
                deals_count = cursor.fetchone()[0]
                
                # Get note classes count
                cursor.execute("SELECT COUNT(*) FROM NoteClasses")
                classes_count = cursor.fetchone()[0]
                
                # Get recent activity
                cursor.execute("SELECT TOP 1 CreatedDate FROM ABS_Deals ORDER BY CreatedDate DESC")
                last_activity = cursor.fetchone()
                last_activity_str = last_activity[0].strftime('%Y-%m-%d %H:%M') if last_activity and last_activity[0] else 'None'
                
                conn.close()
                
                return jsonify({
                    'connected': True,
                    'deals_count': deals_count,
                    'note_classes_count': classes_count,
                    'last_activity': last_activity_str,
                    'parser_available': PARSER_AVAILABLE
                })
                
            except Exception as e:
                return jsonify({'connected': False, 'message': str(e)})
    
    def get_complete_html_template(self):
        """Return the complete HTML template with enhanced functionality"""
        return '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ABS Analysis System - Complete Platform</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            color: #333;
        }
        
        .container {
            max-width: 1400px;
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
        
        .status-indicator.error {
            background: #f44336;
        }
        
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
            min-width: 160px;
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
        
        .form-group input, .form-group select, .form-group textarea {
            padding: 12px;
            border: 2px solid #e1e5e9;
            border-radius: 8px;
            font-size: 14px;
            transition: border-color 0.3s ease;
            font-family: inherit;
        }
        
        .form-group input:focus, .form-group select:focus, .form-group textarea:focus {
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
            font-size: 14px;
        }
        
        .btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
        }
        
        .btn-success {
            background: linear-gradient(135deg, #51cf66 0%, #40c057 100%);
        }
        
        .btn-danger {
            background: linear-gradient(135deg, #ff6b6b 0%, #ee5a52 100%);
        }
        
        .btn-info {
            background: linear-gradient(135deg, #339af0 0%, #228be6 100%);
        }
        
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
            display: flex;
            align-items: center;
            gap: 10px;
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
        
        .alert-warning {
            background: #fff8e1;
            border: 1px solid #ffecb3;
            color: #f57c00;
        }
        
        .alert-info {
            background: #e3f2fd;
            border: 1px solid #bbdefb;
            color: #1976d2;
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
        
        .data-table tr:hover {
            background: #f8f9fa;
        }
        
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
        
        .dashboard-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }
        
        .metric-card {
            background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
            border-radius: 10px;
            padding: 20px;
            text-align: center;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
        }
        
        .metric-value {
            font-size: 2rem;
            font-weight: bold;
            color: #667eea;
            margin-bottom: 5px;
        }
        
        .metric-label {
            color: #666;
            font-weight: 600;
        }
        
        .note-classes-display {
            background: #f8f9fa;
            border-radius: 8px;
            padding: 10px;
            margin-top: 10px;
            font-size: 12px;
        }
        
        .confidence-badge {
            display: inline-block;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: bold;
            color: white;
        }
        
        .confidence-high { background: #4caf50; }
        .confidence-medium { background: #ff9800; }
        .confidence-low { background: #f44336; }
        
        .help-text {
            font-size: 12px;
            color: #666;
            margin-top: 5px;
            font-style: italic;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏦 ABS Analysis System</h1>
            <p>Complete Platform - Document Processing & Deal Management</p>
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
                <span id="dealCount">0 deals loaded</span>
            </div>
        </div>
        
        <div class="tabs">
            <button class="tab-button active" onclick="showTab('document-processing')">📄 Document Processing</button>
            <button class="tab-button" onclick="showTab('manual-entry')">✍️ Manual Entry</button>
            <button class="tab-button" onclick="showTab('analytics')">📊 Analytics</button>
            <button class="tab-button" onclick="showTab('dashboard')">📈 Dashboard</button>
        </div>
        
        <!-- Document Processing Tab -->
        <div id="document-processing" class="tab-content active">
            <h2>📄 Automatic Document Processing</h2>
            <p>Process New Issue Reports automatically using AI extraction. Data will be saved to your database.</p>
            
            <div class="processing-section">
                <h3>📁 Process Entire Folder</h3>
                <p>Enter the full path to a folder containing PDF or Word documents to process all files at once.</p>
                
                <div class="form-group">
                    <label for="folderPath">Folder Path</label>
                    <input type="text" id="folderPath" class="path-input" 
                           placeholder="e.g., C:\\Users\\YourName\\Documents\\New_Issue_Reports"
                           value="C:\\Users\\edfit\\OneDrive - Whitehall Partners\\Data Extraction\\New_Issue_Reports">
                    <div class="help-text">
                        💡 Tip: Copy the folder path from Windows Explorer address bar
                    </div>
                </div>
                
                <button class="btn btn-success" onclick="processFolderDocuments()">
                    🚀 Process All Documents in Folder
                </button>
            </div>
            
            <div class="processing-section">
                <h3>📄 Process Single File</h3>
                <p>Enter the full path to a specific PDF or Word document to process just that file.</p>
                
                <div class="form-group">
                    <label for="filePath">File Path</label>
                    <input type="text" id="filePath" class="path-input" 
                           placeholder="e.g., C:\\Users\\YourName\\Documents\\Sample_Report.pdf">
                    <div class="help-text">
                        💡 Tip: Right-click on file → Properties to get the full path
                    </div>
                </div>
                
                <button class="btn btn-info" onclick="processSingleFile()">
                    📊 Process Single Document
                </button>
            </div>
            
            <div id="processingAlerts"></div>
            
            <div id="processingResults" style="display: none;">
                <h3>📊 Processing Results</h3>
                <div id="processingResultsContent"></div>
            </div>
        </div>
        
        <!-- Manual Entry Tab -->
        <div id="manual-entry" class="tab-content">
            <h2>✍️ Manual Deal Entry</h2>
            <p>Enter deal information manually. All fields will be saved to your database.</p>
            
            <form id="dealForm">
                <div class="form-grid">
                    <div class="form-group">
                        <label>Deal Name *</label>
                        <input type="text" id="dealName" required placeholder="e.g., Sample ABS Deal 2025-1">
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
                            <option value="Auto Loans">Auto Loans</option>
                            <option value="Student Loans">Student Loans</option>
                            <option value="Marketplace Lending">Marketplace Lending</option>
                            <option value="Other">Other</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label>Total Deal Size ($MM) *</label>
                        <input type="number" id="dealSize" step="0.01" required placeholder="e.g., 250.5">
                    </div>
                    <div class="form-group">
                        <label>Original Pool Balance ($MM)</label>
                        <input type="number" id="originalPoolBalance" step="0.01" placeholder="e.g., 280.0">
                        <div class="help-text">Usually larger than deal size</div>
                    </div>
                    <div class="form-group">
                        <label>Class A Advance Rate (%) *</label>
                        <input type="number" id="classAAdvanceRate" step="0.01" max="100" required placeholder="e.g., 75.5">
                    </div>
                    <div class="form-group">
                        <label>Initial OC (%) *</label>
                        <input type="number" id="initialOC" step="0.01" required placeholder="e.g., 12.5">
                    </div>
                    <div class="form-group">
                        <label>Expected CNL Low (%) *</label>
                        <input type="number" id="expectedCNLLow" step="0.01" required placeholder="e.g., 2.8">
                    </div>
                    <div class="form-group">
                        <label>Expected CNL High (%)</label>
                        <input type="number" id="expectedCNLHigh" step="0.01" placeholder="e.g., 3.2">
                    </div>
                    <div class="form-group">
                        <label>Reserve Account (%) *</label>
                        <input type="number" id="reserveAccount" step="0.01" required placeholder="e.g., 1.5">
                    </div>
                    <div class="form-group">
                        <label>Weighted Avg Seasoning (months) *</label>
                        <input type="number" id="avgSeasoning" required placeholder="e.g., 18">
                    </div>
                    <div class="form-group">
                        <label>Top Obligor Concentration (%) *</label>
                        <input type="number" id="topObligorConc" step="0.01" max="100" required placeholder="e.g., 1.2">
                    </div>
                </div>
                
                <button type="submit" class="btn btn-success">💾 Save Deal to Database</button>
                <button type="button" class="btn" onclick="clearForm()">🔄 Clear Form</button>
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
                            <th>Pool Balance ($MM)</th>
                            <th>Advance Rate</th>
                            <th>Source</th>
                            <th>Confidence</th>
                            <th>Note Classes</th>
                            <th>Date Added</th>
                        </tr>
                    </thead>
                    <tbody id="dealsTableBody">
                    </tbody>
                </table>
            </div>
        </div>
        
        <!-- Analytics Tab -->
        <div id="analytics" class="tab-content">
            <h2>📊 Risk Analysis & Stress Testing</h2>
            
            <div style="background: #ffebee; padding: 20px; border-radius: 10px; margin-bottom: 30px;">
                <h3>💥 Portfolio Stress Testing</h3>
                <p>Test your entire portfolio under different economic scenarios.</p>
                
                <div class="form-grid">
                    <div class="form-group">
                        <label>Stress Scenario</label>
                        <select id="stressScenario">
                            <option value="mild">Mild Stress (Loss +20%)</option>
                            <option value="moderate">Moderate Stress (Loss +50%)</option>
                            <option value="severe">Severe Stress (Loss +100%)</option>
                        </select>
                    </div>
                </div>
                
                <button class="btn" onclick="runStressTest()">🧪 Run Stress Test</button>
                
                <div id="stressResults"></div>
            </div>
        </div>
        
        <!-- Dashboard Tab -->
        <div id="dashboard" class="tab-content">
            <h2>📈 Portfolio Dashboard</h2>
            
            <div class="dashboard-grid">
                <div class="metric-card">
                    <div class="metric-value" id="totalDeals">0</div>
                    <div class="metric-label">Total Deals</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value" id="totalVolume">$0M</div>
                    <div class="metric-label">Total Volume</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value" id="totalPoolBalance">$0M</div>
                    <div class="metric-label">Total Pool Balance</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value" id="avgAdvanceRate">0%</div>
                    <div class="metric-label">Avg Advance Rate</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value" id="avgOC">0%</div>
                    <div class="metric-label">Avg Initial OC</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value" id="totalNoteClasses">0</div>
                    <div class="metric-label">Total Note Classes</div>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Global variables
        let allDeals = [];
        let systemStatus = {
            database: false,
            parser: false
        };
        
        // Initialize system
        document.addEventListener('DOMContentLoaded', function() {
            checkSystemStatus();
            loadDeals();
        });
        
        async function checkSystemStatus() {
            try {
                const response = await fetch('/api/database-status');
                const status = await response.json();
                
                systemStatus.database = status.connected;
                systemStatus.parser = status.parser_available;
                
                // Update status indicators
                const dbIndicator = document.getElementById('dbIndicator');
                const dbStatus = document.getElementById('dbStatus');
                const parserIndicator = document.getElementById('parserIndicator');
                const parserStatus = document.getElementById('parserStatus');
                const dealCount = document.getElementById('dealCount');
                
                if (status.connected) {
                    dbIndicator.classList.remove('error');
                    dbStatus.textContent = `Database Connected (${status.deals_count} deals)`;
                    dealCount.textContent = `${status.deals_count} deals, ${status.note_classes_count} note classes`;
                } else {
                    dbIndicator.classList.add('error');
                    dbStatus.textContent = 'Database Connection Failed';
                    dealCount.textContent = 'No data available';
                }
                
                if (status.parser_available) {
                    parserIndicator.classList.remove('error');
                    parserStatus.textContent = 'Document Parser Ready';
                } else {
                    parserIndicator.classList.add('error');
                    parserStatus.textContent = 'Document Parser Unavailable';
                }
                
            } catch (error) {
                console.error('Status check failed:', error);
                document.getElementById('dbIndicator').classList.add('error');
                document.getElementById('dbStatus').textContent = 'Status Check Failed';
                document.getElementById('parserIndicator').classList.add('error');
                document.getElementById('parserStatus').textContent = 'Status Check Failed';
            }
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
            
            if (tabName === 'dashboard') {
                loadDashboard();
            }
        }
        
        async function processFolderDocuments() {
            const folderPath = document.getElementById('folderPath').value.trim();
            
            if (!folderPath) {
                showAlert('Please enter a folder path', 'error', 'processingAlerts');
                return;
            }
            
            if (!systemStatus.parser) {
                showAlert('Document parser is not available. Please check system status.', 'error', 'processingAlerts');
                return;
            }
            
            showAlert('Processing folder... This may take several minutes.', 'info', 'processingAlerts');
            
            try {
                const response = await fetch('/api/process-folder', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({ folderPath })
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showAlert(`✅ Successfully processed ${result.successful} files! ${result.failed} failed.`, 'success', 'processingAlerts');
                    
                    if (result.failed > 0) {
                        showAlert(`Failed files: ${result.failedFiles.join(', ')}`, 'warning', 'processingAlerts');
                    }
                    
                    // Refresh deals list
                    loadDeals();
                    checkSystemStatus();
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
            
            if (!systemStatus.parser) {
                showAlert('Document parser is not available. Please check system status.', 'error', 'processingAlerts');
                return;
            }
            
            showAlert('Processing file...', 'info', 'processingAlerts');
            
            try {
                const response = await fetch('/api/process-file', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({ filePath })
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showAlert(`✅ Successfully processed: ${result.dealName} (Confidence: ${result.confidenceScore}%, Note Classes: ${result.noteClasses})`, 'success', 'processingAlerts');
                    
                    // Clear the file path
                    document.getElementById('filePath').value = '';
                    
                    // Refresh deals list
                    loadDeals();
                    checkSystemStatus();
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
                                 parseFloat(document.getElementById('expectedCNLLow').value) + 0.5,
                reserveAccount: parseFloat(document.getElementById('reserveAccount').value),
                avgSeasoning: parseInt(document.getElementById('avgSeasoning').value),
                topObligorConc: parseFloat(document.getElementById('topObligorConc').value)
            };
            
            try {
                const response = await fetch('/api/deals', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(dealData)
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showAlert('Deal added successfully! 🎉', 'success', 'dealAlerts');
                    clearForm();
                    loadDeals();
                    checkSystemStatus();
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
            document.getElementById('loadingDeals').style.display = 'block';
            document.getElementById('dealsTable').style.display = 'none';
            
            try {
                const response = await fetch('/api/deals');
                allDeals = await response.json();
                
                const tbody = document.getElementById('dealsTableBody');
                tbody.innerHTML = '';
                
                allDeals.forEach(deal => {
                    const row = tbody.insertRow();
                    
                    // Confidence badge
                    let confidenceBadge = '';
                    if (deal.confidenceScore >= 80) {
                        confidenceBadge = `<span class="confidence-badge confidence-high">${deal.confidenceScore}%</span>`;
                    } else if (deal.confidenceScore >= 60) {
                        confidenceBadge = `<span class="confidence-badge confidence-medium">${deal.confidenceScore}%</span>`;
                    } else {
                        confidenceBadge = `<span class="confidence-badge confidence-low">${deal.confidenceScore}%</span>`;
                    }
                    
                    // Note classes summary
                    let noteClassesDisplay = `${deal.noteClasses.length} classes`;
                    if (deal.noteClasses.length > 0) {
                        const classNames = deal.noteClasses.map(nc => nc.class).join(', ');
                        noteClassesDisplay += `<div class="note-classes-display">Classes: ${classNames}</div>`;
                    }
                    
                    row.innerHTML = `
                        <td><strong>${deal.dealName}</strong></td>
                        <td>${deal.sector}</td>
                        <td>$${deal.dealSize.toFixed(0)}M</td>
                        <td>$${deal.originalPoolBalance.toFixed(0)}M</td>
                        <td>${deal.classAAdvanceRate.toFixed(1)}%</td>
                        <td>${deal.sourceFile || 'Manual Entry'}</td>
                        <td>${confidenceBadge}</td>
                        <td>${noteClassesDisplay}</td>
                        <td>${deal.createdDate}</td>
                    `;
                });
                
                document.getElementById('loadingDeals').style.display = 'none';
                document.getElementById('dealsTable').style.display = 'table';
                
            } catch (error) {
                showAlert('Error loading deals: ' + error.message, 'error', 'dealAlerts');
                document.getElementById('loadingDeals').style.display = 'none';
            }
        }
        
        async function runStressTest() {
            const scenario = document.getElementById('stressScenario').value;
            
            try {
                const response = await fetch('/api/stress-test', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({ scenario })
                });
                
                const results = await response.json();
                displayStressResults(results);
                
            } catch (error) {
                showAlert('Error running stress test: ' + error.message, 'error');
            }
        }
        
        function displayStressResults(results) {
            if (results.error) {
                showAlert('Error: ' + results.error, 'error');
                return;
            }
            
            const summary = results.summary;
            
            let html = `
                <h4>Stress Test Results - ${results.scenario.toUpperCase()}</h4>
                <div class="dashboard-grid">
                    <div class="metric-card">
                        <div class="metric-value">${summary.critical}</div>
                        <div class="metric-label">Critical Deals</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">${summary.total_at_risk}</div>
                        <div class="metric-label">Total at Risk</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">$${summary.volume_at_risk}M</div>
                        <div class="metric-label">Volume at Risk</div>
                    </div>
                </div>
            `;
            
            if (results.results && results.results.length > 0) {
                html += `
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>Deal Name</th>
                                <th>Stressed Loss</th>
                                <th>Adequacy Ratio</th>
                                <th>Status</th>
                            </tr>
                        </thead>
                        <tbody>
                `;
                
                results.results.forEach(deal => {
                    const statusClass = deal.status.toLowerCase();
                    html += `
                        <tr>
                            <td>${deal.dealName}</td>
                            <td>${deal.stressedLoss}%</td>
                            <td>${deal.adequacyRatio}</td>
                            <td><span class="confidence-badge confidence-${statusClass === 'strong' ? 'high' : statusClass === 'critical' ? 'low' : 'medium'}">${deal.status}</span></td>
                        </tr>
                    `;
                });
                
                html += '</tbody></table>';
            }
            
            document.getElementById('stressResults').innerHTML = html;
        }
        
        async function loadDashboard() {
            try {
                if (allDeals.length === 0) {
                    await loadDeals();
                }
                
                if (allDeals.length === 0) {
                    document.getElementById('totalDeals').textContent = '0';
                    document.getElementById('totalVolume').textContent = '$0M';
                    document.getElementById('totalPoolBalance').textContent = '$0M';
                    document.getElementById('avgAdvanceRate').textContent = '0%';
                    document.getElementById('avgOC').textContent = '0%';
                    document.getElementById('totalNoteClasses').textContent = '0';
                    return;
                }
                
                const totalVolume = allDeals.reduce((sum, deal) => sum + deal.dealSize, 0);
                const totalPoolBalance = allDeals.reduce((sum, deal) => sum + deal.originalPoolBalance, 0);
                const avgAdvanceRate = allDeals.reduce((sum, deal) => sum + deal.classAAdvanceRate, 0) / allDeals.length;
                const avgOC = allDeals.reduce((sum, deal) => sum + deal.initialOC, 0) / allDeals.length;
                const totalNoteClasses = allDeals.reduce((sum, deal) => sum + deal.noteClasses.length, 0);
                
                document.getElementById('totalDeals').textContent = allDeals.length;
                document.getElementById('totalVolume').textContent = `$${totalVolume.toFixed(0)}M`;
                document.getElementById('totalPoolBalance').textContent = `$${totalPoolBalance.toFixed(0)}M`;
                document.getElementById('avgAdvanceRate').textContent = `${avgAdvanceRate.toFixed(1)}%`;
                document.getElementById('avgOC').textContent = `${avgOC.toFixed(1)}%`;
                document.getElementById('totalNoteClasses').textContent = totalNoteClasses;
                
            } catch (error) {
                showAlert('Error loading dashboard: ' + error.message, 'error');
            }
        }
        
        function showAlert(message, type, containerId = 'dealAlerts') {
            const alertDiv = document.createElement('div');
            alertDiv.className = `alert alert-${type}`;
            alertDiv.textContent = message;
            
            const container = document.getElementById(containerId);
            container.appendChild(alertDiv);
            
            setTimeout(() => {
                alertDiv.remove();
            }, 10000);
        }
    </script>
</body>
</html>
        '''
    
    def run(self, host='127.0.0.1', port=5000, debug=False):
        """Start the complete ABS system"""
        print(f"🚀 Starting Complete ABS Analysis System")
        print(f"🌐 Web Interface: http://{host}:{port}")
        print(f"📊 Database: {os.path.basename(self.db_path)}")
        print(f"📄 Document Parser: {'✅ Available' if PARSER_AVAILABLE else '❌ Not Available'}")
        print("🛑 Press Ctrl+C to stop the server")
        print("=" * 60)
        
        if not debug:
            threading.Timer(1.5, lambda: webbrowser.open(f'http://{host}:{port}')).start()
        
        self.app.run(host=host, port=port, debug=debug)

def command_line_interface():
    """Command line interface for the ABS system"""
    parser = argparse.ArgumentParser(description='ABS Analysis System - Complete Platform')
    parser.add_argument('--web', action='store_true', help='Start web interface (default)')
    parser.add_argument('--process-folder', type=str, help='Process documents in folder')
    parser.add_argument('--process-file', type=str, help='Process single document file')
    parser.add_argument('--db', type=str, help='Database path', 
                       default=r"C:\Users\edfit\OneDrive\Documents\ABS_Performance_Data.db.accdb")
    parser.add_argument('--port', type=int, default=5000, help='Web server port')
    parser.add_argument('--host', type=str, default='127.0.0.1', help='Web server host')
    
    args = parser.parse_args()
    
    print("🏦 ABS Analysis System - Complete Platform")
    print("=" * 50)
    
    if args.process_folder:
        # Command line folder processing
        if not PARSER_AVAILABLE:
            print("❌ Document parser not available")
            return
        
        print(f"📁 Processing folder: {args.process_folder}")
        extractor = initialize_extractor(args.db)
        results = process_folder_enhanced(args.process_folder, export_excel=True, save_to_db=True)
        
        successful = len([r for r in results if 'error' not in r])
        failed = len([r for r in results if 'error' in r])
        
        print(f"✅ Completed: {successful} successful, {failed} failed")
        
    elif args.process_file:
        # Command line single file processing
        if not PARSER_AVAILABLE:
            print("❌ Document parser not available")
            return
        
        print(f"📄 Processing file: {args.process_file}")
        extractor = initialize_extractor(args.db)
        result = extract_and_display_enhanced(args.process_file, save_to_db=True)
        
        if 'error' not in result:
            print("✅ Processing completed successfully")
        else:
            print(f"❌ Processing failed: {result['error']}")
    
    else:
        # Start web interface (default)
        system = CompleteABSSystem(args.db)
        system.run(host=args.host, port=args.port, debug=False)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        command_line_interface()
    else:
        # Default: start web interface
        db_path = r"C:\Users\edfit\OneDrive\Documents\ABS_Performance_Data.db.accdb"
        system = CompleteABSSystem(db_path)
        system.run()
# -*- coding: utf-8 -*-
"""
Created on Sun Jul 13 12:31:08 2025

@author: edfit
"""

# -*- coding: utf-8 -*-
"""
Minimal ABS Analysis System - Guaranteed Working Version
Bypasses pandas/numpy compatibility issues
"""

import sys
import os
import warnings
warnings.filterwarnings('ignore')

# Import basic modules first
from flask import Flask, render_template_string, request, jsonify
import json
from datetime import datetime
import threading
import webbrowser
import argparse

print("🔧 Starting Minimal ABS System...")

# Try to import pandas with fallback
try:
    import pandas as pd
    PANDAS_WORKING = True
    print("✅ Pandas working")
except Exception as e:
    print(f"⚠️  Pandas issue: {e}")
    PANDAS_WORKING = False

# Try to import database functionality
try:
    import pyodbc
    PYODBC_AVAILABLE = True
    print("✅ Database functionality available")
except ImportError:
    PYODBC_AVAILABLE = False
    print("⚠️  Database not available - install pyodbc")

# Try to import CORS
try:
    from flask_cors import CORS
    CORS_AVAILABLE = True
except ImportError:
    CORS_AVAILABLE = False
    print("⚠️  CORS not available")

# Try to import document parser
try:
    from enhanced_document_parser_with_db import (
        initialize_extractor, 
        extract_and_display_enhanced, 
        process_folder_enhanced
    )
    PARSER_AVAILABLE = True
    print("✅ Document parser available")
except ImportError as e:
    PARSER_AVAILABLE = False
    print(f"⚠️  Document parser not available: {e}")

class MinimalABSSystem:
    """Minimal ABS System that works despite dependency issues"""
    
    def __init__(self, db_path):
        self.db_path = db_path
        self.app = Flask(__name__)
        
        if CORS_AVAILABLE:
            CORS(self.app)
        
        self.app.secret_key = 'abs_minimal_system_2025'
        
        # Initialize components
        self.extractor = None
        self.connection_string = None
        
        if PARSER_AVAILABLE:
            try:
                self.extractor = initialize_extractor(db_path)
                print("✅ Document parser initialized")
            except Exception as e:
                print(f"⚠️  Parser init failed: {e}")
        
        if PYODBC_AVAILABLE:
            self.connection_string = self._setup_database_connection()
        
        self.setup_routes()
    
    def _setup_database_connection(self):
        """Setup database connection"""
        if not os.path.exists(self.db_path):
            print(f"❌ Database file not found: {self.db_path}")
            return None
        
        drivers = [
            'Microsoft Access Driver (*.mdb, *.accdb)',
            'Microsoft Office 16.0 Access Database Engine OLE DB Provider'
        ]
        
        for driver in drivers:
            try:
                conn_str = f'DRIVER={{{driver}}};DBQ={self.db_path};'
                conn = pyodbc.connect(conn_str)
                conn.close()
                print(f"✅ Database connected using: {driver}")
                return conn_str
            except Exception:
                continue
        
        print("❌ No database driver found")
        return None
    
    def get_db_connection(self):
        """Get database connection"""
        if not self.connection_string:
            return None
        try:
            return pyodbc.connect(self.connection_string)
        except Exception as e:
            print(f"Database error: {e}")
            return None
    
    def setup_routes(self):
        """Setup web routes"""
        
        @self.app.route('/')
        def index():
            return render_template_string(self.get_minimal_html())
        
        @self.app.route('/api/status')
        def system_status():
            return jsonify({
                'database_available': self.connection_string is not None,
                'parser_available': PARSER_AVAILABLE,
                'pandas_working': PANDAS_WORKING
            })
        
        @self.app.route('/api/deals', methods=['GET'])
        def get_deals():
            if not self.connection_string:
                return jsonify([])
            
            conn = self.get_db_connection()
            if not conn:
                return jsonify([])
            
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT ID, DealName, IssueDate, RatingAgency, Sector, DealSize, 
                           ClassAAdvanceRate, InitialOC, ExpectedCNLLow, 
                           ReserveAccount, OriginalPoolBalance, CreatedDate, SourceFile
                    FROM ABS_Deals ORDER BY CreatedDate DESC
                """)
                
                deals = []
                for row in cursor.fetchall():
                    deals.append({
                        'id': row[0],
                        'dealName': row[1] or '',
                        'issueDate': row[2].strftime('%Y-%m-%d') if row[2] else '',
                        'ratingAgency': row[3] or '',
                        'sector': row[4] or '',
                        'dealSize': float(row[5]) if row[5] else 0,
                        'classAAdvanceRate': float(row[6]) if row[6] else 0,
                        'initialOC': float(row[7]) if row[7] else 0,
                        'expectedCNLLow': float(row[8]) if row[8] else 0,
                        'reserveAccount': float(row[9]) if row[9] else 0,
                        'originalPoolBalance': float(row[10]) if row[10] else 0,
                        'createdDate': row[11].strftime('%Y-%m-%d %H:%M') if row[11] else '',
                        'sourceFile': row[12] or 'Manual'
                    })
                
                conn.close()
                return jsonify(deals)
                
            except Exception as e:
                print(f"Error getting deals: {e}")
                conn.close()
                return jsonify([])
        
        @self.app.route('/api/deals', methods=['POST'])
        def add_deal():
            if not self.connection_string:
                return jsonify({'success': False, 'message': 'Database not available'})
            
            try:
                deal_data = request.json
                conn = self.get_db_connection()
                if not conn:
                    return jsonify({'success': False, 'message': 'Database connection failed'})
                
                cursor = conn.cursor()
                
                # Parse date
                issue_date = None
                if deal_data.get('issueDate'):
                    try:
                        issue_date = datetime.strptime(deal_data['issueDate'], '%Y-%m-%d')
                    except:
                        pass
                
                # Insert deal
                cursor.execute('''
                    INSERT INTO ABS_Deals 
                    (DealName, IssueDate, RatingAgency, Sector, DealSize, 
                     ClassAAdvanceRate, InitialOC, ExpectedCNLLow, ExpectedCNLHigh,
                     ReserveAccount, AvgSeasoning, TopObligorConc, OriginalPoolBalance,
                     CreatedDate, SourceFile, ConfidenceScore)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
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
                    100
                ))
                
                cursor.execute("SELECT @@IDENTITY")
                deal_id = cursor.fetchone()[0]
                
                conn.commit()
                conn.close()
                
                return jsonify({'success': True, 'id': deal_id})
                
            except Exception as e:
                return jsonify({'success': False, 'message': str(e)})
        
        @self.app.route('/api/process-folder', methods=['POST'])
        def process_folder():
            if not PARSER_AVAILABLE:
                return jsonify({'success': False, 'message': 'Parser not available'})
            
            try:
                data = request.json
                folder_path = data.get('folderPath', '').strip()
                
                if not folder_path or not os.path.exists(folder_path):
                    return jsonify({'success': False, 'message': 'Invalid folder path'})
                
                results = process_folder_enhanced(folder_path, True, True)
                successful = len([r for r in results if 'error' not in r])
                failed = len([r for r in results if 'error' in r])
                
                return jsonify({
                    'success': True,
                    'successful': successful,
                    'failed': failed
                })
                
            except Exception as e:
                return jsonify({'success': False, 'message': str(e)})
        
        @self.app.route('/api/process-file', methods=['POST'])
        def process_file():
            if not PARSER_AVAILABLE:
                return jsonify({'success': False, 'message': 'Parser not available'})
            
            try:
                data = request.json
                file_path = data.get('filePath', '').strip()
                
                if not file_path or not os.path.exists(file_path):
                    return jsonify({'success': False, 'message': 'Invalid file path'})
                
                result = extract_and_display_enhanced(file_path, True)
                
                if 'error' in result:
                    return jsonify({'success': False, 'message': result['error']})
                
                return jsonify({
                    'success': True,
                    'dealName': result.get('deal_name', 'Unknown'),
                    'confidenceScore': result.get('confidence_score', 0)
                })
                
            except Exception as e:
                return jsonify({'success': False, 'message': str(e)})
    
    def get_minimal_html(self):
        """Return minimal HTML that works without complex dependencies"""
        return '''
<!DOCTYPE html>
<html>
<head>
    <title>ABS Analysis System</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { text-align: center; background: #667eea; color: white; padding: 20px; border-radius: 10px; margin-bottom: 20px; }
        .status { background: white; padding: 15px; border-radius: 8px; margin-bottom: 20px; }
        .tabs { display: flex; background: white; border-radius: 8px; overflow: hidden; margin-bottom: 20px; }
        .tab { flex: 1; padding: 15px; text-align: center; cursor: pointer; border: none; background: #f8f9fa; }
        .tab.active { background: #667eea; color: white; }
        .tab-content { display: none; background: white; padding: 20px; border-radius: 8px; }
        .tab-content.active { display: block; }
        .form-group { margin-bottom: 15px; }
        .form-group label { display: block; margin-bottom: 5px; font-weight: bold; }
        .form-group input, .form-group select { width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px; }
        .form-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .btn { background: #667eea; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; margin: 5px; }
        .btn:hover { background: #5a67d8; }
        .btn-success { background: #48bb78; }
        .btn-success:hover { background: #38a169; }
        .alert { padding: 10px; margin: 10px 0; border-radius: 4px; }
        .alert-success { background: #f0fff4; border: 1px solid #9ae6b4; color: #22543d; }
        .alert-error { background: #fed7d7; border: 1px solid #feb2b2; color: #742a2a; }
        .alert-info { background: #ebf8ff; border: 1px solid #90cdf4; color: #2a4365; }
        .section { background: #f8f9fa; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
        .section h3 { margin-top: 0; color: #2d3748; }
        .table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        .table th, .table td { padding: 10px; text-align: left; border-bottom: 1px solid #e2e8f0; }
        .table th { background: #f7fafc; font-weight: bold; }
        .path-input { width: 100%; margin-bottom: 10px; }
        .status-indicator { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 5px; }
        .status-ok { background: #48bb78; }
        .status-error { background: #f56565; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏦 ABS Analysis System</h1>
            <p>Minimal Version - Document Processing & Deal Management</p>
        </div>
        
        <div class="status" id="statusBar">
            <h3>System Status</h3>
            <p><span class="status-indicator" id="dbIndicator"></span><span id="dbStatus">Checking...</span></p>
            <p><span class="status-indicator" id="parserIndicator"></span><span id="parserStatus">Checking...</span></p>
            <p><span id="dealCount">Loading deals...</span></p>
        </div>
        
        <div class="tabs">
            <button class="tab active" onclick="showTab('processing')">📄 Document Processing</button>
            <button class="tab" onclick="showTab('manual')">✍️ Manual Entry</button>
            <button class="tab" onclick="showTab('deals')">📋 View Deals</button>
        </div>
        
        <!-- Document Processing -->
        <div id="processing" class="tab-content active">
            <h2>Document Processing</h2>
            
            <div class="section">
                <h3>📁 Process Folder</h3>
                <p>Enter the full path to a folder containing New Issue Reports:</p>
                <input type="text" id="folderPath" class="path-input" 
                       placeholder="C:\\Users\\YourName\\Documents\\Reports"
                       value="C:\\Users\\edfit\\OneDrive - Whitehall Partners\\Data Extraction\\New_Issue_Reports">
                <button class="btn btn-success" onclick="processFolder()">Process All Files</button>
            </div>
            
            <div class="section">
                <h3>📄 Process Single File</h3>
                <p>Enter the full path to a specific PDF or Word document:</p>
                <input type="text" id="filePath" class="path-input" 
                       placeholder="C:\\Users\\YourName\\Documents\\Report.pdf">
                <button class="btn" onclick="processFile()">Process File</button>
            </div>
            
            <div id="processingAlerts"></div>
        </div>
        
        <!-- Manual Entry -->
        <div id="manual" class="tab-content">
            <h2>Manual Deal Entry</h2>
            
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
                <button type="button" class="btn" onclick="clearForm()">🔄 Clear Form</button>
            </form>
            
            <div id="dealAlerts"></div>
        </div>
        
        <!-- View Deals -->
        <div id="deals" class="tab-content">
            <h2>Recent Deals</h2>
            <div id="dealsContainer">
                <p>Loading deals...</p>
            </div>
        </div>
    </div>

    <script>
        let systemStatus = {};
        
        // Initialize
        document.addEventListener('DOMContentLoaded', function() {
            checkStatus();
            loadDeals();
        });
        
        async function checkStatus() {
            try {
                const response = await fetch('/api/status');
                systemStatus = await response.json();
                
                // Update indicators
                document.getElementById('dbIndicator').className = 
                    'status-indicator ' + (systemStatus.database_available ? 'status-ok' : 'status-error');
                document.getElementById('dbStatus').textContent = 
                    systemStatus.database_available ? 'Database Connected' : 'Database Not Available';
                
                document.getElementById('parserIndicator').className = 
                    'status-indicator ' + (systemStatus.parser_available ? 'status-ok' : 'status-error');
                document.getElementById('parserStatus').textContent = 
                    systemStatus.parser_available ? 'Document Parser Ready' : 'Parser Not Available';
                
            } catch (error) {
                console.error('Status check failed:', error);
            }
        }
        
        function showTab(tabName) {
            // Hide all tabs
            document.querySelectorAll('.tab-content').forEach(content => {
                content.classList.remove('active');
            });
            document.querySelectorAll('.tab').forEach(tab => {
                tab.classList.remove('active');
            });
            
            // Show selected tab
            document.getElementById(tabName).classList.add('active');
            event.target.classList.add('active');
            
            if (tabName === 'deals') {
                loadDeals();
            }
        }
        
        async function processFolder() {
            const folderPath = document.getElementById('folderPath').value.trim();
            
            if (!folderPath) {
                showAlert('Please enter a folder path', 'error', 'processingAlerts');
                return;
            }
            
            if (!systemStatus.parser_available) {
                showAlert('Document parser not available', 'error', 'processingAlerts');
                return;
            }
            
            showAlert('Processing folder...', 'info', 'processingAlerts');
            
            try {
                const response = await fetch('/api/process-folder', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ folderPath })
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showAlert(`✅ Processed ${result.successful} files successfully, ${result.failed} failed`, 'success', 'processingAlerts');
                    loadDeals();
                } else {
                    showAlert('Error: ' + result.message, 'error', 'processingAlerts');
                }
                
            } catch (error) {
                showAlert('Error: ' + error.message, 'error', 'processingAlerts');
            }
        }
        
        async function processFile() {
            const filePath = document.getElementById('filePath').value.trim();
            
            if (!filePath) {
                showAlert('Please enter a file path', 'error', 'processingAlerts');
                return;
            }
            
            if (!systemStatus.parser_available) {
                showAlert('Document parser not available', 'error', 'processingAlerts');
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
                    showAlert(`✅ Processed: ${result.dealName} (${result.confidenceScore}% confidence)`, 'success', 'processingAlerts');
                    document.getElementById('filePath').value = '';
                    loadDeals();
                } else {
                    showAlert('Error: ' + result.message, 'error', 'processingAlerts');
                }
                
            } catch (error) {
                showAlert('Error: ' + error.message, 'error', 'processingAlerts');
            }
        }
        
        // Deal form submission
        document.getElementById('dealForm').addEventListener('submit', async function(e) {
            e.preventDefault();
            
            if (!systemStatus.database_available) {
                showAlert('Database not available', 'error', 'dealAlerts');
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
                document.getElementById('dealsContainer').innerHTML = '<p>Database not available</p>';
                return;
            }
            
            try {
                const response = await fetch('/api/deals');
                const deals = await response.json();
                
                document.getElementById('dealCount').textContent = `${deals.length} deals in database`;
                
                if (deals.length === 0) {
                    document.getElementById('dealsContainer').innerHTML = '<p>No deals found</p>';
                    return;
                }
                
                let html = '<table class="table"><thead><tr>' +
                    '<th>Deal Name</th><th>Sector</th><th>Size ($MM)</th><th>Pool Balance ($MM)</th>' +
                    '<th>Advance Rate</th><th>Source</th><th>Date Added</th></tr></thead><tbody>';
                
                deals.forEach(deal => {
                    html += `<tr>
                        <td><strong>${deal.dealName}</strong></td>
                        <td>${deal.sector}</td>
                        <td>$${deal.dealSize.toFixed(0)}M</td>
                        <td>$${deal.originalPoolBalance.toFixed(0)}M</td>
                        <td>${deal.classAAdvanceRate.toFixed(1)}%</td>
                        <td>${deal.sourceFile}</td>
                        <td>${deal.createdDate}</td>
                    </tr>`;
                });
                
                html += '</tbody></table>';
                document.getElementById('dealsContainer').innerHTML = html;
                
            } catch (error) {
                document.getElementById('dealsContainer').innerHTML = '<p>Error loading deals</p>';
            }
        }
        
        function showAlert(message, type, containerId) {
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
    
    def run(self, host='127.0.0.1', port=5000):
        """Start the minimal system"""
        print(f"🚀 Starting Minimal ABS System")
        print(f"🌐 URL: http://{host}:{port}")
        print(f"📊 Database: {'✅' if self.connection_string else '❌'}")
        print(f"📄 Parser: {'✅' if PARSER_AVAILABLE else '❌'}")
        print("🛑 Press Ctrl+C to stop")
        print("=" * 40)
        
        threading.Timer(1.5, lambda: webbrowser.open(f'http://{host}:{port}')).start()
        self.app.run(host=host, port=port, debug=False)

def main():
    """Main function"""
    print("🏦 Minimal ABS Analysis System")
    print("=" * 30)
    
    # Command line arguments
    if len(sys.argv) > 1:
        parser = argparse.ArgumentParser()
        parser.add_argument('--process-folder', help='Process folder')
        parser.add_argument('--process-file', help='Process single file')
        parser.add_argument('--db', default=r"C:\Users\edfit\OneDrive\Documents\ABS_Performance_Data.db.accdb")
        parser.add_argument('--port', type=int, default=5000)
        parser.add_argument('--host', default='127.0.0.1')
        
        args = parser.parse_args()
        
        if args.process_folder and PARSER_AVAILABLE:
            print(f"📁 Processing: {args.process_folder}")
            extractor = initialize_extractor(args.db)
            results = process_folder_enhanced(args.process_folder, True, True)
            print("✅ Done")
            return
        elif args.process_file and PARSER_AVAILABLE:
            print(f"📄 Processing: {args.process_file}")
            extractor = initialize_extractor(args.db)
            result = extract_and_display_enhanced(args.process_file, True)
            print("✅ Done")
            return
        else:
            system = MinimalABSSystem(args.db)
            system.run(args.host, args.port)
    else:
        # Default web interface
        db_path = r"C:\Users\edfit\OneDrive\Documents\ABS_Performance_Data.db.accdb"
        system = MinimalABSSystem(db_path)
        system.run()

if __name__ == "__main__":
    main()
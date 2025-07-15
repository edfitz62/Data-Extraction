# -*- coding: utf-8 -*-
"""
Created on Sat Jul 12 14:49:32 2025

@author: edfit
"""

"""
Complete Web UI for ABS Analysis Platform
Connects to Access database and provides full functionality
"""

from flask import Flask, render_template_string, request, jsonify, redirect, url_for
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
warnings.filterwarnings('ignore')

class WebUIAccessDB:
    """Access database manager for Web UI"""
    
    def __init__(self, db_path):
        self.db_path = db_path
        self.connection_string = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_path};'
    
    def get_connection(self):
        """Get database connection"""
        try:
            return pyodbc.connect(self.connection_string)
        except Exception as e:
            print(f"Database connection error: {e}")
            return None
    
    def insert_deal(self, deal_data):
        """Insert new deal"""
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            insert_sql = '''
                INSERT INTO ABS_Deals 
                (DealName, IssueDate, RatingAgency, Sector, DealSize, 
                 ClassAAdvanceRate, InitialOC, ExpectedCNLLow, ExpectedCNLHigh,
                 ReserveAccount, AvgSeasoning, TopObligorConc, CreatedDate, SourceFile)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            values = (
                deal_data['dealName'],
                deal_data['issueDate'],
                deal_data['ratingAgency'],
                deal_data['sector'],
                float(deal_data['dealSize']),
                float(deal_data['classAAdvanceRate']),
                float(deal_data['initialOC']),
                float(deal_data['expectedCNLLow']),
                float(deal_data['expectedCNLHigh']),
                float(deal_data['reserveAccount']),
                int(deal_data['avgSeasoning']),
                float(deal_data['topObligorConc']),
                datetime.now(),
                'Web UI Entry'
            )
            
            cursor.execute(insert_sql, values)
            cursor.execute("SELECT @@IDENTITY")
            deal_id = cursor.fetchone()[0]
            
            conn.commit()
            conn.close()
            return deal_id
            
        except Exception as e:
            print(f"Insert error: {e}")
            conn.close()
            return None
    
    def get_all_deals(self):
        """Get all deals"""
        conn = self.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT ID, DealName, IssueDate, RatingAgency, Sector, DealSize, 
                       ClassAAdvanceRate, InitialOC, ExpectedCNLLow, ExpectedCNLHigh,
                       ReserveAccount, AvgSeasoning, TopObligorConc, CreatedDate
                FROM ABS_Deals ORDER BY CreatedDate DESC
            """)
            
            deals = []
            for row in cursor.fetchall():
                deals.append({
                    'id': row[0],
                    'dealName': row[1],
                    'issueDate': row[2].strftime('%Y-%m-%d') if row[2] else '',
                    'ratingAgency': row[3],
                    'sector': row[4],
                    'dealSize': float(row[5]) if row[5] else 0,
                    'classAAdvanceRate': float(row[6]) if row[6] else 0,
                    'initialOC': float(row[7]) if row[7] else 0,
                    'expectedCNLLow': float(row[8]) if row[8] else 0,
                    'expectedCNLHigh': float(row[9]) if row[9] else 0,
                    'reserveAccount': float(row[10]) if row[10] else 0,
                    'avgSeasoning': int(row[11]) if row[11] else 0,
                    'topObligorConc': float(row[12]) if row[12] else 0,
                    'createdDate': row[13].strftime('%Y-%m-%d %H:%M') if row[13] else ''
                })
            
            conn.close()
            return deals
            
        except Exception as e:
            print(f"Retrieve error: {e}")
            conn.close()
            return []
    
    def run_stress_test(self, scenario='moderate'):
        """Run stress test on all deals"""
        deals = self.get_all_deals()
        if not deals:
            return {'error': 'No deals found'}
        
        multipliers = {
            'mild': 1.2,
            'moderate': 1.5,
            'severe': 2.0
        }
        
        loss_mult = multipliers.get(scenario, 1.5)
        results = []
        
        for deal in deals:
            stressed_loss = deal['expectedCNLLow'] * loss_mult
            total_enhancement = deal['initialOC'] + deal['reserveAccount']
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
                'dealName': deal['dealName'],
                'stressedLoss': round(stressed_loss, 2),
                'adequacyRatio': round(adequacy_ratio, 2),
                'status': status,
                'dealSize': deal['dealSize']
            })
        
        # Summary statistics
        critical = len([r for r in results if r['status'] == 'Critical'])
        weak = len([r for r in results if r['status'] == 'Weak'])
        volume_at_risk = sum([r['dealSize'] for r in results if r['status'] in ['Critical', 'Weak']])
        
        return {
            'scenario': scenario,
            'results': results,
            'summary': {
                'critical': critical,
                'weak': weak,
                'total_at_risk': critical + weak,
                'volume_at_risk': round(volume_at_risk, 0)
            }
        }
    
    def monte_carlo_simulation(self, deal_id, iterations=10000):
        """Run Monte Carlo simulation on specific deal"""
        deals = self.get_all_deals()
        deal = next((d for d in deals if d['id'] == deal_id), None)
        
        if not deal:
            return {'error': 'Deal not found'}
        
        # Simple Monte Carlo simulation
        np.random.seed(42)
        loss_factors = np.random.normal(1, 0.3, iterations)
        simulated_losses = deal['expectedCNLLow'] * np.maximum(0.1, loss_factors)
        enhancement = deal['initialOC'] + deal['reserveAccount']
        
        breaches = simulated_losses > enhancement
        shortfalls = np.maximum(0, simulated_losses - enhancement)
        
        return {
            'dealName': deal['dealName'],
            'iterations': iterations,
            'breachProbability': round(np.mean(breaches) * 100, 2),
            'avgShortfall': round(np.mean(shortfalls), 2),
            'maxShortfall': round(np.max(shortfalls), 2),
            'percentile95': round(np.percentile(simulated_losses, 95), 2)
        }

class ABSWebApp:
    """Main Web Application"""
    
    def __init__(self, db_path):
        self.app = Flask(__name__)
        CORS(self.app)
        self.db = WebUIAccessDB(db_path)
        self.setup_routes()
    
    def setup_routes(self):
        """Setup all web routes"""
        
        @self.app.route('/')
        def index():
            """Main page"""
            return render_template_string(self.get_html_template())
        
        @self.app.route('/api/deals', methods=['GET'])
        def get_deals():
            """Get all deals"""
            deals = self.db.get_all_deals()
            return jsonify(deals)
        
        @self.app.route('/api/deals', methods=['POST'])
        def add_deal():
            """Add new deal"""
            try:
                deal_data = request.json
                deal_id = self.db.insert_deal(deal_data)
                
                if deal_id:
                    return jsonify({'success': True, 'id': deal_id, 'message': 'Deal added successfully'})
                else:
                    return jsonify({'success': False, 'message': 'Failed to add deal'}), 400
                    
            except Exception as e:
                return jsonify({'success': False, 'message': str(e)}), 500
        
        @self.app.route('/api/stress-test', methods=['POST'])
        def stress_test():
            """Run stress test"""
            try:
                data = request.json
                scenario = data.get('scenario', 'moderate')
                results = self.db.run_stress_test(scenario)
                return jsonify(results)
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/monte-carlo', methods=['POST'])
        def monte_carlo():
            """Run Monte Carlo simulation"""
            try:
                data = request.json
                deal_id = data.get('dealId')
                iterations = data.get('iterations', 10000)
                results = self.db.monte_carlo_simulation(deal_id, iterations)
                return jsonify(results)
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/charts/comparative')
        def comparative_chart():
            """Generate comparative analysis chart"""
            deals = self.db.get_all_deals()
            if not deals:
                return jsonify({'data': [], 'layout': {}})
            
            fig = go.Figure()
            
            # Scatter plot: Enhancement vs Expected Loss
            fig.add_trace(go.Scatter(
                x=[d['expectedCNLLow'] for d in deals],
                y=[d['initialOC'] + d['reserveAccount'] for d in deals],
                mode='markers+text',
                text=[d['dealName'][:20] for d in deals],
                textposition='top center',
                marker=dict(
                    size=[d['dealSize']/20 for d in deals],
                    color=[hash(d['sector']) % 10 for d in deals],
                    colorscale='viridis',
                    showscale=True
                ),
                name='Deals'
            ))
            
            fig.update_layout(
                title='Credit Enhancement vs Expected Loss',
                xaxis_title='Expected CNL (%)',
                yaxis_title='Total Enhancement (%)',
                height=400
            )
            
            return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        
        @self.app.route('/api/charts/sector-distribution')
        def sector_chart():
            """Generate sector distribution chart"""
            deals = self.db.get_all_deals()
            if not deals:
                return jsonify({'data': [], 'layout': {}})
            
            sectors = {}
            for deal in deals:
                sector = deal['sector']
                if sector in sectors:
                    sectors[sector] += 1
                else:
                    sectors[sector] = 1
            
            fig = go.Figure(data=[go.Pie(
                labels=list(sectors.keys()),
                values=list(sectors.values()),
                hole=0.3
            )])
            
            fig.update_layout(
                title='Deal Distribution by Sector',
                height=400
            )
            
            return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    
    def get_html_template(self):
        """Return the complete HTML template"""
        return '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ABS Analysis Platform</title>
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
            min-width: 180px;
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
        
        .btn-success {
            background: linear-gradient(135deg, #51cf66 0%, #40c057 100%);
        }
        
        .btn-danger {
            background: linear-gradient(135deg, #ff6b6b 0%, #ee5a52 100%);
        }
        
        .chart-container {
            background: white;
            border-radius: 10px;
            padding: 20px;
            margin: 20px 0;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            height: 500px;
        }
        
        .dashboard-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 20px;
            margin-top: 20px;
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
        
        .risk-indicator {
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: bold;
        }
        
        .risk-strong { background: #c8e6c9; color: #2e7d32; }
        .risk-adequate { background: #fff9c4; color: #f57c00; }
        .risk-weak { background: #ffecb3; color: #f57c00; }
        .risk-critical { background: #ffcdd2; color: #c62828; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏦 ABS Analysis Platform</h1>
            <p>Connected to Access Database - Real-time Analysis & Risk Management</p>
        </div>
        
        <div class="tabs">
            <button class="tab-button active" onclick="showTab('deal-entry')">📝 Deal Entry</button>
            <button class="tab-button" onclick="showTab('analytics')">📊 Analytics</button>
            <button class="tab-button" onclick="showTab('dashboard')">📈 Dashboard</button>
        </div>
        
        <!-- Deal Entry Tab -->
        <div id="deal-entry" class="tab-content active">
            <h2>New Deal Entry</h2>
            <p>Enter deal information below. Data will be automatically saved to your Access database.</p>
            
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
                        </select>
                    </div>
                    <div class="form-group">
                        <label>Total Deal Size ($MM) *</label>
                        <input type="number" id="dealSize" step="0.01" required placeholder="e.g., 250.5">
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
                <h3>Recent Deals</h3>
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
                            <th>Initial OC</th>
                            <th>Date Added</th>
                            <th>Actions</th>
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
            
            <!-- Stress Testing Section -->
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
            
            <!-- Monte Carlo Section -->
            <div style="background: #e8f5e8; padding: 20px; border-radius: 10px; margin-bottom: 30px;">
                <h3>🎲 Monte Carlo Risk Simulation</h3>
                <p>Simulate thousands of scenarios to assess deal-specific risk.</p>
                
                <div class="form-grid">
                    <div class="form-group">
                        <label>Select Deal</label>
                        <select id="monteCarloDeals">
                            <option value="">Loading deals...</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label>Number of Simulations</label>
                        <select id="simulationCount">
                            <option value="1000">1,000 (Fast)</option>
                            <option value="10000">10,000 (Standard)</option>
                            <option value="50000">50,000 (Precise)</option>
                        </select>
                    </div>
                </div>
                
                <button class="btn" onclick="runMonteCarlo()">🎯 Run Simulation</button>
                
                <div id="monteCarloResults"></div>
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
                    <div class="metric-value" id="avgAdvanceRate">0%</div>
                    <div class="metric-label">Avg Advance Rate</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value" id="avgOC">0%</div>
                    <div class="metric-label">Avg Initial OC</div>
                </div>
            </div>
            
            <div class="dashboard-grid">
                <div class="chart-container">
                    <div id="comparativeChart"></div>
                </div>
                <div class="chart-container">
                    <div id="sectorChart"></div>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Global variables
        let allDeals = [];
        
        // Tab functionality
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
            } else if (tabName === 'analytics') {
                loadAnalyticsDeals();
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
                    showAlert('Deal added successfully! 🎉', 'success');
                    clearForm();
                    loadDeals();
                } else {
                    showAlert('Error: ' + result.message, 'error');
                }
            } catch (error) {
                showAlert('Error: ' + error.message, 'error');
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
                    row.innerHTML = `
                        <td>${deal.dealName}</td>
                        <td>${deal.sector}</td>
                        <td>$${deal.dealSize.toFixed(0)}M</td>
                        <td>${deal.classAAdvanceRate.toFixed(1)}%</td>
                        <td>${deal.initialOC.toFixed(1)}%</td>
                        <td>${deal.createdDate}</td>
                        <td>
                            <button class="btn" style="padding: 5px 10px; margin: 2px;" 
                                    onclick="runMonteCarloForDeal(${deal.id})">📊 Analyze</button>
                        </td>
                    `;
                });
                
                document.getElementById('loadingDeals').style.display = 'none';
                document.getElementById('dealsTable').style.display = 'table';
                
            } catch (error) {
                showAlert('Error loading deals: ' + error.message, 'error');
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
                    html += `
                        <tr>
                            <td>${deal.dealName}</td>
                            <td>${deal.stressedLoss}%</td>
                            <td>${deal.adequacyRatio}</td>
                            <td><span class="risk-indicator risk-${deal.status.toLowerCase()}">${deal.status}</span></td>
                        </tr>
                    `;
                });
                
                html += '</tbody></table>';
            }
            
            document.getElementById('stressResults').innerHTML = html;
        }
        
        async function loadAnalyticsDeals() {
            const select = document.getElementById('monteCarloDeals');
            select.innerHTML = '<option value="">Loading deals...</option>';
            
            try {
                const response = await fetch('/api/deals');
                const deals = await response.json();
                
                select.innerHTML = '<option value="">Select Deal...</option>';
                
                deals.forEach(deal => {
                    const option = document.createElement('option');
                    option.value = deal.id;
                    option.textContent = deal.dealName;
                    select.appendChild(option);
                });
                
            } catch (error) {
                select.innerHTML = '<option value="">Error loading deals</option>';
            }
        }
        
        async function runMonteCarlo() {
            const dealId = document.getElementById('monteCarloDeals').value;
            const iterations = parseInt(document.getElementById('simulationCount').value);
            
            if (!dealId) {
                showAlert('Please select a deal', 'warning');
                return;
            }
            
            try {
                const response = await fetch('/api/monte-carlo', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({ dealId: parseInt(dealId), iterations })
                });
                
                const results = await response.json();
                displayMonteCarloResults(results);
                
            } catch (error) {
                showAlert('Error running Monte Carlo: ' + error.message, 'error');
            }
        }
        
        function displayMonteCarloResults(results) {
            if (results.error) {
                showAlert('Error: ' + results.error, 'error');
                return;
            }
            
            let html = `
                <h4>Monte Carlo Results - ${results.dealName}</h4>
                <div class="dashboard-grid">
                    <div class="metric-card">
                        <div class="metric-value">${results.iterations.toLocaleString()}</div>
                        <div class="metric-label">Simulations</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">${results.breachProbability}%</div>
                        <div class="metric-label">Breach Probability</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">${results.avgShortfall}%</div>
                        <div class="metric-label">Avg Shortfall</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">${results.percentile95}%</div>
                        <div class="metric-label">95th Percentile Loss</div>
                    </div>
                </div>
            `;
            
            document.getElementById('monteCarloResults').innerHTML = html;
        }
        
        async function runMonteCarloForDeal(dealId) {
            document.getElementById('monteCarloDeals').value = dealId;
            document.getElementById('simulationCount').value = '10000';
            
            // Switch to analytics tab
            showTab('analytics');
            document.querySelector('[onclick="showTab(\\'analytics\\')"]').classList.add('active');
            
            // Run the simulation
            await runMonteCarlo();
        }
        
        async function loadDashboard() {
            try {
                const response = await fetch('/api/deals');
                const deals = await response.json();
                
                if (deals.length === 0) {
                    document.getElementById('totalDeals').textContent = '0';
                    document.getElementById('totalVolume').textContent = '$0M';
                    document.getElementById('avgAdvanceRate').textContent = '0%';
                    document.getElementById('avgOC').textContent = '0%';
                    return;
                }
                
                const totalVolume = deals.reduce((sum, deal) => sum + deal.dealSize, 0);
                const avgAdvanceRate = deals.reduce((sum, deal) => sum + deal.classAAdvanceRate, 0) / deals.length;
                const avgOC = deals.reduce((sum, deal) => sum + deal.initialOC, 0) / deals.length;
                
                document.getElementById('totalDeals').textContent = deals.length;
                document.getElementById('totalVolume').textContent = `$${totalVolume.toFixed(0)}M`;
                document.getElementById('avgAdvanceRate').textContent = `${avgAdvanceRate.toFixed(1)}%`;
                document.getElementById('avgOC').textContent = `${avgOC.toFixed(1)}%`;
                
                // Load charts
                await loadCharts();
                
            } catch (error) {
                showAlert('Error loading dashboard: ' + error.message, 'error');
            }
        }
        
        async function loadCharts() {
            try {
                // Load comparative chart
                const compareResponse = await fetch('/api/charts/comparative');
                const compareData = await compareResponse.json();
                Plotly.newPlot('comparativeChart', compareData.data, compareData.layout);
                
                // Load sector chart
                const sectorResponse = await fetch('/api/charts/sector-distribution');
                const sectorData = await sectorResponse.json();
                Plotly.newPlot('sectorChart', sectorData.data, sectorData.layout);
                
            } catch (error) {
                console.error('Error loading charts:', error);
            }
        }
        
        function showAlert(message, type) {
            const alertDiv = document.createElement('div');
            alertDiv.className = `alert alert-${type}`;
            alertDiv.textContent = message;
            
            document.getElementById('dealAlerts').appendChild(alertDiv);
            
            setTimeout(() => {
                alertDiv.remove();
            }, 5000);
        }
        
        // Initialize
        document.addEventListener('DOMContentLoaded', function() {
            loadDeals();
        });
    </script>
</body>
</html>
        '''
    
    def run(self, host='127.0.0.1', port=5000, debug=False):
        """Start the web application"""
        print(f"🌐 Starting ABS Web Platform at http://{host}:{port}")
        print("📊 Connected to Access database")
        print("🛑 Press Ctrl+C to stop the server")
        
        if not debug:
            threading.Timer(1.5, lambda: webbrowser.open(f'http://{host}:{port}')).start()
        
        self.app.run(host=host, port=port, debug=debug)

def launch_web_platform():
    """Launch the complete web platform"""
    db_path = r"C:\Users\edfit\OneDrive\Documents\ABS_Performance_Data.db.accdb"
    
    print("🚀 Launching ABS Web Platform")
    print("=" * 40)
    print(f"Database: {db_path}")
    print("Features: Deal Entry, Analytics, Dashboard")
    print("Auto-opening browser...")
    
    app = ABSWebApp(db_path)
    app.run()

if __name__ == "__main__":
    launch_web_platform()
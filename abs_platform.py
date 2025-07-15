# -*- coding: utf-8 -*-
"""
Created on Sat Jul 12 13:59:38 2025

@author: edfit
"""

"""
ABS Analysis Platform - Python Version for Spyder
Comprehensive Asset-Backed Securities Analysis & Risk Management
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import json
from typing import Dict, List, Optional
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import dash
from dash import dcc, html, Input, Output, State, dash_table
import webbrowser
import threading

warnings.filterwarnings('ignore')

class ABSAnalysisPlatform:
    """
    Python-based ABS Analysis Platform for Spyder IDE
    """
    
    def __init__(self):
        self.deal_database = pd.DataFrame()
        self.benchmarks = {}
        self.integration_assessments = []
        self.app = None
        self.initialize_sample_data()
        
    def initialize_sample_data(self):
        """Initialize with sample deals for demonstration"""
        sample_deals = [
            {
                'deal_name': 'OWN Equipment Fund II LLC',
                'issue_date': '2025-06-25',
                'rating_agency': 'KBRA',
                'sector': 'Equipment ABS',
                'deal_size': 227.78,
                'class_a_advance_rate': 74.10,
                'initial_oc': 17.0,
                'expected_cnl_low': 3.0,
                'expected_cnl_high': 4.0,
                'reserve_account': 2.06,
                'avg_seasoning': 24,
                'top_obligor_conc': 1.0
            },
            {
                'deal_name': 'PEAC Solutions Receivables 2025-1',
                'issue_date': '2025-02-11',
                'rating_agency': 'KBRA',
                'sector': 'Small Ticket Leasing',
                'deal_size': 687.27,
                'class_a_advance_rate': 21.00,
                'initial_oc': 10.70,
                'expected_cnl_low': 3.30,
                'expected_cnl_high': 3.40,
                'reserve_account': 0.50,
                'avg_seasoning': 7,
                'top_obligor_conc': 0.45
            }
        ]
        
        self.deal_database = pd.DataFrame(sample_deals)
        self.deal_database['issue_date'] = pd.to_datetime(self.deal_database['issue_date'])
        print("✅ Sample data loaded successfully")
        
    def add_deal(self, deal_data: Dict):
        """Add new deal to database"""
        new_deal = pd.DataFrame([deal_data])
        self.deal_database = pd.concat([self.deal_database, new_deal], ignore_index=True)
        
        # Check for alerts
        alerts = self.check_deviation_alerts(deal_data)
        for alert in alerts:
            print(f"🚨 ALERT: {alert}")
            
        print(f"✅ Added deal: {deal_data['deal_name']}")
        
    def check_deviation_alerts(self, deal_data: Dict) -> List[str]:
        """Check for significant deviations from sector benchmarks"""
        alerts = []
        sector_deals = self.deal_database[
            (self.deal_database['sector'] == deal_data['sector']) & 
            (self.deal_database['deal_name'] != deal_data['deal_name'])
        ]
        
        if len(sector_deals) == 0:
            return [f"First deal in {deal_data['sector']} sector - no benchmarks available"]
        
        # Calculate sector averages
        avg_advance_rate = sector_deals['class_a_advance_rate'].mean()
        avg_oc = sector_deals['initial_oc'].mean()
        avg_cnl = sector_deals['expected_cnl_low'].mean()
        
        # Check deviations (5%, 2%, 1% thresholds)
        if abs(deal_data['class_a_advance_rate'] - avg_advance_rate) > 5.0:
            alerts.append(f"Class A advance rate ({deal_data['class_a_advance_rate']:.1f}%) deviates from sector avg ({avg_advance_rate:.1f}%)")
        
        if abs(deal_data['initial_oc'] - avg_oc) > 2.0:
            alerts.append(f"Initial OC ({deal_data['initial_oc']:.1f}%) deviates from sector avg ({avg_oc:.1f}%)")
        
        if abs(deal_data['expected_cnl_low'] - avg_cnl) > 1.0:
            alerts.append(f"Expected CNL ({deal_data['expected_cnl_low']:.1f}%) deviates from sector avg ({avg_cnl:.1f}%)")
        
        return alerts
    
    def create_comparative_analysis(self, sector_filter=None, agency_filter=None):
        """Generate comparative analysis charts"""
        df = self.deal_database.copy()
        
        if sector_filter:
            df = df[df['sector'] == sector_filter]
        if agency_filter:
            df = df[df['rating_agency'] == agency_filter]
            
        if len(df) == 0:
            print("❌ No deals found matching filters")
            return None
            
        # Create subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Advance Rates by Agency', 'Enhancement vs Expected Loss', 
                          'Deal Size Distribution', 'Sector Overview'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"type": "pie"}]]
        )
        
        # 1. Advance Rates by Agency
        for agency in df['rating_agency'].unique():
            agency_data = df[df['rating_agency'] == agency]
            fig.add_trace(
                go.Bar(name=agency, 
                      x=agency_data['deal_name'], 
                      y=agency_data['class_a_advance_rate'],
                      text=agency_data['class_a_advance_rate'].round(1),
                      textposition='auto'),
                row=1, col=1
            )
        
        # 2. Enhancement vs Expected Loss
        fig.add_trace(
            go.Scatter(
                x=df['expected_cnl_low'],
                y=df['initial_oc'] + df['reserve_account'],
                mode='markers+text',
                text=df['deal_name'],
                textposition='top center',
                marker=dict(size=df['deal_size']/20, 
                           color=df['sector'].astype('category').cat.codes,
                           colorscale='viridis'),
                name='Enhancement vs Loss'
            ),
            row=1, col=2
        )
        
        # 3. Deal Size Distribution
        fig.add_trace(
            go.Histogram(x=df['deal_size'], nbinsx=10, name='Deal Size Distribution'),
            row=2, col=1
        )
        
        # 4. Sector Pie Chart
        sector_counts = df['sector'].value_counts()
        fig.add_trace(
            go.Pie(labels=sector_counts.index, values=sector_counts.values, name='Sectors'),
            row=2, col=2
        )
        
        fig.update_layout(height=800, title_text="ABS Comparative Analysis Dashboard")
        fig.show()
        
        return fig
    
    def run_stress_test(self, scenario='moderate', custom_loss_multiplier=1.5):
        """Run portfolio stress test"""
        multipliers = {
            'mild': {'loss': 1.2, 'recovery': 0.95},
            'moderate': {'loss': 1.5, 'recovery': 0.85},
            'severe': {'loss': 2.0, 'recovery': 0.75},
            'custom': {'loss': custom_loss_multiplier, 'recovery': 0.85}
        }
        
        mult = multipliers[scenario]
        
        # Calculate stressed metrics
        stressed_deals = self.deal_database.copy()
        stressed_deals['stressed_loss'] = stressed_deals['expected_cnl_low'] * mult['loss']
        stressed_deals['total_enhancement'] = stressed_deals['initial_oc'] + stressed_deals['reserve_account']
        stressed_deals['adequacy_ratio'] = stressed_deals['total_enhancement'] / stressed_deals['stressed_loss']
        stressed_deals['status'] = stressed_deals['adequacy_ratio'].apply(
            lambda x: 'Strong' if x >= 1.5 else 'Adequate' if x >= 1.2 else 'Weak' if x >= 1.0 else 'Critical'
        )
        
        # Results summary
        critical_deals = stressed_deals[stressed_deals['status'] == 'Critical']
        weak_deals = stressed_deals[stressed_deals['status'] == 'Weak']
        
        print(f"\n📊 STRESS TEST RESULTS - {scenario.upper()} SCENARIO")
        print("=" * 50)
        print(f"💀 Critical Deals: {len(critical_deals)}")
        print(f"⚠️  Weak Deals: {len(weak_deals)}")
        print(f"💰 Volume at Risk: ${(critical_deals['deal_size'].sum() + weak_deals['deal_size'].sum()):.0f}M")
        print(f"📈 Portfolio % at Risk: {((len(critical_deals) + len(weak_deals)) / len(stressed_deals) * 100):.1f}%")
        
        if len(critical_deals) > 0 or len(weak_deals) > 0:
            print("\n🚨 DEALS AT RISK:")
            risk_deals = pd.concat([critical_deals, weak_deals])
            print(risk_deals[['deal_name', 'adequacy_ratio', 'stressed_loss', 'total_enhancement', 'status']].to_string(index=False))
        
        # Visualization
        fig = px.scatter(stressed_deals, 
                        x='deal_size', y='adequacy_ratio',
                        color='status', 
                        hover_name='deal_name',
                        title=f'Stress Test Results - {scenario.upper()} Scenario',
                        labels={'deal_size': 'Deal Size ($MM)', 'adequacy_ratio': 'Adequacy Ratio'})
        fig.add_hline(y=1.0, line_dash="dash", line_color="red", annotation_text="Minimum Adequacy")
        fig.add_hline(y=1.2, line_dash="dash", line_color="orange", annotation_text="Adequate Threshold")
        fig.show()
        
        return stressed_deals
    
    def monte_carlo_simulation(self, deal_name, iterations=10000, loss_volatility=0.3, recovery_volatility=0.15):
        """Run Monte Carlo simulation for specific deal"""
        deal = self.deal_database[self.deal_database['deal_name'] == deal_name]
        if len(deal) == 0:
            print(f"❌ Deal '{deal_name}' not found")
            return None
            
        deal = deal.iloc[0]
        
        # Generate random scenarios
        np.random.seed(42)  # For reproducible results
        loss_factors = np.random.normal(1, loss_volatility, iterations)
        recovery_factors = np.random.normal(1, recovery_volatility, iterations)
        
        # Calculate outcomes
        simulated_losses = deal['expected_cnl_low'] * np.maximum(0.1, loss_factors)
        enhancement = deal['initial_oc'] + deal['reserve_account']
        breaches = simulated_losses > enhancement
        shortfalls = np.maximum(0, simulated_losses - enhancement)
        
        # Results
        breach_probability = np.mean(breaches) * 100
        avg_shortfall = np.mean(shortfalls)
        max_shortfall = np.max(shortfalls)
        percentile_95_loss = np.percentile(simulated_losses, 95)
        
        print(f"\n🎲 MONTE CARLO SIMULATION - {deal_name}")
        print("=" * 50)
        print(f"🔢 Simulations: {iterations:,}")
        print(f"💥 Breach Probability: {breach_probability:.1f}%")
        print(f"📉 Average Shortfall: {avg_shortfall:.2f}%")
        print(f"📊 Maximum Shortfall: {max_shortfall:.2f}%")
        print(f"📈 95th Percentile Loss: {percentile_95_loss:.2f}%")
        
        # Visualization
        fig = go.Figure()
        fig.add_trace(go.Histogram(x=simulated_losses, nbinsx=50, name='Loss Distribution'))
        fig.add_vline(x=enhancement, line_dash="dash", line_color="red", 
                     annotation_text=f"Enhancement Level ({enhancement:.1f}%)")
        fig.add_vline(x=percentile_95_loss, line_dash="dash", line_color="orange",
                     annotation_text=f"95th Percentile ({percentile_95_loss:.1f}%)")
        fig.update_layout(title=f'Monte Carlo Loss Distribution - {deal_name}',
                         xaxis_title='Simulated Loss (%)',
                         yaxis_title='Frequency')
        fig.show()
        
        return {
            'breach_probability': breach_probability,
            'avg_shortfall': avg_shortfall,
            'max_shortfall': max_shortfall,
            'percentile_95_loss': percentile_95_loss,
            'simulated_losses': simulated_losses
        }
    
    def peer_analysis(self, target_deal_name, criteria='sector'):
        """Perform peer comparison analysis"""
        target = self.deal_database[self.deal_database['deal_name'] == target_deal_name]
        if len(target) == 0:
            print(f"❌ Deal '{target_deal_name}' not found")
            return None
            
        target = target.iloc[0]
        peers = self.deal_database[self.deal_database['deal_name'] != target_deal_name]
        
        # Apply filters based on criteria
        if criteria == 'sector':
            peers = peers[peers['sector'] == target['sector']]
        elif criteria == 'size':
            size_range = 0.5  # ±50%
            peers = peers[
                (peers['deal_size'] >= target['deal_size'] * (1 - size_range)) &
                (peers['deal_size'] <= target['deal_size'] * (1 + size_range))
            ]
        elif criteria == 'agency':
            peers = peers[peers['rating_agency'] == target['rating_agency']]
        
        if len(peers) == 0:
            print(f"❌ No peers found for {criteria} criteria")
            return None
        
        # Calculate percentiles
        metrics = ['class_a_advance_rate', 'initial_oc', 'expected_cnl_low']
        results = {}
        
        print(f"\n👥 PEER ANALYSIS - {target_deal_name}")
        print("=" * 50)
        print(f"📊 Comparable Peers: {len(peers)}")
        print(f"🔍 Criteria: {criteria}")
        print()
        
        for metric in metrics:
            target_value = target[metric]
            peer_values = peers[metric].values
            percentile = (np.sum(peer_values <= target_value) / len(peer_values)) * 100
            peer_avg = np.mean(peer_values)
            
            results[metric] = {
                'target': target_value,
                'peer_avg': peer_avg,
                'percentile': percentile,
                'difference': target_value - peer_avg
            }
            
            print(f"{metric.replace('_', ' ').title()}:")
            print(f"  Target: {target_value:.1f}%")
            print(f"  Peer Avg: {peer_avg:.1f}%")
            print(f"  Difference: {(target_value - peer_avg):+.1f}%")
            print(f"  Percentile: {percentile:.0f}th")
            print()
        
        # Visualization
        fig = go.Figure()
        
        categories = [m.replace('_', ' ').title() for m in metrics]
        percentiles = [results[m]['percentile'] for m in metrics]
        
        fig.add_trace(go.Scatterpolar(
            r=percentiles,
            theta=categories,
            fill='toself',
            name=f'{target_deal_name} Percentiles'
        ))
        
        fig.update_layout(
            polar=dict(
                radialaxis=dict(visible=True, range=[0, 100])
            ),
            title=f"Peer Comparison - {target_deal_name}",
            showlegend=True
        )
        fig.show()
        
        return results
    
    def integration_risk_assessment(self, deal_name, integration_type='system_integration', 
                                   duration_days=90, staff_retention=85, 
                                   systems_compatibility='compatible'):
        """Assess integration risk for deals with operational transitions"""
        
        # Risk factors with weights
        risk_factors = {
            'System Integration': {'weight': 0.25, 'base_score': 5},
            'Staff Transition': {'weight': 0.20, 'base_score': 5},
            'Process Standardization': {'weight': 0.15, 'base_score': 5},
            'Customer Communication': {'weight': 0.10, 'base_score': 5},
            'Data Integrity': {'weight': 0.15, 'base_score': 5},
            'Regulatory Compliance': {'weight': 0.10, 'base_score': 5},
            'Performance Continuity': {'weight': 0.05, 'base_score': 5}
        }
        
        # Adjust scores based on inputs
        if systems_compatibility == 'different_platforms':
            risk_factors['System Integration']['base_score'] += 2
        elif systems_compatibility == 'same_platform':
            risk_factors['System Integration']['base_score'] -= 1
            
        if staff_retention < 70:
            risk_factors['Staff Transition']['base_score'] += 2
        elif staff_retention > 90:
            risk_factors['Staff Transition']['base_score'] -= 2
            
        if duration_days < 60:
            for factor in risk_factors.values():
                factor['base_score'] += 1
        
        # Ensure scores stay within bounds (1-10)
        for factor in risk_factors.values():
            factor['base_score'] = max(1, min(10, factor['base_score']))
        
        # Calculate overall risk
        overall_risk = sum(f['base_score'] * f['weight'] for f in risk_factors.values())
        risk_level = 'Critical' if overall_risk >= 8 else 'High' if overall_risk >= 6 else 'Medium' if overall_risk >= 4 else 'Low'
        
        print(f"\n⚠️ INTEGRATION RISK ASSESSMENT - {deal_name}")
        print("=" * 50)
        print(f"🔧 Integration Type: {integration_type}")
        print(f"📅 Duration: {duration_days} days")
        print(f"👥 Staff Retention: {staff_retention}%")
        print(f"💻 Systems: {systems_compatibility}")
        print()
        print(f"📊 Overall Risk Score: {overall_risk:.1f}/10")
        print(f"🚨 Risk Level: {risk_level}")
        print()
        
        critical_factors = [name for name, factor in risk_factors.items() if factor['base_score'] >= 8]
        if critical_factors:
            print(f"🔥 Critical Factors: {', '.join(critical_factors)}")
        else:
            print("✅ No critical risk factors identified")
        
        # Visualization
        fig = go.Figure()
        
        factor_names = list(risk_factors.keys())
        scores = [risk_factors[name]['base_score'] for name in factor_names]
        
        fig.add_trace(go.Scatterpolar(
            r=scores,
            theta=factor_names,
            fill='toself',
            name='Risk Scores'
        ))
        
        fig.update_layout(
            polar=dict(
                radialaxis=dict(visible=True, range=[0, 10])
            ),
            title=f"Integration Risk Factors - {deal_name}",
            showlegend=True
        )
        fig.show()
        
        # Store assessment
        assessment = {
            'deal_name': deal_name,
            'integration_type': integration_type,
            'overall_risk': overall_risk,
            'risk_level': risk_level,
            'critical_factors': critical_factors,
            'assessment_date': datetime.now(),
            'risk_factors': risk_factors
        }
        self.integration_assessments.append(assessment)
        
        return assessment
    
    def equipment_liquidation_analysis(self, equipment_type='construction_equipment', 
                                     age_months=24, total_value=274.4, 
                                     market_condition='stable'):
        """Analyze equipment liquidation scenarios"""
        
        # Base recovery rates by scenario and equipment type
        base_rates = {
            'orderly': {
                'construction_equipment': 0.75,
                'office_equipment': 0.45,
                'medical_equipment': 0.65,
                'transportation': 0.70
            },
            'forced': {
                'construction_equipment': 0.60,
                'office_equipment': 0.30,
                'medical_equipment': 0.50,
                'transportation': 0.55
            },
            'distressed': {
                'construction_equipment': 0.40,
                'office_equipment': 0.20,
                'medical_equipment': 0.35,
                'transportation': 0.40
            }
        }
        
        market_adjustments = {
            'strong': 1.1,
            'stable': 1.0,
            'declining': 0.9,
            'volatile': 0.85
        }
        
        scenarios = {}
        
        print(f"\n🏗️ EQUIPMENT LIQUIDATION ANALYSIS")
        print("=" * 50)
        print(f"⚙️  Equipment Type: {equipment_type}")
        print(f"📅 Age: {age_months} months")
        print(f"💰 Total Value: ${total_value:.1f}M")
        print(f"📈 Market Condition: {market_condition}")
        print()
        
        for scenario in base_rates.keys():
            base_rate = base_rates[scenario][equipment_type]
            age_adjustment = max(0, 1 - (age_months * 0.01))  # 1% reduction per month
            market_adjustment = market_adjustments[market_condition]
            
            final_rate = base_rate * age_adjustment * market_adjustment
            recovery_amount = total_value * final_rate
            
            scenarios[scenario] = {
                'recovery_rate': final_rate,
                'recovery_amount': recovery_amount,
                'time_to_liquidation': {'orderly': '6-12 months', 'forced': '3-6 months', 'distressed': '1-3 months'}[scenario]
            }
            
            print(f"{scenario.upper()} Liquidation:")
            print(f"  Recovery Rate: {final_rate:.1%}")
            print(f"  Recovery Amount: ${recovery_amount:.1f}M")
            print(f"  Timeline: {scenarios[scenario]['time_to_liquidation']}")
            print()
        
        # Visualization
        scenario_names = list(scenarios.keys())
        recovery_rates = [scenarios[s]['recovery_rate'] * 100 for s in scenario_names]
        
        fig = go.Figure(data=[
            go.Bar(name='Recovery Rate', x=scenario_names, y=recovery_rates,
                  text=[f"{r:.1f}%" for r in recovery_rates],
                  textposition='auto')
        ])
        
        fig.update_layout(
            title='Equipment Liquidation Recovery Rates',
            xaxis_title='Liquidation Scenario',
            yaxis_title='Recovery Rate (%)',
            yaxis=dict(range=[0, 100])
        )
        fig.show()
        
        return scenarios
    
    def export_data(self, filename='abs_analysis_export.xlsx'):
        """Export all data to Excel file"""
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            self.deal_database.to_excel(writer, sheet_name='Deals', index=False)
            
            # Create benchmarks sheet
            if self.benchmarks:
                benchmark_df = pd.DataFrame(self.benchmarks).T
                benchmark_df.to_excel(writer, sheet_name='Benchmarks')
            
            # Create assessments sheet
            if self.integration_assessments:
                assessments_df = pd.DataFrame([
                    {
                        'deal_name': a['deal_name'],
                        'integration_type': a['integration_type'],
                        'overall_risk': a['overall_risk'],
                        'risk_level': a['risk_level'],
                        'assessment_date': a['assessment_date']
                    } for a in self.integration_assessments
                ])
                assessments_df.to_excel(writer, sheet_name='Risk Assessments', index=False)
        
        print(f"✅ Data exported to {filename}")
    
    def generate_report(self, deal_name=None):
        """Generate comprehensive analysis report"""
        if deal_name:
            deals = self.deal_database[self.deal_database['deal_name'] == deal_name]
            title = f"ABS Analysis Report - {deal_name}"
        else:
            deals = self.deal_database
            title = "ABS Portfolio Analysis Report"
        
        print(f"\n📋 {title}")
        print("=" * 60)
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Summary statistics
        print("📊 PORTFOLIO SUMMARY")
        print("-" * 30)
        print(f"Total Deals: {len(deals)}")
        print(f"Total Volume: ${deals['deal_size'].sum():.0f}M")
        print(f"Average Deal Size: ${deals['deal_size'].mean():.0f}M")
        print(f"Average Advance Rate: {deals['class_a_advance_rate'].mean():.1f}%")
        print(f"Average Initial OC: {deals['initial_oc'].mean():.1f}%")
        print(f"Average Expected CNL: {deals['expected_cnl_low'].mean():.1f}%")
        print()
        
        # Sector breakdown
        print("🏭 SECTOR BREAKDOWN")
        print("-" * 30)
        sector_summary = deals.groupby('sector').agg({
            'deal_size': ['count', 'sum', 'mean'],
            'class_a_advance_rate': 'mean',
            'initial_oc': 'mean'
        }).round(1)
        print(sector_summary.to_string())
        print()
        
        # Recent assessments
        if self.integration_assessments:
            print("⚠️ RECENT RISK ASSESSMENTS")
            print("-" * 30)
            for assessment in self.integration_assessments[-3:]:  # Last 3
                print(f"Deal: {assessment['deal_name']}")
                print(f"Risk Level: {assessment['risk_level']}")
                print(f"Overall Score: {assessment['overall_risk']:.1f}/10")
                print(f"Date: {assessment['assessment_date'].strftime('%Y-%m-%d')}")
                print()
    
    def create_dashboard_app(self):
        """Create interactive Dash web application"""
        self.app = dash.Dash(__name__)
        
        self.app.layout = html.Div([
            html.H1("🏦 ABS Analysis Platform", style={'textAlign': 'center', 'color': '#2c3e50'}),
            html.P("Comprehensive Asset-Backed Securities Analysis & Risk Management", 
                   style={'textAlign': 'center', 'fontSize': '18px', 'color': '#7f8c8d'}),
            
            dcc.Tabs(id="tabs", value='deals', children=[
                dcc.Tab(label='📊 Deal Overview', value='deals'),
                dcc.Tab(label='📈 Comparative Analysis', value='comparative'),
                dcc.Tab(label='💥 Stress Testing', value='stress'),
                dcc.Tab(label='🎲 Monte Carlo', value='monte_carlo'),
                dcc.Tab(label='⚠️ Risk Assessment', value='risk')
            ]),
            
            html.Div(id='tab-content')
        ])
        
        @self.app.callback(Output('tab-content', 'children'), [Input('tabs', 'value')])
        def render_content(tab):
            if tab == 'deals':
                return html.Div([
                    html.H3("Deal Database"),
                    dash_table.DataTable(
                        data=self.deal_database.to_dict('records'),
                        columns=[{"name": i, "id": i} for i in self.deal_database.columns],
                        style_cell={'textAlign': 'left'},
                        style_data={'whiteSpace': 'normal', 'height': 'auto'},
                        page_size=10
                    )
                ])
            elif tab == 'comparative':
                return html.Div([
                    html.H3("Comparative Analysis"),
                    dcc.Graph(figure=self.create_comparative_plotly())
                ])
            # Add other tabs as needed
            
        return self.app
    
    def create_comparative_plotly(self):
        """Create comparative analysis using Plotly"""
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Advance Rates', 'Enhancement vs Loss', 'Deal Sizes', 'Sector Distribution'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"type": "pie"}]]
        )
        
        df = self.deal_database
        
        # Bar chart for advance rates
        for agency in df['rating_agency'].unique():
            agency_data = df[df['rating_agency'] == agency]
            fig.add_trace(
                go.Bar(name=agency, x=agency_data['deal_name'], y=agency_data['class_a_advance_rate']),
                row=1, col=1
            )
        
        # Scatter plot for enhancement vs loss
        fig.add_trace(
            go.Scatter(
                x=df['expected_cnl_low'],
                y=df['initial_oc'] + df['reserve_account'],
                mode='markers',
                text=df['deal_name'],
                name='Enhancement vs Loss'
            ),
            row=1, col=2
        )
        
        # Histogram for deal sizes
        fig.add_trace(
            go.Histogram(x=df['deal_size'], name='Deal Sizes'),
            row=2, col=1
        )
        
        # Pie chart for sectors
        sector_counts = df['sector'].value_counts()
        fig.add_trace(
            go.Pie(labels=sector_counts.index, values=sector_counts.values, name='Sectors'),
            row=2, col=2
        )
        
        fig.update_layout(height=800, title_text="ABS Comparative Analysis")
        return fig
    
    def run_web_dashboard(self, port=8050):
        """Launch web dashboard"""
        if self.app is None:
            self.create_dashboard_app()
        
        print(f"🌐 Launching web dashboard at http://localhost:{port}")
        print("🛑 Press Ctrl+C to stop the server")
        
        # Open browser automatically
        threading.Timer(1.5, lambda: webbrowser.open(f'http://localhost:{port}')).start()
        
        self.app.run_server(debug=False, port=port)


# Example usage functions for Spyder console
def quick_start():
    """Quick start guide for Spyder users"""
    print("""
🏦 ABS Analysis Platform - Quick Start Guide
==========================================

# 1. Create platform instance
platform = ABSAnalysisPlatform()

# 2. View current deals
print(platform.deal_database)

# 3. Add a new deal
new_deal = {
    'deal_name': 'Test Deal 2025-1',
    'issue_date': '2025-01-15',
    'rating_agency': 'S&P',
    'sector': 'Auto ABS',
    'deal_size': 500.0,
    'class_a_advance_rate': 85.0,
    'initial_oc': 12.0,
    'expected_cnl_low': 2.5,
    'expected_cnl_high': 3.0,
    'reserve_account': 1.5,
    'avg_seasoning': 18,
    'top_obligor_conc': 2.0
}
platform.add_deal(new_deal)

# 4. Run analyses
platform.create_comparative_analysis()
platform.run_stress_test('moderate')
platform.monte_carlo_simulation('OWN Equipment Fund II LLC')
platform.peer_analysis('PEAC Solutions Receivables 2025-1', 'sector')

# 5. Generate report
platform.generate_report()

# 6. Launch web dashboard (optional)
# platform.run_web_dashboard()
    """)

def example_analysis():
    """Run example analysis workflow"""
    platform = ABSAnalysisPlatform()
    
    print("🚀 Running example analysis workflow...")
    
    # Comparative analysis
    platform.create_comparative_analysis()
    
    # Stress testing
    platform.run_stress_test('severe')
    
    # Monte Carlo for first deal
    deal_name = platform.deal_database.iloc[0]['deal_name']
    platform.monte_carlo_simulation(deal_name, iterations=5000)
    
    # Peer analysis
    platform.peer_analysis(deal_name, 'sector')
    
    # Integration risk assessment
    platform.integration_risk_assessment(
        deal_name='PEAC Solutions Receivables 2025-1',
        integration_type='servicer_transition',
        duration_days=120,
        staff_retention=85
    )
    
    # Equipment liquidation analysis
    platform.equipment_liquidation_analysis(
        equipment_type='construction_equipment',
        age_months=24,
        total_value=274.4,
        market_condition='stable'
    )
    
    # Generate comprehensive report
    platform.generate_report()
    
    print("✅ Analysis complete!")
    return platform

if __name__ == "__main__":
    # Initialize platform
    platform = ABSAnalysisPlatform()
    
    print("🏦 ABS Analysis Platform initialized!")
    print("📝 Type 'quick_start()' for usage guide")
    print("🚀 Type 'example_analysis()' to run full example")
    print("🌐 Type 'platform.run_web_dashboard()' for web interface")
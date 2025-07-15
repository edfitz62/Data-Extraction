import streamlit as st
import pandas as pd
import sqlite3
import re
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

class CompleteABSSystem:
    """
    Complete ABS Document Processing System
    Handles both New Issue Reports and Surveillance Reports
    """
    
    def __init__(self):
        self.db_path = "complete_abs_system.db"
        self.init_database()
        
    def init_database(self):
        """Initialize comprehensive database schema"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # New Issue Tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ABS_Deals (
                deal_id TEXT PRIMARY KEY,
                deal_name TEXT,
                issuer TEXT,
                deal_type TEXT,
                issuance_date TEXT,
                total_deal_size REAL,
                currency TEXT,
                asset_type TEXT,
                originator TEXT,
                servicer TEXT,
                trustee TEXT,
                rating_agency TEXT,
                legal_final_maturity TEXT,
                revolving_period TEXT,
                amortization_period TEXT,
                payment_frequency TEXT,
                day_count_convention TEXT,
                created_date TEXT,
                document_type TEXT DEFAULT 'NEW_ISSUE'
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS NoteClasses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                deal_id TEXT,
                note_class TEXT,
                original_balance REAL,
                current_balance REAL,
                interest_rate REAL,
                expected_maturity TEXT,
                legal_final_maturity TEXT,
                rating TEXT,
                subordination_level INTEGER,
                payment_priority INTEGER,
                enhancement_level REAL,
                created_date TEXT,
                FOREIGN KEY (deal_id) REFERENCES ABS_Deals(deal_id)
            )
        """)
        
        # Pricing Data Tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PricingData (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT UNIQUE,
                issuer_name TEXT,
                deal_name TEXT,
                cmo_class TEXT,
                mtge_pricing_speed_date TEXT,
                first_sett_dt TEXT,
                orig_amt REAL,
                mtge_deal_pricing_speed REAL,
                deal_typ TEXT,
                description_bn_reported TEXT,
                strctd_prod_class_ast_subclas TEXT,
                rule_144a_elig TEXT,
                lead_manager_1 TEXT,
                mtge_expected_mty_date TEXT,
                sec_date_2a7 TEXT,
                benchmark_bbg_news_created REAL,
                issue_benchmark TEXT,
                issue_spread_to_benchmark REAL,
                issue_yield REAL,
                cpn REAL,
                issue_px REAL,
                original_credit_support REAL,
                current_credit_support REAL,
                orig_wal REAL,
                rating_tier TEXT,
                cusip TEXT,
                wac REAL,
                created_date TEXT,
                updated_date TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PricingHistory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT,
                pricing_date TEXT,
                issue_yield REAL,
                issue_px REAL,
                issue_spread_to_benchmark REAL,
                current_credit_support REAL,
                original_credit_support REAL,
                cpn REAL,
                created_date TEXT,
                FOREIGN KEY (ticker) REFERENCES PricingData(ticker)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS SavedSeries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                series_name TEXT UNIQUE,
                filter_criteria TEXT,
                format_settings TEXT,
                created_date TEXT,
                last_updated TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS SurveillanceReports (
                report_id TEXT PRIMARY KEY,
                deal_id TEXT,
                report_date TEXT,
                collection_period TEXT,
                total_pool_balance REAL,
                collections_amount REAL,
                charge_offs_amount REAL,
                delinquencies_30_plus REAL,
                delinquencies_60_plus REAL,
                delinquencies_90_plus REAL,
                cumulative_losses REAL,
                loss_rate REAL,
                prepayment_rate REAL,
                yield_rate REAL,
                credit_enhancement_level REAL,
                covenant_compliance TEXT,
                servicer_advance_rate REAL,
                created_date TEXT,
                document_type TEXT DEFAULT 'SURVEILLANCE',
                FOREIGN KEY (deal_id) REFERENCES ABS_Deals(deal_id)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS NoteClassPerformance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_id TEXT,
                deal_id TEXT,
                note_class TEXT,
                current_balance REAL,
                principal_payment REAL,
                interest_payment REAL,
                outstanding_balance REAL,
                factor REAL,
                yield_rate REAL,
                enhancement_level REAL,
                rating_current TEXT,
                rating_change TEXT,
                performance_date TEXT,
                created_date TEXT,
                FOREIGN KEY (report_id) REFERENCES SurveillanceReports(report_id),
                FOREIGN KEY (deal_id) REFERENCES ABS_Deals(deal_id)
            )
        """)
        
        conn.commit()
        conn.close()
    
    def detect_document_type(self, text: str) -> str:
        """Auto-detect document type based on content"""
        text_lower = text.lower()
        
        # New Issue indicators
        new_issue_keywords = [
            'offering', 'prospectus', 'issuance', 'rating committee',
            'initial', 'launch', 'pricing', 'underwriter', 'offering memorandum',
            'new issue', 'transaction overview', 'deal structure'
        ]
        
        # Surveillance indicators
        surveillance_keywords = [
            'collections', 'charge-offs', 'delinquencies', 'covenant test',
            'performance', 'surveillance', 'monthly report', 'quarterly report',
            'servicer report', 'collection period', 'loss rate', 'prepayment'
        ]
        
        new_issue_score = sum(1 for keyword in new_issue_keywords if keyword in text_lower)
        surveillance_score = sum(1 for keyword in surveillance_keywords if keyword in text_lower)
        
        if surveillance_score > new_issue_score:
            return 'SURVEILLANCE'
        else:
            return 'NEW_ISSUE'
    
    def extract_new_issue_data(self, text: str) -> Dict:
        """Extract data from new issue reports"""
        data = {
            'deal_name': self._extract_deal_name(text),
            'issuer': self._extract_issuer(text),
            'deal_type': self._extract_deal_type(text),
            'issuance_date': self._extract_issuance_date(text),
            'total_deal_size': self._extract_total_deal_size(text),
            'currency': self._extract_currency(text),
            'asset_type': self._extract_asset_type(text),
            'originator': self._extract_originator(text),
            'servicer': self._extract_servicer(text),
            'trustee': self._extract_trustee(text),
            'rating_agency': self._extract_rating_agency(text),
            'legal_final_maturity': self._extract_legal_final_maturity(text),
            'revolving_period': self._extract_revolving_period(text),
            'amortization_period': self._extract_amortization_period(text),
            'payment_frequency': self._extract_payment_frequency(text),
            'day_count_convention': self._extract_day_count_convention(text),
            'note_classes': self._extract_note_classes_enhanced(text)
        }
        return data
    
    def extract_surveillance_data(self, text: str) -> Dict:
        """Extract data from surveillance reports"""
        data = {
            'deal_id': self._extract_deal_id_from_surveillance(text),
            'report_date': self._extract_surveillance_date(text),
            'collection_period': self._extract_collection_period(text),
            'total_pool_balance': self._extract_pool_balance(text),
            'collections_amount': self._extract_collections(text),
            'charge_offs_amount': self._extract_charge_offs(text),
            'delinquencies_30_plus': self._extract_delinquencies(text, '30'),
            'delinquencies_60_plus': self._extract_delinquencies(text, '60'),
            'delinquencies_90_plus': self._extract_delinquencies(text, '90'),
            'cumulative_losses': self._extract_cumulative_losses(text),
            'loss_rate': self._extract_loss_rate(text),
            'prepayment_rate': self._extract_prepayment_rate(text),
            'yield_rate': self._extract_yield_rate(text),
            'credit_enhancement_level': self._extract_credit_enhancement(text),
            'covenant_compliance': self._extract_covenant_compliance(text),
            'servicer_advance_rate': self._extract_servicer_advance_rate(text),
            'note_class_performance': self._extract_note_class_performance(text)
        }
        return data
    
    def _extract_note_classes_enhanced(self, text: str) -> List[Dict]:
        """Enhanced note class extraction with dynamic detection"""
        note_classes = []
        
        # Enhanced patterns for note class detection
        patterns = [
            r'(Class\s+[A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*)\s+Notes?',
            r'(Series\s+[A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*)\s+Notes?',
            r'([A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*)\s+Notes?',
            r'Note\s+Class\s+([A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*)',
            r'Tranche\s+([A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*)'
        ]
        
        found_classes = set()
        for pattern in patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                class_name = match.group(1).upper()
                if class_name not in found_classes:
                    found_classes.add(class_name)
        
        # Extract details for each found class
        for class_name in sorted(found_classes):
            class_data = self._extract_class_details(text, class_name)
            if class_data:
                note_classes.append(class_data)
        
        return note_classes
    
    def _extract_class_details(self, text: str, class_name: str) -> Dict:
        """Extract detailed information for a specific note class"""
        # Find the section containing this class
        patterns = [
            rf'{re.escape(class_name)}\s+Notes?.*?(?=Class\s+[A-Z]|Series\s+[A-Z]|$)',
            rf'Class\s+{re.escape(class_name)}.*?(?=Class\s+[A-Z]|Series\s+[A-Z]|$)'
        ]
        
        section = ""
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                section = match.group(0)
                break
        
        if not section:
            return None
        
        return {
            'note_class': class_name,
            'original_balance': self._extract_amount(section, ['Original Balance', 'Initial Balance', 'Principal Amount']),
            'current_balance': self._extract_amount(section, ['Current Balance', 'Outstanding Balance']),
            'interest_rate': self._extract_rate(section, ['Interest Rate', 'Coupon Rate', 'Rate']),
            'expected_maturity': self._extract_date(section, ['Expected Maturity', 'Maturity Date']),
            'legal_final_maturity': self._extract_date(section, ['Legal Final Maturity', 'Final Maturity']),
            'rating': self._extract_rating(section),
            'subordination_level': self._determine_subordination_level(class_name),
            'payment_priority': self._determine_payment_priority(class_name),
            'enhancement_level': self._extract_enhancement_level(section)
        }
    
    def _extract_note_class_performance(self, text: str) -> List[Dict]:
        """Extract note class performance data from surveillance reports"""
        performance_data = []
        
        # Look for performance tables or sections
        performance_patterns = [
            r'Note\s+Class\s+Performance',
            r'Class\s+Performance',
            r'Tranche\s+Performance'
        ]
        
        for pattern in performance_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                # Extract performance section
                start_pos = match.start()
                section = text[start_pos:start_pos+2000]  # Get reasonable section
                
                # Find all note classes in performance section
                class_matches = re.finditer(r'(Class\s+[A-Z][A-Z0-9]*|[A-Z][A-Z0-9]*)\s+Notes?', section, re.IGNORECASE)
                
                for class_match in class_matches:
                    class_name = class_match.group(1).upper()
                    class_section = section[class_match.start():class_match.start()+500]
                    
                    performance_data.append({
                        'note_class': class_name,
                        'current_balance': self._extract_amount(class_section, ['Current Balance', 'Outstanding']),
                        'principal_payment': self._extract_amount(class_section, ['Principal Payment', 'Principal']),
                        'interest_payment': self._extract_amount(class_section, ['Interest Payment', 'Interest']),
                        'outstanding_balance': self._extract_amount(class_section, ['Outstanding Balance', 'Outstanding']),
                        'factor': self._extract_factor(class_section),
                        'yield_rate': self._extract_rate(class_section, ['Yield', 'Yield Rate']),
                        'enhancement_level': self._extract_enhancement_level(class_section),
                        'rating_current': self._extract_rating(class_section),
                        'rating_change': self._extract_rating_change(class_section)
                    })
        
        return performance_data
    
    # Surveillance-specific extraction methods
    def _extract_deal_id_from_surveillance(self, text: str) -> str:
        patterns = [
            r'Deal\s+ID[:\s]+([A-Z0-9-]+)',
            r'Transaction[:\s]+([A-Z0-9-]+)',
            r'Series[:\s]+([A-Z0-9-]+)'
        ]
        return self._extract_with_patterns(text, patterns)
    
    def _extract_surveillance_date(self, text: str) -> str:
        patterns = [
            r'Report\s+Date[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
            r'As\s+of[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
            r'Period\s+Ending[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'
        ]
        return self._extract_with_patterns(text, patterns)
    
    def _extract_collection_period(self, text: str) -> str:
        patterns = [
            r'Collection\s+Period[:\s]+([^\n]+)',
            r'Period[:\s]+([^\n]+)',
            r'Reporting\s+Period[:\s]+([^\n]+)'
        ]
        return self._extract_with_patterns(text, patterns)
    
    def _extract_pool_balance(self, text: str) -> float:
        patterns = [
            r'Total\s+Pool\s+Balance[:\s]+\$?([\d,]+\.?\d*)',
            r'Pool\s+Balance[:\s]+\$?([\d,]+\.?\d*)',
            r'Outstanding\s+Balance[:\s]+\$?([\d,]+\.?\d*)'
        ]
        return self._extract_amount_with_patterns(text, patterns)
    
    def _extract_collections(self, text: str) -> float:
        patterns = [
            r'Collections[:\s]+\$?([\d,]+\.?\d*)',
            r'Total\s+Collections[:\s]+\$?([\d,]+\.?\d*)'
        ]
        return self._extract_amount_with_patterns(text, patterns)
    
    def _extract_charge_offs(self, text: str) -> float:
        patterns = [
            r'Charge[- ]?offs?[:\s]+\$?([\d,]+\.?\d*)',
            r'Charge[- ]?Off\s+Amount[:\s]+\$?([\d,]+\.?\d*)'
        ]
        return self._extract_amount_with_patterns(text, patterns)
    
    def _extract_delinquencies(self, text: str, days: str) -> float:
        patterns = [
            rf'{days}\+?\s+days?\s+delinquent[:\s]+\$?([\d,]+\.?\d*)',
            rf'Delinquencies?\s+{days}\+[:\s]+\$?([\d,]+\.?\d*)'
        ]
        return self._extract_amount_with_patterns(text, patterns)
    
    def _extract_cumulative_losses(self, text: str) -> float:
        patterns = [
            r'Cumulative\s+Losses[:\s]+\$?([\d,]+\.?\d*)',
            r'Total\s+Losses[:\s]+\$?([\d,]+\.?\d*)'
        ]
        return self._extract_amount_with_patterns(text, patterns)
    
    def _extract_loss_rate(self, text: str) -> float:
        patterns = [
            r'Loss\s+Rate[:\s]+([\d.]+)%?',
            r'Cumulative\s+Loss\s+Rate[:\s]+([\d.]+)%?'
        ]
        return self._extract_rate_with_patterns(text, patterns)
    
    def _extract_prepayment_rate(self, text: str) -> float:
        patterns = [
            r'Prepayment\s+Rate[:\s]+([\d.]+)%?',
            r'CPR[:\s]+([\d.]+)%?'
        ]
        return self._extract_rate_with_patterns(text, patterns)
    
    def _extract_covenant_compliance(self, text: str) -> str:
        patterns = [
            r'Covenant\s+Compliance[:\s]+([^\n]+)',
            r'Covenant\s+Test[:\s]+([^\n]+)'
        ]
        result = self._extract_with_patterns(text, patterns)
        return result if result else 'Not Specified'
    
    def _extract_servicer_advance_rate(self, text: str) -> float:
        patterns = [
            r'Servicer\s+Advance\s+Rate[:\s]+([\d.]+)%?',
            r'Advance\s+Rate[:\s]+([\d.]+)%?'
        ]
        return self._extract_rate_with_patterns(text, patterns)
    
    def _extract_factor(self, text: str) -> float:
        patterns = [
            r'Factor[:\s]+([\d.]+)',
            r'Pool\s+Factor[:\s]+([\d.]+)'
        ]
        return self._extract_rate_with_patterns(text, patterns)
    
    def _extract_rating_change(self, text: str) -> str:
        patterns = [
            r'Rating\s+Change[:\s]+([^\n]+)',
            r'Rating\s+Action[:\s]+([^\n]+)'
        ]
        return self._extract_with_patterns(text, patterns)
    
    # Helper methods (keeping existing ones and adding new ones)
    def _extract_with_patterns(self, text: str, patterns: List[str]) -> str:
        """Extract text using multiple patterns"""
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return ""
    
    def _extract_amount_with_patterns(self, text: str, patterns: List[str]) -> float:
        """Extract amount using multiple patterns"""
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                amount_str = match.group(1).replace(',', '')
                try:
                    return float(amount_str)
                except ValueError:
                    continue
        return 0.0
    
    def _extract_rate_with_patterns(self, text: str, patterns: List[str]) -> float:
        """Extract rate using multiple patterns"""
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                rate_str = match.group(1)
                try:
                    return float(rate_str)
                except ValueError:
                    continue
        return 0.0
    
    # Keep existing extraction methods for new issue data
    def _extract_deal_name(self, text: str) -> str:
        patterns = [
            r'Deal Name[:\s]+([^\n]+)',
            r'Transaction[:\s]+([^\n]+)',
            r'Series[:\s]+([^\n]+)'
        ]
        return self._extract_with_patterns(text, patterns)
    
    def _extract_issuer(self, text: str) -> str:
        patterns = [
            r'Issuer[:\s]+([^\n]+)',
            r'Issuing Entity[:\s]+([^\n]+)'
        ]
        return self._extract_with_patterns(text, patterns)
    
    def _extract_deal_type(self, text: str) -> str:
        patterns = [
            r'Deal Type[:\s]+([^\n]+)',
            r'Transaction Type[:\s]+([^\n]+)'
        ]
        return self._extract_with_patterns(text, patterns)
    
    def _extract_issuance_date(self, text: str) -> str:
        patterns = [
            r'Issuance Date[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
            r'Issue Date[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'
        ]
        return self._extract_with_patterns(text, patterns)
    
    def _extract_total_deal_size(self, text: str) -> float:
        patterns = [
            r'Total Deal Size[:\s]+\$?([\d,]+\.?\d*)',
            r'Deal Size[:\s]+\$?([\d,]+\.?\d*)'
        ]
        return self._extract_amount_with_patterns(text, patterns)
    
    def _extract_currency(self, text: str) -> str:
        patterns = [
            r'Currency[:\s]+([A-Z]{3})',
            r'\$.*?([A-Z]{3})'
        ]
        result = self._extract_with_patterns(text, patterns)
        return result if result else 'USD'
    
    def _extract_asset_type(self, text: str) -> str:
        patterns = [
            r'Asset Type[:\s]+([^\n]+)',
            r'Underlying Assets[:\s]+([^\n]+)'
        ]
        return self._extract_with_patterns(text, patterns)
    
    def _extract_originator(self, text: str) -> str:
        patterns = [
            r'Originator[:\s]+([^\n]+)',
            r'Sponsor[:\s]+([^\n]+)'
        ]
        return self._extract_with_patterns(text, patterns)
    
    def _extract_servicer(self, text: str) -> str:
        patterns = [
            r'Servicer[:\s]+([^\n]+)',
            r'Master Servicer[:\s]+([^\n]+)'
        ]
        return self._extract_with_patterns(text, patterns)
    
    def _extract_trustee(self, text: str) -> str:
        patterns = [
            r'Trustee[:\s]+([^\n]+)',
            r'Indenture Trustee[:\s]+([^\n]+)'
        ]
        return self._extract_with_patterns(text, patterns)
    
    def _extract_rating_agency(self, text: str) -> str:
        patterns = [
            r'Rating Agency[:\s]+([^\n]+)',
            r'Rated by[:\s]+([^\n]+)'
        ]
        return self._extract_with_patterns(text, patterns)
    
    def _extract_legal_final_maturity(self, text: str) -> str:
        patterns = [
            r'Legal Final Maturity[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
            r'Final Maturity[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'
        ]
        return self._extract_with_patterns(text, patterns)
    
    def _extract_revolving_period(self, text: str) -> str:
        patterns = [
            r'Revolving Period[:\s]+([^\n]+)',
            r'Reinvestment Period[:\s]+([^\n]+)'
        ]
        return self._extract_with_patterns(text, patterns)
    
    def _extract_amortization_period(self, text: str) -> str:
        patterns = [
            r'Amortization Period[:\s]+([^\n]+)',
            r'Amortization[:\s]+([^\n]+)'
        ]
        return self._extract_with_patterns(text, patterns)
    
    def _extract_payment_frequency(self, text: str) -> str:
        patterns = [
            r'Payment Frequency[:\s]+([^\n]+)',
            r'Distribution Frequency[:\s]+([^\n]+)'
        ]
        return self._extract_with_patterns(text, patterns)
    
    def _extract_day_count_convention(self, text: str) -> str:
        patterns = [
            r'Day Count[:\s]+([^\n]+)',
            r'Day Count Convention[:\s]+([^\n]+)'
        ]
        return self._extract_with_patterns(text, patterns)
    
    def _extract_amount(self, text: str, keywords: List[str]) -> float:
        """Extract amount for given keywords"""
        for keyword in keywords:
            pattern = rf'{keyword}[:\s]+\$?([\d,]+\.?\d*)'
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                amount_str = match.group(1).replace(',', '')
                try:
                    return float(amount_str)
                except ValueError:
                    continue
        return 0.0
    
    def _extract_rate(self, text: str, keywords: List[str]) -> float:
        """Extract rate for given keywords"""
        for keyword in keywords:
            pattern = rf'{keyword}[:\s]+([\d.]+)%?'
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    return float(match.group(1))
                except ValueError:
                    continue
        return 0.0
    
    def _extract_date(self, text: str, keywords: List[str]) -> str:
        """Extract date for given keywords"""
        for keyword in keywords:
            pattern = rf'{keyword}[:\s]+(\d{{1,2}}[/-]\d{{1,2}}[/-]\d{{2,4}})'
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        return ""
    
    def _extract_rating(self, text: str) -> str:
        """Extract credit rating"""
        patterns = [
            r'Rating[:\s]+([A-Z]{1,3}[+-]?)',
            r'Rated[:\s]+([A-Z]{1,3}[+-]?)',
            r'([A-Z]{1,3}[+-]?)\s+rating'
        ]
        return self._extract_with_patterns(text, patterns)
    
    def _extract_enhancement_level(self, text: str) -> float:
        """Extract credit enhancement level"""
        patterns = [
            r'Enhancement[:\s]+([\d.]+)%?',
            r'Credit Enhancement[:\s]+([\d.]+)%?',
            r'Subordination[:\s]+([\d.]+)%?'
        ]
        return self._extract_rate_with_patterns(text, patterns)
    
    def _determine_subordination_level(self, class_name: str) -> int:
        """Determine subordination level based on class name"""
        # Senior classes (A, A1, A2, etc.) have lower subordination levels
        if 'A' in class_name:
            return 1
        elif 'B' in class_name:
            return 2
        elif 'C' in class_name:
            return 3
        elif 'D' in class_name:
            return 4
        else:
            return 5
    
    def process_bloomberg_pricing_sheet(self, file_path: str = None, data: pd.DataFrame = None) -> Dict:
        """Process Bloomberg pricing sheet with your specific series requirements"""
        try:
            if data is None and file_path:
                # Read Excel file
                df = pd.read_excel(file_path)
            elif data is not None:
                df = data
            else:
                raise ValueError("Either file_path or data must be provided")
            
            # Clean column names
            df.columns = df.columns.str.strip()
            
            # Your specific column mapping
            column_mapping = {
                'Ticker': 'ticker',
                'Issuer Name': 'issuer_name',
                'Deal Name': 'deal_name',
                'CMO Class': 'cmo_class',
                'Mtge Pricing Speed Date': 'mtge_pricing_speed_date',
                'First Sett Dt': 'first_sett_dt',
                'Orig Amt': 'orig_amt',
                'Mtge Deal Pricing Speed': 'mtge_deal_pricing_speed',
                'Deal Typ': 'deal_typ',
                'Description - BN Reported': 'description_bn_reported',
                'Strctd Prod Class Ast Subclas': 'strctd_prod_class_ast_subclas',
                '144A Elig': 'rule_144a_elig',
                'Lead Manager 1': 'lead_manager_1',
                'Mtge Expected Mty Date': 'mtge_expected_mty_date',
                'SEC Date (2a-7)': 'sec_date_2a7',
                'Benchmark (BBG News Created)': 'benchmark_bbg_news_created',
                'Issue Benchmark': 'issue_benchmark',
                'Issue Spread to Benchmark': 'issue_spread_to_benchmark',
                'Issue Yield': 'issue_yield',
                'Cpn': 'cpn',
                'Issue Px': 'issue_px',
                'Original Credit Support (%)': 'original_credit_support',
                'Current Credit Support (%)': 'current_credit_support',
                'Orig WAL': 'orig_wal',
                'Rating Tier': 'rating_tier',
                'CUSIP': 'cusip',
                'WAC': 'wac'
            }
            
            # Rename columns
            df_mapped = df.rename(columns=column_mapping)
            
            # Remove rows where ticker is empty
            df_mapped = df_mapped.dropna(subset=['ticker'])
            
            # Define percentage columns that need to be converted from percentage to decimal
            percentage_columns = [
                'original_credit_support',
                'current_credit_support',
                'issue_yield',
                'cpn',
                'wac',
                'mtge_deal_pricing_speed'
            ]
            
            # Convert percentage columns (divide by 100, round to 4 decimal places)
            for col in percentage_columns:
                if col in df_mapped.columns:
                    df_mapped[col] = df_mapped[col].apply(self._convert_percentage_to_decimal)
            
            # Convert date columns
            date_columns = [
                'mtge_pricing_speed_date',
                'first_sett_dt',
                'mtge_expected_mty_date',
                'sec_date_2a7'
            ]
            
            for col in date_columns:
                if col in df_mapped.columns:
                    df_mapped[col] = pd.to_datetime(df_mapped[col], errors='coerce').dt.strftime('%Y-%m-%d')
            
            # Get existing tickers from database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT ticker FROM PricingData")
            existing_tickers = set([row[0] for row in cursor.fetchall()])
            
            # Filter for new records only
            new_records = df_mapped[~df_mapped['ticker'].isin(existing_tickers)]
            
            # Process new records
            new_count = 0
            updated_count = 0
            
            for _, row in new_records.iterrows():
                try:
                    # Insert new pricing data
                    cursor.execute("""
                        INSERT INTO PricingData (
                            ticker, issuer_name, deal_name, cmo_class, mtge_pricing_speed_date,
                            first_sett_dt, orig_amt, mtge_deal_pricing_speed, deal_typ,
                            description_bn_reported, strctd_prod_class_ast_subclas, rule_144a_elig,
                            lead_manager_1, mtge_expected_mty_date, sec_date_2a7,
                            benchmark_bbg_news_created, issue_benchmark, issue_spread_to_benchmark,
                            issue_yield, cpn, issue_px, original_credit_support,
                            current_credit_support, orig_wal, rating_tier, cusip, wac,
                            created_date, updated_date
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        row.get('ticker'), row.get('issuer_name'), row.get('deal_name'),
                        row.get('cmo_class'), row.get('mtge_pricing_speed_date'), row.get('first_sett_dt'),
                        self._safe_float(row.get('orig_amt')), self._safe_float(row.get('mtge_deal_pricing_speed')),
                        row.get('deal_typ'), row.get('description_bn_reported'), row.get('strctd_prod_class_ast_subclas'),
                        row.get('rule_144a_elig'), row.get('lead_manager_1'), row.get('mtge_expected_mty_date'),
                        row.get('sec_date_2a7'), self._safe_float(row.get('benchmark_bbg_news_created')),
                        row.get('issue_benchmark'), self._safe_float(row.get('issue_spread_to_benchmark')),
                        self._safe_float(row.get('issue_yield')), self._safe_float(row.get('cpn')),
                        self._safe_float(row.get('issue_px')), self._safe_float(row.get('original_credit_support')),
                        self._safe_float(row.get('current_credit_support')), self._safe_float(row.get('orig_wal')),
                        row.get('rating_tier'), row.get('cusip'), self._safe_float(row.get('wac')),
                        datetime.now().isoformat(), datetime.now().isoformat()
                    ))
                    new_count += 1
                    
                    # Also add to pricing history
                    if row.get('ticker') and (row.get('issue_yield') or row.get('issue_px')):
                        cursor.execute("""
                            INSERT INTO PricingHistory (
                                ticker, pricing_date, issue_yield, issue_px,
                                issue_spread_to_benchmark, current_credit_support,
                                original_credit_support, cpn, created_date
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            row.get('ticker'), row.get('mtge_pricing_speed_date'),
                            self._safe_float(row.get('issue_yield')), self._safe_float(row.get('issue_px')),
                            self._safe_float(row.get('issue_spread_to_benchmark')),
                            self._safe_float(row.get('current_credit_support')),
                            self._safe_float(row.get('original_credit_support')),
                            self._safe_float(row.get('cpn')), datetime.now().isoformat()
                        ))
                
                except sqlite3.IntegrityError:
                    # Ticker already exists, update instead
                    cursor.execute("""
                        UPDATE PricingData SET
                            issue_yield = ?, issue_px = ?, current_credit_support = ?,
                            issue_spread_to_benchmark = ?, cpn = ?, wac = ?,
                            updated_date = ?
                        WHERE ticker = ?
                    """, (
                        self._safe_float(row.get('issue_yield')), self._safe_float(row.get('issue_px')),
                        self._safe_float(row.get('current_credit_support')),
                        self._safe_float(row.get('issue_spread_to_benchmark')),
                        self._safe_float(row.get('cpn')), self._safe_float(row.get('wac')),
                        datetime.now().isoformat(), row.get('ticker')
                    ))
                    updated_count += 1
                    
                    # Add to pricing history for updates too
                    if row.get('ticker') and (row.get('issue_yield') or row.get('issue_px')):
                        cursor.execute("""
                            INSERT INTO PricingHistory (
                                ticker, pricing_date, issue_yield, issue_px,
                                issue_spread_to_benchmark, current_credit_support,
                                original_credit_support, cpn, created_date
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            row.get('ticker'), row.get('mtge_pricing_speed_date'),
                            self._safe_float(row.get('issue_yield')), self._safe_float(row.get('issue_px')),
                            self._safe_float(row.get('issue_spread_to_benchmark')),
                            self._safe_float(row.get('current_credit_support')),
                            self._safe_float(row.get('original_credit_support')),
                            self._safe_float(row.get('cpn')), datetime.now().isoformat()
                        ))
            
            conn.commit()
            conn.close()
            
            return {
                'success': True,
                'new_records': new_count,
                'updated_records': updated_count,
                'total_processed': len(df_mapped),
                'existing_records': len(existing_tickers)
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'new_records': 0,
                'updated_records': 0
            }
    
    def _convert_percentage_to_decimal(self, value) -> float:
        """Convert percentage to decimal with 4 decimal places"""
        if pd.isna(value) or value == '' or value is None:
            return None
        try:
            # Handle string percentages like "25.7%"
            if isinstance(value, str):
                if '%' in value:
                    # Remove % and convert
                    numeric_value = float(value.replace('%', '').strip())
                    return round(numeric_value / 100, 4)
                else:
                    # Just a number as string
                    numeric_value = float(value)
                    # If it's > 1, assume it's already a percentage that needs conversion
                    if numeric_value > 1:
                        return round(numeric_value / 100, 4)
                    else:
                        return round(numeric_value, 4)
            else:
                # Numeric value
                numeric_value = float(value)
                # If it's > 1, assume it's a percentage that needs conversion
                if numeric_value > 1:
                    return round(numeric_value / 100, 4)
                else:
                    return round(numeric_value, 4)
        except (ValueError, TypeError):
            return None
    
    def _safe_float(self, value) -> float:
        """Safely convert value to float"""
        if pd.isna(value) or value == '' or value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def save_series_configuration(self, series_name: str, filter_criteria: Dict, format_settings: Dict) -> bool:
        """Save a series configuration for future use"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT OR REPLACE INTO SavedSeries (
                    series_name, filter_criteria, format_settings, created_date, last_updated
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                series_name, json.dumps(filter_criteria), json.dumps(format_settings),
                datetime.now().isoformat(), datetime.now().isoformat()
            ))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            st.error(f"Error saving series configuration: {str(e)}")
            return False
    
    def get_saved_series(self) -> pd.DataFrame:
        """Get all saved series configurations"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("""
            SELECT * FROM SavedSeries ORDER BY last_updated DESC
        """, conn)
        conn.close()
        return df
    
    def get_pricing_data(self, filters: Dict = None) -> pd.DataFrame:
        """Get pricing data with optional filters"""
        conn = sqlite3.connect(self.db_path)
        
        base_query = """
            SELECT * FROM PricingData
        """
        
        conditions = []
        params = []
        
        if filters:
            if filters.get('deal_types'):
                placeholders = ','.join(['?' for _ in filters['deal_types']])
                conditions.append(f"deal_typ IN ({placeholders})")
                params.extend(filters['deal_types'])
            
            if filters.get('asset_classes'):
                placeholders = ','.join(['?' for _ in filters['asset_classes']])
                conditions.append(f"strctd_prod_class_ast_subclas IN ({placeholders})")
                params.extend(filters['asset_classes'])
            
            if filters.get('rating_tiers'):
                placeholders = ','.join(['?' for _ in filters['rating_tiers']])
                conditions.append(f"rating_tier IN ({placeholders})")
                params.extend(filters['rating_tiers'])
            
            if filters.get('min_amount'):
                conditions.append("orig_amt >= ?")
                params.append(filters['min_amount'])
            
            if filters.get('max_amount'):
                conditions.append("orig_amt <= ?")
                params.append(filters['max_amount'])
            
            if filters.get('min_yield'):
                conditions.append("issue_yield >= ?")
                params.append(filters['min_yield'])
            
            if filters.get('max_yield'):
                conditions.append("issue_yield <= ?")
                params.append(filters['max_yield'])
        
        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)
        
        base_query += " ORDER BY mtge_pricing_speed_date DESC, orig_amt DESC"
        
        df = pd.read_sql_query(base_query, conn, params=params)
        conn.close()
        
        # Convert percentage decimals back to percentage display for UI
        percentage_columns = ['original_credit_support', 'current_credit_support', 'issue_yield', 'cpn', 'wac']
        for col in percentage_columns:
            if col in df.columns:
                df[f'{col}_pct'] = df[col] * 100  # Create percentage display columns
        
        return df
    
    def get_pricing_statistics(self) -> Dict:
        """Get pricing data statistics"""
        conn = sqlite3.connect(self.db_path)
        
        stats = {}
        
        # Basic counts
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM PricingData")
        stats['total_securities'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT deal_name) FROM PricingData")
        stats['unique_deals'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT issuer_name) FROM PricingData")
        stats['unique_issuers'] = cursor.fetchone()[0]
        
        # Deal type breakdown
        cursor.execute("""
            SELECT deal_typ, COUNT(*) as count 
            FROM PricingData 
            WHERE deal_typ IS NOT NULL 
            GROUP BY deal_typ 
            ORDER BY count DESC
        """)
        stats['deal_types'] = dict(cursor.fetchall())
        
        # Asset class breakdown
        cursor.execute("""
            SELECT strctd_prod_class_ast_subclas, COUNT(*) as count 
            FROM PricingData 
            WHERE strctd_prod_class_ast_subclas IS NOT NULL 
            GROUP BY strctd_prod_class_ast_subclas 
            ORDER BY count DESC
        """)
        stats['asset_classes'] = dict(cursor.fetchall())
        
        # Total issuance
        cursor.execute("SELECT SUM(orig_amt) FROM PricingData WHERE orig_amt IS NOT NULL")
        total_issuance = cursor.fetchone()[0]
        stats['total_issuance'] = total_issuance if total_issuance else 0
        
        # Average yield by deal type (convert from decimal to percentage for display)
        cursor.execute("""
            SELECT deal_typ, AVG(issue_yield) * 100 as avg_yield_pct 
            FROM PricingData 
            WHERE deal_typ IS NOT NULL AND issue_yield IS NOT NULL 
            GROUP BY deal_typ
        """)
        stats['avg_yields_by_type'] = dict(cursor.fetchall())
        
        conn.close()
        return stats
    
    def create_pricing_charts(self, filters: Dict = None) -> Dict:
        """Create charts for pricing data analysis"""
        df = self.get_pricing_data(filters)
        charts = {}
        
        if not df.empty:
            # Deal Type Distribution
            deal_type_counts = df['deal_typ'].value_counts()
            fig_deal_types = px.pie(
                values=deal_type_counts.values,
                names=deal_type_counts.index,
                title='Distribution by Deal Type'
            )
            charts['deal_types'] = fig_deal_types
            
            # Issuance Volume by Deal Type
            issuance_by_type = df.groupby('deal_typ')['orig_amt'].sum().sort_values(ascending=False)
            fig_issuance = px.bar(
                x=issuance_by_type.index,
                y=issuance_by_type.values,
                title='Total Issuance Volume by Deal Type',
                labels={'y': 'Issuance Amount ($)', 'x': 'Deal Type'}
            )
            charts['issuance_volume'] = fig_issuance
            
            # Yield Distribution (convert from decimal to percentage for display)
            if 'issue_yield' in df.columns and df['issue_yield'].notna().sum() > 0:
                df_yields = df[df['issue_yield'].notna()].copy()
                df_yields['issue_yield_pct'] = df_yields['issue_yield'] * 100
                fig_yields = px.histogram(
                    df_yields,
                    x='issue_yield_pct',
                    nbins=20,
                    title='Distribution of Issue Yields',
                    labels={'issue_yield_pct': 'Issue Yield (%)', 'count': 'Number of Securities'}
                )
                charts['yield_distribution'] = fig_yields
            
            # Yield vs Credit Support Scatter (convert to percentage for display)
            if 'issue_yield' in df.columns and 'original_credit_support' in df.columns:
                scatter_df = df[(df['issue_yield'].notna()) & (df['original_credit_support'].notna())].copy()
                if not scatter_df.empty:
                    scatter_df['issue_yield_pct'] = scatter_df['issue_yield'] * 100
                    scatter_df['credit_support_pct'] = scatter_df['original_credit_support'] * 100
                    fig_scatter = px.scatter(
                        scatter_df,
                        x='credit_support_pct',
                        y='issue_yield_pct',
                        color='deal_typ',
                        size='orig_amt',
                        hover_data=['ticker', 'deal_name'],
                        title='Issue Yield vs Credit Support',
                        labels={'credit_support_pct': 'Credit Support (%)', 'issue_yield_pct': 'Issue Yield (%)'}
                    )
                    charts['yield_vs_credit_support'] = fig_scatter
        
        return charts
    
    def save_new_issue_data(self, data: Dict) -> str:
        """Save new issue data to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Generate deal ID
        deal_id = f"DEAL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Insert deal data
        cursor.execute("""
            INSERT INTO ABS_Deals (
                deal_id, deal_name, issuer, deal_type, issuance_date, 
                total_deal_size, currency, asset_type, originator, servicer, 
                trustee, rating_agency, legal_final_maturity, revolving_period,
                amortization_period, payment_frequency, day_count_convention, 
                created_date, document_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            deal_id, data['deal_name'], data['issuer'], data['deal_type'],
            data['issuance_date'], data['total_deal_size'], data['currency'],
            data['asset_type'], data['originator'], data['servicer'],
            data['trustee'], data['rating_agency'], data['legal_final_maturity'],
            data['revolving_period'], data['amortization_period'],
            data['payment_frequency'], data['day_count_convention'],
            datetime.now().isoformat(), 'NEW_ISSUE'
        ))
        
        # Insert note class data
        for note_class in data['note_classes']:
            cursor.execute("""
                INSERT INTO NoteClasses (
                    deal_id, note_class, original_balance, current_balance,
                    interest_rate, expected_maturity, legal_final_maturity,
                    rating, subordination_level, payment_priority,
                    enhancement_level, created_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                deal_id, note_class['note_class'], note_class['original_balance'],
                note_class['current_balance'], note_class['interest_rate'],
                note_class['expected_maturity'], note_class['legal_final_maturity'],
                note_class['rating'], note_class['subordination_level'],
                note_class['payment_priority'], note_class['enhancement_level'],
                datetime.now().isoformat()
            ))
        
        conn.commit()
        conn.close()
        return deal_id
    
    def save_surveillance_data(self, data: Dict) -> str:
        """Save surveillance data to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Generate report ID
        report_id = f"SURV_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Insert surveillance report data
        cursor.execute("""
            INSERT INTO SurveillanceReports (
                report_id, deal_id, report_date, collection_period,
                total_pool_balance, collections_amount, charge_offs_amount,
                delinquencies_30_plus, delinquencies_60_plus, delinquencies_90_plus,
                cumulative_losses, loss_rate, prepayment_rate, yield_rate,
                credit_enhancement_level, covenant_compliance, servicer_advance_rate,
                created_date, document_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            report_id, data['deal_id'], data['report_date'], data['collection_period'],
            data['total_pool_balance'], data['collections_amount'], data['charge_offs_amount'],
            data['delinquencies_30_plus'], data['delinquencies_60_plus'], data['delinquencies_90_plus'],
            data['cumulative_losses'], data['loss_rate'], data['prepayment_rate'],
            data['yield_rate'], data['credit_enhancement_level'], data['covenant_compliance'],
            data['servicer_advance_rate'], datetime.now().isoformat(), 'SURVEILLANCE'
        ))
        
        # Insert note class performance data
        for performance in data['note_class_performance']:
            cursor.execute("""
                INSERT INTO NoteClassPerformance (
                    report_id, deal_id, note_class, current_balance,
                    principal_payment, interest_payment, outstanding_balance,
                    factor, yield_rate, enhancement_level, rating_current,
                    rating_change, performance_date, created_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                report_id, data['deal_id'], performance['note_class'],
                performance['current_balance'], performance['principal_payment'],
                performance['interest_payment'], performance['outstanding_balance'],
                performance['factor'], performance['yield_rate'],
                performance['enhancement_level'], performance['rating_current'],
                performance['rating_change'], data['report_date'],
                datetime.now().isoformat()
            ))
        
        conn.commit()
        conn.close()
        return report_id
    
    def get_deals_list(self) -> pd.DataFrame:
        """Get list of all deals"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("""
            SELECT deal_id, deal_name, issuer, deal_type, issuance_date, 
                   total_deal_size, currency, asset_type, created_date
            FROM ABS_Deals 
            ORDER BY created_date DESC
        """, conn)
        conn.close()
        return df
    
    def get_deal_details(self, deal_id: str) -> Dict:
        """Get detailed information for a specific deal"""
        conn = sqlite3.connect(self.db_path)
        
        # Get deal info
        deal_df = pd.read_sql_query("""
            SELECT * FROM ABS_Deals WHERE deal_id = ?
        """, conn, params=(deal_id,))
        
        # Get note classes
        notes_df = pd.read_sql_query("""
            SELECT * FROM NoteClasses WHERE deal_id = ?
        """, conn, params=(deal_id,))
        
        # Get surveillance reports
        surveillance_df = pd.read_sql_query("""
            SELECT * FROM SurveillanceReports WHERE deal_id = ?
            ORDER BY report_date DESC
        """, conn, params=(deal_id,))
        
        conn.close()
        
        return {
            'deal_info': deal_df.to_dict('records')[0] if not deal_df.empty else {},
            'note_classes': notes_df.to_dict('records'),
            'surveillance_reports': surveillance_df.to_dict('records')
        }
    
    def get_surveillance_reports(self) -> pd.DataFrame:
        """Get list of all surveillance reports"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("""
            SELECT sr.report_id, sr.deal_id, ad.deal_name, sr.report_date,
                   sr.collection_period, sr.total_pool_balance, sr.loss_rate,
                   sr.prepayment_rate, sr.covenant_compliance, sr.created_date
            FROM SurveillanceReports sr
            LEFT JOIN ABS_Deals ad ON sr.deal_id = ad.deal_id
            ORDER BY sr.report_date DESC
        """, conn)
        conn.close()
        return df
    
    def create_performance_charts(self, deal_id: str) -> Dict:
        """Create performance charts for a deal"""
        conn = sqlite3.connect(self.db_path)
        
        # Get surveillance data
        surveillance_df = pd.read_sql_query("""
            SELECT * FROM SurveillanceReports 
            WHERE deal_id = ? 
            ORDER BY report_date
        """, conn, params=(deal_id,))
        
        # Get note class performance
        performance_df = pd.read_sql_query("""
            SELECT * FROM NoteClassPerformance 
            WHERE deal_id = ? 
            ORDER BY performance_date, note_class
        """, conn, params=(deal_id,))
        
        conn.close()
        
        charts = {}
        
        if not surveillance_df.empty:
            # Pool Balance Over Time
            fig_balance = px.line(
                surveillance_df, 
                x='report_date', 
                y='total_pool_balance',
                title='Pool Balance Over Time',
                labels={'total_pool_balance': 'Pool Balance ($)', 'report_date': 'Date'}
            )
            charts['pool_balance'] = fig_balance
            
            # Loss Rate Over Time
            fig_loss = px.line(
                surveillance_df, 
                x='report_date', 
                y='loss_rate',
                title='Loss Rate Over Time',
                labels={'loss_rate': 'Loss Rate (%)', 'report_date': 'Date'}
            )
            charts['loss_rate'] = fig_loss
            
            # Delinquencies Trend
            fig_delinq = go.Figure()
            fig_delinq.add_trace(go.Scatter(
                x=surveillance_df['report_date'],
                y=surveillance_df['delinquencies_30_plus'],
                mode='lines+markers',
                name='30+ Days',
                line=dict(color='orange')
            ))
            fig_delinq.add_trace(go.Scatter(
                x=surveillance_df['report_date'],
                y=surveillance_df['delinquencies_60_plus'],
                mode='lines+markers',
                name='60+ Days',
                line=dict(color='red')
            ))
            fig_delinq.add_trace(go.Scatter(
                x=surveillance_df['report_date'],
                y=surveillance_df['delinquencies_90_plus'],
                mode='lines+markers',
                name='90+ Days',
                line=dict(color='darkred')
            ))
            fig_delinq.update_layout(
                title='Delinquencies Over Time',
                xaxis_title='Date',
                yaxis_title='Amount ($)',
                hovermode='x unified'
            )
            charts['delinquencies'] = fig_delinq
        
        if not performance_df.empty:
            # Note Class Performance
            fig_class = px.line(
                performance_df, 
                x='performance_date', 
                y='current_balance',
                color='note_class',
                title='Note Class Balances Over Time',
                labels={'current_balance': 'Current Balance ($)', 'performance_date': 'Date'}
            )
            charts['note_class_performance'] = fig_class
        
        return charts


def main():
    st.set_page_config(
        page_title="Complete ABS Document Processing System",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="collapsed"  # Hide sidebar for more space
    )
    
    # Custom CSS for Gradio-like styling
    st.markdown("""
    <style>
    .main-header {
        text-align: center;
        padding: 1rem 0;
        background: linear-gradient(90deg, #1f77b4, #ff7f0e);
        color: white;
        margin-bottom: 2rem;
        border-radius: 10px;
    }
    .quadrant-container {
        border: 2px solid #e0e0e0;
        border-radius: 10px;
        padding: 1rem;
        margin: 0.5rem;
        background-color: #f8f9fa;
        height: 400px;
        overflow-y: auto;
    }
    .quadrant-title {
        font-weight: bold;
        font-size: 1.2em;
        color: #1f77b4;
        margin-bottom: 1rem;
        text-align: center;
        border-bottom: 2px solid #1f77b4;
        padding-bottom: 0.5rem;
    }
    .input-group {
        margin-bottom: 1rem;
        padding: 0.5rem;
        background: white;
        border-radius: 5px;
        border: 1px solid #ddd;
    }
    .stButton > button {
        width: 100%;
        background: linear-gradient(90deg, #1f77b4, #ff7f0e);
        color: white;
        border: none;
        padding: 0.5rem 1rem;
        border-radius: 5px;
        font-weight: bold;
    }
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #1f77b4;
        margin: 0.5rem 0;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Main Header
    st.markdown("""
    <div class="main-header">
        <h1>🏦 Complete ABS Document Processing System</h1>
        <p>Efficient Document Processing • Surveillance Monitoring • Bloomberg Pricing Integration</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Initialize system
    if 'abs_system' not in st.session_state:
        st.session_state.abs_system = CompleteABSSystem()
    
    # Main content in quadrant layout
    col1, col2 = st.columns(2)
    
    with col1:
        # Quadrant 1: Document Processing
        with st.container():
            st.markdown('<div class="quadrant-container">', unsafe_allow_html=True)
            st.markdown('<div class="quadrant-title">📄 Document Processing</div>', unsafe_allow_html=True)
            
            # Document type selector
            doc_type = st.selectbox(
                "Document Type",
                ["New Issue Report", "Surveillance Report"],
                key="doc_type_selector"
            )
            
            # File upload
            uploaded_file = st.file_uploader(
                "Upload Document",
                type=['txt', 'pdf', 'docx', 'xlsx'],
                key="main_doc_upload"
            )
            
            if uploaded_file is not None:
                st.success(f"📁 {uploaded_file.name} uploaded")
                
                # Processing options in table format
                st.markdown("**Processing Options:**")
                options_data = {
                    "Option": ["Auto-detect Type", "Extract Note Classes", "Save to Database", "Generate Report"],
                    "Enabled": [True, True, True, False]
                }
                
                edited_options = st.data_editor(
                    pd.DataFrame(options_data),
                    use_container_width=True,
                    hide_index=True,
                    key="processing_options"
                )
                
                if st.button("🚀 Process Document", key="process_main_doc"):
                    process_document_workflow(uploaded_file, doc_type, edited_options)
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Quadrant 3: Bloomberg Pricing
        with st.container():
            st.markdown('<div class="quadrant-container">', unsafe_allow_html=True)
            st.markdown('<div class="quadrant-title">💹 Bloomberg Pricing</div>', unsafe_allow_html=True)
            
            # Pricing file upload
            pricing_file = st.file_uploader(
                "Upload Bloomberg Pricing Sheet",
                type=['xlsx', 'xls'],
                key="pricing_file_upload"
            )
            
            if pricing_file is not None:
                st.success(f"📊 {pricing_file.name} uploaded")
                
                # Processing summary table
                st.markdown("**Processing Summary:**")
                if st.button("📈 Process Pricing Data", key="process_pricing_main"):
                    result = process_bloomberg_pricing(pricing_file)
                    
                    summary_data = {
                        "Metric": ["New Records", "Updated Records", "Total Processed", "Existing Records"],
                        "Count": [result['new_records'], result['updated_records'], 
                                result['total_processed'], result['existing_records']]
                    }
                    
                    st.data_editor(
                        pd.DataFrame(summary_data),
                        use_container_width=True,
                        hide_index=True,
                        disabled=True,
                        key="pricing_summary"
                    )
            
            # Quick filters
            st.markdown("**Quick Filters:**")
            filter_data = get_filter_options()
            
            edited_filters = st.data_editor(
                filter_data,
                use_container_width=True,
                hide_index=True,
                key="quick_filters"
            )
            
            if st.button("🔍 Apply Filters", key="apply_filters_main"):
                apply_filters_workflow(edited_filters)
            
            st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        # Quadrant 2: Data Analytics
        with st.container():
            st.markdown('<div class="quadrant-container">', unsafe_allow_html=True)
            st.markdown('<div class="quadrant-title">📊 Data Analytics</div>', unsafe_allow_html=True)
            
            # Analytics dashboard
            stats = st.session_state.abs_system.get_pricing_statistics()
            
            if stats['total_securities'] > 0:
                # Metrics in table format
                metrics_data = {
                    "Metric": ["Total Securities", "Unique Deals", "Unique Issuers", "Total Issuance"],
                    "Value": [
                        f"{stats['total_securities']:,}",
                        f"{stats['unique_deals']:,}",
                        f"{stats['unique_issuers']:,}",
                        f"${stats['total_issuance']/1e9:.1f}B"
                    ]
                }
                
                st.data_editor(
                    pd.DataFrame(metrics_data),
                    use_container_width=True,
                    hide_index=True,
                    disabled=True,
                    key="analytics_metrics"
                )
                
                # Chart selection
                chart_options = {
                    "Chart Type": ["Deal Type Distribution", "Yield Distribution", "Issuance Volume", "Yield vs Credit Support"],
                    "Display": [True, False, True, False]
                }
                
                selected_charts = st.data_editor(
                    pd.DataFrame(chart_options),
                    use_container_width=True,
                    hide_index=True,
                    key="chart_selection"
                )
                
                if st.button("📈 Generate Charts", key="generate_charts_main"):
                    generate_analytics_charts(selected_charts)
            else:
                st.info("📋 No data available. Upload documents to see analytics.")
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Quadrant 4: Database Explorer
        with st.container():
            st.markdown('<div class="quadrant-container">', unsafe_allow_html=True)
            st.markdown('<div class="quadrant-title">🗄️ Database Explorer</div>', unsafe_allow_html=True)
            
            # Database selection
            db_table = st.selectbox(
                "Select Table",
                ["Pricing Data", "Deal Information", "Surveillance Reports", "Note Classes"],
                key="db_table_selector"
            )
            
            # Query builder in table format
            if db_table == "Pricing Data":
                query_data = get_pricing_query_builder()
            elif db_table == "Deal Information":
                query_data = get_deals_query_builder()
            elif db_table == "Surveillance Reports":
                query_data = get_surveillance_query_builder()
            else:
                query_data = get_note_classes_query_builder()
            
            edited_query = st.data_editor(
                query_data,
                use_container_width=True,
                hide_index=True,
                key=f"query_builder_{db_table.lower().replace(' ', '_')}"
            )
            
            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("🔍 Query Data", key="query_data_main"):
                    execute_database_query(db_table, edited_query)
            
            with col_b:
                if st.button("📥 Export CSV", key="export_csv_main"):
                    export_data_workflow(db_table, edited_query)
            
            st.markdown('</div>', unsafe_allow_html=True)
    
    # Results section (full width below quadrants)
    if 'show_results' in st.session_state and st.session_state.show_results:
        st.markdown("---")
        st.markdown("## 📋 Results")
        
        if 'results_data' in st.session_state:
            display_results_section()


def process_document_workflow(uploaded_file, doc_type, options):
    """Process document with workflow"""
    with st.spinner("Processing document..."):
        try:
            # Read file content
            if uploaded_file.type == "text/plain":
                text_content = str(uploaded_file.read(), "utf-8")
            else:
                # Use sample data for demo
                if doc_type == "New Issue Report":
                    text_content = get_sample_new_issue_content()
                else:
                    text_content = get_sample_surveillance_content()
            
            # Auto-detect if enabled
            if options.iloc[0]['Enabled']:  # Auto-detect Type
                detected_type = st.session_state.abs_system.detect_document_type(text_content)
                st.info(f"🔍 Detected: {detected_type}")
            
            # Process based on type
            if doc_type == "New Issue Report":
                extracted_data = st.session_state.abs_system.extract_new_issue_data(text_content)
                if options.iloc[2]['Enabled']:  # Save to Database
                    deal_id = st.session_state.abs_system.save_new_issue_data(extracted_data)
                    st.success(f"✅ Saved as Deal ID: {deal_id}")
            else:
                extracted_data = st.session_state.abs_system.extract_surveillance_data(text_content)
                if options.iloc[2]['Enabled']:  # Save to Database
                    report_id = st.session_state.abs_system.save_surveillance_data(extracted_data)
                    st.success(f"✅ Saved as Report ID: {report_id}")
            
            # Store results for display
            st.session_state.show_results = True
            st.session_state.results_data = extracted_data
            st.session_state.results_type = doc_type
            
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")


def process_bloomberg_pricing(pricing_file):
    """Process Bloomberg pricing file"""
    try:
        df = pd.read_excel(pricing_file)
        result = st.session_state.abs_system.process_bloomberg_pricing_sheet(data=df)
        
        if result['success']:
            st.success("✅ Pricing data processed successfully!")
        else:
            st.error(f"❌ Error: {result['error']}")
        
        return result
    except Exception as e:
        st.error(f"❌ Error processing pricing file: {str(e)}")
        return {'new_records': 0, 'updated_records': 0, 'total_processed': 0, 'existing_records': 0}


def get_filter_options():
    """Get filter options for pricing data"""
    try:
        all_data = st.session_state.abs_system.get_pricing_data()
        if not all_data.empty:
            return pd.DataFrame({
                "Filter": ["Deal Type", "Asset Class", "Rating Tier", "Min Amount", "Max Amount"],
                "Value": ["AUTOS", "Auto Loan", "AAA", "1000000", "1000000000"],
                "Enabled": [False, False, False, False, False]
            })
        else:
            return pd.DataFrame({
                "Filter": ["Deal Type", "Asset Class", "Rating Tier", "Min Amount", "Max Amount"],
                "Value": ["", "", "", "", ""],
                "Enabled": [False, False, False, False, False]
            })
    except:
        return pd.DataFrame({
            "Filter": ["Deal Type", "Asset Class", "Rating Tier", "Min Amount", "Max Amount"],
            "Value": ["", "", "", "", ""],
            "Enabled": [False, False, False, False, False]
        })


def apply_filters_workflow(filters_df):
    """Apply filters and show results"""
    try:
        # Build filter dictionary from table
        filters = {}
        for _, row in filters_df.iterrows():
            if row['Enabled'] and row['Value']:
                filter_name = row['Filter'].lower().replace(' ', '_')
                if filter_name in ['min_amount', 'max_amount']:
                    filters[filter_name] = float(row['Value'])
                else:
                    filters[f"{filter_name}s"] = [row['Value']]
        
        # Apply filters
        filtered_data = st.session_state.abs_system.get_pricing_data(filters)
        
        # Store results
        st.session_state.show_results = True
        st.session_state.results_data = filtered_data
        st.session_state.results_type = "Filtered Pricing Data"
        
        st.success(f"✅ Found {len(filtered_data)} records matching filters")
        
    except Exception as e:
        st.error(f"❌ Error applying filters: {str(e)}")


def generate_analytics_charts(charts_df):
    """Generate and display analytics charts"""
    try:
        enabled_charts = charts_df[charts_df['Display']]['Chart Type'].tolist()
        charts = st.session_state.abs_system.create_pricing_charts()
        
        # Store charts for display
        st.session_state.show_results = True
        st.session_state.results_data = charts
        st.session_state.results_type = "Analytics Charts"
        st.session_state.enabled_charts = enabled_charts
        
        st.success(f"✅ Generated {len(enabled_charts)} charts")
        
    except Exception as e:
        st.error(f"❌ Error generating charts: {str(e)}")


def get_pricing_query_builder():
    """Get pricing data query builder options"""
    return pd.DataFrame({
        "Field": ["ticker", "issuer_name", "deal_typ", "rating_tier", "issue_yield"],
        "Operator": ["LIKE", "=", "IN", "=", ">="],
        "Value": ["", "", "", "", ""],
        "Include": [True, True, True, False, False]
    })


def get_deals_query_builder():
    """Get deals query builder options"""
    return pd.DataFrame({
        "Field": ["deal_name", "issuer", "deal_type", "total_deal_size"],
        "Operator": ["LIKE", "=", "=", ">="],
        "Value": ["", "", "", ""],
        "Include": [True, True, False, False]
    })


def get_surveillance_query_builder():
    """Get surveillance query builder options"""
    return pd.DataFrame({
        "Field": ["deal_id", "report_date", "loss_rate", "covenant_compliance"],
        "Operator": ["=", ">=", "<=", "="],
        "Value": ["", "", "", ""],
        "Include": [True, False, False, False]
    })


def get_note_classes_query_builder():
    """Get note classes query builder options"""
    return pd.DataFrame({
        "Field": ["deal_id", "note_class", "rating", "interest_rate"],
        "Operator": ["=", "LIKE", "=", ">="],
        "Value": ["", "", "", ""],
        "Include": [True, True, False, False]
    })


def execute_database_query(table_name, query_df):
    """Execute database query based on table and filters"""
    try:
        if table_name == "Pricing Data":
            data = st.session_state.abs_system.get_pricing_data()
        elif table_name == "Deal Information":
            data = st.session_state.abs_system.get_deals_list()
        elif table_name == "Surveillance Reports":
            data = st.session_state.abs_system.get_surveillance_reports()
        else:
            # Note classes - would need a separate method
            data = pd.DataFrame()
        
        # Store results
        st.session_state.show_results = True
        st.session_state.results_data = data
        st.session_state.results_type = f"{table_name} Query Results"
        
        st.success(f"✅ Retrieved {len(data)} records from {table_name}")
        
    except Exception as e:
        st.error(f"❌ Error querying {table_name}: {str(e)}")


def export_data_workflow(table_name, query_df):
    """Export data as CSV"""
    try:
        if table_name == "Pricing Data":
            data = st.session_state.abs_system.get_pricing_data()
        elif table_name == "Deal Information":
            data = st.session_state.abs_system.get_deals_list()
        elif table_name == "Surveillance Reports":
            data = st.session_state.abs_system.get_surveillance_reports()
        else:
            data = pd.DataFrame()
        
        if not data.empty:
            csv = data.to_csv(index=False)
            st.download_button(
                label=f"📥 Download {table_name} CSV",
                data=csv,
                file_name=f"{table_name.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime='text/csv'
            )
        else:
            st.warning("No data to export")
            
    except Exception as e:
        st.error(f"❌ Error exporting data: {str(e)}")


def display_results_section():
    """Display results in the bottom section"""
    results_data = st.session_state.results_data
    results_type = st.session_state.results_type
    
    if results_type == "Analytics Charts":
        # Display charts
        enabled_charts = st.session_state.get('enabled_charts', [])
        
        if 'Deal Type Distribution' in enabled_charts and 'deal_types' in results_data:
            st.plotly_chart(results_data['deal_types'], use_container_width=True)
        
        if 'Yield Distribution' in enabled_charts and 'yield_distribution' in results_data:
            st.plotly_chart(results_data['yield_distribution'], use_container_width=True)
        
        if 'Issuance Volume' in enabled_charts and 'issuance_volume' in results_data:
            st.plotly_chart(results_data['issuance_volume'], use_container_width=True)
        
        if 'Yield vs Credit Support' in enabled_charts and 'yield_vs_credit_support' in results_data:
            st.plotly_chart(results_data['yield_vs_credit_support'], use_container_width=True)
    
    elif isinstance(results_data, pd.DataFrame):
        # Display data table
        st.markdown(f"### {results_type}")
        
        if not results_data.empty:
            # Limit display for performance
            display_data = results_data.head(100)
            
            st.dataframe(
                display_data,
                use_container_width=True,
                hide_index=True
            )
            
            if len(results_data) > 100:
                st.info(f"Showing first 100 of {len(results_data)} records")
            
            # Export option
            csv = results_data.to_csv(index=False)
            st.download_button(
                label="📥 Download Full Results CSV",
                data=csv,
                file_name=f"{results_type.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime='text/csv'
            )
        else:
            st.info("No data to display")
    
    else:
        # Display extracted document data
        st.markdown(f"### {results_type} - Extracted Data")
        
        # Convert to display format
        if isinstance(results_data, dict):
            display_df = pd.DataFrame([(k, v) for k, v in results_data.items()], 
                                    columns=['Field', 'Value'])
            st.dataframe(display_df, use_container_width=True, hide_index=True)


def get_sample_new_issue_content():
    """Sample new issue content for demo"""
    return """
    DEAL INFORMATION
    Deal Name: Sample ABS Deal 2024-1
    Issuer: Sample Trust
    Deal Type: Auto Loan ABS
    Total Deal Size: $500,000,000
    Asset Type: Auto Loans
    Issuance Date: 01/15/2024
    Originator: Sample Auto Finance
    Servicer: Sample Servicing Co
    Trustee: Sample Trust Company
    Rating Agency: Moody's
    
    NOTE CLASSES
    Class A Notes: $400,000,000 @ 3.25%
    Class B Notes: $75,000,000 @ 4.50%
    Class C Notes: $25,000,000 @ 6.75%
    
    Legal Final Maturity: 01/15/2029
    Payment Frequency: Monthly
    """


def get_sample_surveillance_content():
    """Sample surveillance content for demo"""
    return """
    SURVEILLANCE REPORT
    Deal ID: DEAL_20240115_120000
    Report Date: 03/15/2024
    Collection Period: February 2024
    
    POOL PERFORMANCE
    Total Pool Balance: $450,000,000
    Collections: $15,000,000
    Charge-offs: $500,000
    
    DELINQUENCIES
    30+ Days Delinquent: $2,000,000
    60+ Days Delinquent: $800,000
    90+ Days Delinquent: $300,000
    
    RATES
    Loss Rate: 1.25%
    Prepayment Rate: 8.5%
    Credit Enhancement: 15.0%
    Covenant Compliance: In Compliance
    """
    
    if page == "📄 Document Processing":
        st.header("Document Processing")
        
        # Document type tabs
        tab1, tab2 = st.tabs(["🆕 New Issue Reports", "📊 Surveillance Reports"])
        
        with tab1:
            st.subheader("New Issue Report Processing")
            st.markdown("Upload and process new ABS deal documents")
            
            uploaded_file = st.file_uploader(
                "Upload New Issue Document",
                type=['txt', 'pdf', 'docx'],
                help="Upload new issue reports, prospectuses, or offering memoranda",
                key="new_issue_upload"
            )
            
            if uploaded_file is not None:
                # Read file content
                if uploaded_file.type == "text/plain":
                    text_content = str(uploaded_file.read(), "utf-8")
                else:
                    text_content = "Sample new issue content for demo"
                
                # Auto-detect document type
                doc_type = st.session_state.abs_system.detect_document_type(text_content)
                st.info(f"🔍 Detected Document Type: **{doc_type}**")
                
                if st.button("Process New Issue Document", key="process_new_issue"):
                    with st.spinner("Processing new issue document..."):
                        if doc_type == 'NEW_ISSUE':
                            extracted_data = st.session_state.abs_system.extract_new_issue_data(text_content)
                            deal_id = st.session_state.abs_system.save_new_issue_data(extracted_data)
                            
                            st.success(f"✅ New issue processed successfully! Deal ID: {deal_id}")
                            
                            # Display extracted data
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.subheader("Deal Information")
                                st.write(f"**Deal Name:** {extracted_data['deal_name']}")
                                st.write(f"**Issuer:** {extracted_data['issuer']}")
                                st.write(f"**Deal Type:** {extracted_data['deal_type']}")
                                st.write(f"**Total Size:** ${extracted_data['total_deal_size']:,.0f}")
                                st.write(f"**Asset Type:** {extracted_data['asset_type']}")
                                st.write(f"**Issuance Date:** {extracted_data['issuance_date']}")
                            
                            with col2:
                                st.subheader("Note Classes")
                                if extracted_data['note_classes']:
                                    for note_class in extracted_data['note_classes']:
                                        st.write(f"**{note_class['note_class']}:** ${note_class['original_balance']:,.0f} @ {note_class['interest_rate']:.2f}%")
                                else:
                                    st.write("No note classes detected")
                        else:
                            st.warning("⚠️ This appears to be a surveillance report. Please use the Surveillance Reports tab.")
        
        with tab2:
            st.subheader("Surveillance Report Processing")
            st.markdown("Upload and process ongoing surveillance and performance reports")
            
            uploaded_file = st.file_uploader(
                "Upload Surveillance Document",
                type=['txt', 'pdf', 'docx'],
                help="Upload surveillance reports, monthly/quarterly performance reports",
                key="surveillance_upload"
            )
            
            if uploaded_file is not None:
                try:
                    # Read file content
                    if uploaded_file.type == "text/plain":
                        text_content = str(uploaded_file.read(), "utf-8")
                    else:
                        # For demo purposes, use sample surveillance content
                        text_content = """
                        SURVEILLANCE REPORT
                        Deal ID: DEAL_20240115_120000
                        Report Date: 03/15/2024
                        Collection Period: February 2024
                        
                        POOL PERFORMANCE
                        Total Pool Balance: $450,000,000
                        Collections: $15,000,000
                        Charge-offs: $500,000
                        
                        DELINQUENCIES
                        30+ Days Delinquent: $2,000,000
                        60+ Days Delinquent: $800,000
                        90+ Days Delinquent: $300,000
                        
                        RATES
                        Loss Rate: 1.25%
                        Prepayment Rate: 8.5%
                        Credit Enhancement: 15.0%
                        Covenant Compliance: In Compliance
                        """
                    
                    # Auto-detect document type
                    doc_type = st.session_state.abs_system.detect_document_type(text_content)
                    st.info(f"🔍 Detected Document Type: **{doc_type}**")
                    
                    if st.button("Process Surveillance Document", key="process_surveillance"):
                        with st.spinner("Processing surveillance document..."):
                            try:
                                if doc_type == 'SURVEILLANCE':
                                    extracted_data = st.session_state.abs_system.extract_surveillance_data(text_content)
                                    
                                    # Ensure deal_id exists or create a default one
                                    if not extracted_data.get('deal_id'):
                                        extracted_data['deal_id'] = f"DEAL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                                    
                                    report_id = st.session_state.abs_system.save_surveillance_data(extracted_data)
                                    
                                    st.success(f"✅ Surveillance report processed successfully! Report ID: {report_id}")
                                    
                                    # Display extracted data
                                    col1, col2 = st.columns(2)
                                    
                                    with col1:
                                        st.subheader("Pool Performance")
                                        st.write(f"**Deal ID:** {extracted_data.get('deal_id', 'N/A')}")
                                        st.write(f"**Report Date:** {extracted_data.get('report_date', 'N/A')}")
                                        st.write(f"**Pool Balance:** ${extracted_data.get('total_pool_balance', 0):,.0f}")
                                        st.write(f"**Collections:** ${extracted_data.get('collections_amount', 0):,.0f}")
                                        st.write(f"**Charge-offs:** ${extracted_data.get('charge_offs_amount', 0):,.0f}")
                                        st.write(f"**Loss Rate:** {extracted_data.get('loss_rate', 0):.2f}%")
                                    
                                    with col2:
                                        st.subheader("Delinquencies")
                                        st.write(f"**30+ Days:** ${extracted_data.get('delinquencies_30_plus', 0):,.0f}")
                                        st.write(f"**60+ Days:** ${extracted_data.get('delinquencies_60_plus', 0):,.0f}")
                                        st.write(f"**90+ Days:** ${extracted_data.get('delinquencies_90_plus', 0):,.0f}")
                                        st.write(f"**Covenant Compliance:** {extracted_data.get('covenant_compliance', 'N/A')}")
                                        st.write(f"**Credit Enhancement:** {extracted_data.get('credit_enhancement_level', 0):.2f}%")
                                else:
                                    st.warning("⚠️ This appears to be a new issue report. Please use the New Issue Reports tab.")
                            
                            except Exception as e:
                                st.error(f"❌ Error processing surveillance document: {str(e)}")
                                st.write("**Debug Info:**")
                                st.write(f"Document type detected: {doc_type}")
                                st.write(f"Text length: {len(text_content)}")
                
                except Exception as e:
                    st.error(f"❌ Error reading file: {str(e)}")
    
    elif page == "📊 Deal Dashboard":
        st.header("Deal Dashboard")
        
        # Get deals list
        deals_df = st.session_state.abs_system.get_deals_list()
        
        if not deals_df.empty:
            st.subheader("Active Deals")
            
            # Display deals table
            st.dataframe(
                deals_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "deal_id": "Deal ID",
                    "deal_name": "Deal Name",
                    "issuer": "Issuer",
                    "deal_type": "Type",
                    "issuance_date": "Issuance Date",
                    "total_deal_size": st.column_config.NumberColumn(
                        "Deal Size", format="$%.0f"
                    ),
                    "currency": "Currency",
                    "asset_type": "Asset Type",
                    "created_date": "Created"
                }
            )
            
            # Deal summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Deals", len(deals_df))
            
            with col2:
                total_size = deals_df['total_deal_size'].sum()
                st.metric("Total Deal Size", f"${total_size:,.0f}")
            
            with col3:
                unique_issuers = deals_df['issuer'].nunique()
                st.metric("Unique Issuers", unique_issuers)
            
            with col4:
                avg_deal_size = deals_df['total_deal_size'].mean()
                st.metric("Average Deal Size", f"${avg_deal_size:,.0f}")
        else:
            st.info("No deals found. Upload some documents to get started!")
    
    elif page == "📈 Surveillance Reports":
        st.header("Surveillance Reports")
        
        # Get surveillance reports
        surveillance_df = st.session_state.abs_system.get_surveillance_reports()
        
        if not surveillance_df.empty:
            st.subheader("Recent Surveillance Reports")
            
            # Display surveillance reports table
            st.dataframe(
                surveillance_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "report_id": "Report ID",
                    "deal_id": "Deal ID",
                    "deal_name": "Deal Name",
                    "report_date": "Report Date",
                    "collection_period": "Collection Period",
                    "total_pool_balance": st.column_config.NumberColumn(
                        "Pool Balance", format="$%.0f"
                    ),
                    "loss_rate": st.column_config.NumberColumn(
                        "Loss Rate", format="%.2f%%"
                    ),
                    "prepayment_rate": st.column_config.NumberColumn(
                        "Prepayment Rate", format="%.2f%%"
                    ),
                    "covenant_compliance": "Covenant Status"
                }
            )
            
            # Surveillance summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Reports", len(surveillance_df))
            
            with col2:
                total_pool_balance = surveillance_df['total_pool_balance'].sum()
                st.metric("Total Pool Balance", f"${total_pool_balance:,.0f}")
            
            with col3:
                avg_loss_rate = surveillance_df['loss_rate'].mean()
                st.metric("Average Loss Rate", f"{avg_loss_rate:.2f}%")
            
            with col4:
                avg_prepayment_rate = surveillance_df['prepayment_rate'].mean()
                st.metric("Average Prepayment Rate", f"{avg_prepayment_rate:.2f}%")
        else:
            st.info("No surveillance reports found. Upload some surveillance documents to get started!")
    
    elif page == "💹 Bloomberg Pricing":
        st.header("Bloomberg Pricing Data")
        
        # Pricing data tabs
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Upload Data", "📈 Analytics", "⚙️ Saved Series", "📋 Data Explorer"])
        
        with tab1:
            st.subheader("Upload Bloomberg Pricing Sheet")
            st.markdown("Upload your weekly Bloomberg ABS pricing Excel file to update the database")
            
            uploaded_pricing_file = st.file_uploader(
                "Upload Bloomberg Pricing File",
                type=['xlsx', 'xls'],
                help="Upload your Bloomberg ABS pricing spreadsheet",
                key="pricing_upload"
            )
            
            if uploaded_pricing_file is not None:
                if st.button("Process Pricing Data", key="process_pricing"):
                    with st.spinner("Processing Bloomberg pricing data..."):
                        try:
                            # Read the uploaded file
                            df = pd.read_excel(uploaded_pricing_file)
                            
                            # Process the data
                            result = st.session_state.abs_system.process_bloomberg_pricing_sheet(data=df)
                            
                            if result['success']:
                                st.success(f"✅ Pricing data processed successfully!")
                                
                                col1, col2, col3, col4 = st.columns(4)
                                with col1:
                                    st.metric("New Records", result['new_records'])
                                with col2:
                                    st.metric("Updated Records", result['updated_records'])
                                with col3:
                                    st.metric("Total Processed", result['total_processed'])
                                with col4:
                                    st.metric("Existing Records", result['existing_records'])
                                
                                if result['new_records'] > 0 or result['updated_records'] > 0:
                                    st.info(f"📊 Database updated with {result['new_records']} new securities and {result['updated_records']} updated records")
                                else:
                                    st.info("ℹ️ No new records to add - all securities already in database")
                            else:
                                st.error(f"❌ Error processing pricing data: {result['error']}")
                        
                        except Exception as e:
                            st.error(f"❌ Error reading file: {str(e)}")
        
        with tab2:
            st.subheader("Pricing Analytics")
            
            # Get pricing statistics
            stats = st.session_state.abs_system.get_pricing_statistics()
            
            if stats['total_securities'] > 0:
                # Summary metrics
                st.subheader("Market Overview")
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total Securities", f"{stats['total_securities']:,}")
                
                with col2:
                    st.metric("Unique Deals", f"{stats['unique_deals']:,}")
                
                with col3:
                    st.metric("Unique Issuers", f"{stats['unique_issuers']:,}")
                
                with col4:
                    st.metric("Total Issuance", f"${stats['total_issuance']/1e9:.1f}B")
                
                # Filters for analytics
                st.subheader("Filter Data")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    # Get unique values for filters
                    all_data = st.session_state.abs_system.get_pricing_data()
                    
                    selected_deal_types = st.multiselect(
                        "Deal Types",
                        options=sorted(all_data['deal_typ'].dropna().unique()) if not all_data.empty else [],
                        default=[]
                    )
                
                with col2:
                    selected_asset_classes = st.multiselect(
                        "Asset Classes",
                        options=sorted(all_data['strctd_prod_class_ast_subclas'].dropna().unique()) if not all_data.empty else [],
                        default=[]
                    )
                
                with col3:
                    selected_rating_tiers = st.multiselect(
                        "Rating Tiers",
                        options=sorted(all_data['rating_tier'].dropna().unique()) if not all_data.empty else [],
                        default=[]
                    )
                
                # Build filters
                filters = {}
                if selected_deal_types:
                    filters['deal_types'] = selected_deal_types
                if selected_asset_classes:
                    filters['asset_classes'] = selected_asset_classes
                if selected_rating_tiers:
                    filters['rating_tiers'] = selected_rating_tiers
                
                # Create charts
                charts = st.session_state.abs_system.create_pricing_charts(filters)
                
                # Display charts
                if charts:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if 'deal_types' in charts:
                            st.plotly_chart(charts['deal_types'], use_container_width=True)
                        if 'yield_distribution' in charts:
                            st.plotly_chart(charts['yield_distribution'], use_container_width=True)
                    
                    with col2:
                        if 'issuance_volume' in charts:
                            st.plotly_chart(charts['issuance_volume'], use_container_width=True)
                        if 'yield_vs_credit_support' in charts:
                            st.plotly_chart(charts['yield_vs_credit_support'], use_container_width=True)
            else:
                st.info("No pricing data available. Upload a Bloomberg pricing file to get started!")
        
        with tab3:
            st.subheader("Saved Series Configuration")
            st.markdown("Configure and save custom series for regular monitoring")
            
            # Create new series
            with st.expander("Create New Series", expanded=False):
                series_name = st.text_input("Series Name", placeholder="e.g., Auto Loan AAA Securities")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**Filter Criteria**")
                    deal_types_filter = st.multiselect("Deal Types", options=["AUTOS", "CREDIT CARD", "EQUIPMENT", "STUDENT LOAN"])
                    asset_classes_filter = st.multiselect("Asset Classes", options=["Auto Loan", "Credit Card", "Equipment"])
                    rating_filter = st.multiselect("Rating Tiers", options=["AAA", "AA", "A", "BBB"])
                
                with col2:
                    st.write("**Format Settings**")
                    include_columns = st.multiselect(
                        "Include Columns",
                        options=["ticker", "issuer_name", "deal_name", "cmo_class", "issue_yield", "issue_price", "original_amount"],
                        default=["ticker", "issuer_name", "deal_name", "cmo_class", "issue_yield"]
                    )
                    sort_by = st.selectbox("Sort By", options=["issue_yield", "original_amount", "pricing_speed_date"])
                
                if st.button("Save Series Configuration"):
                    if series_name:
                        filter_criteria = {
                            'deal_types': deal_types_filter,
                            'asset_classes': asset_classes_filter,
                            'rating_tiers': rating_filter
                        }
                        format_settings = {
                            'include_columns': include_columns,
                            'sort_by': sort_by
                        }
                        
                        success = st.session_state.abs_system.save_series_configuration(
                            series_name, filter_criteria, format_settings
                        )
                        
                        if success:
                            st.success(f"✅ Series '{series_name}' saved successfully!")
                        else:
                            st.error("❌ Error saving series configuration")
                    else:
                        st.error("Please enter a series name")
            
            # Display saved series
            saved_series = st.session_state.abs_system.get_saved_series()
            
            if not saved_series.empty:
                st.subheader("Saved Series")
                
                for _, series in saved_series.iterrows():
                    with st.expander(f"📊 {series['series_name']}", expanded=False):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write("**Filter Criteria:**")
                            criteria = json.loads(series['filter_criteria'])
                            for key, value in criteria.items():
                                if value:
                                    st.write(f"- {key}: {', '.join(value)}")
                        
                        with col2:
                            st.write("**Format Settings:**")
                            settings = json.loads(series['format_settings'])
                            st.write(f"- Columns: {', '.join(settings.get('include_columns', []))}")
                            st.write(f"- Sort by: {settings.get('sort_by', 'N/A')}")
                        
                        st.write(f"**Last Updated:** {series['last_updated']}")
                        
                        if st.button(f"Generate Report", key=f"generate_{series['id']}"):
                            # Generate report for this series
                            criteria = json.loads(series['filter_criteria'])
                            settings = json.loads(series['format_settings'])
                            
                            filtered_data = st.session_state.abs_system.get_pricing_data(criteria)
                            
                            if not filtered_data.empty:
                                # Select only requested columns
                                display_columns = settings.get('include_columns', filtered_data.columns.tolist())
                                available_columns = [col for col in display_columns if col in filtered_data.columns]
                                
                                display_df = filtered_data[available_columns]
                                
                                # Sort data
                                sort_col = settings.get('sort_by', 'pricing_speed_date')
                                if sort_col in display_df.columns:
                                    display_df = display_df.sort_values(sort_col, ascending=False)
                                
                                st.dataframe(display_df, use_container_width=True, hide_index=True)
                                
                                # Download button
                                csv = display_df.to_csv(index=False)
                                st.download_button(
                                    label=f"Download {series['series_name']} CSV",
                                    data=csv,
                                    file_name=f"{series['series_name'].replace(' ', '_')}_report.csv",
                                    mime='text/csv'
                                )
                            else:
                                st.info("No data matches the filter criteria")
            else:
                st.info("No saved series found. Create your first series above!")
        
        with tab4:
            st.subheader("Pricing Data Explorer")
            
            # Get all pricing data
            all_pricing_data = st.session_state.abs_system.get_pricing_data()
            
            if not all_pricing_data.empty:
                # Search and filter options
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    search_term = st.text_input("Search (Ticker, Issuer, Deal Name)", placeholder="Enter search term...")
                
                with col2:
                    show_count = st.selectbox("Show Records", options=[25, 50, 100, 250, 500], index=1)
                
                with col3:
                    sort_column = st.selectbox(
                        "Sort By", 
                        options=['pricing_speed_date', 'original_amount', 'issue_yield', 'ticker'],
                        index=0
                    )
                
                # Apply search filter
                if search_term:
                    mask = (
                        all_pricing_data['ticker'].str.contains(search_term, case=False, na=False) |
                        all_pricing_data['issuer_name'].str.contains(search_term, case=False, na=False) |
                        all_pricing_data['deal_name'].str.contains(search_term, case=False, na=False)
                    )
                    filtered_data = all_pricing_data[mask]
                else:
                    filtered_data = all_pricing_data
                
                # Sort and limit
                if sort_column in filtered_data.columns:
                    filtered_data = filtered_data.sort_values(sort_column, ascending=False)
                
                filtered_data = filtered_data.head(show_count)
                
                # Display data
                st.dataframe(
                    filtered_data,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "ticker": "Ticker",
                        "issuer_name": "Issuer",
                        "deal_name": "Deal Name",
                        "cmo_class": "Class",
                        "orig_amt": st.column_config.NumberColumn("Original Amount", format="$%.0f"),
                        "issue_yield": st.column_config.NumberColumn("Issue Yield", format="%.4f"),
                        "issue_yield_pct": st.column_config.NumberColumn("Issue Yield %", format="%.2f%%"),
                        "issue_px": st.column_config.NumberColumn("Issue Price", format="%.3f"),
                        "original_credit_support": st.column_config.NumberColumn("Credit Support", format="%.4f"),
                        "original_credit_support_pct": st.column_config.NumberColumn("Credit Support %", format="%.2f%%"),
                        "deal_typ": "Deal Type",
                        "mtge_pricing_speed_date": "Pricing Date",
                        "cpn": st.column_config.NumberColumn("Coupon", format="%.4f"),
                        "cpn_pct": st.column_config.NumberColumn("Coupon %", format="%.2f%%"),
                        "cusip": "CUSIP",
                        "wac": st.column_config.NumberColumn("WAC", format="%.4f"),
                        "wac_pct": st.column_config.NumberColumn("WAC %", format="%.2f%%")
                    }
                )
                
                # Download button
                csv = filtered_data.to_csv(index=False)
                st.download_button(
                    label="Download Current View as CSV",
                    data=csv,
                    file_name=f"pricing_data_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime='text/csv'
                )
                
                st.info(f"Showing {len(filtered_data)} of {len(all_pricing_data)} total records")
            else:
                st.info("No pricing data available. Upload a Bloomberg pricing file to get started!")
        st.header("Deal Details")
        
        # Get deals for selection
        deals_df = st.session_state.abs_system.get_deals_list()
        
        if not deals_df.empty:
            # Deal selection
            selected_deal = st.selectbox(
                "Select a Deal",
                deals_df['deal_id'].tolist(),
                format_func=lambda x: f"{x} - {deals_df[deals_df['deal_id']==x]['deal_name'].iloc[0]}"
            )
            
            if selected_deal:
                # Get deal details
                deal_details = st.session_state.abs_system.get_deal_details(selected_deal)
                
                # Display deal information
                st.subheader("Deal Information")
                deal_info = deal_details['deal_info']
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Deal Name:** {deal_info.get('deal_name', 'N/A')}")
                    st.write(f"**Issuer:** {deal_info.get('issuer', 'N/A')}")
                    st.write(f"**Deal Type:** {deal_info.get('deal_type', 'N/A')}")
                    st.write(f"**Asset Type:** {deal_info.get('asset_type', 'N/A')}")
                    st.write(f"**Originator:** {deal_info.get('originator', 'N/A')}")
                
                with col2:
                    st.write(f"**Total Deal Size:** ${deal_info.get('total_deal_size', 0):,.0f}")
                    st.write(f"**Currency:** {deal_info.get('currency', 'N/A')}")
                    st.write(f"**Issuance Date:** {deal_info.get('issuance_date', 'N/A')}")
                    st.write(f"**Servicer:** {deal_info.get('servicer', 'N/A')}")
                    st.write(f"**Trustee:** {deal_info.get('trustee', 'N/A')}")
                
                # Note Classes
                st.subheader("Note Classes")
                if deal_details['note_classes']:
                    notes_df = pd.DataFrame(deal_details['note_classes'])
                    st.dataframe(
                        notes_df[['note_class', 'original_balance', 'current_balance', 'interest_rate', 'rating']],
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "note_class": "Note Class",
                            "original_balance": st.column_config.NumberColumn(
                                "Original Balance", format="$%.0f"
                            ),
                            "current_balance": st.column_config.NumberColumn(
                                "Current Balance", format="$%.0f"
                            ),
                            "interest_rate": st.column_config.NumberColumn(
                                "Interest Rate", format="%.2f%%"
                            ),
                            "rating": "Rating"
                        }
                    )
                else:
                    st.info("No note classes found for this deal")
                
                # Performance Charts
                st.subheader("Performance Charts")
                charts = st.session_state.abs_system.create_performance_charts(selected_deal)
                
                if charts:
                    if 'pool_balance' in charts:
                        st.plotly_chart(charts['pool_balance'], use_container_width=True)
                    
                    if 'loss_rate' in charts:
                        st.plotly_chart(charts['loss_rate'], use_container_width=True)
                    
                    if 'delinquencies' in charts:
                        st.plotly_chart(charts['delinquencies'], use_container_width=True)
                    
                    if 'note_class_performance' in charts:
                        st.plotly_chart(charts['note_class_performance'], use_container_width=True)
                else:
                    st.info("No performance data available. Upload surveillance reports to see charts.")
        else:
            st.info("No deals found. Upload some documents to get started!")


if __name__ == "__main__":
    main()
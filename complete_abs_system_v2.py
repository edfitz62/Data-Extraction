# -*- coding: utf-8 -*-
"""
Created on Mon Jul 14 09:37:09 2025

@author: edfit
"""

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
        
        # Surveillance Tables
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
    
    def _determine_payment_priority(self, class_name: str) -> int:
        """Determine payment priority based on class name"""
        # Same logic as subordination level for now
        return self._determine_subordination_level(class_name)
    
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
        initial_sidebar_state="expanded"
    )
    
    st.title("🏦 Complete ABS Document Processing System")
    st.markdown("**Process both New Issue Reports and Surveillance Reports**")
    
    # Initialize system
    if 'abs_system' not in st.session_state:
        st.session_state.abs_system = CompleteABSSystem()
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Select Page",
        ["📄 Document Processing", "📊 Deal Dashboard", "📈 Surveillance Reports", "🔍 Deal Details"]
    )
    
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
                # Read file content
                if uploaded_file.type == "text/plain":
                    text_content = str(uploaded_file.read(), "utf-8")
                else:
                    text_content = "Sample surveillance content for demo"
                
                # Auto-detect document type
                doc_type = st.session_state.abs_system.detect_document_type(text_content)
                st.info(f"🔍 Detected Document Type: **{doc_type}**")
                
                if st.button("Process Surveillance Document", key="process_surveillance"):
                    with st.spinner("Processing surveillance document..."):
                        if doc_type == 'SURVEILLANCE':
                            extracted_data = st.session_state.abs_system.extract_surveillance_data(text_content)
                            report_id = st.session_state.abs_system.save_surveillance_data(extracted_data)
                            
                            st.success(f"✅ Surveillance report processed successfully! Report ID: {report_id}")
                            
                            # Display extracted data
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.subheader("Pool Performance")
                                st.write(f"**Deal ID:** {extracted_data['deal_id']}")
                                st.write(f"**Report Date:** {extracted_data['report_date']}")
                                st.write(f"**Pool Balance:** ${extracted_data['total_pool_balance']:,.0f}")
                                st.write(f"**Collections:** ${extracted_data['collections_amount']:,.0f}")
                                st.write(f"**Charge-offs:** ${extracted_data['charge_offs_amount']:,.0f}")
                                st.write(f"**Loss Rate:** {extracted_data['loss_rate']:.2f}%")
                            
                            with col2:
                                st.subheader("Delinquencies")
                                st.write(f"**30+ Days:** ${extracted_data['delinquencies_30_plus']:,.0f}")
                                st.write(f"**60+ Days:** ${extracted_data['delinquencies_60_plus']:,.0f}")
                                st.write(f"**90+ Days:** ${extracted_data['delinquencies_90_plus']:,.0f}")
                                st.write(f"**Covenant Compliance:** {extracted_data['covenant_compliance']}")
                                st.write(f"**Credit Enhancement:** {extracted_data['credit_enhancement_level']:.2f}%")
                        else:
                            st.warning("⚠️ This appears to be a new issue report. Please use the New Issue Reports tab.")
    
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
    
    elif page == "🔍 Deal Details":
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
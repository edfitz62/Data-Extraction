# -*- coding: utf-8 -*-
"""
Created on Mon Jul 14 22:59:03 2025

@author: edfit
"""

# Enhanced Table Parser and Financial Data Extractor
# Add these functions to your existing ImprovedDocumentExtractor class

import re
import pandas as pd
from typing import Dict, List, Tuple, Optional
import numpy as np

class EnhancedFinancialExtractor:
    """Enhanced extraction for structured financial data and tables"""
    
    def __init__(self):
        self.note_class_patterns = self._build_note_class_patterns()
        self.financial_patterns = self._build_financial_patterns()
        self.table_patterns = self._build_table_patterns()
    
    def _build_note_class_patterns(self) -> Dict[str, str]:
        """Build comprehensive patterns for note class extraction"""
        return {
            'note_table_header': r'(?i)class\s+(?:initial\s+)?amount.*?interest.*?rate.*?maturity.*?rating',
            'note_row': r'(?i)(a-?\d*|b|c|d|e)\s+([0-9,]+(?:\.[0-9]+)?)\s+([0-9.]+%)\s+([a-z]+ \d{1,2}, \d{4})\s+([0-9.]+%)\s+([a-z+()sf ]+)',
            'class_amounts': r'(?i)class\s+(a-?\d*|b|c|d|e)\s+([0-9,]+(?:\.[0-9]+)?)',
            'interest_rates': r'(?i)class\s+(a-?\d*|b|c|d|e).*?([0-9.]+%)',
            'maturity_dates': r'(?i)(a-?\d*|b|c|d|e).*?([a-z]+ \d{1,2}, \d{4})',
            'ratings': r'(?i)class\s+(a-?\d*|b|c|d|e).*?(k\d+\+?|aaa|aa\+?|a\+?|bbb\+?|bb\+?|b\+?).*?\(sf\)'
        }
    
    def _build_financial_patterns(self) -> Dict[str, str]:
        """Build patterns for financial metrics extraction"""
        return {
            'deal_size': r'(?i)(?:aggregate|total|deal).*?(?:size|value|amount).*?[\$]?([0-9,]+(?:\.[0-9]+)?)\s*(?:million|mil|m)',
            'pool_balance': r'(?i)pool\s+balance.*?[\$]?([0-9,]+(?:\.[0-9]+)?)',
            'weighted_avg_term': r'(?i)weighted.*?average.*?(?:original|remaining).*?term.*?([0-9]+)',
            'seasoning': r'(?i)weighted.*?average.*?seasoning.*?([0-9]+)',
            'overcollateralization': r'(?i)(?:initial|target).*?o/?c.*?([0-9.]+%)',
            'reserve_account': r'(?i)reserve\s+account.*?([0-9.]+%)',
            'excess_spread': r'(?i)excess\s+spread.*?([0-9.]+%)',
            'credit_enhancement': r'(?i)(?:total|initial).*?(?:hard\s+)?credit\s+enhancement.*?([0-9.]+%)'
        }
    
    def _build_table_patterns(self) -> Dict[str, str]:
        """Build patterns for table structure recognition"""
        return {
            'table_delimiter': r'[-=]{3,}',
            'column_separator': r'\s{2,}|\t+',
            'row_separator': r'\n\s*',
            'currency_amount': r'[\$]?([0-9,]+(?:\.[0-9]+)?)',
            'percentage': r'([0-9.]+%)',
            'date_format': r'([a-z]+ \d{1,2}, \d{4})'
        }
    
    def extract_note_classes_table(self, text: str) -> List[Dict]:
        """Extract structured note classes data from tables"""
        note_classes = []
        
        # Method 1: Find structured table with headers
        table_data = self._find_structured_table(text)
        if table_data:
            note_classes.extend(self._parse_note_table(table_data))
        
        # Method 2: Pattern-based extraction as fallback
        if not note_classes:
            note_classes = self._extract_notes_by_patterns(text)
        
        return note_classes
    
    def _find_structured_table(self, text: str) -> Optional[str]:
        """Find and extract structured note classes table"""
        # Look for table with Class, Amount, Interest Rate, Maturity, Rating columns
        table_pattern = r'(?i)(?:class.*?amount.*?interest.*?rate.*?maturity.*?rating.*?)((?:\n.*?(?:a-?\d*|b|c|d|e).*?[0-9,]+.*?[0-9.]+%.*?){2,})'
        
        match = re.search(table_pattern, text, re.MULTILINE | re.DOTALL)
        if match:
            return match.group(1)
        
        return None
    
    def _parse_note_table(self, table_text: str) -> List[Dict]:
        """Parse structured table text into note class data"""
        note_classes = []
        lines = table_text.strip().split('\n')
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith('Total'):
                continue
            
            # Try to parse each line as a note class row
            note_data = self._parse_note_row(line)
            if note_data:
                note_classes.append(note_data)
        
        return note_classes
    
    def _parse_note_row(self, line: str) -> Optional[Dict]:
        """Parse a single table row into note class data"""
        # Enhanced pattern to capture note class data
        patterns = [
            # Pattern 1: Class Amount Rate Maturity Enhancement Rating
            r'(?i)(a-?\d*|b|c|d|e)\s+([0-9,]+(?:\.[0-9]+)?)\s+([0-9.]+%)\s+([a-z]+ \d{1,2}, \d{4})\s+([0-9.]+%)\s+([a-z+()sf ]+)',
            # Pattern 2: Class Amount Rate Maturity Rating (no enhancement)
            r'(?i)(a-?\d*|b|c|d|e)\s+([0-9,]+(?:\.[0-9]+)?)\s+([0-9.]+%)\s+([a-z]+ \d{1,2}, \d{4})\s+([a-z+()sf ]+)',
            # Pattern 3: More flexible parsing
            r'(?i)(a-?\d*|b|c|d|e)\s+([0-9,]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, line)
            if match:
                groups = match.groups()
                
                # Extract available data
                note_class = groups[0].upper()
                amount_str = groups[1].replace(',', '') if len(groups) > 1 else '0'
                
                try:
                    amount = float(amount_str) * 1000  # Convert from thousands
                except ValueError:
                    amount = 0
                
                # Extract other fields if available
                interest_rate = float(groups[2].replace('%', '')) if len(groups) > 2 and '%' in groups[2] else 0
                maturity = groups[3] if len(groups) > 3 and re.search(r'[a-z]+ \d{1,2}, \d{4}', groups[3], re.I) else ""
                rating = groups[-1] if len(groups) > 4 else ""
                
                return {
                    'note_class': note_class,
                    'original_balance': amount,
                    'current_balance': amount,  # Initially same as original
                    'interest_rate': interest_rate,
                    'expected_maturity': maturity,
                    'legal_final_maturity': maturity,
                    'rating': rating,
                    'subordination_level': self._get_subordination_level(note_class),
                    'payment_priority': self._get_subordination_level(note_class),
                    'enhancement_level': 0  # Will be calculated separately
                }
        
        return None
    
    def _extract_notes_by_patterns(self, text: str) -> List[Dict]:
        """Fallback method using individual patterns"""
        note_classes = []
        
        # Find all unique note classes mentioned
        class_pattern = r'(?i)class\s+(a-?\d*|b|c|d|e)(?:\s+notes?)?'
        classes = list(set(re.findall(class_pattern, text)))
        
        for note_class in classes:
            note_data = {
                'note_class': note_class.upper(),
                'original_balance': self._extract_class_amount(text, note_class),
                'current_balance': 0,
                'interest_rate': self._extract_class_rate(text, note_class),
                'expected_maturity': self._extract_class_maturity(text, note_class),
                'legal_final_maturity': "",
                'rating': self._extract_class_rating(text, note_class),
                'subordination_level': self._get_subordination_level(note_class),
                'payment_priority': self._get_subordination_level(note_class),
                'enhancement_level': 0
            }
            
            # Only add if we found meaningful data
            if note_data['original_balance'] > 0 or note_data['interest_rate'] > 0:
                note_classes.append(note_data)
        
        return note_classes
    
    def _extract_class_amount(self, text: str, note_class: str) -> float:
        """Extract amount for specific note class"""
        patterns = [
            rf'(?i)class\s+{re.escape(note_class)}\s+([0-9,]+(?:\.[0-9]+)?)',
            rf'(?i){re.escape(note_class)}\s+([0-9,]+(?:\.[0-9]+)?)\s+[0-9.]+%'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    amount_str = match.group(1).replace(',', '')
                    return float(amount_str) * 1000  # Convert from thousands
                except ValueError:
                    continue
        
        return 0
    
    def _extract_class_rate(self, text: str, note_class: str) -> float:
        """Extract interest rate for specific note class"""
        patterns = [
            rf'(?i)class\s+{re.escape(note_class)}.*?([0-9.]+%)',
            rf'(?i){re.escape(note_class)}.*?([0-9.]+%)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    return float(match.group(1).replace('%', ''))
                except ValueError:
                    continue
        
        return 0
    
    def _extract_class_maturity(self, text: str, note_class: str) -> str:
        """Extract maturity date for specific note class"""
        patterns = [
            rf'(?i)class\s+{re.escape(note_class)}.*?([a-z]+ \d{{1,2}}, \d{{4}})',
            rf'(?i){re.escape(note_class)}.*?([a-z]+ \d{{1,2}}, \d{{4}})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)
        
        return ""
    
    def _extract_class_rating(self, text: str, note_class: str) -> str:
        """Extract rating for specific note class"""
        patterns = [
            rf'(?i)class\s+{re.escape(note_class)}.*?(k\d+\+?|aaa|aa\+?|a\+?|bbb\+?|bb\+?|b\+?).*?\(sf\)',
            rf'(?i){re.escape(note_class)}.*?(k\d+\+?|aaa|aa\+?|a\+?|bbb\+?|bb\+?|b\+?)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1).upper()
        
        return ""
    
    def _get_subordination_level(self, note_class: str) -> int:
        """Determine subordination level based on note class"""
        class_hierarchy = {
            'A-1': 1, 'A-2': 1, 'A-3': 1, 'A': 1,
            'B': 2, 'C': 3, 'D': 4, 'E': 5
        }
        return class_hierarchy.get(note_class.upper(), 1)
    
    def extract_transaction_parties(self, text: str) -> Dict[str, str]:
        """Extract key transaction parties"""
        parties = {}
        
        party_patterns = {
            'issuer': r'(?i)issuer[:\s]+([^\n\r]+?)(?:\n|$)',
            'servicer': r'(?i)servicer[/\\]?administrator[:\s]+([^\n\r]+?)(?:\n|$)',
            'sponsor': r'(?i)sponsor[/\\]?seller[:\s]+([^\n\r]+?)(?:\n|$)',
            'trustee': r'(?i)(?:indenture\s+)?trustee[:\s]+([^\n\r]+?)(?:\n|$)',
            'backup_servicer': r'(?i)back-?up\s+servicer[:\s]+([^\n\r]+?)(?:\n|$)',
            'rating_agency': r'(?i)(?:rating\s+)?(?:agency|kbra|moody|fitch|s&p)[:\s]*([^\n\r]+?)(?:\n|$)'
        }
        
        for party_type, pattern in party_patterns.items():
            match = re.search(pattern, text)
            if match:
                parties[party_type] = match.group(1).strip()
        
        return parties
    
    def extract_financial_metrics(self, text: str) -> Dict[str, float]:
        """Extract key financial metrics"""
        metrics = {}
        
        for metric_name, pattern in self.financial_patterns.items():
            match = re.search(pattern, text)
            if match:
                try:
                    value_str = match.group(1).replace(',', '').replace('%', '')
                    if 'million' in pattern or 'mil' in pattern:
                        metrics[metric_name] = float(value_str) * 1000000
                    elif '%' in match.group(0):
                        metrics[metric_name] = float(value_str)
                    else:
                        metrics[metric_name] = float(value_str)
                except ValueError:
                    continue
        
        return metrics
    
    def extract_comprehensive_deal_data(self, text: str) -> Dict:
        """Main function to extract all comprehensive deal data"""
        # Extract note classes with full financial details
        note_classes = self.extract_note_classes_table(text)
        
        # Extract transaction parties
        parties = self.extract_transaction_parties(text)
        
        # Extract financial metrics
        metrics = self.extract_financial_metrics(text)
        
        # Extract additional deal information
        deal_info = self._extract_basic_deal_info(text)
        
        # Combine all data
        comprehensive_data = {
            **deal_info,
            **parties,
            'note_classes': note_classes,
            'financial_metrics': metrics,
            'total_notes': len(note_classes),
            'total_deal_size': sum(nc.get('original_balance', 0) for nc in note_classes),
            'weighted_avg_rate': self._calculate_weighted_avg_rate(note_classes)
        }
        
        return comprehensive_data
    
    def _extract_basic_deal_info(self, text: str) -> Dict:
        """Extract basic deal information"""
        info = {}
        
        # Deal name
        name_patterns = [
            r'(?i)([A-Z][A-Za-z\s]+\d{4}-\d+)',
            r'(?i)(PEAC\s+Solutions\s+Receivables\s+\d{4}-\d+)',
            r'(?i)([A-Z]+\s+\d{4}-\d+)'
        ]
        
        for pattern in name_patterns:
            match = re.search(pattern, text)
            if match:
                info['deal_name'] = match.group(1).strip()
                break
        
        # Asset type
        if re.search(r'(?i)equipment.*?abs', text):
            info['asset_type'] = 'Equipment ABS'
        elif re.search(r'(?i)auto.*?abs', text):
            info['asset_type'] = 'Auto ABS'
        else:
            info['asset_type'] = 'ABS'
        
        # Date
        date_match = re.search(r'([A-Z][a-z]+ \d{1,2}, \d{4})', text)
        if date_match:
            info['issuance_date'] = date_match.group(1)
        
        return info
    
    def _calculate_weighted_avg_rate(self, note_classes: List[Dict]) -> float:
        """Calculate weighted average interest rate"""
        if not note_classes:
            return 0
        
        total_weighted = sum(
            nc.get('original_balance', 0) * nc.get('interest_rate', 0)
            for nc in note_classes
        )
        total_balance = sum(nc.get('original_balance', 0) for nc in note_classes)
        
        return total_weighted / total_balance if total_balance > 0 else 0


# Integration function to add to your existing ImprovedDocumentExtractor
def enhance_existing_extractor(extractor_instance):
    """Add enhanced functionality to existing extractor"""
    extractor_instance.financial_extractor = EnhancedFinancialExtractor()
    
    # Add new method to existing class
    def extract_enhanced_data(self, text: str) -> Dict:
        """Enhanced extraction method"""
        # Get basic extraction
        basic_data = self.extract_abs_data(text)
        
        # Get enhanced financial data
        enhanced_data = self.financial_extractor.extract_comprehensive_deal_data(text)
        
        # Merge the data, with enhanced data taking precedence
        combined_data = {**basic_data, **enhanced_data}
        
        return combined_data
    
    # Bind the new method to the instance
    import types
    extractor_instance.extract_enhanced_data = types.MethodType(extract_enhanced_data, extractor_instance)
    
    return extractor_instance


# Usage example:
"""
# To use with your existing system:

# 1. Add to your existing ImprovedDocumentExtractor
enhanced_extractor = enhance_existing_extractor(your_existing_extractor)

# 2. Use the enhanced extraction
enhanced_results = enhanced_extractor.extract_enhanced_data(document_text)

# 3. The results will now include:
# - Detailed note classes with amounts, rates, maturities, ratings
# - Complete transaction parties information
# - Financial metrics and ratios
# - Proper table parsing of structured data

print(f"Found {len(enhanced_results['note_classes'])} note classes:")
for note in enhanced_results['note_classes']:
    print(f"  Class {note['note_class']}: ${note['original_balance']:,.0f} at {note['interest_rate']:.2f}%")
"""
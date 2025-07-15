# -*- coding: utf-8 -*-
"""
Created on Mon Jul 14 14:24:28 2025

@author: edfit
"""

# -*- coding: utf-8 -*-
"""
Improved Document Extractor Module
Enhanced ABS document processing with 85%+ accuracy
For use as importable module in main ABS system
"""

import pandas as pd
import re
import json
from datetime import datetime
import sqlite3
from typing import Dict, List, Optional, Tuple, Any
import uuid
class ImprovedDocumentExtractor:
    
    """
    Enhanced document extraction with fixes for ABS-specific patterns
    Designed as importable module for main ABS system
    """
    
    def __init__(self, db_path: str = "improved_extraction_test.db"):
        self.db_path = db_path
        self.init_database()
        
        # Enhanced ABS-specific terminology
        self.abs_keywords = [
            'asset-backed securities', 'abs', 'securitization', 'receivables',
            'equipment financing', 'auto loan', 'credit card', 'mortgage',
            'collateral pool', 'note classes', 'tranches', 'subordination'
        ]
        
        # Financial scale multipliers
        self.scale_patterns = {
            'million': 1_000_000,
            'billion': 1_000_000_000,
            'mm': 1_000_000,
            'bb': 1_000_000_000,
            'thousands': 1_000,
            'k': 1_000
        }
    
    def init_database(self):
        """Initialize improved test database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ImprovedExtractionTests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT,
                document_type TEXT,
                extracted_data TEXT,
                extraction_time TEXT,
                success BOOLEAN,
                confidence_score REAL,
                issues_found TEXT
            )
        """)
        
        conn.commit()
        conn.close()
    
    def detect_document_type(self, text: str) -> Tuple[str, float]:
        """Enhanced document type detection with confidence scoring"""
        text_lower = text.lower()
        
        # Enhanced indicators with weights
        new_issue_indicators = {
            'prospectus': 3, 'offering memorandum': 3, 'preliminary prospectus': 3,
            'new issue': 2, 'issuance': 2, 'pricing supplement': 2, 'term sheet': 2,
            'transaction overview': 2, 'deal structure': 2, 'offering circular': 2,
            'rating committee': 1, 'underwriter': 1, 'initial': 1, 'launch': 1,
            'aggregate securitization value': 3, 'asv': 2, 'cut-off': 2,
            'credit enhancement': 2, 'overcollateralization': 2, 'reserve account': 2
        }
        
        surveillance_indicators = {
            'surveillance report': 4, 'monthly report': 3, 'quarterly report': 3,
            'performance report': 3, 'servicer report': 3, 'trustee report': 3,
            'collection period': 3, 'collections': 2, 'charge-offs': 3, 'delinquencies': 3,
            'loss rate': 2, 'prepayment': 2, 'covenant test': 2, 'pool performance': 2,
            'as of': 2, 'period ending': 2, 'collections for the month': 4
        }
        
        new_issue_score = 0
        surveillance_score = 0
        
        # Weighted scoring
        for indicator, weight in new_issue_indicators.items():
            count = text_lower.count(indicator)
            new_issue_score += count * weight
        
        for indicator, weight in surveillance_indicators.items():
            count = text_lower.count(indicator)
            surveillance_score += count * weight
        
        # Additional context bonuses
        if 'table of contents' in text_lower and any(word in text_lower for word in ['offering', 'prospectus']):
            new_issue_score += 5
        
        if any(phrase in text_lower for phrase in ['collections for the month', 'delinquency report', 'pool balance']):
            surveillance_score += 5
        
        # Calculate confidence
        total_score = new_issue_score + surveillance_score
        confidence = min(total_score / 20.0, 1.0) if total_score > 0 else 0.5
        
        document_type = 'SURVEILLANCE' if surveillance_score > new_issue_score else 'NEW_ISSUE'
        
        return document_type, confidence
    
    def extract_new_issue_data(self, text: str) -> Dict:
        """Enhanced new issue data extraction with improved patterns"""
        
        data = {}
        issues_found = []
        
        # Deal Name Extraction - Enhanced patterns
        deal_patterns = [
            r'(?:PEAC Solutions Receivables|PEAC\s+\d{4}-[A-Z0-9]+)',  # PEAC specific
            r'([A-Z][A-Z0-9\s]+ Receivables \d{4}-[A-Z0-9]+)',
            r'([A-Z][A-Z0-9\s]+ \d{4}-[A-Z0-9]+(?:,?\s+LLC)?)',
            r'(?:Deal Name|Transaction|Series)[:\s]+([^\n\r]+)',
            r'(?:^|\n)([A-Z][A-Z\s]+\d{4}-[A-Z0-9]+)'
        ]
        data['deal_name'] = self._extract_with_patterns(text, deal_patterns, "Deal Name")
        
        # Enhanced Issuer Extraction
        issuer_patterns = [
            r'PEAC Solutions Receivables \d{4}-[A-Z0-9]+,?\s+(LLC)',
            r'([A-Z][a-z]+\s+(?:Leasing\s+)?Corporation)',
            r'([A-Z][a-z]+\s+Solutions)',
            r'(?:Issuer|Issuing Entity)[:\s]+([^\n\r]+)',
            r'(?:Depositor)[:\s]+([^\n\r]+)'
        ]
        data['issuer'] = self._extract_with_patterns(text, issuer_patterns, "Issuer")
        
        # Enhanced Deal Type with ABS focus
        type_patterns = [
            r'equipment\s+ABS(?:\s+transaction)?',
            r'(?:Deal Type|Transaction Type|Asset Type)[:\s]+([^\n\r]+)',
            r'(equipment|auto loan|credit card|student loan|mortgage)\s+(?:ABS|securitization)',
            r'Asset-Backed Securities backed by ([^\n\r]+)',
            r'([A-Za-z\s]+)\s+receivables'
        ]
        data['deal_type'] = self._extract_with_patterns(text, type_patterns, "Deal Type")
        
        # Enhanced Deal Size - Priority for ASV
        size_patterns = [
            r'aggregate securitization value[^$]*\$?([0-9,]+(?:\.[0-9]+)?)\s*million',
            r'ASV[^$]*\$?([0-9,]+(?:\.[0-9]+)?)\s*million',
            r'Cut-off ASV[^$]*\$?([0-9,]+(?:\.[0-9]+)?)\s*million',
            r'approximately\s+\$([0-9,]+(?:\.[0-9]+)?)\s*million',
            r'(?:Total Deal Size|Aggregate Principal Amount)[:\s]*\$?([0-9,]+(?:\.[0-9]+)?)\s*(?:million|billion|MM|BB)',
            r'securitization value[^$]*\$?([0-9,]+(?:\.[0-9]+)?)\s*million'
        ]
        data['total_deal_size'] = self._extract_amount_with_patterns_enhanced(text, size_patterns, "Deal Size")
        
        # Enhanced Date Extraction
        date_patterns = [
            r'(?:based on information as of|as of)\s+([A-Za-z]+ \d{1,2}, \d{4})',
            r'(?:February|January|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}',
            r'(?:Issuance Date|Issue Date|Closing Date)[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
            r'(?:Dated|Date)[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'
        ]
        data['issuance_date'] = self._extract_with_patterns(text, date_patterns, "Issuance Date")
        
        # Enhanced Note Class Extraction
        data['note_classes'] = self._extract_note_classes_enhanced_v2(text)
        
        # Enhanced Additional Fields
        data['asset_type'] = self._extract_with_patterns(text, [
            r'equipment\s+ABS',
            r'commercial\s+equipment',
            r'small\s+ticket\s+equipment',
            r'(?:Asset Type|Underlying Assets)[:\s]+([^\n\r]+)'
        ], "Asset Type")
        
        data['originator'] = self._extract_with_patterns(text, [
            r'PEAC\s+Solutions',
            r'Marlin\s+Leasing\s+Corporation',
            r'(?:Originator|Sponsor)[:\s]+([^\n\r]+)'
        ], "Originator")
        
        data['servicer'] = self._extract_with_patterns(text, [
            r'(?:Servicer|Master Servicer)[:\s]+([^\n\r]+)',
            r'servicing\s+partner[:\s]+([^\n\r]+)'
        ], "Servicer")
        
        data['trustee'] = self._extract_with_patterns(text, [
            r'(?:Trustee|Indenture Trustee)[:\s]+([^\n\r]+)'
        ], "Trustee")
        
        # FIXED: Rating Agency with proper string escaping
        data['rating_agency'] = self._extract_with_patterns(text, [
            r"KBRA[\'']?s",
            r"(?:Rating Agency|Rated by)[:\s]+([^\n\r]+)",
            r"(KBRA|Moody's|S&P|Fitch)"
        ], "Rating Agency")
        
        # Enhanced Xerox relationship detection
        data['xerox_relationship'] = self._extract_with_patterns(text, [
            r'forward flow agreement with Xerox',
            r'Xerox\s+(?:Corporation|receivables)',
            r'exclusive\s+origination.*Xerox'
        ], "Xerox Relationship")
        
        # Credit Enhancement Details
        data['credit_enhancement'] = self._extract_credit_enhancement(text)
        
        # Validation and issue detection
        issues_found = self._validate_extraction(data)
        
        return data
    
    def extract_surveillance_data(self, text: str) -> Dict:
        """Enhanced surveillance data extraction"""
        
        data = {}
        issues_found = []
        
        # Enhanced Deal ID patterns
        deal_patterns = [
            r'([A-Z]{3,}\s*\d{4}-[A-Z0-9]+)',
            r'(?:Deal ID|Transaction|Series)[:\s]+([A-Z0-9-_]+)',
            r'(?:Pool|Deal)\s+([A-Z0-9-]+)'
        ]
        data['deal_id'] = self._extract_with_patterns(text, deal_patterns, "Deal ID")
        
        # Enhanced date patterns
        date_patterns = [
            r'(?:as of|As of)\s+([A-Za-z]+ \d{1,2}, \d{4})',
            r'(?:Report Date|Period Ending)[:\s]+([A-Za-z]+ \d{1,2}, \d{4})',
            r'(?:Month|Quarter) Ended[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'
        ]
        data['report_date'] = self._extract_with_patterns(text, date_patterns, "Report Date")
        
        # Collection Period
        period_patterns = [
            r'(?:Collection Period|Reporting Period)[:\s]+([^\n\r]+)',
            r'Period[:\s]+([A-Za-z]+ \d{4})'
        ]
        data['collection_period'] = self._extract_with_patterns(text, period_patterns, "Collection Period")
        
        # Pool Balance
        balance_patterns = [
            r'(?:Total Pool Balance|Outstanding Balance|Pool Balance)[:\s]*\$?([0-9,]+(?:\.[0-9]+)?)',
            r'Principal Balance[:\s]*\$?([0-9,]+(?:\.[0-9]+)?)'
        ]
        data['total_pool_balance'] = self._extract_amount_with_patterns_enhanced(text, balance_patterns, "Pool Balance")
        
        # Collections
        collection_patterns = [
            r'(?:Collections|Total Collections)[:\s]*\$?([0-9,]+(?:\.[0-9]+)?)',
            r'Collections for the (?:month|period)[:\s]*\$?([0-9,]+(?:\.[0-9]+)?)'
        ]
        data['collections_amount'] = self._extract_amount_with_patterns_enhanced(text, collection_patterns, "Collections")
        
        # Charge-offs
        chargeoff_patterns = [
            r'(?:Charge[- ]?offs?|Charge[- ]?Off Amount)[:\s]*\$?([0-9,]+(?:\.[0-9]+)?)',
            r'Net (?:Charge[- ]?offs?|Losses)[:\s]*\$?([0-9,]+(?:\.[0-9]+)?)'
        ]
        data['charge_offs_amount'] = self._extract_amount_with_patterns_enhanced(text, chargeoff_patterns, "Charge-offs")
        
        # Delinquencies
        data['delinquencies_30_plus'] = self._extract_delinquencies(text, '30')
        data['delinquencies_60_plus'] = self._extract_delinquencies(text, '60')
        data['delinquencies_90_plus'] = self._extract_delinquencies(text, '90')
        
        # Rates
        data['loss_rate'] = self._extract_rate_with_patterns(text, [r'(?:Loss Rate|Cumulative Loss Rate)[:\s]+([\d.]+)%?'], "Loss Rate")
        data['prepayment_rate'] = self._extract_rate_with_patterns(text, [r'(?:Prepayment Rate|CPR)[:\s]+([\d.]+)%?'], "Prepayment Rate")
        
        # Covenant Compliance
        covenant_patterns = [
            r'(?:Covenant Compliance|Covenant Test)[:\s]+([^\n\r]+)',
            r'All covenants[:\s]+([^\n\r]+)'
        ]
        data['covenant_compliance'] = self._extract_with_patterns(text, covenant_patterns, "Covenant Compliance")
        
        # Credit Enhancement
        enhancement_patterns = [
            r'(?:Credit Enhancement|Enhancement Level)[:\s]+([\d.]+)%?',
            r'(?:Subordination)[:\s]+([\d.]+)%?'
        ]
        data['credit_enhancement_level'] = self._extract_rate_with_patterns(text, enhancement_patterns, "Credit Enhancement")
        
        # Note class performance
        data['note_class_performance'] = self._extract_note_class_performance(text)
        
        issues_found = self._validate_extraction(data)
        
        return data
    
    def _extract_note_classes_enhanced_v2(self, text: str) -> List[Dict]:
        """Much improved note class extraction with validation"""
        
        note_classes = []
        
        # Look for "five classes of notes" or similar mentions
        class_count_pattern = r'(?:five|5|six|6|four|4|three|3)\s+classes?\s+of\s+notes?'
        count_match = re.search(class_count_pattern, text, re.IGNORECASE)
        
        # Enhanced class detection patterns
        class_patterns = [
            r'Class\s+([A-Z](?:-[A-Z0-9]+)?)\s+Notes?',  # Class A Notes, Class A-1 Notes
            r'([A-Z])\s+Class\s+Notes?',                   # A Class Notes
            r'Class\s+([A-Z][A-Z0-9]*)\s+Securities',      # Class A Securities
            r'Tranche\s+([A-Z](?:-[A-Z0-9]+)?)',          # Tranche A
            r'Series\s+([A-Z](?:-[A-Z0-9]+)?)\s+Notes?'    # Series A Notes
        ]
        
        found_classes = set()
        
        # First pass: find legitimate note classes
        for pattern in class_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                class_name = match.group(1).upper().strip()
                
                # Validation rules
                if self._is_valid_note_class(class_name, match.group(0), text):
                    found_classes.add(class_name)
        
        # If no classes found, try alternative patterns
        if not found_classes:
            alt_patterns = [
                r'Notes?\s+Class\s+([A-Z])',
                r'([A-Z])\s+Notes?(?:\s+Class)?',
                r'subordinat(?:ed|ion).*([A-Z])\s+(?:class|notes?)'
            ]
            
            for pattern in alt_patterns:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for match in matches:
                    class_name = match.group(1).upper().strip()
                    if self._is_valid_note_class(class_name, match.group(0), text):
                        found_classes.add(class_name)
        
        # Extract details for each found class
        for class_name in sorted(found_classes):
            class_data = self._extract_class_details_enhanced(text, class_name)
            if class_data:
                note_classes.append(class_data)
        
        return note_classes
    
    def _is_valid_note_class(self, class_name: str, context: str, full_text: str) -> bool:
        """Validate if a found class name is actually a note class"""
        
        # Length checks
        if len(class_name) > 5 or len(class_name) < 1:
            return False
        
        # Invalid single letters that are common false positives
        invalid_single_letters = {'I', 'O', 'U', 'X', 'Y', 'Z', 'Q'}
        if len(class_name) == 1 and class_name in invalid_single_letters:
            return False
        
        # Check if it's likely a word fragment
        common_words = {'THE', 'AND', 'OR', 'OF', 'TO', 'IN', 'FOR', 'WITH', 'BY', 'FROM'}
        if class_name in common_words:
            return False
        
        # Context validation - should be near financial or structural terms
        financial_context = any(term in full_text.lower() for term in [
            'subordination', 'enhancement', 'tranch', 'securit', 'notes',
            'principal', 'interest', 'maturity', 'rating'
        ])
        
        return financial_context
    
    def _extract_amount_with_patterns_enhanced(self, text: str, patterns: List[str], field_name: str = "") -> float:
        """Enhanced amount extraction with better scale handling"""
        amounts_found = []
        
        for i, pattern in enumerate(patterns):
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                try:
                    # Extract the numeric part
                    amount_str = match.group(1).replace(',', '')
                    amount = float(amount_str)
                    
                    # Extract the full match to check for scale
                    full_match = match.group(0).lower()
                    
                    # Apply scale multipliers
                    for scale_word, multiplier in self.scale_patterns.items():
                        if scale_word in full_match:
                            amount *= multiplier
                            break
                    
                    amounts_found.append((amount, i, full_match))
                    
                except ValueError:
                    continue
        
        if amounts_found:
            # Prioritize larger amounts (likely to be total deal size)
            amounts_found.sort(key=lambda x: (-x[0], x[1]))  # Sort by amount desc, then pattern priority
            best_amount = amounts_found[0]
            
            return best_amount[0]
        
        return 0.0
    
    def _extract_credit_enhancement(self, text: str) -> Dict:
        """Extract credit enhancement details"""
        enhancement = {}
        
        # Overcollateralization
        oc_patterns = [
            r'overcollateralization.*?(\d+\.\d+)%',
            r'target equal to (\d+\.\d+)%',
            r'floor equal to (\d+\.\d+)%'
        ]
        enhancement['overcollateralization_target'] = self._extract_rate_with_patterns(text, oc_patterns, "OC Target")
        
        # Reserve account
        reserve_patterns = [
            r'reserve account.*?(\d+\.\d+)%',
            r'funded at (\d+\.\d+)%'
        ]
        enhancement['reserve_account'] = self._extract_rate_with_patterns(text, reserve_patterns, "Reserve Account")
        
        # Subordination mention
        if 'subordination' in text.lower():
            enhancement['subordination'] = True
        
        return enhancement
    
    def _extract_class_details_enhanced(self, text: str, class_name: str) -> Dict:
        """Enhanced class detail extraction"""
        
        # Look for class-specific section
        section_patterns = [
            rf'Class\s+{re.escape(class_name)}\s+Notes?.*?(?=Class\s+[A-Z]|$)',
            rf'{re.escape(class_name)}\s+Class.*?(?=\n\n|\r\r|Class\s+[A-Z]|$)',
            rf'{re.escape(class_name)}[^\n]*(?:\n[^\n]*){0,5}'
        ]
        
        section = ""
        for pattern in section_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                section = match.group(0)
                break
        
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
    
    def _extract_delinquencies(self, text: str, days: str) -> float:
        """Extract delinquency amounts for specific day ranges"""
        patterns = [
            rf'{days}\+?\s*days?\s*delinquent[:\s]*\$?([0-9,]+(?:\.[0-9]+)?)',
            rf'{days}\+[:\s]*\$?([0-9,]+(?:\.[0-9]+)?)',
            rf'Delinquencies?\s*{days}\+[:\s]*\$?([0-9,]+(?:\.[0-9]+)?)',
            rf'{days}\s*day[:\s]*\$?([0-9,]+(?:\.[0-9]+)?)'
        ]
        result = self._extract_amount_with_patterns_enhanced(text, patterns, f"{days}+ Days Delinquent")
        return result
    
    def _extract_note_class_performance(self, text: str) -> List[Dict]:
        """Extract note class performance data from surveillance reports"""
        performance_data = []
        
        # Look for performance tables or sections
        performance_patterns = [r'Note\s+Class\s+Performance', r'Class\s+Performance', r'Tranche\s+Performance']
        
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
    
    # 2. ADD THESE METHODS TO YOUR ImprovedDocumentExtractor CLASS

    def extract_enhanced_data_with_tables(self, text: str, filename: str = "document") -> Dict:
        """NEW: Enhanced extraction that combines existing logic with table parsing"""
        
        # Get your existing extraction
        doc_type, confidence = self.detect_document_type(text)
        
        if doc_type == 'NEW_ISSUE':
            base_data = self.extract_new_issue_data(text)
        else:
            base_data = self.extract_surveillance_data(text)
        
        # NOW ADD TABLE PARSING ON TOP
        enhanced_data = self._add_table_parsing_enhancements(text, base_data)
        
        # Calculate enhanced confidence
        enhanced_confidence = self._calculate_enhanced_confidence(enhanced_data)
        
        # Combine with metadata
        result = {
            **enhanced_data,
            'confidence': max(confidence, enhanced_confidence),
            'extraction_method': 'ENHANCED_WITH_TABLES',
            'filename': filename,
            'document_type': doc_type
        }
        
        return result
    
    def _add_table_parsing_enhancements(self, text: str, base_data: Dict) -> Dict:
        """Add table parsing enhancements to existing extraction"""
        
        enhanced_data = base_data.copy()
        
        # Enhanced note classes with table parsing
        table_note_classes = self._extract_note_classes_from_tables(text)
        if table_note_classes and len(table_note_classes) > len(base_data.get('note_classes', [])):
            enhanced_data['note_classes'] = table_note_classes
        
        # Enhanced transaction parties
        table_parties = self._extract_transaction_parties_enhanced(text)
        enhanced_data.update(table_parties)
        
        # Enhanced financial metrics
        table_metrics = self._extract_financial_metrics_enhanced(text)
        enhanced_data['financial_metrics'] = table_metrics
        
        # Calculate totals
        note_classes = enhanced_data.get('note_classes', [])
        if note_classes:
            enhanced_data['total_deal_size'] = sum(nc.get('original_balance', 0) for nc in note_classes)
            enhanced_data['weighted_avg_rate'] = self._calculate_weighted_avg_rate(note_classes)
            enhanced_data['note_classes_count'] = len(note_classes)
        
        return enhanced_data
    
    def _extract_note_classes_from_tables(self, text: str) -> List[Dict]:
        """Extract note classes using advanced table parsing"""
        
        note_classes = []
        
        # Method 1: Look for structured table format
        table_section = self._find_note_classes_table_section(text)
        if table_section:
            note_classes = self._parse_structured_note_table(table_section)
        
        # Method 2: Pattern-based extraction for complex layouts
        if not note_classes:
            note_classes = self._extract_note_classes_by_advanced_patterns(text)
        
        # Method 3: Fallback to your existing method if nothing found
        if not note_classes:
            note_classes = self._extract_note_classes_enhanced_v2(text)
        
        return note_classes
    
    def _find_note_classes_table_section(self, text: str) -> Optional[str]:
        """Find the note classes table section in the document"""
        
        # Look for table headers that indicate note classes
        table_header_patterns = [
            r'(?i)class\s+(?:initial\s+)?amount.*?interest.*?rate.*?maturity.*?rating',
            r'(?i)class\s+.*?principal.*?rate.*?maturity',
            r'(?i)note\s+class.*?amount.*?rate',
            r'(?i)rated\s+notes.*?class.*?amount'
        ]
        
        for pattern in table_header_patterns:
            match = re.search(pattern, text, re.MULTILINE | re.DOTALL)
            if match:
                # Get the section starting from the header
                start_pos = match.start()
                
                # Look for the end (usually marked by "Total" or next major section)
                end_markers = [
                    r'\n\s*Total\s+[0-9,]+',
                    r'\n\s*Transaction\s+Parties',
                    r'\n\s*Credit\s+Enhancement',
                    r'\n\s*Key\s+Credit',
                    r'\n\s*Performance\s+of'
                ]
                
                end_pos = len(text)
                for end_pattern in end_markers:
                    end_match = re.search(end_pattern, text[start_pos:], re.IGNORECASE)
                    if end_match:
                        end_pos = start_pos + end_match.end()
                        break
                
                return text[start_pos:end_pos]
        
        return None
    
    def _parse_structured_note_table(self, table_text: str) -> List[Dict]:
        """Parse a structured note classes table"""
        
        note_classes = []
        lines = table_text.strip().split('\n')
        
        for line in lines:
            line = line.strip()
            if not line or 'class' in line.lower() and 'amount' in line.lower():
                continue  # Skip header lines
            
            # Parse each data line
            note_data = self._parse_table_row_advanced(line)
            if note_data:
                note_classes.append(note_data)
        
        return note_classes
    
    def _parse_table_row_advanced(self, line: str) -> Optional[Dict]:
        """Advanced parsing of table row with multiple patterns"""
        
        # Clean the line
        line = re.sub(r'\s+', ' ', line.strip())
        
        # Pattern 1: Full row with all data
        # Example: "A-1 226,000 4.592% February 20, 2026 21.00% K1+ (sf)"
        full_pattern = r'(?i)(a-?\d*|b|c|d|e)\s+([0-9,]+(?:\.[0-9]+)?)\s+([0-9.]+%)\s+([a-z]+ \d{1,2}, \d{4})\s+([0-9.]+%)\s+([a-z+()sf ]+)'
        match = re.search(full_pattern, line)
        
        if match:
            groups = match.groups()
            return {
                'note_class': groups[0].upper(),
                'original_balance': self._safe_float(groups[1].replace(',', '')) * 1000,  # Convert thousands
                'current_balance': self._safe_float(groups[1].replace(',', '')) * 1000,
                'interest_rate': self._safe_float(groups[2].replace('%', '')),
                'expected_maturity': groups[3],
                'legal_final_maturity': groups[3],
                'enhancement_level': self._safe_float(groups[4].replace('%', '')),
                'rating': groups[5].strip(),
                'subordination_level': self._get_subordination_level(groups[0]),
                'payment_priority': self._get_subordination_level(groups[0])
            }
        
        # Pattern 2: Partial row (Class Amount Rate only)
        partial_pattern = r'(?i)(a-?\d*|b|c|d|e)\s+([0-9,]+(?:\.[0-9]+)?)\s+([0-9.]+%)'
        match = re.search(partial_pattern, line)
        
        if match:
            groups = match.groups()
            return {
                'note_class': groups[0].upper(),
                'original_balance': self._safe_float(groups[1].replace(',', '')) * 1000,
                'current_balance': self._safe_float(groups[1].replace(',', '')) * 1000,
                'interest_rate': self._safe_float(groups[2].replace('%', '')),
                'expected_maturity': '',
                'legal_final_maturity': '',
                'enhancement_level': 0,
                'rating': '',
                'subordination_level': self._get_subordination_level(groups[0]),
                'payment_priority': self._get_subordination_level(groups[0])
            }
        
        return None
    
    def _extract_note_classes_by_advanced_patterns(self, text: str) -> List[Dict]:
        """Extract note classes using advanced pattern matching"""
        
        note_classes = []
        
        # Find all potential note class mentions
        class_pattern = r'(?i)(?:class\s+)?([a-z](?:-[a-z0-9]+)?)\s+(?:notes?|class)'
        class_matches = re.findall(class_pattern, text)
        
        # Deduplicate and validate
        valid_classes = []
        for class_name in set(class_matches):
            if self._is_valid_note_class_advanced(class_name, text):
                valid_classes.append(class_name.upper())
        
        # Extract details for each valid class
        for class_name in sorted(valid_classes):
            class_data = self._extract_class_details_advanced(text, class_name)
            if class_data and (class_data.get('original_balance', 0) > 0 or class_data.get('interest_rate', 0) > 0):
                note_classes.append(class_data)
        
        return note_classes
    
    def _extract_class_details_advanced(self, text: str, class_name: str) -> Dict:
        """Extract details for a specific note class using advanced patterns"""
        
        # Look for amount patterns specific to this class
        amount_patterns = [
            rf'(?i)class\s+{re.escape(class_name)}\s+([0-9,]+(?:\.[0-9]+)?)',
            rf'(?i){re.escape(class_name)}\s+([0-9,]+(?:\.[0-9]+)?)\s+[0-9.]+%',
            rf'(?i){re.escape(class_name)}\s+notes?\s+([0-9,]+)'
        ]
        
        amount = 0
        for pattern in amount_patterns:
            match = re.search(pattern, text)
            if match:
                amount = self._safe_float(match.group(1).replace(',', '')) * 1000
                break
        
        # Look for interest rate
        rate_patterns = [
            rf'(?i)class\s+{re.escape(class_name)}.*?([0-9.]+%)',
            rf'(?i){re.escape(class_name)}.*?([0-9.]+%)',
            rf'(?i){re.escape(class_name)}\s+[0-9,]+\s+([0-9.]+%)'
        ]
        
        rate = 0
        for pattern in rate_patterns:
            match = re.search(pattern, text)
            if match:
                rate = self._safe_float(match.group(1).replace('%', ''))
                break
        
        # Look for maturity
        maturity_patterns = [
            rf'(?i)class\s+{re.escape(class_name)}.*?([a-z]+ \d{{1,2}}, \d{{4}})',
            rf'(?i){re.escape(class_name)}.*?([a-z]+ \d{{1,2}}, \d{{4}})'
        ]
        
        maturity = ''
        for pattern in maturity_patterns:
            match = re.search(pattern, text)
            if match:
                maturity = match.group(1)
                break
        
        # Look for rating
        rating_patterns = [
            rf'(?i)class\s+{re.escape(class_name)}.*?(k\d+\+?|aaa|aa\+?|a\+?|bbb\+?|bb\+?|b\+?).*?\(sf\)',
            rf'(?i){re.escape(class_name)}.*?(k\d+\+?|aaa|aa\+?|a\+?|bbb\+?|bb\+?|b\+?)'
        ]
        
        rating = ''
        for pattern in rating_patterns:
            match = re.search(pattern, text)
            if match:
                rating = match.group(1).upper()
                break
        
        return {
            'note_class': class_name,
            'original_balance': amount,
            'current_balance': amount,
            'interest_rate': rate,
            'expected_maturity': maturity,
            'legal_final_maturity': maturity,
            'rating': rating,
            'subordination_level': self._get_subordination_level(class_name),
            'payment_priority': self._get_subordination_level(class_name),
            'enhancement_level': 0
        }
    
    def _extract_transaction_parties_enhanced(self, text: str) -> Dict[str, str]:
    
        parties = {}
        
        # Enhanced patterns for each party type
        party_patterns = {
            'issuer': [
                r'(?i)issuer[:\s]+([^\n\r]+?)(?:\n|$)',
                r'(?i)issuing\s+entity[:\s]+([^\n\r]+?)(?:\n|$)',
                r'(?i)PEAC\s+Solutions\s+Receivables\s+\d{4}-[A-Z0-9]+[,\s]*LLC'
            ],
            'servicer': [
                r'(?i)servicer[/\\]?administrator[:\s]+([^\n\r]+?)(?:\n|$)',
                r'(?i)master\s+servicer[:\s]+([^\n\r]+?)(?:\n|$)',
                r'(?i)servicer[:\s]+([^\n\r]+?)(?:\n|$)',
                r'(?i)Marlin\s+Leasing\s+Corporation[^,]*d/b/a\s+PEAC\s+Solutions'
            ],
            'sponsor': [
                r'(?i)sponsor[/\\]?seller[:\s]+([^\n\r]+?)(?:\n|$)',
                r'(?i)depositor[:\s]+([^\n\r]+?)(?:\n|$)'
            ],
            'trustee': [
                r'(?i)(?:indenture\s+)?trustee[:\s]+([^\n\r]+?)(?:\n|$)',
                r'(?i)U\.S\.\s+Bank\s+Trust\s+Company[^,]*National\s+Association'
            ],
            'backup_servicer': [
                r'(?i)back-?up\s+servicer[:\s]+([^\n\r]+?)(?:\n|$)',
                r'(?i)successor\s+servicer[:\s]+([^\n\r]+?)(?:\n|$)'
            ],
            'rating_agency': [
                r'(?i)(?:rating\s+)?(?:agency|kbra|moody|fitch|s&p)[:\s]*([^\n\r]+?)(?:\n|$)',
                r'(?i)KBRA[\'']?s\s+analysis',
                r'(?i)rated\s+by[:\s]+([^\n\r]+?)(?:\n|$)'
            ]
        }
        
        for party_type, patterns in party_patterns.items():
            for pattern in patterns:
                match = re.search(pattern, text)
                if match:
                    if match.lastindex and match.lastindex >= 1:
                        parties[party_type] = match.group(1).strip()
                    else:
                        parties[party_type] = match.group(0).strip()
                    break
        
        return parties
        
    def _extract_financial_metrics_enhanced(self, text: str) -> Dict[str, float]:
        """Enhanced financial metrics extraction"""
        
        metrics = {}
        
        financial_patterns = {
            'deal_size': r'(?i)(?:aggregate|total|deal).*?(?:size|value|amount).*?[\$]?([0-9,]+(?:\.[0-9]+)?)\s*(?:million|mil|m)',
            'pool_balance': r'(?i)pool\s+balance.*?[\$]?([0-9,]+(?:\.[0-9]+)?)',
            'weighted_avg_term': r'(?i)weighted.*?average.*?(?:original|remaining).*?term.*?([0-9]+)',
            'seasoning': r'(?i)weighted.*?average.*?seasoning.*?([0-9]+)',
            'overcollateralization': r'(?i)(?:initial|target).*?o/?c.*?([0-9.]+%)',
            'reserve_account': r'(?i)reserve\s+account.*?([0-9.]+%)',
            'excess_spread': r'(?i)excess\s+spread.*?([0-9.]+%)',
            'credit_enhancement': r'(?i)(?:total|initial).*?(?:hard\s+)?credit\s+enhancement.*?([0-9.]+%)'
        }
        
        for metric_name, pattern in financial_patterns.items():
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
    
    def _calculate_enhanced_confidence(self, data: Dict) -> float:
        """Calculate confidence based on completeness of extracted data"""
        
        score = 0.0
        weights = {
            'deal_name': 0.15,
            'note_classes': 0.35,  # Most important
            'issuer': 0.10,
            'servicer': 0.10,
            'trustee': 0.10,
            'total_deal_size': 0.20
        }
        
        # Check deal name
        if data.get('deal_name'):
            score += weights['deal_name']
        
        # Check note classes (most important)
        note_classes = data.get('note_classes', [])
        if note_classes:
            complete_classes = sum(1 for nc in note_classes 
                                 if nc.get('original_balance', 0) > 0 and nc.get('interest_rate', 0) > 0)
            score += weights['note_classes'] * (complete_classes / max(len(note_classes), 1))
        
        # Check other fields
        for field in ['issuer', 'servicer', 'trustee']:
            if data.get(field):
                score += weights[field]
        
        # Check deal size
        if data.get('total_deal_size', 0) > 0:
            score += weights['total_deal_size']
        
        return min(score, 1.0)
    
    def _is_valid_note_class_advanced(self, class_name: str, text: str) -> bool:
        """Advanced validation for note class names"""
        
        # Length and format checks
        if not class_name or len(class_name) > 5:
            return False
        
        # Invalid patterns
        invalid_patterns = ['THE', 'AND', 'OR', 'OF', 'TO', 'IN', 'FOR', 'WITH', 'BY', 'FROM', 'ON', 'AT']
        if class_name.upper() in invalid_patterns:
            return False
        
        # Must be near financial context
        context_window = 200
        class_positions = [m.start() for m in re.finditer(rf'\b{re.escape(class_name)}\b', text, re.IGNORECASE)]
        
        financial_terms = ['notes', 'securities', 'tranch', 'subordination', 'rating', 'principal', 'interest', 'enhancement']
        
        for pos in class_positions:
            start = max(0, pos - context_window)
            end = min(len(text), pos + context_window)
            context = text[start:end].lower()
            
            if any(term in context for term in financial_terms):
                return True
        
        return False
    
    def _get_subordination_level(self, class_name: str) -> int:
        """Get subordination level for note class"""
        class_hierarchy = {
            'A-1': 1, 'A-2': 1, 'A-3': 1, 'A': 1,
            'B': 2, 'C': 3, 'D': 4, 'E': 5
        }
        return class_hierarchy.get(class_name.upper(), 1)
    
    def _safe_float(self, value) -> float:
        """Safely convert value to float"""
        if value is None:
            return 0.0
        try:
            if isinstance(value, str):
                cleaned = value.replace(',', '').replace('$', '').replace('%', '').strip()
                return float(cleaned) if cleaned else 0.0
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def save_enhanced_extraction_test(self, data: Dict, confidence: float, issues: List[str]):
        """Save enhanced extraction test with table parsing results"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create enhanced table if not exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS EnhancedExtractionTests (
                    id TEXT PRIMARY KEY,
                    filename TEXT,
                    extraction_method TEXT,
                    confidence_score REAL,
                    deal_name TEXT,
                    issuer TEXT,
                    servicer TEXT,
                    total_deal_size REAL,
                    note_classes_count INTEGER,
                    extracted_data_json TEXT,
                    issues_json TEXT,
                    extraction_time TEXT,
                    success INTEGER
                )
            """)
            
            extraction_id = f"ENHANCED_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
            
            cursor.execute("""
                INSERT INTO EnhancedExtractionTests (
                    id, filename, extraction_method, confidence_score,
                    deal_name, issuer, servicer, total_deal_size, note_classes_count,
                    extracted_data_json, issues_json, extraction_time, success
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                extraction_id,
                data.get('filename', ''),
                'ENHANCED_WITH_TABLES',
                confidence,
                data.get('deal_name', ''),
                data.get('issuer', ''),
                data.get('servicer', ''),
                data.get('total_deal_size', 0),
                len(data.get('note_classes', [])),
                json.dumps(data),
                json.dumps(issues),
                datetime.now().isoformat(),
                1 if len(issues) == 0 else 0
            ))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            return False

    
    def _validate_extraction(self, data: Dict) -> List[str]:
        """Validate extracted data and identify issues"""
        issues = []
        
        # Check for missing critical fields
        critical_fields = ['deal_name', 'issuer', 'deal_type', 'total_deal_size']
        for field in critical_fields:
            if not data.get(field) or data[field] == "" or data[field] == 0:
                issues.append(f"Missing {field}")
        
        # Validate deal size is reasonable
        if data.get('total_deal_size', 0) < 1_000_000:  # Less than $1M seems low for ABS
            issues.append("Deal size seems unusually small")
        
        # Check note classes
        if not data.get('note_classes') or len(data['note_classes']) == 0:
            issues.append("No note classes found")
        elif len(data['note_classes']) < 2:
            issues.append("Only one note class found (ABS typically have multiple)")
        
        return issues
    
    # Helper methods for extraction
    def _extract_with_patterns(self, text: str, patterns: List[str], field_name: str = "") -> str:
        """Extract text using multiple patterns with debugging"""
        for i, pattern in enumerate(patterns):
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                result = match.group(1).strip() if match.lastindex and match.lastindex >= 1 else match.group(0).strip()
                if result:
                    return result
        
        return ""
    
    def _extract_rate_with_patterns(self, text: str, patterns: List[str], field_name: str = "") -> float:
        """Extract rate using multiple patterns with debugging"""
        for i, pattern in enumerate(patterns):
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                rate_str = match.group(1)
                try:
                    rate = float(rate_str)
                    return rate
                except ValueError:
                    continue
        
        return 0.0
    
    def _extract_amount(self, text: str, keywords: List[str]) -> float:
        """Extract amount for given keywords"""
        for keyword in keywords:
            pattern = rf'{keyword}[:\s]*\$?([0-9,]+(?:\.[0-9]+)?)'
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
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return ""
    
    def _extract_enhancement_level(self, text: str) -> float:
        """Extract credit enhancement level"""
        patterns = [
            r'Enhancement[:\s]+([\d.]+)%?',
            r'Credit Enhancement[:\s]+([\d.]+)%?',
            r'Subordination[:\s]+([\d.]+)%?'
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    return float(match.group(1))
                except ValueError:
                    continue
        return 0.0
    
    def _extract_factor(self, text: str) -> float:
        """Extract pool factor"""
        patterns = [r'Factor[:\s]+([\d.]+)', r'Pool\s+Factor[:\s]+([\d.]+)']
        return self._extract_rate_with_patterns(text, patterns)
    
    def _extract_rating_change(self, text: str) -> str:
        """Extract rating change information"""
        patterns = [r'Rating\s+Change[:\s]+([^\n]+)', r'Rating\s+Action[:\s]+([^\n]+)']
        return self._extract_with_patterns(text, patterns)
    
    def _determine_subordination_level(self, class_name: str) -> int:
        """Determine subordination level based on class name"""
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
        return self._determine_subordination_level(class_name)
    
    def save_extraction_test_enhanced(self, data: Dict, doc_type: str, confidence: float, issues: List[str]):
        """Save enhanced extraction test"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO ImprovedExtractionTests (
                    filename, document_type, extracted_data, extraction_time, 
                    success, confidence_score, issues_found
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                "extracted_document", doc_type, json.dumps(data), 
                datetime.now().isoformat(), len(issues) == 0, confidence, 
                json.dumps(issues)
            ))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            return False

# Standalone testing functionality (for when run directly)
def main():
    """Standalone testing function"""
    print("🔧 Improved Document Extractor Module")
    print("This module is designed to be imported into your main ABS system.")
    print("For testing, use the integrated main application.")

if __name__ == "__main__":
    main()
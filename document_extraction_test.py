# -*- coding: utf-8 -*-
"""
Created on Mon Jul 14 12:37:11 2025

@author: edfit
"""

import streamlit as st
import pandas as pd
import re
import json
from datetime import datetime
from typing import Dict, List, Optional
import sqlite3

class FocusedDocumentExtractor:
    """
    Focused document extraction module using proven extraction methods
    """
    
    def __init__(self):
        self.db_path = "document_extraction_test.db"
        self.init_database()
    
    def init_database(self):
        """Initialize test database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ExtractionTests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT,
                document_type TEXT,
                extracted_data TEXT,
                extraction_time TEXT,
                success BOOLEAN
            )
        """)
        
        conn.commit()
        conn.close()
    
    def detect_document_type(self, text: str) -> str:
        """Enhanced document type detection"""
        text_lower = text.lower()
        
        # New Issue indicators (more specific)
        new_issue_indicators = [
            'prospectus', 'offering memorandum', 'preliminary prospectus',
            'new issue', 'issuance', 'pricing supplement', 'term sheet',
            'transaction overview', 'deal structure', 'offering circular',
            'rating committee', 'underwriter', 'initial', 'launch'
        ]
        
        # Surveillance indicators (more specific)
        surveillance_indicators = [
            'surveillance report', 'monthly report', 'quarterly report',
            'performance report', 'servicer report', 'trustee report',
            'collection period', 'collections', 'charge-offs', 'delinquencies',
            'loss rate', 'prepayment', 'covenant test', 'pool performance'
        ]
        
        new_issue_score = 0
        surveillance_score = 0
        
        # Count indicators with context
        for indicator in new_issue_indicators:
            if indicator in text_lower:
                new_issue_score += text_lower.count(indicator)
        
        for indicator in surveillance_indicators:
            if indicator in text_lower:
                surveillance_score += text_lower.count(indicator)
        
        # Additional context checks
        if 'table of contents' in text_lower and ('offering' in text_lower or 'prospectus' in text_lower):
            new_issue_score += 5
        
        if any(word in text_lower for word in ['collections for the month', 'delinquency report', 'pool balance']):
            surveillance_score += 5
        
        st.write(f"**Detection Scores:** New Issue: {new_issue_score}, Surveillance: {surveillance_score}")
        
        return 'SURVEILLANCE' if surveillance_score > new_issue_score else 'NEW_ISSUE'
    
    def extract_new_issue_data(self, text: str) -> Dict:
        """Enhanced new issue data extraction"""
        st.write("🔍 **Extracting New Issue Data...**")
        
        data = {}
        
        # Deal Name Extraction
        deal_patterns = [
            r'(?:Deal Name|Transaction|Series)[:\s]+([^\n\r]+)',
            r'([A-Z][A-Z0-9\s]+ \d{4}-[A-Z0-9]+)',  # Pattern like "AMXCA 2024-1"
            r'([A-Z]{3,}\s+\d{4}-[A-Z0-9]+)',
            r'(?:^|\n)([A-Z][A-Z\s]+\d{4}-[A-Z0-9]+)'
        ]
        data['deal_name'] = self._extract_with_patterns(text, deal_patterns, "Deal Name")
        
        # Issuer Extraction
        issuer_patterns = [
            r'(?:Issuer|Issuing Entity)[:\s]+([^\n\r]+)',
            r'(?:Depositor)[:\s]+([^\n\r]+)',
            r'([A-Z][a-z]+\s+(?:Trust|Corporation|LLC|Inc\.?))'
        ]
        data['issuer'] = self._extract_with_patterns(text, issuer_patterns, "Issuer")
        
        # Deal Type
        type_patterns = [
            r'(?:Deal Type|Transaction Type|Asset Type)[:\s]+([^\n\r]+)',
            r'(Auto Loan|Credit Card|Student Loan|Equipment|Mortgage) (?:ABS|Securitization)',
            r'Asset-Backed Securities backed by ([^\n\r]+)'
        ]
        data['deal_type'] = self._extract_with_patterns(text, type_patterns, "Deal Type")
        
        # Total Deal Size
        size_patterns = [
            r'(?:Total Deal Size|Aggregate Principal Amount|Total Principal)[:\s]*\$?([0-9,]+(?:\.[0-9]+)?)\s*(?:million|billion|MM|BB)?',
            r'\$([0-9,]+(?:\.[0-9]+)?)\s*(?:million|billion|MM|BB)',
            r'Principal Amount[:\s]*\$?([0-9,]+(?:\.[0-9]+)?)'
        ]
        data['total_deal_size'] = self._extract_amount_with_patterns(text, size_patterns, "Deal Size")
        
        # Issuance Date
        date_patterns = [
            r'(?:Issuance Date|Issue Date|Closing Date)[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
            r'(?:Dated|Date)[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'
        ]
        data['issuance_date'] = self._extract_with_patterns(text, date_patterns, "Issuance Date")
        
        # Enhanced Note Class Extraction
        data['note_classes'] = self._extract_note_classes_enhanced(text)
        
        # Additional fields
        data['asset_type'] = self._extract_with_patterns(text, [r'(?:Asset Type|Underlying Assets)[:\s]+([^\n\r]+)'], "Asset Type")
        data['originator'] = self._extract_with_patterns(text, [r'(?:Originator|Sponsor)[:\s]+([^\n\r]+)'], "Originator")
        data['servicer'] = self._extract_with_patterns(text, [r'(?:Servicer|Master Servicer)[:\s]+([^\n\r]+)'], "Servicer")
        data['trustee'] = self._extract_with_patterns(text, [r'(?:Trustee|Indenture Trustee)[:\s]+([^\n\r]+)'], "Trustee")
        data['rating_agency'] = self._extract_with_patterns(text, [r'(?:Rating Agency|Rated by)[:\s]+([^\n\r]+)'], "Rating Agency")
        
        # Show extraction results
        self._display_extraction_results(data, "New Issue")
        
        return data
    
    def extract_surveillance_data(self, text: str) -> Dict:
        """Enhanced surveillance data extraction"""
        st.write("🔍 **Extracting Surveillance Data...**")
        
        data = {}
        
        # Deal ID
        deal_patterns = [
            r'(?:Deal ID|Transaction|Series)[:\s]+([A-Z0-9-_]+)',
            r'([A-Z]{3,}\s*\d{4}-[A-Z0-9]+)',
            r'(?:Pool|Deal)\s+([A-Z0-9-]+)'
        ]
        data['deal_id'] = self._extract_with_patterns(text, deal_patterns, "Deal ID")
        
        # Report Date
        date_patterns = [
            r'(?:Report Date|As of|Period Ending)[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
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
        data['total_pool_balance'] = self._extract_amount_with_patterns(text, balance_patterns, "Pool Balance")
        
        # Collections
        collection_patterns = [
            r'(?:Collections|Total Collections)[:\s]*\$?([0-9,]+(?:\.[0-9]+)?)',
            r'Collections for the (?:month|period)[:\s]*\$?([0-9,]+(?:\.[0-9]+)?)'
        ]
        data['collections_amount'] = self._extract_amount_with_patterns(text, collection_patterns, "Collections")
        
        # Charge-offs
        chargeoff_patterns = [
            r'(?:Charge[- ]?offs?|Charge[- ]?Off Amount)[:\s]*\$?([0-9,]+(?:\.[0-9]+)?)',
            r'Net (?:Charge[- ]?offs?|Losses)[:\s]*\$?([0-9,]+(?:\.[0-9]+)?)'
        ]
        data['charge_offs_amount'] = self._extract_amount_with_patterns(text, chargeoff_patterns, "Charge-offs")
        
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
        
        # Show extraction results
        self._display_extraction_results(data, "Surveillance")
        
        return data
    
    def _extract_note_classes_enhanced(self, text: str) -> List[Dict]:
        """Enhanced note class extraction with better pattern matching"""
        st.write("🔍 **Extracting Note Classes...**")
        
        note_classes = []
        
        # Enhanced patterns for note class detection
        class_patterns = [
            r'(Class\s+[A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*)\s+Notes?',
            r'(Series\s+[A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*)\s+Notes?',
            r'([A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*)\s+Notes?(?:\s+Class)?',
            r'Note\s+Class\s+([A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*)',
            r'Tranche\s+([A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*)',
            r'Class\s+([A-Z])\s+Securities',
            r'([A-Z])\s+Class\s+Notes?'
        ]
        
        found_classes = set()
        
        for pattern in class_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                class_name = match.group(1).upper().strip()
                # Clean up class name
                class_name = re.sub(r'^(CLASS|SERIES)\s+', '', class_name)
                if class_name and len(class_name) <= 10:  # Reasonable length check
                    found_classes.add(class_name)
                    st.write(f"Found note class: **{class_name}**")
        
        # Extract details for each found class
        for class_name in sorted(found_classes):
            class_data = self._extract_class_details(text, class_name)
            if class_data:
                note_classes.append(class_data)
        
        st.write(f"**Total note classes found: {len(note_classes)}**")
        return note_classes
    
    def _extract_class_details(self, text: str, class_name: str) -> Dict:
        """Extract detailed information for a specific note class"""
        
        # Create search patterns for this class
        search_patterns = [
            rf'{re.escape(class_name)}\s+Notes?.*?(?=Class\s+[A-Z]|Series\s+[A-Z]|\n\n|\r\r)',
            rf'Class\s+{re.escape(class_name)}.*?(?=Class\s+[A-Z]|Series\s+[A-Z]|\n\n|\r\r)',
            rf'{re.escape(class_name)}[^\n]*\n[^\n]*(?:\n[^\n]*)?'
        ]
        
        section = ""
        for pattern in search_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                section = match.group(0)
                break
        
        if not section:
            # Fallback: search in a wider context
            class_index = text.upper().find(class_name.upper())
            if class_index != -1:
                section = text[max(0, class_index-100):class_index+300]
        
        if not section:
            return {
                'note_class': class_name,
                'original_balance': 0.0,
                'current_balance': 0.0,
                'interest_rate': 0.0,
                'expected_maturity': '',
                'rating': '',
                'subordination_level': self._determine_subordination_level(class_name),
                'payment_priority': self._determine_payment_priority(class_name),
                'enhancement_level': 0.0
            }
        
        return {
            'note_class': class_name,
            'original_balance': self._extract_amount(section, ['Original Balance', 'Initial Balance', 'Principal Amount', 'Amount']),
            'current_balance': self._extract_amount(section, ['Current Balance', 'Outstanding Balance', 'Outstanding']),
            'interest_rate': self._extract_rate(section, ['Interest Rate', 'Coupon Rate', 'Rate', 'Coupon']),
            'expected_maturity': self._extract_date(section, ['Expected Maturity', 'Maturity Date', 'Maturity']),
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
        result = self._extract_amount_with_patterns(text, patterns, f"{days}+ Days Delinquent")
        return result
    
    # Helper methods for extraction
    def _extract_with_patterns(self, text: str, patterns: List[str], field_name: str = "") -> str:
        """Extract text using multiple patterns with debugging"""
        for i, pattern in enumerate(patterns):
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                result = match.group(1).strip()
                if result:
                    if field_name:
                        st.write(f"✅ **{field_name}**: {result} (Pattern {i+1})")
                    return result
        
        if field_name:
            st.write(f"❌ **{field_name}**: Not found")
        return ""
    
    def _extract_amount_with_patterns(self, text: str, patterns: List[str], field_name: str = "") -> float:
        """Extract amount using multiple patterns with debugging"""
        for i, pattern in enumerate(patterns):
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                amount_str = match.group(1).replace(',', '')
                try:
                    amount = float(amount_str)
                    # Handle millions/billions
                    if 'million' in match.group(0).lower() or 'mm' in match.group(0).lower():
                        amount *= 1000000
                    elif 'billion' in match.group(0).lower() or 'bb' in match.group(0).lower():
                        amount *= 1000000000
                    
                    if field_name:
                        st.write(f"✅ **{field_name}**: ${amount:,.0f} (Pattern {i+1})")
                    return amount
                except ValueError:
                    continue
        
        if field_name:
            st.write(f"❌ **{field_name}**: Not found")
        return 0.0
    
    def _extract_rate_with_patterns(self, text: str, patterns: List[str], field_name: str = "") -> float:
        """Extract rate using multiple patterns with debugging"""
        for i, pattern in enumerate(patterns):
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                rate_str = match.group(1)
                try:
                    rate = float(rate_str)
                    if field_name:
                        st.write(f"✅ **{field_name}**: {rate}% (Pattern {i+1})")
                    return rate
                except ValueError:
                    continue
        
        if field_name:
            st.write(f"❌ **{field_name}**: Not found")
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
    
    def _display_extraction_results(self, data: Dict, doc_type: str):
        """Display extraction results in organized format"""
        st.subheader(f"📋 {doc_type} Extraction Results")
        
        # Create a results dataframe for display
        results = []
        for key, value in data.items():
            if key != 'note_classes':
                if isinstance(value, (int, float)) and value > 0:
                    if 'amount' in key.lower() or 'size' in key.lower() or 'balance' in key.lower():
                        display_value = f"${value:,.0f}"
                    elif 'rate' in key.lower():
                        display_value = f"{value}%"
                    else:
                        display_value = str(value)
                else:
                    display_value = str(value) if value else "Not found"
                
                results.append({
                    "Field": key.replace('_', ' ').title(),
                    "Value": display_value
                })
        
        # Display main fields
        if results:
            st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)
        
        # Display note classes if present
        if 'note_classes' in data and data['note_classes']:
            st.subheader("📊 Note Classes")
            note_class_df = pd.DataFrame(data['note_classes'])
            st.dataframe(note_class_df, use_container_width=True, hide_index=True)
        
        # Save to test database
        self._save_extraction_test(data, doc_type)
    
    def _save_extraction_test(self, data: Dict, doc_type: str):
        """Save extraction test to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO ExtractionTests (
                    filename, document_type, extracted_data, extraction_time, success
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                "test_document", doc_type, json.dumps(data), 
                datetime.now().isoformat(), True
            ))
            
            conn.commit()
            conn.close()
            st.success("✅ Extraction test saved to database")
        except Exception as e:
            st.error(f"❌ Error saving test: {str(e)}")


def main():
    st.set_page_config(
        page_title="Document Extraction Test",
        page_icon="🔍",
        layout="wide"
    )
    
    st.title("🔍 Focused Document Extraction Test")
    st.markdown("Test and debug document extraction with enhanced pattern matching")
    
    # Initialize extractor
    if 'extractor' not in st.session_state:
        st.session_state.extractor = FocusedDocumentExtractor()
    
    # File upload
    uploaded_file = st.file_uploader(
        "Upload Document for Testing",
        type=['txt', 'pdf', 'docx'],
        help="Upload a document to test extraction"
    )
    
    # Text input for testing
    st.subheader("OR Enter Text Directly")
    sample_text = st.text_area(
        "Paste document text here for testing:",
        height=200,
        placeholder="Paste your document text here to test extraction patterns..."
    )
    
    if uploaded_file is not None:
        # Read file content
        if uploaded_file.type == "text/plain":
            text_content = str(uploaded_file.read(), "utf-8")
            st.success(f"📁 Loaded {uploaded_file.name} ({len(text_content)} characters)")
        else:
            st.warning("⚠️ Non-text files will use sample content for this test")
            text_content = sample_text if sample_text else ""
    else:
        text_content = sample_text
    
    if text_content:
        st.subheader("📋 Document Analysis")
        
        # Show text preview
        with st.expander("📄 Document Preview", expanded=False):
            st.text(text_content[:1000] + "..." if len(text_content) > 1000 else text_content)
        
        # Document type detection
        st.subheader("🔍 Document Type Detection")
        detected_type = st.session_state.extractor.detect_document_type(text_content)
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🆕 Extract as New Issue", key="extract_new_issue"):
                st.session_state.extractor.extract_new_issue_data(text_content)
        
        with col2:
            if st.button("📊 Extract as Surveillance", key="extract_surveillance"):
                st.session_state.extractor.extract_surveillance_data(text_content)
        
        # Auto-extract based on detection
        if st.button(f"🤖 Auto-Extract (Detected: {detected_type})", key="auto_extract"):
            if detected_type == 'NEW_ISSUE':
                st.session_state.extractor.extract_new_issue_data(text_content)
            else:
                st.session_state.extractor.extract_surveillance_data(text_content)
    
    else:
        st.info("📝 Upload a file or enter text to begin extraction testing")


if __name__ == "__main__":
    main()
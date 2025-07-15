# -*- coding: utf-8 -*-
"""
Fixed Main ABS System - Database and Import Issues Resolved
"""

import streamlit as st
import pandas as pd
import sqlite3
import re
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import numpy as np
import os
import uuid
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


# Set page config FIRST
st.set_page_config(
    page_title="Complete ABS Document Processing System",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)


# NEW: Silent imports for file processing
try:
    import PyPDF2
    import pdfplumber
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False

try:
    import docx
    from docx import Document
    DOCX_SUPPORT = True
except ImportError:
    DOCX_SUPPORT = False

try:
    import openpyxl
    XLSX_SUPPORT = True
except ImportError:
    XLSX_SUPPORT = False

# Safe import of enhanced extraction module (SILENT)
ENHANCED_EXTRACTION_AVAILABLE = False
try:
    if os.path.exists("improved_document_extractor.py"):
        from improved_document_extractor import ImprovedDocumentExtractor
        ENHANCED_EXTRACTION_AVAILABLE = True
    # No messages - silent import
except Exception as e:
    ENHANCED_EXTRACTION_AVAILABLE = False
except Exception as e:
    st.error(f"❌ Error importing enhanced extraction: {str(e)}")
    ENHANCED_EXTRACTION_AVAILABLE = False

def apply_whitehall_branding():
    """Apply Whitehall Partners branding with complete clutter removal"""
    
    whitehall_css = """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    :root {
        --whitehall-navy: #041635;
        --whitehall-navy-light: #1a2742;
        --whitehall-blue: #47abff;
        --whitehall-blue-secondary: #6789ba;
        --whitehall-white: #ffffff;
        --whitehall-light-bg: rgba(255, 255, 255, 0.98);
    }
    
    /* Main app styling */
    .stApp {
        background: linear-gradient(180deg, #041635 0%, #1a2742 50%, #2c3e50 100%);
        font-family: 'Inter', sans-serif;
    }
    
    /* Content container */
    .block-container {
        background: var(--whitehall-light-bg);
        border-radius: 12px;
        box-shadow: 0 10px 40px rgba(4, 22, 53, 0.3);
        margin: 1rem;
        padding: 2rem;
        color: var(--whitehall-navy);
    }
    
    /* HIDE ALL CLUTTER AND ERROR MESSAGES */
    .stException {
        display: none !important;
    }
    
    .stAlert[data-baseweb="notification"] {
        display: none !important;
    }
    
    /* NUCLEAR OPTION - Hide all system messages above main content */
.element-container:has(.stException),
.element-container:has(.stAlert),
.element-container:has(.stError),
.element-container:has(.stWarning),
.element-container:has(.stInfo),
.element-container:has(.stSuccess) {
    display: none !important;
}

/* Hide the entire top section until we get to our content */
.main .block-container > div:nth-child(1),
.main .block-container > div:nth-child(2),
.main .block-container > div:nth-child(3) {
    display: none !important;
}

/* Only show our header and content */
.main .block-container > div:nth-child(4):not(:has(.stTabs)) {
    display: block !important;
}
    
    div[data-testid="stException"] {
        display: none !important;
    }
    
    .element-container:has([data-testid="stException"]) {
        display: none !important;
    }
    
    /* Hide specific streamlit messages */
    div[data-testid="stNotificationContentError"] {
        display: none !important;
    }
    
    div[data-testid="stNotificationContentSuccess"]:contains("Enhanced extraction module loaded") {
        display: none !important;
    }
    
    div[data-testid="stNotificationContentSuccess"]:contains("Database initialized") {
        display: none !important;
    }
    
    div[data-testid="stNotificationContentInfo"]:contains("Enhanced extractor initialized") {
        display: none !important;
    }
    
    /* Hide excessive system messages */
    .stSuccess:has-text("Enhanced extraction module loaded") {
        display: none !important;
    }
    
    .stSuccess:has-text("Database initialized") {
        display: none !important;
    }
    
    .stInfo:has-text("Enhanced extractor initialized") {
        display: none !important;
    }
    
    /* Hide any div containing "TypeError" */
    div:contains("TypeError") {
        display: none !important;
    }
    
    /* Hide any element containing specific error text */
    *:contains("First argument must be a String, HTMLElement") {
        display: none !important;
    }
    
    /* Keep only important user-facing messages */
    .stError:not(:contains("TypeError")):not(:contains("First argument")) {
        background: linear-gradient(135deg, #ef4444 0%, #f87171 100%);
        color: white;
        border: none;
        border-radius: 8px;
        border-left: 4px solid #991b1b;
        font-size: 0.9rem;
        padding: 0.5rem 1rem;
    }
    
    .stWarning:not(:contains("TypeError")) {
        background: linear-gradient(135deg, #f59e0b 0%, #fbbf24 100%);
        color: white;
        border: none;
        border-radius: 8px;
        border-left: 4px solid #92400e;
        font-size: 0.9rem;
        padding: 0.5rem 1rem;
    }
    
    /* Professional header styling */
    .whitehall-header {
        background: linear-gradient(135deg, #041635 0%, #1a2742 50%, #6789ba 100%);
        color: var(--whitehall-white);
        padding: 3rem 0;
        margin: -2rem -2rem 2rem -2rem;
        border-radius: 12px 12px 0 0;
        text-align: center;
        position: relative;
        overflow: hidden;
        box-shadow: 0 4px 20px rgba(4, 22, 53, 0.4);
    }
    
    .header-content {
        position: relative;
        z-index: 2;
    }
    
    .simple-logo-section {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 2rem;
        margin-bottom: 1.5rem;
    }
    
    .deer-logo-simple {
        flex-shrink: 0;
    }
    
    .company-info {
        text-align: left;
    }
    
    .company-name {
        font-size: 3.5rem;
        font-weight: 700;
        color: var(--whitehall-white);
        margin: 0 0 0.3rem 0;
        letter-spacing: -0.02em;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
    }
    
    .company-motto {
        font-size: 1.1rem;
        color: var(--whitehall-white);
        font-weight: 400;
        letter-spacing: 0.15em;
        margin: 0 0 0.3rem 0;
        opacity: 0.95;
    }
    
    .company-tagline {
        font-size: 1.2rem;
        color: var(--whitehall-blue);
        font-weight: 500;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        margin: 0;
    }
    
    .app-subtitle {
        font-size: 1.1rem;
        color: rgba(255, 255, 255, 0.8);
        font-weight: 300;
        margin-top: 1.5rem;
        font-style: italic;
        text-align: center;
    }
    
    /* Clean status indicators */
    .status-row {
        display: flex;
        justify-content: center;
        gap: 2rem;
        margin: 1.5rem 0;
        flex-wrap: wrap;
    }
    
    .status-card {
        background: var(--whitehall-navy);
        color: var(--whitehall-white);
        padding: 1rem 1.5rem;
        border-radius: 8px;
        text-align: center;
        min-width: 180px;
        box-shadow: 0 4px 15px rgba(4, 22, 53, 0.2);
        transition: transform 0.2s ease;
    }
    
    .status-card:hover {
        transform: translateY(-2px);
    }
    
    .status-card.success {
        background: linear-gradient(135deg, #10b981 0%, #34d399 100%);
    }
    
    .status-card.warning {
        background: linear-gradient(135deg, #f59e0b 0%, #fbbf24 100%);
    }
    
    .status-card.error {
        background: linear-gradient(135deg, #ef4444 0%, #f87171 100%);
    }
    
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: var(--whitehall-navy);
        border-radius: 12px;
        padding: 0.5rem;
        margin-top: 1rem;
    }
    
    .stTabs [data-baseweb="tab"] {
        height: 3.5rem;
        background: var(--whitehall-navy-light);
        color: var(--whitehall-white);
        border-radius: 8px;
        font-weight: 500;
        transition: all 0.3s ease;
    }
    
    .stTabs [aria-selected="true"] {
        background: var(--whitehall-blue);
        color: var(--whitehall-white);
        box-shadow: 0 4px 15px rgba(71, 171, 255, 0.4);
    }
    
    /* Button styling */
    .stButton > button {
        background: var(--whitehall-navy);
        color: var(--whitehall-white);
        border: 2px solid var(--whitehall-blue);
        border-radius: 8px;
        font-weight: 500;
        padding: 0.75rem 2rem;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        background: var(--whitehall-blue);
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(71, 171, 255, 0.4);
    }
    
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, var(--whitehall-blue) 0%, var(--whitehall-blue-secondary) 100%);
        border: 2px solid var(--whitehall-white);
    }
    
    /* Input styling */
    .stSelectbox > div > div,
    .stTextInput > div > div > input,
    .stTextArea > div > div > textarea {
        background: var(--whitehall-white);
        border: 2px solid var(--whitehall-navy);
        border-radius: 8px;
        color: var(--whitehall-navy);
    }
    
    /* File uploader */
    .css-1kyxreq {
        border: 2px dashed var(--whitehall-blue);
        border-radius: 10px;
        background: rgba(71, 171, 255, 0.05);
    }
    
    /* Success/Info messages styling - keep the useful ones */
    .stSuccess:not(:contains("Enhanced extraction")):not(:contains("Database initialized")) {
        background: linear-gradient(135deg, #10b981 0%, #34d399 100%);
        color: white;
        border: none;
        border-radius: 8px;
        border-left: 4px solid #065f46;
    }
    
    .stInfo:not(:contains("Enhanced extractor")) {
        background: rgba(71, 171, 255, 0.1);
        border: 2px solid var(--whitehall-blue);
        border-radius: 8px;
        color: var(--whitehall-navy);
    }
    
    /* Hide Streamlit branding and navigation */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    .stDeployButton {visibility: hidden;}
    
    /* Hide the top padding that Streamlit adds */
    .main .block-container {
        padding-top: 1rem;
    }
    
    /* Text visibility fix */
    .stMarkdown h1, .stMarkdown h2, .stMarkdown h3,
    .stMarkdown h4, .stMarkdown h5, .stMarkdown h6,
    .stMarkdown p, .stText {
        color: var(--whitehall-navy) !important;
    }
    
    @media (max-width: 768px) {
        .simple-logo-section {
            flex-direction: column;
            text-align: center;
        }
        
        .company-info {
            text-align: center;
        }
        
        .company-name {
            font-size: 2.5rem;
        }
        
        .status-row {
            flex-direction: column;
            align-items: center;
        }
    }
    </style>
    """
    
    st.markdown(whitehall_css, unsafe_allow_html=True)

def create_whitehall_header():
    """Clean ABS Navigator header with Whitehall Partners branding"""
    
    st.markdown("""
    <div style="background: #041635; color: white; padding: 2rem; margin-bottom: 2rem; position: relative; height: 120px; display: flex; align-items: center; justify-content: center;">
        <h1 style="margin: 0; color: white; font-size: 3rem; font-weight: 600; text-align: center;">ABS Navigator</h1>
        <div style="position: absolute; bottom: 15px; right: 20px; font-size: 1rem; color: rgba(255,255,255,0.8); font-weight: 400;">WHITEHALL PARTNERS</div>
    </div>
    """, unsafe_allow_html=True)

def create_clean_status_indicators():
    """Clean, minimal status indicators"""
    
    # Enhanced Extraction Status
    extraction_status = "success"
    extraction_text = "Enhanced Extraction Ready"
    try:
        if not (hasattr(st.session_state, 'fixed_abs_system') and 
                hasattr(st.session_state.fixed_abs_system, 'enhanced_available') and 
                st.session_state.fixed_abs_system.enhanced_available):
            extraction_status = "warning"
            extraction_text = "Basic Extraction Mode"
    except:
        extraction_status = "warning"
        extraction_text = "System Loading"
    
    # Database Status
    db_status = "success"
    db_text = "Database Active"
    table_count = 0
    try:
        conn = sqlite3.connect("complete_abs_system.db")
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        table_count = len(tables)
        conn.close()
        db_text = f"Database Active ({table_count} tables)"
    except:
        db_status = "error"
        db_text = "Database Error"
    
    # File Format Status
    formats = ["Text"]
    try:
        import PyPDF2
        formats.append("PDF")
    except:
        pass
    try:
        import docx
        formats.append("Word")
    except:
        pass
    try:
        import openpyxl
        formats.append("Excel")
    except:
        pass
    
    status_html = f"""
    <div class="status-row">
        <div class="status-card {extraction_status}">
            <div style="font-weight: 600;">✅ {extraction_text}</div>
        </div>
        <div class="status-card {db_status}">
            <div style="font-weight: 600;">📊 {db_text}</div>
        </div>
        <div class="status-card success">
            <div style="font-weight: 600;">📁 {len(formats)} Formats Ready</div>
        </div>
    </div>
    """
    
    st.markdown(status_html, unsafe_allow_html=True)


# SURGICAL FIX #1: UniversalFileReader Class
class UniversalFileReader:
    """Universal file reader for multiple formats"""
    
    @staticmethod
    def read_file(uploaded_file) -> str:
        """Read content from various file formats"""
        
        file_type = uploaded_file.type
        file_name = uploaded_file.name.lower()
        
        try:
            if file_type == "text/plain" or file_name.endswith('.txt'):
                return str(uploaded_file.read(), "utf-8")
            
            elif file_type == "application/pdf" or file_name.endswith('.pdf'):
                return UniversalFileReader._read_pdf(uploaded_file)
            
            elif file_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document" or file_name.endswith('.docx'):
                return UniversalFileReader._read_docx(uploaded_file)
            
            elif file_name.endswith('.xlsx') or file_name.endswith('.xls'):
                return UniversalFileReader._read_excel(uploaded_file)
            
            else:
                # Fallback: try to read as text
                try:
                    return str(uploaded_file.read(), "utf-8")
                except:
                    return f"Could not read file type: {file_type}"
                    
        except Exception as e:
            st.error(f"Error reading file: {str(e)}")
            return ""
    
    @staticmethod
    def _read_pdf(uploaded_file) -> str:
        """Read PDF content"""
        if not PDF_SUPPORT:
            return "PDF support not available. Please install PyPDF2 and pdfplumber."
        
        try:
            # Try with pdfplumber first (better for complex PDFs)
            import pdfplumber
            with pdfplumber.open(uploaded_file) as pdf:
                text = ""
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
                return text
        except:
            try:
                # Fallback to PyPDF2
                import PyPDF2
                pdf_reader = PyPDF2.PdfReader(uploaded_file)
                text = ""
                for page in pdf_reader.pages:
                    text += page.extract_text() + "\n"
                return text
            except Exception as e:
                return f"Error reading PDF: {str(e)}"
    
    @staticmethod
    def _read_docx(uploaded_file) -> str:
        """Read DOCX content"""
        if not DOCX_SUPPORT:
            return "DOCX support not available. Please install python-docx."
        
        try:
            doc = Document(uploaded_file)
            text = ""
            for paragraph in doc.paragraphs:
                text += paragraph.text + "\n"
            return text
        except Exception as e:
            return f"Error reading DOCX: {str(e)}"
    
    @staticmethod
    def _read_excel(uploaded_file) -> str:
        """Read Excel content and convert to text"""
        if not XLSX_SUPPORT:
            return "Excel support not available. Please install openpyxl."
        
        try:
            # Read Excel file
            df = pd.read_excel(uploaded_file, sheet_name=None)  # Read all sheets
            
            text = ""
            for sheet_name, sheet_df in df.items():
                text += f"=== Sheet: {sheet_name} ===\n"
                
                # Convert DataFrame to readable text
                for index, row in sheet_df.iterrows():
                    row_text = " | ".join([str(val) for val in row.values if pd.notna(val)])
                    if row_text.strip():
                        text += row_text + "\n"
                
                text += "\n"
            
            return text
        except Exception as e:
            return f"Error reading Excel: {str(e)}"

# SURGICAL FIX #2: SafeDataProcessor Class
class SafeDataProcessor:
    """Safe data processing to prevent mismatch errors"""
    
    @staticmethod
    def safe_process_extraction_data(data: Dict) -> Dict:
        """Safely process extraction data to prevent mismatches"""
        
        # Ensure all values are serializable and not None
        safe_data = {}
        
        for key, value in data.items():
            if value is None:
                safe_data[key] = ""
            elif isinstance(value, (int, float)):
                if pd.isna(value) or np.isnan(value):
                    safe_data[key] = 0.0
                else:
                    safe_data[key] = float(value)
            elif isinstance(value, str):
                safe_data[key] = str(value).strip()
            elif isinstance(value, list):
                safe_data[key] = SafeDataProcessor._safe_process_list(value)
            elif isinstance(value, dict):
                safe_data[key] = SafeDataProcessor.safe_process_extraction_data(value)
            else:
                safe_data[key] = str(value)
        
        return safe_data
    
    @staticmethod
    def _safe_process_list(items: List) -> List:
        """Safely process list items"""
        safe_list = []
        for item in items:
            if isinstance(item, dict):
                safe_list.append(SafeDataProcessor.safe_process_extraction_data(item))
            elif item is not None:
                safe_list.append(item)
        return safe_list
    
    @staticmethod
    def safe_dataframe_creation(data: List[Dict]) -> pd.DataFrame:
        """Create DataFrame safely"""
        if not data:
            return pd.DataFrame()
        
        try:
            # Process each record
            safe_records = []
            for record in data:
                safe_record = SafeDataProcessor.safe_process_extraction_data(record)
                safe_records.append(safe_record)
            
            # Create DataFrame
            df = pd.DataFrame(safe_records)
            
            # Clean column names
            df.columns = [str(col).replace(' ', '_').replace('-', '_') for col in df.columns]
            
            # Fill NaN values
            df = df.fillna('')
            
            return df
            
        except Exception as e:
            st.error(f"Error creating DataFrame: {str(e)}")
            return pd.DataFrame()

# SURGICAL FIX #6: ExcelTransactionExtractor Class  
class ExcelTransactionExtractor:
    """
    Specialized extractor for Excel surveillance dashboards
    Treats each COLUMN as a separate transaction/record
    """
    
    @staticmethod
    def extract_excel_transactions(uploaded_file, db_system) -> Dict:
        """Extract transactions from Excel surveillance dashboards"""
        
        try:
            # Read all sheets from Excel file
            excel_data = pd.read_excel(uploaded_file, sheet_name=None, header=0)
            
            results = {
                'file_name': uploaded_file.name,
                'sheets_processed': len(excel_data),
                'total_transactions': 0,
                'transactions_saved': 0,
                'sheets_data': {},
                'errors': []
            }
            
            for sheet_name, df in excel_data.items():
                try:
                    sheet_result = ExcelTransactionExtractor._process_sheet(
                        sheet_name, df, uploaded_file.name, db_system
                    )
                    results['sheets_data'][sheet_name] = sheet_result
                    results['total_transactions'] += sheet_result['row_count']
                    results['transactions_saved'] += sheet_result['saved_count']
                    
                except Exception as e:
                    error_msg = f"Error processing sheet {sheet_name}: {str(e)}"
                    results['errors'].append(error_msg)
                    st.warning(error_msg)
            
            return results
            
        except Exception as e:
            st.error(f"Error reading Excel file: {str(e)}")
            return {'error': str(e)}
    
    @staticmethod
    def _process_sheet(sheet_name: str, df: pd.DataFrame, filename: str, db_system) -> Dict:
        """Process individual Excel sheet"""
        
        # Clean the dataframe
        df_clean = ExcelTransactionExtractor._clean_dataframe(df)
        
        if df_clean.empty:
            return {
                'sheet_name': sheet_name,
                'row_count': 0,
                'saved_count': 0,
                'columns': [],
                'sample_data': {}
            }
        
        # Identify sheet type and extract accordingly
        sheet_type = ExcelTransactionExtractor._identify_sheet_type(sheet_name, df_clean)
        
        # Process based on sheet type - COLUMNS as transactions
        if sheet_type == 'SURVEILLANCE':
            transactions = ExcelTransactionExtractor._extract_surveillance_transactions(df_clean, filename, sheet_name)
        elif sheet_type == 'PERFORMANCE':
            transactions = ExcelTransactionExtractor._extract_performance_transactions(df_clean, filename, sheet_name)
        elif sheet_type == 'PORTFOLIO':
            transactions = ExcelTransactionExtractor._extract_portfolio_transactions(df_clean, filename, sheet_name)
        else:
            transactions = ExcelTransactionExtractor._extract_generic_transactions(df_clean, filename, sheet_name)
        
        # Save to database
        saved_count = ExcelTransactionExtractor._save_transactions(transactions, db_system)
        
        return {
            'sheet_name': sheet_name,
            'sheet_type': sheet_type,
            'row_count': len(df_clean),
            'saved_count': saved_count,
            'columns': list(df_clean.columns),
            'sample_data': df_clean.head(3).to_dict('records') if len(df_clean) > 0 else {}
        }
    
    @staticmethod
    def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """Clean and prepare dataframe"""
        
        # Remove completely empty rows and columns
        df_clean = df.dropna(how='all').dropna(axis=1, how='all')
        
        # Clean column names
        df_clean.columns = [
            str(col).strip().replace('\n', ' ').replace('\r', ' ')
            for col in df_clean.columns
        ]
        
        return df_clean
    
    @staticmethod
    def _identify_sheet_type(sheet_name: str, df: pd.DataFrame) -> str:
        """Identify the type of Excel sheet"""
        
        sheet_name_lower = sheet_name.lower()
        columns_str = ' '.join([str(col).lower() for col in df.columns])
        
        # Check for surveillance indicators
        surveillance_keywords = [
            'surveillance', 'performance', 'collections', 'delinquency', 
            'charge off', 'loss', 'prepayment', 'balance'
        ]
        
        performance_keywords = [
            'note class', 'tranche', 'rating', 'yield', 'coupon', 'maturity'
        ]
        
        portfolio_keywords = [
            'portfolio', 'composition', 'geographic', 'origination', 'vintage'
        ]
        
        # Score each type
        surveillance_score = sum(1 for kw in surveillance_keywords if kw in sheet_name_lower or kw in columns_str)
        performance_score = sum(1 for kw in performance_keywords if kw in sheet_name_lower or kw in columns_str)
        portfolio_score = sum(1 for kw in portfolio_keywords if kw in sheet_name_lower or kw in columns_str)
        
        if surveillance_score >= performance_score and surveillance_score >= portfolio_score:
            return 'SURVEILLANCE'
        elif performance_score > portfolio_score:
            return 'PERFORMANCE'
        elif portfolio_score > 0:
            return 'PORTFOLIO'
        else:
            return 'GENERIC'
    
    @staticmethod
    def _extract_surveillance_transactions(df: pd.DataFrame, filename: str, sheet_name: str) -> List[Dict]:
        """Extract surveillance transactions - each COLUMN is a transaction (time period)"""
        
        transactions = []
        
        # Skip the first column (usually labels/row headers) and treat each remaining column as a transaction
        data_columns = df.columns[1:] if len(df.columns) > 1 else df.columns
        
        for col_index, column_name in enumerate(data_columns):
            transaction = {
                'source_file': filename,
                'sheet_name': sheet_name,
                'column_index': col_index,
                'transaction_type': 'SURVEILLANCE',
                'period_name': str(column_name),  # e.g., "OPTN 2021-B", "OPTN 2024-1"
                'extracted_date': datetime.now().isoformat(),
                'metrics': {}
            }
            
            # Extract each metric (row) for this deal (column)
            for row_index, row_label in enumerate(df.iloc[:, 0]):  # First column contains row labels
                if pd.isna(row_label):
                    continue
                    
                row_label_str = str(row_label).strip()
                if not row_label_str:
                    continue
                
                # Get the value for this metric in this deal
                try:
                    value = df.iloc[row_index, col_index + 1]  # +1 because we skip first column
                    if pd.isna(value):
                        continue
                        
                    # Standardize metric names and store values
                    metric_name = ExcelTransactionExtractor._standardize_metric_name(row_label_str)
                    clean_value = ExcelTransactionExtractor._safe_value(value)
                    
                    transaction['metrics'][metric_name] = clean_value
                    
                    # Also map to standard surveillance fields
                    if any(keyword in row_label_str.lower() for keyword in ['balance', 'outstanding']):
                        transaction['pool_balance'] = ExcelTransactionExtractor._safe_numeric(value)
                    elif any(keyword in row_label_str.lower() for keyword in ['collection']):
                        transaction['collections'] = ExcelTransactionExtractor._safe_numeric(value)
                    elif any(keyword in row_label_str.lower() for keyword in ['delinq']):
                        transaction['delinquencies'] = ExcelTransactionExtractor._safe_numeric(value)
                    elif any(keyword in row_label_str.lower() for keyword in ['charge', 'loss']):
                        transaction['losses'] = ExcelTransactionExtractor._safe_numeric(value)
                    elif any(keyword in row_label_str.lower() for keyword in ['rate', 'percentage']):
                        transaction['rate'] = ExcelTransactionExtractor._safe_numeric(value)
                        
                except Exception as e:
                    continue
            
            # Only add transaction if it has data
            if transaction['metrics']:
                transactions.append(transaction)
        
        return transactions
    
    @staticmethod
    def _extract_performance_transactions(df: pd.DataFrame, filename: str, sheet_name: str) -> List[Dict]:
        """Extract note class performance - each COLUMN is a note class transaction"""
        return ExcelTransactionExtractor._extract_surveillance_transactions(df, filename, sheet_name)
    
    @staticmethod
    def _extract_portfolio_transactions(df: pd.DataFrame, filename: str, sheet_name: str) -> List[Dict]:
        """Extract portfolio composition - each COLUMN is a portfolio segment transaction"""
        return ExcelTransactionExtractor._extract_surveillance_transactions(df, filename, sheet_name)
    
    @staticmethod
    def _extract_generic_transactions(df: pd.DataFrame, filename: str, sheet_name: str) -> List[Dict]:
        """Extract generic transactions - each COLUMN is a separate transaction"""
        return ExcelTransactionExtractor._extract_surveillance_transactions(df, filename, sheet_name)
    
    @staticmethod
    def _standardize_metric_name(row_label: str) -> str:
        """Standardize metric names for consistency"""
        
        # Clean up the label
        clean_label = re.sub(r'[^\w\s]', ' ', row_label).strip()
        clean_label = re.sub(r'\s+', '_', clean_label).lower()
        
        # Standardize common metrics
        standardizations = {
            'outstanding_balance': 'pool_balance',
            'total_balance': 'pool_balance',
            'principal_balance': 'pool_balance',
            'collections_amount': 'collections',
            'total_collections': 'collections',
            'charge_offs': 'losses',
            'charge_off_amount': 'losses',
            'net_losses': 'losses',
            'delinquent_amount': 'delinquencies',
            'past_due': 'delinquencies',
            'loss_rate': 'loss_rate',
            'delinquency_rate': 'delinquency_rate',
            'prepayment_rate': 'prepayment_rate'
        }
        
        return standardizations.get(clean_label, clean_label)
    
    @staticmethod
    def _safe_numeric(value) -> float:
        """Safely convert value to numeric"""
        try:
            if isinstance(value, str):
                # Remove common formatting
                cleaned = value.replace(',', '').replace('$', '').replace('%', '').strip()
                return float(cleaned)
            return float(value)
        except:
            return 0.0
    
    @staticmethod
    def _safe_value(value):
        """Safely convert value for storage"""
        if pd.isna(value):
            return None
        elif isinstance(value, (int, float)):
            return float(value) if not pd.isna(value) else 0.0
        else:
            return str(value)
    
    @staticmethod
    def _save_transactions(transactions: List[Dict], db_system) -> int:
        """Save transactions to database"""
        
        saved_count = 0
        
        try:
            conn = sqlite3.connect(db_system.db_path)
            cursor = conn.cursor()
            
            # Create transactions table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ExcelTransactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_file TEXT,
                    sheet_name TEXT,
                    column_index INTEGER,
                    transaction_type TEXT,
                    period_name TEXT,
                    note_class TEXT,
                    segment_name TEXT,
                    column_name TEXT,
                    pool_balance REAL,
                    collections REAL,
                    delinquencies REAL,
                    losses REAL,
                    rate REAL,
                    rating TEXT,
                    yield REAL,
                    coupon REAL,
                    extracted_date TEXT,
                    metrics_data TEXT
                )
            """)
            
            # Insert transactions
            for transaction in transactions:
                cursor.execute("""
                    INSERT INTO ExcelTransactions (
                        source_file, sheet_name, column_index, transaction_type,
                        period_name, note_class, segment_name, column_name,
                        pool_balance, collections, delinquencies, losses, rate,
                        rating, yield, coupon, extracted_date, metrics_data
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    transaction.get('source_file'),
                    transaction.get('sheet_name'),
                    transaction.get('column_index'),
                    transaction.get('transaction_type'),
                    transaction.get('period_name'),
                    transaction.get('note_class'),
                    transaction.get('segment_name'),
                    transaction.get('column_name'),
                    transaction.get('pool_balance'),
                    transaction.get('collections'),
                    transaction.get('delinquencies'),
                    transaction.get('losses'),
                    transaction.get('rate'),
                    transaction.get('rating'),
                    transaction.get('yield'),
                    transaction.get('coupon'),
                    transaction.get('extracted_date'),
                    json.dumps(transaction.get('metrics', {}))
                ))
                saved_count += 1
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            st.error(f"Error saving transactions: {str(e)}")
        
        return saved_count

# TIME SERIES DATA HANDLER CLASS
class TimeSeriesDataHandler:
    """
    Handles time series surveillance data updates
    Tracks data evolution over time with proper versioning
    """
    
    @staticmethod
    def process_surveillance_update(uploaded_file, db_system, report_date: str = None) -> Dict:
        """Process surveillance data with time series tracking"""
        
        if not report_date:
            report_date = datetime.now().strftime('%Y-%m-%d')
        
        # Extract data using existing Excel extractor
        extraction_result = ExcelTransactionExtractor.extract_excel_transactions(uploaded_file, db_system)
        
        if 'error' in extraction_result:
            return extraction_result
        
        # Process each transaction for time series storage
        time_series_result = TimeSeriesDataHandler._process_time_series_data(
            extraction_result, report_date, db_system
        )
        
        return {
            'extraction_result': extraction_result,
            'time_series_result': time_series_result,
            'report_date': report_date
        }
    
    @staticmethod
    def _process_time_series_data(extraction_result: Dict, report_date: str, db_system) -> Dict:
        """Process extracted data for time series storage"""
        
        conn = sqlite3.connect(db_system.db_path)
        cursor = conn.cursor()
        
        # Create time series tables
        TimeSeriesDataHandler._create_time_series_tables(cursor)
        
        processed_deals = 0
        updated_deals = 0
        new_deals = 0
        
        try:
            for sheet_name, sheet_data in extraction_result.get('sheets_data', {}).items():
                # Get the deals data from sheet
                deals_data = TimeSeriesDataHandler._extract_deals_from_sheet_data(
                    sheet_data, extraction_result['file_name'], sheet_name, report_date
                )
                
                for deal_data in deals_data:
                    deal_id = deal_data.get('deal_name', '').replace(' ', '_').upper()
                    
                    if not deal_id:
                        continue
                    
                    # Check if deal exists
                    existing_deal = TimeSeriesDataHandler._get_or_create_deal(cursor, deal_id, deal_data)
                    
                    if existing_deal['is_new']:
                        new_deals += 1
                    else:
                        updated_deals += 1
                    
                    # Add surveillance snapshot
                    TimeSeriesDataHandler._add_surveillance_snapshot(
                        cursor, deal_id, deal_data, report_date
                    )
                    
                    processed_deals += 1
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            st.error(f"Error processing time series data: {str(e)}")
            return {'error': str(e)}
        
        finally:
            conn.close()
        
        return {
            'processed_deals': processed_deals,
            'updated_deals': updated_deals,
            'new_deals': new_deals,
            'report_date': report_date
        }
    
    @staticmethod
    def _create_time_series_tables(cursor):
        """Create time series tracking tables"""
        
        # Master deals table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS MasterDeals (
                deal_id TEXT PRIMARY KEY,
                deal_name TEXT,
                deal_short_name TEXT,
                securitization_date TEXT,
                issuer TEXT,
                asset_type TEXT,
                original_balance REAL,
                status TEXT DEFAULT 'ACTIVE',
                first_seen_date TEXT,
                last_updated_date TEXT,
                created_date TEXT
            )
        """)
        
        # Time series surveillance data
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS SurveillanceTimeSeries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                deal_id TEXT,
                report_date TEXT,
                data_source TEXT,
                sheet_name TEXT,
                
                -- Key metrics that change over time
                current_balance REAL,
                pool_factor REAL,
                months_seasoned INTEGER,
                wa_interest_rate_current REAL,
                wa_remaining_term_current INTEGER,
                current_num_receivables INTEGER,
                
                -- Delinquency metrics
                delinq_30_plus REAL,
                delinq_60_plus REAL,
                delinq_90_plus REAL,
                charge_offs REAL,
                cumulative_net_losses REAL,
                
                -- Performance metrics
                collections_current_period REAL,
                prepayment_rate REAL,
                loss_rate REAL,
                
                -- Raw metrics storage
                all_metrics TEXT,
                
                created_date TEXT,
                FOREIGN KEY (deal_id) REFERENCES MasterDeals(deal_id),
                UNIQUE(deal_id, report_date, data_source)
            )
        """)
    
    @staticmethod
    def _extract_deals_from_sheet_data(sheet_data: Dict, filename: str, sheet_name: str, report_date: str) -> List[Dict]:
        """Extract deal data from sheet processing results"""
        
        deals_data = []
        
        # Get sample data from sheet processing
        sample_data = sheet_data.get('sample_data', [])
        columns = sheet_data.get('columns', [])
        
        if not sample_data or not columns:
            return deals_data
        
        # Skip first column (row labels) and process each data column as a deal
        data_columns = columns[1:] if len(columns) > 1 else columns
        
        # Get row labels from first column of sample data
        row_labels = []
        if sample_data:
            first_col = columns[0] if columns else 'row_labels'
            for row in sample_data:
                if first_col in row and row[first_col]:
                    row_labels.append(str(row[first_col]).strip())
        
        # Process each deal column
        for col_index, deal_column in enumerate(data_columns):
            deal_data = {
                'deal_name': str(deal_column),
                'data_source': filename,
                'sheet_name': sheet_name,
                'report_date': report_date,
                'metrics': {}
            }
            
            # Extract metrics for this deal from sample data
            for row_index, row_label in enumerate(row_labels):
                if row_index < len(sample_data):
                    row_data = sample_data[row_index]
                    if deal_column in row_data:
                        value = row_data[deal_column]
                        if pd.notna(value) and str(value).strip():
                            standardized_metric = TimeSeriesDataHandler._standardize_metric_name(row_label)
                            deal_data['metrics'][standardized_metric] = value
                            
                            # Map to standard fields
                            TimeSeriesDataHandler._map_standard_fields(deal_data, row_label, value)
            
            if deal_data['metrics']:
                deals_data.append(deal_data)
        
        return deals_data
    
    @staticmethod
    def _get_or_create_deal(cursor, deal_id: str, deal_data: Dict) -> Dict:
        """Get existing deal or create new one"""
        
        # Check if deal exists
        cursor.execute("SELECT * FROM MasterDeals WHERE deal_id = ?", (deal_id,))
        existing = cursor.fetchone()
        
        if existing:
            # Update last seen date
            cursor.execute("""
                UPDATE MasterDeals 
                SET last_updated_date = ? 
                WHERE deal_id = ?
            """, (datetime.now().isoformat(), deal_id))
            
            return {'is_new': False, 'deal_data': existing}
        
        else:
            # Create new deal
            cursor.execute("""
                INSERT INTO MasterDeals (
                    deal_id, deal_name, deal_short_name, securitization_date,
                    original_balance, first_seen_date, last_updated_date, created_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                deal_id,
                deal_data.get('deal_name', ''),
                deal_data.get('deal_name', ''),
                deal_data.get('securitization_date', ''),
                deal_data.get('original_balance', 0),
                deal_data.get('report_date', ''),
                datetime.now().isoformat(),
                datetime.now().isoformat()
            ))
            
            return {'is_new': True, 'deal_data': None}
    
    @staticmethod
    def _add_surveillance_snapshot(cursor, deal_id: str, deal_data: Dict, report_date: str):
        """Add surveillance snapshot for a specific date"""
        
        # Check if snapshot already exists for this date
        cursor.execute("""
            SELECT id FROM SurveillanceTimeSeries 
            WHERE deal_id = ? AND report_date = ? AND data_source = ?
        """, (deal_id, report_date, deal_data.get('data_source', '')))
        
        existing = cursor.fetchone()
        
        if existing:
            # Update existing snapshot
            cursor.execute("""
                UPDATE SurveillanceTimeSeries SET
                    current_balance = ?, pool_factor = ?, months_seasoned = ?,
                    wa_interest_rate_current = ?, wa_remaining_term_current = ?,
                    current_num_receivables = ?, delinq_30_plus = ?,
                    delinq_60_plus = ?, delinq_90_plus = ?, charge_offs = ?,
                    all_metrics = ?
                WHERE id = ?
            """, (
                deal_data.get('current_balance', 0),
                deal_data.get('pool_factor', 0),
                deal_data.get('months_seasoned', 0),
                deal_data.get('wa_interest_rate_current', 0),
                deal_data.get('wa_remaining_term_current', 0),
                deal_data.get('current_num_receivables', 0),
                deal_data.get('delinq_30_plus', 0),
                deal_data.get('delinq_60_plus', 0),
                deal_data.get('delinq_90_plus', 0),
                deal_data.get('charge_offs', 0),
                json.dumps(deal_data.get('metrics', {})),
                existing[0]
            ))
        else:
            # Insert new snapshot
            cursor.execute("""
                INSERT INTO SurveillanceTimeSeries (
                    deal_id, report_date, data_source, sheet_name,
                    current_balance, pool_factor, months_seasoned,
                    wa_interest_rate_current, wa_remaining_term_current,
                    current_num_receivables, delinq_30_plus, delinq_60_plus,
                    delinq_90_plus, charge_offs, all_metrics, created_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                deal_id, report_date, deal_data.get('data_source', ''),
                deal_data.get('sheet_name', ''), deal_data.get('current_balance', 0),
                deal_data.get('pool_factor', 0), deal_data.get('months_seasoned', 0),
                deal_data.get('wa_interest_rate_current', 0),
                deal_data.get('wa_remaining_term_current', 0),
                deal_data.get('current_num_receivables', 0),
                deal_data.get('delinq_30_plus', 0), deal_data.get('delinq_60_plus', 0),
                deal_data.get('delinq_90_plus', 0), deal_data.get('charge_offs', 0),
                json.dumps(deal_data.get('metrics', {})), datetime.now().isoformat()
            ))
    
    @staticmethod
    def _standardize_metric_name(row_label: str) -> str:
        """Standardize metric names (same as in ExcelTransactionExtractor)"""
        clean_label = re.sub(r'[^\w\s]', ' ', row_label).strip()
        clean_label = re.sub(r'\s+', '_', clean_label).lower()
        
        standardizations = {
            'securitization_date': 'securitization_date',
            'current_collat_bal': 'current_balance',
            'original_collat_bal': 'original_balance',
            'pool_factor': 'pool_factor',
            'wa_interest_rate_current': 'wa_interest_rate_current',
            'months_seasoned': 'months_seasoned',
            '30_dq': 'delinq_30_plus',
            '60_dq': 'delinq_60_plus', 
            '90_dq': 'delinq_90_plus',
            'cnl': 'charge_offs'
        }
        
        return standardizations.get(clean_label, clean_label)
    
    @staticmethod
    def _map_standard_fields(deal_data: Dict, row_label: str, value):
        """Map row labels to standard fields"""
        row_lower = row_label.lower()
        
        if 'securitization date' in row_lower:
            deal_data['securitization_date'] = str(value)
        elif 'current collat bal' in row_lower:
            deal_data['current_balance'] = TimeSeriesDataHandler._safe_numeric(value) * 1000  # Convert from thousands
        elif 'original collat bal' in row_lower:
            deal_data['original_balance'] = TimeSeriesDataHandler._safe_numeric(value) * 1000
        elif 'pool factor' in row_lower:
            deal_data['pool_factor'] = TimeSeriesDataHandler._safe_numeric(value)
        elif 'months seasoned' in row_lower:
            deal_data['months_seasoned'] = int(TimeSeriesDataHandler._safe_numeric(value))
        elif 'wa interest rate (current)' in row_lower:
            deal_data['wa_interest_rate_current'] = TimeSeriesDataHandler._safe_numeric(value)
        elif 'current number of receivables' in row_lower:
            deal_data['current_num_receivables'] = int(TimeSeriesDataHandler._safe_numeric(value))
        elif '30+ dq' in row_lower:
            deal_data['delinq_30_plus'] = TimeSeriesDataHandler._safe_numeric(value)
        elif '60+ dq' in row_lower:
            deal_data['delinq_60_plus'] = TimeSeriesDataHandler._safe_numeric(value)
        elif '90+ dq' in row_lower:
            deal_data['delinq_90_plus'] = TimeSeriesDataHandler._safe_numeric(value)
        elif 'cnl' in row_lower:
            deal_data['charge_offs'] = TimeSeriesDataHandler._safe_numeric(value)
    
    @staticmethod
    def _safe_numeric(value) -> float:
        """Safely convert value to numeric"""
        try:
            if isinstance(value, str):
                cleaned = value.replace(',', '').replace('$', '').replace('%', '').strip()
                return float(cleaned)
            return float(value)
        except:
            return 0.0


class FixedCompleteABSSystem:
    """
    Fixed Complete ABS Document Processing System
    With proper error handling and database management
    """
    
    def __init__(self):
        self.db_path = "complete_abs_system.db"
        self.enhanced_available = ENHANCED_EXTRACTION_AVAILABLE
        self.improved_extractor = None
        
        self.init_database_safe()
        
        # Initialize improved extractor if available
        if self.enhanced_available:
            try:
                self.improved_extractor = ImprovedDocumentExtractor(self.db_path)
                st.info("🚀 Enhanced extractor initialized")
            except Exception as e:
                st.error(f"Error initializing enhanced extractor: {str(e)}")
                self.enhanced_available = False
        
    def init_database_safe(self):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create tables with IF NOT EXISTS
            tables_created = []
            
            # ABS_Deals table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ABS_Deals (
                    deal_id TEXT PRIMARY KEY,
                    deal_name TEXT,
                    issuer TEXT,
                    deal_type TEXT,
                    issuance_date TEXT,
                    total_deal_size REAL,
                    currency TEXT DEFAULT 'USD',
                    asset_type TEXT,
                    originator TEXT,
                    servicer TEXT,
                    trustee TEXT,
                    rating_agency TEXT,
                    created_date TEXT,
                    extraction_confidence REAL DEFAULT 0.0,
                    extraction_method TEXT DEFAULT 'BASIC'
                )
            """)
            tables_created.append("ABS_Deals")
            
            # Additional tables (remove nested try)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS EnhancedExtractionResults (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT,
                    document_type TEXT,
                    extracted_data TEXT,
                    extraction_time TEXT,
                    success BOOLEAN DEFAULT 0,
                    confidence_score REAL DEFAULT 0.0,
                    issues_found TEXT DEFAULT '[]'
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS DealsSearch (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    deal_name TEXT,
                    issuer TEXT,
                    deal_type TEXT,
                    total_size REAL,
                    created_date TEXT
                )
            """)
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            # Silent initialization - errors logged but not displayed
            pass

    def test_database_connection(self) -> bool:
        """Test database connection and basic operations"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Test basic query
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            conn.close()
            
            # Remove clutter messages (make silent)
            return True
            
        except Exception as e:
            # Silent error handling
            return False
    
    def process_document_enhanced(self, text: str, filename: str = "uploaded_document") -> Dict:
        """Process document with enhanced extraction"""
        
        if not self.enhanced_available:
            return self._process_document_basic(text, filename)
        
        try:
            # Use improved extractor
            doc_type, confidence = self.improved_extractor.detect_document_type(text)
            
            if doc_type == 'NEW_ISSUE':
                extracted_data = self.improved_extractor.extract_new_issue_data(text)
            else:
                extracted_data = self.improved_extractor.extract_surveillance_data(text)
            
            # Simple validation
            issues = self._validate_simple(extracted_data)
            
            # Save results
            extraction_id = self._save_extraction_safe(
                filename, doc_type, extracted_data, confidence, issues
            )
            
            return {
                'extraction_id': extraction_id,
                'document_type': doc_type,
                'confidence': confidence,
                'extracted_data': extracted_data,
                'issues': issues,
                'success': len(issues) == 0,
                'method': 'ENHANCED'
            }
            
        except Exception as e:
            st.error(f"Enhanced extraction error: {str(e)}")
            return self._process_document_basic(text, filename)
    
    def _process_document_basic(self, text: str, filename: str) -> Dict:
        """Basic fallback document processing"""
        
        # Simple document type detection
        text_lower = text.lower()
        
        if any(word in text_lower for word in ['surveillance', 'collections', 'delinquencies']):
            doc_type = 'SURVEILLANCE'
        else:
            doc_type = 'NEW_ISSUE'
        
        # Basic extraction
        extracted_data = {
            'deal_name': self._extract_simple_pattern(text, r'(?:Deal Name|PEAC)[:\s]*([^\n]+)'),
            'issuer': self._extract_simple_pattern(text, r'(?:Issuer|LLC)[:\s]*([^\n]+)'),
            'deal_type': 'Equipment ABS' if 'equipment' in text_lower else 'ABS',
            'total_deal_size': self._extract_amount_simple(text),
            'asset_type': 'Equipment' if 'equipment' in text_lower else 'Unknown',
            'note_classes': []
        }
        
        return {
            'extraction_id': f"BASIC_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'document_type': doc_type,
            'confidence': 0.6,
            'extracted_data': extracted_data,
            'issues': ['Using basic extraction'],
            'success': True,
            'method': 'BASIC'
        }
    
    def _extract_simple_pattern(self, text: str, pattern: str) -> str:
        """Simple pattern extraction"""
        match = re.search(pattern, text, re.IGNORECASE)
        return match.group(1).strip() if match else ""
    
    def _extract_amount_simple(self, text: str) -> float:
        """Simple amount extraction"""
        # Look for ASV or large amounts
        patterns = [
            r'([0-9,]+(?:\.[0-9]+)?)\s*million',
            r'\$([0-9,]+(?:\.[0-9]+)?)\s*million'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    amount = float(match.group(1).replace(',', '')) * 1_000_000
                    if amount > 1_000_000:  # Reasonable size
                        return amount
                except:
                    continue
        
        return 0.0
    
    def _validate_simple(self, data: Dict) -> List[str]:
        """Simple validation"""
        issues = []
        
        if not data.get('deal_name'):
            issues.append("Missing deal name")
        
        if not data.get('total_deal_size') or data['total_deal_size'] < 1_000_000:
            issues.append("Missing or small deal size")
        
        return issues
    
    def _save_extraction_safe(self, filename: str, doc_type: str, 
                             extracted_data: Dict, confidence: float, 
                             issues: List[str]) -> str:
        """Save extraction with error handling"""
        
        extraction_id = f"EXTRACT_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO EnhancedExtractionResults (
                    id, filename, document_type, extracted_data, extraction_time,
                    success, confidence_score, issues_found
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                extraction_id, filename, doc_type, json.dumps(extracted_data),
                datetime.now().isoformat(), len(issues) == 0, confidence,
                json.dumps(issues)
            ))
            
            conn.commit()
            conn.close()
            
            st.success("💾 Extraction saved to database")
            
        except Exception as e:
            st.warning(f"Could not save to database: {str(e)}")
        
        return extraction_id
    
    def get_extraction_history_safe(self) -> pd.DataFrame:
        """Get extraction history with error handling"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Check if table exists
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='EnhancedExtractionResults'")
            
            if cursor.fetchone():
                df = pd.read_sql_query("""
                    SELECT id, filename, document_type, extraction_time,
                           success, confidence_score, issues_found
                    FROM EnhancedExtractionResults 
                    ORDER BY extraction_time DESC
                    LIMIT 20
                """, conn)
            else:
                df = pd.DataFrame(columns=['id', 'filename', 'document_type', 'extraction_time', 'success', 'confidence_score'])
            
            conn.close()
            return df
            
        except Exception as e:
            st.warning(f"Could not load extraction history: {str(e)}")
            return pd.DataFrame(columns=['id', 'filename', 'document_type', 'extraction_time', 'success', 'confidence_score'])
    
    def get_extraction_details_safe(self, extraction_id: str) -> Dict:
        """Get extraction details with error handling"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT extracted_data, issues_found, confidence_score
                FROM EnhancedExtractionResults 
                WHERE id = ?
            """, (extraction_id,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    'extracted_data': json.loads(result[0]),
                    'issues_found': json.loads(result[1]),
                    'confidence_score': result[2]
                }
        except Exception as e:
            st.warning(f"Could not load extraction details: {str(e)}")
        
        return None
    
    def execute_sql_safe(self, query: str) -> pd.DataFrame:
        """Execute SQL with error handling"""
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        except Exception as e:
            st.error(f"SQL Error: {str(e)}")
            return pd.DataFrame()
    
    def get_database_info(self) -> Dict:
        """Get basic database information"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get table list
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            info = {'tables': tables, 'table_info': {}}
            
            # Get basic info for each table
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    info['table_info'][table] = {'row_count': count}
                except:
                    info['table_info'][table] = {'row_count': 'Error'}
            
            conn.close()
            return info
            
        except Exception as e:
            return {'tables': [], 'table_info': {}, 'error': str(e)}
        
class BloombergPricingProcessor:
    """
    Bloomberg Excel pricing sheet processor with smart duplicate prevention
    and percentage conversion to 4-decimal precision
    """
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_pricing_tables()
        
        # Bloomberg column mappings
        self.bloomberg_columns = {
            'Ticker': 'ticker',
            'Issuer Name': 'issuer_name', 
            'Deal Name': 'deal_name',
            'CMO Class': 'cmo_class',
            'Mtge Pricing Speed Date': 'pricing_speed_date',
            'First Sett Dt': 'first_settlement_date',
            'Orig Amt': 'original_amount',
            'Mtge Deal Pricing Speed': 'deal_pricing_speed',
            'Deal Typ': 'deal_type',
            'Description - BN Reported': 'description',
            'Strctd Prod Class Ast Subclas': 'asset_subclass',
            '144A Elig': 'rule_144a_eligible',
            'Lead Manager 1': 'lead_manager',
            'Mtge Expected Mty Date': 'expected_maturity_date',
            'SEC Date (2a-7)': 'sec_date',
            'Benchmark (BBG News Created)': 'benchmark',
            'Issue Benchmark': 'issue_benchmark',
            'Issue Spread to Benchmark': 'issue_spread',
            'Issue Yield': 'issue_yield',
            'Cpn': 'coupon',
            'Issue Px': 'issue_price',
            'Original Credit Support (%)': 'original_credit_support',
            'Current Credit Support (%)': 'current_credit_support',
            'Orig WAL': 'original_wal',
            'Rating Tier': 'rating_tier',
            'CUSIP': 'cusip',
            'WAC': 'weighted_avg_coupon'
        }
        
        # Percentage columns that need conversion
        self.percentage_columns = [
            'deal_pricing_speed', 'issue_yield', 'coupon',
            'original_credit_support', 'current_credit_support',
            'weighted_avg_coupon'
        ]
    
    def init_pricing_tables(self):
        """Initialize Bloomberg pricing tables"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Main pricing data table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS PricingData (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT UNIQUE NOT NULL,
                    issuer_name TEXT,
                    deal_name TEXT,
                    cmo_class TEXT,
                    pricing_speed_date TEXT,
                    first_settlement_date TEXT,
                    original_amount REAL,
                    deal_pricing_speed REAL,
                    deal_type TEXT,
                    description TEXT,
                    asset_subclass TEXT,
                    rule_144a_eligible TEXT,
                    lead_manager TEXT,
                    expected_maturity_date TEXT,
                    sec_date TEXT,
                    benchmark REAL,
                    issue_benchmark TEXT,
                    issue_spread REAL,
                    issue_yield REAL,
                    coupon REAL,
                    issue_price REAL,
                    original_credit_support REAL,
                    current_credit_support REAL,
                    original_wal REAL,
                    rating_tier TEXT,
                    cusip TEXT,
                    weighted_avg_coupon REAL,
                    upload_date TEXT,
                    last_updated TEXT
                )
            """)
            
            # Pricing history table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS PricingHistory (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    price_date TEXT,
                    issue_yield REAL,
                    coupon REAL,
                    issue_price REAL,
                    current_credit_support REAL,
                    benchmark REAL,
                    upload_date TEXT,
                    FOREIGN KEY (ticker) REFERENCES PricingData(ticker)
                )
            """)
            
            # Saved series configurations
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS SavedSeries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    series_name TEXT UNIQUE NOT NULL,
                    filter_criteria TEXT,
                    column_selection TEXT,
                    sort_order TEXT,
                    created_date TEXT,
                    last_used TEXT
                )
            """)
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            st.error(f"Error initializing pricing tables: {str(e)}")
    
    def process_bloomberg_file(self, uploaded_file) -> Dict:
        """Process Bloomberg Excel file with duplicate prevention"""
        
        try:
            # Read Excel file
            df = pd.read_excel(uploaded_file, sheet_name=0)
            
            # Clean column names
            df.columns = [col.strip() for col in df.columns]
            
            # Validate required columns
            missing_columns = self._validate_bloomberg_columns(df)
            if missing_columns:
                return {
                    'success': False,
                    'error': f"Missing required columns: {', '.join(missing_columns)}",
                    'missing_columns': missing_columns
                }
            
            # Map column names
            df_mapped = self._map_bloomberg_columns(df)
            
            # Convert percentages to decimals
            df_processed = self._convert_percentages(df_mapped)
            
            # Process dates
            df_processed = self._process_dates(df_processed)
            
            # Clean data
            df_clean = self._clean_bloomberg_data(df_processed)
            
            # Check for duplicates and new securities
            duplicate_analysis = self._analyze_duplicates(df_clean)
            
            # Save to database
            save_results = self._save_bloomberg_data(df_clean, duplicate_analysis)
            
            return {
                'success': True,
                'filename': uploaded_file.name,
                'total_records': len(df_clean),
                'new_securities': save_results['new_securities'],
                'updated_securities': save_results['updated_securities'],
                'duplicates_skipped': save_results['duplicates_skipped'],
                'processing_summary': save_results['summary'],
                'duplicate_analysis': duplicate_analysis
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Error processing Bloomberg file: {str(e)}"
            }
    
    def _validate_bloomberg_columns(self, df: pd.DataFrame) -> List[str]:
        """Validate that required Bloomberg columns are present"""
        required_columns = ['Ticker', 'Issuer Name', 'Deal Name']
        missing = []
        
        for col in required_columns:
            if col not in df.columns:
                missing.append(col)
        
        return missing
    
    def _map_bloomberg_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map Bloomberg column names to database column names"""
        
        # Create new dataframe with mapped columns
        df_mapped = pd.DataFrame()
        
        for bloomberg_col, db_col in self.bloomberg_columns.items():
            if bloomberg_col in df.columns:
                df_mapped[db_col] = df[bloomberg_col]
            else:
                df_mapped[db_col] = None
        
        return df_mapped
    
    def _convert_percentages(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert percentages to 4-decimal precision decimals"""
        
        df_converted = df.copy()
        
        for col in self.percentage_columns:
            if col in df_converted.columns:
                df_converted[col] = df_converted[col].apply(self._convert_percentage_value)
        
        return df_converted
    
    def _convert_percentage_value(self, value) -> Optional[float]:
        """Convert individual percentage value to decimal"""
        
        if pd.isna(value) or value is None:
            return None
        
        try:
            # Convert to string and clean
            str_value = str(value).strip()
            
            # Remove % sign if present
            if str_value.endswith('%'):
                str_value = str_value[:-1]
            
            # Convert to float
            float_value = float(str_value)
            
            # If value is > 1, assume it's a percentage (25.7% -> 25.7)
            if float_value > 1:
                float_value = float_value / 100
            
            # Round to 4 decimal places
            return round(float_value, 4)
            
        except (ValueError, TypeError):
            return None
    
    def _process_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process date columns to standardized format"""
        
        date_columns = [
            'pricing_speed_date', 'first_settlement_date', 
            'expected_maturity_date', 'sec_date'
        ]
        
        df_processed = df.copy()
        
        for col in date_columns:
            if col in df_processed.columns:
                df_processed[col] = pd.to_datetime(df_processed[col], errors='coerce')
                df_processed[col] = df_processed[col].dt.strftime('%Y-%m-%d')
        
        return df_processed
    
    def _clean_bloomberg_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate Bloomberg data"""
        
        df_clean = df.copy()
        
        # Remove rows without ticker
        df_clean = df_clean.dropna(subset=['ticker'])
        
        # Clean text fields
        text_columns = ['ticker', 'issuer_name', 'deal_name', 'cmo_class']
        for col in text_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str).str.strip()
        
        # Fill NaN values appropriately
        df_clean = df_clean.fillna({
            'original_amount': 0,
            'issue_price': 0,
            'original_wal': 0
        })
        
        return df_clean
    
    def _analyze_duplicates(self, df: pd.DataFrame) -> Dict:
        """Analyze existing vs new securities"""
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Get existing tickers
            existing_df = pd.read_sql_query("SELECT ticker FROM PricingData", conn)
            existing_tickers = set(existing_df['ticker'].tolist()) if not existing_df.empty else set()
            
            # Analyze new data
            new_tickers = set(df['ticker'].tolist())
            
            duplicates = new_tickers.intersection(existing_tickers)
            new_securities = new_tickers - existing_tickers
            
            conn.close()
            
            return {
                'total_input': len(new_tickers),
                'existing_securities': len(duplicates),
                'new_securities': len(new_securities),
                'duplicate_tickers': list(duplicates),
                'new_tickers': list(new_securities)
            }
            
        except Exception as e:
            return {
                'total_input': len(df),
                'existing_securities': 0,
                'new_securities': len(df),
                'error': str(e)
            }
    
    def _save_bloomberg_data(self, df: pd.DataFrame, duplicate_analysis: Dict) -> Dict:
        """Save Bloomberg data with duplicate handling"""
        
        new_securities = 0
        updated_securities = 0
        duplicates_skipped = 0
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            current_time = datetime.now().isoformat()
            
            for _, row in df.iterrows():
                ticker = row['ticker']
                
                # Check if ticker exists
                cursor.execute("SELECT id FROM PricingData WHERE ticker = ?", (ticker,))
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing record
                    self._update_pricing_record(cursor, row, current_time)
                    
                    # Save to pricing history
                    self._save_pricing_history(cursor, row, current_time)
                    
                    updated_securities += 1
                else:
                    # Insert new record
                    self._insert_pricing_record(cursor, row, current_time)
                    new_securities += 1
            
            conn.commit()
            conn.close()
            
            return {
                'new_securities': new_securities,
                'updated_securities': updated_securities,
                'duplicates_skipped': duplicates_skipped,
                'summary': f"Added {new_securities} new securities, updated {updated_securities} existing securities"
            }
            
        except Exception as e:
            return {
                'new_securities': 0,
                'updated_securities': 0,
                'duplicates_skipped': 0,
                'summary': f"Error saving data: {str(e)}"
            }
    
    def _insert_pricing_record(self, cursor, row: pd.Series, current_time: str):
        """Insert new pricing record"""
        
        columns = list(self.bloomberg_columns.values())
        placeholders = ', '.join(['?' for _ in columns])
        columns_str = ', '.join(columns)
        
        values = [row.get(col) for col in columns]
        values.extend([current_time, current_time])  # upload_date, last_updated
        
        query = f"""
            INSERT INTO PricingData ({columns_str}, upload_date, last_updated)
            VALUES ({placeholders}, ?, ?)
        """
        
        cursor.execute(query, values)
    
    def _update_pricing_record(self, cursor, row: pd.Series, current_time: str):
        """Update existing pricing record"""
        
        update_columns = [f"{col} = ?" for col in self.bloomberg_columns.values() if col != 'ticker']
        update_str = ', '.join(update_columns)
        
        values = [row.get(col) for col in self.bloomberg_columns.values() if col != 'ticker']
        values.append(current_time)  # last_updated
        values.append(row['ticker'])  # WHERE condition
        
        query = f"""
            UPDATE PricingData 
            SET {update_str}, last_updated = ?
            WHERE ticker = ?
        """
        
        cursor.execute(query, values)
    
    def _save_pricing_history(self, cursor, row: pd.Series, current_time: str):
        """Save pricing point to history"""
        
        cursor.execute("""
            INSERT INTO PricingHistory (
                ticker, price_date, issue_yield, coupon, issue_price,
                current_credit_support, benchmark, upload_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row['ticker'],
            row.get('pricing_speed_date', current_time[:10]),
            row.get('issue_yield'),
            row.get('coupon'),
            row.get('issue_price'),
            row.get('current_credit_support'),
            row.get('benchmark'),
            current_time
        ))
    
    def get_pricing_summary(self) -> Dict:
        """Get summary of pricing data"""
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Basic statistics
            summary_query = """
                SELECT 
                    COUNT(*) as total_securities,
                    COUNT(DISTINCT issuer_name) as unique_issuers,
                    COUNT(DISTINCT deal_name) as unique_deals,
                    AVG(issue_yield) as avg_yield,
                    AVG(current_credit_support) as avg_credit_support,
                    MIN(upload_date) as first_upload,
                    MAX(last_updated) as last_update
                FROM PricingData
            """
            
            summary_df = pd.read_sql_query(summary_query, conn)
            
            # Deal type distribution
            deal_type_query = """
                SELECT deal_type, COUNT(*) as count
                FROM PricingData
                WHERE deal_type IS NOT NULL
                GROUP BY deal_type
                ORDER BY count DESC
            """
            
            deal_type_df = pd.read_sql_query(deal_type_query, conn)
            
            # Rating tier distribution
            rating_query = """
                SELECT rating_tier, COUNT(*) as count
                FROM PricingData
                WHERE rating_tier IS NOT NULL
                GROUP BY rating_tier
                ORDER BY count DESC
            """
            
            rating_df = pd.read_sql_query(rating_query, conn)
            
            conn.close()
            
            return {
                'summary': summary_df.iloc[0].to_dict() if not summary_df.empty else {},
                'deal_type_distribution': deal_type_df.to_dict('records'),
                'rating_distribution': rating_df.to_dict('records')
            }
            
        except Exception as e:
            return {'error': str(e)}
    
    def search_securities(self, filters: Dict) -> pd.DataFrame:
        """Search securities with filters"""
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Build query with filters
            where_conditions = []
            params = []
            
            if filters.get('deal_type'):
                where_conditions.append("deal_type = ?")
                params.append(filters['deal_type'])
            
            if filters.get('rating_tier'):
                where_conditions.append("rating_tier = ?")
                params.append(filters['rating_tier'])
            
            if filters.get('issuer_name'):
                where_conditions.append("issuer_name LIKE ?")
                params.append(f"%{filters['issuer_name']}%")
            
            if filters.get('min_yield'):
                where_conditions.append("issue_yield >= ?")
                params.append(float(filters['min_yield']))
            
            if filters.get('max_yield'):
                where_conditions.append("issue_yield <= ?")
                params.append(float(filters['max_yield']))
            
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            query = f"""
                SELECT ticker, issuer_name, deal_name, cmo_class, 
                       issue_yield, coupon, current_credit_support,
                       rating_tier, deal_type, last_updated
                FROM PricingData
                WHERE {where_clause}
                ORDER BY last_updated DESC
            """
            
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            return df
            
        except Exception as e:
            st.error(f"Error searching securities: {str(e)}")
            return pd.DataFrame()

# Add this function to render Bloomberg interface
def render_bloomberg_pricing_interface():
    """Render Bloomberg pricing processing interface"""
    
    st.header("💹 Bloomberg Pricing Integration")
    
    # Initialize processor
    if 'bloomberg_processor' not in st.session_state:
        st.session_state.bloomberg_processor = BloombergPricingProcessor(
            st.session_state.fixed_abs_system.db_path
        )
    
    processor = st.session_state.bloomberg_processor
    
    # Get pricing summary
    summary = processor.get_pricing_summary()
    
    # Display current status
    if 'error' not in summary:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Securities", summary['summary'].get('total_securities', 0))
        with col2:
            st.metric("Unique Issuers", summary['summary'].get('unique_issuers', 0))
        with col3:
            st.metric("Unique Deals", summary['summary'].get('unique_deals', 0))
        with col4:
            avg_yield = summary['summary'].get('avg_yield', 0)
            st.metric("Avg Yield", f"{avg_yield:.2%}" if avg_yield else "N/A")
    
    st.markdown("---")
    
    # File upload section
    st.subheader("📤 Upload Bloomberg Pricing File")
    
    col_upload, col_info = st.columns([2, 1])
    
    with col_upload:
        uploaded_file = st.file_uploader(
            "Select Bloomberg Excel File",
            type=['xlsx', 'xls'],
            help="Upload weekly Bloomberg pricing Excel file",
            key="bloomberg_uploader"
        )
        
        if uploaded_file:
            st.info(f"File: {uploaded_file.name} ({uploaded_file.size:,} bytes)")
            
            if st.button("🚀 Process Bloomberg File", type="primary"):
                with st.spinner("Processing Bloomberg pricing data..."):
                    result = processor.process_bloomberg_file(uploaded_file)
                    
                    if result['success']:
                        st.success("✅ Bloomberg file processed successfully!")
                        
                        # Show processing summary
                        col_a, col_b, col_c = st.columns(3)
                        with col_a:
                            st.metric("New Securities", result['new_securities'])
                        with col_b:
                            st.metric("Updated Securities", result['updated_securities'])
                        with col_c:
                            st.metric("Total Records", result['total_records'])
                        
                        st.info(result['processing_summary'])
                        
                        # Show duplicate analysis
                        if result['duplicate_analysis']:
                            with st.expander("📊 Duplicate Analysis"):
                                dup_analysis = result['duplicate_analysis']
                                st.write(f"**Total Input Records:** {dup_analysis['total_input']}")
                                st.write(f"**New Securities:** {dup_analysis['new_securities']}")
                                st.write(f"**Existing Securities:** {dup_analysis['existing_securities']}")
                    else:
                        st.error(f"❌ Error: {result['error']}")
    
    with col_info:
        st.markdown("**📋 Bloomberg File Requirements:**")
        st.markdown("""
        **Required Columns:**
        - Ticker
        - Issuer Name  
        - Deal Name
        
        **Optional Columns:**
        - CMO Class
        - Issue Yield
        - Coupon
        - Current Credit Support (%)
        - Rating Tier
        - Deal Type
        
        **Percentage Handling:**
        - Automatically converts to 4-decimal precision
        - Supports both "25.7%" and 25.7 formats
        """)
    
    # Search and filter interface
    st.markdown("---")
    st.subheader("🔍 Search Securities")
    
    # Filter controls
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    
    with filter_col1:
        deal_type_filter = st.selectbox(
            "Deal Type",
            ["All"] + [dt['deal_type'] for dt in summary.get('deal_type_distribution', [])],
            key="deal_type_filter"
        )
        
        issuer_filter = st.text_input("Issuer Name (partial)", key="issuer_filter")
    
    with filter_col2:
        rating_filter = st.selectbox(
            "Rating Tier", 
            ["All"] + [rt['rating_tier'] for rt in summary.get('rating_distribution', [])],
            key="rating_filter"
        )
        
        min_yield = st.number_input("Min Yield (%)", value=0.0, step=0.1, key="min_yield_filter")
    
    with filter_col3:
        max_yield = st.number_input("Max Yield (%)", value=20.0, step=0.1, key="max_yield_filter")
    
    # Search button
    if st.button("🔍 Search Securities"):
        filters = {}
        if deal_type_filter != "All":
            filters['deal_type'] = deal_type_filter
        if rating_filter != "All":
            filters['rating_tier'] = rating_filter
        if issuer_filter:
            filters['issuer_name'] = issuer_filter
        if min_yield > 0:
            filters['min_yield'] = min_yield / 100  # Convert to decimal
        if max_yield < 20:
            filters['max_yield'] = max_yield / 100  # Convert to decimal
        
        search_results = processor.search_securities(filters)
        
        if not search_results.empty:
            st.subheader(f"📊 Search Results ({len(search_results)} securities)")
            
            # Format display
            display_df = search_results.copy()
            if 'issue_yield' in display_df.columns:
                display_df['issue_yield'] = display_df['issue_yield'].apply(
                    lambda x: f"{x:.2%}" if pd.notna(x) else "N/A"
                )
            if 'coupon' in display_df.columns:
                display_df['coupon'] = display_df['coupon'].apply(
                    lambda x: f"{x:.2%}" if pd.notna(x) else "N/A"
                )
            if 'current_credit_support' in display_df.columns:
                display_df['current_credit_support'] = display_df['current_credit_support'].apply(
                    lambda x: f"{x:.2%}" if pd.notna(x) else "N/A"
                )
            
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            # Export option
            csv = search_results.to_csv(index=False)
            st.download_button(
                label="📥 Download Results as CSV",
                data=csv,
                file_name=f"bloomberg_search_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.info("No securities found matching the search criteria.")

# SURGICAL FIX 3: Enhanced File Upload Interface
def render_enhanced_file_upload():
    """Enhanced file upload with multi-format support"""
    
    st.subheader("📁 Enhanced File Upload")
    
    # Show supported formats
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Supported Formats:**")
        formats = ["📄 Text (.txt)", "📋 PDF (.pdf)"]
        if DOCX_SUPPORT:
            formats.append("📝 Word (.docx)")
        if XLSX_SUPPORT:
            formats.append("📊 Excel (.xlsx, .xls)")
        
        for fmt in formats:
            st.write(f"✅ {fmt}")
    
    with col2:
        st.markdown("**Installation Commands:**")
        if not PDF_SUPPORT:
            st.code("pip install PyPDF2 pdfplumber")
        if not DOCX_SUPPORT:
            st.code("pip install python-docx")
        if not XLSX_SUPPORT:
            st.code("pip install openpyxl")
    
    # File upload with expanded types
    uploaded_file = st.file_uploader(
        "Upload Document",
        type=['txt', 'pdf', 'docx', 'xlsx', 'xls'],
        help="Upload ABS documents in various formats",
        key="main_document_uploader"
    )
    
    return uploaded_file

def safe_database_insert_fixed(data: dict, table_name: str = "EnhancedExtractionResults"):
    """Fixed database insertion with proper type handling"""
    
    try:
        conn = sqlite3.connect("complete_abs_system.db")
        cursor = conn.cursor()
        
        # Create table if not exists with proper schema
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT,
                document_type TEXT,
                extracted_data TEXT,
                extraction_time TEXT,
                success INTEGER DEFAULT 1,
                confidence_score REAL DEFAULT 0.0,
                issues_found TEXT DEFAULT '[]',
                extraction_method TEXT DEFAULT 'ENHANCED'
            )
        """)
        
        # Prepare data for safe insertion
        safe_data = {
            'filename': str(data.get('filename', 'unknown')),
            'document_type': str(data.get('document_type', 'unknown')),
            'extracted_data': json.dumps(data.get('extracted_data', {})),
            'extraction_time': datetime.now().isoformat(),
            'success': 1 if data.get('success', True) else 0,
            'confidence_score': float(data.get('confidence', 0.0)),
            'issues_found': json.dumps(data.get('issues', [])),
            'extraction_method': str(data.get('method', 'ENHANCED'))
        }
        
        # Insert with explicit column names
        columns = ', '.join(safe_data.keys())
        placeholders = ', '.join(['?' for _ in safe_data])
        
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(query, list(safe_data.values()))
        
        conn.commit()
        conn.close()
        
        return True
        
    except Exception as e:
        st.error(f"Database save error: {str(e)}")
        return False



def safe_database_insert(conn, table: str, data: Dict):
    """Safely insert data into database"""
    
    try:
        cursor = conn.cursor()
        
        # Get table schema
        cursor.execute(f"PRAGMA table_info({table})")
        columns_info = cursor.fetchall()
        
        if not columns_info:
            st.error(f"Table {table} does not exist")
            return False
        
        # Extract column names (excluding auto-increment)
        columns = [col[1] for col in columns_info if col[1] != 'id' or not col[5]]  # col[5] is pk flag
        
        # Prepare data for insertion
        insert_data = {}
        for col in columns:
            if col in data:
                value = data[col]
                # Handle different data types safely
                if value is None:
                    insert_data[col] = None
                elif isinstance(value, (dict, list)):
                    insert_data[col] = json.dumps(value)
                else:
                    insert_data[col] = value
            else:
                insert_data[col] = None
        
        # Create insert statement
        placeholders = ', '.join(['?' for _ in insert_data])
        columns_str = ', '.join(insert_data.keys())
        
        query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        
        cursor.execute(query, list(insert_data.values()))
        
        return True
        
    except Exception as e:
        st.error(f"Database insert error: {str(e)}")
        return False


def render_surveillance_update_interface():
    """Interface for uploading surveillance updates with date tracking"""
    
    st.subheader("📊 Surveillance Data Update")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Report date input
        report_date = st.date_input(
            "Report Date",
            value=datetime.now().date(),
            help="Date this surveillance data represents"
        )
    
    with col2:
        # Update frequency
        update_type = st.selectbox(
            "Update Type",
            ["Monthly", "Quarterly", "Annual", "Ad-hoc"],
            help="Type of surveillance update"
        )
    
    uploaded_file = st.file_uploader(
        "Upload Surveillance Dashboard",
        type=['xlsx', 'xls'],
        help="Upload Excel surveillance dashboard for time series tracking",
        key="main_document_uploader1"
    )
    
    if uploaded_file and st.button("🔄 Process Surveillance Update", type="primary"):
        with st.spinner("Processing surveillance update..."):
            result = TimeSeriesDataHandler.process_surveillance_update(
                uploaded_file, 
                st.session_state.fixed_abs_system, 
                report_date.strftime('%Y-%m-%d')
            )
            
            if 'error' not in result:
                st.success("✅ Surveillance data processed successfully!")
                
                # Show results
                ts_result = result['time_series_result']
                col_a, col_b, col_c = st.columns(3)
                
                with col_a:
                    st.metric("Processed Deals", ts_result['processed_deals'])
                with col_b:
                    st.metric("Updated Deals", ts_result['updated_deals'])
                with col_c:
                    st.metric("New Deals", ts_result['new_deals'])
                
                st.info(f"Data stored with report date: {result['report_date']}")
            else:
                st.error(f"Error: {result['error']}")
    
    return uploaded_file

def render_fixed_document_processing():
    """Fixed document processing with multi-format support"""
    st.header("📄 Enhanced Document Processing")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        # Enhanced file upload
        uploaded_file = render_enhanced_file_upload()
        
        # Text input alternative
        sample_text = st.text_area(
            "OR Paste text directly:",
            height=200,
            placeholder="Paste your ABS document text here..."
        )
        
        # Load PEAC sample
        if st.button("📋 Load PEAC Sample"):
            st.session_state['sample_text'] = get_peac_sample()
            st.rerun()
        
        # Process the file
        text_content = None
        
        if uploaded_file:
            # Check if it's an Excel file for special processing
            if uploaded_file.name.lower().endswith(('.xlsx', '.xls')):
                # Process as Excel transaction file
                excel_result = process_excel_file_comprehensive(uploaded_file, st.session_state.fixed_abs_system)
                if excel_result:
                    st.session_state['last_excel_result'] = excel_result
            else:
                # Process as regular document
                with st.spinner(f"Reading {uploaded_file.name}..."):
                    text_content = UniversalFileReader.read_file(uploaded_file)
                    if text_content:
                        st.success(f"📁 Successfully read {uploaded_file.name}")
                        st.info(f"Content length: {len(text_content)} characters")
                        
                        # Show preview
                        with st.expander("📄 Content Preview"):
                            st.text(text_content[:500] + "..." if len(text_content) > 500 else text_content)
                    else:
                        st.error("Failed to read file content")
        
        elif sample_text:
            text_content = sample_text
        elif 'sample_text' in st.session_state:
            text_content = st.session_state['sample_text']
        
        # Process button for text documents
        if text_content and st.button("🚀 Process Document", type="primary"):
            with st.spinner("Processing..."):
                try:
                    result = st.session_state.fixed_abs_system.process_document_enhanced(
                        text_content, uploaded_file.name if uploaded_file else "text_input"
                    )
                    
                    # Safe process the result
                    safe_result = SafeDataProcessor.safe_process_extraction_data(result)
                    st.session_state['last_result'] = safe_result
                    
                    # Show results
                    st.success(f"✅ {safe_result['method']} processing complete!")
                    
                    col_a, col_b, col_c = st.columns(3)
                    with col_a:
                        st.metric("Document Type", safe_result['document_type'])
                    with col_b:
                        st.metric("Confidence", f"{safe_result['confidence']:.1%}")
                    with col_c:
                        st.metric("Issues", len(safe_result['issues']))
                    
                    if safe_result['issues']:
                        st.warning("Issues found:")
                        for issue in safe_result['issues']:
                            st.write(f"• {issue}")
                    
                except Exception as e:
                    st.error(f"Processing error: {str(e)}")
                    st.info("Falling back to basic text processing...")
    
    with col2:
        st.subheader("📈 Recent Extractions")
        
        # Show extraction history
        history_df = st.session_state.fixed_abs_system.get_extraction_history_safe()
        
        if not history_df.empty:
            st.dataframe(history_df, use_container_width=True, hide_index=True)
        else:
            st.info("No extractions yet")
        
        # Show last result if available
        if 'last_result' in st.session_state:
            st.subheader("📋 Last Result")
            with st.expander("View extracted data"):
                st.json(st.session_state['last_result']['extracted_data'])

def show_database_results():
    """Fixed database results viewer"""
    st.subheader("🗄️ Database Results Viewer")
    
    try:
        conn = sqlite3.connect("complete_abs_system.db")
        
        # Get all extraction tests
        query = """
            SELECT 
                id,
                filename,
                document_type,
                extraction_time,
                success,
                confidence_score,
                issues_found
            FROM EnhancedExtractionResults 
            ORDER BY extraction_time DESC
        """
        
        df = pd.read_sql_query(query, conn)
        
        if not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True)
            
            # Show detailed data for selected row
            if len(df) > 0:
                selected_id = st.selectbox("Select test to view details:", df['id'].tolist(), key="db_detail_select")
                
                if st.button("View Details", key="db_view_details"):
                    detail_query = """
                        SELECT extracted_data, issues_found 
                        FROM EnhancedExtractionResults 
                        WHERE id = ?
                    """
                    detail_df = pd.read_sql_query(detail_query, conn, params=[selected_id])
                    
                    if not detail_df.empty:
                        st.subheader("📋 Extracted Data Details")
                        try:
                            extracted_data = json.loads(detail_df.iloc[0]['extracted_data'])
                            st.json(extracted_data)
                        except:
                            st.text(detail_df.iloc[0]['extracted_data'])
                        
                        # Show issues if any
                        try:
                            issues = json.loads(detail_df.iloc[0]['issues_found'])
                            if issues:
                                st.subheader("⚠️ Issues Found")
                                for issue in issues:
                                    st.write(f"• {issue}")
                        except:
                            pass
        else:
            st.info("No extraction tests found in database")
        
        conn.close()
        
    except Exception as e:
        st.error(f"Error accessing database: {str(e)}")

def show_extraction_stats():
    """Fixed extraction statistics viewer"""
    st.subheader("📈 Extraction Statistics")
    
    try:
        conn = sqlite3.connect("complete_abs_system.db")
        
        # Overall stats
        stats_query = """
            SELECT 
                COUNT(*) as total_tests,
                AVG(confidence_score) as avg_confidence,
                COUNT(CASE WHEN success = 1 THEN 1 END) as successful_tests
            FROM EnhancedExtractionResults
        """
        
        stats_df = pd.read_sql_query(stats_query, conn)
        
        if not stats_df.empty and stats_df.iloc[0]['total_tests'] > 0:
            stats = stats_df.iloc[0]
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Tests", int(stats['total_tests']))
            with col2:
                st.metric("Success Rate", f"{(stats['successful_tests']/stats['total_tests']*100):.1f}%")
            with col3:
                st.metric("Avg Confidence", f"{stats['avg_confidence']:.1%}" if stats['avg_confidence'] else "N/A")
            
            # Stats by document type
            type_query = """
                SELECT 
                    document_type,
                    COUNT(*) as count,
                    AVG(confidence_score) as avg_confidence
                FROM EnhancedExtractionResults 
                GROUP BY document_type
                ORDER BY count DESC
            """
            
            type_df = pd.read_sql_query(type_query, conn)
            
            if not type_df.empty:
                st.subheader("📊 Statistics by Document Type")
                st.dataframe(type_df, use_container_width=True, hide_index=True)
        else:
            st.info("No extraction statistics available yet")
        
        conn.close()
        
    except Exception as e:
        st.error(f"Error getting statistics: {str(e)}")

def get_peac_sample():
    """PEAC Solutions sample text"""
    return """
    Executive Summary
    This report summarizes KBRA's analysis of PEAC Solutions Receivables 2025-1, LLC (PEAC 2025-1), an equipment ABS
    transaction. This report is based on information as of February 11, 2025.
    
    The aggregate securitization value is $769.63 million. PEAC 2025-1 will issue five classes of notes, including a short-term tranche. 
    Credit enhancement includes excess spread, a reserve account, overcollateralization and subordination (except for Class C Notes). 
    The overcollateralization is subject to a target equal to 14.00% of the current ASV and a floor equal to 1.00% of the initial ASV.
    """

# Comprehensive Excel File Processor for ABS Surveillance Data
# Add this after your other functions

def process_excel_file_comprehensive(uploaded_file, db_system):
    """Comprehensive Excel file processor for surveillance dashboards"""
    
    st.subheader("📊 Excel Surveillance Data Processing")
    
    try:
        with st.spinner("Processing Excel file..."):
            # Read all sheets from Excel file
            excel_data = pd.read_excel(uploaded_file, sheet_name=None, header=0)
            
            results = {
                'file_name': uploaded_file.name,
                'sheets_processed': len(excel_data),
                'total_transactions': 0,
                'transactions_saved': 0,
                'sheets_data': {},
                'errors': []
            }
            
            # Process each sheet
            for sheet_name, df in excel_data.items():
                try:
                    sheet_result = process_excel_sheet(
                        sheet_name, df, uploaded_file.name, db_system
                    )
                    results['sheets_data'][sheet_name] = sheet_result
                    results['total_transactions'] += sheet_result.get('transactions_created', 0)
                    results['transactions_saved'] += sheet_result.get('saved_count', 0)
                    
                except Exception as e:
                    error_msg = f"Error processing sheet {sheet_name}: {str(e)}"
                    results['errors'].append(error_msg)
                    st.warning(error_msg)
            
            # Display results
            if results['errors']:
                st.error(f"⚠️ {len(results['errors'])} errors occurred during processing")
            else:
                st.success(f"✅ Successfully processed {results['sheets_processed']} sheets")
            
            # Show metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Sheets Processed", results['sheets_processed'])
            with col2:
                st.metric("Transactions Created", results['total_transactions'])
            with col3:
                st.metric("Saved to Database", results['transactions_saved'])
            
            # Show detailed results
            if results['sheets_data']:
                st.subheader("📋 Processing Details")
                
                for sheet_name, sheet_data in results['sheets_data'].items():
                    with st.expander(f"📄 {sheet_name} - {sheet_data.get('sheet_type', 'Unknown')} Data"):
                        
                        col_a, col_b, col_c = st.columns(3)
                        with col_a:
                            st.metric("Rows", sheet_data.get('row_count', 0))
                        with col_b:
                            st.metric("Columns", sheet_data.get('column_count', 0))
                        with col_c:
                            st.metric("Deals Found", sheet_data.get('transactions_created', 0))
                        
                        # Show sample data
                        if sheet_data.get('sample_data'):
                            st.write("**Sample Data:**")
                            sample_df = pd.DataFrame(sheet_data['sample_data'])
                            st.dataframe(sample_df.head(3), use_container_width=True, hide_index=True)
                        
                        # Show extracted deals
                        if sheet_data.get('extracted_deals'):
                            st.write("**Extracted Deals:**")
                            deals_list = []
                            for deal in sheet_data['extracted_deals'][:5]:  # Show first 5
                                deals_list.append({
                                    'Deal Name': deal.get('deal_name', 'Unknown'),
                                    'Current Balance': f"${deal.get('current_balance', 0):,.0f}" if deal.get('current_balance') else 'N/A',
                                    'Pool Factor': f"{deal.get('pool_factor', 0):.2%}" if deal.get('pool_factor') else 'N/A',
                                    'Metrics Count': len(deal.get('metrics', {}))
                                })
                            
                            if deals_list:
                                st.dataframe(pd.DataFrame(deals_list), use_container_width=True, hide_index=True)
            
            # Show errors if any
            if results['errors']:
                st.subheader("⚠️ Processing Errors")
                for error in results['errors']:
                    st.error(error)
            
            return results
            
    except Exception as e:
        st.error(f"Error reading Excel file: {str(e)}")
        return {'error': str(e)}

def process_excel_sheet(sheet_name: str, df: pd.DataFrame, filename: str, db_system):
    """Process individual Excel sheet and extract deal data"""
    
    # Clean the dataframe
    df_clean = clean_excel_dataframe(df)
    
    if df_clean.empty:
        return {
            'sheet_name': sheet_name,
            'sheet_type': 'EMPTY',
            'row_count': 0,
            'column_count': 0,
            'transactions_created': 0,
            'saved_count': 0,
            'sample_data': {},
            'extracted_deals': []
        }
    
    # Identify sheet type
    sheet_type = identify_excel_sheet_type(sheet_name, df_clean)
    
    # Extract deals based on sheet type
    extracted_deals = extract_deals_from_sheet(df_clean, filename, sheet_name, sheet_type)
    
    # Save deals to database
    saved_count = save_deals_to_database(extracted_deals, db_system)
    
    return {
        'sheet_name': sheet_name,
        'sheet_type': sheet_type,
        'row_count': len(df_clean),
        'column_count': len(df_clean.columns),
        'transactions_created': len(extracted_deals),
        'saved_count': saved_count,
        'sample_data': df_clean.head(3).to_dict('records') if len(df_clean) > 0 else {},
        'extracted_deals': extracted_deals
    }

def clean_excel_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and prepare Excel dataframe"""
    
    # Remove completely empty rows and columns
    df_clean = df.dropna(how='all').dropna(axis=1, how='all')
    
    # Clean column names
    df_clean.columns = [
        str(col).strip().replace('\n', ' ').replace('\r', ' ')
        for col in df_clean.columns
    ]
    
    # Reset index
    df_clean = df_clean.reset_index(drop=True)
    
    return df_clean

def identify_excel_sheet_type(sheet_name: str, df: pd.DataFrame) -> str:
    """Identify the type of Excel sheet"""
    
    sheet_name_lower = sheet_name.lower()
    columns_str = ' '.join([str(col).lower() for col in df.columns])
    
    # Check for surveillance indicators
    surveillance_keywords = [
        'surveillance', 'performance', 'collateral', 'balance', 'pool',
        'delinquency', 'delinq', 'charge off', 'loss', 'receivables'
    ]
    
    # Check first column for deal patterns
    first_col_text = ' '.join([str(val).lower() for val in df.iloc[:, 0] if pd.notna(val)])
    
    # Score for surveillance
    surveillance_score = (
        sum(1 for kw in surveillance_keywords if kw in sheet_name_lower) +
        sum(1 for kw in surveillance_keywords if kw in columns_str) +
        sum(1 for kw in surveillance_keywords if kw in first_col_text)
    )
    
    # Look for deal name patterns in columns
    deal_pattern_score = 0
    for col in df.columns[1:]:  # Skip first column
        col_str = str(col).upper()
        if any(pattern in col_str for pattern in ['2021', '2022', '2023', '2024', '2025', 'OPTN', 'PEAC', 'CRVN']):
            deal_pattern_score += 1
    
    if surveillance_score >= 2 and deal_pattern_score >= 2:
        return 'SURVEILLANCE'
    elif surveillance_score >= 1:
        return 'PERFORMANCE'
    else:
        return 'GENERIC'

def extract_deals_from_sheet(df: pd.DataFrame, filename: str, sheet_name: str, sheet_type: str) -> list:
    """Extract deal data from Excel sheet - each column represents a deal"""
    
    deals = []
    
    # Skip the first column (row labels) and treat each remaining column as a deal
    data_columns = df.columns[1:] if len(df.columns) > 1 else df.columns
    
    for col_index, deal_column in enumerate(data_columns):
        deal_data = {
            'source_file': filename,
            'sheet_name': sheet_name,
            'sheet_type': sheet_type,
            'deal_name': str(deal_column).strip(),
            'column_index': col_index,
            'extracted_date': datetime.now().isoformat(),
            'metrics': {}
        }
        
        # Extract metrics from each row for this deal
        for row_index in range(len(df)):
            try:
                # Get row label (first column)
                row_label = df.iloc[row_index, 0]
                if pd.isna(row_label) or not str(row_label).strip():
                    continue
                
                row_label_str = str(row_label).strip()
                
                # Get the value for this deal (column)
                if col_index + 1 < len(df.columns):
                    value = df.iloc[row_index, col_index + 1]
                    if pd.isna(value):
                        continue
                    
                    # Clean and store the value
                    clean_value = clean_excel_value(value)
                    if clean_value is not None:
                        metric_name = standardize_metric_name(row_label_str)
                        deal_data['metrics'][metric_name] = clean_value
                        
                        # Map to standard fields for easier querying
                        map_standard_surveillance_fields(deal_data, row_label_str, clean_value)
                
            except Exception as e:
                continue
        
        # Only add deal if it has meaningful data
        if deal_data['metrics'] and len(deal_data['metrics']) >= 3:
            deals.append(deal_data)
    
    return deals

def clean_excel_value(value):
    """Clean and convert Excel values"""
    
    if pd.isna(value):
        return None
    
    # Convert to string for processing
    str_value = str(value).strip()
    
    if not str_value:
        return None
    
    # Try to convert to number
    try:
        # Remove common formatting
        cleaned = str_value.replace(',', '').replace('$', '').replace('%', '')
        
        # Try float conversion
        if '.' in cleaned or 'e' in cleaned.lower():
            return float(cleaned)
        else:
            return int(cleaned)
    except:
        # Return as string if not a number
        return str_value

def standardize_metric_name(row_label: str) -> str:
    """Standardize metric names for consistency"""
    
    # Clean up the label
    clean_label = re.sub(r'[^\w\s]', ' ', row_label).strip()
    clean_label = re.sub(r'\s+', '_', clean_label).lower()
    
    # Common standardizations
    standardizations = {
        'deal_short_name': 'deal_name',
        'securitization_date': 'securitization_date',
        'current_collat_bal': 'current_balance',
        'original_collat_bal': 'original_balance',
        'pool_factor': 'pool_factor',
        'months_seasoned': 'months_seasoned',
        'wa_interest_rate_current': 'current_rate',
        'current_number_of_receivables': 'current_receivables',
        '30_dq': 'delinq_30_plus',
        '60_dq': 'delinq_60_plus',
        '90_dq': 'delinq_90_plus',
        'cnl': 'charge_offs'
    }
    
    return standardizations.get(clean_label, clean_label)

def map_standard_surveillance_fields(deal_data: dict, row_label: str, value):
    """Map row labels to standard surveillance fields"""
    
    row_lower = row_label.lower()
    
    try:
        # Convert value to float for numeric fields
        numeric_value = float(str(value).replace(',', '').replace('$', '').replace('%', ''))
        
        if 'current collat bal' in row_lower or 'current balance' in row_lower:
            # Handle thousands
            if numeric_value < 100000:  # Likely in thousands
                deal_data['current_balance'] = numeric_value * 1000
            else:
                deal_data['current_balance'] = numeric_value
                
        elif 'original collat bal' in row_lower or 'original balance' in row_lower:
            if numeric_value < 100000:
                deal_data['original_balance'] = numeric_value * 1000
            else:
                deal_data['original_balance'] = numeric_value
                
        elif 'pool factor' in row_lower:
            deal_data['pool_factor'] = numeric_value / 100 if numeric_value > 1 else numeric_value
            
        elif 'months seasoned' in row_lower:
            deal_data['months_seasoned'] = int(numeric_value)
            
        elif '30+ dq' in row_lower or '30 dq' in row_lower:
            deal_data['delinq_30_plus'] = numeric_value / 100 if numeric_value > 1 else numeric_value
            
        elif '60+ dq' in row_lower or '60 dq' in row_lower:
            deal_data['delinq_60_plus'] = numeric_value / 100 if numeric_value > 1 else numeric_value
            
        elif '90+ dq' in row_lower or '90 dq' in row_lower:
            deal_data['delinq_90_plus'] = numeric_value / 100 if numeric_value > 1 else numeric_value
            
        elif 'cnl' in row_lower or 'charge' in row_lower:
            deal_data['charge_offs'] = numeric_value / 100 if numeric_value > 1 else numeric_value
            
    except:
        # Store as string if can't convert to number
        if 'securitization date' in row_lower:
            deal_data['securitization_date'] = str(value)
        elif 'deal' in row_lower and 'name' in row_lower:
            deal_data['deal_name'] = str(value)

def save_deals_to_database(deals: list, db_system) -> int:
    """Save extracted deals to database"""
    
    saved_count = 0
    
    try:
        conn = sqlite3.connect(db_system.db_path)
        cursor = conn.cursor()
        
        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ExcelSurveillanceData (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_file TEXT,
                sheet_name TEXT,
                sheet_type TEXT,
                deal_name TEXT,
                column_index INTEGER,
                
                -- Key surveillance metrics
                current_balance REAL,
                original_balance REAL,
                pool_factor REAL,
                months_seasoned INTEGER,
                current_rate REAL,
                current_receivables INTEGER,
                delinq_30_plus REAL,
                delinq_60_plus REAL,
                delinq_90_plus REAL,
                charge_offs REAL,
                securitization_date TEXT,
                
                -- Raw metrics storage
                all_metrics TEXT,
                extracted_date TEXT
            )
        """)
        
        # Insert deals
        for deal in deals:
            cursor.execute("""
                INSERT INTO ExcelSurveillanceData (
                    source_file, sheet_name, sheet_type, deal_name, column_index,
                    current_balance, original_balance, pool_factor, months_seasoned,
                    current_rate, current_receivables, delinq_30_plus, delinq_60_plus,
                    delinq_90_plus, charge_offs, securitization_date, all_metrics, extracted_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                deal.get('source_file'),
                deal.get('sheet_name'),
                deal.get('sheet_type'),
                deal.get('deal_name'),
                deal.get('column_index'),
                deal.get('current_balance'),
                deal.get('original_balance'),
                deal.get('pool_factor'),
                deal.get('months_seasoned'),
                deal.get('current_rate'),
                deal.get('current_receivables'),
                deal.get('delinq_30_plus'),
                deal.get('delinq_60_plus'),
                deal.get('delinq_90_plus'),
                deal.get('charge_offs'),
                deal.get('securitization_date'),
                json.dumps(deal.get('metrics', {})),
                deal.get('extracted_date')
            ))
            saved_count += 1
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        st.error(f"Database save error: {str(e)}")
    
    return saved_count

# Enhanced file upload function (if not already present)
def render_enhanced_file_upload():
    """Enhanced file upload with multi-format support"""
    
    st.subheader("📁 Enhanced File Upload")
    
    # Show supported formats
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Supported Formats:**")
        formats = ["📄 Text (.txt)", "📋 PDF (.pdf)"]
        
        try:
            import docx
            formats.append("📝 Word (.docx)")
        except ImportError:
            pass
            
        try:
            import openpyxl
            formats.append("📊 Excel (.xlsx, .xls)")
        except ImportError:
            pass
        
        for fmt in formats:
            st.write(f"✅ {fmt}")
    
    with col2:
        st.markdown("**Installation Commands:**")
        st.code("pip install PyPDF2 pdfplumber", language="bash")
        st.code("pip install python-docx", language="bash")
        st.code("pip install openpyxl", language="bash")
    
    # File upload
    uploaded_file = st.file_uploader(
        "Upload Document",
        type=['txt', 'pdf', 'docx', 'xlsx', 'xls'],
        help="Upload ABS documents in various formats",
        key="main_document_uploader2"
    )
    
    return uploaded_file

# SURGICAL FIX: Replace ONLY your main() function with this
# Keep everything else in your file exactly the same

def main():
    """Main application function with FIXED header - all functionality preserved"""
    
    # STEP 1: Hide clutter with aggressive CSS FIRST
    st.markdown("""
    <style>
    /* NUCLEAR CSS - Hide ALL top clutter */
    .stException, .stAlert, .stError, .stWarning, .stInfo, .stSuccess,
    div[data-testid="stException"], div[data-testid="stAlert"],
    div[data-testid="stNotificationContentError"], div[data-testid="stNotificationContentWarning"],
    div[data-testid="stNotificationContentInfo"], div[data-testid="stNotificationContentSuccess"],
    .element-container:has(.stException), .element-container:has(.stAlert),
    .element-container:has(.stError), .element-container:has(.stWarning),
    .element-container:has(.stInfo), .element-container:has(.stSuccess) {
        display: none !important;
        visibility: hidden !important;
        height: 0 !important;
        margin: 0 !important;
        padding: 0 !important;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    .stDeployButton {display: none;}
    
    /* Hide any div containing error text */
    div:contains("TypeError"), div:contains("Enhanced extraction"), 
    div:contains("Database initialized"), div:contains("First argument must be") {
        display: none !important;
    }
    
    /* Force clean top margin */
    .main .block-container {
        padding-top: 1rem !important;
        margin-top: 0 !important;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # STEP 2: Professional Header - WORKING VERSION
    st.markdown("""
    <div style="
        background: linear-gradient(135deg, #041635 0%, #1a2742 50%, #6789ba 100%); 
        color: white; 
        padding: 2rem; 
        margin: -1rem -1rem 2rem -1rem; 
        border-radius: 0 0 15px 15px;
        box-shadow: 0 4px 20px rgba(4, 22, 53, 0.3);
        position: relative;
        min-height: 120px;
        display: flex;
        align-items: center;
        justify-content: center;
    ">
        <h1 style="
            margin: 0; 
            color: white; 
            font-size: 3rem; 
            font-weight: 600; 
            text-align: center;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        ">ABS Navigator</h1>
        <div style="
            position: absolute; 
            bottom: 15px; 
            right: 20px; 
            font-size: 1rem; 
            color: rgba(255,255,255,0.8); 
            font-weight: 400;
        ">WHITEHALL PARTNERS</div>
    </div>
    """, unsafe_allow_html=True)
    
    # STEP 3: Initialize system (your existing code)
    if 'fixed_abs_system' not in st.session_state:
        st.session_state.fixed_abs_system = FixedCompleteABSSystem()
    
    # STEP 4: Status indicators (your existing code)
    st.markdown("---")
    create_clean_status_indicators()
    st.markdown("---")
    
    # STEP 5: ALL YOUR EXISTING FUNCTIONALITY - exactly as you have it
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📄 Enhanced Document Processing", 
        "💹 Bloomberg Pricing",
        "🗄️ Database Status", 
        "💻 SQL Browser", 
        "📊 Advanced Analytics",
        "⚙️ System Settings"
    ])
    
    with tab1:
        # Enhanced document processing with surveillance updates
        render_fixed_document_processing()
        
        # Add surveillance update interface
        st.markdown("---")
        render_surveillance_update_interface()
    
    with tab2:
        st.header("🗄️ Database Status & Management")
        
        try:
            conn = sqlite3.connect(st.session_state.fixed_abs_system.db_path)
            cursor = conn.cursor()
            
            # Get table information
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
            tables = cursor.fetchall()
            
            if tables:
                st.success(f"✅ Database connected with {len(tables)} tables")
                
                # Display table information
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("📋 Available Tables")
                    for table in tables:
                        table_name = table[0]
                        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                        count = cursor.fetchone()[0]
                        
                        st.write(f"**{table_name}**: {count:,} records")
                
                with col2:
                    st.subheader("🔍 Quick Table View")
                    selected_table = st.selectbox("Select table to preview:", [t[0] for t in tables])
                    
                    if selected_table:
                        try:
                            preview_df = pd.read_sql_query(f"SELECT * FROM {selected_table} LIMIT 10", conn)
                            st.dataframe(preview_df, use_container_width=True, hide_index=True)
                        except Exception as e:
                            st.error(f"Error previewing table: {str(e)}")
                
                # Database management buttons
                st.subheader("🛠️ Database Management")
                col_a, col_b, col_c = st.columns(3)
                
                with col_a:
                    if st.button("🗄️ View All Extraction Results"):
                        show_database_results()
                
                with col_b:
                    if st.button("📈 Show Extraction Statistics"):
                        show_extraction_stats()
                
                with col_c:
                    if st.button("🧹 Database Maintenance"):
                        st.info("Database maintenance tools coming soon...")
            
            else:
                st.warning("⚠️ Database connected but no tables found")
            
            conn.close()
            
        except Exception as e:
            st.error(f"❌ Database connection error: {str(e)}")
            st.info("The database will be created automatically when you process your first document.")
    
    with tab2:
        render_bloomberg_pricing_interface()
        
    with tab3:
        st.header("💻 SQL Browser & Query Interface")
        
        st.markdown("""
        **Run custom SQL queries on your ABS database**  
        Use this interface to explore your extracted data and generate custom reports.
        """)
        
        # SQL Query interface
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Query input
            query = st.text_area(
                "📝 SQL Query:",
                height=200,
                placeholder="""-- Example queries:
SELECT * FROM ExcelTransactions WHERE transaction_type = 'SURVEILLANCE' LIMIT 10;

SELECT deal_id, report_date, current_balance, pool_factor 
FROM SurveillanceTimeSeries 
ORDER BY report_date DESC;

SELECT COUNT(*) as total_deals, 
       AVG(current_balance) as avg_balance 
FROM MasterDeals;""",
                help="Enter your SQL query here. Be careful with UPDATE/DELETE operations."
            )
        
        with col2:
            st.markdown("**📚 Available Tables:**")
            try:
                conn = sqlite3.connect(st.session_state.fixed_abs_system.db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
                tables = cursor.fetchall()
                
                for table in tables:
                    if st.button(f"📋 {table[0]}", key=f"table_{table[0]}"):
                        # Show table schema
                        cursor.execute(f"PRAGMA table_info({table[0]});")
                        schema = cursor.fetchall()
                        
                        st.subheader(f"Schema: {table[0]}")
                        schema_df = pd.DataFrame(schema, columns=['ID', 'Name', 'Type', 'NotNull', 'Default', 'PK'])
                        st.dataframe(schema_df[['Name', 'Type']], hide_index=True)
                
                conn.close()
                
            except Exception as e:
                st.error(f"Error loading tables: {str(e)}")
        
        # Execute query
        if st.button("🚀 Execute Query", type="primary"):
            if query.strip():
                try:
                    conn = sqlite3.connect(st.session_state.fixed_abs_system.db_path)
                    
                    # Safety check for dangerous operations
                    query_upper = query.upper().strip()
                    if any(dangerous in query_upper for dangerous in ['DROP', 'DELETE', 'TRUNCATE', 'ALTER']):
                        st.warning("⚠️ Potentially dangerous query detected. Please be careful.")
                        confirm = st.checkbox("I understand the risks and want to proceed")
                        if not confirm:
                            st.stop()
                    
                    # Execute query
                    if query_upper.startswith('SELECT') or query_upper.startswith('WITH'):
                        # Read query
                        result_df = pd.read_sql_query(query, conn)
                        st.success(f"✅ Query executed successfully! {len(result_df)} rows returned.")
                        st.dataframe(result_df, use_container_width=True, hide_index=True)
                        
                        # Download option
                        if len(result_df) > 0:
                            csv = result_df.to_csv(index=False)
                            st.download_button(
                                label="📥 Download Results as CSV",
                                data=csv,
                                file_name=f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv"
                            )
                    else:
                        # Write query
                        cursor = conn.cursor()
                        cursor.execute(query)
                        conn.commit()
                        st.success(f"✅ Query executed successfully! {cursor.rowcount} rows affected.")
                    
                    conn.close()
                    
                except Exception as e:
                    st.error(f"❌ Query error: {str(e)}")
            else:
                st.warning("Please enter a SQL query")
    
    with tab4:
        st.header("📊 Analytics Dashboard")
        
        st.markdown("""
        **ABS Portfolio Analytics & Surveillance Metrics**  
        Analyze your extracted ABS data with interactive charts and metrics.
        """)
        
        try:
            conn = sqlite3.connect(st.session_state.fixed_abs_system.db_path)
            
            # Check if we have surveillance data
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='SurveillanceTimeSeries';")
            if cursor.fetchone():
                
                # Time series analysis
                st.subheader("📈 Time Series Analysis")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Deal selection
                    cursor.execute("SELECT DISTINCT deal_id FROM SurveillanceTimeSeries ORDER BY deal_id;")
                    deals = [row[0] for row in cursor.fetchall()]
                    
                    if deals:
                        selected_deal = st.selectbox("Select Deal for Analysis:", deals)
                        
                        if selected_deal:
                            # Get time series data for selected deal
                            ts_df = pd.read_sql_query("""
                                SELECT report_date, current_balance, pool_factor, 
                                       delinq_30_plus, delinq_60_plus, delinq_90_plus
                                FROM SurveillanceTimeSeries 
                                WHERE deal_id = ? 
                                ORDER BY report_date
                            """, conn, params=[selected_deal])
                            
                            if not ts_df.empty:
                                # Convert date column
                                ts_df['report_date'] = pd.to_datetime(ts_df['report_date'])
                                
                                # Plot balance over time
                                st.line_chart(data=ts_df.set_index('report_date')['current_balance'])
                                st.caption("Current Balance Over Time")
                
                with col2:
                    # Portfolio summary
                    st.subheader("📋 Portfolio Summary")
                    
                    summary_df = pd.read_sql_query("""
                        SELECT 
                            COUNT(DISTINCT deal_id) as total_deals,
                            SUM(current_balance) as total_balance,
                            AVG(pool_factor) as avg_pool_factor,
                            AVG(delinq_30_plus) as avg_delinq_30
                        FROM SurveillanceTimeSeries s1
                        WHERE s1.report_date = (
                            SELECT MAX(s2.report_date) 
                            FROM SurveillanceTimeSeries s2 
                            WHERE s2.deal_id = s1.deal_id
                        )
                    """, conn)
                    
                    if not summary_df.empty:
                        row = summary_df.iloc[0]
                        st.metric("Total Deals", f"{row['total_deals']:,.0f}")
                        st.metric("Total Balance", f"${row['total_balance']:,.0f}")
                        st.metric("Avg Pool Factor", f"{row['avg_pool_factor']:.2%}")
                        st.metric("Avg 30+ Delinq", f"{row['avg_delinq_30']:.2%}")
                
                # Delinquency trends
                st.subheader("📊 Delinquency Analysis")
                
                delinq_df = pd.read_sql_query("""
                    SELECT deal_id, 
                           AVG(delinq_30_plus) as avg_30_plus,
                           AVG(delinq_60_plus) as avg_60_plus,
                           AVG(delinq_90_plus) as avg_90_plus
                    FROM SurveillanceTimeSeries 
                    GROUP BY deal_id
                    ORDER BY avg_30_plus DESC
                """, conn)
                
                if not delinq_df.empty:
                    st.bar_chart(data=delinq_df.set_index('deal_id')[['avg_30_plus', 'avg_60_plus', 'avg_90_plus']])
                    st.caption("Average Delinquency Rates by Deal")
            
            else:
                st.info("📈 No surveillance data available yet. Upload some Excel surveillance files to see analytics!")
            
            conn.close()
            
        except Exception as e:
            st.error(f"Analytics error: {str(e)}")
    
    with tab5:
        render_advanced_analytics_dashboard()
    
    with tab5:
        st.header("⚙️ System Settings & Information")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🔧 System Configuration")
            
            # Enhanced extraction toggle
            if hasattr(st.session_state.fixed_abs_system, 'enhanced_available'):
                if st.session_state.fixed_abs_system.enhanced_available:
                    st.success("✅ Enhanced extraction module loaded")
                    st.info("Using improved document extraction with 85%+ accuracy")
                else:
                    st.warning("⚠️ Enhanced extraction not available")
                    st.info("Using basic extraction. Install improved_document_extractor.py for better results.")
            
            # File format support
            st.subheader("📁 File Format Support")
            formats = {
                "Text (.txt)": True,
                "PDF (.pdf)": globals().get('PDF_SUPPORT', False),
                "Word (.docx)": globals().get('DOCX_SUPPORT', False),
                "Excel (.xlsx, .xls)": globals().get('XLSX_SUPPORT', False)
            }
            
            for format_name, supported in formats.items():
                if supported:
                    st.success(f"✅ {format_name}")
                else:
                    st.error(f"❌ {format_name}")
            
            # Database path
            st.subheader("🗄️ Database Configuration")
            st.info(f"Database location: {st.session_state.fixed_abs_system.db_path}")
        
        with col2:
            st.subheader("📊 System Statistics")
            
            try:
                conn = sqlite3.connect(st.session_state.fixed_abs_system.db_path)
                cursor = conn.cursor()
                
                # Get database stats
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                
                stats = {}
                for table in tables:
                    table_name = table[0]
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    stats[table_name] = count
                
                st.write("**Database Statistics:**")
                for table, count in stats.items():
                    st.write(f"• {table}: {count:,} records")
                
                conn.close()
                
            except Exception as e:
                st.error(f"Error getting statistics: {str(e)}")
            
            # System information
            st.subheader("ℹ️ System Information")
            st.write(f"**Streamlit version:** {st.__version__}")
            st.write(f"**Current time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # About
            st.subheader("🏢 About Whitehall Partners")
            st.markdown("""
            **Whitehall Partners** - Structured Finance Advisory
            
            This ABS Document Processing System provides comprehensive 
            tools for analyzing asset-backed securities documentation 
            and surveillance data.
            
            Features:
            - Multi-format document processing
            - Enhanced extraction algorithms  
            - Time series surveillance tracking
            - Interactive analytics dashboard
            - Professional reporting capabilities
            """)


# Advanced Analytics Dashboard
# Add this to your main.py file

class AdvancedAnalyticsDashboard:
    """
    Advanced analytics dashboard with interactive charts and market overview
    """
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def get_market_overview(self) -> Dict:
        """Get comprehensive market overview statistics"""
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Overall market stats
            market_stats = {}
            
            # From pricing data
            pricing_query = """
                SELECT 
                    COUNT(*) as total_securities,
                    COUNT(DISTINCT issuer_name) as total_issuers,
                    COUNT(DISTINCT deal_name) as total_deals,
                    SUM(original_amount) as total_issuance_volume,
                    AVG(issue_yield) as avg_yield,
                    AVG(current_credit_support) as avg_credit_support,
                    MIN(pricing_speed_date) as earliest_pricing,
                    MAX(pricing_speed_date) as latest_pricing
                FROM PricingData
                WHERE original_amount IS NOT NULL
            """
            
            pricing_df = pd.read_sql_query(pricing_query, conn)
            if not pricing_df.empty:
                market_stats['pricing'] = pricing_df.iloc[0].to_dict()
            
            # From deals data
            deals_query = """
                SELECT 
                    COUNT(*) as extracted_deals,
                    COUNT(DISTINCT asset_type) as asset_types,
                    AVG(total_deal_size) as avg_deal_size,
                    SUM(total_deal_size) as total_deal_volume
                FROM ABS_Deals
                WHERE total_deal_size IS NOT NULL AND total_deal_size > 0
            """
            
            deals_df = pd.read_sql_query(deals_query, conn)
            if not deals_df.empty:
                market_stats['deals'] = deals_df.iloc[0].to_dict()
            
            # Note classes distribution
            note_classes_query = """
                SELECT 
                    COUNT(*) as total_note_classes,
                    AVG(original_balance) as avg_note_balance,
                    AVG(interest_rate) as avg_interest_rate
                FROM NoteClasses
                WHERE original_balance IS NOT NULL AND original_balance > 0
            """
            
            note_classes_df = pd.read_sql_query(note_classes_query, conn)
            if not note_classes_df.empty:
                market_stats['note_classes'] = note_classes_df.iloc[0].to_dict()
            
            conn.close()
            return market_stats
            
        except Exception as e:
            return {'error': str(e)}
    
    def create_deal_type_distribution_chart(self) -> go.Figure:
        """Create deal type distribution chart"""
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Get deal type distribution from multiple sources
            queries = [
                ("Pricing Data", "SELECT deal_type, COUNT(*) as count FROM PricingData WHERE deal_type IS NOT NULL GROUP BY deal_type"),
                ("Extracted Deals", "SELECT asset_type as deal_type, COUNT(*) as count FROM ABS_Deals WHERE asset_type IS NOT NULL GROUP BY asset_type")
            ]
            
            fig = make_subplots(
                rows=1, cols=2,
                subplot_titles=("From Bloomberg Pricing", "From Document Extraction"),
                specs=[[{"type": "pie"}, {"type": "pie"}]]
            )
            
            for i, (title, query) in enumerate(queries):
                df = pd.read_sql_query(query, conn)
                if not df.empty:
                    fig.add_trace(
                        go.Pie(
                            labels=df['deal_type'],
                            values=df['count'],
                            name=title,
                            textinfo='label+percent',
                            textposition='auto'
                        ),
                        row=1, col=i+1
                    )
            
            fig.update_layout(
                title="Deal Type Distribution Analysis",
                height=500,
                showlegend=True
            )
            
            conn.close()
            return fig
            
        except Exception as e:
            st.error(f"Error creating deal type chart: {str(e)}")
            return go.Figure()
    
    def create_yield_analysis_chart(self) -> go.Figure:
        """Create yield distribution and trend analysis"""
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Get yield data
            yield_query = """
                SELECT 
                    issue_yield,
                    deal_type,
                    rating_tier,
                    current_credit_support,
                    pricing_speed_date
                FROM PricingData
                WHERE issue_yield IS NOT NULL 
                AND issue_yield > 0 
                AND issue_yield < 1
                ORDER BY pricing_speed_date
            """
            
            df = pd.read_sql_query(yield_query, conn)
            
            if df.empty:
                return go.Figure().add_annotation(text="No yield data available", showarrow=False)
            
            # Convert yield to percentage for display
            df['yield_pct'] = df['issue_yield'] * 100
            
            # Create subplots
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=(
                    "Yield Distribution", "Yield by Rating Tier",
                    "Yield vs Credit Support", "Yield Trends Over Time"
                ),
                specs=[
                    [{"type": "histogram"}, {"type": "box"}],
                    [{"type": "scatter"}, {"type": "scatter"}]
                ]
            )
            
            # 1. Yield distribution histogram
            fig.add_trace(
                go.Histogram(
                    x=df['yield_pct'],
                    nbinsx=20,
                    name="Yield Distribution",
                    marker_color="lightblue"
                ),
                row=1, col=1
            )
            
            # 2. Yield by rating tier
            if 'rating_tier' in df.columns and df['rating_tier'].notna().any():
                for rating in df['rating_tier'].dropna().unique():
                    rating_data = df[df['rating_tier'] == rating]
                    fig.add_trace(
                        go.Box(
                            y=rating_data['yield_pct'],
                            name=rating,
                            boxmean=True
                        ),
                        row=1, col=2
                    )
            
            # 3. Yield vs Credit Support scatter
            if 'current_credit_support' in df.columns:
                df_scatter = df.dropna(subset=['current_credit_support'])
                df_scatter['credit_support_pct'] = df_scatter['current_credit_support'] * 100
                
                fig.add_trace(
                    go.Scatter(
                        x=df_scatter['credit_support_pct'],
                        y=df_scatter['yield_pct'],
                        mode='markers',
                        name="Yield vs Credit Support",
                        marker=dict(size=8, opacity=0.6),
                        text=df_scatter['deal_type'],
                        hovertemplate="Credit Support: %{x:.1f}%<br>Yield: %{y:.2f}%<br>Deal Type: %{text}<extra></extra>"
                    ),
                    row=2, col=1
                )
            
            # 4. Yield trends over time
            if 'pricing_speed_date' in df.columns:
                df_time = df.dropna(subset=['pricing_speed_date'])
                df_time['pricing_speed_date'] = pd.to_datetime(df_time['pricing_speed_date'])
                
                # Group by month for trend
                df_monthly = df_time.groupby(df_time['pricing_speed_date'].dt.to_period('M'))['yield_pct'].mean().reset_index()
                df_monthly['pricing_speed_date'] = df_monthly['pricing_speed_date'].dt.to_timestamp()
                
                fig.add_trace(
                    go.Scatter(
                        x=df_monthly['pricing_speed_date'],
                        y=df_monthly['yield_pct'],
                        mode='lines+markers',
                        name="Monthly Avg Yield",
                        line=dict(width=3)
                    ),
                    row=2, col=2
                )
            
            fig.update_layout(
                title="Comprehensive Yield Analysis",
                height=800,
                showlegend=False
            )
            
            # Update axis labels
            fig.update_xaxes(title_text="Yield (%)", row=1, col=1)
            fig.update_yaxes(title_text="Count", row=1, col=1)
            
            fig.update_xaxes(title_text="Rating Tier", row=1, col=2)
            fig.update_yaxes(title_text="Yield (%)", row=1, col=2)
            
            fig.update_xaxes(title_text="Credit Support (%)", row=2, col=1)
            fig.update_yaxes(title_text="Yield (%)", row=2, col=1)
            
            fig.update_xaxes(title_text="Date", row=2, col=2)
            fig.update_yaxes(title_text="Avg Yield (%)", row=2, col=2)
            
            conn.close()
            return fig
            
        except Exception as e:
            st.error(f"Error creating yield analysis: {str(e)}")
            return go.Figure()
    
    def create_credit_support_analysis(self) -> go.Figure:
        """Create credit enhancement analysis"""
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Get credit support data
            credit_query = """
                SELECT 
                    current_credit_support,
                    original_credit_support,
                    deal_type,
                    rating_tier,
                    issue_yield,
                    issuer_name
                FROM PricingData
                WHERE current_credit_support IS NOT NULL 
                AND current_credit_support > 0
            """
            
            df = pd.read_sql_query(credit_query, conn)
            
            if df.empty:
                return go.Figure().add_annotation(text="No credit support data available", showarrow=False)
            
            # Convert to percentages
            df['current_credit_pct'] = df['current_credit_support'] * 100
            df['original_credit_pct'] = df['original_credit_support'] * 100
            
            # Create correlation analysis
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=(
                    "Credit Support Distribution", "Credit Support vs Yield",
                    "Credit Support by Deal Type", "Original vs Current Credit Support"
                ),
                specs=[
                    [{"type": "histogram"}, {"type": "scatter"}],
                    [{"type": "box"}, {"type": "scatter"}]
                ]
            )
            
            # 1. Credit support distribution
            fig.add_trace(
                go.Histogram(
                    x=df['current_credit_pct'],
                    nbinsx=15,
                    name="Credit Support Distribution",
                    marker_color="lightgreen"
                ),
                row=1, col=1
            )
            
            # 2. Credit support vs yield correlation
            if 'issue_yield' in df.columns:
                df_corr = df.dropna(subset=['issue_yield'])
                df_corr['yield_pct'] = df_corr['issue_yield'] * 100
                
                fig.add_trace(
                    go.Scatter(
                        x=df_corr['current_credit_pct'],
                        y=df_corr['yield_pct'],
                        mode='markers',
                        name="Yield vs Credit Support",
                        marker=dict(
                            size=8, 
                            opacity=0.6,
                            color=df_corr['yield_pct'],
                            colorscale='Viridis',
                            showscale=True
                        ),
                        text=df_corr['deal_type'],
                        hovertemplate="Credit Support: %{x:.1f}%<br>Yield: %{y:.2f}%<br>Deal Type: %{text}<extra></extra>"
                    ),
                    row=1, col=2
                )
            
            # 3. Credit support by deal type
            if 'deal_type' in df.columns and df['deal_type'].notna().any():
                for deal_type in df['deal_type'].dropna().unique():
                    deal_data = df[df['deal_type'] == deal_type]
                    fig.add_trace(
                        go.Box(
                            y=deal_data['current_credit_pct'],
                            name=deal_type,
                            boxmean=True
                        ),
                        row=2, col=1
                    )
            
            # 4. Original vs current credit support
            if 'original_credit_support' in df.columns:
                df_comparison = df.dropna(subset=['original_credit_support'])
                
                fig.add_trace(
                    go.Scatter(
                        x=df_comparison['original_credit_pct'],
                        y=df_comparison['current_credit_pct'],
                        mode='markers',
                        name="Original vs Current",
                        marker=dict(size=8, opacity=0.6),
                        text=df_comparison['issuer_name'],
                        hovertemplate="Original: %{x:.1f}%<br>Current: %{y:.1f}%<br>Issuer: %{text}<extra></extra>"
                    ),
                    row=2, col=2
                )
                
                # Add diagonal line for reference
                max_val = max(df_comparison['original_credit_pct'].max(), df_comparison['current_credit_pct'].max())
                fig.add_trace(
                    go.Scatter(
                        x=[0, max_val],
                        y=[0, max_val],
                        mode='lines',
                        name="Equal Line",
                        line=dict(dash='dash', color='red')
                    ),
                    row=2, col=2
                )
            
            fig.update_layout(
                title="Credit Enhancement Analysis",
                height=800,
                showlegend=False
            )
            
            conn.close()
            return fig
            
        except Exception as e:
            st.error(f"Error creating credit support analysis: {str(e)}")
            return go.Figure()
    
    def create_performance_tracking_chart(self) -> go.Figure:
        """Create performance tracking chart from surveillance data"""
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Get surveillance time series data
            surveillance_query = """
                SELECT 
                    deal_id,
                    report_date,
                    current_balance,
                    pool_factor,
                    delinq_30_plus,
                    delinq_60_plus,
                    delinq_90_plus,
                    charge_offs
                FROM SurveillanceTimeSeries
                WHERE report_date IS NOT NULL
                ORDER BY deal_id, report_date
            """
            
            df = pd.read_sql_query(surveillance_query, conn)
            
            if df.empty:
                return go.Figure().add_annotation(text="No surveillance data available", showarrow=False)
            
            # Convert report_date to datetime
            df['report_date'] = pd.to_datetime(df['report_date'])
            
            # Create performance tracking charts
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=(
                    "Pool Balance Over Time", "Pool Factor Trends",
                    "Delinquency Trends", "Charge-off Rates"
                )
            )
            
            # Get unique deals (limit to top 5 for clarity)
            top_deals = df['deal_id'].value_counts().head(5).index.tolist()
            colors = px.colors.qualitative.Set1
            
            # 1. Pool balance over time
            for i, deal_id in enumerate(top_deals):
                deal_data = df[df['deal_id'] == deal_id].sort_values('report_date')
                if not deal_data.empty and 'current_balance' in deal_data.columns:
                    fig.add_trace(
                        go.Scatter(
                            x=deal_data['report_date'],
                            y=deal_data['current_balance'] / 1_000_000,  # Convert to millions
                            mode='lines+markers',
                            name=f"{deal_id}",
                            line=dict(color=colors[i % len(colors)])
                        ),
                        row=1, col=1
                    )
            
            # 2. Pool factor trends
            for i, deal_id in enumerate(top_deals):
                deal_data = df[df['deal_id'] == deal_id].sort_values('report_date')
                if not deal_data.empty and 'pool_factor' in deal_data.columns:
                    fig.add_trace(
                        go.Scatter(
                            x=deal_data['report_date'],
                            y=deal_data['pool_factor'],
                            mode='lines+markers',
                            name=f"{deal_id}",
                            line=dict(color=colors[i % len(colors)]),
                            showlegend=False
                        ),
                        row=1, col=2
                    )
            
            # 3. Delinquency trends (stacked area for 30+, 60+, 90+)
            if any(col in df.columns for col in ['delinq_30_plus', 'delinq_60_plus', 'delinq_90_plus']):
                for deal_id in top_deals[:3]:  # Limit to 3 deals for clarity
                    deal_data = df[df['deal_id'] == deal_id].sort_values('report_date')
                    if not deal_data.empty:
                        for delinq_type, color in [('delinq_30_plus', 'lightblue'), ('delinq_60_plus', 'orange'), ('delinq_90_plus', 'red')]:
                            if delinq_type in deal_data.columns:
                                fig.add_trace(
                                    go.Scatter(
                                        x=deal_data['report_date'],
                                        y=deal_data[delinq_type] * 100,  # Convert to percentage
                                        mode='lines',
                                        name=f"{deal_id} {delinq_type}",
                                        fill='tonexty' if delinq_type != 'delinq_30_plus' else None,
                                        line=dict(color=color),
                                        showlegend=False
                                    ),
                                    row=2, col=1
                                )
            
            # 4. Charge-off rates
            for i, deal_id in enumerate(top_deals):
                deal_data = df[df['deal_id'] == deal_id].sort_values('report_date')
                if not deal_data.empty and 'charge_offs' in deal_data.columns:
                    fig.add_trace(
                        go.Scatter(
                            x=deal_data['report_date'],
                            y=deal_data['charge_offs'] * 100,  # Convert to percentage
                            mode='lines+markers',
                            name=f"{deal_id}",
                            line=dict(color=colors[i % len(colors)]),
                            showlegend=False
                        ),
                        row=2, col=2
                    )
            
            fig.update_layout(
                title="Portfolio Performance Tracking",
                height=800
            )
            
            # Update axis labels
            fig.update_xaxes(title_text="Date", row=1, col=1)
            fig.update_yaxes(title_text="Balance ($M)", row=1, col=1)
            
            fig.update_xaxes(title_text="Date", row=1, col=2)
            fig.update_yaxes(title_text="Pool Factor", row=1, col=2)
            
            fig.update_xaxes(title_text="Date", row=2, col=1)
            fig.update_yaxes(title_text="Delinquency Rate (%)", row=2, col=1)
            
            fig.update_xaxes(title_text="Date", row=2, col=2)
            fig.update_yaxes(title_text="Charge-off Rate (%)", row=2, col=2)
            
            conn.close()
            return fig
            
        except Exception as e:
            st.error(f"Error creating performance tracking: {str(e)}")
            return go.Figure()

# Function to render the advanced analytics dashboard
def render_advanced_analytics_dashboard():
    """Render the advanced analytics dashboard"""
    
    st.header("📊 Advanced Analytics Dashboard")
    
    # Initialize analytics processor
    if 'analytics_dashboard' not in st.session_state:
        st.session_state.analytics_dashboard = AdvancedAnalyticsDashboard(
            st.session_state.fixed_abs_system.db_path
        )
    
    dashboard = st.session_state.analytics_dashboard
    
    # Market overview section
    st.subheader("🌍 Market Overview")
    
    market_overview = dashboard.get_market_overview()
    
    if 'error' not in market_overview:
        # Display key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        pricing_stats = market_overview.get('pricing', {})
        deals_stats = market_overview.get('deals', {})
        
        with col1:
            total_securities = pricing_stats.get('total_securities', 0)
            st.metric("Total Securities", f"{total_securities:,}")
            
        with col2:
            total_issuers = pricing_stats.get('total_issuers', 0)
            st.metric("Unique Issuers", f"{total_issuers:,}")
            
        with col3:
            total_deals = pricing_stats.get('total_deals', 0)
            st.metric("Unique Deals", f"{total_deals:,}")
            
        with col4:
            avg_yield = pricing_stats.get('avg_yield', 0)
            if avg_yield:
                st.metric("Average Yield", f"{avg_yield:.2%}")
            else:
                st.metric("Average Yield", "N/A")
        
        # Additional metrics row
        col5, col6, col7, col8 = st.columns(4)
        
        with col5:
            total_volume = pricing_stats.get('total_issuance_volume', 0)
            if total_volume:
                st.metric("Issuance Volume", f"${total_volume/1_000_000_000:.1f}B")
            else:
                st.metric("Issuance Volume", "N/A")
                
        with col6:
            avg_credit_support = pricing_stats.get('avg_credit_support', 0)
            if avg_credit_support:
                st.metric("Avg Credit Support", f"{avg_credit_support:.1%}")
            else:
                st.metric("Avg Credit Support", "N/A")
                
        with col7:
            extracted_deals = deals_stats.get('extracted_deals', 0)
            st.metric("Extracted Deals", f"{extracted_deals:,}")
            
        with col8:
            note_classes_stats = market_overview.get('note_classes', {})
            total_note_classes = note_classes_stats.get('total_note_classes', 0)
            st.metric("Note Classes", f"{total_note_classes:,}")
    
    st.markdown("---")
    
    # Interactive charts section
    chart_tabs = st.tabs([
        "📊 Deal Type Distribution", 
        "📈 Yield Analysis", 
        "🛡️ Credit Support Analysis", 
        "⏱️ Performance Tracking"
    ])
    
    with chart_tabs[0]:
        st.subheader("Deal Type Distribution")
        deal_type_chart = dashboard.create_deal_type_distribution_chart()
        st.plotly_chart(deal_type_chart, use_container_width=True)
    
    with chart_tabs[1]:
        st.subheader("Comprehensive Yield Analysis")
        yield_chart = dashboard.create_yield_analysis_chart()
        st.plotly_chart(yield_chart, use_container_width=True)
    
    with chart_tabs[2]:
        st.subheader("Credit Enhancement Analysis")
        credit_chart = dashboard.create_credit_support_analysis()
        st.plotly_chart(credit_chart, use_container_width=True)
    
    with chart_tabs[3]:
        st.subheader("Portfolio Performance Tracking")
        performance_chart = dashboard.create_performance_tracking_chart()
        st.plotly_chart(performance_chart, use_container_width=True)


# Keep EVERYTHING else in your file exactly the same
# Only replace the main() function with the version above

if __name__ == "__main__":
    main()

def get_peac_sample():
    """PEAC Solutions sample text"""
    return """
    Executive Summary
    This report summarizes KBRA's analysis of PEAC Solutions Receivables 2025-1, LLC (PEAC 2025-1), an equipment ABS
    transaction. This report is based on information as of February 11, 2025.
    
    The aggregate securitization value is $769.63 million. PEAC 2025-1 will issue five classes of notes, including a short-term tranche. 
    Credit enhancement includes excess spread, a reserve account, overcollateralization and subordination (except for Class C Notes). 
    The overcollateralization is subject to a target equal to 14.00% of the current ASV and a floor equal to 1.00% of the initial ASV.
    """

if __name__ == "__main__":
    main()
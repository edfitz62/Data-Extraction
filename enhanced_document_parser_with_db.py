# -*- coding: utf-8 -*-
"""
Created on Sun Jul 13 12:03:35 2025

@author: edfit
"""

import re
import os
import json
from datetime import datetime
from typing import Dict, List, Any, Optional
import pandas as pd


# Document processing libraries
try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False
    print("⚠️  PyMuPDF not available - install with: pip install PyMuPDF")

# Document processing libraries
try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False
    print("⚠️  PyMuPDF not available - install with: pip install PyMuPDF")

try:
    import PyPDF2
    PYPDF2_AVAILABLE = True
except ImportError:
    PYPDF2_AVAILABLE = False
    print("⚠️  PyPDF2 not available - install with: pip install PyPDF2")

try:
    from docx import Document
    DOCX_AVAILABLE = True
except ImportError:
    DOCX_AVAILABLE = False
    print("⚠️  python-docx not available - install with: pip install python-docx")

# Access database integration
try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False
    print("⚠️  pyodbc not available - install with: pip install pyodbc")

# ============================================================================
# 🎯 PATH CONFIGURATION FUNCTIONS - ADD RIGHT HERE!
# Place these AFTER imports but BEFORE classes
# ============================================================================

def get_script_directory():
    """Get the directory where this script is located"""
    return os.path.dirname(os.path.abspath(__file__))

def get_database_path():
    """Get database path relative to script location"""
    script_dir = get_script_directory()
    return os.path.join(script_dir, "ABS_Performance_Data.accdb")

def get_output_directory():
    """Get or create output directory for Excel files"""
    script_dir = get_script_directory()
    output_dir = os.path.join(script_dir, "Extraction_Results")
    
    # Create directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"✅ Created output directory: {output_dir}")
    
    return output_dir

def get_sharepoint_source_path():
    """Get the SharePoint source folder path"""
    # Update this path to match your actual SharePoint sync location
    return r"C:\Users\edfit\OneDrive - Whitehall Partners\ABS Library\01 - New Issues\By Sector"

def get_timestamped_output_path(prefix="extraction"):
    """Get a timestamped output file path"""
    output_dir = get_output_directory()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}_{timestamp}.xlsx"
    return os.path.join(output_dir, filename)

# Set the default database path using the function
DEFAULT_DB_PATH = get_database_path()

# Print path information for user
print(f"📁 Script directory: {get_script_directory()}")
print(f"💾 Database path: {DEFAULT_DB_PATH}")
print(f"📊 Output directory: {get_output_directory()}")

class AccessDBManager:
    """Access database manager for document parser"""
    
    def __init__(self, db_path):
        self.db_path = db_path
        self.connection_string = None
        self._setup_connection()
    
    def _setup_connection(self):
        """Setup connection to Access database"""
        if not PYODBC_AVAILABLE:
            print("❌ pyodbc not available - database features disabled")
            return False
            
        if not os.path.exists(self.db_path):
            print(f"❌ Database file not found: {self.db_path}")
            return False
        
        # Try different Access drivers
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
                self.connection_string = conn_str
                print(f"✅ Connected to Access database using: {driver}")
                return True
            except Exception as e:
                continue
        
        print("❌ No Access driver found - database features disabled")
        return False
    
    def get_connection(self):
        """Get database connection"""
        if not self.connection_string:
            return None
        try:
            return pyodbc.connect(self.connection_string)
        except Exception as e:
            print(f"Database connection error: {e}")
            return None
    
    def create_tables_if_needed(self):
        """Create required tables if they don't exist"""
        if not self.connection_string:
            return False
        
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            # Check if ABS_Deals table exists
            try:
                cursor.execute("SELECT TOP 1 * FROM ABS_Deals")
                print("✅ ABS_Deals table found")
            except:
                # Create ABS_Deals table
                cursor.execute('''
                    CREATE TABLE ABS_Deals (
                        ID AUTOINCREMENT PRIMARY KEY,
                        DealName TEXT(255),
                        IssueDate DATETIME,
                        RatingAgency TEXT(50),
                        Sector TEXT(100),
                        DealSize CURRENCY,
                        ClassAAdvanceRate DOUBLE,
                        InitialOC DOUBLE,
                        ExpectedCNLLow DOUBLE,
                        ExpectedCNLHigh DOUBLE,
                        ReserveAccount DOUBLE,
                        AvgSeasoning INTEGER,
                        TopObligorConc DOUBLE,
                        OriginalPoolBalance CURRENCY,
                        CreatedDate DATETIME,
                        SourceFile TEXT(255),
                        ConfidenceScore INTEGER
                    )
                ''')
                print("✅ Created ABS_Deals table")
            
            # Check if NoteClasses table exists
            try:
                cursor.execute("SELECT TOP 1 * FROM NoteClasses")
                print("✅ NoteClasses table found")
            except:
                # Create NoteClasses table
                cursor.execute('''
                    CREATE TABLE NoteClasses (
                        ID AUTOINCREMENT PRIMARY KEY,
                        DealID INTEGER,
                        Class TEXT(10),
                        Amount CURRENCY,
                        AmountMillions DOUBLE,
                        InterestRate DOUBLE,
                        Rating TEXT(10),
                        Maturity TEXT(50),
                        Confidence INTEGER,
                        CreatedDate DATETIME
                    )
                ''')
                print("✅ Created NoteClasses table")
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            print(f"Error creating tables: {e}")
            conn.close()
            return False
    
    def save_deal_to_database(self, extraction_result):
        """Save extracted deal data to Access database"""
        if not self.connection_string:
            return None
        
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            
            # Parse issue date
            issue_date = None
            if extraction_result.get('issue_date'):
                try:
                    issue_date = datetime.strptime(extraction_result['issue_date'], '%m/%d/%Y')
                except:
                    try:
                        issue_date = datetime.strptime(extraction_result['issue_date'], '%Y-%m-%d')
                    except:
                        pass
            
            # Insert main deal
            insert_sql = '''
                INSERT INTO ABS_Deals 
                (DealName, IssueDate, RatingAgency, Sector, DealSize, 
                 ClassAAdvanceRate, InitialOC, ExpectedCNLLow, ExpectedCNLHigh,
                 ReserveAccount, AvgSeasoning, TopObligorConc, OriginalPoolBalance,
                 CreatedDate, SourceFile, ConfidenceScore)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            values = (
                extraction_result.get('deal_name', ''),
                issue_date,
                extraction_result.get('rating_agency', ''),
                extraction_result.get('sector', ''),
                float(extraction_result.get('deal_size', 0)) if extraction_result.get('deal_size') else None,
                float(extraction_result.get('class_a_advance_rate', 0)) if extraction_result.get('class_a_advance_rate') else None,
                float(extraction_result.get('initial_oc', 0)) if extraction_result.get('initial_oc') else None,
                float(extraction_result.get('expected_cnl_low', 0)) if extraction_result.get('expected_cnl_low') else None,
                float(extraction_result.get('expected_cnl_high', 0)) if extraction_result.get('expected_cnl_high') else None,
                float(extraction_result.get('reserve_account', 0)) if extraction_result.get('reserve_account') else None,
                int(extraction_result.get('avg_seasoning', 0)) if extraction_result.get('avg_seasoning') else None,
                float(extraction_result.get('top_obligor_conc', 0)) if extraction_result.get('top_obligor_conc') else None,
                float(extraction_result.get('original_pool_balance_millions', 0)) if extraction_result.get('original_pool_balance_millions') else None,
                datetime.now(),
                os.path.basename(extraction_result.get('source_file', '')),
                int(extraction_result.get('confidence_score', 0))
            )
            
            cursor.execute(insert_sql, values)
            cursor.execute("SELECT @@IDENTITY")
            deal_id = cursor.fetchone()[0]
            
            # Insert note classes
            note_classes = extraction_result.get('note_classes', [])
            for note_class in note_classes:
                cursor.execute('''
                    INSERT INTO NoteClasses 
                    (DealID, Class, Amount, AmountMillions, InterestRate, Rating, Maturity, Confidence, CreatedDate)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    deal_id,
                    note_class.get('class', ''),
                    float(note_class.get('amount', 0)) if note_class.get('amount') else None,
                    float(note_class.get('amount_millions', 0)) if note_class.get('amount_millions') else None,
                    float(note_class.get('interest_rate', 0)) if note_class.get('interest_rate') else None,
                    note_class.get('rating', ''),
                    note_class.get('maturity', ''),
                    int(note_class.get('confidence', 0)),
                    datetime.now()
                ))
            
            conn.commit()
            conn.close()
            
            print(f"✅ Saved to database: {extraction_result.get('deal_name', 'Unknown')} (ID: {deal_id})")
            print(f"   📊 Saved {len(note_classes)} note classes")
            return deal_id
            
        except Exception as e:
            print(f"❌ Error saving to database: {e}")
            conn.close()
            return None

class EnhancedNewIssueExtractor:
    def __init__(self, db_path=None):
        """Initialize the enhanced document extractor for New Issue Reports."""
        self.db_manager = None
        if db_path:
            self.db_manager = AccessDBManager(db_path)
            if self.db_manager.connection_string:
                self.db_manager.create_tables_if_needed()
                print(f"🔗 Database integration enabled: {os.path.basename(db_path)}")
            else:
                print("⚠️  Database integration disabled - continuing with Excel export only")
        self.note_class_patterns = {
            # Note class identification patterns
            'class_headers': [
                r'class\s+([a-z])\s+notes?',
                r'([a-z])\s+class\s+notes?',
                r'class\s+([a-z])',
                r'([a-z])-\s*class',
                r'senior\s+class\s+([a-z])',
                r'subordinate\s+class\s+([a-z])',
                r'notes?\s+class\s+([a-z])'
            ],
            
            # Amount patterns for note classes
            'amount_patterns': [
                r'\$\s*([\d,]+\.?\d*)\s*(?:million|mm|m)?',
                r'([\d,]+\.?\d*)\s*(?:million|mm|m)',
                r'\$\s*([\d,]+,\d{3})',
                r'amount[:\s]*\$?\s*([\d,]+\.?\d*)',
                r'principal[:\s]*\$?\s*([\d,]+\.?\d*)',
                r'balance[:\s]*\$?\s*([\d,]+\.?\d*)'
            ],
            
            # Interest rate patterns
            'rate_patterns': [
                r'([\d\.]+)%',
                r'rate[:\s]*([\d\.]+)',
                r'coupon[:\s]*([\d\.]+)',
                r'interest[:\s]*([\d\.]+)%?'
            ],
            
            # Rating patterns
            'rating_patterns': [
                r'([A-Z]{1,3}[+-]?)',
                r'rating[:\s]*([A-Z]{1,3}[+-]?)',
                r'rated[:\s]*([A-Z]{1,3}[+-]?)'
            ]
        }
        
        self.pool_balance_patterns = [
            r'original\s+pool\s+balance[:\s]*\$?\s*([\d,]+\.?\d*)\s*(?:million|mm|m)?',
            r'initial\s+pool\s+balance[:\s]*\$?\s*([\d,]+\.?\d*)\s*(?:million|mm|m)?',
            r'pool\s+balance[:\s]*\$?\s*([\d,]+\.?\d*)\s*(?:million|mm|m)?',
            r'total\s+pool[:\s]*\$?\s*([\d,]+\.?\d*)\s*(?:million|mm|m)?',
            r'aggregate\s+principal\s+balance[:\s]*\$?\s*([\d,]+\.?\d*)\s*(?:million|mm|m)?',
            r'total\s+initial\s+principal[:\s]*\$?\s*([\d,]+\.?\d*)\s*(?:million|mm|m)?'
        ]
        
        # Enhanced extraction patterns for existing fields
        self.enhanced_patterns = {
            'deal_name': [
                r'(?:deal|transaction|issuance)[:\s]*([^\n\r]{5,100})',
                r'^([A-Z][A-Za-z\s&\-\d]+(?:trust|llc|ltd|inc).*?(?:series|20\d{2}).*?)(?:\n|\r|$)',
                r'([A-Z][A-Za-z\s&\-\d]{10,80}(?:trust|funding|capital|finance).*?(?:20\d{2}|series).*?)',
                r'title[:\s]*([^\n\r]{10,100})'
            ],
            
            'deal_size': [
                r'total\s+issuance[:\s]*\$?\s*([\d,]+\.?\d*)\s*(?:million|mm|m)?',
                r'aggregate\s+principal[:\s]*\$?\s*([\d,]+\.?\d*)\s*(?:million|mm|m)?',
                r'deal\s+size[:\s]*\$?\s*([\d,]+\.?\d*)\s*(?:million|mm|m)?',
                r'total\s+notes[:\s]*\$?\s*([\d,]+\.?\d*)\s*(?:million|mm|m)?'
            ],
            
            'issue_date': [
                r'issue\s+date[:\s]*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})',
                r'closing\s+date[:\s]*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})',
                r'settlement\s+date[:\s]*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})',
                r'dated[:\s]*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})'
            ],
            
            'rating_agency': [
                r'(kbra|moody\'?s|standard\s*&\s*poor\'?s|s&p|fitch|dbrs)',
                r'rated\s+by[:\s]*(kbra|moody\'?s|s&p|fitch|dbrs)',
                r'rating\s+agency[:\s]*(kbra|moody\'?s|s&p|fitch|dbrs)'
            ]
        }

    def extract_text_from_pdf(self, file_path: str) -> str:
        """Extract text from PDF using multiple methods."""
        text = ""
        
        # Try PyMuPDF first (better for complex layouts)
        if PYMUPDF_AVAILABLE:
            try:
                doc = fitz.open(file_path)
                for page in doc:
                    text += page.get_text()
                doc.close()
                if text.strip():
                    return text
            except Exception as e:
                print(f"PyMuPDF failed: {e}")
        
        # Fallback to PyPDF2
        if PYPDF2_AVAILABLE:
            try:
                with open(file_path, 'rb') as file:
                    reader = PyPDF2.PdfReader(file)
                    for page in reader.pages:
                        text += page.extract_text()
                if text.strip():
                    return text
            except Exception as e:
                print(f"PyPDF2 failed: {e}")
        
        return text

    def extract_text_from_docx(self, file_path: str) -> str:
        """Extract text from DOCX file."""
        if not DOCX_AVAILABLE:
            return ""
        
        try:
            doc = Document(file_path)
            text = ""
            for paragraph in doc.paragraphs:
                text += paragraph.text + "\n"
            return text
        except Exception as e:
            print(f"DOCX extraction failed: {e}")
            return ""

    def extract_note_classes(self, text: str) -> List[Dict[str, Any]]:
        """Extract all note classes with their details."""
        note_classes = []
        text_lines = text.split('\n')
        
        # Look for note class information
        for i, line in enumerate(text_lines):
            line_clean = line.strip().lower()
            
            # Check if this line contains a note class identifier
            for pattern in self.note_class_patterns['class_headers']:
                match = re.search(pattern, line_clean, re.IGNORECASE)
                if match:
                    class_letter = match.group(1).upper()
                    
                    # Extract details for this note class
                    note_class = self._extract_note_class_details(
                        class_letter, line, text_lines[max(0, i-2):min(len(text_lines), i+5)]
                    )
                    
                    if note_class:
                        note_classes.append(note_class)
        
        # Remove duplicates and sort by class letter
        unique_classes = {}
        for nc in note_classes:
            class_id = nc['class']
            if class_id not in unique_classes or nc['confidence'] > unique_classes[class_id]['confidence']:
                unique_classes[class_id] = nc
        
        return sorted(list(unique_classes.values()), key=lambda x: x['class'])

    def _extract_note_class_details(self, class_letter: str, main_line: str, context_lines: List[str]) -> Optional[Dict[str, Any]]:
        """Extract detailed information for a specific note class."""
        note_class = {
            'class': class_letter,
            'amount': None,
            'amount_millions': None,
            'interest_rate': None,
            'rating': None,
            'maturity': None,
            'confidence': 0
        }
        
        # Combine all context for analysis
        full_context = ' '.join(context_lines).lower()
        
        # Extract amount
        for pattern in self.note_class_patterns['amount_patterns']:
            match = re.search(pattern, full_context, re.IGNORECASE)
            if match:
                amount_str = match.group(1).replace(',', '')
                try:
                    amount = float(amount_str)
                    note_class['amount'] = amount
                    # Convert to millions if needed
                    if 'million' in full_context or amount > 1000:
                        note_class['amount_millions'] = amount if amount < 1000 else amount / 1000000
                    else:
                        note_class['amount_millions'] = amount
                    note_class['confidence'] += 25
                    break
                except ValueError:
                    continue
        
        # Extract interest rate
        for pattern in self.note_class_patterns['rate_patterns']:
            match = re.search(pattern, full_context)
            if match:
                try:
                    rate = float(match.group(1))
                    if 0.1 <= rate <= 15:  # Reasonable range for interest rates
                        note_class['interest_rate'] = rate
                        note_class['confidence'] += 20
                        break
                except ValueError:
                    continue
        
        # Extract rating
        for pattern in self.note_class_patterns['rating_patterns']:
            match = re.search(pattern, full_context, re.IGNORECASE)
            if match:
                rating = match.group(1).upper()
                if len(rating) <= 4 and any(c in rating for c in 'ABCD'):
                    note_class['rating'] = rating
                    note_class['confidence'] += 15
                    break
        
        # Extract maturity information
        maturity_patterns = [
            r'maturity[:\s]*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})',
            r'due[:\s]*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})',
            r'(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})\s*maturity'
        ]
        
        for pattern in maturity_patterns:
            match = re.search(pattern, full_context, re.IGNORECASE)
            if match:
                note_class['maturity'] = match.group(1)
                note_class['confidence'] += 10
                break
        
        # Base confidence for finding the class
        note_class['confidence'] += 30
        
        return note_class if note_class['confidence'] >= 40 else None

    def extract_original_pool_balance(self, text: str) -> Optional[Dict[str, Any]]:
        """Extract the original pool balance."""
        text_clean = text.lower()
        
        for pattern in self.pool_balance_patterns:
            match = re.search(pattern, text_clean, re.IGNORECASE)
            if match:
                balance_str = match.group(1).replace(',', '')
                try:
                    balance = float(balance_str)
                    
                    # Determine if it's in millions
                    context = text_clean[max(0, match.start()-50):match.end()+50]
                    is_millions = any(indicator in context for indicator in ['million', 'mm', '$m'])
                    
                    return {
                        'original_pool_balance': balance,
                        'original_pool_balance_millions': balance if is_millions or balance < 1000 else balance / 1000000,
                        'pool_balance_raw_text': match.group(0),
                        'confidence': 80
                    }
                except ValueError:
                    continue
        
        return None

    def extract_enhanced_fields(self, text: str) -> Dict[str, Any]:
        """Extract enhanced deal information."""
        result = {}
        
        for field, patterns in self.enhanced_patterns.items():
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                if match:
                    if field == 'deal_size':
                        try:
                            size_str = match.group(1).replace(',', '')
                            size = float(size_str)
                            result[field] = size if size < 1000 else size / 1000000
                        except ValueError:
                            continue
                    elif field == 'rating_agency':
                        agency = match.group(1).lower()
                        agency_map = {
                            'kbra': 'KBRA',
                            "moody's": 'Moody\'s',
                            'moodys': 'Moody\'s',
                            's&p': 'S&P',
                            'standard & poor\'s': 'S&P',
                            'fitch': 'Fitch',
                            'dbrs': 'DBRS'
                        }
                        result[field] = agency_map.get(agency, agency.upper())
                    else:
                        result[field] = match.group(1).strip()
                    break
        
        return result

    def process_file(self, file_path: str, save_to_db: bool = True) -> Dict[str, Any]:
        """Process a single file and extract all information."""
        if not os.path.exists(file_path):
            return {'error': f'File not found: {file_path}'}
        
        # Extract text based on file type
        file_ext = os.path.splitext(file_path)[1].lower()
        
        if file_ext == '.pdf':
            text = self.extract_text_from_pdf(file_path)
        elif file_ext in ['.docx', '.doc']:
            text = self.extract_text_from_docx(file_path)
        else:
            return {'error': f'Unsupported file type: {file_ext}'}
        
        if not text.strip():
            return {'error': 'No text could be extracted from the file'}
        
        # Extract all information
        result = {
            'source_file': file_path,
            'extraction_date': datetime.now().isoformat(),
            'file_type': file_ext
        }
        
        # Enhanced basic fields
        enhanced_fields = self.extract_enhanced_fields(text)
        result.update(enhanced_fields)
        
        # Extract all note classes
        note_classes = self.extract_note_classes(text)
        result['note_classes'] = note_classes
        result['total_note_classes'] = len(note_classes)
        
        # Calculate total deal size from note classes if not found in basic extraction
        if not result.get('deal_size') and note_classes:
            total_from_classes = sum(nc.get('amount_millions', 0) for nc in note_classes if nc.get('amount_millions'))
            if total_from_classes > 0:
                result['deal_size'] = total_from_classes
                result['deal_size_source'] = 'calculated_from_note_classes'
        
        # Extract original pool balance
        pool_balance = self.extract_original_pool_balance(text)
        if pool_balance:
            result.update(pool_balance)
        
        # Calculate confidence score
        confidence_factors = []
        if result.get('deal_name'): confidence_factors.append(20)
        if result.get('deal_size'): confidence_factors.append(15)
        if result.get('original_pool_balance'): confidence_factors.append(15)
        if result.get('issue_date'): confidence_factors.append(10)
        if result.get('rating_agency'): confidence_factors.append(10)
        if note_classes: confidence_factors.append(len(note_classes) * 10)
        
        result['confidence_score'] = min(100, sum(confidence_factors))
        
        # Save to database if enabled and requested
        if save_to_db and self.db_manager and self.db_manager.connection_string:
            db_id = self.db_manager.save_deal_to_database(result)
            if db_id:
                result['database_id'] = db_id
        
        return result

    def process_folder(self, folder_path: str, save_to_db: bool = True) -> List[Dict[str, Any]]:
        """Process all supported files in a folder."""
        if not os.path.exists(folder_path):
            return [{'error': f'Folder not found: {folder_path}'}]
        
        results = []
        supported_extensions = ['.pdf', '.docx', '.doc']
        
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path):
                file_ext = os.path.splitext(filename)[1].lower()
                if file_ext in supported_extensions:
                    print(f"Processing: {filename}")
                    result = self.process_file(file_path, save_to_db)
                    results.append(result)
        
        return results
    
       
    def process_folder_recursive(self, folder_path: str, save_to_db: bool = True, max_depth: int = 5) -> List[Dict[str, Any]]:
        if not os.path.exists(folder_path):
            return [{'error': f'Folder not found: {folder_path}'}]
        
        results = []
        supported_extensions = ['.pdf', '.docx', '.doc']
        total_files = 0
        processed_files = 0
        
        print(f"🔍 Scanning folder and subfolders: {folder_path}")
        
        # Walk through all directories and subdirectories
        for root, dirs, files in os.walk(folder_path):
            # Calculate current depth
            depth = root.replace(folder_path, '').count(os.sep)
            if depth >= max_depth:
                dirs[:] = []  # Don't go deeper
                continue
            
            # Print current folder being processed
            relative_path = os.path.relpath(root, folder_path)
            if relative_path == '.':
                print(f"📁 Processing main folder...")
            else:
                print(f"📁 Processing subfolder: {relative_path}")
            
            # Process files in current directory
            folder_file_count = 0
            for filename in files:
                file_ext = os.path.splitext(filename)[1].lower()
                if file_ext in supported_extensions:
                    total_files += 1
                    folder_file_count += 1
                    file_path = os.path.join(root, filename)
                    
                    print(f"   📄 Processing: {filename}")
                    try:
                        result = self.process_file(file_path, save_to_db)
                        
                        # Add folder information to the result
                        if 'error' not in result:
                            result['source_folder'] = relative_path if relative_path != '.' else 'main'
                            result['folder_category'] = os.path.basename(root) if relative_path != '.' else 'main'
                            processed_files += 1
                            print(f"      ✅ Success: {result.get('deal_name', 'Unknown')} (Confidence: {result.get('confidence_score', 0)}%)")
                        else:
                            print(f"      ❌ Failed: {result.get('error', 'Unknown error')}")
                        
                        results.append(result)
                        
                    except Exception as e:
                        error_result = {
                            'error': f'Processing failed for {filename}: {str(e)}',
                            'source_file': file_path,
                            'source_folder': relative_path if relative_path != '.' else 'main'
                        }
                        results.append(error_result)
                        print(f"      ❌ Exception: {str(e)}")
            
            if folder_file_count > 0:
                print(f"   📊 Processed {folder_file_count} files in this folder")
            elif len([f for f in files if os.path.splitext(f)[1].lower() in supported_extensions]) == 0:
                print(f"   📂 No supported files found in this folder")
        
        # Summary
        successful = len([r for r in results if 'error' not in r])
        failed = len([r for r in results if 'error' in r])
        
        print(f"\n📊 RECURSIVE PROCESSING SUMMARY:")
        print(f"   📁 Total folders scanned: {len(set(r.get('source_folder', 'main') for r in results)) if results else 0}")
        print(f"   📄 Total files found: {total_files}")
        print(f"   ✅ Successfully processed: {successful}")
        print(f"   ❌ Failed: {failed}")
        
        return results
    
    # ADD THE ENHANCED EXCEL EXPORT METHOD HERE:
    def export_to_excel_enhanced(self, results: List[Dict[str, Any]], output_path: str):
        """Export extraction results to Excel with folder breakdown."""
        if not results:
            print("No results to export")
            return
        
        # Prepare main deal data
        main_data = []
        note_classes_data = []
        folder_summary = {}
        
        for result in results:
            if 'error' in result:
                continue
            
            # Track folder statistics
            folder = result.get('folder_category', 'main')
            if folder not in folder_summary:
                folder_summary[folder] = {'count': 0, 'total_size': 0, 'avg_confidence': 0}
            
            folder_summary[folder]['count'] += 1
            folder_summary[folder]['total_size'] += result.get('deal_size', 0)
            folder_summary[folder]['avg_confidence'] += result.get('confidence_score', 0)
            
            # Main deal information
            main_row = {
                'Deal Name': result.get('deal_name', ''),
                'Folder Category': result.get('folder_category', 'main'),
                'Source Folder': result.get('source_folder', 'main'),
                'Deal Size (Millions)': result.get('deal_size', ''),
                'Original Pool Balance (Millions)': result.get('original_pool_balance_millions', ''),
                'Issue Date': result.get('issue_date', ''),
                'Rating Agency': result.get('rating_agency', ''),
                'Total Note Classes': result.get('total_note_classes', 0),
                'Confidence Score': result.get('confidence_score', 0),
                'Source File': os.path.basename(result.get('source_file', '')),
                'Extraction Date': result.get('extraction_date', '')
            }
            main_data.append(main_row)
            
            # Note classes data
            deal_name = result.get('deal_name', 'Unknown Deal')
            for note_class in result.get('note_classes', []):
                note_row = {
                    'Deal Name': deal_name,
                    'Folder Category': result.get('folder_category', 'main'),
                    'Class': note_class.get('class', ''),
                    'Amount (Millions)': note_class.get('amount_millions', ''),
                    'Interest Rate (%)': note_class.get('interest_rate', ''),
                    'Rating': note_class.get('rating', ''),
                    'Maturity': note_class.get('maturity', ''),
                    'Confidence': note_class.get('confidence', 0),
                    'Source File': os.path.basename(result.get('source_file', ''))
                }
                note_classes_data.append(note_row)
        
        # Calculate folder summary statistics
        for folder in folder_summary:
            if folder_summary[folder]['count'] > 0:
                folder_summary[folder]['avg_confidence'] = folder_summary[folder]['avg_confidence'] / folder_summary[folder]['count']
        
        # Prepare folder summary data
        summary_data = []
        for folder, stats in folder_summary.items():
            summary_data.append({
                'Folder': folder,
                'Files Processed': stats['count'],
                'Total Deal Size (Millions)': stats['total_size'],
                'Average Confidence': round(stats['avg_confidence'], 1)
            })
        
        # Create Excel file
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Folder summary sheet
            if summary_data:
                df_summary = pd.DataFrame(summary_data)
                df_summary.to_excel(writer, sheet_name='Folder Summary', index=False)
            
            # Main deals sheet
            if main_data:
                df_main = pd.DataFrame(main_data)
                df_main.to_excel(writer, sheet_name='Deals Summary', index=False)
            
            # Note classes sheet
            if note_classes_data:
                df_notes = pd.DataFrame(note_classes_data)
                df_notes.to_excel(writer, sheet_name='Note Classes', index=False)
        
        print(f"✅ Enhanced results exported to: {output_path}")
        print(f"📊 Exported {len(main_data)} deals from {len(folder_summary)} folders with {len(note_classes_data)} note classes")

    def export_to_excel(self, results: List[Dict[str, Any]], output_path: str):
        """Export extraction results to Excel with separate sheets for note classes."""
        if not results:
            print("No results to export")
            return
        
        # Prepare main deal data
        main_data = []
        note_classes_data = []
        
        for result in results:
            if 'error' in result:
                continue
            
            # Main deal information
            main_row = {
                'Deal Name': result.get('deal_name', ''),
                'Deal Size (Millions)': result.get('deal_size', ''),
                'Original Pool Balance (Millions)': result.get('original_pool_balance_millions', ''),
                'Issue Date': result.get('issue_date', ''),
                'Rating Agency': result.get('rating_agency', ''),
                'Total Note Classes': result.get('total_note_classes', 0),
                'Confidence Score': result.get('confidence_score', 0),
                'Source File': os.path.basename(result.get('source_file', '')),
                'Extraction Date': result.get('extraction_date', '')
            }
            main_data.append(main_row)
            
            # Note classes data
            deal_name = result.get('deal_name', 'Unknown Deal')
            for note_class in result.get('note_classes', []):
                note_row = {
                    'Deal Name': deal_name,
                    'Class': note_class.get('class', ''),
                    'Amount (Millions)': note_class.get('amount_millions', ''),
                    'Interest Rate (%)': note_class.get('interest_rate', ''),
                    'Rating': note_class.get('rating', ''),
                    'Maturity': note_class.get('maturity', ''),
                    'Confidence': note_class.get('confidence', 0),
                    'Source File': os.path.basename(result.get('source_file', ''))
                }
                note_classes_data.append(note_row)
        
        # Create Excel file
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Main deals sheet
            if main_data:
                df_main = pd.DataFrame(main_data)
                df_main.to_excel(writer, sheet_name='Deals Summary', index=False)
            
            # Note classes sheet
            if note_classes_data:
                df_notes = pd.DataFrame(note_classes_data)
                df_notes.to_excel(writer, sheet_name='Note Classes', index=False)
        
        print(f"✅ Results exported to: {output_path}")
        print(f"📊 Exported {len(main_data)} deals with {len(note_classes_data)} note classes")

# Initialize the enhanced extractor with database path
# Update this path to match your Access database location
DEFAULT_DB_PATH = r"C:\Users\edfit\OneDrive\Documents\ABS_Performance_Data.db.accdb"

def initialize_extractor(db_path=None):
    """Initialize extractor with optional database integration"""
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    
    extractor = EnhancedNewIssueExtractor(db_path)
    return extractor

# Create default instance
extractor = initialize_extractor()

def extract_and_display_enhanced(file_path: str, save_to_db: bool = True):
    """Extract and display all information including note classes and pool balance."""
    result = extractor.process_file(file_path, save_to_db)
    
    if 'error' in result:
        print(f"❌ Error: {result['error']}")
        return
    
    print(f"\n🎯 ENHANCED EXTRACTION RESULTS")
    print(f"{'='*60}")
    print(f"📄 File: {os.path.basename(result['source_file'])}")
    print(f"🎯 Confidence: {result['confidence_score']}%")
    
    # Database status
    if result.get('database_id'):
        print(f"💾 Saved to database with ID: {result['database_id']}")
    elif save_to_db:
        print(f"⚠️  Database save failed or disabled")
    
    print(f"\n📋 DEAL INFORMATION:")
    print(f"   Deal Name: {result.get('deal_name', 'Not found')}")
    print(f"   Deal Size: ${result.get('deal_size', 'Not found')}M")
    print(f"   Issue Date: {result.get('issue_date', 'Not found')}")
    print(f"   Rating Agency: {result.get('rating_agency', 'Not found')}")
    
    # Original Pool Balance
    if result.get('original_pool_balance_millions'):
        print(f"\n💰 ORIGINAL POOL BALANCE:")
        print(f"   Balance: ${result['original_pool_balance_millions']:.2f}M")
        print(f"   Confidence: {result.get('confidence', 'N/A')}%")
    
    # Note Classes
    note_classes = result.get('note_classes', [])
    if note_classes:
        print(f"\n📊 NOTE CLASSES ({len(note_classes)} found):")
        print(f"{'Class':<8} {'Amount (M)':<12} {'Rate (%)':<10} {'Rating':<8} {'Maturity':<12} {'Conf':<5}")
        print(f"{'-'*65}")
        
        total_amount = 0
        for nc in note_classes:
            amount = nc.get('amount_millions', 0) or 0
            total_amount += amount
            
            print(f"{nc['class']:<8} "
                  f"${amount:<11.1f} "
                  f"{nc.get('interest_rate', 'N/A'):<10} "
                  f"{nc.get('rating', 'N/A'):<8} "
                  f"{nc.get('maturity', 'N/A'):<12} "
                  f"{nc.get('confidence', 0):<5}%")
        
        print(f"{'-'*65}")
        print(f"{'TOTAL':<8} ${total_amount:<11.1f}")
    else:
        print(f"\n📊 NOTE CLASSES: None found")
    
    return result

def process_folder_enhanced(folder_path: str, export_excel: bool = True, save_to_db: bool = True):
    """Process a folder and optionally export to Excel and/or save to database."""
    print(f"🔍 Processing folder: {folder_path}")
    results = extractor.process_folder(folder_path, save_to_db)
    
    successful_extractions = [r for r in results if 'error' not in r]
    failed_extractions = [r for r in results if 'error' in r]
    
    print(f"\n📊 PROCESSING SUMMARY:")
    print(f"   ✅ Successful: {len(successful_extractions)}")
    print(f"   ❌ Failed: {len(failed_extractions)}")
    
    if save_to_db and extractor.db_manager and extractor.db_manager.connection_string:
        db_saved = len([r for r in successful_extractions if r.get('database_id')])
        print(f"   💾 Saved to database: {db_saved}")
    
    if failed_extractions:
        print(f"\n❌ FAILED FILES:")
        for result in failed_extractions:
            print(f"   {result.get('error', 'Unknown error')}")
    
    if successful_extractions and export_excel:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"new_issue_extraction_{timestamp}.xlsx"
        extractor.export_to_excel(successful_extractions, output_path)
    
    return results

def quick_database_test():
    """Test database connection and show current data"""
    if not extractor.db_manager or not extractor.db_manager.connection_string:
        print("❌ No database connection available")
        return
    
    conn = extractor.db_manager.get_connection()
    if not conn:
        print("❌ Cannot connect to database")
        return
    
    try:
        cursor = conn.cursor()
        
        # Check ABS_Deals table
        cursor.execute("SELECT COUNT(*) FROM ABS_Deals")
        deals_count = cursor.fetchone()[0]
        print(f"📊 Current deals in database: {deals_count}")
        
        if deals_count > 0:
            cursor.execute("SELECT TOP 3 DealName, DealSize, CreatedDate FROM ABS_Deals ORDER BY CreatedDate DESC")
            print(f"\n📋 Recent deals:")
            for row in cursor.fetchall():
                print(f"   {row[0]} - ${row[1]}M - {row[2].strftime('%Y-%m-%d %H:%M') if row[2] else 'N/A'}")
        
        # Check NoteClasses table
        cursor.execute("SELECT COUNT(*) FROM NoteClasses")
        classes_count = cursor.fetchone()[0]
        print(f"📊 Total note classes in database: {classes_count}")
        
        conn.close()
        print("✅ Database connection test successful")
        
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        conn.close()
        
def process_folder_enhanced_recursive(folder_path: str, export_excel: bool = True, save_to_db: bool = True, max_depth: int = 5):
    """Process a folder recursively and optionally export to Excel and/or save to database."""
    print(f"🔍 Starting recursive processing of: {folder_path}")
    print(f"📊 Maximum depth: {max_depth} levels")
    
    # Use the global extractor instance
    results = extractor.process_folder_recursive(folder_path, save_to_db, max_depth)
    
    successful_extractions = [r for r in results if 'error' not in r]
    failed_extractions = [r for r in results if 'error' in r]
    
    print(f"\n📊 FINAL PROCESSING SUMMARY:")
    print(f"   ✅ Successful: {len(successful_extractions)}")
    print(f"   ❌ Failed: {len(failed_extractions)}")
    
    if save_to_db and extractor.db_manager and extractor.db_manager.connection_string:
        db_saved = len([r for r in successful_extractions if r.get('database_id')])
        print(f"   💾 Saved to database: {db_saved}")
    
    # Show breakdown by folder
    if successful_extractions:
        folder_breakdown = {}
        for result in successful_extractions:
            folder = result.get('folder_category', 'main')
            if folder not in folder_breakdown:
                folder_breakdown[folder] = 0
            folder_breakdown[folder] += 1
        
        print(f"\n📁 SUCCESS BREAKDOWN BY FOLDER:")
        for folder, count in sorted(folder_breakdown.items()):
            print(f"   {folder}: {count} files")
    
    if failed_extractions:
        print(f"\n❌ FAILED FILES (first 10):")
        for result in failed_extractions[:10]:
            folder = result.get('source_folder', 'unknown')
            error = result.get('error', 'Unknown error')
            print(f"   {folder}: {error}")
        
        if len(failed_extractions) > 10:
            print(f"   ... and {len(failed_extractions) - 10} more failures")
    
    if successful_extractions and export_excel:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"recursive_extraction_{timestamp}.xlsx"
        extractor.export_to_excel_enhanced(successful_extractions, output_path)
    
    return results

def process_sharepoint_folder():
    """Process your SharePoint folder structure"""
    
    # Your SharePoint path - update this to your actual synced OneDrive path
    base_path = r"C:\Users\edfit\OneDrive - Whitehall Partners\ABS Library\01 - New Issues\By Sector"
    
    print("🔍 SHAREPOINT FOLDER PROCESSING")
    print("=" * 50)
    print(f"Processing folder: {base_path}")
    print("This will process ALL subfolders including:")
    print("  📁 Autos")
    print("  📁 Consumer Loans") 
    print("  📁 Credit Cards")
    print("  📁 Equipment")
    print("  📁 HIL")
    print("  📁 IT")
    print("  📁 Marketplace Lending")
    print("  📁 Other")
    print("  📁 SFR")
    print("  📁 Small Business")
    print("  📁 Timeshare")
    print("  📁 Transportation")
    print("  📁 WBS")
    print("  📁 And any other subfolders...")
    print()
    
    # Check if path exists
    if not os.path.exists(base_path):
        print(f"❌ Path not found: {base_path}")
        print("💡 Make sure your SharePoint is synced to OneDrive")
        print("💡 Or update the path in the function")
        return []
    
    # Process recursively with max depth of 3 levels
    results = process_folder_enhanced_recursive(
        folder_path=base_path,
        export_excel=True,
        save_to_db=True,
        max_depth=3  # Adjust this if you have deeper folder structures
    )
    
    return results

def process_specific_sector(sector_name):
    """Process just one sector folder"""
    
    base_path = r"C:\Users\edfit\OneDrive - Whitehall Partners\ABS Library\01 - New Issues\By Sector"
    sector_path = os.path.join(base_path, sector_name)
    
    print(f"📁 PROCESSING {sector_name.upper()} SECTOR ONLY")
    print("=" * 40)
    
    # Check if sector path exists
    if not os.path.exists(sector_path):
        print(f"❌ Sector folder not found: {sector_path}")
        print("💡 Available sectors:")
        if os.path.exists(base_path):
            for item in os.listdir(base_path):
                if os.path.isdir(os.path.join(base_path, item)):
                    print(f"   📁 {item}")
        return []
    
    results = process_folder_enhanced_recursive(
        folder_path=sector_path,
        export_excel=True,
        save_to_db=True,
        max_depth=2
    )
    
    return results

def quick_database_test():
    # ... your existing function ...
    pass

def reinitialize_with_database(db_path):
    # ... your existing function ...
    pass        

def reinitialize_with_database(db_path):
    """Reinitialize extractor with a different database path"""
    global extractor
    extractor = initialize_extractor(db_path)
    print(f"🔄 Reinitialized with database: {os.path.basename(db_path)}")
    return extractor


# ============================================================================
# USAGE EXAMPLES (existing, but update with new examples)
# ============================================================================
print("""
🚀 ENHANCED NEW ISSUE EXTRACTOR WITH RECURSIVE PROCESSING!

📋 What's New:
   ✅ Extracts ALL note classes with amounts, rates, ratings
   ✅ Finds original pool balance
   ✅ Enhanced deal information extraction
   ✅ SAVES TO ACCESS DATABASE automatically
   ✅ Exports to Excel with separate sheets
   ✅ RECURSIVE FOLDER PROCESSING - processes all subfolders
   ✅ Folder categorization and breakdown

💡 NEW Usage Examples:

# Process your ENTIRE SharePoint "By Sector" folder (ALL subfolders):
results = process_sharepoint_folder()

# Process just one sector (e.g., Autos, Credit Cards, Equipment):
results = process_specific_sector("Autos")
results = process_specific_sector("Credit Cards")
results = process_specific_sector("Equipment")

# Process any folder recursively:
results = process_folder_enhanced_recursive(
    folder_path="C:\\your\\folder\\path",
    export_excel=True,
    save_to_db=True,
    max_depth=5
)

💡 Usage Examples:

# Test database connection:
quick_database_test()

# Extract from single file (saves to DB + displays):
file_path = r"C:\\path\\to\\your\\new_issue_report.pdf"
result = extract_and_display_enhanced(file_path, save_to_db=True)

# Process entire folder (saves to DB + Excel):
folder_path = r"C:\\path\\to\\reports\\folder"
results = process_folder_enhanced(folder_path, export_excel=True, save_to_db=True)

# Extract without saving to database:
result = extract_and_display_enhanced(file_path, save_to_db=False)

# Use different database:
new_extractor = reinitialize_with_database(r"C:\\path\\to\\different\\database.accdb")

# Process single file programmatically:
result = extractor.process_file(file_path, save_to_db=True)
if result.get('database_id'):
    print(f"✅ Saved to database with ID: {result['database_id']}")
    print(f"Found {len(result.get('note_classes', []))} note classes")
    print(f"Pool balance: ${result.get('original_pool_balance_millions', 0)}M")

🔧 Database Setup:
   📁 Default database path: {DEFAULT_DB_PATH}
   🛠️  To use different database: reinitialize_with_database("your_path.accdb")
   📊 Compatible with your Web UI and ABS Platform

Ready to test with your New Issue Reports! 📄✨💾
""")
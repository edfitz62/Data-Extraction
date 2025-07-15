# backend\main.py
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import pandas as pd
import json
from datetime import datetime
import os

# Create FastAPI app
app = FastAPI(
    title="ABS Navigator API",
    description="Whitehall Partners - Asset-Backed Securities Document Processing System",
    version="1.0.0"
)

# Enable CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Simple database initialization
def init_database():
    conn = sqlite3.connect("abs_navigator.db")
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ABS_Deals (
            deal_id TEXT PRIMARY KEY,
            deal_name TEXT,
            issuer TEXT,
            deal_type TEXT,
            total_deal_size REAL,
            created_date TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS EnhancedExtractionResults (
            id TEXT PRIMARY KEY,
            filename TEXT,
            document_type TEXT,
            extracted_data TEXT,
            extraction_time TEXT,
            success BOOLEAN DEFAULT 0,
            confidence_score REAL DEFAULT 0.0
        )
    """)
    
    conn.commit()
    conn.close()

# Initialize database on startup
init_database()

@app.get("/")
async def root():
    return {
        "message": "ABS Navigator API",
        "company": "Whitehall Partners",
        "status": "running"
    }

@app.get("/api/system-status")
async def get_system_status():
    try:
        conn = sqlite3.connect("abs_navigator.db")
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM ABS_Deals")
        deals_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM EnhancedExtractionResults")
        extractions_count = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            "extraction_status": "Enhanced",
            "database_status": "Active",
            "deals_count": deals_count,
            "extractions_count": extractions_count,
            "surveillance_count": 0,
            "formats_supported": ["Text", "PDF", "Word", "Excel"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/process-document")
async def process_document(file: UploadFile = File(...)):
    try:
        content = await file.read()
        
        # Simple text processing for now
        if file.filename.lower().endswith('.txt'):
            text_content = content.decode('utf-8')
        else:
            text_content = f"File uploaded: {file.filename} ({len(content)} bytes)"
        
        # Simple extraction
        extracted_data = {
            "deal_name": "Sample Deal",
            "issuer": "Sample Issuer", 
            "file_size": len(content),
            "file_type": file.content_type
        }
        
        # Save to database
        extraction_id = f"EXTRACT_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        conn = sqlite3.connect("abs_navigator.db")
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO EnhancedExtractionResults 
            (id, filename, document_type, extracted_data, extraction_time, success, confidence_score)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            extraction_id, file.filename, "DOCUMENT", json.dumps(extracted_data),
            datetime.now().isoformat(), True, 0.85
        ))
        conn.commit()
        conn.close()
        
        return {
            "success": True,
            "filename": file.filename,
            "document_type": "DOCUMENT",
            "confidence": 0.85,
            "extraction_id": extraction_id,
            "extracted_data": extracted_data,
            "method": "basic"
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "filename": file.filename
        }

@app.get("/api/extraction-history")
async def get_extraction_history():
    try:
        conn = sqlite3.connect("abs_navigator.db")
        df = pd.read_sql_query("""
            SELECT id, filename, document_type, extraction_time, success, confidence_score
            FROM EnhancedExtractionResults 
            ORDER BY extraction_time DESC LIMIT 20
        """, conn)
        conn.close()
        
        return df.to_dict('records')
    except Exception as e:
        return []

@app.get("/api/database-tables")
async def get_database_tables():
    try:
        conn = sqlite3.connect("abs_navigator.db")
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = cursor.fetchall()
        
        table_info = {}
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            table_info[table_name] = count
        
        conn.close()
        return table_info
    except Exception as e:
        return {}

@app.post("/api/sql-query")
async def execute_sql_query(query_data: dict):
    try:
        query = query_data.get("query", "")
        
        # Basic safety check
        dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER']
        if any(keyword in query.upper() for keyword in dangerous_keywords):
            return {"success": False, "error": "Dangerous query detected"}
        
        conn = sqlite3.connect("abs_navigator.db")
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return {
            "success": True,
            "data": df.to_dict('records'),
            "columns": df.columns.tolist(),
            "row_count": len(df)
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/sample-data")
async def get_sample_data():
    return {
        "sample_text": """Executive Summary
This report summarizes KBRA's analysis of PEAC Solutions Receivables 2025-1, LLC (PEAC 2025-1), an equipment ABS transaction. This report is based on information as of February 11, 2025.

The aggregate securitization value is .63 million. PEAC 2025-1 will issue five classes of notes, including a short-term tranche."""
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)

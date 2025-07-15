# -*- coding: utf-8 -*-
"""
Created on Mon Jul 14 11:36:24 2025

@author: edfit
"""

import streamlit as st
import pandas as pd
from datetime import datetime

def create_filter_interface():
    """Create a proper filter interface with correct data types"""
    
    st.title("🔧 Filter Table Test")
    st.markdown("Testing proper data type handling for filters")
    
    # Separate filter sections by data type
    st.subheader("Text Filters")
    
    # Text-based filters
    text_filters = pd.DataFrame({
        "Filter": ["Deal Type", "Asset Class", "Rating Tier"],
        "Value": ["", "", ""],
        "Enabled": [False, False, False]
    })
    
    edited_text_filters = st.data_editor(
        text_filters,
        use_container_width=True,
        hide_index=True,
        key="text_filters",
        column_config={
            "Filter": st.column_config.TextColumn("Filter", disabled=True),
            "Value": st.column_config.SelectboxColumn(
                "Value",
                options=["AUTOS", "CREDIT CARD", "EQUIPMENT", "STUDENT LOAN", "Auto Loan", "Credit Card", "Equipment", "AAA", "AA", "A", "BBB"],
                help="Select filter value"
            ),
            "Enabled": st.column_config.CheckboxColumn("Enabled")
        }
    )
    
    st.subheader("Numeric Filters")
    
    # Numeric filters
    numeric_filters = pd.DataFrame({
        "Filter": ["Min Amount", "Max Amount", "Min Yield", "Max Yield"],
        "Value": [0.0, 0.0, 0.0, 0.0],
        "Enabled": [False, False, False, False]
    })
    
    edited_numeric_filters = st.data_editor(
        numeric_filters,
        use_container_width=True,
        hide_index=True,
        key="numeric_filters",
        column_config={
            "Filter": st.column_config.TextColumn("Filter", disabled=True),
            "Value": st.column_config.NumberColumn(
                "Value",
                min_value=0.0,
                format="%.2f",
                help="Enter numeric value"
            ),
            "Enabled": st.column_config.CheckboxColumn("Enabled")
        }
    )
    
    st.subheader("Date Filters")
    
    # Date filters
    date_filters = pd.DataFrame({
        "Filter": ["Start Date", "End Date"],
        "Value": [datetime.now().strftime("%Y-%m-%d"), datetime.now().strftime("%Y-%m-%d")],
        "Enabled": [False, False]
    })
    
    edited_date_filters = st.data_editor(
        date_filters,
        use_container_width=True,
        hide_index=True,
        key="date_filters",
        column_config={
            "Filter": st.column_config.TextColumn("Filter", disabled=True),
            "Value": st.column_config.DateColumn(
                "Value",
                format="YYYY-MM-DD",
                help="Select date in YYYY-MM-DD format"
            ),
            "Enabled": st.column_config.CheckboxColumn("Enabled")
        }
    )
    
    # Apply filters button
    if st.button("🔍 Apply Filters", key="apply_test_filters"):
        st.subheader("Filter Results")
        
        # Process text filters
        text_results = {}
        for _, row in edited_text_filters.iterrows():
            if row['Enabled'] and row['Value']:
                text_results[row['Filter']] = row['Value']
        
        # Process numeric filters
        numeric_results = {}
        for _, row in edited_numeric_filters.iterrows():
            if row['Enabled'] and row['Value'] > 0:
                numeric_results[row['Filter']] = row['Value']
        
        # Process date filters
        date_results = {}
        for _, row in edited_date_filters.iterrows():
            if row['Enabled']:
                date_results[row['Filter']] = row['Value']
        
        # Display results
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.write("**Text Filters:**")
            for key, value in text_results.items():
                st.write(f"- {key}: {value}")
        
        with col2:
            st.write("**Numeric Filters:**")
            for key, value in numeric_results.items():
                st.write(f"- {key}: {value:,.2f}")
        
        with col3:
            st.write("**Date Filters:**")
            for key, value in date_results.items():
                if isinstance(value, str):
                    st.write(f"- {key}: {value}")
                else:
                    st.write(f"- {key}: {value.strftime('%Y-%m-%d')}")
        
        # Build filter dictionary
        all_filters = {**text_results, **numeric_results, **date_results}
        
        st.subheader("Combined Filter Dictionary")
        st.json(all_filters)


def create_alternative_single_table():
    """Alternative approach with proper type handling in single table"""
    
    st.subheader("Alternative: Single Table with Type Conversion")
    
    # Create filter data with consistent string types
    filter_data = pd.DataFrame({
        "Filter": ["Deal Type", "Asset Class", "Rating Tier", "Min Amount", "Max Amount"],
        "Type": ["text", "text", "text", "number", "number"],
        "Value": ["", "", "", "", ""],  # All strings initially
        "Enabled": [False, False, False, False, False]
    })
    
    edited_filters = st.data_editor(
        filter_data,
        use_container_width=True,
        hide_index=True,
        key="single_table_filters",
        column_config={
            "Filter": st.column_config.TextColumn("Filter", disabled=True),
            "Type": st.column_config.TextColumn("Type", disabled=True),
            "Value": st.column_config.TextColumn("Value", help="Enter value (numbers as text)"),
            "Enabled": st.column_config.CheckboxColumn("Enabled")
        }
    )
    
    if st.button("🔍 Apply Single Table Filters", key="apply_single_filters"):
        filters_dict = {}
        
        for _, row in edited_filters.iterrows():
            if row['Enabled'] and row['Value'].strip():
                filter_name = row['Filter']
                value = row['Value'].strip()
                filter_type = row['Type']
                
                # Convert based on type
                if filter_type == "number":
                    try:
                        filters_dict[filter_name] = float(value)
                    except ValueError:
                        st.error(f"Invalid number for {filter_name}: {value}")
                        continue
                else:
                    filters_dict[filter_name] = value
        
        st.subheader("Processed Filters")
        st.json(filters_dict)


def test_date_formatting():
    """Test proper date formatting"""
    
    st.subheader("Date Format Testing")
    
    # Test different date inputs
    test_dates = [
        "2025-07-11",
        "07/11/2025", 
        "7/11/25",
        "July 11, 2025",
        datetime(2025, 7, 11)
    ]
    
    st.write("**Date Conversion Test:**")
    
    for date_input in test_dates:
        try:
            if isinstance(date_input, str):
                converted = pd.to_datetime(date_input, format='%Y-%m-%d', errors='coerce')
                if pd.isna(converted):
                    # Try without format for flexibility
                    converted = pd.to_datetime(date_input, errors='coerce')
                
                if not pd.isna(converted):
                    formatted = converted.strftime('%Y-%m-%d')
                    st.write(f"✅ {date_input} → {formatted}")
                else:
                    st.write(f"❌ {date_input} → Failed to convert")
            else:
                formatted = date_input.strftime('%Y-%m-%d')
                st.write(f"✅ {date_input} → {formatted}")
        except Exception as e:
            st.write(f"❌ {date_input} → Error: {str(e)}")


def main():
    st.set_page_config(
        page_title="Filter Table Test",
        page_icon="🔧",
        layout="wide"
    )
    
    st.markdown("---")
    
    # Test different approaches
    tab1, tab2, tab3 = st.tabs(["Separate Tables", "Single Table", "Date Testing"])
    
    with tab1:
        create_filter_interface()
    
    with tab2:
        create_alternative_single_table()
    
    with tab3:
        test_date_formatting()


if __name__ == "__main__":
    main()
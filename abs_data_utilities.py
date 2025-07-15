# -*- coding: utf-8 -*-
"""
Created on Fri Jul 11 11:53:01 2025

@author: edfit
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Tuple, Any
import logging
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

class ABSDataValidator:
    """
    Validation and quality check utilities for ABS data.
    """
    
    def __init__(self, dataframes: Dict[str, pd.DataFrame]):
        self.dataframes = dataframes
        self.validation_results = {}
    
    def run_all_validations(self) -> Dict[str, Any]:
        """Run comprehensive validation checks on all dataframes."""
        logger.info("Starting comprehensive data validation...")
        
        self.validation_results = {
            'data_completeness': self._check_data_completeness(),
            'data_consistency': self._check_data_consistency(),
            'range_validation': self._check_data_ranges(),
            'duplicate_check': self._check_duplicates(),
            'relationship_validation': self._check_relationships(),
            'temporal_validation': self._check_temporal_consistency()
        }
        
        return self.validation_results
    
    def _check_data_completeness(self) -> Dict[str, Any]:
        """Check for missing data across all dataframes."""
        completeness = {}
        
        for name, df in self.dataframes.items():
            if df.empty:
                completeness[name] = {"status": "EMPTY", "missing_pct": 100}
                continue
            
            missing_data = df.isnull().sum()
            total_cells = len(df) * len(df.columns)
            missing_pct = (missing_data.sum() / total_cells) * 100
            
            completeness[name] = {
                "status": "GOOD" if missing_pct < 10 else "WARNING" if missing_pct < 25 else "POOR",
                "missing_pct": round(missing_pct, 2),
                "missing_by_column": missing_data[missing_data > 0].to_dict(),
                "total_rows": len(df)
            }
        
        return completeness
    
    def _check_data_consistency(self) -> Dict[str, Any]:
        """Check for data consistency across dataframes."""
        consistency = {}
        
        # Check if deal names are consistent across dataframes
        deal_names_by_df = {}
        for name, df in self.dataframes.items():
            if 'deal_short_name' in df.columns:
                deal_names_by_df[name] = set(df['deal_short_name'].dropna().unique())
        
        if len(deal_names_by_df) > 1:
            all_deals = set.union(*deal_names_by_df.values())
            consistency['deal_name_consistency'] = {
                'all_deals': list(all_deals),
                'deals_by_dataframe': {k: list(v) for k, v in deal_names_by_df.items()},
                'missing_deals': {
                    df_name: list(all_deals - deals) 
                    for df_name, deals in deal_names_by_df.items()
                }
            }
        
        # Check issuer consistency
        issuer_names_by_df = {}
        for name, df in self.dataframes.items():
            if 'issuer_name' in df.columns:
                issuer_names_by_df[name] = set(df['issuer_name'].dropna().unique())
        
        if issuer_names_by_df:
            consistency['issuer_consistency'] = {
                df_name: list(issuers) for df_name, issuers in issuer_names_by_df.items()
            }
        
        return consistency
    
    def _check_data_ranges(self) -> Dict[str, Any]:
        """Validate that numeric data falls within expected ranges."""
        range_validation = {}
        
        # Define expected ranges for common metrics
        expected_ranges = {
            'pool_factor': (0, 1),
            'delinquency_30_plus': (0, 1),
            'delinquency_60_plus': (0, 1),
            'delinquency_90_plus': (0, 1),
            'cnl_rate': (0, 1),
            'extensions_rate': (0, 1),
            'wa_interest_rate_closing': (0, 1),
            'wa_interest_rate_current': (0, 1),
            'oc_current': (0, 10),  # Allowing for high OC ratios
            'reserve_pct_original': (0, 1),
            'reserve_pct_current': (0, 1)
        }
        
        for df_name, df in self.dataframes.items():
            df_validation = {}
            
            for column in df.columns:
                if column in expected_ranges and df[column].dtype in ['float64', 'int64']:
                    min_val, max_val = expected_ranges[column]
                    out_of_range = df[(df[column] < min_val) | (df[column] > max_val)]
                    
                    df_validation[column] = {
                        'expected_range': expected_ranges[column],
                        'actual_range': (df[column].min(), df[column].max()),
                        'out_of_range_count': len(out_of_range),
                        'out_of_range_values': out_of_range[['deal_short_name', column]].to_dict('records') if len(out_of_range) > 0 else []
                    }
            
            if df_validation:
                range_validation[df_name] = df_validation
        
        return range_validation
    
    def _check_duplicates(self) -> Dict[str, Any]:
        """Check for duplicate records."""
        duplicate_check = {}
        
        for name, df in self.dataframes.items():
            if df.empty:
                continue
            
            # Check for complete duplicates
            complete_duplicates = df.duplicated().sum()
            
            # Check for duplicates based on key columns
            key_columns = ['deal_short_name', 'as_of_date']
            if all(col in df.columns for col in key_columns):
                key_duplicates = df.duplicated(subset=key_columns).sum()
            else:
                key_duplicates = 0
            
            duplicate_check[name] = {
                'complete_duplicates': complete_duplicates,
                'key_duplicates': key_duplicates,
                'total_rows': len(df)
            }
        
        return duplicate_check
    
    def _check_relationships(self) -> Dict[str, Any]:
        """Check relationships between dataframes."""
        relationships = {}
        
        # Check if performance metrics align with deal master
        if 'deal_master' in self.dataframes and 'performance_metrics' in self.dataframes:
            master_deals = set(self.dataframes['deal_master']['deal_short_name'].dropna())
            perf_deals = set(self.dataframes['performance_metrics']['deal_short_name'].dropna())
            
            relationships['master_vs_performance'] = {
                'deals_in_master_only': list(master_deals - perf_deals),
                'deals_in_performance_only': list(perf_deals - master_deals),
                'common_deals': list(master_deals & perf_deals)
            }
        
        return relationships
    
    def _check_temporal_consistency(self) -> Dict[str, Any]:
        """Check temporal consistency in the data."""
        temporal = {}
        
        for name, df in self.dataframes.items():
            if 'as_of_date' in df.columns:
                dates = pd.to_datetime(df['as_of_date'], errors='coerce').dropna()
                
                if len(dates) > 0:
                    temporal[name] = {
                        'date_range': (dates.min().strftime('%Y-%m-%d'), dates.max().strftime('%Y-%m-%d')),
                        'unique_dates': len(dates.unique()),
                        'future_dates': len(dates[dates > datetime.now()]),
                        'null_dates': df['as_of_date'].isnull().sum()
                    }
        
        return temporal
    
    def generate_validation_report(self) -> str:
        """Generate a comprehensive validation report."""
        if not self.validation_results:
            self.run_all_validations()
        
        report = ["ABS DATA VALIDATION REPORT", "=" * 50, ""]
        
        # Data Completeness
        report.append("DATA COMPLETENESS:")
        for df_name, results in self.validation_results['data_completeness'].items():
            status = results['status']
            missing_pct = results['missing_pct']
            report.append(f"  {df_name}: {status} ({missing_pct}% missing)")
        
        report.append("")
        
        # Data Consistency
        report.append("DATA CONSISTENCY:")
        if 'deal_name_consistency' in self.validation_results['data_consistency']:
            deal_consistency = self.validation_results['data_consistency']['deal_name_consistency']
            report.append(f"  Total unique deals found: {len(deal_consistency['all_deals'])}")
            report.append(f"  Deals: {', '.join(deal_consistency['all_deals'])}")
        
        report.append("")
        
        # Range Validation
        report.append("RANGE VALIDATION:")
        range_issues = 0
        for df_name, validations in self.validation_results['range_validation'].items():
            for column, validation in validations.items():
                if validation['out_of_range_count'] > 0:
                    range_issues += validation['out_of_range_count']
                    report.append(f"  {df_name}.{column}: {validation['out_of_range_count']} values out of range")
        
        if range_issues == 0:
            report.append("  No range validation issues found.")
        
        report.append("")
        
        # Duplicates
        report.append("DUPLICATE CHECK:")
        total_duplicates = 0
        for df_name, dup_info in self.validation_results['duplicate_check'].items():
            total_duplicates += dup_info['complete_duplicates']
            if dup_info['complete_duplicates'] > 0:
                report.append(f"  {df_name}: {dup_info['complete_duplicates']} complete duplicates")
        
        if total_duplicates == 0:
            report.append("  No duplicates found.")
        
        return "\n".join(report)

class ABSDataAnalyzer:
    """
    Analysis and visualization utilities for ABS data.
    """
    
    def __init__(self, dataframes: Dict[str, pd.DataFrame]):
        self.dataframes = dataframes
    
    def create_performance_dashboard(self) -> None:
        """Create a comprehensive performance dashboard."""
        if 'performance_metrics' not in self.dataframes:
            logger.warning("Performance metrics data not available for dashboard")
            return
        
        df = self.dataframes['performance_metrics']
        
        # Set up the plot
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('ABS Performance Dashboard', fontsize=16, fontweight='bold')
        
        # 1. Delinquency Rates by Deal
        if all(col in df.columns for col in ['deal_short_name', 'delinquency_30_plus']):
            ax1 = axes[0, 0]
            delinq_data = df[['deal_short_name', 'delinquency_30_plus', 'delinquency_60_plus', 'delinquency_90_plus']].dropna()
            
            if not delinq_data.empty:
                x = range(len(delinq_data))
                width = 0.25
                
                ax1.bar([i - width for i in x], delinq_data['delinquency_30_plus'], width, label='30+ DQ', alpha=0.8)
                ax1.bar(x, delinq_data['delinquency_60_plus'], width, label='60+ DQ', alpha=0.8)
                ax1.bar([i + width for i in x], delinq_data['delinquency_90_plus'], width, label='90+ DQ', alpha=0.8)
                
                ax1.set_xlabel('Deals')
                ax1.set_ylabel('Delinquency Rate')
                ax1.set_title('Delinquency Rates by Deal')
                ax1.set_xticks(x)
                ax1.set_xticklabels(delinq_data['deal_short_name'], rotation=45, ha='right')
                ax1.legend()
                ax1.grid(True, alpha=0.3)
        
        # 2. CNL Rates by Issuer
        if all(col in df.columns for col in ['issuer_name', 'cnl_rate']):
            ax2 = axes[0, 1]
            cnl_data = df[['issuer_name', 'cnl_rate']].dropna()
            
            if not cnl_data.empty:
                cnl_by_issuer = cnl_data.groupby('issuer_name')['cnl_rate'].mean()
                cnl_by_issuer.plot(kind='bar', ax=ax2, color=['#1f77b4', '#ff7f0e'])
                ax2.set_title('Average CNL Rate by Issuer')
                ax2.set_ylabel('CNL Rate')
                ax2.tick_params(axis='x', rotation=0)
                ax2.grid(True, alpha=0.3)
        
        # 3. Pool Factor Distribution
        if 'pool_factor' in df.columns:
            ax3 = axes[1, 0]
            pool_factors = df['pool_factor'].dropna()
            
            if not pool_factors.empty:
                ax3.hist(pool_factors, bins=20, alpha=0.7, color='skyblue', edgecolor='black')
                ax3.set_xlabel('Pool Factor')
                ax3.set_ylabel('Frequency')
                ax3.set_title('Pool Factor Distribution')
                ax3.grid(True, alpha=0.3)
        
        # 4. Overcollateralization vs CNL
        if all(col in df.columns for col in ['oc_current', 'cnl_rate']):
            ax4 = axes[1, 1]
            oc_cnl_data = df[['oc_current', 'cnl_rate', 'issuer_name']].dropna()
            
            if not oc_cnl_data.empty:
                colors = ['red' if issuer == 'Carvana' else 'blue' for issuer in oc_cnl_data['issuer_name']]
                ax4.scatter(oc_cnl_data['cnl_rate'], oc_cnl_data['oc_current'], c=colors, alpha=0.6)
                ax4.set_xlabel('CNL Rate')
                ax4.set_ylabel('Overcollateralization Ratio')
                ax4.set_title('Overcollateralization vs CNL Rate')
                ax4.grid(True, alpha=0.3)
                
                # Add legend
                from matplotlib.patches import Patch
                legend_elements = [Patch(facecolor='blue', label='Octane'),
                                 Patch(facecolor='red', label='Carvana')]
                ax4.legend(handles=legend_elements)
        
        plt.tight_layout()
        plt.show()
    
    def create_historical_trends(self) -> None:
        """Create historical performance trend charts."""
        if 'historical_performance' not in self.dataframes:
            logger.warning("Historical performance data not available")
            return
        
        df = self.dataframes['historical_performance']
        
        if df.empty:
            logger.warning("Historical performance dataframe is empty")
            return
        
        # Group by metric type and create separate charts
        metric_types = df['metric_type'].unique()
        
        fig, axes = plt.subplots(len(metric_types), 1, figsize=(12, 4 * len(metric_types)))
        if len(metric_types) == 1:
            axes = [axes]
        
        fig.suptitle('Historical Performance Trends', fontsize=16, fontweight='bold')
        
        for i, metric_type in enumerate(metric_types):
            ax = axes[i]
            metric_data = df[df['metric_type'] == metric_type]
            
            # Pivot data for plotting
            pivot_data = metric_data.pivot_table(
                index='period_offset', 
                columns='deal_short_name', 
                values='metric_value'
            )
            
            # Plot lines for each deal
            for deal in pivot_data.columns:
                deal_data = pivot_data[deal].dropna()
                if not deal_data.empty:
                    ax.plot(deal_data.index, deal_data.values, marker='o', label=deal, alpha=0.7)
            
            ax.set_title(f'{metric_type} Historical Trends')
            ax.set_xlabel('Period')
            ax.set_ylabel('Rate')
            ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
            ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.show()
    
    def generate_summary_statistics(self) -> Dict[str, pd.DataFrame]:
        """Generate summary statistics for all dataframes."""
        summaries = {}
        
        for name, df in self.dataframes.items():
            if df.empty:
                continue
            
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                summary = df[numeric_cols].describe()
                summaries[name] = summary
        
        return summaries
    
    def create_deal_comparison_report(self) -> pd.DataFrame:
        """Create a comprehensive deal comparison report."""
        if 'deal_master' not in self.dataframes:
            logger.warning("Deal master data not available for comparison")
            return pd.DataFrame()
        
        # Start with deal master data
        comparison = self.dataframes['deal_master'].copy()
        
        # Add performance metrics
        if 'performance_metrics' in self.dataframes:
            perf_cols = ['deal_short_name', 'delinquency_30_plus', 'delinquency_60_plus', 
                        'delinquency_90_plus', 'cnl_rate', 'extensions_rate']
            perf_data = self.dataframes['performance_metrics'][perf_cols].drop_duplicates()
            comparison = comparison.merge(perf_data, on='deal_short_name', how='left')
        
        # Add collateral characteristics
        if 'collateral_characteristics' in self.dataframes:
            collat_cols = ['deal_short_name', 'wa_interest_rate_current', 'wa_remaining_term_current']
            collat_data = self.dataframes['collateral_characteristics'][collat_cols].drop_duplicates()
            comparison = comparison.merge(collat_data, on='deal_short_name', how='left')
        
        return comparison

def create_database_export(dataframes: Dict[str, pd.DataFrame], 
                          connection_string: str = None) -> None:
    """
    Export dataframes to database tables.
    
    Args:
        dataframes: Dictionary of dataframes to export
        connection_string: Database connection string (SQLite by default)
    """
    import sqlite3
    from sqlalchemy import create_engine
    
    if connection_string is None:
        # Default to SQLite
        connection_string = 'sqlite:///abs_performance_data.db'
    
    engine = create_engine(connection_string)
    
    try:
        for name, df in dataframes.items():
            if not df.empty:
                df.to_sql(name, engine, if_exists='replace', index=False)
                logger.info(f"Exported {len(df)} rows to table '{name}'")
        
        logger.info(f"All data successfully exported to database: {connection_string}")
        
    except Exception as e:
        logger.error(f"Error exporting to database: {str(e)}")
        raise

def main_analysis_example():
    """
    Example of how to use the analysis utilities.
    """
    from abs_data_extractor import ABSDataExtractor
    
    # Initialize extractor and get data
    extractor = ABSDataExtractor()
    dataframes = extractor.extract_from_files(
        "Octane Receivables Trust Comprehensive Surveillance Dashboard.xlsx",
        "Carvana Auto Receivables Trust Comprehensive Surveillance Dashboard.xlsx"
    )
    
    # Validate data
    validator = ABSDataValidator(dataframes)
    validation_report = validator.generate_validation_report()
    print(validation_report)
    
    # Analyze data
    analyzer = ABSDataAnalyzer(dataframes)
    
    # Create visualizations
    analyzer.create_performance_dashboard()
    analyzer.create_historical_trends()
    
    # Generate summary statistics
    summaries = analyzer.generate_summary_statistics()
    for name, summary in summaries.items():
        print(f"\n{name.upper()} SUMMARY STATISTICS:")
        print(summary)
    
    # Create deal comparison
    comparison = analyzer.create_deal_comparison_report()
    print("\nDEAL COMPARISON REPORT:")
    print(comparison.to_string())
    
    # Export to database
    create_database_export(dataframes)

if __name__ == "__main__":
    main_analysis_example()
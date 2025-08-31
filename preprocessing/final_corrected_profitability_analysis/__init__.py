"""
Final Corrected Profitability Analysis Module

This module contains all functions specifically for the 
"FINAL CORRECTED PROFITABILITY ANALYSIS - FIXES ALL COLUMN CONFLICTS" section
from the new.ipynb notebook.

This implementation focuses on:
- Loading and preprocessing base Uber trip data
- Integrating spatial, temporal, and weather features 
- Handling column conflicts and data quality issues
- Comprehensive profitability dataset creation and testing

Functions:
- load_base_uber_data_for_profitability_corrected: Base data loading
- load_and_integrate_all_features_final: Complete feature integration
- save_comprehensive_profitability_dataset_final: Dataset saving with deduplication
- test_enhanced_profitability_pipeline_final: Pipeline testing and validation
"""

from .data_pipeline import (
    load_base_uber_data_for_profitability_corrected,
    load_and_integrate_all_features_final,
    save_comprehensive_profitability_dataset_final,
    test_enhanced_profitability_pipeline_final,
    process_full_profitability_dataset,
    process_all_months_profitability_batch,
    get_spark_session,
    validate_data_availability
)

__version__ = "1.0.0"
__author__ = "Profitability Analysis Pipeline"

__all__ = [
    'load_base_uber_data_for_profitability_corrected',
    'load_and_integrate_all_features_final',
    'save_comprehensive_profitability_dataset_final',
    'test_enhanced_profitability_pipeline_final',
    'process_full_profitability_dataset',
    'process_all_months_profitability_batch',
    'get_spark_session',
    'validate_data_availability'
]

"""
Preprocessing package for Uber profitability analysis.

This package contains data loading and preprocessing functions for the
Uber profitability analysis pipeline.
"""

# Import from data pipeline
from .final_corrected_profitability_analysis import (
    load_base_uber_data_for_profitability_corrected,
    load_and_integrate_all_features_final,
    save_comprehensive_profitability_dataset_final,
    test_enhanced_profitability_pipeline_final,
    process_full_profitability_dataset,
    process_all_months_profitability_batch,
    get_spark_session,
    validate_data_availability
)

# Import from feature filtering
from .filtering_features import (
    quick_correlation_analysis,
    plot_correlation_matrix,
    basic_feature_screening
)

__version__ = "1.0.0"

__all__ = [
    # Data pipeline functions
    'load_base_uber_data_for_profitability_corrected',
    'load_and_integrate_all_features_final',
    'save_comprehensive_profitability_dataset_final',
    'test_enhanced_profitability_pipeline_final',
    'process_full_profitability_dataset',
    'process_all_months_profitability_batch',
    'get_spark_session',
    'validate_data_availability',
    
    # Feature filtering functions
    'quick_correlation_analysis',
    'plot_correlation_matrix',
    'basic_feature_screening'
]
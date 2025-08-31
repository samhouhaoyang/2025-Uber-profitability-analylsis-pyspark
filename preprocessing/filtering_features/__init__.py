"""
Simple Feature Filtering Module

Basic correlation analysis and feature screening before model training.
"""

from .correlation_analysis import (
    analyze_feature_correlations,
    plot_correlation_matrix,
    suggest_features_to_remove,
    get_common_redundant_features,
    quick_correlation_analysis
)

from .feature_selection import (
    get_features_to_remove_before_modeling,
    remove_features_from_dataframe,
    basic_feature_screening
)

from .data_joining import (
    check_external_data_availability,
    join_all_external_data,
    create_enhanced_monthly_dataset,
)

__all__ = [
    # Correlation analysis
    'analyze_feature_correlations',
    'plot_correlation_matrix',
    'suggest_features_to_remove',
    'get_common_redundant_features',
    'quick_correlation_analysis',
    
    # Feature selection
    'get_features_to_remove_before_modeling',
    'remove_features_from_dataframe',
    'basic_feature_screening',
    
    # Data joining
    'check_external_data_availability',
    'join_all_external_data', 
    'create_enhanced_monthly_dataset',
]
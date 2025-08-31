# Feature Filtering Module

Simple feature correlation analysis and basic screening before model training.

## Purpose

This module provides **preliminary screening tools** for features, not complete feature selection. It helps identify:
- Highly correlated features that might be redundant
- Common features that should be removed before modeling
- Basic visualizations of feature relationships

## Quick Usage

```python
from preprocessing.filtering_features import quick_correlation_analysis, plot_correlation_matrix

# Quick analysis
results = quick_correlation_analysis(df, threshold=0.8)

# Plot correlation matrix  
correlation_matrix, _ = analyze_feature_correlations(df)
plot_correlation_matrix(correlation_matrix)

# Basic feature screening
from preprocessing.filtering_features import basic_feature_screening
clean_df = basic_feature_screening(df)
```

## Main Functions

### `quick_correlation_analysis(df, threshold=0.8)`
- One-function analysis of correlations
- Returns summary with suggestions

### `plot_correlation_matrix(correlation_matrix)`
- Simple heatmap visualization
- Shows feature correlations

### `basic_feature_screening(df)`
- Removes common problematic features
- Returns cleaned DataFrame

## Note

This is **preliminary screening only**. Always validate suggestions with domain knowledge before final feature selection.
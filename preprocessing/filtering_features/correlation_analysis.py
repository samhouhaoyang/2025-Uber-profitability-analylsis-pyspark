"""
Feature Correlation Analysis Module

Simple preliminary feature screening before model training.
This is NOT complete feature selection, just initial screening for reference.
"""

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Set, Tuple, Optional
import matplotlib.pyplot as plt
import seaborn as sns


def analyze_feature_correlations(
    df: DataFrame, 
    correlation_threshold: float = 0.9,
    sample_fraction: float = 0.01
) -> Tuple[Optional[pd.DataFrame], List[Tuple[str, str, float]]]:
    """
    Analyze feature correlations and find highly correlated pairs.
    
    Args:
        df: Spark DataFrame
        correlation_threshold: Correlation threshold (default 0.9)
        sample_fraction: Sample fraction for faster computation (default 0.01)
        
    Returns:
        correlation_matrix: Pandas correlation matrix
        high_corr_pairs: List of (feature1, feature2, correlation) tuples
    """
    print(f"🔍 Analyzing feature correlations (threshold >= {correlation_threshold})")
    
    # Get numeric columns
    numeric_columns = []
    for col_name, col_type in df.dtypes:
        if col_type in ['int', 'bigint', 'float', 'double', 'decimal']:
            numeric_columns.append(col_name)
    
    print(f"📊 Found {len(numeric_columns)} numeric features")
    
    if len(numeric_columns) < 2:
        print("❌ Too few numeric features")
        return None, []
    
    # Sample data for faster computation
    if sample_fraction < 1.0:
        sampled_df = df.sample(fraction=sample_fraction, seed=42)
        print(f"🎯 Using {sample_fraction*100:.1f}% sample")
    else:
        sampled_df = df
    
    try:
        # Convert to pandas and calculate correlation
        pandas_df = sampled_df.select(numeric_columns).toPandas()
        correlation_matrix = pandas_df.corr()
        
        print(f"✅ Correlation matrix: {len(pandas_df):,} rows, {len(pandas_df.columns)} columns")
        
        # Find highly correlated pairs
        high_corr_pairs = []
        for i in range(len(correlation_matrix.columns)):
            for j in range(i+1, len(correlation_matrix.columns)):
                corr_value = correlation_matrix.iloc[i, j]
                if abs(corr_value) >= correlation_threshold:
                    col1 = correlation_matrix.columns[i]
                    col2 = correlation_matrix.columns[j]
                    high_corr_pairs.append((col1, col2, corr_value))
        
        high_corr_pairs.sort(key=lambda x: abs(x[2]), reverse=True)
        print(f"🎯 Found {len(high_corr_pairs)} highly correlated pairs")
        
        return correlation_matrix, high_corr_pairs
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None, []


def plot_correlation_matrix(correlation_matrix: pd.DataFrame, figsize: tuple = (10, 8)):
    """
    Plot correlation matrix as heatmap.
    
    Args:
        correlation_matrix: Pandas correlation matrix
        figsize: Figure size (width, height)
    """
    if correlation_matrix is None or correlation_matrix.empty:
        print("❌ No correlation matrix to plot")
        return
    
    print("📊 Creating correlation heatmap...")
    
    plt.figure(figsize=figsize)
    
    # Create heatmap
    sns.heatmap(
        correlation_matrix,
        annot=True,
        cmap='coolwarm',
        center=0,
        square=True,
        fmt='.2f',
        cbar_kws={"shrink": .8}
    )
    
    plt.title('Feature Correlation Matrix\n(Preliminary Screening)', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.show()
    
    # Print summary
    corr_values = correlation_matrix.values
    high_corr_count = sum(1 for i in range(len(corr_values)) 
                         for j in range(i+1, len(corr_values)) 
                         if abs(corr_values[i,j]) >= 0.7)
    
    print(f"📈 Summary: {high_corr_count} pairs with |correlation| >= 0.7")


def suggest_features_to_remove(high_corr_pairs: List[Tuple[str, str, float]]) -> Set[str]:
    """
    Simple suggestions for feature removal based on correlation.
    
    Args:
        high_corr_pairs: List of highly correlated feature pairs
        
    Returns:
        Set of features suggested for removal
    """
    if not high_corr_pairs:
        print("✅ No highly correlated features found")
        return set()
    
    print(f"🔍 Analyzing {len(high_corr_pairs)} correlated pairs")
    
    features_to_remove = set()
    
    for col1, col2, corr in high_corr_pairs:
        print(f"📊 {col1} ↔ {col2}: {corr:.3f}")
        
        # Simple rules: remove derived features
        if 'revenue_per' in col2:
            features_to_remove.add(col2)
            print(f"  🗑️ Suggest removing {col2} (derived feature)")
        elif 'revenue_per' in col1:
            features_to_remove.add(col1)
            print(f"  🗑️ Suggest removing {col1} (derived feature)")
        elif 'duration_minutes' in col2 and 'trip_time' in col1:
            features_to_remove.add(col2)
            print(f"  🗑️ Suggest removing {col2} (unit conversion)")
        elif 'avg_speed' in col2:
            features_to_remove.add(col2)
            print(f"  🗑️ Suggest removing {col2} (computable)")
        else:
            # Default: remove second feature
            features_to_remove.add(col2)
            print(f"  🗑️ Suggest removing {col2} (default)")
    
    print(f"\n📋 Total suggestions: {len(features_to_remove)} features")
    if features_to_remove:
        print(f"Suggested removals: {sorted(list(features_to_remove))}")
    
    return features_to_remove


def get_common_redundant_features() -> Set[str]:
    """
    Return commonly redundant features based on domain knowledge.
    """
    return {
        'trip_duration_minutes',  # same as trip_time
        'avg_speed_mph',         # computable from distance/time
        'revenue_per_mile',      # computable from fare/distance
        'revenue_per_minute',    # computable from fare/time
    }


def quick_correlation_analysis(df: DataFrame, threshold: float = 0.8) -> dict:
    """
    Quick correlation analysis with summary.
    
    Args:
        df: Spark DataFrame
        threshold: Correlation threshold
        
    Returns:
        Dictionary with analysis results
    """
    print("🚀 Quick correlation analysis")
    print("="*30)
    
    # Run analysis
    corr_matrix, high_corr_pairs = analyze_feature_correlations(df, threshold, 0.01)
    
    # Get suggestions
    correlation_suggestions = suggest_features_to_remove(high_corr_pairs)
    domain_suggestions = get_common_redundant_features()
    
    # Check which suggestions actually exist in data
    existing_columns = set(df.columns)
    valid_suggestions = correlation_suggestions.union(domain_suggestions).intersection(existing_columns)
    
    results = {
        'total_features': len(df.columns),
        'high_corr_pairs': len(high_corr_pairs),
        'suggested_removals': valid_suggestions,
        'features_after_removal': len(existing_columns) - len(valid_suggestions)
    }
    
    print(f"\n📈 Results:")
    print(f"  Original features: {results['total_features']}")
    print(f"  High correlations: {results['high_corr_pairs']} pairs")
    print(f"  Suggested removals: {len(results['suggested_removals'])}")
    print(f"  Features after removal: {results['features_after_removal']}")
    
    return results
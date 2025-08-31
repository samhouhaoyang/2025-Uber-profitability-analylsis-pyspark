"""
Simple Feature Selection Module

Basic feature screening before model training.
"""

from pyspark.sql import DataFrame
from typing import Set


def get_features_to_remove_before_modeling() -> Set[str]:
    """
    Features that should typically be removed before modeling.
    
    Returns:
        Set of feature names to consider removing
    """
    return {
        # IDs and identifiers
        'hvfhs_license_num',
        'dispatching_base_num',
        'originating_base_num',
        
        # Raw timestamps (use engineered time features instead)
        'pickup_datetime',
        'dropoff_datetime',
        'request_datetime',
        
        # Potential data leakage
        'shared_request_flag',
        'shared_match_flag',
        'access_a_ride_flag',
        'wav_request_flag',
        
        # Fee components (if predicting total revenue)
        'tolls',
        'bcf',
        'sales_tax',
        'congestion_surcharge',
        'airport_fee',
        'tips',
        
        # Redundant derived features
        'trip_duration_minutes',  # same as trip_time
        'avg_speed_mph',         # computable
        'revenue_per_mile',      # computable
        'revenue_per_minute',    # computable
    }


def remove_features_from_dataframe(df: DataFrame, features_to_remove: Set[str]) -> DataFrame:
    """
    Remove specified features from DataFrame.
    
    Args:
        df: Input DataFrame
        features_to_remove: Set of feature names to remove
        
    Returns:
        DataFrame with features removed
    """
    existing_columns = set(df.columns)
    valid_removals = features_to_remove.intersection(existing_columns)
    invalid_removals = features_to_remove - existing_columns
    
    print(f"🗑️ Feature removal:")
    print(f"  Requested: {len(features_to_remove)}")
    print(f"  Valid: {len(valid_removals)}")
    print(f"  Not found: {len(invalid_removals)}")
    
    if valid_removals:
        print(f"  Removing: {sorted(list(valid_removals))}")
        columns_to_keep = [col for col in df.columns if col not in valid_removals]
        result_df = df.select(columns_to_keep)
        print(f"  Result: {len(df.columns)} → {len(result_df.columns)} features")
        return result_df
    else:
        print("  No features removed")
        return df


def basic_feature_screening(df: DataFrame) -> DataFrame:
    """
    Apply basic feature screening before modeling.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame after basic screening
    """
    print("🔍 Basic feature screening")
    print("="*25)
    
    # Get features to remove
    features_to_remove = get_features_to_remove_before_modeling()
    
    # Remove features
    cleaned_df = remove_features_from_dataframe(df, features_to_remove)
    
    print(f"✅ Screening complete")
    
    return cleaned_df
"""
Data Joining Module

This module handles joining external datasets (weather, spatial, temporal) 
to the main Uber profitability dataset on a monthly basis.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os
from typing import Optional, Dict, Tuple


def check_external_data_availability(year: int, month: int) -> Dict[str, bool]:
    """
    Check which external datasets are available for a specific year/month.
    
    Args:
        year: Year to check
        month: Month to check
        
    Returns:
        Dictionary indicating availability of each dataset type
    """
    base_path = "./lake/external_features"
    
    datasets = {
        'weather': f"{base_path}/weather/year={year}/month={month:02d}/",
        'spatial': f"{base_path}/spatial/year={year}/month={month:02d}/",
        'temporal': f"{base_path}/temporal/year={year}/month={month:02d}/"
    }
    
    availability = {}
    for dataset_type, path in datasets.items():
        availability[dataset_type] = os.path.exists(path)
        
    return availability


def load_external_dataset(dataset_type: str, year: int, month: int) -> Optional[DataFrame]:
    """
    Load an external dataset for a specific year/month.
    
    Args:
        dataset_type: Type of dataset ('weather', 'spatial', 'temporal')
        year: Year to load
        month: Month to load
        
    Returns:
        Spark DataFrame or None if not available
    """
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active Spark session found")
    
    base_path = "./lake/external_features"
    dataset_path = f"{base_path}/{dataset_type}/year={year}/month={month:02d}/"
    
    try:
        if os.path.exists(dataset_path):
            df = spark.read.parquet(dataset_path)
            print(f"✅ Loaded {dataset_type} data: {df.count():,} rows, {len(df.columns)} columns")
            return df
        else:
            print(f"⚠️ {dataset_type} data not found for {year}-{month:02d}")
            return None
            
    except Exception as e:
        print(f"❌ Error loading {dataset_type} data: {e}")
        return None


def join_weather_data(main_df: DataFrame, weather_df: DataFrame) -> DataFrame:
    """
    Join weather data to main dataset based on pickup date.
    
    Args:
        main_df: Main Uber trip dataset
        weather_df: Weather dataset
        
    Returns:
        DataFrame with weather features joined
    """
    print("🌤️ Joining weather data...")
    
    try:
        # Prepare main dataset with date column for joining
        main_with_date = main_df.withColumn(
            "pickup_date", 
            F.to_date(F.col("pickup_datetime"))
        )
        
        # Prepare weather dataset
        weather_prepared = weather_df.select([
            F.col("weather_date").alias("pickup_date"),
            *[col for col in weather_df.columns if col != "weather_date"]
        ])
        
        # Join on pickup date
        joined_df = main_with_date.join(
            weather_prepared,
            on="pickup_date",
            how="left"
        ).drop("pickup_date")
        
        print(f"✅ Weather join complete: {joined_df.count():,} rows")
        return joined_df
        
    except Exception as e:
        print(f"❌ Weather join failed: {e}")
        return main_df


def join_spatial_data(main_df: DataFrame, spatial_df: DataFrame) -> DataFrame:
    """
    Join spatial data to main dataset based on pickup and dropoff locations.
    
    Args:
        main_df: Main Uber trip dataset  
        spatial_df: Spatial features dataset
        
    Returns:
        DataFrame with spatial features joined
    """
    print("🗺️ Joining spatial data...")
    
    try:
        # Detect the location ID column name
        location_id_col = None
        possible_names = ["LocationID", "location_id", "zone_id", "id", "LOCATIONID"]
        
        for col_name in possible_names:
            if col_name in spatial_df.columns:
                location_id_col = col_name
                break
        
        if location_id_col is None:
            print(f"❌ No location ID column found in spatial data")
            print(f"Available columns: {spatial_df.columns}")
            return main_df
        
        print(f"🎯 Using location ID column: {location_id_col}")
        
        # Get non-ID columns for renaming
        feature_cols = [col for col in spatial_df.columns if col != location_id_col]
        
        # Join on pickup location
        pickup_spatial = spatial_df.select([
            F.col(location_id_col).alias("PULocationID"),
            *[F.col(col).alias(f"pickup_{col}") for col in feature_cols]
        ])
        
        main_with_pickup = main_df.join(
            pickup_spatial,
            on="PULocationID", 
            how="left"
        )
        
        # Join on dropoff location
        dropoff_spatial = spatial_df.select([
            F.col(location_id_col).alias("DOLocationID"),
            *[F.col(col).alias(f"dropoff_{col}") for col in feature_cols]
        ])
        
        joined_df = main_with_pickup.join(
            dropoff_spatial,
            on="DOLocationID",
            how="left"
        )
        
        pickup_features = len([col for col in joined_df.columns if col.startswith("pickup_")])
        dropoff_features = len([col for col in joined_df.columns if col.startswith("dropoff_")])
        
        print(f"✅ Spatial join complete:")
        print(f"  • Total rows: {joined_df.count():,}")
        print(f"  • Pickup features added: {pickup_features}")
        print(f"  • Dropoff features added: {dropoff_features}")
        
        return joined_df
        
    except Exception as e:
        print(f"❌ Spatial join failed: {e}")
        import traceback
        traceback.print_exc()
        return main_df


def join_temporal_data(main_df: DataFrame, temporal_df: DataFrame) -> DataFrame:
    """
    Join temporal data to main dataset based on pickup datetime.
    
    Args:
        main_df: Main Uber trip dataset
        temporal_df: Temporal features dataset
        
    Returns:
        DataFrame with temporal features joined
    """
    print("📅 Joining temporal data...")
    
    try:
        # Prepare temporal dataset
        temporal_prepared = temporal_df.select([
            F.col("pickup_datetime"),
            *[col for col in temporal_df.columns if col != "pickup_datetime"]
        ])
        
        # Join on pickup datetime
        joined_df = main_df.join(
            temporal_prepared,
            on="pickup_datetime",
            how="left"
        )
        
        print(f"✅ Temporal join complete: {joined_df.count():,} rows")
        return joined_df
        
    except Exception as e:
        print(f"❌ Temporal join failed: {e}")
        return main_df


def join_all_external_data(main_df: DataFrame, year: int, month: int) -> Tuple[DataFrame, Dict[str, bool]]:
    """
    Join all available external datasets to the main dataset for a specific month.
    
    Args:
        main_df: Main Uber trip dataset
        year: Year to process
        month: Month to process
        
    Returns:
        Tuple of (enhanced_dataframe, join_status)
    """
    print(f"🔗 Joining external data for {year}-{month:02d}")
    print("=" * 45)
    
    # Check data availability
    availability = check_external_data_availability(year, month)
    
    print(f"📊 External data availability:")
    for dataset_type, available in availability.items():
        status = "✅ Available" if available else "❌ Missing"
        print(f"  • {dataset_type.capitalize()}: {status}")
    
    enhanced_df = main_df
    join_status = {}
    
    # Join weather data
    if availability['weather']:
        weather_df = load_external_dataset('weather', year, month)
        if weather_df is not None:
            enhanced_df = join_weather_data(enhanced_df, weather_df)
            join_status['weather'] = True
        else:
            join_status['weather'] = False
    else:
        print("⚠️ Skipping weather join - data not available")
        join_status['weather'] = False
    
    # Join spatial data
    if availability['spatial']:
        spatial_df = load_external_dataset('spatial', year, month)
        if spatial_df is not None:
            enhanced_df = join_spatial_data(enhanced_df, spatial_df)
            join_status['spatial'] = True
        else:
            join_status['spatial'] = False
    else:
        print("⚠️ Skipping spatial join - data not available")
        join_status['spatial'] = False
    
    # Join temporal data
    if availability['temporal']:
        temporal_df = load_external_dataset('temporal', year, month)
        if temporal_df is not None:
            enhanced_df = join_temporal_data(enhanced_df, temporal_df)
            join_status['temporal'] = True
        else:
            join_status['temporal'] = False
    else:
        print("⚠️ Skipping temporal join - data not available")
        join_status['temporal'] = False
    
    # Summary
    successful_joins = sum(join_status.values())
    print(f"\n📈 Join Summary:")
    print(f"  • Original features: {len(main_df.columns)}")
    print(f"  • Enhanced features: {len(enhanced_df.columns)}")
    print(f"  • Features added: {len(enhanced_df.columns) - len(main_df.columns)}")
    print(f"  • Successful joins: {successful_joins}/3")
    
    return enhanced_df, join_status


def create_enhanced_monthly_dataset(year: int, month: int, 
                                   input_path: str = None, 
                                   output_path: str = None,
                                   sample_fraction: float = 1.0) -> str:
    """
    Create an enhanced dataset by joining external features to monthly Uber data.
    
    Args:
        year: Year to process
        month: Month to process
        input_path: Path to input data (if None, uses default comprehensive path)
        output_path: Path to save enhanced data (if None, uses default)
        sample_fraction: Fraction of data to process (1.0 = all data)
        
    Returns:
        Path to the saved enhanced dataset
    """
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active Spark session found")
    
    print(f"🚀 Creating Enhanced Dataset for {year}-{month:02d}")
    print("=" * 50)
    
    # Default paths
    if input_path is None:
        input_path = f"./lake/comprehensive/full_profitability_{year}_{month:02d}/year={year}/month={month:02d}/"
    
    if output_path is None:
        output_path = f"./lake/enhanced/profitability_with_externals_{year}_{month:02d}/year={year}/month={month:02d}/"
    
    try:
        # Load main dataset
        print(f"📂 Loading main dataset from: {input_path}")
        main_df = spark.read.parquet(input_path)
        
        if sample_fraction < 1.0:
            main_df = main_df.sample(fraction=sample_fraction, seed=42)
            print(f"🎯 Using {sample_fraction*100:.1f}% sample")
        
        print(f"✅ Main data loaded: {main_df.count():,} rows, {len(main_df.columns)} columns")
        
        # Join external data
        enhanced_df, join_status = join_all_external_data(main_df, year, month)
        
        # Cache the result
        enhanced_df = enhanced_df.cache()
        
        # Save enhanced dataset
        print(f"\n💾 Saving enhanced dataset to: {output_path}")
        enhanced_df.write.mode("overwrite").parquet(output_path)
        
        # Verify save
        final_count = enhanced_df.count()
        final_columns = len(enhanced_df.columns)
        
        print(f"✅ Enhanced dataset saved successfully!")
        print(f"  📊 Final records: {final_count:,}")
        print(f"  🔢 Final columns: {final_columns}")
        print(f"  📁 Output path: {output_path}")
        
        return output_path
        
    except Exception as e:
        print(f"❌ Error creating enhanced dataset: {e}")
        raise


__all__ = [
    'check_external_data_availability',
    'load_external_dataset', 
    'join_weather_data',
    'join_spatial_data',
    'join_temporal_data',
    'join_all_external_data',
    'create_enhanced_monthly_dataset'
]

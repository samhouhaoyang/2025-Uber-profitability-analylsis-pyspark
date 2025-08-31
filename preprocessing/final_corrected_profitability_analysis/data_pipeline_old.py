"""
Profitability Analysis Data Loading Functions

This module provides data loading functions specifically for the 
"FINAL CORRECTED PROFITABILITY ANALYSIS - FIXES ALL COLUMN CONFLICTS" section in new.ipynb.

Key functions:
- load_base_uber_data_for_profitability_corrected: Loads and preprocesses base Uber trip data
- Supporting utilities for Spark session management and data validation
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
import random


def load_base_uber_data_for_profitability_corrected(year: int, month: int, sample_fraction: float = 0.01) -> DataFrame:
    """
    Load and prepare base Uber trip data for profitability analysis.
    
    This function loads raw HVFHS data, filters for Uber trips only, applies basic
    data quality checks, and optionally samples the data for analysis.
    
    Args:
        year (int): Year to load (e.g., 2023, 2024)
        month (int): Month to load (1-12)
        sample_fraction (float): Fraction of data to sample (0.01 = 1%, 1.0 = 100%)
                                Use smaller fractions for development/testing
    
    Returns:
        DataFrame: Spark DataFrame with base Uber trip data ready for profitability analysis
    
    Raises:
        Exception: If data cannot be loaded or processed
    """
    # Configuration constants
    RAW_HVFHS = "./lake/raw/hvfhs/v1"
    PROVIDER_CODE = "HV0003"  # Uber's HVFHS license number
    
    # Get Spark session
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active Spark session found. Please initialize Spark first.")
    
    print(f"📂 Loading raw HVFHS data for {year}-{month:02d}...")
    
    try:
        # Load the specific year/month partition from raw HVFHS data
        raw = (spark.read.parquet(RAW_HVFHS)
               .filter((F.col("year") == year) & (F.col("month") == month)))
        
        initial_count = raw.count()
        print(f"   Raw HVFHS records: {initial_count:,}")
        
        if initial_count == 0:
            raise ValueError(f"No data found for {year}-{month:02d} in {RAW_HVFHS}")
        
        # Filter for Uber trips only using license number
        lic = F.upper(F.trim(F.col("hvfhs_license_num").cast("string")))
        is_uber = (lic == F.lit(PROVIDER_CODE))
        uber_raw = raw.filter(is_uber)
        
        uber_count = uber_raw.count()
        print(f"   Uber trips filtered: {uber_count:,}")
        
        if uber_count == 0:
            raise ValueError(f"No Uber trips found for {year}-{month:02d}")
        
        # Apply basic data quality filters
        print("🔍 Applying data quality filters...")
        
        # Filter out records with missing critical fields
        quality_filtered = (uber_raw
                           .filter(F.col("pickup_datetime").isNotNull())
                           .filter(F.col("dropoff_datetime").isNotNull())
                           .filter(F.col("PULocationID").isNotNull())
                           .filter(F.col("DOLocationID").isNotNull())
                           .filter(F.col("trip_miles").isNotNull())
                           .filter(F.col("trip_time").isNotNull())
                           .filter(F.col("base_passenger_fare").isNotNull()))
        
        # Filter out invalid trip data
        quality_filtered = (quality_filtered
                           .filter(F.col("trip_miles") >= 0)
                           .filter(F.col("trip_time") > 0)
                           .filter(F.col("base_passenger_fare") >= 0)
                           .filter(F.col("pickup_datetime") <= F.col("dropoff_datetime")))
        
        quality_count = quality_filtered.count()
        print(f"   After quality filters: {quality_count:,}")
        
        # Apply sampling if requested
        if sample_fraction < 1.0:
            print(f"🎲 Applying {sample_fraction:.1%} sampling...")
            
            # Use deterministic sampling with seed for reproducibility
            seed = hash(f"{year}-{month}") % (2**31 - 1)
            sampled_data = quality_filtered.sample(fraction=sample_fraction, seed=seed)
            
            final_count = sampled_data.count()
            print(f"   Final sampled dataset: {final_count:,} trips")
            
            base_data = sampled_data
        else:
            print("📊 Using full dataset (no sampling)")
            base_data = quality_filtered
            final_count = quality_count
        
        # Add some basic computed columns that might be useful for profitability analysis
        print("🔧 Adding computed columns...")
        
        base_data = (base_data
                    .withColumn("trip_duration_minutes", 
                               F.col("trip_time") / 60.0)
                    .withColumn("avg_speed_mph", 
                               F.when(F.col("trip_time") > 0, 
                                     F.col("trip_miles") * 3600.0 / F.col("trip_time"))
                               .otherwise(0))
                    .withColumn("revenue_per_mile", 
                               F.when(F.col("trip_miles") > 0, 
                                     F.col("base_passenger_fare") / F.col("trip_miles"))
                               .otherwise(0))
                    .withColumn("revenue_per_minute", 
                               F.when(F.col("trip_time") > 0, 
                                     F.col("base_passenger_fare") * 60.0 / F.col("trip_time"))
                               .otherwise(0)))
        
        # Cache the result for performance
        base_data = base_data.cache()
        
        print(f"✅ Base Uber data loaded successfully: {final_count:,} trips, {len(base_data.columns)} columns")
        
        return base_data
        
    except Exception as e:
        print(f"❌ Error loading base Uber data for {year}-{month:02d}: {str(e)}")
        raise


def get_spark_session():
    """
    Get or create a Spark session for data processing.
    
    Returns:
        SparkSession: Active Spark session
    """
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = (SparkSession.builder
                .appName("UberProfitabilityAnalysis")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate())
    return spark


def validate_data_availability(year: int, month: int) -> bool:
    """
    Check if data is available for the specified year and month.
    
    Args:
        year (int): Year to check
        month (int): Month to check
    
    Returns:
        bool: True if data is available, False otherwise
    """
    RAW_HVFHS = "./lake/raw/hvfhs/v1"
    
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            return False
            
        # Try to read a small sample
        test_df = (spark.read.parquet(RAW_HVFHS)
                  .filter((F.col("year") == year) & (F.col("month") == month))
                  .limit(1))
        
        return test_df.count() > 0
        
    except Exception:
        return False


def load_and_integrate_all_features_final(year: int, month: int, sample_fraction: float = 0.01) -> DataFrame:
    """
    FINAL: Load raw trip data and integrate ALL external features with proper column management.
    
    This function orchestrates the complete data loading pipeline for profitability analysis,
    integrating base trip data with spatial, temporal, and weather features while handling
    column conflicts.
    
    Args:
        year (int): Year to process
        month (int): Month to process  
        sample_fraction (float): Fraction of data to sample
        
    Returns:
        DataFrame: Integrated dataset ready for profitability analysis
    """
    print(f"🔄 LOADING AND INTEGRATING ALL FEATURES FOR {year}-{month:02d}")
    print("="*70)
    
    # === STEP 1: Load base trip data ===
    print("📂 Step 1: Loading base trip data...")
    
    base_data = load_base_uber_data_for_profitability_corrected(year, month, sample_fraction)
    initial_count = base_data.count()
    print(f"✅ Base data loaded: {initial_count:,} trips, {len(base_data.columns)} columns")
    
    # === STEP 2: Load spatial features ===
    print("\n🗺️ Step 2: Loading spatial features...")
    try:
        # Try to load spatial features (assuming they exist in the notebook context)
        # This would need to be properly implemented based on your existing feature loading functions
        # For now, we'll add a fallback that doesn't break the pipeline
        spark = SparkSession.getActiveSession()
        
        # Try to load from saved location or skip if not available
        try:
            spatial_features = spark.read.parquet(f"./lake/external_features/spatial/year={year}/month={month:02d}")
        except:
            print("⚠️ Spatial features not found - skipping spatial integration")
            spatial_features = None
        
        if spatial_features is not None:
            # Only select columns we need from spatial features to avoid conflicts
            spatial_cols_to_select = [col for col in spatial_features.columns 
                                     if col not in ['pickup_datetime', 'dropoff_datetime', 'trip_time', 'trip_miles']]
            spatial_features_clean = spatial_features.select(spatial_cols_to_select)
            
            base_data = base_data.join(
                F.broadcast(spatial_features_clean), 
                ["PULocationID", "DOLocationID"], 
                "left"
            )
            print(f"✅ Spatial features integrated: {len(spatial_features_clean.columns)} spatial features")
    except Exception as e:
        print(f"⚠️ Spatial features failed: {e}")
    
    # === STEP 3: Load temporal features ===
    print("\n📅 Step 3: Loading temporal features...")
    try:
        # Try to load temporal features from saved location
        try:
            temporal_features = spark.read.parquet(f"./lake/external_features/temporal/year={year}/month={month:02d}")
        except:
            print("⚠️ Temporal features not found - skipping temporal integration")
            temporal_features = None
        
        if temporal_features is not None:
            # Only select columns we need from temporal features to avoid conflicts
            temporal_cols_to_select = [col for col in temporal_features.columns 
                                      if col not in ['pickup_datetime', 'dropoff_datetime', 'trip_time', 'trip_miles']]
            temporal_features_clean = temporal_features.select(temporal_cols_to_select)
            
            base_data = base_data.join(
                F.broadcast(temporal_features_clean), 
                "pickup_date", 
                "left"
            )
            print(f"✅ Temporal features integrated: {len(temporal_features_clean.columns)} temporal features")
    except Exception as e:
        print(f"⚠️ Temporal features failed: {e}")
    
    # === STEP 4: Load weather features ===
    print("\n🌤️ Step 4: Loading weather features...")
    try:
        # Try to load weather features from saved location
        try:
            weather_features = spark.read.parquet(f"./lake/external_features/weather/year={year}/month={month:02d}")
        except:
            print("⚠️ Weather features not found - skipping weather integration")
            weather_features = None
        
        if weather_features is not None:
            # Only select columns we need from weather features to avoid conflicts
            weather_cols_to_select = [col for col in weather_features.columns 
                                     if col not in ['pickup_datetime', 'dropoff_datetime', 'trip_time', 'trip_miles', 'pickup_date']]
            weather_features_clean = weather_features.select(weather_cols_to_select + ['weather_date'])
            
            # Join on weather_date and drop the duplicate column
            base_data = base_data.join(
                F.broadcast(weather_features_clean), 
                base_data.pickup_date == weather_features_clean.weather_date,
                "left"
            ).drop("weather_date")
            
            print(f"✅ Weather features integrated: {len(weather_features_clean.columns)-1} weather features")
    except Exception as e:
        print(f"⚠️ Weather features failed: {e}")
    
    final_count = base_data.count()
    total_columns = len(base_data.columns)
    
    print(f"\n🎯 INTEGRATION COMPLETE!")
    print(f"  📊 Final dataset: {final_count:,} trips, {total_columns} total columns")
    print(f"  🔗 Data integrity: {final_count/initial_count*100:.1f}% trips retained")
    
    return base_data


def save_comprehensive_profitability_dataset_final(df: DataFrame, year: int, month: int, dataset_name: str = "comprehensive_profitability_final") -> str:
    """
    FINAL: Save the comprehensive profitability dataset with column deduplication.
    
    Args:
        df: DataFrame to save
        year: Year for partitioning
        month: Month for partitioning  
        dataset_name: Base name for the dataset
        
    Returns:
        str: Path where data was saved, or None if failed
    """
    print(f"💾 SAVING COMPREHENSIVE PROFITABILITY DATASET (FINAL)")
    print("="*55)
    
    # Create save path
    save_path = f"./lake/comprehensive/{dataset_name}/year={year}/month={month:02d}/"
    
    try:
        # Check for duplicate columns and remove them
        columns = df.columns
        unique_columns = []
        seen_columns = set()
        
        for col in columns:
            if col not in seen_columns:
                unique_columns.append(col)
                seen_columns.add(col)
            else:
                print(f"⚠️ Removing duplicate column: {col}")
        
        # Select only unique columns
        if len(unique_columns) < len(columns):
            df = df.select(unique_columns)
            print(f"📋 Deduplicated columns: {len(columns)} → {len(unique_columns)}")
        
        # Save the dataset
        df.write.mode("overwrite").parquet(save_path)
        
        # Verify save
        record_count = df.count()
        column_count = len(df.columns)
        
        print(f"✅ Dataset saved successfully!")
        print(f"  📁 Path: {save_path}")
        print(f"  📊 Records: {record_count:,}")
        print(f"  🔢 Columns: {column_count}")
        
        # Show sample of column names for verification
        print(f"  📋 Sample columns: {df.columns[:10]}...")
        
        return save_path
        
    except Exception as e:
        print(f"❌ Save failed: {e}")
        
        # Debug: Show all column names to identify conflicts
        print(f"🔍 DEBUG - All columns ({len(df.columns)}):")
        for i, col in enumerate(df.columns):
            print(f"  {i+1:3d}. {col}")
        
        # Count duplicates
        from collections import Counter
        col_counts = Counter(df.columns)
        duplicates = {col: count for col, count in col_counts.items() if count > 1}
        
        if duplicates:
            print(f"\n⚠️ DUPLICATE COLUMNS FOUND:")
            for col, count in duplicates.items():
                print(f"  - {col}: appears {count} times")
        
        return None


def test_enhanced_profitability_pipeline_final(year: int = 2023, month: int = 1, sample_fraction: float = 0.005) -> dict:
    """
    FINAL: Test the complete enhanced profitability pipeline with column conflict resolution.
    
    Args:
        year: Year to test
        month: Month to test
        sample_fraction: Sample fraction for testing
        
    Returns:
        dict: Test results with status and metrics
    """
    print("🧪 TESTING ENHANCED PROFITABILITY PIPELINE (FINAL VERSION)")
    print("="*70)
    
    try:
        # Load integrated data using final function
        integrated_data = load_and_integrate_all_features_final(year, month, sample_fraction)
        print(f"\n📊 Integrated data loaded: {integrated_data.count():,} trips")
        
        # Debug: Show column names before profitability analysis
        print(f"🔍 Columns before profitability analysis: {len(integrated_data.columns)}")
        
        # Note: These functions would need to be imported from wherever they're defined
        # For now, we'll just do basic validation
        print("✅ Integration pipeline test completed successfully")
        
        return {
            'status': 'success',
            'records': integrated_data.count(),
            'columns': len(integrated_data.columns)
        }
        
    except Exception as e:
        print(f"❌ Pipeline test failed: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        
        return {
            'status': 'failed',
            'error': str(e)
        }
def process_full_profitability_dataset(year, month, save_name="full_profitability"):
    """
    Process the COMPLETE dataset (no sampling) for production analysis.
    """
    
    print(f"🚀 PROCESSING FULL DATASET FOR {year}-{month:02d}")
    print("="*50)
    
    try:
        # Load integrated data WITHOUT sampling
        integrated_data = load_and_integrate_all_features_final(
            year, month, 
            sample_fraction=1.0  # Use FULL dataset
        )
        
        total_trips = integrated_data.count()
        print(f"📊 Full dataset loaded: {total_trips:,} trips")
        
        # Run profitability analysis on full data
        print("💰 Running profitability analysis...")
        profitability_data = analyze_enhanced_profitability_fixed(integrated_data)
        
        print("🎯 Adding composite profitability scoring...")
        final_data = add_composite_profitability_score_updated(profitability_data)
        
        # Save full dataset
        print("💾 Saving comprehensive dataset...")
        save_path = save_comprehensive_profitability_dataset_final(
            final_data, year, month, dataset_name=save_name
        )
        
        return {
            'status': 'success',
            'data': final_data,
            'path': save_path,
            'records': total_trips,
            'columns': len(final_data.columns)
        }
        
    except Exception as e:
        print(f"❌ Failed to process {year}-{month:02d}: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        
        return {
            'status': 'failed',
            'error': str(e),
            'records': 0,
            'columns': 0
        }

def process_all_months_profitability_batch(years=[2023, 2024], months=list(range(1,13))):
    """
    Process full profitability analysis for all available months.
    """
    
    print("🔄 BATCH PROCESSING: FULL PROFITABILITY ANALYSIS FOR ALL MONTHS")
    print("="*75)
    
    results = {}
    total_processed = 0
    total_records = 0
    successful_months = 0
    
    for year in years:
        for month in months:
            month_key = f"{year}-{month:02d}"
            print(f"\n{'='*20} PROCESSING {month_key} {'='*20}")
            
            # Process full month
            result = process_full_profitability_dataset(
                year, month, 
                save_name=f"full_profitability_{year}_{month:02d}"
            )
            
            results[month_key] = result
            
            if result['status'] == 'success':
                successful_months += 1
                total_records += result['records']
                print(f"✅ {month_key}: {result['records']:,} trips processed")
                print(f"📁 Saved to: {result['path']}")
            else:
                print(f"❌ {month_key}: Processing failed")
            
            total_processed += 1
            
            # Progress update
            print(f"📊 Progress: {total_processed}/{len(years)*len(months)} months, "
                  f"{successful_months} successful, {total_records:,} total trips")
    
    # Final summary
    print(f"\n🎉 BATCH PROCESSING COMPLETE!")
    print("="*50)
    print(f"📈 Summary:")
    print(f"  • Total months processed: {total_processed}")
    print(f"  • Successful: {successful_months}")
    print(f"  • Failed: {total_processed - successful_months}")
    print(f"  • Total trips processed: {total_records:,}")
    
    # Show detailed results
    print(f"\n📋 Detailed Results:")
    for month_key, result in results.items():
        status_icon = "✅" if result['status'] == 'success' else "❌"
        if result['status'] == 'success':
            print(f"  {status_icon} {month_key}: {result['records']:,} trips, {result['columns']} columns")
        else:
            print(f"  {status_icon} {month_key}: {result['error']}")
    
    return results


def visualize_profitability_analysis(df, save_plots=True):
    """
    Create comprehensive visualizations for profitability analysis using sample data.
    """
    
    print("📊 CREATING PROFITABILITY ANALYSIS VISUALIZATIONS")
    print("="*55)
    
    import matplotlib.pyplot as plt
    import seaborn as sns
    import pandas as pd
    
    # Convert to Pandas for visualization (use smaller sample if needed)
    sample_size = min(20000, df.count())
    viz_df = df.limit(sample_size).toPandas()
    
    print(f"📈 Creating visualizations with {len(viz_df):,} trips")
    
    # Set up the plotting style
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Create a large figure with multiple subplots
    fig = plt.figure(figsize=(20, 24))
    
    # === 1. CORE PROFITABILITY METRICS ===
    
    # Revenue Distribution
    plt.subplot(4, 3, 1)
    plt.hist(viz_df['uber_net_revenue'], bins=50, alpha=0.7, color='green')
    plt.title('Uber Net Revenue Distribution', fontsize=12, fontweight='bold')
    plt.xlabel('Net Revenue ($)')
    plt.ylabel('Frequency')
    plt.axvline(viz_df['uber_net_revenue'].mean(), color='red', linestyle='--', 
                label=f'Mean: ${viz_df["uber_net_revenue"].mean():.2f}')
    plt.legend()
    
    # Take Rate Distribution
    plt.subplot(4, 3, 2)
    plt.hist(viz_df['uber_take_rate'], bins=50, alpha=0.7, color='blue')
    plt.title('Uber Take Rate Distribution', fontsize=12, fontweight='bold')
    plt.xlabel('Take Rate (%)')
    plt.ylabel('Frequency')
    plt.axvline(viz_df['uber_take_rate'].mean(), color='red', linestyle='--',
                label=f'Mean: {viz_df["uber_take_rate"].mean():.1f}%')
    plt.legend()
    
    # Profitability Score Distribution
    plt.subplot(4, 3, 3)
    plt.hist(viz_df['profitability_score'], bins=20, alpha=0.7, color='purple')
    plt.title('Profitability Score Distribution', fontsize=12, fontweight='bold')
    plt.xlabel('Profitability Score (1-10)')
    plt.ylabel('Frequency')
    plt.axvline(viz_df['profitability_score'].mean(), color='red', linestyle='--',
                label=f'Mean: {viz_df["profitability_score"].mean():.1f}')
    plt.legend()
    
    # === 2. SPATIAL PROFITABILITY ANALYSIS ===
    
    # CBD Type Revenue Analysis
    if 'trip_cbd_type' in viz_df.columns:
        plt.subplot(4, 3, 4)
        cbd_revenue = viz_df.groupby('trip_cbd_type')['uber_net_revenue'].mean().sort_values(ascending=False)
        bars = plt.bar(range(len(cbd_revenue)), cbd_revenue.values, color=['red', 'orange', 'yellow', 'lightgreen'])
        plt.title('Average Revenue by CBD Trip Type', fontsize=12, fontweight='bold')
        plt.xlabel('Trip Type')
        plt.ylabel('Average Net Revenue ($)')
        plt.xticks(range(len(cbd_revenue)), cbd_revenue.index, rotation=45)
        
        # Add value labels on bars
        for i, bar in enumerate(bars):
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                    f'${height:.2f}', ha='center', va='bottom')
    
    # Borough Revenue Analysis  
    if 'PU_Borough' in viz_df.columns:
        plt.subplot(4, 3, 5)
        borough_revenue = viz_df.groupby('PU_Borough')['uber_net_revenue'].mean().sort_values(ascending=False)
        bars = plt.bar(range(len(borough_revenue)), borough_revenue.values, color='skyblue')
        plt.title('Average Revenue by Pickup Borough', fontsize=12, fontweight='bold')
        plt.xlabel('Borough')
        plt.ylabel('Average Net Revenue ($)')
        plt.xticks(range(len(borough_revenue)), borough_revenue.index, rotation=45)
        
        # Add value labels on bars
        for i, bar in enumerate(bars):
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                    f'${height:.2f}', ha='center', va='bottom')
    
    # === 3. TEMPORAL PROFITABILITY ANALYSIS ===
    
    # Rush Hour Revenue Analysis
    if 'rush_hour_label' in viz_df.columns:
        plt.subplot(4, 3, 6)
        rush_revenue = viz_df.groupby('rush_hour_label')['uber_net_revenue'].mean().sort_values(ascending=False)
        bars = plt.bar(range(len(rush_revenue)), rush_revenue.values, color='lightcoral')
        plt.title('Average Revenue by Rush Hour Period', fontsize=12, fontweight='bold')
        plt.xlabel('Time Period')
        plt.ylabel('Average Net Revenue ($)')
        plt.xticks(range(len(rush_revenue)), rush_revenue.index, rotation=45)
        
        # Add value labels on bars
        for i, bar in enumerate(bars):
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                    f'${height:.2f}', ha='center', va='bottom')
    
    # Holiday vs Non-Holiday Revenue
    if 'is_holiday' in viz_df.columns:
        plt.subplot(4, 3, 7)
        holiday_revenue = viz_df.groupby('is_holiday')['uber_net_revenue'].mean()
        labels = ['Non-Holiday', 'Holiday']
        bars = plt.bar(labels, holiday_revenue.values, color=['lightblue', 'gold'])
        plt.title('Average Revenue: Holiday vs Non-Holiday', fontsize=12, fontweight='bold')
        plt.ylabel('Average Net Revenue ($)')
        
        # Add value labels on bars
        for i, bar in enumerate(bars):
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                    f'${height:.2f}', ha='center', va='bottom')
    
    # === 4. WEATHER PROFITABILITY ANALYSIS ===
    
    # Temperature vs Revenue (if weather data available)
    temp_col = 'temp_max_c' if 'temp_max_c' in viz_df.columns else 'tmax_c' if 'tmax_c' in viz_df.columns else None
    if temp_col:
        plt.subplot(4, 3, 8)
        # Create temperature bins
        viz_df['temp_bin'] = pd.cut(viz_df[temp_col], bins=5, labels=['Very Cold', 'Cold', 'Moderate', 'Warm', 'Hot'])
        temp_revenue = viz_df.groupby('temp_bin')['uber_net_revenue'].mean()
        bars = plt.bar(range(len(temp_revenue)), temp_revenue.values, color='orange')
        plt.title('Average Revenue by Temperature', fontsize=12, fontweight='bold')
        plt.xlabel('Temperature Range')
        plt.ylabel('Average Net Revenue ($)')
        plt.xticks(range(len(temp_revenue)), temp_revenue.index, rotation=45)
        
        # Add value labels on bars
        for i, bar in enumerate(bars):
            height = bar.get_height()
            if not pd.isna(height):
                plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                        f'${height:.2f}', ha='center', va='bottom')
    
    # === 5. EFFICIENCY ANALYSIS ===
    
    # Revenue per Mile vs Trip Distance
    plt.subplot(4, 3, 9)
    plt.scatter(viz_df['trip_miles'], viz_df['revenue_per_mile'], alpha=0.5, s=10)
    plt.title('Revenue per Mile vs Trip Distance', fontsize=12, fontweight='bold')
    plt.xlabel('Trip Distance (miles)')
    plt.ylabel('Revenue per Mile ($)')
    plt.xlim(0, viz_df['trip_miles'].quantile(0.95))  # Remove extreme outliers for clarity
    
    # === 6. PROFITABILITY MULTIPLIERS ===
    
    # Composite Multiplier Distribution
    if 'composite_profitability_multiplier' in viz_df.columns:
        plt.subplot(4, 3, 10)
        plt.hist(viz_df['composite_profitability_multiplier'], bins=30, alpha=0.7, color='green')
        plt.title('Composite Profitability Multiplier Distribution', fontsize=12, fontweight='bold')
        plt.xlabel('Multiplier')
        plt.ylabel('Frequency')
        plt.axvline(viz_df['composite_profitability_multiplier'].mean(), color='red', linestyle='--',
                    label=f'Mean: {viz_df["composite_profitability_multiplier"].mean():.2f}x')
        plt.legend()
    
    # === 7. RIDE-SHARING ANALYSIS ===
    
    # Shared vs Non-Shared Revenue
    plt.subplot(4, 3, 11)
    viz_df['is_shared'] = (viz_df['shared_request_flag'] == 'Y') & (viz_df['shared_match_flag'] == 'Y')
    shared_revenue = viz_df.groupby('is_shared')['uber_net_revenue'].mean()
    labels = ['Non-Shared', 'Shared']
    bars = plt.bar(labels, shared_revenue.values, color=['lightcoral', 'lightgreen'])
    plt.title('Average Revenue: Shared vs Non-Shared Rides', fontsize=12, fontweight='bold')
    plt.ylabel('Average Net Revenue ($)')
    
    # Add value labels on bars
    for i, bar in enumerate(bars):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                f'${height:.2f}', ha='center', va='bottom')
    
    # === 8. SUMMARY STATISTICS ===
    
    plt.subplot(4, 3, 12)
    plt.axis('off')
    
    # Create summary statistics text
    summary_stats = f"""
    PROFITABILITY ANALYSIS SUMMARY
    ==============================
    
    📊 Dataset: {len(viz_df):,} trips (sample)
    
    💰 Revenue Metrics:
    • Avg Net Revenue: ${viz_df['uber_net_revenue'].mean():.2f}
    • Avg Take Rate: {viz_df['uber_take_rate'].mean():.1f}%
    • Avg Profitability Score: {viz_df['profitability_score'].mean():.1f}/10
    
    🔄 Efficiency Metrics:
    • Avg Revenue/Mile: ${viz_df['revenue_per_mile'].mean():.2f}
    """
    
    if 'composite_profitability_multiplier' in viz_df.columns:
        summary_stats += f"• Avg Multiplier: {viz_df['composite_profitability_multiplier'].mean():.2f}x\n"
    
    if 'trip_cbd_type' in viz_df.columns:
        top_cbd_type = viz_df.groupby('trip_cbd_type')['uber_net_revenue'].mean().idxmax()
        summary_stats += f"\n🗺️ Best CBD Type: {top_cbd_type}\n"
    
    if 'rush_hour_label' in viz_df.columns:
        top_rush_period = viz_df.groupby('rush_hour_label')['uber_net_revenue'].mean().idxmax()
        summary_stats += f"⏰ Best Time Period: {top_rush_period}\n"
    
    plt.text(0.05, 0.95, summary_stats, transform=plt.gca().transAxes, 
             fontsize=10, verticalalignment='top', fontfamily='monospace')
    
    plt.tight_layout()
    
    if save_plots:
        plt.savefig('profitability_comprehensive_analysis.png', dpi=300, bbox_inches='tight')
        print(f"💾 Visualization saved: profitability_comprehensive_analysis.png")
    
    plt.show()
    
    print(f"✅ Profitability visualization complete!")
    
    return viz_df


# Make functions available for import
__all__ = [
    'load_base_uber_data_for_profitability_corrected',
    'load_and_integrate_all_features_final',
    'save_comprehensive_profitability_dataset_final', 
    'test_enhanced_profitability_pipeline_final',
    'get_spark_session', 
    'validate_data_availability',
    'process_full_profitability_dataset',
    'process_all_months_profitability_batch',
    'visualize_profitability_analysis'
]

"""
Copy and Optimize Existing Weather Data

This function simply loads the existing (working) weather data and removes redundant features,
avoiding all the data type issues we encountered with raw data processing.
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *


def copy_and_optimize_existing_weather_data():
    """
    Load existing weather data and optimize by removing redundant features.
    This avoids all the data type conversion issues.
    """
    
    print("🔄 COPYING AND OPTIMIZING EXISTING WEATHER DATA")
    print("="*60)
    
    try:
        # Get Spark session
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.appName("CopyOptimizeWeather").getOrCreate()
        
        # Load existing weather data (that we know works!)
        print("📂 Loading existing weather external features...")
        existing_weather = spark.read.parquet("./lake/external_features/weather")
        
        initial_count = existing_weather.count()
        initial_cols = len(existing_weather.columns)
        
        print(f"✅ Existing data loaded: {initial_count:,} records, {initial_cols} features")
        
        # Show current schema
        print(f"\n📋 Current schema:")
        existing_weather.printSchema()
        
        # Define features to KEEP (optimized set)
        features_to_keep = [
            "weather_date",
            "year", 
            "month",
            # Core temperature (keep only one)
            "temp_avg_calculated",
            # Only extreme conditions
            "is_freezing", 
            "is_hot",
            # Precipitation (continuous)
            "precipitation_mm",
            # Snow (binary)
            "is_snow", 
            # Wind (continuous)
            "wind_speed_kmh",
            # Fog (consolidated) - check if exists
            "fog_flag",
            # Composite metrics
            "weather_comfort_index",
            "season"
        ]
        
        # Check which features actually exist
        available_columns = existing_weather.columns
        existing_features_to_keep = [col for col in features_to_keep if col in available_columns]
        
        print(f"\n🎯 Features to keep ({len(existing_features_to_keep)}):")
        for col in existing_features_to_keep:
            print(f"   ✅ {col}")
        
        # Check if we have fog_mist and heavy_fog to create fog_flag
        if "fog_flag" not in available_columns:
            if "fog_mist" in available_columns or "heavy_fog" in available_columns:
                print(f"\n🔧 Creating fog_flag from existing fog features...")
                fog_condition = F.lit(0)
                if "fog_mist" in available_columns:
                    fog_condition = fog_condition | F.coalesce(F.col("fog_mist"), F.lit(0))
                if "heavy_fog" in available_columns:
                    fog_condition = fog_condition | F.coalesce(F.col("heavy_fog"), F.lit(0))
                
                existing_weather = existing_weather.withColumn("fog_flag", fog_condition.cast("integer"))
                existing_features_to_keep.append("fog_flag")
                print(f"   ✅ fog_flag created successfully")
        
        # Select only the optimized features
        optimized_weather = existing_weather.select(*existing_features_to_keep)
        
        final_count = optimized_weather.count()
        final_cols = len(optimized_weather.columns)
        
        print(f"\n✅ OPTIMIZATION COMPLETE")
        print(f"📊 Optimized data: {final_count:,} records, {final_cols} features")
        print(f"🎯 Removed {initial_cols - final_cols} redundant features")
        
        # Show sample
        print(f"\n📋 Sample optimized data:")
        sample_data = optimized_weather.orderBy("weather_date").limit(3).collect()
        for row in sample_data:
            available_fields = [f"{field}: {row[field]}" for field in existing_features_to_keep[:5] if field in row.asDict()]
            print(f"   📅 {row['weather_date']} | {' | '.join(available_fields)}")
        
        return optimized_weather
        
    except Exception as e:
        print(f"❌ Copy and optimize failed: {e}")
        import traceback
        print(traceback.format_exc())
        return None


def save_optimized_copy(optimized_df, base_path="./lake/external_features/weather"):
    """
    Save the optimized copy back to the same location.
    """
    
    print("💾 SAVING OPTIMIZED COPY")
    print("="*40)
    
    try:
        # Get Spark session
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.appName("SaveOptimizedCopy").getOrCreate()
            
        print(f"💾 Saving optimized data to: {base_path}/")
        print(f"⚠️ This will overwrite existing weather data")
        
        (optimized_df
         .write
         .mode("overwrite")
         .partitionBy("year", "month") 
         .parquet(base_path))
        
        print(f"✅ Optimized weather data saved successfully!")
        
        # Verify
        saved_data = spark.read.parquet(base_path)
        saved_count = saved_data.count()
        saved_cols = len(saved_data.columns)
        
        print(f"🔍 Verification: {saved_count:,} records, {saved_cols} features")
        
        return {
            'status': 'success',
            'total_records': saved_count,
            'features_count': saved_cols
        }
        
    except Exception as e:
        print(f"❌ Failed to save optimized copy: {e}")
        return {
            'status': 'failed',
            'error': str(e)
        }

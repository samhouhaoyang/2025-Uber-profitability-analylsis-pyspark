"""
Simple Weather Data Processor - 最基本版本
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *


def simple_process_weather_data():
    """
    最简单的天气数据处理版本 - 避免复杂的数据类型问题
    """
    
    print("🔧 SIMPLE WEATHER DATA PROCESSING")
    print("="*50)
    
    try:
        # Get Spark session
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.appName("SimpleWeatherProcessing").getOrCreate()
        
        # Load raw data
        print("📂 Loading raw NOAA weather data...")
        weather_csv = "./lake/landing/lookups/NOAA nyc weather.csv"
        raw_weather = spark.read.option("header", "true").csv(weather_csv)
        
        print(f"✅ Raw data loaded: {raw_weather.count():,} records")
        
        # Basic filtering and date processing
        print("\n📅 Processing dates...")
        weather_with_date = (raw_weather
            .withColumn("weather_date", F.to_date(F.col("DATE")))
            .filter(F.col("weather_date").isNotNull())
        )
        
        print(f"✅ Date processed: {weather_with_date.count():,} records")
        
        # Very simple feature creation - avoiding complex type conversions
        print("\n🔧 Creating basic features...")
        weather_features = (weather_with_date
            .withColumn("year", F.year("weather_date"))
            .withColumn("month", F.month("weather_date"))
            .withColumn("season", 
                F.when(F.col("month").isin([12, 1, 2]), "winter")
                .when(F.col("month").isin([3, 4, 5]), "spring")
                .when(F.col("month").isin([6, 7, 8]), "summer")
                .otherwise("fall")
            )
        )
        
        # Simple aggregation by date
        print("\n📊 Aggregating by date...")
        weather_aggregated = (weather_features
            .groupBy("weather_date", "year", "month", "season")
            .agg(
                F.count("*").alias("station_count"),
                F.first("season").alias("season_final")
            )
            .select("weather_date", "year", "month", "season_final")
            .withColumnRenamed("season_final", "season")
        )
        
        final_count = weather_aggregated.count()
        print(f"\n✅ SIMPLE PROCESSING COMPLETE")
        print(f"📊 Final dataset: {final_count:,} daily records")
        print(f"🎯 Basic features: {len(weather_aggregated.columns)} columns")
        
        # Safe sample display
        print(f"\n📋 Sample data:")
        sample_data = weather_aggregated.orderBy("weather_date").limit(3).collect()
        for row in sample_data:
            print(f"   📅 {row['weather_date']} | Season: {row['season']}")
        
        return weather_aggregated
        
    except Exception as e:
        print(f"❌ Simple processing failed: {e}")
        import traceback
        print(traceback.format_exc())
        return None


def save_simple_weather_data(weather_df, base_path="./lake/external_features/weather"):
    """
    保存简单的天气数据
    """
    
    print("💾 SAVING SIMPLE WEATHER DATA")
    print("="*40)
    
    try:
        # Get Spark session
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.appName("SaveSimpleWeather").getOrCreate()
            
        print(f"💾 Saving to: {base_path}/")
        
        (weather_df
         .write
         .mode("overwrite")
         .partitionBy("year", "month") 
         .parquet(base_path))
        
        print(f"✅ Simple weather data saved successfully!")
        
        # Verify
        saved_data = spark.read.parquet(base_path)
        saved_count = saved_data.count()
        
        print(f"🔍 Verification: {saved_count:,} records saved")
        
        return {
            'status': 'success',
            'total_records': saved_count
        }
        
    except Exception as e:
        print(f"❌ Failed to save: {e}")
        return {
            'status': 'failed',
            'error': str(e)
        }

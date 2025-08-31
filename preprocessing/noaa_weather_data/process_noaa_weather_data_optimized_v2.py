"""
Optimized Weather Data Processor V2

Contains functions for processing NOAA weather data with optimized feature set.
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
def process_noaa_weather_data_optimized_v2(quality_results=None):
    """
    优化版本的NOAA天气数据处理 - 只生成必要的features
    
    Key optimizations:
    - 保留核心连续变量 (temp_avg_calculated, precipitation_mm, wind_speed_kmh)
    - 只保留极端条件分类 (is_freezing, is_hot)  
    - 合并fog相关特征为单一flag
    - 移除稀有天气事件
    - 保留综合指标 (weather_comfort_index, season)
    
    Args:
        quality_results (dict): Results from data quality assessment
    
    Returns:
        DataFrame: Processed weather data with optimized feature set
    """
    
    print("🔧 OPTIMIZED WEATHER DATA PROCESSING V2 - MINIMAL FEATURE SET")
    print("="*70)
    
    try:
        # Get Spark session
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.appName("OptimizedWeatherProcessing").getOrCreate()
        # === LOAD RAW DATA (same as original) ===
        print("📂 Loading raw NOAA weather data...")
        # Use the correct path for NOAA weather data
        weather_csv = "./lake/landing/lookups/NOAA nyc weather.csv"
        raw_weather = spark.read.option("header", "true").option("inferSchema", "true").csv(weather_csv)
        
        if raw_weather.count() == 0:
            print("❌ No raw weather data found")
            return None
            
        print(f"✅ Raw data loaded: {raw_weather.count():,} records")
        
        # === STATION SELECTION (same as original) ===
        print("\n🏢 Selecting NYC weather stations...")
        
        # Select representative NYC stations
        selected_stations = ['USW00094728', 'USW00014732', 'USC00305816']  # JFK, LGA, Central Park
        
        weather_filtered = raw_weather.filter(F.col("STATION").isin(selected_stations))
        
        print(f"✅ Filtered to NYC stations: {weather_filtered.count():,} records")
        
        # === DATE PROCESSING (same as original) ===
        print("\n📅 Processing dates and basic cleaning...")
        
        weather_with_date = (weather_filtered
            .withColumn("weather_date", F.to_date(F.col("DATE")))
            .filter(F.col("weather_date").isNotNull())
            .filter(F.col("TMAX").isNotNull() | F.col("TMIN").isNotNull())
        )
        
        # === FEATURE ENGINEERING - OPTIMIZED VERSION ===
        print("\n🔧 Creating OPTIMIZED feature set...")
        
        # Pre-clean raw columns as strings to avoid implicit numeric casts
        tmax_s = F.trim(F.col("TMAX").cast("string"))
        tmin_s = F.trim(F.col("TMIN").cast("string"))
        prcp_s = F.trim(F.col("PRCP").cast("string"))
        snow_s = F.trim(F.col("SNOW").cast("string"))
        awnd_s = F.trim(F.col("AWND").cast("string"))

        weather_features = (weather_with_date
            # === CORE TEMPERATURE FEATURES (OPTIMIZED) ===
            .withColumn(
                "tmax_c",
                F.when(
                    tmax_s.isNotNull() & (tmax_s != "") & (tmax_s != "T"),
                    tmax_s.cast("double") / 10.0
                ).otherwise(F.lit(None))
            )
            .withColumn(
                "tmin_c",
                F.when(
                    tmin_s.isNotNull() & (tmin_s != "") & (tmin_s != "T"),
                    tmin_s.cast("double") / 10.0
                ).otherwise(F.lit(None))
            )
            .withColumn(
                "temp_avg_calculated",
                F.when(
                    F.col("tmax_c").isNotNull() & F.col("tmin_c").isNotNull(),
                    (F.col("tmax_c") + F.col("tmin_c")) / 2.0
                ).otherwise(F.lit(None))
            )
            
            # === EXTREME TEMPERATURE FLAGS ONLY ===
            .withColumn("is_freezing", F.when(F.col("tmin_c").isNotNull() & (F.col("tmin_c") <= 0), 1).otherwise(0))
            .withColumn("is_hot", F.when(F.col("tmax_c").isNotNull() & (F.col("tmax_c") >= 30), 1).otherwise(0))
            
            # === PRECIPITATION - CONTINUOUS ONLY ===
            .withColumn(
                "precipitation_mm",
                F.when(
                    prcp_s.isNotNull() & (prcp_s != "") & (prcp_s != "T"),
                    prcp_s.cast("double") / 10.0
                ).otherwise(F.lit(0.0))
            )
            
            # === SNOW - SIMPLE BINARY ===
            .withColumn(
                "is_snow",
                F.when(
                    snow_s.isNotNull() & (snow_s != "") & (snow_s != "T") & (snow_s.cast("double") > 0),
                    1
                ).otherwise(0)
            )
            
            # === WIND - CONTINUOUS ONLY ===
            .withColumn(
                "wind_speed_kmh",
                F.when(
                    awnd_s.isNotNull() & (awnd_s != "") & (awnd_s != "T"),
                    awnd_s.cast("double") * 0.036
                ).otherwise(F.lit(0.0))
            )
            
            # === FOG - CONSOLIDATED FLAG ===
            .withColumn("fog_mist_raw", F.when(F.array_contains(F.split(F.coalesce(F.col("WT01"), F.lit("")), ""), "1"), 1).otherwise(0))
            .withColumn("heavy_fog_raw", F.when(F.array_contains(F.split(F.coalesce(F.col("WT05"), F.lit("")), ""), "1"), 1).otherwise(0))
            .withColumn("fog_flag", F.when((F.col("fog_mist_raw") == 1) | (F.col("heavy_fog_raw") == 1), 1).otherwise(0))  # KEEP - consolidated
            
            # === WEATHER COMFORT INDEX ===
            .withColumn("weather_comfort_index", 
                F.when(
                    (F.col("temp_avg_calculated") >= 15) & (F.col("temp_avg_calculated") <= 25) &
                    (F.col("precipitation_mm") == 0) & (F.col("wind_speed_kmh") < 20),
                    5  # Perfect
                ).when(
                    (F.col("temp_avg_calculated") >= 10) & (F.col("temp_avg_calculated") <= 30) &
                    (F.col("precipitation_mm") < 5) & (F.col("wind_speed_kmh") < 30),
                    4  # Good
                ).when(
                    (F.col("temp_avg_calculated") >= 0) & (F.col("temp_avg_calculated") <= 35) &
                    (F.col("precipitation_mm") < 10),
                    3  # Fair
                ).when(
                    (F.col("temp_avg_calculated") >= -10) & (F.col("temp_avg_calculated") <= 40),
                    2  # Poor
                ).otherwise(1)  # Very Poor
            )  # KEEP - composite metric
            
            # === SEASONAL FEATURES ===
            .withColumn("month", F.month("weather_date"))
            .withColumn("season", 
                F.when(F.col("month").isin([12, 1, 2]), "winter")
                .when(F.col("month").isin([3, 4, 5]), "spring")
                .when(F.col("month").isin([6, 7, 8]), "summer")
                .otherwise("fall")
            )  # KEEP - seasonal patterns
        )
        
        # === DATA VALIDATION (improved) ===
        print("\n🔍 Validating data quality...")
        
        weather_validated = (weather_features
            .filter(
                (F.col("tmax_c").isNull()) | 
                (F.col("tmin_c").isNull()) | 
                (F.col("tmax_c") >= F.col("tmin_c"))
            )
            .filter(
                (F.col("tmax_c").isNull()) | 
                ((F.col("tmax_c") >= -30) & (F.col("tmax_c") <= 45))
            )
            .filter(
                (F.col("tmin_c").isNull()) | 
                ((F.col("tmin_c") >= -35) & (F.col("tmin_c") <= 35))
            )
            .filter(F.col("weather_date").isNotNull())
        )
        
        # === STATION AGGREGATION - OPTIMIZED VERSION ===
        print("\n📊 Aggregating stations with optimized features...")
        
        # Get list of stations
        stations = weather_validated.select("STATION").distinct().collect()
        station_list = [row["STATION"] for row in stations]
        
        if len(station_list) > 1:
            print(f"📊 Aggregating {len(station_list)} stations...")
            
            weather_aggregated = (weather_validated
                .groupBy("weather_date")
                .agg(
                    # === OPTIMIZED FEATURE SET ONLY ===
                    F.avg("temp_avg_calculated").alias("temp_avg_calculated"),      # KEEP
                    F.max("is_freezing").alias("is_freezing"),                     # KEEP
                    F.max("is_hot").alias("is_hot"),                               # KEEP
                    F.max("precipitation_mm").alias("precipitation_mm"),           # KEEP
                    F.max("is_snow").alias("is_snow"),                             # KEEP
                    F.avg("wind_speed_kmh").alias("wind_speed_kmh"),               # KEEP
                    F.max("fog_flag").alias("fog_flag"),                           # KEEP - consolidated
                    F.avg("weather_comfort_index").alias("weather_comfort_index"), # KEEP
                    F.first("season").alias("season"),                             # KEEP
                    F.first("month").alias("month")                                # KEEP for processing
                    
                                   # REMOVED - rare
                )
            )
        else:
            print("📊 Using single station data...")
            weather_aggregated = weather_validated.select([
                "weather_date", "temp_avg_calculated", "is_freezing", "is_hot", 
                "precipitation_mm", "is_snow", "wind_speed_kmh", "fog_flag",
                "weather_comfort_index", "season", "month"
            ])
        
        # === FINAL SUMMARY ===
        final_count = weather_aggregated.count()
        optimized_features = len(weather_aggregated.columns)
        
        print(f"\n✅ OPTIMIZED PROCESSING COMPLETE")
        print(f"📊 Final dataset: {final_count:,} daily records")
        print(f"🎯 Optimized features: {optimized_features} columns (vs ~30+ in original)")
        
        print(f"\n🔧 OPTIMIZED FEATURE SET:")
        print(f"  🌡️ Temperature: temp_avg_calculated, is_freezing, is_hot")
        print(f"  🌧️ Precipitation: precipitation_mm")  
        print(f"  ❄️ Snow: is_snow")
        print(f"  💨 Wind: wind_speed_kmh")
        print(f"  🌫️ Fog: fog_flag (consolidated)")
        print(f"  📊 Composite: weather_comfort_index, season")
        
        # Show sample (safely)
        print(f"\n📋 Sample of optimized data:")
        try:
            sample_data = weather_aggregated.select(
                "weather_date", "temp_avg_calculated", "is_freezing", "is_hot",
                "precipitation_mm", "is_snow", "wind_speed_kmh", "fog_flag", 
                "weather_comfort_index", "season"
            ).limit(5).collect()
            
            for row in sample_data:
                print(f"   📅 {row['weather_date']} | 🌡️ {row['temp_avg_calculated']:.1f}°C | " +
                      f"❄️ {row['is_freezing']} | 🔥 {row['is_hot']} | " +
                      f"🌧️ {row['precipitation_mm']:.1f}mm")
        except Exception as e:
            print(f"   ⚠️ Sample display skipped due to data issues: {e}")
        
        return weather_aggregated
        
    except Exception as e:
        print(f"❌ Processing failed: {e}")
        import traceback
        print(traceback.format_exc())
        return None


def assess_noaa_weather_data_quality():
    """
    Comprehensive data quality assessment of NOAA weather data.
    
    Returns:
        dict: Data quality assessment results
    """
    
    print("🔍 COMPREHENSIVE NOAA WEATHER DATA QUALITY ASSESSMENT")
    print("="*60)
    
    try:
        # Get Spark session
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.appName("WeatherQualityAssessment").getOrCreate()
        # Load raw data
        weather_csv = "./lake/landing/lookups/NOAA nyc weather.csv"
        weather_raw = spark.read.csv(weather_csv, header=True)
        
        total_records = weather_raw.count()
        print(f"📊 Total records loaded: {total_records:,}")
        
        # === 1. BASIC DATA STRUCTURE ANALYSIS ===
        print(f"\n1️⃣ DATA STRUCTURE ANALYSIS")
        print("-" * 30)
        
        print(f"📋 Schema overview:")
        weather_raw.printSchema()
        
        total_columns = len(weather_raw.columns)
        print(f"📊 Total columns: {total_columns}")
        
        # === 2. STATION ANALYSIS ===
        print(f"\n2️⃣ WEATHER STATION ANALYSIS")
        print("-" * 30)
        
        # Station distribution
        station_stats = weather_raw.groupBy("STATION", "NAME").count().orderBy(F.desc("count"))
        station_count = station_stats.count()
        
        print(f"📍 Total weather stations: {station_count}")
        print(f"📊 Station distribution:")
        try:
            station_stats.show(10, truncate=False)
        except Exception as e:
            print(f"   ⚠️ Station display skipped: {e}")
        
        # Get station data coverage
        station_coverage = station_stats.collect()
        
        # === 3. DATE RANGE ANALYSIS ===
        print(f"\n3️⃣ DATE RANGE ANALYSIS")
        print("-" * 30)
        
        # Convert dates and analyze range
        date_analysis = (weather_raw
            .withColumn("parsed_date", F.to_date("DATE", "yyyy-MM-dd"))
            .filter(F.col("parsed_date").isNotNull())
        )
        
        date_range = date_analysis.agg(
            F.min("parsed_date").alias("start_date"),
            F.max("parsed_date").alias("end_date"),
            F.count("parsed_date").alias("valid_dates")
        ).collect()[0]
        
        print(f"📅 Date range: {date_range['start_date']} to {date_range['end_date']}")
        print(f"📊 Valid date records: {date_range['valid_dates']:,}")
        
        # Date completeness by year
        yearly_coverage = (date_analysis
            .withColumn("year", F.year("parsed_date"))
            .groupBy("year").count()
            .orderBy("year")
        )
        
        print(f"📈 Yearly coverage:")
        try:
            yearly_coverage.show()
        except Exception as e:
            print(f"   ⚠️ Yearly coverage display skipped: {e}")
        
        # === 4. DATA COMPLETENESS ANALYSIS ===
        print(f"\n4️⃣ DATA COMPLETENESS ANALYSIS")
        print("-" * 30)
        
        # Key weather variables
        key_variables = ['TMAX', 'TMIN', 'TAVG', 'PRCP', 'SNOW', 'AWND']
        
        completeness_stats = {}
        
        for var in key_variables:
            if var in weather_raw.columns:
                non_null_count = weather_raw.filter(
                    (F.col(var).isNotNull()) & 
                    (F.col(var) != "") & 
                    (F.col(var) != "T")  # T means trace amount
                ).count()
                
                completeness_pct = (non_null_count / total_records) * 100
                completeness_stats[var] = {
                    'non_null_count': non_null_count,
                    'completeness_pct': completeness_pct
                }
                
                print(f"🌡️ {var}: {non_null_count:,}/{total_records:,} ({completeness_pct:.1f}%)")
        
        # === 5. STATION-SPECIFIC COMPLETENESS ===
        print(f"\n5️⃣ STATION-SPECIFIC DATA QUALITY")
        print("-" * 30)
        
        # Analyze top stations for data completeness
        top_stations = [row['STATION'] for row in station_coverage[:5]]
        
        for station in top_stations:
            station_data = weather_raw.filter(F.col("STATION") == station)
            station_count = station_data.count()
            
            # Check temperature data availability
            temp_data = station_data.filter(
                (F.col("TMAX").isNotNull() & (F.col("TMAX") != "")) |
                (F.col("TMIN").isNotNull() & (F.col("TMIN") != ""))
            ).count()
            
            temp_completeness = (temp_data / station_count) * 100
            
            station_name = station_data.select("NAME").first()[0]
            print(f"📍 {station} ({station_name[:50]}...)")
            print(f"   📊 Total records: {station_count:,}")
            print(f"   🌡️ Temperature data: {temp_data:,} ({temp_completeness:.1f}%)")
            print()
        
        # === 6. MISSING DATA PATTERNS ===
        print(f"\n6️⃣ MISSING DATA PATTERNS")
        print("-" * 30)
        
        # Analyze missing data patterns
        missing_analysis = weather_raw.select([
            F.count(F.when(F.col(c).isNull() | (F.col(c) == ""), c)).alias(f"{c}_missing")
            for c in key_variables if c in weather_raw.columns
        ])
        
        missing_results = missing_analysis.collect()[0]
        
        print(f"📊 Missing data summary:")
        for var in key_variables:
            if f"{var}_missing" in missing_results.asDict():
                missing_count = missing_results[f"{var}_missing"]
                missing_pct = (missing_count / total_records) * 100
                print(f"❌ {var}: {missing_count:,} missing ({missing_pct:.1f}%)")
        
        # === 7. DATA QUALITY ISSUES ===
        print(f"\n7️⃣ DATA QUALITY ISSUES")
        print("-" * 30)
        
        quality_issues = []
        
        # Check for unrealistic temperature values
        extreme_temps = weather_raw.filter(
            (F.col("TMAX").cast("double") > 120) |  # >120°F unrealistic
            (F.col("TMAX").cast("double") < -50) |  # <-50°F unrealistic  
            (F.col("TMIN").cast("double") > 100) |  # >100°F unrealistic for min
            (F.col("TMIN").cast("double") < -60)    # <-60°F unrealistic
        ).count()
        
        if extreme_temps > 0:
            quality_issues.append(f"Extreme temperature values: {extreme_temps:,} records")
            print(f"🚨 Extreme temperature values: {extreme_temps:,} records")
        
        # Check for negative precipitation  
        negative_precip = weather_raw.filter(F.col("PRCP").cast("double") < 0).count()
        if negative_precip > 0:
            quality_issues.append(f"Negative precipitation: {negative_precip:,} records")
            print(f"🚨 Negative precipitation: {negative_precip:,} records")
        
        # Check for TMAX < TMIN
        temp_inconsistency = weather_raw.filter(
            (F.col("TMAX").isNotNull()) & (F.col("TMIN").isNotNull()) &
            (F.col("TMAX").cast("double") < F.col("TMIN").cast("double"))
        ).count()
        
        if temp_inconsistency > 0:
            quality_issues.append(f"TMAX < TMIN inconsistencies: {temp_inconsistency:,} records")
            print(f"🚨 TMAX < TMIN inconsistencies: {temp_inconsistency:,} records")
        
        if not quality_issues:
            print(f"✅ No major data quality issues detected")
        
        # === 8. RECOMMENDATIONS ===
        print(f"\n8️⃣ DATA QUALITY RECOMMENDATIONS")
        print("-" * 30)
        
        recommendations = []
        
        # Find best station for analysis
        best_station = None
        best_completeness = 0
        
        for station in top_stations:
            station_data = weather_raw.filter(F.col("STATION") == station)
            temp_complete = station_data.filter(
                (F.col("TMAX").isNotNull() & (F.col("TMAX") != "")) &
                (F.col("TMIN").isNotNull() & (F.col("TMIN") != ""))
            ).count()
            
            total_station_records = station_data.count()
            completeness = temp_complete / total_station_records
            
            if completeness > best_completeness:
                best_completeness = completeness
                best_station = station
        
        if best_station:
            recommendations.append(f"Use {best_station} as primary station ({best_completeness:.1%} temperature completeness)")
            print(f"✅ Primary station recommendation: {best_station} ({best_completeness:.1%} completeness)")
        
        # Missing data handling
        temp_completeness_overall = completeness_stats.get('TMAX', {}).get('completeness_pct', 0)
        if temp_completeness_overall < 80:
            recommendations.append("Consider interpolation or multiple station aggregation for missing temperature data")
            print(f"⚠️ Temperature data completeness low ({temp_completeness_overall:.1f}%) - consider interpolation")
        
        if completeness_stats.get('PRCP', {}).get('completeness_pct', 0) > 70:
            recommendations.append("Precipitation data sufficient for analysis")
            print(f"✅ Precipitation data quality sufficient")
        
        # === SUMMARY ===
        print(f"\n" + "="*60)
        print(f"📋 DATA QUALITY ASSESSMENT SUMMARY")
        print(f"="*60)
        
        return {
            'total_records': total_records,
            'station_count': station_count,
            'date_range': {
                'start': str(date_range['start_date']),
                'end': str(date_range['end_date']),
                'valid_dates': date_range['valid_dates']
            },
            'completeness_stats': completeness_stats,
            'quality_issues': quality_issues,
            'recommendations': recommendations,
            'best_station': best_station,
            'best_station_completeness': best_completeness
        }
        
    except Exception as e:
        print(f"❌ Data quality assessment failed: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return None


def save_optimized_weather_to_external_features(weather_df, base_path="./lake/external_features/weather"):
    """
    Save optimized weather data to external features directory in the monthly partitioned format.
    
    Args:
        weather_df: Optimized weather DataFrame with weather_date column
        base_path: Base path for external features (default: "./lake/external_features/weather")
    
    Returns:
        dict: Summary of saved data
    """
    
    print("💾 SAVING OPTIMIZED WEATHER DATA TO EXTERNAL FEATURES")
    print("="*60)
    
    try:
        # Get Spark session
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.appName("SaveOptimizedWeather").getOrCreate()
        # Add year and month columns for partitioning
        weather_partitioned = (weather_df
            .withColumn("year", F.year("weather_date"))
            .withColumn("month", F.month("weather_date"))
        )
        
        # Get the date range for summary
        date_stats = weather_partitioned.agg(
            F.min("weather_date").alias("min_date"),
            F.max("weather_date").alias("max_date"),
            F.count("weather_date").alias("total_records")
        ).collect()[0]
        
        print(f"📊 Data to save:")
        print(f"   📅 Date range: {date_stats['min_date']} to {date_stats['max_date']}")
        print(f"   📊 Total records: {date_stats['total_records']:,}")
        print(f"   🎯 Features: {len(weather_df.columns)}")
        
        # Save with year/month partitioning
        print(f"\n💾 Saving to: {base_path}/")
        
        (weather_partitioned
         .write
         .mode("overwrite")
         .partitionBy("year", "month") 
         .parquet(base_path))
        
        print(f"✅ Weather data saved successfully!")
        
        # Verify what was saved
        saved_data = spark.read.parquet(base_path)
        saved_count = saved_data.count()
        saved_cols = len(saved_data.columns)
        
        print(f"\n🔍 Verification:")
        print(f"   📊 Saved records: {saved_count:,}")
        print(f"   🎯 Saved features: {saved_cols}")
        
        # Show partition structure
        print(f"\n📁 Partition structure:")
        partitions = saved_data.select("year", "month").distinct().orderBy("year", "month").collect()
        for partition in partitions:
            print(f"   📂 year={partition['year']}/month={partition['month']:02d}")
        
        return {
            'status': 'success',
            'total_records': saved_count,
            'features_count': saved_cols,
            'date_range': {
                'start': str(date_stats['min_date']),
                'end': str(date_stats['max_date'])
            },
            'partitions': len(partitions)
        }
        
    except Exception as e:
        print(f"❌ Failed to save weather data: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return {
            'status': 'failed',
            'error': str(e)
        }

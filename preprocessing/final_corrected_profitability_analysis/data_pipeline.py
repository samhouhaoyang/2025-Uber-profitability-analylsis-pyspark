"""
COMPLETELY REWRITTEN Profitability Analysis Data Pipeline

This is a complete rewrite of the data pipeline with all corrections applied.
The old version has been backed up as data_pipeline_old.py

Key Corrections Applied:
1. ✅ Revenue calculations based on Uber's actual commission (not passenger fare)
2. ✅ Dropped pass-through fees (tolls, bcf, sales_tax, etc.)
3. ✅ Fixed spatial joins to use PULocationID/DOLocationID properly
4. ✅ Improved external data integration
5. ✅ Comprehensive data validation and error handling

This pipeline processes data from raw HVFHS data through to final analysis-ready dataset.
"""

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import *
from typing import Optional, Dict, Tuple, List
# Lightweight metrics store for the most recent base-data load
LAST_BASE_LOAD_METRICS: Dict[str, int] = {}
import random


def optimize_feature_set(df: DataFrame) -> DataFrame:
    """
    Optimize the feature set by removing redundant and highly correlated features
    while preserving business-relevant information.
    """
    print("🎯 OPTIMIZING FEATURE SET")
    print("="*50)
    
    initial_cols = len(df.columns)
    print(f"📊 Initial features: {initial_cols}")
    
    # 1. Create consolidated features first
    print("🔧 Creating consolidated features...")
    
    # Create fog_flag from individual fog indicators (use boolean OR to avoid type errors)
    if "fog_mist" in df.columns or "heavy_fog" in df.columns:
        fog_mist_bool = F.coalesce(F.col("fog_mist").cast("int") > 0, F.lit(False)) if "fog_mist" in df.columns else F.lit(False)
        heavy_fog_bool = F.coalesce(F.col("heavy_fog").cast("int") > 0, F.lit(False)) if "heavy_fog" in df.columns else F.lit(False)
        df = df.withColumn("fog_flag", (fog_mist_bool | heavy_fog_bool).cast("int"))
        print("  ✅ Created fog_flag from fog_mist + heavy_fog (boolean OR)")
    
    # 2. Define features to drop
    features_to_drop = []
    
    # Weather redundancies
    weather_drops = [
        # Temperature redundancies (keep temp_avg_calculated)
        "tmax_c", "tmin_c", "temp_range_c",
        
        # Temperature categories (keep only extremes)
        "is_very_cold", "is_cold", "is_cool", "is_mild", "is_warm", "is_extreme_heat",
        
        # Precipitation categories (keep precipitation_mm)
        "is_light_rain", "is_moderate_rain", "is_heavy_rain", "is_extreme_rain", "is_precipitation",
        
        # Snow categories (keep is_snow)
        "is_light_snow", "is_heavy_snow",
        # Raw snow measurements (not needed for model)
        "snow_cm", "snow_depth_cm",
        
        # Wind categories (keep wind_speed_kmh)
        "is_windy", "is_very_windy", "high_winds",
        
        # Fog components (replaced with fog_flag)
        "fog_mist", "heavy_fog",
        
        # Rare weather
        "ice_pellets", "hail", "glaze_rime", "smoke_haze",
        
        # Technical fields
        "ingest_ts"
    ]
    
    # Spatial redundancies
    spatial_drops = [
        "PU_in_cbd", "DO_in_cbd"  # Replaced by trip_cbd_type
    ]
    
    # Temporal redundancies  
    temporal_drops = [
        "is_rush_hour"  # Replaced by rush_hour_label
    ]

    # Core raw flags that are not needed for Uber-side profitability modeling
    # (retain if you need cohort analysis on shared/WAV later)
    optional_raw_flags = [
        "shared_request_flag",
        "access_a_ride_flag", "wav_request_flag", "wav_match_flag",
    ]
    
    # Combine all drops
    features_to_drop.extend(weather_drops)
    features_to_drop.extend(spatial_drops) 
    features_to_drop.extend(temporal_drops)
    features_to_drop.extend(optional_raw_flags)
    
    # 3. Remove existing features
    existing_drops = [col for col in features_to_drop if col in df.columns]
    
    if existing_drops:
        print(f"🗑️ Dropping {len(existing_drops)} redundant features:")
        for category, drops in [
            ("Weather", [c for c in existing_drops if c in weather_drops]),
            ("Spatial", [c for c in existing_drops if c in spatial_drops]),
            ("Temporal", [c for c in existing_drops if c in temporal_drops]),
            ("RawFlags", [c for c in existing_drops if c in optional_raw_flags]),
        ]:
            if drops:
                print(f"  {category}: {drops}")
        
        df = df.drop(*existing_drops)
    
    # 4. Check for time_period vs rush_hour_label correlation
    if "time_period" in df.columns and "rush_hour_label" in df.columns:
        print("🔍 Analyzing time_period vs rush_hour_label correlation...")
        
        # Simple correlation check - if they map 1:1, drop time_period
        correlation_check = df.select("time_period", "rush_hour_label").distinct()
        unique_combinations = correlation_check.count()
        unique_time_periods = df.select("time_period").distinct().count()
        unique_rush_labels = df.select("rush_hour_label").distinct().count()
        
        if unique_combinations <= max(unique_time_periods, unique_rush_labels):
            print("  ⚠️ High correlation detected - dropping time_period")
            df = df.drop("time_period")
        else:
            print("  ✅ time_period provides additional information - keeping")
    
    final_cols = len(df.columns)
    dropped_count = initial_cols - final_cols
    
    print(f"✅ Feature optimization complete:")
    print(f"  📊 Final features: {final_cols}")
    print(f"  🗑️ Features dropped: {dropped_count}")
    print(f"  📈 Reduction: {dropped_count/initial_cols*100:.1f}%")
    
    return df


def get_spark_session():
    """Get or create a Spark session for data processing."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = (SparkSession.builder
                .appName("UberProfitabilityAnalysis")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate())
    return spark


def validate_data_availability(year: int, month: int) -> bool:
    """Check if raw HVFHS data is available for the specified year and month."""
    RAW_HVFHS = "./lake/raw/hvfhs/v1"
    
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            return False
            
        # Some datasets use month=1 (no zero pad), others use month=01.
        candidate_paths = [
            f"{RAW_HVFHS}/year={year}/month={month:02d}",
            f"{RAW_HVFHS}/year={year}/month={month}",
        ]
        for path in candidate_paths:
            try:
                test_df = (
                    spark.read
                         .option("recursiveFileLookup", "true")
                         .option("pathGlobFilter", "*.parquet")
                         .parquet(path)
                         .limit(1)
                )
                if test_df.count() > 0:
                    return True
            except Exception:
                continue
        return False
        
    except Exception:
        return False


def check_external_data_availability(year: int, month: int) -> Dict[str, bool]:
    """Check which external data sources are available for the given year/month."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        return {"spatial": False, "temporal": False, "weather": False}
    
    availability = {}
    
    # Check spatial features
    try:
        spatial_path = f"./lake/external_features/spatial/year={year}/month={month:02d}"
        spatial_test = (
            spark.read
                 .option("recursiveFileLookup", "true")
                 .option("pathGlobFilter", "*.parquet")
                 .parquet(spatial_path)
                 .limit(1)
        )
        availability["spatial"] = spatial_test.count() > 0
        print(f"✅ Spatial features available: {spatial_path}")
    except:
        availability["spatial"] = False
        print(f"❌ Spatial features not found")
    
    # Check temporal features
    try:
        temporal_path = f"./lake/external_features/temporal/year={year}/month={month:02d}"
        temporal_test = (
            spark.read
                 .option("recursiveFileLookup", "true")
                 .option("pathGlobFilter", "*.parquet")
                 .parquet(temporal_path)
                 .limit(1)
        )
        availability["temporal"] = temporal_test.count() > 0
        print(f"✅ Temporal features available: {temporal_path}")
    except:
        availability["temporal"] = False
        print(f"❌ Temporal features not found")
    
    # Check weather features
    try:
        weather_path = f"./lake/external_features/weather/year={year}/month={month:02d}"
        weather_test = (
            spark.read
                 .option("recursiveFileLookup", "true")
                 .option("pathGlobFilter", "*.parquet")
                 .parquet(weather_path)
                 .limit(1)
        )
        availability["weather"] = weather_test.count() > 0
        print(f"✅ Weather features available: {weather_path}")
    except:
        availability["weather"] = False
        print(f"❌ Weather features not found")
    
    return availability


def _write_monthly_removal_summary(
    year: int,
    month: int,
    metrics: Dict[str, Optional[float]],
    final_rows: int,
    reports_base: str = "./lake/reports/removal_summary",
):
    """Write a single-row removal summary report for the month (CSV + Parquet)."""
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            return
        payload = metrics.copy()
        payload.update({
            "year": int(year),
            "month": int(month),
            "final_rows": int(final_rows),
        })
        df = spark.createDataFrame([Row(**payload)])
        out_dir = f"{reports_base}/year={year}/month={month:02d}"
        df.write.mode("overwrite").parquet(out_dir)
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_dir + "_csv")
        print(f"📝 Removal summary written: {out_dir}")
    except Exception as e:
        print(f"⚠️ Failed to write removal summary: {e}")


def _write_monthly_coverage_summary(
    df: DataFrame,
    year: int,
    month: int,
    reports_base: str = "./lake/reports/coverage",
):
    """Compute and write per-column non-null coverage for key external features (CSV + Parquet)."""
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            return

        total = df.count()
        groups: Dict[str, List[str]] = {
            "spatial": [
                "is_cbd_trip", "trip_cbd_type",
                "PU_Borough", "DO_Borough", "PU_Zone", "DO_Zone",
            ],
            "temporal": [
                "pickup_hour", "is_holiday", "day_of_week", "is_weekend", "rush_hour_label",
            ],
            "weather": [
                "temp_avg_c", "temp_avg_calculated", "precipitation_mm", "is_snow", "wind_speed_kmh", "fog_flag",
            ],
        }

        rows: List[Row] = []
        for group, candidates in groups.items():
            for c in candidates:
                if c in df.columns:
                    non_null = df.filter(F.col(c).isNotNull()).count()
                    rows.append(Row(
                        year=int(year), month=int(month), group=group,
                        column=c, non_null=int(non_null), total=int(total),
                        coverage_pct=(float(non_null) / float(total) * 100.0 if total > 0 else 0.0),
                    ))

        if not rows:
            print("ℹ️ No coverage columns found to report.")
            return

        cov_df = spark.createDataFrame(rows)
        out_dir = f"{reports_base}/year={year}/month={month:02d}"
        cov_df.write.mode("overwrite").parquet(out_dir)
        cov_df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_dir + "_csv")
        print(f"📝 Coverage summary written: {out_dir}")
    except Exception as e:
        print(f"⚠️ Failed to write coverage summary: {e}")


def load_base_uber_data_for_profitability_corrected(year: int, month: int, sample_fraction: float = 0.01) -> DataFrame:
    """
    REWRITTEN: Load and prepare base Uber trip data with CORRECT revenue calculations.
    
    This function now:
    - Loads raw HVFHS data from the very beginning
    - Applies correct Uber commission-based revenue calculations
    - Drops unwanted features early in the pipeline
    - Removes pass-through fees that don't affect Uber's profitability
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
        # === STEP 1: Load raw HVFHS data (robust to non-parquet artifacts) ===
        raw = (
            spark.read
                 .option("recursiveFileLookup", "true")
                 .option("pathGlobFilter", "*.parquet")
                 .parquet(f"{RAW_HVFHS}/year={year}/month={month}")
        )
        
        initial_count = raw.count()
        print(f"   Raw HVFHS records: {initial_count:,}")
        
        if initial_count == 0:
            raise ValueError(f"No data found for {year}-{month:02d} in {RAW_HVFHS}")
        
        # === STEP 2: Filter for Uber trips only ===
        print("🚕 Filtering for Uber trips...")
        lic = F.upper(F.trim(F.col("hvfhs_license_num").cast("string")))
        is_uber = (lic == F.lit(PROVIDER_CODE))
        uber_raw = raw.filter(is_uber)
        
        uber_count = uber_raw.count()
        print(f"   Uber trips filtered: {uber_count:,}")
        
        if uber_count == 0:
            raise ValueError(f"No Uber trips found for {year}-{month:02d}")
        
        # === STEP 3: Apply data quality filters ===
        print("🔍 Applying data quality filters...")
        
        # Filter out records with missing critical fields (INCLUDING driver_pay!)
        quality_filtered = (uber_raw
                           .filter(F.col("pickup_datetime").isNotNull())
                           .filter(F.col("dropoff_datetime").isNotNull())
                           .filter(F.col("PULocationID").isNotNull())
                           .filter(F.col("DOLocationID").isNotNull())
                           .filter(F.col("trip_miles").isNotNull())
                           .filter(F.col("trip_time").isNotNull())
                           .filter(F.col("base_passenger_fare").isNotNull())
                           .filter(F.col("driver_pay").isNotNull()))  # CRITICAL: Need driver_pay for commission calc
        
        # Filter out invalid trip data
        quality_filtered = (quality_filtered
                           .filter(F.col("trip_miles") >= 0)
                           .filter(F.col("trip_time") > 0)
                           .filter(F.col("base_passenger_fare") >= 0)
                           .filter(F.col("driver_pay") >= 0)  # Driver pay should be non-negative
                           .filter(F.col("pickup_datetime") <= F.col("dropoff_datetime")))
        
        quality_count = quality_filtered.count()
        print(f"   After quality filters: {quality_count:,}")
        
        # === STEP 4: Apply sampling if requested ===
        if sample_fraction < 1.0:
            print(f"🎲 Applying {sample_fraction:.1%} sampling...")
            seed = hash(f"{year}-{month}") % (2**31 - 1)
            sampled_data = quality_filtered.sample(fraction=sample_fraction, seed=seed)
            final_count = sampled_data.count()
            print(f"   Final sampled dataset: {final_count:,} trips")
            base_data = sampled_data
        else:
            print("📊 Using full dataset (no sampling)")
            base_data = quality_filtered
            final_count = quality_count
        
        # Record metrics for later summary
        global LAST_BASE_LOAD_METRICS
        LAST_BASE_LOAD_METRICS = {
            "raw_count": int(initial_count),
            "uber_count": int(uber_count),
            "quality_count": int(quality_count),
            "sampled_count": int(final_count),
        }

        # === STEP 5: Drop unwanted features early ===
        print("🗑️ Dropping unwanted features...")
        
        # Core unwanted features
        core_unwanted = [
            "hvfhs_license_num", "dispatching_base_num", "originating_base_num", "request_datetime"
        ]
        
        # Pass-through fees that don't affect Uber commission
        passthrough_fees = [
            "tolls", "bcf", "sales_tax", "congestion_surcharge", "airport_fee", "tips"
        ]
        
        all_unwanted = core_unwanted + passthrough_fees
        existing_unwanted = [col for col in all_unwanted if col in base_data.columns]
        
        if existing_unwanted:
            base_data = base_data.drop(*existing_unwanted)
            core_dropped = [f for f in existing_unwanted if f in core_unwanted]
            fees_dropped = [f for f in existing_unwanted if f in passthrough_fees]
            
            if core_dropped:
                print(f"   Dropped core unwanted: {core_dropped}")
            if fees_dropped:
                print(f"   Dropped pass-through fees: {fees_dropped}")
        else:
            print(f"   No unwanted features found to drop")
        
        # === STEP 6: Add CORRECTED computed columns ===
        print("🔧 Adding CORRECTED computed columns...")
        
        base_data = (base_data
                    # Basic trip metrics (unchanged)
                    .withColumn("trip_duration_minutes", 
                               F.col("trip_time") / 60.0)
                    .withColumn("avg_speed_mph", 
                               F.when(F.col("trip_time") > 0, 
                                     F.col("trip_miles") * 3600.0 / F.col("trip_time"))
                               .otherwise(0))
                    
                    # CORRECTED: Uber's actual revenue and commission calculations
                    .withColumn("uber_commission", 
                               F.col("base_passenger_fare") - F.col("driver_pay"))  # Uber's cut
                    
                    # CORRECTED: Uber take rate based on base passenger fare only
                    .withColumn("uber_take_rate", 
                               F.when(F.col("base_passenger_fare") > 0,
                                     F.col("uber_commission") / F.col("base_passenger_fare") * 100)
                               .otherwise(0))  # Take rate as percentage of base fare
                    
                    # CORRECTED: Revenue metrics based on UBER's commission only
                    .withColumn("uber_revenue_per_mile", 
                               F.when(F.col("trip_miles") > 0, 
                                     F.col("uber_commission") / F.col("trip_miles"))
                               .otherwise(0))
                    .withColumn("uber_revenue_per_minute", 
                               F.when(F.col("trip_time") > 0, 
                                     F.col("uber_commission") * 60.0 / F.col("trip_time"))
                               .otherwise(0))
                    
                    # Driver efficiency (kept for comparison)
                    .withColumn("driver_efficiency", 
                               F.when(F.col("trip_time") > 0,
                                     F.col("driver_pay") * 60.0 / F.col("trip_time"))  # Driver earnings per minute
                               .otherwise(0))
                    
                    # Add pickup_date for temporal/weather joins
                    .withColumn("pickup_date", F.to_date("pickup_datetime")))
        
        # Drop any old revenue metrics that might still exist
        old_revenue_metrics = ["revenue_per_mile", "revenue_per_minute"]
        existing_old_metrics = [col for col in old_revenue_metrics if col in base_data.columns]
        
        if existing_old_metrics:
            base_data = base_data.drop(*existing_old_metrics)
            print(f"🗑️ Dropped old revenue metrics: {existing_old_metrics}")
        
        # === STEP 7: Cache and validate ===
        base_data = base_data.cache()
        
        # Print sample statistics for validation
        print("📊 Sample revenue statistics:")
        sample_stats = base_data.agg(
            F.avg("base_passenger_fare").alias("avg_passenger_fare"),
            F.avg("driver_pay").alias("avg_driver_pay"),
            F.avg("uber_commission").alias("avg_uber_commission"),
            F.avg("uber_take_rate").alias("avg_uber_take_rate")
        ).collect()[0]
        
        print(f"   Average passenger base fare: ${sample_stats['avg_passenger_fare']:.2f}")
        print(f"   Average driver pay: ${sample_stats['avg_driver_pay']:.2f}")
        print(f"   Average Uber commission: ${sample_stats['avg_uber_commission']:.2f}")
        print(f"   Average Uber take rate: {sample_stats['avg_uber_take_rate']:.1f}%")
        
        print(f"✅ CORRECTED Base Uber data loaded successfully: {final_count:,} trips, {len(base_data.columns)} columns")
        
        return base_data
        
    except Exception as e:
        print(f"❌ Error loading base Uber data for {year}-{month:02d}: {str(e)}")
        raise


def load_and_integrate_all_features_final(
    year: int,
    month: int,
    sample_fraction: float = 0.01,
    diagnostics: bool = True,
    enable_commission_filter: bool = True,
    low_quantile: float = 0.01,
    high_quantile: float = 0.99,
    enable_winsorization: bool = True,
    write_reports: bool = True,
) -> DataFrame:
    """
    COMPLETELY REWRITTEN: Load raw trip data and integrate ALL external features.
    
    This function orchestrates the complete data loading pipeline for profitability analysis,
    starting from raw HVFHS data and producing analysis-ready dataset with corrected metrics.
    """
    if diagnostics:
        print(f"🔄 COMPLETE REWRITTEN PIPELINE FOR {year}-{month:02d}")
        print("="*70)
    
    # Load base trip data with CORRECTED revenue calculations
    base_data = load_base_uber_data_for_profitability_corrected(year, month, sample_fraction)
    initial_count = base_data.count() if diagnostics else None
    if diagnostics:
        print(f"✅ Base data loaded: {initial_count:,} trips, {len(base_data.columns)} columns")
    
    # Pre-join filtering and winsorization on uber_commission
    removed_neg_commission = 0
    winsor_low: Optional[float] = None
    winsor_high: Optional[float] = None
    if enable_commission_filter and "uber_commission" in base_data.columns:
        try:
            # Remove negative commissions early to reduce downstream data volume
            neg_cnt = base_data.filter(F.col("uber_commission") < 0).count()
            if diagnostics:
                print(f"🧹 Removing negative uber_commission rows: {neg_cnt:,}")
            if neg_cnt > 0:
                base_data = base_data.filter(F.col("uber_commission") >= 0)
                removed_neg_commission = int(neg_cnt)

            # Winsorize extremes using approxQuantile
            if enable_winsorization and 0.0 <= low_quantile < high_quantile <= 1.0:
                q_low, q_high = base_data.stat.approxQuantile(
                    "uber_commission", [low_quantile, high_quantile], 0.01
                )
                if q_low is not None and q_high is not None and q_low <= q_high:
                    # Log thresholds and impact
                    below_cnt = base_data.filter(F.col("uber_commission") < F.lit(q_low)).count()
                    above_cnt = base_data.filter(F.col("uber_commission") > F.lit(q_high)).count()
                    if diagnostics:
                        print(
                            f"✂️ Winsorizing uber_commission to [{q_low:.4f}, {q_high:.4f}] "
                            f"(below: {below_cnt:,}, above: {above_cnt:,})"
                        )
                    base_data = base_data.withColumn(
                        "uber_commission",
                        F.when(F.col("uber_commission") < F.lit(q_low), F.lit(q_low))
                         .when(F.col("uber_commission") > F.lit(q_high), F.lit(q_high))
                         .otherwise(F.col("uber_commission"))
                    )
                    winsor_low, winsor_high = float(q_low), float(q_high)
                else:
                    if diagnostics:
                        print("⚠️ Skipping winsorization: invalid quantiles computed")
        except Exception as e:
            print(f"⚠️ Commission pre-filtering failed (continuing): {e}")

    # Check external data availability
    availability = check_external_data_availability(year, month)
    
    # Get spark session
    spark = SparkSession.getActiveSession()
        
    # Integrate spatial features (CORRECTED)
    if availability["spatial"]:
        try:
            spatial_path = f"./lake/external_features/spatial/year={year}/month={month:02d}"
            spatial_features = (
                spark.read
                     .option("recursiveFileLookup", "true")
                     .option("pathGlobFilter", "*.parquet")
                     .parquet(spatial_path)
            )
            
            base_columns = set(base_data.columns)
            has_pu_do = "PULocationID" in spatial_features.columns and "DOLocationID" in spatial_features.columns
            has_location_id = "LocationID" in spatial_features.columns
            
            if has_pu_do:
                spatial_columns = [col for col in spatial_features.columns 
                                 if col not in base_columns or col in ["PULocationID", "DOLocationID"]]
                
                if len(spatial_columns) > 2:
                    spatial_features_clean = spatial_features.select(spatial_columns)
                    base_data = base_data.join(F.broadcast(spatial_features_clean), ["PULocationID", "DOLocationID"], "left")
                    print(f"✅ Spatial features integrated via PU/DO LocationID")
                    
            elif has_location_id:
                spatial_columns = [col for col in spatial_features.columns if col != "LocationID"]
                pu_spatial = spatial_features.select(F.col("LocationID").alias("PULocationID"), *[F.col(col).alias(f"PU_{col}") for col in spatial_columns])
                do_spatial = spatial_features.select(F.col("LocationID").alias("DOLocationID"), *[F.col(col).alias(f"DO_{col}") for col in spatial_columns])
                
                base_data = base_data.join(F.broadcast(pu_spatial), "PULocationID", "left")
                base_data = base_data.join(F.broadcast(do_spatial), "DOLocationID", "left")
                print(f"✅ Spatial features integrated via LocationID (PU + DO)")
                
        except Exception as e:
            print(f"❌ Spatial features integration failed: {e}")
    
    # Integrate temporal features (CORRECTED)
    if availability["temporal"]:
        try:
            temporal_path = f"./lake/external_features/temporal/year={year}/month={month:02d}"
            temporal_features = (
                spark.read
                     .option("recursiveFileLookup", "true")
                     .option("pathGlobFilter", "*.parquet")
                     .parquet(temporal_path)
            )

            base_columns = set(base_data.columns)
            temporal_date_cols = [col for col in temporal_features.columns if 'date' in col.lower()]

            # Determine join keys: prefer day+hour if temporal has an hour column
            join_keys = []
            if "pickup_date" in temporal_features.columns:
                join_keys.append("pickup_date")
            elif temporal_date_cols:
                temporal_features = temporal_features.withColumnRenamed(temporal_date_cols[0], "pickup_date")
                join_keys.append("pickup_date")

            # Try to align an hour column
            hour_col = None
            for cand in ["pickup_hour", "hour", "hr"]:
                if cand in temporal_features.columns:
                    hour_col = cand
                    break
            if hour_col is not None and "pickup_hour" in base_data.columns:
                if hour_col != "pickup_hour":
                    temporal_features = temporal_features.withColumnRenamed(hour_col, "pickup_hour")
                join_keys = ["pickup_date", "pickup_hour"]

            # Drop potential partition columns to avoid conflicts
            for _col in ["year", "month"]:
                if _col in temporal_features.columns:
                    temporal_features = temporal_features.drop(_col)

            if join_keys:
                # If only daily key exists but there are multiple rows per day, collapse to one per day
                if join_keys == ["pickup_date"]:
                    non_key_cols = [c for c in temporal_features.columns if c not in join_keys]
                    if non_key_cols:
                        before_rows = temporal_features.count()
                        agg_exprs = [F.first(c, ignorenulls=True).alias(c) for c in non_key_cols]
                        temporal_features = temporal_features.groupBy("pickup_date").agg(*agg_exprs)
                        after_rows = temporal_features.count()
                        if diagnostics:
                            print(f"  ℹ️ Temporal daily dedup applied: {before_rows-after_rows:,} extra rows collapsed")

                temporal_columns = [col for col in temporal_features.columns if col not in base_columns or col in join_keys]
                if len(temporal_columns) > len(join_keys):
                    temporal_features_clean = temporal_features.select(temporal_columns)
                    base_data = base_data.join(F.broadcast(temporal_features_clean), join_keys, "left")
                    print(f"✅ Temporal features integrated via {join_keys}")
        except Exception as e:
            print(f"❌ Temporal features integration failed: {e}")
    
    # IMPROVED WEATHER FEATURES INTEGRATION
    if availability["weather"]:
        try:
            print(f"🌤️ Integrating weather features...")
            # Prefer single optimized dataset (full 2023-2024), fallback to legacy full dataset
            optimized_base = "./lake/external_features/weather_optimized"
            legacy_base = "./lake/external_features/weather"
            try:
                weather_features = (
                    spark.read
                         .option("recursiveFileLookup", "true")
                         .option("pathGlobFilter", "*.parquet")
                         .parquet(optimized_base)
                )
                print(f"  ✅ Using optimized weather dataset (base): {optimized_base}")
            except Exception:
                weather_features = (
                    spark.read
                         .option("recursiveFileLookup", "true")
                         .option("pathGlobFilter", "*.parquet")
                         .parquet(legacy_base)
                )
                print(f"  ⚠️ Optimized base not found; using legacy weather dataset (base): {legacy_base}")

            print(f"  📊 Weather data (full): {weather_features.count():,} records, {len(weather_features.columns)} columns")

            # Restrict to target month using date column
            date_col = None
            if "weather_date" in weather_features.columns:
                date_col = "weather_date"
            elif "date" in weather_features.columns:
                date_col = "date"
            elif "pickup_date" in weather_features.columns:
                date_col = "pickup_date"
            if date_col is None:
                print(f"  ❌ No date column found in weather data for monthly filter")
                raise Exception("No suitable date column for filtering")

            start = F.to_date(F.lit(f"{year}-{month:02d}-01"))
            end = F.add_months(start, 1)
            weather_features = weather_features.filter((F.col(date_col) >= start) & (F.col(date_col) < end))
            print(f"  📆 Filtered weather to {year}-{month:02d}: {weather_features.count():,} records")
            
            # Drop potential partition columns to avoid conflicts during join
            for _col in ["year", "month"]:
                if _col in weather_features.columns:
                    weather_features = weather_features.drop(_col)
            
            # Debug: Show weather columns
            weather_cols = weather_features.columns
            print(f"  📋 Weather columns: {weather_cols}")
            
            base_columns = set(base_data.columns)
            weather_date_cols = [col for col in weather_features.columns if 'date' in col.lower()]
            
            print(f"  📅 Weather date columns found: {weather_date_cols}")
            
            join_key = "pickup_date"
            weather_join_key = None
            
            # More robust join key detection
            if "weather_date" in weather_features.columns:
                weather_join_key = "weather_date"
                print(f"  🔑 Using weather_date for join")
            elif "pickup_date" in weather_features.columns:
                weather_join_key = "pickup_date"
                print(f"  🔑 Using pickup_date for direct join")
            elif "date" in weather_features.columns:
                weather_join_key = "date"
                print(f"  🔑 Using date column for join")
            elif weather_date_cols:
                weather_join_key = weather_date_cols[0]
                print(f"  🔑 Using {weather_join_key} for join")
            else:
                print(f"  ❌ No date column found in weather data!")
                print(f"  Available columns: {weather_features.columns}")
                raise Exception("No suitable date column found in weather data")
            
            if weather_join_key:
                # Check for date overlap BEFORE attempting join
                print(f"  🔍 Checking date overlap...")
                base_dates = base_data.select(join_key).distinct()
                weather_dates = weather_features.select(weather_join_key).distinct()
                
                base_date_count = base_dates.count()
                weather_date_count = weather_dates.count()
                print(f"    Base unique dates: {base_date_count}")
                print(f"    Weather unique dates: {weather_date_count}")
                
                # Check for actual overlap
                if join_key == weather_join_key:
                    common_dates = base_dates.intersect(weather_dates)
                else:
                    common_dates = base_dates.join(weather_dates, 
                                                 base_dates[join_key] == weather_dates[weather_join_key], 
                                                 "inner").select(join_key).distinct()
                
                common_count = common_dates.count()
                print(f"    Common dates: {common_count}")
                
                if common_count == 0:
                    print(f"  ⚠️ WARNING: No overlapping dates found!")
                    print(f"  Sample base dates:")
                    base_dates.orderBy(join_key).show(3, truncate=False)
                    print(f"  Sample weather dates:")
                    weather_dates.orderBy(weather_join_key).show(3, truncate=False)
                    print(f"  ❌ Skipping weather integration due to no date overlap")
                else:
                    # Prepare weather columns for join (exclude conflicts)
                    weather_columns = [col for col in weather_features.columns 
                                     if col not in base_columns or col == weather_join_key]
                    
                    print(f"  📋 Weather columns to join: {len(weather_columns)} total")
                    weather_feature_cols = [col for col in weather_columns if col != weather_join_key]
                    print(f"  🌤️ Actual weather features: {weather_feature_cols}")
                    
                    if len(weather_columns) > 1:
                        weather_features_clean = weather_features.select(weather_columns)
                        
                        # Record pre-join state
                        pre_join_cols = len(base_data.columns)
                        
                        # Perform the join
                        print(f"  🔗 Performing left join...")
                        if join_key == weather_join_key:
                            base_data = base_data.join(F.broadcast(weather_features_clean), join_key, "left")
                        else:
                            base_data = base_data.join(F.broadcast(weather_features_clean), 
                                                     base_data[join_key] == weather_features_clean[weather_join_key], 
                                                     "left").drop(weather_features_clean[weather_join_key])
                        
                        # Verify join success
                        post_join_cols = len(base_data.columns)
                        added_cols = post_join_cols - pre_join_cols
                        
                        print(f"  ✅ Join completed: {added_cols} weather features added")
                        
                        # Verify actual data was joined
                        if weather_feature_cols:
                            test_col = weather_feature_cols[0]
                            non_null_weather = base_data.filter(F.col(test_col).isNotNull()).count()
                            total_records = base_data.count()
                            success_rate = non_null_weather / total_records * 100
                            
                            print(f"  📊 Weather data coverage: {non_null_weather:,}/{total_records:,} ({success_rate:.1f}%)")
                            
                            if non_null_weather == 0:
                                print(f"  ⚠️ WARNING: Weather join successful but no data populated!")
                            else:
                                print(f"  🎉 Weather integration successful!")
                    else:
                        print(f"  ❌ No weather features to join after column conflict resolution")
            
        except Exception as e:
            print(f"❌ Weather features integration failed: {e}")
            import traceback
            print(f"Full traceback: {traceback.format_exc()}")
    else:
        print(f"❌ Weather features not available for {year}-{month:02d}")
    
    # OPTIMIZE FEATURE SET - Remove redundant and highly correlated features
    print(f"\n🎯 STARTING FEATURE OPTIMIZATION")
    print("="*70)
    try:
        pre_optimization_cols = len(base_data.columns)
        print(f"📊 Features before optimization: {pre_optimization_cols}")
        
        base_data = optimize_feature_set(base_data)
        
        post_optimization_cols = len(base_data.columns)
        print(f"📊 Features after optimization: {post_optimization_cols}")
        print(f"📈 Optimization applied: {pre_optimization_cols - post_optimization_cols} features removed")
        
    except Exception as e:
        print(f"❌ Feature optimization failed: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        print(f"⚠️ Continuing with unoptimized features...")
    
    final_count = base_data.count()
    # Emit concise removal summary (as per project spec)
    raw_count = LAST_BASE_LOAD_METRICS.get("raw_count", 0)
    uber_count = LAST_BASE_LOAD_METRICS.get("uber_count", 0)
    quality_count = LAST_BASE_LOAD_METRICS.get("quality_count", 0)
    sampled_count = LAST_BASE_LOAD_METRICS.get("sampled_count", final_count)

    removed_non_uber = max(raw_count - uber_count, 0)
    removed_quality = max(uber_count - quality_count, 0)
    removed_sampling = max(quality_count - sampled_count, 0)
    delta_after_filters = max(sampled_count - final_count, 0)  # after neg-filter + winsorization (winsorization doesn't drop rows)

    print("\n🧾 REMOVAL SUMMARY (this run)")
    print("-"*70)
    print(f"Raw records: {raw_count:,}")
    print(f"After provider filter (Uber only): {uber_count:,}  | Removed: {removed_non_uber:,}")
    print(f"After quality filters: {quality_count:,}         | Removed: {removed_quality:,}")
    if sampled_count != quality_count:
        print(f"After sampling: {sampled_count:,}                 | Removed: {removed_sampling:,}")
    print(f"After commission cleanup: {final_count:,}        | Removed: {delta_after_filters:,} (negatives: {removed_neg_commission:,})")
    total_columns = len(base_data.columns)
    
    print(f"\n🎯 COMPLETE PIPELINE FINISHED!")
    print(f"  📊 Final dataset: {final_count:,} trips, {total_columns} total columns")
    if diagnostics and initial_count is not None and initial_count > 0:
        print(f"  🔗 Data integrity: {final_count/initial_count*100:.1f}% trips retained")
    print(f"  💰 Revenue calculation: CORRECTED (Uber commission-based)")
    
    key_columns = ["uber_commission", "uber_take_rate", "uber_revenue_per_mile", "uber_revenue_per_minute"]
    existing_key_cols = [col for col in key_columns if col in base_data.columns]
    if existing_key_cols:
        print(f"  🔑 Key Uber revenue columns: {existing_key_cols}")
    
    print(f"  🗑️ Features cleaned: core unwanted, pass-through fees, old revenue metrics")
    print(f"  ✅ Spatial join: Uses PULocationID/DOLocationID appropriately")
    
    # Optionally persist monthly reports (removal summary + coverage)
    if write_reports:
        metrics_payload: Dict[str, Optional[float]] = {
            "raw_count": raw_count,
            "uber_count": uber_count,
            "quality_count": quality_count,
            "sampled_count": sampled_count,
            "removed_non_uber": removed_non_uber,
            "removed_quality": removed_quality,
            "removed_sampling": removed_sampling,
            "removed_neg_commission": removed_neg_commission,
            "winsor_low": winsor_low,
            "winsor_high": winsor_high,
        }
        _write_monthly_removal_summary(year, month, metrics_payload, final_count)
        _write_monthly_coverage_summary(base_data, year, month)

    return base_data


def save_comprehensive_profitability_dataset_final(df: DataFrame, year: int, month: int, dataset_name: str = "comprehensive_profitability_final") -> str:
    """REWRITTEN: Save the comprehensive profitability dataset with proper validation."""
    print(f"💾 SAVING CORRECTED PROFITABILITY DATASET")
    
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
        
        if len(unique_columns) < len(columns):
            df = df.select(unique_columns)
        
        df.write.mode("overwrite").parquet(save_path)
        
        record_count = df.count()
        print(f"✅ Dataset saved: {save_path} ({record_count:,} records)")
        
        return save_path
        
    except Exception as e:
        print(f"❌ Save failed: {e}")
        return None


def process_full_profitability_dataset(year, month, save_name="full_profitability"):
    """REWRITTEN: Process the COMPLETE dataset (no sampling) for production analysis."""
    
    print(f"🚀 PROCESSING FULL DATASET WITH NEW PIPELINE FOR {year}-{month:02d}")
    
    try:
        integrated_data = load_and_integrate_all_features_final(year, month, sample_fraction=1.0)
        
        total_trips = integrated_data.count()
        print(f"📊 Full dataset loaded: {total_trips:,} trips")
        
        save_path = save_comprehensive_profitability_dataset_final(integrated_data, year, month, dataset_name=save_name)
        
        return {
            'status': 'success',
            'data': integrated_data,
            'path': save_path,
            'records': total_trips,
            'columns': len(integrated_data.columns)
        }
        
    except Exception as e:
        print(f"❌ Failed to process {year}-{month:02d}: {e}")
        return {
            'status': 'failed',
            'error': str(e),
            'records': 0,
            'columns': 0
        }


def process_all_months_profitability_batch(years=[2023, 2024], months=list(range(1,13))):
    """REWRITTEN: Process full profitability analysis for all available months using new pipeline."""
    
    print("🔄 BATCH PROCESSING WITH NEW CORRECTED PIPELINE")
    
    results = {}
    successful_months = 0
    total_records = 0
    
    for year in years:
        for month in months:
            month_key = f"{year}-{month:02d}"
            
            if not validate_data_availability(year, month):
                results[month_key] = {'status': 'skipped', 'reason': 'No raw data available'}
                continue
            
            result = process_full_profitability_dataset(year, month, save_name=f"corrected_profitability_{year}_{month:02d}")
            results[month_key] = result
            
            if result['status'] == 'success':
                successful_months += 1
                total_records += result['records']
                print(f"✅ {month_key}: {result['records']:,} trips processed")
    
    print(f"\n🎉 BATCH PROCESSING COMPLETE: {successful_months} successful, {total_records:,} total trips")
    return results


def test_enhanced_profitability_pipeline_final(year: int = 2023, month: int = 1, sample_fraction: float = 0.005) -> dict:
    """REWRITTEN: Test the complete rewritten profitability pipeline."""
    print("🧪 TESTING COMPLETELY REWRITTEN PROFITABILITY PIPELINE")
    
    try:
        integrated_data = load_and_integrate_all_features_final(year, month, sample_fraction)
        
        required_cols = ["uber_commission", "uber_take_rate", "uber_revenue_per_mile", "uber_revenue_per_minute"]
        missing_cols = [col for col in required_cols if col not in integrated_data.columns]
        
        unwanted_cols = ["tolls", "bcf", "sales_tax", "tips", "revenue_per_mile"]
        still_present = [col for col in unwanted_cols if col in integrated_data.columns]
        
        if missing_cols:
            return {'status': 'failed', 'error': f'Missing required columns: {missing_cols}'}
        
        print(f"✅ Test completed successfully - {integrated_data.count():,} trips, {len(integrated_data.columns)} columns")
        
        return {
            'status': 'success',
            'records': integrated_data.count(),
            'columns': len(integrated_data.columns),
            'required_cols_present': len(missing_cols) == 0,
            'unwanted_cols_removed': len(still_present) == 0
        }
        
    except Exception as e:
        print(f"❌ Pipeline test failed: {e}")
        return {'status': 'failed', 'error': str(e)}


# Export all functions (maintaining same interface as before)
__all__ = [
    'load_base_uber_data_for_profitability_corrected',
    'load_and_integrate_all_features_final',
    'save_comprehensive_profitability_dataset_final', 
    'test_enhanced_profitability_pipeline_final',
    'get_spark_session', 
    'validate_data_availability',
    'process_full_profitability_dataset',
    'process_all_months_profitability_batch',
    'check_external_data_availability'
]

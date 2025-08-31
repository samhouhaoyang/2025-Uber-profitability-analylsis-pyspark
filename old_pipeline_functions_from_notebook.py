# BACKUP OF OLD PIPELINE FUNCTIONS FROM new.ipynb
# Created on: 2025-08-25 20:23:13
# These functions have been replaced by the new data_pipeline.py


# === CELL 46 ===
# ==============================================================================
# FINAL CORRECTED PROFITABILITY ANALYSIS - FIXES ALL COLUMN CONFLICTS
# ==============================================================================

def load_and_integrate_all_features_final(year, month, sample_fraction=0.01):
    """
    FINAL: Load raw trip data and integrate ALL external features with proper column management.
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
        spatial_features = load_monthly_spatial_features(year, month)
        
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
        temporal_features = load_monthly_temporal_features(year, month)
        
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
        weather_features = load_monthly_weather_features(year, month)
        
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


def save_comprehensive_profitability_dataset_final(df, year, month, dataset_name="comprehensive_profitability_final"):
    """
    FINAL: Save the comprehensive profitability dataset with column deduplication.
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


def test_enhanced_profitability_pipeline_final(year=2023, month=1, sample_fraction=0.005):
    """
    FINAL: Test the complete enhanced profitability pipeline with column conflict resolution.
    """
    
    print("🧪 TESTING ENHANCED PROFITABILITY PIPELINE (FINAL VERSION)")
    print("="*70)
    
    try:
        # Load integrated data using final function
        integrated_data = load_and_integrate_all_features_final(year, month, sample_fraction)
        print(f"\n📊 Integrated data loaded: {integrated_data.count():,} trips")
        
        # Debug: Show column names before profitability analysis
        print(f"🔍 Columns before profitability analysis: {len(integrated_data.columns)}")
        
        # Run enhanced profitability analysis with fixed data types
        profitability_data = analyze_enhanced_profitability_fixed(integrated_data)
        
        # Add composite scoring
        final_data = add_composite_profitability_score_updated(profitability_data)
        
        # Debug: Show final column count
        print(f"🔍 Final columns after all processing: {len(final_data.columns)}")
        
        # Quick analysis of results
        print(f"\n📈 PROFITABILITY ANALYSIS RESULTS:")
        
        # Sample for analysis (smaller sample to avoid memory issues)
        sample_size = min(5000, final_data.count())
        sample_df = final_data.limit(sample_size).toPandas()
        
        print(f"  💰 Average Uber net revenue: ${sample_df['uber_net_revenue'].mean():.2f}")
        print(f"  📊 Average take rate: {sample_df['uber_take_rate'].mean():.1f}%")
        print(f"  🎯 Average profitability score: {sample_df['profitability_score'].mean():.1f}/10")
        
        # Feature impact analysis
        if 'composite_profitability_multiplier' in sample_df.columns:
            avg_multiplier = sample_df['composite_profitability_multiplier'].mean()
            print(f"  🔄 Average composite multiplier: {avg_multiplier:.2f}x")
        
        # Context-specific insights
        trip_cbd_col = 'trip_cbd_type' if 'trip_cbd_type' in sample_df.columns else 'trip_type_cbd'
        if trip_cbd_col in sample_df.columns:
            cbd_revenue = sample_df.groupby(trip_cbd_col)['uber_net_revenue'].mean()
            print(f"  🗺️ CBD Revenue Analysis:")
            for trip_type, revenue in cbd_revenue.items():
                print(f"    - {trip_type}: ${revenue:.2f}")
        
        if 'rush_hour_label' in sample_df.columns:
            rush_revenue = sample_df.groupby('rush_hour_label')['uber_net_revenue'].mean()
            print(f"  ⏰ Rush Hour Revenue Analysis:")
            for period, revenue in rush_revenue.items():
                print(f"    - {period}: ${revenue:.2f}")
        
        print(f"\n✅ Enhanced profitability pipeline test successful!")
        print(f"📊 Final dataset: {len(final_data.columns)} total columns")
        
        return final_data
        
    except Exception as e:
        print(f"❌ Enhanced profitability pipeline failed: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return None




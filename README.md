# Project README

## Run order (notebooks)
1. Preprocessing.ipynb
   - Start Spark session, load raw HVFHS data, build curated layers/external features (spatial, temporal, weather).
   - Runs the corrected profitability data pipeline as needed.
   - Outputs under `./lake/curated/` and `./lake/external_features/` and `./lake/comprehensive/`.

2. EDA on features.ipynb
   - Exploratory analysis on the curated data and engineered features.
   - Visual checks and diagnostics on temporal/spatial/weather features.

3. model.ipynb
   - Modeling:
     - LASSO on `uber_commission` (interpretability).
     - Random Forest on `y_cpm = uber_commission / trip_miles` (efficiency per mile).
   - Saves models under `./lake/models/`:
     - `glm_lasso_pipeline`
     - `rf_pipeline` (absolute-commission RF, if run earlier)
     - `rf_cpm_pipeline` (commission-per-mile RF)

## Quick setup
- Python ≥ 3.10, Spark 4.x, Jupyter.
- Recommended (WSL-safe) Spark session:
```python
from pyspark.sql import SparkSession
spark = (
    SparkSession.builder.appName("WSL-safe")
    .master("local[2]")
    .config("spark.task.cpus", "1")
    .config("spark.sql.shuffle.partitions", "32")
    .config("spark.default.parallelism", "32")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.driver.memory", "8g")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)
```

## Key data locations
- Raw HVFHS: `./lake/raw/hvfhs/v1`
- Curated core: `./lake/curated/hvfhs_core*/`
- External features:
  - Spatial: `./lake/external_features/spatial/`
  - Temporal: `./lake/external_features/temporal/`
  - Weather: `./lake/external_features/weather_optimized/`
- Comprehensive outputs: `./lake/comprehensive/<dataset_name>/year=YYYY/month=MM/`
- Model artifacts: `./lake/models/`

## Modeling notes
- CPM target: filter `trip_miles > 0.1`; avoid leakage by excluding revenue/take-rate/per-unit monetary fields and raw denominators.
- RF for CPM: use indexed categoricals; set `maxBins` high enough for high-cardinality features (e.g., 512).
- Subsampling: RF typically trains on 5% year-based splits (2023 train, 2024 test) for compute.

## Tips
- Remove Windows metadata files like `*:Zone.Identifier` (not used by code).
- Heavy, batch pipeline runners are documented but not required for everyday analysis; run only if you need fresh artifacts.

## Reproducibility
- Seeds are set where applicable (`seed=42`).
- Thresholds for outlier handling (if enabled) are logged under `./lake/reports/`.

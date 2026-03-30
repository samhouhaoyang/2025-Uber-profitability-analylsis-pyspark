# Optimising Dispatch Profitability: A Data-Driven Analysis of Uber's NYC Operations

> Analysing temporal, spatial, and ride-sharing factors across **~290 million Uber trips** (2023–2024) to identify actionable dispatch strategies for maximising platform commission revenue.

**Author:** Haoyang Hou 

---

## Overview

This project uses the TLC High Volume For-Hire Vehicle (HVFHV) Trip Record Data to model and explain Uber's platform-side profitability in New York City. Two complementary targets are studied:

| Target | Definition | Purpose |
|---|---|---|
| `uber_commission` | Base passenger fare − driver pay | Absolute platform revenue per trip |
| `commission_per_mile` (CPM) | `uber_commission / trip_miles` | Relative profit efficiency per mile |

**Key finding:** Geography and trip structure — not weather or holidays — dominate Uber's dispatch profitability. Airport trips (LaGuardia, JFK, Newark) and cross-borough Manhattan-linked rides are consistently the highest-value dispatch targets.

---

## Project Structure

```
.
├── Preprocessing.ipynb           # Data ingestion, feature engineering, curated lake build
├── EDA on features.ipynb         # Exploratory analysis and geospatial visualisation
├── model.ipynb                   # LASSO and Random Forest modelling
├── preprocessing/                # Reusable pipeline modules
│   ├── filtering_features/       # Feature correlation screening
│   └── final_corrected_profitability_analysis/  # Profitability data pipeline
├── lake/                         # Data lake (raw → curated → models)
│   ├── raw/hvfhs/v1/             # Raw TLC HVFHV parquet files (download separately)
│   ├── curated/                  # Cleaned, Uber-only trip records
│   ├── external_features/        # Spatial, temporal, and weather features
│   ├── comprehensive/            # Fully joined profitability datasets
│   └── models/                   # Saved Spark ML pipelines
└── Optimising Dispatch Profitability Report.pdf
```

---

## Setup

### Requirements

- Python ≥ 3.10, Apache Spark 4.x, Jupyter
- Download TLC HVFHV data and place it under `./lake/raw/hvfhs/v1/` before running

### Recommended Spark Session (WSL-safe)

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("WSL-safe")
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

---

## Notebook Run Order

### 1. `Preprocessing.ipynb`
- Filters to Uber-only trips (`HV0003`), applies quality filters, and winsorises commissions at the 1st/99th percentile
- Engineers profitability features: `uber_commission`, `commission_per_mile`, `avg_speed_mph`, `driver_efficiency`, etc.
- Joins external datasets (holidays, weather, CBD zones) via left joins
- Outputs curated data under `./lake/curated/`, `./lake/external_features/`, and `./lake/comprehensive/`

### 2. `EDA on features.ipynb`
- Distribution checks on both target variables (heavily right-skewed; median commission ~$4.44)
- Geospatial visualisation of CPM by TLC zone — airports and Manhattan dominate
- Temporal × structural interaction plots (intra-CBD vs. non-CBD hourly patterns)
- Ride-sharing efficiency analysis by distance band

### 3. `model.ipynb`
- **LASSO Regression** on `uber_commission`: sparse variable selection across 65,000+ one-hot encoded features; λ = 0.1
- **Random Forest** on `commission_per_mile`: 120 trees, max depth 12; captures non-linear spatial/structural interactions
- Saves trained pipelines to `./lake/models/`:
  - `glm_lasso_pipeline`
  - `rf_cpm_pipeline`

---

## Data

### Key Locations

| Path | Contents |
|---|---|
| `./lake/raw/hvfhs/v1/` | Raw TLC HVFHV parquet (all providers) |
| `./lake/curated/hvfhs_core*/` | Uber-only, quality-filtered trips |
| `./lake/external_features/spatial/` | CBD zone classifications |
| `./lake/external_features/temporal/` | Holiday calendar features |
| `./lake/external_features/weather_optimized/` | NOAA daily weather features |
| `./lake/comprehensive/<dataset>/year=YYYY/month=MM/` | Fully joined profitability datasets |
| `./lake/models/` | Saved Spark ML pipelines |

### Dataset Scale

| Stage | 2023 Rows | 2024 Rows | Total |
|---|---|---|---|
| Raw HVFHV (all providers) | 211,973,723 | 239,470,448 | 451,444,171 |
| Uber-only filter | 152,853,704 | 179,125,798 | 331,979,502 |
| After quality filters | 152,778,818 | 179,099,765 | 331,878,583 |
| Final (after commission cleanup) | 135,187,730 | 154,407,749 | **289,595,479** |

### External Data Sources

- **Holiday calendar:** U.S. OPM (federal) + NYC OPA (city) holidays, 2023–2024
- **Weather:** NOAA Global Historical Climatology Network (daily precipitation & temperature)
- **Spatial:** MTA Central Business District (CBD) Taxi Zones

---

## Results

| Model | Target | RMSE | MAE | R² |
|---|---|---|---|---|
| LASSO Regression | `uber_commission` | 4.52 | 3.08 | 0.27 |
| Random Forest | `commission_per_mile` | 1.48 | 1.09 | 0.28 |

### Key Drivers

**Spatial zones** account for ~48% of CPM variation (Random Forest). Airport dropoffs alone contribute +$8.39 in signed Lasso coefficient sum. Trip structure (cross-borough, CBD involvement) contributes ~31%. Weather and holiday signals are negligible — shrunk to zero by LASSO.

### Dispatch Recommendations

1. **Prioritise airport trips** — LaGuardia, JFK, and Newark are the highest-commission destinations; dispatch should favour airport-linked matches during peak flight windows
2. **Encourage cross-borough and CBD-linked rides** — surge pricing and driver bonuses should favour trips spanning boroughs or connecting outer boroughs to Manhattan
3. **Deprioritise short intra-CBD trips during peaks** — these yield lower per-mile efficiency; prefer longer or cross-borough rides during rush hours and weekends
4. **Align incentives with demand cycles** — weekend/evening bonuses and rider promos match the highest-demand periods
5. **Minimise weight on weather/holiday factors** — these signals show negligible impact; focus resources on geography and trip structure

---

## Modelling Notes

- **CPM target:** filter `trip_miles > 0.1` to avoid division artifacts; exclude all revenue/take-rate/per-unit monetary fields to prevent leakage
- **Random Forest:** use indexed categoricals; set `maxBins ≥ 512` for high-cardinality zone features
- **Subsampling:** LASSO trained on 50% of 2023, tested on 10% of 2024; RF trained and tested on 5% year-based splits for compute efficiency
- **Seeds:** set to `seed=42` throughout for reproducibility

---

## Reproducibility

- All seeds fixed at `seed=42`
- Outlier thresholds and winsorisation bounds logged under `./lake/reports/`
- Remove Windows metadata files (`*:Zone.Identifier`) — not used by any pipeline code

---

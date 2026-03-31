# Preprocessing Layer

This directory contains Databricks Delta Live Tables (DLT) pipelines that transform raw Bronze data into the curated **Silver** and **Gold** layers.

## Contents

| File | Purpose |
|------|---------|
| `airquality_cities_preprocessing.ipynb` | Exploratory preprocessing notebook |
| `air_quality_transformations/transformations/silver_transformations.py` | Bronze → Silver DLT pipeline |
| `air_quality_transformations/transformations/gold_transformations.py` | Silver → Gold DLT pipeline |
| `air_quality_transformations/utilities/my_utils.py` | UDFs and helper functions |

---

## Silver Layer (`silver_transformations.py`)

Produces `air_quality_slv` from all Bronze sources.

**Transformations applied:**

| Step | Description |
|------|-------------|
| Deduplication | Keep the reading with the highest observation count per city/date/pollutant |
| Country standardisation | Map free-text names to ISO-2 codes (e.g. `Nigeria → NG`) |
| City extraction | UDF parses city name from messy raw location strings |
| Data quality gates | DLT `expect_or_drop` rules drop rows missing critical columns |
| Z-ordering | Optimised for queries on `Country`, `City`, `Date` |
| Change Data Feed | Enabled for efficient downstream incremental reads |

---

## Gold Layer (`gold_transformations.py`)

Builds a star schema and an ML feature store on top of Silver.

### Dimension Tables

| Table | Description |
|-------|-------------|
| `dim_locations` | City/country metadata: lat, lon, population, n_stations, monitoring_density_score |
| `dim_pollutants` | 7 pollutant species with WHO guideline thresholds |
| `dim_dates` | Calendar dimension (2010–2023) with year, month, quarter, season flags |

### Fact Tables

| Table | Granularity | Key Metrics |
|-------|-------------|-------------|
| `fact_air_quality_daily` | City × Date | min / max / median / variance / count for all 7 pollutants; AQI category |
| `fact_air_quality_aggregated` | City × Month and Year | WHO exceedance rate, trend indicators |

### Analytics View

`view_nigeria_vs_world` — comparative PM2.5 / PM10 analysis between Nigeria and global averages.

### ML Feature Table (`ml_features_table`)

Engineered features used by the XGBoost model:

| Group | Features |
|-------|---------|
| Temporal | year, month, quarter, day_of_week, is_weekend, season indicators |
| Location | lat, lon, population, n_stations, population_per_station, monitoring_density_score |
| PM2.5 history | Rolling averages (7d/30d/90d), lagged values (1d/7d/14d/30d), 7d volatility, 30d min/max/range, trend |
| Co-pollutants | pm10_30d_avg, no2_30d_avg |
| WHO flags | was_exceeding_who_7d, was_exceeding_who_30d, pm25_recent_ratio |

**Target:** `pm25_30d_forward_median` — no data leakage; all features are strictly backward-looking.

---

## Running the Pipelines

1. Import the `air_quality_transformations` folder into your Databricks workspace as a DLT pipeline source.
2. Create a DLT pipeline pointing to `silver_transformations.py`, then a second pipeline for `gold_transformations.py`.
3. Trigger or schedule each pipeline; DLT handles incremental updates automatically.

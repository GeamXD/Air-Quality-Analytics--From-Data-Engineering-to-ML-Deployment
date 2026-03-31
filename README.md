# Air Quality Analytics: From Data Engineering to ML Deployment

An end-to-end data analytics and machine learning platform for analyzing, forecasting, and visualizing air quality metrics. The project covers the complete data lifecycle — from ingestion and transformation to ML modeling, deployment, and interactive visualization — demonstrating production-ready data engineering and data science practices.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Repository Structure](#repository-structure)
3. [Data Pipeline](#data-pipeline)
   - [Data Sources](#data-sources)
   - [Bronze Layer – Raw Ingestion](#bronze-layer--raw-ingestion)
   - [Silver Layer – Cleaning & Standardization](#silver-layer--cleaning--standardization)
   - [Gold Layer – Dimensional Model & ML Features](#gold-layer--dimensional-model--ml-features)
4. [Machine Learning](#machine-learning)
   - [Feature Engineering](#feature-engineering)
   - [Model Training](#model-training)
   - [Model Serving](#model-serving)
5. [Streamlit Application](#streamlit-application)
6. [Dashboard](#dashboard)
7. [Tech Stack](#tech-stack)
8. [Setup & Running Locally](#setup--running-locally)
9. [Deployment](#deployment)
10. [Data Attribution](#data-attribution)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│  DATA SOURCES                                           │
│  OpenAQ API v3  ──┐                                     │
│  AQICN API      ──┼──► api_call.py ──► CSV / Parquet    │
│  Historical CSV ──┘                                     │
└──────────────────────────────────┬──────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────┐
│  DATABRICKS DELTA LAKE                                   │
│                                                          │
│  Bronze  ──►  Silver  ──►  Gold (Star Schema + ML Features) │
│  (raw)       (cleaned)     (analytics + model features)  │
└──────────────────────────────────┬───────────────────────┘
                                   │
                     ┌─────────────┴──────────────┐
                     ▼                            ▼
          ┌─────────────────────┐   ┌─────────────────────────┐
          │  Databricks SQL     │   │  XGBoost Model          │
          │  Analytics Dashboard│   │  (Databricks Serving)   │
          └─────────────────────┘   └────────────┬────────────┘
                                                 ▼
                                    ┌────────────────────────┐
                                    │  Streamlit App         │
                                    │  (Google Cloud Run)    │
                                    └────────────────────────┘
```

---

## Repository Structure

```
.
├── Ingestion/                          # Data ingestion layer
│   ├── 1.Setup Schema.dbquery.ipynb   # Initialize Databricks schema
│   ├── 2.Setup Bronze Tables.dbquery.ipynb  # Create bronze Delta tables
│   └── air_quality_incre/
│       └── transformations/
│           └── air_quality_api_call_incre.py  # Incremental API ingest
│
├── Preprocessing/                      # Transformation layer (DLT pipelines)
│   ├── airquality_cities_preprocessing.ipynb
│   └── air_quality_transformations/
│       ├── transformations/
│       │   ├── silver_transformations.py  # Bronze → Silver
│       │   └── gold_transformations.py    # Silver → Gold
│       └── utilities/
│           └── my_utils.py               # UDFs and helpers
│
├── ML/                                 # ML training and serving
│   ├── Airquality Forecast.ipynb       # XGBoost training notebook
│   ├── feature_importance.csv          # Model feature rankings
│   ├── city_forecast_90d.csv           # 90-day city-level forecasts
│   └── air-quality-app/                # Streamlit production app
│       ├── app.py                      # Application entry point
│       ├── app.yaml                    # Cloud Run service definition
│       ├── requirements.txt
│       └── .streamlit/
│           └── secrets.toml            # Databricks token (not committed)
│
├── Dashboard/
│   └── Air Quality Dashboard.lvdash.json  # Databricks dashboard export
│
├── datacollection_scrp/                # Standalone data collection scripts
│   ├── api_call.py                     # OpenAQ API client (rate-limited)
│   ├── to_parquet.py                   # CSV → Parquet converter
│   └── get_data.sh                     # Shell helper
│
├── datasets_landing_parquet/           # Raw data landing zone (~169 MB)
│   ├── airquality-covid19-cities.json
│   ├── nigeria_air_quality_data.csv
│   └── parquets/
│
├── SAMP.py                             # Exploratory data script
├── requirements.txt                    # Development dependencies
└── image.png                           # Architecture diagram
```

---

## Data Pipeline

### Data Sources

| Source | API / File | Coverage |
|--------|-----------|----------|
| **OpenAQ API v3** | REST, 60 req/min, 2,000 req/hr | 2,000+ stations globally, 100+ in Nigeria |
| **AQICN API** | REST | Historical and real-time data |
| **Historical dataset** | Parquet files | COVID-era global air quality |

### Bronze Layer – Raw Ingestion

Databricks Delta tables store the raw API payloads with ingestion metadata:

| Table | Description |
|-------|-------------|
| `air_quality_raw` | Initial historical batch load |
| `air_quality_nigeria_incremental` | Scheduled incremental loads for Nigeria |
| `air_quality_global_incremental` | Scheduled incremental loads globally |

Every record includes `_ingest_ts`, `_source_file`, and `_source_format` audit columns.

**Incremental loading** (`air_quality_api_call_incre.py`) uses a watermark on the latest date already present in the table to avoid re-ingesting existing records.

### Silver Layer – Cleaning & Standardization

`silver_transformations.py` applies Databricks Delta Live Tables (DLT) expectations to produce `air_quality_slv`:

- **Deduplication** – keep the reading with the highest observation count per city/date/pollutant
- **Country standardisation** – map free-text country names to ISO-2 codes (`Nigeria → NG`)
- **City name extraction** – UDF-based extraction from messy location strings
- **Data quality gates** – DLT `expect_or_drop` rules enforce NOT NULL on critical columns
- **Storage optimisation** – Z-order on `Country`, `City`, `Date`; Delta Change Data Feed enabled

### Gold Layer – Dimensional Model & ML Features

`gold_transformations.py` builds a star schema plus an ML feature store on top of the Silver layer.

#### Dimension Tables

| Table | Key Columns |
|-------|------------|
| `dim_locations` | `country`, `city`, `lat`, `lon`, `population`, `n_stations`, `monitoring_density_score` |
| `dim_pollutants` | `pollutant` (7 species), `who_guideline_ugm3`, `unit` |
| `dim_dates` | `date`, `year`, `month`, `quarter`, `day_of_week`, `is_weekend`, season flags |

**Pollutants tracked:** PM2.5, PM10, NO₂, O₃, SO₂, CO, BC

#### Fact Tables

| Table | Granularity | Key Metrics |
|-------|-------------|-------------|
| `fact_air_quality_daily` | City × Date | min / max / median / variance / count per pollutant, AQI category, quality flags |
| `fact_air_quality_aggregated` | City × Month and City × Year | WHO exceedance rates, trend indicators |

#### ML Feature Table

`ml_features_table` contains 32 engineered features used for XGBoost training:

| Feature Group | Features |
|---------------|---------|
| **Temporal** | year, month, quarter, day_of_week, is_weekend, is_winter/spring/summer/fall |
| **Location** | lat, lon, population, n_stations, population_per_station, monitoring_density_score |
| **PM2.5 history** | 7d / 30d / 90d rolling averages; 1d / 7d / 14d / 30d lags; 7d volatility; 30d min/max/range; trend |
| **Co-pollutants** | pm10_30d_avg, no2_30d_avg |
| **WHO flags** | was_exceeding_who_7d, was_exceeding_who_30d, pm25_recent_ratio |

**Target variable:** `pm25_30d_forward_median` — PM2.5 median concentration over the next 30 days (µg/m³).  
All features are strictly backward-looking (no data leakage).

---

## Machine Learning

### Feature Engineering

Features are built using Spark SQL window functions with `ROWS BETWEEN … PRECEDING` to ensure no future data leaks into training rows.

### Model Training

The `Airquality Forecast.ipynb` notebook:

1. Reads `ml_features_table` from the Gold layer
2. Trains an **XGBoost** regression model to predict `pm25_30d_forward_median`
3. Logs the model and metrics to **MLflow** (Databricks-managed)
4. Registers the best run as the **champion** model in the Databricks Model Registry

**Top 5 features by importance:**

| Rank | Feature | Importance |
|------|---------|-----------|
| 1 | pm25_30d_avg | 47.4% |
| 2 | pm25_7d_avg | 28.4% |
| 3 | pm25_lag_1d | 4.4% |
| 4 | was_exceeding_who_30d | 4.2% |
| 5 | is_spring | 1.7% |

### Model Serving

The champion model is exposed as a **Databricks Model Serving** REST endpoint:

```
POST https://<workspace>.cloud.databricks.com/serving-endpoints/air_quality_xgb/invocations
Authorization: Bearer <DATABRICKS_TOKEN>
Content-Type: application/json
```

Request body format (Databricks `dataframe_records`):
```json
{
  "dataframe_records": [
    {
      "pm25_30d_avg": 35.2,
      "pm25_7d_avg": 38.1,
      "lat": 6.45,
      "lon": 3.39,
      ...
    }
  ]
}
```

---

## Streamlit Application

**Location:** `ML/air-quality-app/app.py`

The web app provides an interactive PM2.5 forecasting interface:

- **User inputs:** date, geographic coordinates (lat/lon), population, number of monitoring stations, historical PM2.5 statistics, WHO exceedance flags
- **Auto-derived features:** temporal fields (year, month, quarter, season, is_weekend) computed from the selected date
- **Output:** predicted PM2.5 value (µg/m³), EPA AQI category, colour-coded risk level, and actionable health recommendations
- **Embedded dashboard:** read-only Databricks analytics view accessible from the app sidebar

AQI thresholds used:

| PM2.5 (µg/m³) | Category |
|---------------|----------|
| 0 – 12 | 🟢 Good |
| 12.1 – 35.4 | 🟡 Moderate |
| 35.5 – 55.4 | 🟠 Unhealthy for Sensitive Groups |
| 55.5 – 150.4 | 🔴 Unhealthy |
| 150.5 – 250.4 | 🟣 Very Unhealthy |
| > 250.4 | ⚫ Hazardous |

---

## Dashboard

An analytics dashboard (`Dashboard/Air Quality Dashboard.lvdash.json`) built in **Databricks SQL** covers:

- Global and Nigeria-specific PM2.5 / PM10 trends
- WHO guideline exceedance rates by city and year
- Comparative `view_nigeria_vs_world` analysis
- Seasonal patterns and monitoring coverage metrics

The dashboard JSON can be imported directly into a Databricks workspace via the Dashboards UI.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Lake | Databricks Delta Lake |
| Batch / Streaming | Apache Spark (PySpark), DLT pipelines |
| Orchestration | Databricks Workflows |
| Storage format | Delta / Parquet |
| ML framework | XGBoost, MLflow |
| Model serving | Databricks Model Serving |
| Web application | Streamlit 1.38 |
| Cloud deployment | Google Cloud Run |
| External APIs | OpenAQ API v3, AQICN API |
| Languages | Python 3, SQL, Bash |

---

## Setup & Running Locally

### Prerequisites

- Python 3.9+
- A Databricks workspace with Delta Lake and Model Serving enabled
- Google Cloud SDK (for deployment only)

### Install dependencies

```bash
# Development environment
pip install -r requirements.txt

# Streamlit app only
pip install -r ML/air-quality-app/requirements.txt
```

### Configure secrets

Create `ML/air-quality-app/.streamlit/secrets.toml`:

```toml
DATABRICKS_TOKEN = "dapiXXXXXXXXXXXXXXXX"
```

### Run the Streamlit app locally

```bash
cd ML/air-quality-app
streamlit run app.py
```

### Collect raw data (optional)

```bash
cd datacollection_scrp
python api_call.py          # fetch from OpenAQ
python to_parquet.py        # convert CSV to Parquet
```

---

## Deployment

The Streamlit app is containerised and deployed to **Google Cloud Run**:

```bash
# From ML/air-quality-app/
gcloud run deploy air-quality-app \
  --source . \
  --region us-central1 \
  --allow-unauthenticated
```

The `app.yaml` defines the service entry point (`streamlit run app.py`). The `DATABRICKS_TOKEN` secret should be injected via Cloud Run secret manager rather than committed to source control.

---

## Data Attribution

- **AQICN / World Air Quality Index Project** – Attribution is mandatory for all uses. Public usage by for-profit corporations requires explicit agreement with the WAQI team. Non-profit usage requires prior email notification.  
  Reference scale: https://aqicn.org/scale/
- **OpenAQ** – Data is published under open licences; see [openaq.org](https://openaq.org) for terms.
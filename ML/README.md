# ML Layer

This directory contains the XGBoost model training notebook, pre-computed outputs, and the production Streamlit application.

## Contents

| File / Folder | Purpose |
|---------------|---------|
| `Airquality Forecast.ipynb` | Model training, evaluation, and MLflow registration |
| `feature_importance.csv` | XGBoost feature importances from the champion run |
| `city_forecast_90d.csv` | Pre-computed 90-day PM2.5 forecasts per city |
| `air-quality-app/` | Production Streamlit web application |

---

## Model Training (`Airquality Forecast.ipynb`)

1. Reads `ml_features_table` from the Gold layer
2. Trains an **XGBoost** regressor to predict `pm25_30d_forward_median` (30-day forward PM2.5 median in µg/m³)
3. Logs parameters, metrics, and the serialised model to **MLflow** (Databricks-managed experiment tracking)
4. Promotes the best run to the **champion** alias in the Databricks Model Registry

### Top Features

| Rank | Feature | Importance |
|------|---------|-----------|
| 1 | pm25_30d_avg | 47.4% |
| 2 | pm25_7d_avg | 28.4% |
| 3 | pm25_lag_1d | 4.4% |
| 4 | was_exceeding_who_30d | 4.2% |
| 5 | is_spring | 1.7% |

---

## Streamlit Application (`air-quality-app/`)

### Files

| File | Description |
|------|-------------|
| `app.py` | Main application (442 lines) |
| `app.yaml` | Google Cloud Run service definition |
| `requirements.txt` | Runtime dependencies (`streamlit==1.38.0`, `requests`, `pandas`) |
| `.streamlit/secrets.toml` | Databricks token — **not committed to source control** |

### Features

- Interactive PM2.5 prediction form (date, coordinates, population, historical PM2.5 stats)
- Temporal features (year, month, season, is_weekend) auto-derived from the selected date
- Prediction output with EPA AQI category, colour-coded risk badge, and health recommendations
- Embedded read-only Databricks analytics dashboard

### Running Locally

```bash
cd air-quality-app
# Create .streamlit/secrets.toml with your DATABRICKS_TOKEN
streamlit run app.py
```

### Model Serving Endpoint

The app calls the Databricks Model Serving REST endpoint:

```
POST /serving-endpoints/air_quality_xgb/invocations
```

Request payload example:

```json
{
  "dataframe_records": [
    {
      "pm25_30d_avg": 35.2,
      "pm25_7d_avg": 38.1,
      "lat": 6.45,
      "lon": 3.39,
      "population": 15000000,
      "n_stations": 5,
      "month": 3,
      "is_spring": 1
    }
  ]
}
```

### Deployment

```bash
gcloud run deploy air-quality-app \
  --source . \
  --region us-central1 \
  --allow-unauthenticated
```

Inject `DATABRICKS_TOKEN` via Cloud Run secret manager instead of `secrets.toml` in production.

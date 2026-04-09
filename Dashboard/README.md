# Dashboard

This directory contains the Databricks SQL analytics dashboard for the Air Quality Analytics platform.

![Air Quality Dashboard](https://github.com/GeamXD/Air-Quality-Analytics--From-Data-Engineering-to-ML-Deployment/blob/main/Dashboard/Air%20Quality%20Dashboard%202026-03-31%2018_51_1.jpeg?raw=true)

## Contents

| File | Description |
|------|-------------|
| `Air Quality Dashboard.lvdash.json` | Databricks dashboard export (Lakeview format) |

## Dashboard Panels

The dashboard provides the following analytics views:

- **Global PM2.5 / PM10 trends** — time-series of pollutant concentrations across all tracked cities
- **Nigeria focus** — country-level drill-down with historical trend and monitoring station coverage
- **WHO exceedance rates** — percentage of days exceeding WHO guidelines per city and year
- **Nigeria vs. World** — comparative analysis powered by `view_nigeria_vs_world` (Gold layer)
- **Seasonal patterns** — month-by-month aggregations highlighting peak pollution periods
- **Monitoring coverage** — station density scores and data completeness metrics

## Importing the Dashboard

1. Open your Databricks workspace.
2. Navigate to **Dashboards** in the left sidebar.
3. Click **Import** and upload `Air Quality Dashboard.lvdash.json`.
4. The dashboard will bind automatically to the Gold layer tables in the configured catalog/schema.

## Embedded View

The dashboard is also accessible read-only inside the Streamlit application via an embedded iframe pointing to the Databricks dashboard share URL.

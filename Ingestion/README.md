# Ingestion Layer

This directory contains the notebooks and scripts responsible for pulling raw air quality data from external APIs and landing it in the **Bronze layer** of the Databricks Delta Lakehouse.

## Contents

| File / Folder | Purpose |
|---------------|---------|
| `1.Setup Schema.dbquery.ipynb` | Create the Databricks catalog/schema used by all downstream layers |
| `2.Setup Bronze Tables.dbquery.ipynb` | Define and initialise the three Bronze Delta tables |
| `air_quality_incre/transformations/air_quality_api_call_incre.py` | Incremental API ingest job |

## Bronze Tables

| Table | Source | Load pattern |
|-------|--------|-------------|
| `air_quality_raw` | Historical Parquet files | One-time batch |
| `air_quality_nigeria_incremental` | OpenAQ API v3 (Nigeria stations) | Incremental / scheduled |
| `air_quality_global_incremental` | OpenAQ API v3 + AQICN (global) | Incremental / scheduled |

Every record is stamped with `_ingest_ts`, `_source_file`, and `_source_format` audit columns.

## Incremental Ingest (`air_quality_api_call_incre.py`)

- Reads the latest date already present in the target table (watermark)
- Fetches only new data from the OpenAQ API, respecting rate limits (60 req/min, 2,000 req/hr)
- Deduplicates against existing rows before writing to Delta

## Running the Setup Notebooks

1. Open the notebooks in your Databricks workspace.
2. Attach to a cluster with `pyspark` and `delta` installed.
3. Run `1.Setup Schema` first, then `2.Setup Bronze Tables`.
4. Schedule `air_quality_api_call_incre.py` as a Databricks Job for ongoing incremental loads.

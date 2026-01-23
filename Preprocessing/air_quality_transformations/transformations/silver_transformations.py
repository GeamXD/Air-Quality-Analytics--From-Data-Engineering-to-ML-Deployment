# Import modules
import dlt as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utilities import my_utils

# ==========================================
# SILVER LAYER: Clean and deduplicate data
# ==========================================
catalog = "air_quality"
silver_schema = "02_silver"


@dp.table(
    name=f"{catalog}.{silver_schema}.air_quality_slv",
    comment="Cleaned and deduplicated air quality data from all bronze sources",
    table_properties={
        "pipelines.autoOptimize.zOrderCols": "Country,City,Date",
        "delta.enableChangeDataFeed": "true",
    },
)
@dp.expect_or_drop("valid_date", "Date IS NOT NULL")
@dp.expect_or_drop("valid_country", "Country IS NOT NULL")
@dp.expect_or_drop("valid_city", "City IS NOT NULL")
@dp.expect_or_drop("valid_specie", "Specie IS NOT NULL")
def air_quality_slv():
    """
    Combines historical, Nigeria incremental, and global incremental data,
    applies transformations, and removes duplicates.
    """
    # Read bronze tables
    historical_air_q = spark.read.table("air_quality.01_bronze.air_quality_raw")
    ngn_incre = spark.read.table(
        "air_quality.01_bronze.air_quality_nigeria_incremental"
    )
    global_incre = spark.read.table(
        "air_quality.01_bronze.air_quality_global_incremental"
    )

    # Ensure date schema matches for all dataframes
    historical_air_q = historical_air_q.withColumn("Date", F.col("Date").cast("date"))
    ngn_incre = ngn_incre.withColumn("Date", F.col("Date").cast("date"))
    global_incre = global_incre.withColumn("Date", F.col("Date").cast("date"))

    # Drop metadata columns from all dataframes before union
    columns_to_drop = ["_ingest_ts", "_source_file", "_source_format"]

    historical_air_q = historical_air_q.drop(
        *[col for col in columns_to_drop if col in historical_air_q.columns]
    )
    ngn_incre = ngn_incre.drop(
        *[col for col in columns_to_drop if col in ngn_incre.columns]
    )
    global_incre = global_incre.drop(
        *[col for col in columns_to_drop if col in global_incre.columns]
    )

    # Union all dataframes
    df = historical_air_q.union(ngn_incre).union(global_incre)

    # ==========================================
    # APPLY ALL TRANSFORMATIONS ON UNIONED DATA
    # ==========================================

    # 1. Replace "Nigeria" and "NGN" with "NG"
    df = df.withColumn(
        "Country",
        F.when(
            F.upper(F.trim(F.col("Country"))).isin("NIGERIA", "NGN"), F.lit("NG")
        ).otherwise(F.col("Country")),
    )

    # 2. Clean city names (only for Nigeria records)
    df = df.withColumn(
        "City",
        F.when(
            F.col("Country") == "NG", my_utils.extract_city(F.col("City"))
        ).otherwise(F.col("City")),
    )

    # 3. Map cities to country codes where Country is Unknown or NULL
    city_country_code_map = {
        "beijing": "CN",
        "london": "GB",
        "newyork": "US",
        "delhi": "IN",
        "tokyo": "JP",
        "paris": "FR",
        "losangeles": "US",
        "shanghai": "CN",
        "mumbai": "IN",
        "sydney": "AU",
    }

    # Create mapping expression using when/otherwise
    country_mapping_expr = F.col("Country")
    for city, code in city_country_code_map.items():
        country_mapping_expr = F.when(
            (F.lower(F.trim(F.col("City"))) == city)
            & (F.col("Country").isNull() | (F.col("Country") == "Unknown")),
            code,
        ).otherwise(country_mapping_expr)

    df = df.withColumn("Country", country_mapping_expr)

    # Remove duplicates using window function
    # Keep the record with the highest count value when duplicates exist
    window_spec = Window.partitionBy("Date", "Country", "City", "Specie").orderBy(
        F.col("count").desc_nulls_last(), F.col("max").desc_nulls_last()
    )

    df_deduplicated = (
        df.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # Add processing metadata
    df_final = df_deduplicated.withColumn("_processed_ts", F.current_timestamp())

    return df_final

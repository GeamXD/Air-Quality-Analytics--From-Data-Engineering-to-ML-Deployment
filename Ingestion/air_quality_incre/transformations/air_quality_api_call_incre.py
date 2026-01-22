import dlt
import requests
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import time

# ============================================================================
# CONFIGURATION
# ============================================================================

# OpenAQ Configuration
OPENAQ_API_KEY = spark.conf.get("OPENAQ_API_KEY")
OPENAQ_BASE_URL = "https://api.openaq.org/v3"
NIGERIA_COUNTRY_ID = 100

# AQICN Configuration
AQICN_API_TOKEN = spark.conf.get("AQICN_API_TOKEN")
AQICN_BASE_URL = "https://api.waqi.info"

# Comparison cities for AQICN (non-Nigeria countries)
COMPARISON_CITIES = [
    "beijing",
    "london",
    "newyork",
    "delhi",
    "tokyo",
    "paris",
    "losangeles",
    "shanghai",
    "mumbai",
    "sydney",
]  # might modify this list

# Schema matching your existing air_quality_raw table
AIR_QUALITY_SCHEMA = StructType(
    [
        StructField("Date", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Specie", StringType(), True),
        StructField("count", IntegerType(), True),
        StructField("min", DoubleType(), True),
        StructField("max", DoubleType(), True),
        StructField("median", DoubleType(), True),
        StructField("variance", DoubleType(), True),
        StructField("_ingest_ts", TimestampType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_source_format", StringType(), True),
    ]
)

# ============================================================================
# TABLE 1: NIGERIA INCREMENTAL DATA (OpenAQ API)
# ============================================================================


@dlt.table(
    name="air_quality_nigeria_incremental",
    comment="Incremental Nigeria air quality data from OpenAQ API - updates weekly",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_or_drop("valid_date_nigeria", "Date IS NOT NULL")
@dlt.expect_or_drop("valid_country_nigeria", "Country = 'Nigeria'")
@dlt.expect("valid_measurements_nigeria", "min >= 0 AND max >= 0 AND median >= 0")
def nigeria_air_quality_incremental():
    """
    Fetch recent Nigeria data from OpenAQ API.
    Scheduled to run weekly to capture new measurements.
    """

    def fetch_openaq_nigeria():
        """Fetch Nigeria data from OpenAQ API with rate limiting"""
        headers = {"X-API-Key": OPENAQ_API_KEY}
        all_rows = []

        try:
            # Step 1: Get all locations in Nigeria
            print(f"Fetching Nigeria locations...")
            locations_url = f"{OPENAQ_BASE_URL}/locations"
            params = {"countries_id": NIGERIA_COUNTRY_ID, "limit": 1000, "page": 1}

            locations = []
            while True:
                time.sleep(1)  # Rate limiting
                response = requests.get(locations_url, params=params, headers=headers)
                response.raise_for_status()

                data = response.json()
                results = data.get("results", [])
                locations.extend(results)

                print(f"Retrieved {len(results)} locations (Page {params['page']})")

                if len(results) < params["limit"]:
                    break
                params["page"] += 1

            print(f"Total locations found: {len(locations)}")

            # Step 2: Fetch data from each sensor (last 7-14 days)
            total_sensors = sum(len(loc.get("sensors", [])) for loc in locations)
            processed = 0

            for location in locations:
                city = location.get("name", "Unknown")
                country_name = location.get("country", {}).get("name", "Nigeria")

                for sensor in location.get("sensors", []):
                    sensor_id = sensor["id"]
                    specie = sensor.get("parameter", {}).get("name", "unknown")

                    processed += 1
                    print(
                        f"[{processed}/{total_sensors}] Processing sensor {sensor_id} ({specie}) at {city}"
                    )

                    # Fetch last 14 days of daily aggregated data
                    # Calculate date range (last 14 days from today)
                    date_to = datetime.now().strftime("%Y-%m-%d")
                    date_from = (datetime.now() - timedelta(days=14)).strftime(
                        "%Y-%m-%d"
                    )

                    sensor_url = f"{OPENAQ_BASE_URL}/sensors/{sensor_id}/days"
                    sensor_params = {
                        "limit": 1000,
                        "page": 1,
                        "date_from": date_from,
                        "date_to": date_to,
                    }

                    time.sleep(1)  # Rate limiting

                    try:
                        sensor_response = requests.get(
                            sensor_url, params=sensor_params, headers=headers
                        )
                        sensor_response.raise_for_status()
                        days = sensor_response.json().get("results", [])

                        # Additional filter to ensure we only get recent data
                        cutoff_date = datetime.now() - timedelta(days=14)

                        for day in days:
                            summary = day.get("summary", {})
                            period = day.get("period", {})

                            # Extract date
                            datetime_info = period.get("datetimeFrom", {})
                            date_str = datetime_info.get(
                                "local", datetime_info.get("utc", "")
                            )
                            date = date_str.split("T")[0] if date_str else None

                            # Skip if date is None or older than 14 days
                            if not date:
                                continue

                            try:
                                record_date = datetime.strptime(date, "%Y-%m-%d")
                                if record_date < cutoff_date:
                                    continue  # Skip old data
                            except ValueError:
                                continue  # Skip invalid dates

                            # Calculate variance from standard deviation
                            sd = summary.get("sd")
                            variance = sd**2 if sd is not None else 0.0

                            all_rows.append(
                                {
                                    "Date": date,
                                    "Country": country_name,
                                    "City": city,
                                    "Specie": specie,
                                    "count": summary.get("count", 0),
                                    "min": summary.get("min", 0.0),
                                    "max": summary.get("max", 0.0),
                                    "median": summary.get("median", 0.0),
                                    "variance": variance,
                                    "_ingest_ts": datetime.now(),
                                    "_source_file": f"openaq_sensor_{sensor_id}",
                                    "_source_format": "api_openaq",
                                }
                            )

                    except Exception as e:
                        print(f"Error fetching sensor {sensor_id}: {e}")
                        continue

            print(f"Total records fetched: {len(all_rows)}")
            return all_rows

        except Exception as e:
            print(f"Error fetching OpenAQ data: {e}")
            return []

    # Fetch data and convert to Spark DataFrame
    data = fetch_openaq_nigeria()

    if data:
        pandas_df = pd.DataFrame(data)
        spark_df = spark.createDataFrame(pandas_df, schema=AIR_QUALITY_SCHEMA)

        # Deduplicate based on Date, City, Specie (keep latest ingestion)
        deduplicated = (
            spark_df.withColumn(
                "row_num",
                F.row_number().over(
                    Window.partitionBy("Date", "City", "Specie").orderBy(
                        F.desc("_ingest_ts")
                    )
                ),
            )
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )

        return deduplicated
    else:
        # Return empty DataFrame with correct schema
        return spark.createDataFrame([], schema=AIR_QUALITY_SCHEMA)


# ============================================================================
# TABLE 2: GLOBAL INCREMENTAL DATA (AQICN API)
# ============================================================================


@dlt.table(
    name="air_quality_global_incremental",
    comment="Incremental global air quality data from AQICN API - updates weekly",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_or_drop("valid_date_global", "Date IS NOT NULL")
@dlt.expect_or_drop("valid_country_global", "Country IS NOT NULL")
@dlt.expect("valid_measurements_global", "min >= 0 AND max >= 0 AND median >= 0")
def global_air_quality_incremental():
    """
    Fetch recent global data from AQICN API for comparison countries.
    Scheduled to run weekly to capture new measurements.
    """

    def fetch_aqicn_city(city_name):
        """Fetch current and recent data for a city from AQICN"""
        url = f"{AQICN_BASE_URL}/feed/{city_name}/"
        params = {"token": AQICN_API_TOKEN}

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if data.get("status") != "ok":
                print(f"API returned status: {data.get('status')} for {city_name}")
                return []

            city_data = data.get("data", {})

            # Extract city info
            city_info = city_data.get("city", {})
            city_display_name = city_info.get("name", city_name)

            # Parse country from city name (usually last part after comma)
            country = (
                city_display_name.split(",")[-1].strip()
                if "," in city_display_name
                else "Unknown"
            )

            # Extract measurement time
            time_info = city_data.get("time", {})
            date_str = time_info.get("s", "")
            date = (
                date_str.split(" ")[0]
                if date_str
                else datetime.now().strftime("%Y-%m-%d")
            )

            # Extract pollutant measurements from iaqi
            iaqi = city_data.get("iaqi", {})

            rows = []
            for pollutant, values in iaqi.items():
                if isinstance(values, dict) and "v" in values:
                    measurement = float(values["v"])

                    rows.append(
                        {
                            "Date": date,
                            "Country": country,
                            "City": city_display_name.split(",")[0].strip(),
                            "Specie": pollutant,
                            "count": 1,  # Single real-time measurement
                            "min": measurement,
                            "max": measurement,
                            "median": measurement,
                            "variance": 0.0,  # Real-time data has no variance
                            "_ingest_ts": datetime.now(),
                            "_source_file": f"aqicn_{city_name}",
                            "_source_format": "api_aqicn",
                        }
                    )

            print(f"Fetched {len(rows)} measurements for {city_name}")
            return rows

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print(f"Rate limit hit for {city_name}. Waiting 60 seconds...")
                time.sleep(60)
                return fetch_aqicn_city(city_name)  # Retry
            else:
                print(f"HTTP Error fetching AQICN data for {city_name}: {e}")
        except Exception as e:
            print(f"Error fetching AQICN data for {city_name}: {e}")

        return []

    # Fetch data for all comparison cities
    all_data = []

    for city in COMPARISON_CITIES:
        city_data = fetch_aqicn_city(city)
        all_data.extend(city_data)
        time.sleep(2)  # Rate limiting between requests

    if all_data:
        pandas_df = pd.DataFrame(all_data)
        spark_df = spark.createDataFrame(pandas_df, schema=AIR_QUALITY_SCHEMA)

        # Deduplicate based on Date, City, Specie (keep latest ingestion)
        deduplicated = (
            spark_df.withColumn(
                "row_num",
                F.row_number().over(
                    Window.partitionBy("Date", "City", "Specie").orderBy(
                        F.desc("_ingest_ts")
                    )
                ),
            )
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )

        return deduplicated
    else:
        # Return empty DataFrame with correct schema
        return spark.createDataFrame([], schema=AIR_QUALITY_SCHEMA)
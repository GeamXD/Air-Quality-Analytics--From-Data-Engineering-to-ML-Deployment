# Import modules
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ==========================================
# GOLD LAYER: Dimensional model
# ==========================================
catalog = "air_quality"
gold_schema = "03_gold"


@dp.table(
    name=f"{catalog}.{gold_schema}.dim_locations",
)
@dp.expect_or_drop("valid_country", "Country IS NOT NULL")
@dp.expect_or_drop("valid_city", "City IS NOT NULL")
def dim_locations():
    return spark.sql(
        """
                        WITH location_base AS (
                        SELECT DISTINCT
                            p.country,
                            p.city,
                            p.city_lat AS lat,
                            p.city_lon AS lon,
                            p.population,
                            p.n_stations
                        FROM air_quality.`02_silver`.airquality_places p
                    ),
                    location_metrics AS (
                        SELECT
                            *,
                            -- Calculate population per monitoring station
                            CASE 
                                WHEN n_stations > 0 THEN ROUND(population / n_stations, 2)
                                ELSE NULL 
                            END AS population_per_station,

                            -- Monitoring density score (0-100 scale)
                            -- Lower population_per_station = better coverage = higher score
                            CASE 
                                WHEN n_stations = 0 THEN 0
                                WHEN population / n_stations <= 50000 THEN 100
                                WHEN population / n_stations <= 100000 THEN 80
                                WHEN population / n_stations <= 250000 THEN 60
                                WHEN population / n_stations <= 500000 THEN 40
                                WHEN population / n_stations <= 1000000 THEN 20
                                ELSE 10
                            END AS monitoring_density_score
                        FROM location_base
                    )
                    SELECT
                        -- Generate surrogate key
                        MD5(CONCAT(country, '|', city)) AS location_key,
                        country,
                        city,
                        lat,
                        lon,
                        population,
                        n_stations,
                        population_per_station,
                        monitoring_density_score,
                        CURRENT_TIMESTAMP() AS created_at,
                        CURRENT_TIMESTAMP() AS updated_at
                    FROM location_metrics;
    """
    )


@dp.table(
    name=f"{catalog}.{gold_schema}.dim_pollutants",
)
@dp.expect_or_drop("valid_pollutant_key", "pollutant_key IS NOT NULL")
@dp.expect_or_drop("valid_specie_name", "specie_name IS NOT NULL")
def dim_pollutants():
    return spark.sql(
        """
SELECT
    ROW_NUMBER() OVER (ORDER BY specie_name) AS pollutant_key,
    specie_name,
    who_guideline_24h,
    who_guideline_annual,
    health_impact_category,
    description
FROM (
    VALUES
        ('bc', NULL, NULL, 'High', 'Black Carbon – no numeric WHO guideline (good practice only)'),
        ('co', 4.0, NULL, 'Moderate', 'Carbon Monoxide – 24-h WHO AQG 4 mg/m³'),
        ('no2', 25.0, 10.0, 'High', 'Nitrogen Dioxide – 24-h 25 µg/m³, annual 10 µg/m³'),
        ('o3', 100.0, 60.0, 'Moderate', 'Ozone – 8-h 100 µg/m³, peak season 60 µg/m³'),
        ('pm10', 45.0, 15.0, 'High', 'Particulate Matter (PM10) – 24-h 45 µg/m³, annual 15 µg/m³'),
        ('pm25', 15.0, 5.0, 'Critical', 'Fine Particulate Matter (PM2.5) – 24-h 15 µg/m³, annual 5 µg/m³'),
        ('so2', 40.0, NULL, 'Moderate', 'Sulfur Dioxide – 24-h WHO AQG 40 µg/m³')
) AS t(specie_name, who_guideline_24h, who_guideline_annual, health_impact_category, description);
"""
    )


@dp.table(
    name=f"{catalog}.{gold_schema}.dim_dates",
)
@dp.expect_or_drop("valid_date_key", "date_key IS NOT NULL")
@dp.expect_or_drop("valid_date", "date IS NOT NULL")
def dim_dates():
    return spark.sql(
        """
WITH date_spine AS (
    -- Generate ~13 years of dates starting from 2015-01-01
    SELECT explode(
        sequence(
            DATE '2015-01-01',
            DATE_ADD(DATE '2015-01-01', 5000)
        )
    ) AS date
)
SELECT
    CAST(date_format(date, 'yyyyMMdd') AS INT) AS date_key,
    date,
    year(date) AS year,
    quarter(date) AS quarter,
    month(date) AS month,
    day(date) AS day,
    dayofweek(date) AS day_of_week,
    date_format(date, 'EEEE') AS day_name,
    date_format(date, 'MMMM') AS month_name,
    weekofyear(date) AS week_of_year,
    dayofyear(date) AS day_of_year,

    -- Season calculation (Northern Hemisphere)
    CASE 
        WHEN month(date) IN (12, 1, 2) THEN 'Winter'
        WHEN month(date) IN (3, 4, 5) THEN 'Spring'
        WHEN month(date) IN (6, 7, 8) THEN 'Summer'
        WHEN month(date) IN (9, 10, 11) THEN 'Fall'
    END AS season,

    -- Business flags
    CASE WHEN dayofweek(date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN day(date) = 1 THEN TRUE ELSE FALSE END AS is_month_start,
    CASE WHEN day(date) = day(last_day(date)) THEN TRUE ELSE FALSE END AS is_month_end
FROM date_spine;
"""
    )


@dp.table(
    name=f"{catalog}.{gold_schema}.fact_air_quality_daily",
)
@dp.expect_or_drop("valid_fact_key", "fact_key IS NOT NULL")
@dp.expect_or_drop("valid_date_key", "date_key IS NOT NULL")
@dp.expect_or_drop("valid_location_key", "location_key IS NOT NULL")
def fact_air_quality_daily():
    return spark.sql(
        """
WITH pivoted_data AS (
    SELECT
        date,
        country,
        city,
        -- PM2.5 metrics
        MAX(CASE WHEN specie = 'pm25' THEN min END) AS pm25_min,
        MAX(CASE WHEN specie = 'pm25' THEN max END) AS pm25_max,
        MAX(CASE WHEN specie = 'pm25' THEN median END) AS pm25_median,
        MAX(CASE WHEN specie = 'pm25' THEN variance END) AS pm25_variance,
        MAX(CASE WHEN specie = 'pm25' THEN count END) AS pm25_count,

        -- PM10 metrics
        MAX(CASE WHEN specie = 'pm10' THEN min END) AS pm10_min,
        MAX(CASE WHEN specie = 'pm10' THEN max END) AS pm10_max,
        MAX(CASE WHEN specie = 'pm10' THEN median END) AS pm10_median,
        MAX(CASE WHEN specie = 'pm10' THEN variance END) AS pm10_variance,
        MAX(CASE WHEN specie = 'pm10' THEN count END) AS pm10_count,

        -- NO2 metrics
        MAX(CASE WHEN specie = 'no2' THEN min END) AS no2_min,
        MAX(CASE WHEN specie = 'no2' THEN max END) AS no2_max,
        MAX(CASE WHEN specie = 'no2' THEN median END) AS no2_median,
        MAX(CASE WHEN specie = 'no2' THEN variance END) AS no2_variance,
        MAX(CASE WHEN specie = 'no2' THEN count END) AS no2_count,

        -- O3 metrics
        MAX(CASE WHEN specie = 'o3' THEN min END) AS o3_min,
        MAX(CASE WHEN specie = 'o3' THEN max END) AS o3_max,
        MAX(CASE WHEN specie = 'o3' THEN median END) AS o3_median,
        MAX(CASE WHEN specie = 'o3' THEN variance END) AS o3_variance,
        MAX(CASE WHEN specie = 'o3' THEN count END) AS o3_count,

        -- SO2 metrics
        MAX(CASE WHEN specie = 'so2' THEN min END) AS so2_min,
        MAX(CASE WHEN specie = 'so2' THEN max END) AS so2_max,
        MAX(CASE WHEN specie = 'so2' THEN median END) AS so2_median,
        MAX(CASE WHEN specie = 'so2' THEN variance END) AS so2_variance,
        MAX(CASE WHEN specie = 'so2' THEN count END) AS so2_count,

        -- CO metrics
        MAX(CASE WHEN specie = 'co' THEN min END) AS co_min,
        MAX(CASE WHEN specie = 'co' THEN max END) AS co_max,
        MAX(CASE WHEN specie = 'co' THEN median END) AS co_median,
        MAX(CASE WHEN specie = 'co' THEN variance END) AS co_variance,
        MAX(CASE WHEN specie = 'co' THEN count END) AS co_count,

        -- BC metrics
        MAX(CASE WHEN specie = 'bc' THEN min END) AS bc_min,
        MAX(CASE WHEN specie = 'bc' THEN max END) AS bc_max,
        MAX(CASE WHEN specie = 'bc' THEN median END) AS bc_median,
        MAX(CASE WHEN specie = 'bc' THEN variance END) AS bc_variance,
        MAX(CASE WHEN specie = 'bc' THEN count END) AS bc_count
    FROM air_quality.`02_silver`.air_quality_slv
    GROUP BY date, country, city
)
SELECT
    -- Surrogate key
    MD5(CONCAT(CAST(p.date AS STRING), '|', p.country, '|', p.city)) AS fact_key,

    -- Foreign keys
    CAST(date_format(p.date, 'yyyyMMdd') AS BIGINT) AS date_key,  -- FIXED: Changed INT64 to BIGINT
    l.location_key,

    -- Natural keys
    p.date,
    p.country,
    p.city,

    -- All pollutant metrics
    p.pm25_min, p.pm25_max, p.pm25_median, p.pm25_variance, p.pm25_count,
    p.pm10_min, p.pm10_max, p.pm10_median, p.pm10_variance, p.pm10_count,
    p.no2_min, p.no2_max, p.no2_median, p.no2_variance, p.no2_count,
    p.o3_min, p.o3_max, p.o3_median, p.o3_variance, p.o3_count,
    p.so2_min, p.so2_max, p.so2_median, p.so2_variance, p.so2_count,
    p.co_min, p.co_max, p.co_median, p.co_variance, p.co_count,
    p.bc_min, p.bc_max, p.bc_median, p.bc_variance, p.bc_count,

    -- Air Quality Index (simplified AQI based on PM2.5)
    CASE
        WHEN p.pm25_median <= 12 THEN 'Good'
        WHEN p.pm25_median <= 35.4 THEN 'Moderate'
        WHEN p.pm25_median <= 55.4 THEN 'Unhealthy for Sensitive Groups'
        WHEN p.pm25_median <= 150.4 THEN 'Unhealthy'
        WHEN p.pm25_median <= 250.4 THEN 'Very Unhealthy'
        ELSE 'Hazardous'
    END AS aqi_category,

    -- Data quality flag
    CASE 
        WHEN p.pm25_count >= 18 THEN 'High'  -- 75%+ hourly readings
        WHEN p.pm25_count >= 12 THEN 'Medium'
        ELSE 'Low'
    END AS data_quality,

    CURRENT_TIMESTAMP() AS created_at
FROM pivoted_data p
LEFT JOIN air_quality.`03_gold`.dim_locations l ON p.country = l.country AND p.city = l.city; 
"""
    )


@dp.table(
    name=f"{catalog}.{gold_schema}.fact_air_quality_aggregated",
)
def fact_air_quality_aggregated():
    return spark.sql(
        """
WITH monthly_agg AS (
    SELECT
        l.location_key,
        f.country,
        f.city,
        d.year,
        d.month,
        d.quarter,

        -- Aggregation period
        'monthly' AS aggregation_period,
        MIN(f.date) AS period_start_date,
        MAX(f.date) AS period_end_date,

        -- PM2.5 aggregations
        AVG(f.pm25_median) AS pm25_avg_median,
        MIN(f.pm25_min) AS pm25_period_min,
        MAX(f.pm25_max) AS pm25_period_max,
        STDDEV(f.pm25_median) AS pm25_std_dev,

        -- WHO exceedance days (24-hour guideline: 15 µg/m³)
        SUM(CASE WHEN f.pm25_median > 15 THEN 1 ELSE 0 END) AS pm25_exceedance_days,
        COUNT(*) AS total_days_measured,
        ROUND(SUM(CASE WHEN f.pm25_median > 15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pm25_exceedance_pct,

        -- PM10 aggregations
        AVG(f.pm10_median) AS pm10_avg_median,
        SUM(CASE WHEN f.pm10_median > 45 THEN 1 ELSE 0 END) AS pm10_exceedance_days,

        -- NO2 aggregations
        AVG(f.no2_median) AS no2_avg_median,
        SUM(CASE WHEN f.no2_median > 25 THEN 1 ELSE 0 END) AS no2_exceedance_days,

        -- Data quality metrics
        AVG(f.pm25_count) AS avg_daily_measurements,
        MIN(f.pm25_count) AS min_daily_measurements

    FROM air_quality.`03_gold`.fact_air_quality_daily f  -- FIXED: Added backticks
    LEFT JOIN air_quality.`03_gold`.dim_locations l ON f.location_key = l.location_key
    LEFT JOIN air_quality.`03_gold`.dim_dates d ON f.date_key = d.date_key  -- FIXED: Changed dim_date to dim_dates
    WHERE f.pm25_median IS NOT NULL
    GROUP BY l.location_key, f.country, f.city, d.year, d.month, d.quarter
),
yearly_agg AS (
    SELECT
        l.location_key,
        f.country,
        f.city,
        d.year,
        NULL AS month,
        NULL AS quarter,

        'yearly' AS aggregation_period,
        MIN(f.date) AS period_start_date,
        MAX(f.date) AS period_end_date,

        AVG(f.pm25_median) AS pm25_avg_median,
        MIN(f.pm25_min) AS pm25_period_min,
        MAX(f.pm25_max) AS pm25_period_max,
        STDDEV(f.pm25_median) AS pm25_std_dev,

        SUM(CASE WHEN f.pm25_median > 15 THEN 1 ELSE 0 END) AS pm25_exceedance_days,
        COUNT(*) AS total_days_measured,
        ROUND(SUM(CASE WHEN f.pm25_median > 15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pm25_exceedance_pct,

        AVG(f.pm10_median) AS pm10_avg_median,
        SUM(CASE WHEN f.pm10_median > 45 THEN 1 ELSE 0 END) AS pm10_exceedance_days,

        AVG(f.no2_median) AS no2_avg_median,
        SUM(CASE WHEN f.no2_median > 25 THEN 1 ELSE 0 END) AS no2_exceedance_days,

        AVG(f.pm25_count) AS avg_daily_measurements,
        MIN(f.pm25_count) AS min_daily_measurements

    FROM air_quality.`03_gold`.fact_air_quality_daily f  -- FIXED: Added backticks
    LEFT JOIN air_quality.`03_gold`.dim_locations l ON f.location_key = l.location_key
    LEFT JOIN air_quality.`03_gold`.dim_dates d ON f.date_key = d.date_key  -- FIXED: Changed dim_date to dim_dates
    WHERE f.pm25_median IS NOT NULL
    GROUP BY l.location_key, f.country, f.city, d.year
)
SELECT
    *,
    -- Trend indicator (compare to previous period)
    LAG(pm25_avg_median) OVER (
        PARTITION BY location_key, aggregation_period 
        ORDER BY year, COALESCE(month, 0)
    ) AS pm25_prev_period,

    CASE
        WHEN LAG(pm25_avg_median) OVER (
            PARTITION BY location_key, aggregation_period 
            ORDER BY year, COALESCE(month, 0)
        ) IS NULL THEN 'No Data'
        WHEN pm25_avg_median > LAG(pm25_avg_median) OVER (
            PARTITION BY location_key, aggregation_period 
            ORDER BY year, COALESCE(month, 0)
        ) * 1.1 THEN 'Worsening'
        WHEN pm25_avg_median < LAG(pm25_avg_median) OVER (
            PARTITION BY location_key, aggregation_period 
            ORDER BY year, COALESCE(month, 0)
        ) * 0.9 THEN 'Improving'
        ELSE 'Stable'
    END AS trend_indicator,

    CURRENT_TIMESTAMP() AS created_at
FROM (
    SELECT * FROM monthly_agg
    UNION ALL
    SELECT * FROM yearly_agg
);
"""
    )


@dp.table(name=f"{catalog}.{gold_schema}.view_nigeria_vs_world")
@dp.expect_or_drop("valid_country", "country IS NOT NULL")
def view_nigeria_vs_world():
    return spark.sql(
        """
WITH nigeria_stats AS (
    SELECT
        f.city,
        l.population,
        l.n_stations,
        l.monitoring_density_score,
        AVG(f.pm25_median) AS avg_pm25,
        AVG(f.pm10_median) AS avg_pm10,
        AVG(f.no2_median) AS avg_no2,
        SUM(CASE WHEN f.pm25_median > 15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pm25_exceedance_pct,
        COUNT(*) AS total_days
    FROM air_quality.`03_gold`.fact_air_quality_daily f
    JOIN air_quality.`03_gold`.dim_locations l ON f.location_key = l.location_key
    WHERE f.country = 'NG'
    GROUP BY f.city, l.population, l.n_stations, l.monitoring_density_score
),
benchmark_countries AS (
    -- Select peer developing nations + some developed for comparison
    SELECT
        f.country,
        f.city,
        l.population,
        l.n_stations,
        AVG(f.pm25_median) AS avg_pm25,
        AVG(f.pm10_median) AS avg_pm10,
        AVG(f.no2_median) AS avg_no2,
        SUM(CASE WHEN f.pm25_median > 15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pm25_exceedance_pct
    FROM air_quality.`03_gold`.fact_air_quality_daily f  -- FIXED: Added backticks
    JOIN air_quality.`03_gold`.dim_locations l ON f.location_key = l.location_key
    WHERE f.country IN ('IN', 'CN', 'BR', 'ZA', 'KE', 'US', 'GB', 'DE', 'SG')
    GROUP BY f.country, f.city, l.population, l.n_stations
),
country_aggregates AS (
    SELECT
        country,
        COUNT(DISTINCT city) AS n_cities,
        SUM(population) AS total_population,
        SUM(n_stations) AS total_stations,
        AVG(avg_pm25) AS country_avg_pm25,
        AVG(pm25_exceedance_pct) AS country_exceedance_pct,
        -- Population-weighted pollution
        SUM(avg_pm25 * population) / SUM(population) AS population_weighted_pm25
    FROM benchmark_countries
    GROUP BY country
)
SELECT
    -- Nigeria cities
    'NG' AS country,
    ns.city,
    ns.population,
    ns.n_stations,
    ns.monitoring_density_score,
    ns.avg_pm25,
    ns.avg_pm10,
    ns.avg_no2,
    ns.pm25_exceedance_pct,

    -- WHO comparison
    15.0 AS who_guideline_24h,
    ns.avg_pm25 - 15.0 AS deviation_from_who,

    -- Nigeria vs benchmark comparison
    (SELECT AVG(country_avg_pm25) FROM country_aggregates) AS benchmark_avg_pm25,
    ns.avg_pm25 - (SELECT AVG(country_avg_pm25) FROM country_aggregates) AS vs_benchmark,

    -- Population at risk
    ns.population AS population_at_risk,

    -- Risk category
    CASE
        WHEN ns.avg_pm25 > 35 THEN 'Critical'
        WHEN ns.avg_pm25 > 25 THEN 'High'
        WHEN ns.avg_pm25 > 15 THEN 'Moderate'
        ELSE 'Low'
    END AS risk_category

FROM nigeria_stats ns

UNION ALL

-- Add benchmark country summaries for comparison
SELECT
    ca.country,
    'National Average' AS city,
    ca.total_population AS population,
    ca.total_stations AS n_stations,
    NULL AS monitoring_density_score,
    ca.population_weighted_pm25 AS avg_pm25,
    NULL AS avg_pm10,
    NULL AS avg_no2,
    ca.country_exceedance_pct AS pm25_exceedance_pct,
    15.0 AS who_guideline_24h,
    ca.population_weighted_pm25 - 15.0 AS deviation_from_who,
    (SELECT AVG(country_avg_pm25) FROM country_aggregates) AS benchmark_avg_pm25,
    ca.population_weighted_pm25 - (SELECT AVG(country_avg_pm25) FROM country_aggregates) AS vs_benchmark,
    ca.total_population AS population_at_risk,
    CASE
        WHEN ca.population_weighted_pm25 > 35 THEN 'Critical'
        WHEN ca.population_weighted_pm25 > 25 THEN 'High'
        WHEN ca.population_weighted_pm25 > 15 THEN 'Moderate'
        ELSE 'Low'
    END AS risk_category
FROM country_aggregates ca;
"""
    )


@dp.table(
    name=f"{catalog}.{gold_schema}.ml_features_table",
)
# @dp.expect_or_drop("valid_country", "country IS NOT NULL")
def ml_features_table():
    return spark.sql(
        """
WITH daily_features AS (
    SELECT
        f.location_key,
        f.date,
        d.year,
        d.month,
        d.day_of_week,
        d.quarter,
        d.is_weekend,

        -- Location features
        l.population,
        l.n_stations,
        l.population_per_station,
        l.monitoring_density_score,
        l.lat,
        l.lon,

        -- Current day pollution
        f.pm25_median,
        f.pm10_median,
        f.no2_median,

        -- Rolling averages (7-day)
        AVG(f.pm25_median) OVER (
            PARTITION BY f.location_key
            ORDER BY f.date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS pm25_7d_avg,

        -- Rolling averages (30-day)
        AVG(f.pm25_median) OVER (
            PARTITION BY f.location_key
            ORDER BY f.date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS pm25_30d_avg,

        -- Rolling averages (90-day)
        AVG(f.pm25_median) OVER (
            PARTITION BY f.location_key
            ORDER BY f.date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS pm25_90d_avg,

        -- Volatility (7-day standard deviation)
        STDDEV(f.pm25_median) OVER (
            PARTITION BY f.location_key 
            ORDER BY f.date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS pm25_7d_volatility,

        -- Trend (difference between current and 7-day average)
        f.pm25_median - AVG(f.pm25_median) OVER (
            PARTITION BY f.location_key 
            ORDER BY f.date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS pm25_trend,

        -- Lag features (previous days)
        LAG(f.pm25_median, 1) OVER (PARTITION BY f.location_key ORDER BY f.date) AS pm25_lag_1d,
        LAG(f.pm25_median, 7) OVER (PARTITION BY f.location_key ORDER BY f.date) AS pm25_lag_7d,

        -- Target variable: 30-day forward average PM2.5
        AVG(f.pm25_median) OVER (
            PARTITION BY f.location_key
            ORDER BY f.date
            ROWS BETWEEN CURRENT ROW AND 29 FOLLOWING
        ) AS target_next_30d_pm25_median

    FROM air_quality.`03_gold`.fact_air_quality_daily f  -- FIXED: Added backticks
    LEFT JOIN air_quality.`03_gold`.dim_locations l ON f.location_key = l.location_key
    LEFT JOIN air_quality.`03_gold`.dim_dates d ON f.date_key = d.date_key  -- FIXED: Changed dim_date to dim_dates
    WHERE f.pm25_median IS NOT NULL
)
SELECT
    location_key,
    date,
    year,
    month,
    day_of_week,
    quarter,
    is_weekend,

    -- Seasonal indicators (one-hot encoded)
    CASE WHEN month IN (12, 1, 2) THEN 1 ELSE 0 END AS is_winter,
    CASE WHEN month IN (3, 4, 5) THEN 1 ELSE 0 END AS is_spring,
    CASE WHEN month IN (6, 7, 8) THEN 1 ELSE 0 END AS is_summer,
    CASE WHEN month IN (9, 10, 11) THEN 1 ELSE 0 END AS is_fall,

    -- Location features
    population,
    n_stations,
    population_per_station,
    monitoring_density_score,
    lat,
    lon,

    -- Historical pollution features
    pm25_median AS pm25_current,
    pm10_median AS pm10_current,
    no2_median AS no2_current,
    pm25_7d_avg,
    pm25_30d_avg,
    pm25_90d_avg,
    pm25_7d_volatility,
    pm25_trend,
    pm25_lag_1d,
    pm25_lag_7d,

    -- Derived features
    CASE WHEN pm25_median > 15 THEN 1 ELSE 0 END AS is_exceeding_who,
    pm25_median / NULLIF(pm25_30d_avg, 0) AS pm25_ratio_to_30d_avg,

    -- Target variable
    target_next_30d_pm25_median,

    -- Binary classification target (will PM2.5 exceed WHO guideline in next 30 days?)
    CASE WHEN target_next_30d_pm25_median > 15 THEN 1 ELSE 0 END AS target_will_exceed_who,

    CURRENT_TIMESTAMP() AS created_at
FROM daily_features
WHERE 
    -- Ensure we have enough historical data for features
    pm25_7d_avg IS NOT NULL 
    AND pm25_30d_avg IS NOT NULL
    -- Only include rows where we have a target (not the last 30 days of data)
    AND target_next_30d_pm25_median IS NOT NULL;
"""
)
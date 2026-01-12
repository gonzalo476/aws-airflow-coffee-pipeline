-- count total rows and check null values
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN t2m IS NULL THEN 1 ELSE 0 END) AS nulls_t2m,
    SUM(CASE WHEN t2m_max IS NULL THEN 1 ELSE 0 END) AS nulls_t2m_max,
    SUM(CASE WHEN t2m_min IS NULL THEN 1 ELSE 0 END) AS nulls_t2m_min,
    SUM(CASE WHEN prectotcorr IS NULL THEN 1 ELSE 0 END) AS nulls_prectotcorr,
    SUM(CASE WHEN rh2m IS NULL THEN 1 ELSE 0 END) AS nulls_rh2m,
    SUM(CASE WHEN allsky_sfc_sw_dwn IS NULL THEN 1 ELSE 0 END) AS nulls_allsky,
    SUM(CASE WHEN city IS NULL OR TRIM(city) = '' THEN 1 ELSE 0 END) AS nulls_o_vacios_city
FROM weather;

-- check t2m in range -150 to 400
SELECT
    *
FROM weather
WHERE
    TRY_CAST(t2m AS DOUBLE) < -100
    OR TRY_CAST(t2m AS DOUBLE) > 400
LIMIT 200;

-- check t2m_min > t2m_max
SELECT *
FROM weather
WHERE TRY_CAST(t2m_min AS double) IS NOT NULL
    AND TRY_CAST(t2m_max AS double) IS NOT NULL
    AND TRY_CAST(t2m_min AS double) > TRY_CAST(t2m_max AS double)
LIMIT 200;

-- check if t2m is outside t2m_min and t2m_max
SELECT * 
FROM weather
WHERE TRY_CAST(t2m AS double) IS NOT NULL
    AND TRY_CAST(t2m_min AS double) IS NOT NULL
    AND TRY_CAST(t2m_max AS double) IS NOT NULL
    AND TRY_CAST(t2m AS double) < TRY_CAST(t2m_min AS double)
        OR TRY_CAST(t2m AS double) > TRY_CAST(t2m_max AS double)
LIMIT 200;

-- check if t2m_max - t2m_min is too big > 50 C
SELECT *
FROM weather
WHERE TRY_CAST(t2m_max AS double) - TRY_CAST(t2m_min AS double) > 50
LIMIT 200;

-- check prectotcorr negative values
SELECT *
FROM weather
WHERE TRY_CAST(prectotcorr AS double) < 0
    OR prectotcorr IN (-9999, -999.0)
LIMIT 200;

-- check rh2m humidity between 0% and 100%
SELECT *
FROM weather
WHERE TRY_CAST(rh2m as double) < 0 OR TRY_CAST(rh2m AS double) > 100
LIMIT 200;

-- check allsky_sfc_sw_dwn between 0 and 1000
SELECT *
FROM weather
WHERE TRY_CAST(allsky_sfc_sw_dwn AS double) < 0
    AND TRY_CAST(allsky_sfc_sw_dwn AS double) > 1000
LIMIT 200;

-- check city distinct values
SELECT DISTINCT city
FROM weather;

-- check if there are rows that not equal to 'naha', 'okinawa', 'nago' or 'kunigami'
SELECT *
FROM weather
WHERE LOWER(TRIM(city)) NOT IN ('naha', 'okinawa', 'nago', 'kunigami')
    OR city IS NULL;


-- create silver weather table
CREATE TABLE db_coffee_clean.weather_data_silver
WITH (
    format = 'PARQUET',
    external_location = 's3://coffee-clean-zone-gonz67/weather/',
    parquet_compression = 'SNAPPY',
    partitioned_by = ARRAY['year']
) AS
SELECT
    CAST(index AS DATE) AS capture_date,
    CAST(t2m AS DOUBLE) AS t2m,
    CAST(t2m_min AS DOUBLE) AS t2m_min,
    CAST(t2m_max AS DOUBLE) AS t2m_max,
    CAST(prectotcorr AS DOUBLE) AS prectotcorr,
    CAST(rh2m AS DOUBLE) AS rh2m,
    CAST(allsky_sfc_sw_dwn AS DOUBLE) AS allsky_sfc_sw_dwn,
    TRIM(city) AS city,
    YEAR(CAST(index AS DATE)) AS "year"
FROM weather
WHERE index IS NOT NULL;

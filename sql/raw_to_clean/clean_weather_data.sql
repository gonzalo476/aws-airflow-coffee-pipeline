-- count total rows and check null values
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN t2m IS NULL THEN 1 ELSE 0 END) AS nulls_t2m,
    SUM(CASE WHEN t2m_max IS NULL THEN 1 ELSE 0 END) AS nulls_t2m_max,
    SUM(CASE WHEN t2m_min IS NULL THEN 1 ELSE 0 END) AS nulls_t2m_min,
    SUM(CASE WHEN prectotcorr IS NULL THEN 1 ELSE 0 END) AS nulls_prectotcorr,
    SUM(CASE WHEN rh2m IS NULL THEN 1 ELSE 0 END) AS nulls_rh2m,
    SUM(CASE WHEN allsky_sfc_sw_dwn IS NULL THEN 1 ELSE 0 END) AS nulls_allsky,
    SUM(CASE WHEN city IS NULL OR TRIM(city) = '' THEN 1 ELSE 0 END) AS nulls_city
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

-- create cleaned weather table
CREATE TABLE db_coffee_clean.weather_data
WITH (
    format = 'PARQUET',
    external_location = '',
    parquet_compression = 'SNAPPY',
    partitioned_by = ARRAY['year']
) AS
SELECT
    -- date standarization
    CAST(index AS DATE) AS capture_date,

    -- Metrics cleaning
    -- t2m
    CASE
        WHEN TRY_CAST(t2m AS DOUBLE) BETWEEN -100 AND 400 THEN CAST(t2m AS DOUBLE)
        ELSE NULL
    END AS t2m,

    -- t2m_min
    CASE
        WHEN TRY_CAST(t2m_min AS DOUBLE) BETWEEN -100 AND 400 THEN CAST(t2m_min AS DOUBLE)
        ELSE NULL
    END AS t2m_min,

    -- t2m_max
    CASE
        WHEN TRY_CAST(t2m_max AS DOUBLE) BETWEEN -100 AND 400 THEN CAST(t2m_max AS DOUBLE)
        ELSE NULL
    END AS t2m_max,

    -- logic for rain, no nulls
    CASE
        WHEN TRY_CAST(prectotcorr AS DOUBLE) >= 0 THEN CAST(prectotcorr AS DOUBLE)
        ELSE NULL
    END AS prectotcorr,

    -- humidity logic
    CASE
        WHEN TRY_CAST(rh2m AS DOUBLE) BETWEEN 0 AND 100 THEN CAST(rh2m AS DOUBLE)
        ELSE NULL
    END AS rh2m,

    CAST(allsky_sfc_sw_dwn AS DOUBLE) AS allsky_sfc_sw_dwn,

    -- text standarization
    LOWER(TRIM(city)) AS city,

    -- partition column
    YEAR(CAST(index AS DATE)) AS "year"

FROM weather
WHERE
    index IS NOT NULL

    AND LOWER(TRIM(city)) IN ('naha', 'okinawa', 'nago', 'kunigami')

    AND (
        TRY_CAST(t2m_min AS DOUBLE) IS NULL
        OR TRY_CAST(t2m_max AS DOUBLE) IS NULL
        OR TRY_CAST(t2m_min AS DOUBLE) <= TRY_CAST(t2m_max AS DOUBLE) 
    );

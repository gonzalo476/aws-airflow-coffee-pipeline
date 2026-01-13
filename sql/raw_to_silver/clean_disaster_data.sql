-- only JPN iso needed
-- create a new raw table to make querys eficient
CREATE TABLE disasters
WITH (
    format = 'PARQUET'
) AS
SELECT
    *
FROM "2026_01_12"
WHERE iso3 = 'JPN';

-- count total rows and check null values
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS nulls_id,
    SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) AS nulls_country,
    SUM(CASE WHEN iso3 IS NULL THEN 1 ELSE 0 END) AS nulls_iso3,
    SUM(CASE WHEN gwno IS NULL THEN 1 ELSE 0 END) AS nulls_gwno,
    SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) AS nulls_year,
    SUM(CASE WHEN geo_id IS NULL THEN 1 ELSE 0 END) AS nulls_geo_id,
    SUM(CASE WHEN geolocation IS NULL THEN 1 ELSE 0 END) AS nulls_geolocation,
    SUM(CASE WHEN level IS NULL THEN 1 ELSE 0 END) AS nulls_level,
    SUM(CASE WHEN adm1 IS NULL THEN 1 ELSE 0 END) AS nulls_adm1,
    SUM(CASE WHEN adm2 IS NULL THEN 1 ELSE 0 END) AS nulls_adm2,
    SUM(CASE WHEN adm3 IS NULL THEN 1 ELSE 0 END) AS nulls_adm3,
    SUM(CASE WHEN location IS NULL THEN 1 ELSE 0 END) AS nulls_location,
    SUM(CASE WHEN historical IS NULL THEN 1 ELSE 0 END) AS nulls_historical,
    SUM(CASE WHEN hist_country IS NULL THEN 1 ELSE 0 END) AS nulls_hist_country,
    SUM(CASE WHEN disastertype IS NULL THEN 1 ELSE 0 END) AS nulls_disastertype,
    SUM(CASE WHEN disasterno IS NULL THEN 1 ELSE 0 END) AS nulls_disasterno,
    SUM(CASE WHEN latitude IS NULL THEN 1 ELSE 0 END) AS nulls_latitude,
    SUM(CASE WHEN longitude IS NULL THEN 1 ELSE 0 END) AS nulls_longitude
FROM disasters;

-- logical duplicates
SELECT
    disasterno,
    geo_id,
    COUNT(*) AS cnt
FROM disasters
GROUP BY 1, 2
HAVING COUNT(*) > 1;

-- check age range 1960 - 2018
SELECT
    MIN(year) AS min_year,
    MAX(year) AS max_year
FROM disasters;

-- invalid coords
SELECT COUNT(*) AS invalid_coords
FROM disasters
WHERE latitude NOT BETWEEN -90 AND 90
    OR longitude NOT BETWEEN -180 AND 180;

-- category distincts
SELECT DISTINCT disastertype
FROM disasters
ORDER BY 1;

-- country distincts
SELECT DISTINCT country
FROM disasters;

-- create silver disaster table
-- capa silver: year, adm1, disastertype, disasterno, latitude, longitude
CREATE TABLE db_coffee_clean.disaster_data_silver
WITH (
    format = 'PARQUET',
    external_location = '<RAW S3 BUCKET>',
    parquet_compression = 'SNAPPY'
) AS
SELECT
    CAST(year AS INT) AS year,
    TRIM(adm1) AS zone,
    TRIM(disastertype) AS disastertype,
    TRIM(disasterno) AS disasterno,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude
FROM disasters
WHERE
    year IS NOT NULL
    AND year BETWEEN 1960 AND 2018
    AND disastertype IS NOT NULL
    AND adm1 IS NOT NULL
    AND disasterno IS NOT NULL
    AND latitude BETWEEN -90 AND 90
    AND longitude BETWEEN -180 AND 180;

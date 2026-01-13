CREATE TABLE db_coffee_clean.soil_data
WITH (
    format = 'PARQUET',
    external_location = '',
    parquet_compression = 'SNAPPY'
)
AS
SELECT
    lat,
    lon,
    clay,
    ph,
    sand,
    soc
FROM db_coffee_raw.soil
WHERE clay > 0
    AND ph > 0
    AND sand > 0;
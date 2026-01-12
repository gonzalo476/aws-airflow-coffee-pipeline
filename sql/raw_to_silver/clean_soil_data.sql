CREATE TABLE db_coffee_clean.soil_data_silver
WITH (
    format = 'PARQUET',
    external_location = '<RAW S3 BUCKET>',
    parquet_compression = 'SNAPPY'
)
AS
SELECT
    lat,
    CAST(lon AS DECIMAL(10, 3)) AS lon,
    clay,
    ph,
    sand,
    soc,
    CAST(current_timestamp AS timestamp) as processed_at
FROM db_coffee_raw.soil
WHERE clay > 0
    AND ph > 0
    AND sand > 0;
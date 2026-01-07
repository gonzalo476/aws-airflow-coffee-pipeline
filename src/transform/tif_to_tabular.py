
def tiff_to_dataframe(tiff_path: str, output_csv: str) -> None:
    import rasterio
    import pandas as pd

    with rasterio.open(tiff_path) as src:
        data = src.read(1)
        transform = src.transform

    rows, cols = data.shape
    records = []

    for r in range(rows):
        for c in range(cols):
            value = data[r, c]
            if value == src.nodata:
                continue

            lon, lat = transform * (c, r)
            records.append((lat, lon, value))

    df = pd.DataFrame(records, columns=["lat", "lon", "value"])
    df.to_csv(output_csv, index=False)
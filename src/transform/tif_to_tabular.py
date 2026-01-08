def tiffs_to_dataframe(output_csv: str) -> None:
    import rasterio
    import pandas as pd
    import numpy as np

    rasters = {
        "clay": "./data/raw/soil/clay.tif",
        "ph": "./data/raw/soil/ph.tif",
        "sand": "./data/raw/soil/sand.tif",
        "soc": "./data/raw/soil/soc.tif",
    }

    # Storage for loaded data and metadata
    data_arrays = {}
    nodata_values = {}
    
    # 1. Open files and load arrays INTO MEMORY once
    first_var = True
    for var, path in rasters.items(): # Fix: added .items()
        with rasterio.open(path) as ds:
            if first_var:
                # Use the first raster to set the grid reference
                transform = ds.transform
                rows, cols = ds.shape
                ref_crs = ds.crs
                first_var = False
            else:
                # Basic validation
                assert ds.shape == (rows, cols), f"{var} has different dimensions"

            data_arrays[var] = ds.read(1) # Read the full band once
            nodata_values[var] = ds.nodata

    # 2. Process pixels
    records = []
    for r in range(rows):
        for c in range(cols):
            # Calculate coordinates using the transform matrix
            lon, lat = transform * (c, r)
            
            row = {"lat": lat, "lon": lon}
            
            # Extract values from the pre-loaded arrays
            for var, arr in data_arrays.items():
                val = arr[r, c]
                row[var] = None if val == nodata_values[var] else float(val)
            
            records.append(row)

    # 3. Save to CSV
    df = pd.DataFrame(records)
    df.to_csv(output_csv, index=False)
    print(f"Successfully processed {len(df)} rows to {output_csv}")
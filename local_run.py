from src.transform.tif_to_tabular import tiff_to_dataframe

tiff_to_dataframe(
    tiff_path="./data/raw/soil/clay.tif",
    output_csv="./data/curated/soilgrid_clay.csv"
)
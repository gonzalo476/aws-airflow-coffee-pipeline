FROM apache/airflow:3.1.5-python3.10

USER root
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    pandas \
    numpy \
    rasterio \
    boto3 \
    requests

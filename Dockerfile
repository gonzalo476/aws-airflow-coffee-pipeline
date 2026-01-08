FROM apache/airflow:3.1.5-python3.10

USER root

RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    build-essential \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

USER airflow

RUN pip install --no-cache-dir \
    pandas \
    numpy \
    rasterio \
    boto3 \
    requests \
    apache-airflow-providers-fab

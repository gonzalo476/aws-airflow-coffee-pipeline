# Pipeline de Data Engineering — Airflow + Docker + AWS + Power BI

Pipeline para la *ingestión*, *transformación* y *exposición* analítica de datos climáticos usando **Airflow**, **AWS** y **Power BI**.

## Contexto del problema

El proyecto procesa datos climáticos proveniente de una **API** externa y archivos `csv` históricos. El principal objetivo es **centralizar la información, limpiarla y exponerla** para el análisis. 

El proyecto resuelve principalmente en la viabilidad de la producción de café en **Japón**, ya que actualmente es uno de los principales países **asiáticos** que **más café importa**. De acuerdo con el [OEC](https://oec.world/en), en **2023** Japón importó alrededor de **1,500 millones** de dólares en café; esto lo convierte en el país asiático que más café importa en el mundo.

Para el origen del dato tenemos datos confiables como:

- [NASA Power API](https://power.larc.nasa.gov/docs/): Para obtener datos meteorológicos desde la **api pública**.
- [SoilGrids](https://soilgrids.org/): Para obtener datos del suelo en formato `tiff`.
- [NASA Geocoded Disasters Dataset](https://www.earthdata.nasa.gov/data/catalog/sedac-ciesin-sedac-pend-gdis-1.00): Para obtener datos históricos sobre desastres naturales en formato `csv`. 

## Arquitectura

La arquitectura sigue un enfoque **ELT** en donde **Airflow** orquesta la ingestión y ejecución de tareas, **S3** actúa como un data lake, **Glue** gestiona las transformaciones y **Athena** actúa para transformar los datos en datos para análisis en **Power BI**.

![](./images/coffe-elt-diagram.png)

## ¿Por que Airflow-AWS-PowerBI?

- **Airflow:** se utiliza para facilitar la ejecución de tareas en secuencia. Permite programar procesos periódicos y orquestar la subida de archivos a S3 una vez que finaliza la extracción. Para este proyecto en particular, fue de gran ayuda para extraer datos de la API, convertirlos a formato Parquet y cargarlos en el *data lake*.
- **AWS:** se utiliza para procesar los datos una vez almacenados en **S3**, servicio ampliamente utilizado para la creación de *data lakes*. El ecosistema incluye **AWS Glue**, un servicio *serverless* que facilita la limpieza y preparación de datos. **Athena**, por su parte, es un servicio de consultas interactivo que permite analizar la información directamente desde S3. Para funcionar, Athena utiliza el **Data Catalog** de AWS Glue para identificar los esquemas de los datos. Mediante el uso de **Crawlers**, se actualiza dicho catálogo automáticamente, permitiendo que Athena visualice y consulte los datos almacenados en el *data lake*.
- **Power BI:** se utiliza en la etapa final para la creación de *dashboards*, permitiendo el análisis y la representación gráfica de los datos almacenados en S3. Su capacidad de visualización facilita la toma de decisiones basada en los datos procesados.

## Flujo de datos

Dentro del flujo de datos, manejamos tres formatos distintos: **JSON, CSV y TIFF**. Para la orquestación, utilizamos **Airflow** dentro de un contenedor de **Docker**; esto permite replicar el entorno en cualquier máquina, garantizando la escalabilidad del proyecto.

- **JSON:** se extraen datos climáticos públicos de la API de la NASA mediante Airflow. El alcance del estudio comprende el periodo 2014-2024 para las ciudades de Okinawa, Naha, Nago y Kunigami. Los datos se transforman a formato **Parquet** y se cargan en **S3**.
- **CSV:** se procesa un conjunto de datos global de la NASA sobre desastres naturales. Airflow automatiza la conversión de estos archivos a Parquet y su posterior carga en S3.
- **TIFF:** al ser un formato rasterizado, se emplea la librería **rasterio** para extraer la información almacenada por píxel. Una vez extraídos los datos a un formato tabular, se convierten a Parquet y se almacenan en S3.

Una vez que la data reside en el bucket `s3://raw-zone/`, se configura un **AWS Glue Crawler** para actualizar el **Data Catalog**, permitiendo que **Athena** identifique los esquemas. Mediante consultas SQL en Athena, se realizan las transformaciones y limpieza necesarias, depositando los resultados en el bucket `s3://clean-zone/`. Finalmente, **Power BI** se conecta a esta zona para la visualización y representación de los datos.
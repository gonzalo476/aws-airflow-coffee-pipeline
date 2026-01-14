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

## Modelado de datos

Dentro del *data lake*, existen dos *buckets*, **raw** y **clean**, para almacenar y segmentar los diferentes estados de la información. Esta estructura es fundamental para garantizar la organización y escalabilidad a futuro.

- **raw-zone:** recibe exclusivamente los datos crudos obtenidos desde la API o archivos CSV descargados de la web. Los datos se mantienen intactos, tal como llegaron, ya que constituyen nuestra 'fuente de verdad'.
- **clean-zone:** contiene los datos limpios y procesados, listos para que Power BI realice el análisis. En esta zona, los datos se transforman para ser más manejables: se eliminan valores nulos, se corrigen errores de escritura y se optimizan las tablas. En este proyecto, se eliminaron columnas innecesarias y se particionó la información por año como en el caso de los datos de *weather* para mejorar el rendimiento de las consultas.

## Calidad del dato

No se debe confiar ciegamente en los datos; incluso proviniendo de fuentes confiables, pueden contener valores no deseados. Por ello, es crucial validar la información y su veracidad. Los datos se validan mediante SQL, como en el siguiente ejemplo:

```sql
CASE
    WHEN TRY_CAST(latitude AS DOUBLE) BETWEEN -90 AND 90 THEN CAST(latitude AS DOUBLE)
    ELSE NULL
END AS latitude
```

Aun sabiendo que el campo corresponde a una latitud y contiene coordenadas, debemos validar esta información por seguridad e integridad de los datos.

## Costos y optimización

AWS factura por datos escaneados y capacidad de cómputo, entre otros conceptos. Una estrategia para reducir costos en este proyecto fue ejecutar **Airflow dentro de un contenedor de Docker**, aprovechando los recursos locales en lugar de costear un clúster en la nube, el cual resulta más caro y complejo de mantener. El uso de contenedores permite, además, reproducir el entorno en cualquier máquina fácilmente. Por otro lado, dado que **Athena** cobra por el volumen de datos escaneados, la mejor solución fue implementar el formato **Parquet**. Esto redujo drásticamente el tamaño de los archivos (por ejemplo, un CSV de 30 MB se redujo a solo 128 kb), logrando que las consultas sean mucho más rápidas y económicas.

## Trade-offs

- **Airflow batch vs Streaming:** El actual pipeline esta orquestado por Airflow. Airflow no trabaja en tiempo real; si se necesitara obtener datos de un sensor ya sea de algún lugar para capturar data en minutos o segundos este ya dejaría de ser una buena decisión. El pipeline está diseñado para batch analytics. Para requisitos como baja latencia seria necesario optar por otro enfoque de streaming como Kinesis, aumentando la complejidad y costos.
- **Glue + Athena vs Base de datos relacional:** Usar Glue y Athena permiten una arquitectura bajo demanda. a medida que la demanda crece tiene escalabilidad automática. Esta deja de ser buena opción cuando se requieren datos con baja latencia y cuando el patrón de consultas es repetitivo siempre en la misma data. Dado que el enfoque es analítico y no transaccional ni el dato depende o se relaciona con mas datos.
- **Crawlers automaticos vs esquemas controlados**: Los crawlers aceleran la iteración inicial de definir esquemas para la base de datos. Ya que detectan automáticamente el tipo de valor y crean un esquema para los datos. Por otro lado se tiene menor control de esquemas y mas probabilidad a errores en la representación de cada dato en la etapa inicial en un entorno de producción.
- **Raw + Clean en S3 vs transformación directa:** Separar el *data lake* en zonas Raw y Clean nos permiten trazabilidad cuando el volumen es pequeño y altamente confiable. Se prioriza reproducibilidad sobre simplicidad.

## Consumidores del dato

Hoy en día este tipo de pipeline tiene gran valor ya que agrega bastante valor para los consumidores del dato, ya que permitirá realizar mediciones, predicciones o dashboards. Entre ellos están: 

- Analistas de Negocio.
- Analistas de datos.
- Científicos de datos (ML/AI).

## Limitaciones y mejoras futuras

| Limitación                                                   | Impacto                                                      | Mejoras                                                      |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Pipeline diseñado para procesamiento en batch y no soporta casos de uso como streaming | Los datos están disponibles con latencia de horas, lo cual si es eficiente para análisis histórico. | Para requerimientos como leer datos de un sensor en tiempo real con baja latencia, el pipeline podría evolucionar a servicios de streaming como Kinesis. |
| La extracción se basa principalmente en el estado de ejecución de Airflow, el momento en donde el dato se obtiene. | No se detectan degradaciones en la calidad del dato, por ejemplo valores nulos. | Incorporar reglas de data quality mas avanzadas y métricas históricas permitirá detectar anomalías de forma temprana. |

## Takeaways técnicos

- Diseñar el pipeline ELT sobre un data lake facilita su escalabilidad.
- Una correcta partición de datos *weather* por año para un impacto de performance y costos.
- Todas las decisiones tomadas a corto plazo son necesariametne optimas a largo plazo sin controles adicionales.
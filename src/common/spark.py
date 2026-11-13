"""Construcción de la SparkSession compartida por todos los jobs."""

import logging

from pyspark.sql import SparkSession


def get_spark(app_name: str, master: str = "local[*]") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        # Volumen chico: 8 particiones de shuffle alcanzan y evitan 200 tareas vacías.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        # Overwrite dinámico: reescribe solo las particiones que la corrida toca
        # y deja intactas las de otros días (D-011).
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Conservar el progreso de todos los micro-lotes (120 archivos) para la
        # evidencia; el default de 100 trunca el resumen de la corrida.
        .config("spark.sql.streaming.numRecentProgressUpdates", "1000")
        # Sin barras de progreso: los logs de cada job ya informan conteos y tiempos.
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    # foreachBatch levanta el callback server de Py4J, que loguea cada mensaje en INFO.
    logging.getLogger("py4j").setLevel(logging.WARNING)
    return spark

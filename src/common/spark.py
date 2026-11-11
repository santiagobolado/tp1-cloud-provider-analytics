"""Construcción de la SparkSession compartida por todos los jobs."""

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
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

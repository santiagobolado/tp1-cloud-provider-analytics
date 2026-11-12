"""Gold -> DataFrames con la forma exacta de cada tabla de serving (D-017).

Funciones puras: se prueban sin Cassandra.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

WINDOW_DAYS = 14

P1_COLS = ["org_id", "usage_date", "service", "org_name", "plan_tier", "daily_cost_usd",
           "daily_cost_gross_usd", "credit_adjustment_usd", "requests", "cpu_hours",
           "storage_gb_hours", "genai_tokens", "carbon_kg", "event_count", "source",
           "last_reconciled_at", "loaded_at"]

P2_COLS = ["org_id", "as_of_date", "rank", "service", "cost_14d_usd", "requests_14d",
           "days_with_usage", "window_start", "loaded_at"]


def usage_table(mart: DataFrame) -> DataFrame:
    """P1: una fila por (org_id, usage_date, service), igual que el mart."""
    if "last_reconciled_at" not in mart.columns:                 # capa rápida
        mart = mart.withColumn("last_reconciled_at", F.lit(None).cast("timestamp"))
    return (mart.withColumn("genai_tokens", F.col("genai_tokens").cast("long"))
                .withColumn("event_count", F.col("event_count").cast("long"))
                .withColumn("loaded_at", F.current_timestamp())
                .select(*P1_COLS))


def top_services_table(mart: DataFrame) -> DataFrame:
    """P2: Top de servicios por costo en los 14 días que terminan en cada as_of_date.

    "Últimos 14 días" se ancla a cada fecha del dataset y no a hoy: el dataset
    termina el 2025-08-31. Se calculan todas las fechas de corte con ventana
    completa, así la consulta funciona para cualquier día y no solo el último.
    """
    bounds = mart.agg(F.min("usage_date").alias("lo"), F.max("usage_date").alias("hi")).first()
    as_of = (mart.select(F.col("usage_date").alias("as_of_date")).distinct()
             .filter(F.col("as_of_date") >= F.date_add(F.lit(bounds["lo"]), WINDOW_DAYS - 1)))
    window_start = F.date_sub(F.col("as_of_date"), WINDOW_DAYS - 1)
    agg = (mart.join(F.broadcast(as_of),
                     (F.col("usage_date") <= F.col("as_of_date"))
                     & (F.col("usage_date") >= window_start))
           .groupBy("org_id", "as_of_date", "service")
           .agg(F.sum("daily_cost_usd").alias("cost_14d_usd"),
                F.sum("requests").alias("requests_14d"),
                F.count("*").cast("int").alias("days_with_usage")))
    rank = F.row_number().over(Window.partitionBy("org_id", "as_of_date")
                               .orderBy(F.desc("cost_14d_usd"), F.asc("service")))
    return (agg.withColumn("rank", rank.cast("int"))
               .withColumn("cost_14d_usd", F.round("cost_14d_usd", 4))
               .withColumn("window_start", window_start)
               .withColumn("loaded_at", F.current_timestamp())
               .select(*P2_COLS))

"""Mart FinOps org_daily_usage_by_service (consigna §6.2 y §7.3).

Grano: una fila por (org_id, usage_date, service).

DAILY_USAGE_AGGS es la única definición de las métricas del mart. La usan la
ruta streaming (flujo C) y la reconciliación batch (flujo D), que es como se
mitiga la lógica duplicada del patrón híbrido (§6.7, riesgo G-04): las dos
rutas no pueden divergir porque calculan con la misma lista de expresiones.

Todas las agregaciones son sumas o conteos: asociativas, por eso Spark puede
agregar parcialmente antes del shuffle (el combiner de la §10.3) y por eso el
mart no guarda promedios, que se derivan como suma / conteo.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

MART = "org_daily_usage_by_service"
KEY = ["org_id", "usage_date", "service"]


def daily_usage_aggs() -> list:
    c = F.col
    return [
        F.sum("cost_usd").alias("daily_cost_usd"),                      # neto, con créditos
        F.sum(F.when(c("cost_usd") > 0, c("cost_usd")).otherwise(0.0)).alias("daily_cost_gross_usd"),
        F.sum(F.when(c("is_credit_adjustment"), c("cost_usd")).otherwise(0.0)).alias("credit_adjustment_usd"),
        F.sum("requests").alias("requests"),
        F.sum("cpu_hours").alias("cpu_hours"),
        F.sum("storage_gb_hours").alias("storage_gb_hours"),
        F.sum("genai_tokens").alias("genai_tokens"),                    # nulo si no hubo GenAI v2
        F.sum("carbon_kg").alias("carbon_kg"),                          # nulo si el día es solo v1
        F.count(F.lit(1)).alias("event_count"),
        F.sum(F.when(c("value_num").isNull(), 1).otherwise(0)).alias("events_metric_null"),
        F.first("org_name", ignorenulls=True).alias("org_name"),
        F.first("plan_tier", ignorenulls=True).alias("plan_tier"),
    ]


def aggregate_daily_usage(silver: DataFrame) -> DataFrame:
    """Ruta batch: Silver completo -> mart."""
    return (silver.withColumnRenamed("event_date", "usage_date")
                  .groupBy(*KEY).agg(*daily_usage_aggs()))


def round_money(df: DataFrame) -> DataFrame:
    cols = ["daily_cost_usd", "daily_cost_gross_usd", "credit_adjustment_usd"]
    return df.select(*[F.round(F.col(x), 4).alias(x) if x in cols else F.col(x) for x in df.columns])


def usage_date_from_window(window_col: str = "window") -> Column:
    return F.to_date(F.col(f"{window_col}.start"))

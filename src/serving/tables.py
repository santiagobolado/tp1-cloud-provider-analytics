"""Gold -> DataFrames con la forma exacta de cada tabla de serving (D-017, D-024).

Funciones puras: se prueban sin Cassandra.

Las tablas de Q3, Q4 y Q5 agregan una partición de total con la clave `'all'`
(severidad o organización). Cassandra no agrega entre particiones: el total
se precalcula en Spark, sumando conteos y montos (no promediando tasas).
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

WINDOW_DAYS = 14
ALL = "all"          # clave de la partición de total (D-018, D-024)

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


# ---------------------------------------------------------------------------
# Q3, Q4 y Q5 (D-018, D-024)
# ---------------------------------------------------------------------------

P3_COLS = ["severity", "ticket_date", "ticket_count", "open_count", "sla_breached_count",
           "sla_breach_rate", "csat_responses", "csat_avg", "org_count", "loaded_at"]

P4_COLS = ["org_id", "month", "org_name", "plan_tier", "currency", "fx_applied", "subtotal_usd",
           "credits_usd", "taxes_usd", "net_revenue_usd", "total_billed_usd", "invoice_count",
           "has_credit_note", "org_count", "loaded_at"]

P5_COLS = ["org_id", "usage_date", "org_name", "plan_tier", "genai_tokens", "genai_cost_usd",
           "cost_per_1k_tokens_usd", "event_count", "org_count", "loaded_at"]


def _rate(num: str, den: str, digits: int) -> F.Column:
    return F.round(F.when(F.col(den) > 0, F.col(num) / F.col(den)), digits)


def tickets_severity_table(mart: DataFrame) -> DataFrame:
    """Q3: una fila por (severidad, día) y por ('all', día), con días sin tickets en 0.

    La serie se completa con ceros para todo el rango del dataset: "evolución
    por día" incluye los días sin tickets críticos (son 47 de 115 con alguno).
    """
    sums = [F.sum(c).alias(c) for c in
            ("ticket_count", "open_count", "sla_breached_count", "csat_responses", "csat_sum")]
    by_sev = mart.groupBy("severity", "ticket_date").agg(
        *sums, F.countDistinct("org_id").alias("org_count"))
    total = (mart.groupBy("ticket_date")
             .agg(*sums, F.countDistinct("org_id").alias("org_count"))
             .withColumn("severity", F.lit(ALL)))
    lo, hi = mart.agg(F.min("ticket_date"), F.max("ticket_date")).first()
    days = mart.sparkSession.sql(
        f"SELECT explode(sequence(DATE'{lo}', DATE'{hi}')) AS ticket_date")
    sevs = mart.select("severity").distinct().union(mart.sparkSession.createDataFrame([(ALL,)], "severity string"))
    counts = ["ticket_count", "open_count", "sla_breached_count", "csat_responses", "org_count"]
    return (sevs.crossJoin(days)
            .join(by_sev.unionByName(total), ["severity", "ticket_date"], "left")
            .fillna(0, subset=counts)
            .withColumn("sla_breach_rate", _rate("sla_breached_count", "ticket_count", 4))
            .withColumn("csat_avg", _rate("csat_sum", "csat_responses", 2))
            .withColumn("loaded_at", F.current_timestamp())
            .select(*[F.col(c).cast("int") if c in counts else F.col(c) for c in P3_COLS]))


def revenue_table(mart: DataFrame) -> DataFrame:
    """Q4: revenue por (org, mes) más el total de la plataforma (org_id = 'all')."""
    money = ["subtotal_usd", "credits_usd", "taxes_usd", "net_revenue_usd", "total_billed_usd"]
    orgs = mart.withColumn("org_count", F.lit(1))
    total = (mart.groupBy("month")
             .agg(*[F.round(F.sum(m), 2).alias(m) for m in money],
                  F.sum("invoice_count").alias("invoice_count"),
                  F.max(F.col("has_credit_note").cast("int")).cast("boolean").alias("has_credit_note"),
                  F.array_join(F.array_sort(F.array_distinct(F.flatten(
                      F.collect_list(F.split("currency", ","))))), ",").alias("currency"),
                  F.countDistinct("org_id").alias("org_count"))
             .withColumn("org_id", F.lit(ALL))
             .withColumn("org_name", F.lit("Todas las organizaciones"))
             .withColumn("plan_tier", F.lit(None).cast("string"))
             .withColumn("fx_applied", F.lit(None).cast("double")))  # mezcla de monedas
    return (orgs.unionByName(total, allowMissingColumns=True)
            .withColumn("invoice_count", F.col("invoice_count").cast("int"))
            .withColumn("org_count", F.col("org_count").cast("int"))
            .withColumn("loaded_at", F.current_timestamp())
            .select(*P4_COLS))


def genai_table(mart: DataFrame) -> DataFrame:
    """Q5: tokens y costo por (org, día) más el total diario (org_id = 'all')."""
    orgs = mart.withColumn("org_count", F.lit(1))
    total = (mart.groupBy("usage_date")
             .agg(F.sum("genai_tokens").alias("genai_tokens"),
                  F.round(F.sum("genai_cost_usd"), 4).alias("genai_cost_usd"),
                  F.sum("event_count").alias("event_count"),
                  F.countDistinct("org_id").alias("org_count"))
             .withColumn("cost_per_1k_tokens_usd",
                         F.round(F.col("genai_cost_usd") * 1000 / F.col("genai_tokens"), 6))
             .withColumn("org_id", F.lit(ALL))
             .withColumn("org_name", F.lit("Todas las organizaciones"))
             .withColumn("plan_tier", F.lit(None).cast("string")))
    return (orgs.unionByName(total)
            .withColumn("genai_tokens", F.col("genai_tokens").cast("long"))
            .withColumn("event_count", F.col("event_count").cast("int"))
            .withColumn("org_count", F.col("org_count").cast("int"))
            .withColumn("loaded_at", F.current_timestamp())
            .select(*P5_COLS))

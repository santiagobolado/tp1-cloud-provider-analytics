"""Marts Gold de FinOps (revenue), Soporte y Producto/GenAI (consigna §7.3).

| Mart                       | Grano                          | Responde |
|----------------------------|--------------------------------|----------|
| revenue_by_org_month       | org_id, month                  | P4 / Q4  |
| tickets_by_org_date        | org_id, ticket_date, severity  | P3 / Q3  |
| genai_tokens_by_org_date   | org_id, usage_date             | P5 / Q5  |

Mismo criterio que org_daily_usage_by_service (gold.py): cada mart guarda las
sumas y conteos, que son aditivos, y además el indicador derivado que pide la
consigna (tasa de SLA breach, CSAT promedio, costo por 1.000 tokens). Los
conteos permiten re-agregar sin error: la tasa de SLA de "todas las
severidades" en Q3 se calcula sumando conteos, no promediando tasas.

Funciones puras (DataFrame -> DataFrame); el job y el control de conservación
están en gold_domains_job.py.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REVENUE_KEY = ["org_id", "month"]
TICKETS_KEY = ["org_id", "ticket_date", "severity"]
GENAI_KEY = ["org_id", "usage_date"]


def _ratio(num: str, den: str, digits: int = 4) -> F.Column:
    return F.round(F.when(F.col(den) > 0, F.col(num) / F.col(den)), digits)


# ---------------------------------------------------------------------------
# FinOps · revenue_by_org_month (D-019)
# ---------------------------------------------------------------------------

def revenue_by_org_month(billing: DataFrame) -> DataFrame:
    """Silver de billing -> revenue mensual en USD por organización.

    Hoy hay una factura por (org, mes), pero el mart no lo asume: suma. Si un
    mes mezclara monedas, `currency` las lista y `fx_applied` queda nulo,
    porque una sola tasa no describiría la conversión.
    """
    money = ["credits_usd", "taxes_usd", "net_revenue_usd", "total_billed_usd"]
    return (billing
            .withColumn("subtotal_usd", F.col("subtotal") * F.col("fx_applied"))
            .groupBy(*REVENUE_KEY)
            .agg(F.first("org_name", ignorenulls=True).alias("org_name"),
                 F.first("plan_tier", ignorenulls=True).alias("plan_tier"),
                 F.array_join(F.array_sort(F.collect_set("currency")), ",").alias("currency"),
                 F.when(F.countDistinct("fx_applied") == 1, F.first("fx_applied"))
                  .alias("fx_applied"),
                 F.round(F.sum("subtotal_usd"), 2).alias("subtotal_usd"),
                 *[F.round(F.sum(m), 2).alias(m) for m in money],
                 F.count(F.lit(1)).alias("invoice_count"),
                 F.max(F.col("is_credit_note").cast("int")).cast("boolean").alias("has_credit_note")))


# ---------------------------------------------------------------------------
# Soporte · tickets_by_org_date
# ---------------------------------------------------------------------------

def tickets_by_org_date(tickets: DataFrame) -> DataFrame:
    """Silver de tickets -> conteos, SLA y CSAT por (org, día de apertura, severidad).

    El CSAT promedio usa solo respuestas válidas (1-5, D-022): divide por
    `csat_responses`, no por la cantidad de tickets.
    """
    return (tickets.groupBy(*TICKETS_KEY)
            .agg(F.first("org_name", ignorenulls=True).alias("org_name"),
                 F.first("plan_tier", ignorenulls=True).alias("plan_tier"),
                 F.count(F.lit(1)).alias("ticket_count"),
                 F.sum(F.col("is_open").cast("int")).alias("open_count"),
                 F.sum(F.col("sla_breached").cast("int")).alias("sla_breached_count"),
                 F.count("csat").alias("csat_responses"),
                 F.sum("csat").alias("csat_sum"))
            .withColumn("sla_breach_rate", _ratio("sla_breached_count", "ticket_count"))
            .withColumn("csat_avg", _ratio("csat_sum", "csat_responses", 2)))


# ---------------------------------------------------------------------------
# Producto · genai_tokens_by_org_date (D-020)
# ---------------------------------------------------------------------------

def genai_events(events: DataFrame) -> DataFrame:
    """Eventos con tokens: v2 y service = genai (D-002). Los v1 no los medían."""
    return events.filter((F.col("service") == "genai") & (F.col("schema_version") == 2))


def genai_tokens_by_org_date(events: DataFrame) -> DataFrame:
    """Silver de eventos -> tokens y costo estimado por (org, día).

    El costo estimado es la suma de `cost_usd` de los mismos eventos que
    aportan los tokens. El costo GenAI de los eventos v1 (sin tokens) sigue en
    org_daily_usage_by_service: acá se reportaría un costo sin su consumo.
    """
    return (genai_events(events).withColumnRenamed("event_date", "usage_date")
            .groupBy(*GENAI_KEY)
            .agg(F.first("org_name", ignorenulls=True).alias("org_name"),
                 F.first("plan_tier", ignorenulls=True).alias("plan_tier"),
                 F.sum("genai_tokens").alias("genai_tokens"),
                 F.round(F.sum("cost_usd"), 4).alias("genai_cost_usd"),
                 F.count(F.lit(1)).alias("event_count"))
            .withColumn("cost_per_1k_tokens_usd",
                        F.round(F.when(F.col("genai_tokens") > 0,
                                       F.col("genai_cost_usd") * 1000 / F.col("genai_tokens")), 6)))


MARTS = {
    "revenue_by_org_month": (revenue_by_org_month, "billing_monthly", REVENUE_KEY),
    "tickets_by_org_date": (tickets_by_org_date, "support_tickets", TICKETS_KEY),
    "genai_tokens_by_org_date": (genai_tokens_by_org_date, "usage_events", GENAI_KEY),
}

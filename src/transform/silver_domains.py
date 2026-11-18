"""Bronze -> Silver de los maestros de dominio: facturación, tickets, usuarios y marketing.

Mismo criterio que la Silver de eventos (silver.py): funciones puras, ninguna
fila sale de Silver por una regla de calidad, los defectos se marcan en
`quality_flags` y se copian a quarantine con su motivo (D-009, D-022).

Reglas de la frontera Bronze -> Silver de estos maestros (D-022):

  billing_monthly
    fx_usd_ruidoso                tasa != 1 en factura USD; se fuerza 1,0 (D-007)       flag
    nota_de_credito               subtotal < 0 (13 facturas)                            flag
    impuesto_signo_inconsistente  taxes con signo opuesto al subtotal                   flag
  support_tickets
    csat_fuera_de_escala          csat fuera de 1..5; el csat de consumo queda nulo     quarantine
    resolved_antes_de_created     invariante temporal (0 casos; control preventivo)     quarantine
  users
    last_login_antes_de_created   invariante temporal del TP1 §9.7 (232 casos)         quarantine
  marketing_touches
    converted_sin_click           invariante del embudo del TP1 §9.7 (96 casos)         quarantine
  todos
    org_sin_dimension             org_id sin match en customers_orgs (left join)        quarantine

Nulos semánticos (no son defectos, no van a quarantine): `resolved_at` nulo =
ticket abierto, `csat` nulo = encuesta no respondida, `last_login` nulo =
nunca ingresó, `credits` nulo = sin crédito (D-007).

`users.email` no se promueve a Silver: es un dato personal que ningún mart
necesita (minimización, docs/gobierno.md §5).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CSAT_MIN, CSAT_MAX = 1.0, 5.0          # escala de negocio; lo observado es 0..6 (D-022)

# Qué flags son defectos del dato (copia en quarantine). El resto son flags de
# negocio o de corrección ya resuelta, como costo_negativo en eventos.
QUARANTINE_RULES = {
    "billing_monthly": ["org_sin_dimension"],
    "support_tickets": ["csat_fuera_de_escala", "resolved_antes_de_created", "org_sin_dimension"],
    "users": ["last_login_antes_de_created", "org_sin_dimension"],
    "marketing_touches": ["converted_sin_click", "org_sin_dimension"],
}
RECORD_KEY = {"billing_monthly": "invoice_id", "support_tickets": "ticket_id",
              "users": "user_id", "marketing_touches": "touch_id"}


def _norm(col: str) -> F.Column:
    return F.lower(F.trim(F.col(col)))


def _with_org(df: DataFrame, orgs: DataFrame) -> DataFrame:
    """Left join contra la dimensión: una org desconocida no hace perder la fila."""
    dim = orgs.select("org_id", "org_name", "plan_tier", F.lit(True).alias("_has_org"))
    return df.join(F.broadcast(dim), "org_id", "left")


def _flags(*conds) -> F.Column:
    """Array de los nombres de regla cuya condición es verdadera."""
    arr = F.array(*[F.when(c, F.lit(name)) for name, c in conds])
    return F.filter(arr, lambda x: x.isNotNull())


def _org_missing():
    return ("org_sin_dimension", F.col("_has_org").isNull())


def _lineage():
    return ["source_file", "ingest_ts", F.current_timestamp().alias("silver_ts")]


# ---------------------------------------------------------------------------
# Facturación (FinOps · P4)
# ---------------------------------------------------------------------------

def conform_billing(bronze: DataFrame, orgs: DataFrame) -> DataFrame:
    """Factura con montos normalizados a USD (D-007, D-019)."""
    usd = F.col("currency") == "USD"
    fx = F.when(usd, F.lit(1.0)).otherwise(F.col("exchange_rate_to_usd"))
    credits = F.coalesce(F.col("credits"), F.lit(0.0))
    sign_mismatch = (F.signum("subtotal") * F.signum("taxes")) < 0

    df = (_with_org(bronze.withColumn("currency", F.upper(F.trim("currency"))), orgs)
          .withColumn("credits", credits)
          .withColumn("fx_applied", fx))
    return df.select(
        "invoice_id", "org_id", "org_name", "plan_tier", "month", "currency",
        "subtotal", "credits", "taxes", "exchange_rate_to_usd", "fx_applied",
        (F.col("credits") * F.col("fx_applied")).alias("credits_usd"),
        (F.col("taxes") * F.col("fx_applied")).alias("taxes_usd"),
        ((F.col("subtotal") - F.col("credits")) * F.col("fx_applied")).alias("net_revenue_usd"),
        ((F.col("subtotal") - F.col("credits") + F.col("taxes")) * F.col("fx_applied"))
        .alias("total_billed_usd"),
        (F.col("subtotal") < 0).alias("is_credit_note"),
        _flags(("fx_usd_ruidoso", usd & (F.col("exchange_rate_to_usd") != 1.0)),
               ("nota_de_credito", F.col("subtotal") < 0),
               ("impuesto_signo_inconsistente", sign_mismatch),
               _org_missing()).alias("quality_flags"),
        *_lineage())


# ---------------------------------------------------------------------------
# Tickets (Soporte · P3)
# ---------------------------------------------------------------------------

def conform_tickets(bronze: DataFrame, orgs: DataFrame) -> DataFrame:
    csat_ok = F.col("csat").between(CSAT_MIN, CSAT_MAX)
    df = _with_org(bronze, orgs)
    return df.select(
        "ticket_id", "org_id", "org_name", "plan_tier",
        _norm("category").alias("category"), _norm("severity").alias("severity"),
        F.col("created_at").alias("ticket_date"), "resolved_at",
        F.col("resolved_at").isNull().alias("is_open"),
        F.datediff("resolved_at", "created_at").alias("resolution_days"),
        # D-009 aplicado al CSAT: se anula el campo derivado, no el ticket.
        F.when(csat_ok, F.col("csat")).alias("csat"),
        F.col("csat").alias("csat_raw"),
        "sla_breached",
        _flags(("csat_fuera_de_escala", F.col("csat").isNotNull() & ~csat_ok),
               ("resolved_antes_de_created", F.col("resolved_at") < F.col("created_at")),
               _org_missing()).alias("quality_flags"),
        *_lineage())


# ---------------------------------------------------------------------------
# Usuarios y marketing (invariantes entre columnas del TP1, §9.7 · O-5)
# ---------------------------------------------------------------------------

def conform_users(bronze: DataFrame, orgs: DataFrame) -> DataFrame:
    df = _with_org(bronze, orgs)
    return df.select(
        "user_id", "org_id", _norm("role").alias("role"), "active", "created_at", "last_login",
        F.col("last_login").isNotNull().alias("has_logged_in"),
        _flags(("last_login_antes_de_created", F.col("last_login") < F.col("created_at")),
               _org_missing()).alias("quality_flags"),
        *_lineage())


def conform_marketing(bronze: DataFrame, orgs: DataFrame) -> DataFrame:
    df = _with_org(bronze, orgs)
    return df.select(
        "touch_id", "org_id", _norm("campaign").alias("campaign"), _norm("channel").alias("channel"),
        F.col("timestamp").alias("touch_date"), "clicked", "converted",
        _flags(("converted_sin_click", F.col("converted") & ~F.col("clicked")),
               _org_missing()).alias("quality_flags"),
        *_lineage())


CONFORMERS = {"billing_monthly": conform_billing, "support_tickets": conform_tickets,
              "users": conform_users, "marketing_touches": conform_marketing}


def quarantine_copies(entity: str, silver: DataFrame) -> DataFrame:
    """Una fila por regla de defecto violada; el registro sigue en Silver."""
    key = RECORD_KEY[entity]
    return (silver.withColumn("regla", F.explode("quality_flags"))
                  .filter(F.col("regla").isin(QUARANTINE_RULES[entity]))
                  .select(F.lit(entity).alias("entity"), "regla",
                          F.col(key).alias("record_id"),
                          F.to_json(F.struct(*[c for c in silver.columns if c != "quality_flags"]))
                          .alias("record_json"),
                          F.lit("promovido_con_flag").alias("destino_en_silver"),
                          "source_file", "ingest_ts"))

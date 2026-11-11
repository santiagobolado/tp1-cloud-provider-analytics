"""Esquemas explícitos y claves naturales de las fuentes batch de Landing.

Nunca se usa inferSchema fuera del perfilado exploratorio: el tipo de cada
columna es una decisión, no una adivinanza de Spark.

Cada esquema incluye `_corrupt_record`: cuando una fila no respeta el esquema
(p. ej. una fecha mal escrita), Spark guarda ahí la línea cruda en lugar de
convertir el campo a nulo en silencio. Esas filas van a quarantine.
"""

from dataclasses import dataclass

from pyspark.sql import types as T

CORRUPT_COL = "_corrupt_record"


def _schema(*fields: T.StructField) -> T.StructType:
    return T.StructType(list(fields) + [T.StructField(CORRUPT_COL, T.StringType())])


S, D, B, DT = T.StringType(), T.DoubleType(), T.BooleanType(), T.DateType()


def f(name, dtype):
    return T.StructField(name, dtype, nullable=True)


@dataclass(frozen=True)
class BatchSource:
    entity: str               # nombre de la tabla en Bronze
    file: str                 # archivo en Landing
    schema: T.StructType
    natural_key: tuple        # clave de deduplicación verificada contra los datos


SOURCES = [
    BatchSource("customers_orgs", "customers_orgs.csv", _schema(
        f("org_id", S), f("org_name", S), f("industry", S), f("hq_region", S),
        f("plan_tier", S), f("is_enterprise", B), f("signup_date", DT),
        f("sales_rep", S), f("lifecycle_stage", S), f("marketing_source", S),
        f("nps_score", D),
    ), ("org_id",)),
    BatchSource("users", "users.csv", _schema(
        f("user_id", S), f("org_id", S), f("email", S), f("role", S),
        f("active", B), f("created_at", DT), f("last_login", DT),
    ), ("user_id",)),
    BatchSource("resources", "resources.csv", _schema(
        f("resource_id", S), f("org_id", S), f("service", S), f("region", S),
        f("created_at", DT), f("state", S),
        f("tags_json", S),    # JSON embebido; se parsea a array en Silver
    ), ("resource_id",)),
    BatchSource("support_tickets", "support_tickets.csv", _schema(
        f("ticket_id", S), f("org_id", S), f("category", S), f("severity", S),
        f("created_at", DT), f("resolved_at", DT), f("csat", D),
        f("sla_breached", B),
    ), ("ticket_id",)),
    BatchSource("marketing_touches", "marketing_touches.csv", _schema(
        f("touch_id", S), f("org_id", S), f("campaign", S), f("channel", S),
        f("timestamp", DT), f("clicked", B), f("converted", B),
    ), ("touch_id",)),
    BatchSource("nps_surveys", "nps_surveys.csv", _schema(
        f("org_id", S), f("survey_date", DT), f("nps_score", D), f("comment", S),
    ), ("org_id", "survey_date")),   # no tiene id propio: una encuesta por org y fecha
    BatchSource("billing_monthly", "billing_monthly.csv", _schema(
        f("invoice_id", S), f("org_id", S), f("month", DT), f("subtotal", D),
        f("credits", D), f("taxes", D), f("currency", S),
        f("exchange_rate_to_usd", D),
    ), ("invoice_id",)),
]

SOURCES_BY_ENTITY = {s.entity: s for s in SOURCES}

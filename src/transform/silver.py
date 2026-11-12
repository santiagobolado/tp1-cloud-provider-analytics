"""Bronze -> Silver: conformación de eventos y maestros.

Silver es la capa donde las dos rutas convergen (§5.3): los eventos se tipan,
se compatibilizan v1/v2, se enriquecen contra las dimensiones y se proyectan
a features. Las funciones `conform_*` son puras (DataFrame -> DataFrame) para
que la ruta streaming (foreachBatch) y la reconciliación batch usen el mismo
código (mitigación de lógica duplicada, §6.7).

Reglas de calidad de la frontera Bronze -> Silver (§9.7, D-009, D-014). Ningún
evento sale de Silver por una regla de calidad: lo que se marca, se marca en
`quality_flags`; lo que no se puede calcular queda en nulo; y una copia va a
quarantine con su motivo para auditoría.

  value_nulo            la métrica no se puede calcular; el costo sí promueve (D-009)
  value_no_numerico     `value` no castea a double; ídem
  unit_nulo_con_value   unidad faltante; se imputa la canónica de la métrica (D-014)
  costo_negativo        cost_usd_increment < -0.01 -> is_credit_adjustment (D-010)
  genai_tokens_inconsistente  tokens presentes  <=>  v2 y service = genai (D-002)
  org_sin_dimension     org_id sin match en customers_orgs (left join, no se pierde)
  event_id_duplicado    re-entrega fuera de la ventana de dedupe de Bronze (D-013)
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Unidad canónica por métrica: verificada sobre los 41.125 eventos con unidad
# (relación 1 a 1, sin excepciones). Es lo que hace recuperable el `unit` nulo.
CANONICAL_UNIT = {"cpu_hours": "hours", "requests": "count", "storage_gb_hours": "gb_hours"}
METRICS = list(CANONICAL_UNIT)
CREDIT_THRESHOLD = -0.01        # consigna §6.2: cost_usd_increment >= -0.01


def _norm(col: str) -> F.Column:
    return F.lower(F.trim(F.col(col)))


def _canonical_unit(metric: F.Column) -> F.Column:
    expr = F.lit(None).cast("string")
    for m, u in CANONICAL_UNIT.items():
        expr = F.when(metric == m, F.lit(u)).otherwise(expr)
    return expr


def latest_snapshot(df: DataFrame) -> DataFrame:
    """Bronze guarda una foto por ingest_date (D-011); Silver usa la última."""
    last = df.agg(F.max("ingest_date")).first()[0]
    return df.filter(F.col("ingest_date") == last)


# ---------------------------------------------------------------------------
# Maestros
# ---------------------------------------------------------------------------

def conform_orgs(orgs_bronze: DataFrame, nps_bronze: DataFrame) -> DataFrame:
    """Dimensión de organizaciones con el NPS resuelto según D-006."""
    last_survey = (nps_bronze.filter(F.col("nps_score").isNotNull())
                   .withColumn("_rn", F.row_number().over(
                       Window.partitionBy("org_id").orderBy(F.desc("survey_date"))))
                   .filter("_rn = 1")
                   .select("org_id", F.col("nps_score").alias("nps_score_last_survey"),
                           F.col("survey_date").alias("nps_survey_date")))
    nps_ok = F.col("nps_score_crm").between(-100, 100)
    return (orgs_bronze
            .select("org_id", F.trim("org_name").alias("org_name"), _norm("industry").alias("industry"),
                    _norm("hq_region").alias("hq_region"), _norm("plan_tier").alias("plan_tier"),
                    "is_enterprise", "signup_date", "sales_rep", _norm("lifecycle_stage").alias("lifecycle_stage"),
                    _norm("marketing_source").alias("marketing_source"),
                    F.col("nps_score").alias("nps_score_crm"), "ingest_ts")
            .join(last_survey, "org_id", "left")                  # 20 orgs sin encuesta: left
            # Regla de consumo de D-006: última encuesta, si no hay, CRM; se marca el origen.
            .withColumn("nps_score", F.coalesce("nps_score_last_survey",
                                                F.when(nps_ok, F.col("nps_score_crm"))))
            .withColumn("nps_source", F.when(F.col("nps_score_last_survey").isNotNull(), "survey")
                                       .when(nps_ok, "crm"))
            .withColumn("quality_flags", F.filter(F.array(
                F.when(F.col("nps_score_crm").isNotNull() & ~nps_ok, F.lit("nps_fuera_de_rango"))),
                lambda x: x.isNotNull())))


def conform_resources(res_bronze: DataFrame) -> DataFrame:
    return (res_bronze
            .select("resource_id", "org_id", _norm("service").alias("service"),
                    _norm("region").alias("region"), "created_at", _norm("state").alias("state"),
                    # JSON embebido en el CSV -> array real (§3.4, variedad anidada)
                    F.from_json("tags_json", "array<string>").alias("tags"), "ingest_ts"))


# ---------------------------------------------------------------------------
# Eventos
# ---------------------------------------------------------------------------

def conform_events(ev_bronze: DataFrame, orgs: DataFrame) -> DataFrame:
    """Eventos de Bronze -> Silver. Devuelve todas las filas con sus flags."""
    metric = _norm("metric")
    value_num = F.col("value").try_cast("double")             # D-003: fallback controlado
    unit_in = _norm("unit")
    has_value = F.col("value").isNotNull()

    ev = (ev_bronze
          .withColumn("service", _norm("service"))
          .withColumn("region", _norm("region"))
          .withColumn("metric", metric)
          .withColumn("value_num", value_num)
          .withColumn("unit_imputed", unit_in.isNull() & has_value)
          .withColumn("unit", F.coalesce(unit_in, F.when(has_value, _canonical_unit(metric))))
          .withColumn("cost_usd", F.col("cost_usd_increment"))
          .withColumn("event_date", F.to_date("event_ts"))
          # v1 -> esquema ampliado con nulos explícitos (O8). Nulo = "no medido",
          # distinto de 0: Gold no inventa carbono para eventos v1.
          .withColumn("carbon_kg", F.when(F.col("schema_version") >= 2, F.col("carbon_kg")))
          .withColumn("is_credit_adjustment", F.col("cost_usd") < CREDIT_THRESHOLD))

    genai_expected = (F.col("schema_version") == 2) & (F.col("service") == "genai")
    flags = F.array(
        F.when(~has_value, F.lit("value_nulo")),
        F.when(has_value & F.col("value_num").isNull(), F.lit("value_no_numerico")),
        F.when(F.col("unit_imputed"), F.lit("unit_nulo_con_value")),
        F.when(F.col("is_credit_adjustment"), F.lit("costo_negativo")),
        F.when(F.col("genai_tokens").isNotNull() != genai_expected,
               F.lit("genai_tokens_inconsistente")),
    )

    # Features (consigna §4.4): el formato largo metric/value se proyecta a
    # columnas en Silver para que Gold solo sume (es el map de la §10.2).
    for m in METRICS:
        ev = ev.withColumn(m, F.when(F.col("metric") == m, F.col("value_num")).otherwise(F.lit(0.0)))

    # Dedupe sobre la tabla completa: cubre re-entregas que Bronze no vio (D-013).
    w = Window.partitionBy("event_id").orderBy("ingest_ts", "source_file")
    ev = ev.withColumn("_rn", F.row_number().over(w))

    dim = orgs.select("org_id", "org_name", "industry", "plan_tier", "is_enterprise",
                      F.col("hq_region").alias("org_hq_region"), F.lit(True).alias("_has_org"))
    ev = ev.join(F.broadcast(dim), "org_id", "left")          # 80 filas: broadcast, sin shuffle

    flags = F.concat(flags, F.array(
        F.when(F.col("_has_org").isNull(), F.lit("org_sin_dimension")),
        F.when(F.col("_rn") > 1, F.lit("event_id_duplicado"))))

    return (ev.withColumn("quality_flags", F.filter(flags, lambda x: x.isNotNull()))
              .withColumn("is_duplicate", F.col("_rn") > 1)
              .withColumn("silver_ts", F.current_timestamp())
              .select("event_id", "event_ts", "event_date", "org_id", "org_name", "industry",
                      "plan_tier", "is_enterprise", "org_hq_region", "resource_id", "service",
                      "region", "metric", F.col("value").alias("value_raw"), "value_num", "unit",
                      "cost_usd", *METRICS, "genai_tokens", "carbon_kg", "schema_version",
                      "is_credit_adjustment", "quality_flags", "is_duplicate",
                      "source_file", "ingest_ts", "silver_ts"))


# Reglas que generan copia en quarantine. costo_negativo no: es un flag de
# negocio (posible nota de crédito), no un defecto del dato (§8.7).
QUARANTINE_RULES = ["value_nulo", "value_no_numerico", "unit_nulo_con_value",
                    "genai_tokens_inconsistente", "org_sin_dimension", "event_id_duplicado"]


def split_silver(conformed: DataFrame) -> tuple:
    """(filas para Silver, copias para quarantine con una fila por regla violada)."""
    silver = conformed.filter(~F.col("is_duplicate")).drop("is_duplicate")
    quarantine = (conformed
                  .withColumn("regla", F.explode("quality_flags"))
                  .filter(F.col("regla").isin(QUARANTINE_RULES))
                  .select(F.lit("usage_events").alias("entity"), "regla", "event_id",
                          F.to_json(F.struct(*[c for c in conformed.columns
                                               if c not in ("quality_flags", "is_duplicate")]))
                          .alias("record_json"),
                          F.when(F.col("regla") == "event_id_duplicado", "descartado")
                           .otherwise("promovido_con_flag").alias("destino_en_silver"),
                          "source_file", "ingest_ts"))
    return silver, quarantine

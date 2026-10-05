"""
TP1 - Cloud Provider Analytics
Perfilado exploratorio de las 8 fuentes de Landing.

Objetivo: generar la evidencia de lectura y exploracion de datos exigida por el
punto 12 del alcance de la primera entrega, y producir los insumos cuantitativos
de la seccion 4 (Inventario y perfil de fuentes) del documento de diseno.

Este script NO transforma ni escribe en el Data Lake: solo lee Landing en modo
read-only y emite metricas a evidence/. Landing es inmutable por definicion.

Ejecucion local:
    python notebooks/01_perfilado_fuentes.py
Ejecucion en Colab:
    !pip -q install pyspark
    %run notebooks/01_perfilado_fuentes.py
"""

import json
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---------------------------------------------------------------------------
# Configuracion
# ---------------------------------------------------------------------------

LANDING = os.environ.get("LANDING_PATH", "data/landing")
EVIDENCE = os.environ.get("EVIDENCE_PATH", "evidence")
os.makedirs(EVIDENCE, exist_ok=True)

spark = (
    SparkSession.builder.appName("tp1-perfilado-landing")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

report = []          # lineas markdown del informe
metrics = {}         # metricas estructuradas para reutilizar en el documento


def w(line=""):
    report.append(line)
    print(line)


# ---------------------------------------------------------------------------
# Utilidades de perfilado
# ---------------------------------------------------------------------------

def profile_columns(df, name):
    """Nulos, vacios y cardinalidad por columna."""
    total = df.count()
    rows = []
    for c, dtype in df.dtypes:
        aggs = [F.count(F.when(F.col(c).isNull(), c)).alias("nulls"),
                F.count_distinct(F.col(c)).alias("distinct")]
        if dtype == "string":
            aggs.append(
                F.count(F.when(F.trim(F.col(c)) == "", c)).alias("empty")
            )
        r = df.agg(*aggs).collect()[0]
        empty = r["empty"] if dtype == "string" else 0
        missing = r["nulls"] + empty
        rows.append({
            "columna": c,
            "tipo": dtype,
            "nulos": r["nulls"],
            "vacios": empty,
            "pct_faltante": round(100.0 * missing / total, 2) if total else 0.0,
            "distintos": r["distinct"],
        })
    metrics.setdefault(name, {})["total_filas"] = total
    metrics[name]["columnas"] = rows
    return total, rows


def md_table(rows, cols):
    out = ["| " + " | ".join(cols) + " |",
           "|" + "|".join(["---"] * len(cols)) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(r[c]) for c in cols) + " |")
    return out


def section(df, name, titulo):
    w(f"\n## {titulo}\n")
    total, rows = profile_columns(df, name)
    w(f"Filas: **{total}**  ·  Columnas: **{len(df.columns)}**\n")
    w("\n".join(md_table(
        rows, ["columna", "tipo", "nulos", "vacios", "pct_faltante", "distintos"]
    )))
    return total


def top_values(df, col, name, n=12):
    vals = (df.groupBy(col).count().orderBy(F.desc("count")).limit(n).collect())
    pares = [(r[col], r["count"]) for r in vals]
    metrics.setdefault(name, {}).setdefault("dominios", {})[col] = [
        {"valor": str(v), "n": c} for v, c in pares
    ]
    w(f"\n**Dominio de `{col}`**: " +
      ", ".join(f"`{v}` ({c})" for v, c in pares))


def date_range(df, col, name):
    r = df.agg(F.min(col).alias("min"), F.max(col).alias("max")).collect()[0]
    metrics.setdefault(name, {}).setdefault("rangos", {})[col] = {
        "min": str(r["min"]), "max": str(r["max"])
    }
    w(f"\n**Rango de `{col}`**: {r['min']} -> {r['max']}")


# ---------------------------------------------------------------------------
# Encabezado
# ---------------------------------------------------------------------------

w("# Evidencia de exploracion de datos - Landing")
w("")
w(f"Generado: {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
w(f"Motor: Apache Spark {spark.version} (PySpark, master=local[*])")
w(f"Origen: `{LANDING}` (lectura read-only, Landing inmutable)")
w("")
w("> Todas las cifras de este documento son reproducibles ejecutando "
  "`notebooks/01_perfilado_fuentes.py`.")

# ---------------------------------------------------------------------------
# 1. customers_orgs.csv
# ---------------------------------------------------------------------------

orgs = (spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{LANDING}/customers_orgs.csv"))
section(orgs, "customers_orgs", "customers_orgs.csv - Dimension de organizaciones")
top_values(orgs, "plan_tier", "customers_orgs")
top_values(orgs, "hq_region", "customers_orgs")
top_values(orgs, "lifecycle_stage", "customers_orgs")
date_range(orgs, "signup_date", "customers_orgs")

# La escala de NPS es -100..100. Verificado contra la distribucion real:
# min -38, max 101 -> el techo teorico +100 esta violado por un registro.
nps_stats = orgs.agg(F.min("nps_score").alias("min"),
                     F.max("nps_score").alias("max")).collect()[0]
nps_fuera = orgs.filter((F.col("nps_score") < -100) |
                        (F.col("nps_score") > 100)).count()
nps_null = orgs.filter(F.col("nps_score").isNull()).count()
dup_orgs = orgs.count() - orgs.select("org_id").distinct().count()
metrics["customers_orgs"]["anomalias"] = {
    "nps_min": float(nps_stats["min"]), "nps_max": float(nps_stats["max"]),
    "nps_fuera_de_rango_-100_100": nps_fuera,
    "nps_nulo": nps_null,
    "org_id_duplicados": dup_orgs,
}
w(f"\n**Anomalias**: `nps_score` en [{nps_stats['min']}, {nps_stats['max']}] "
  f"(escala NPS valida: -100 a 100) -> fuera de rango: **{nps_fuera}**  ·  "
  f"nulo: **{nps_null}**  ·  `org_id` duplicados: **{dup_orgs}**")

# ---------------------------------------------------------------------------
# 2. users.csv
# ---------------------------------------------------------------------------

users = (spark.read.option("header", True).option("inferSchema", True)
         .csv(f"{LANDING}/users.csv"))
section(users, "users", "users.csv - Usuarios por organizacion")
top_values(users, "role", "users")
date_range(users, "created_at", "users")
date_range(users, "last_login", "users")

sin_login = users.filter(F.col("last_login").isNull()).count()
login_previo = users.filter(F.col("last_login") < F.col("created_at")).count()
metrics["users"]["anomalias"] = {
    "sin_last_login": sin_login,
    "last_login_anterior_a_created_at": login_previo,
}
con_login = users.filter(F.col("last_login").isNotNull()).count()
metrics["users"]["anomalias"]["usuarios_con_login"] = con_login
w(f"\n**Anomalias**: usuarios sin `last_login`: **{sin_login}**  ·  "
  f"`last_login` anterior a `created_at`: **{login_previo}** de {con_login} "
  f"con login registrado ({100.0*login_previo/con_login:.1f}%) -> violacion de "
  f"invariante temporal, no un nulo")

# ---------------------------------------------------------------------------
# 3. resources.csv
# ---------------------------------------------------------------------------

res = (spark.read.option("header", True).option("inferSchema", True)
       .csv(f"{LANDING}/resources.csv"))
section(res, "resources", "resources.csv - Recursos cloud")
top_values(res, "service", "resources")
top_values(res, "region", "resources")
top_values(res, "state", "resources")
date_range(res, "created_at", "resources")

tags_null = res.filter(F.col("tags_json").isNull()).count()
metrics["resources"]["anomalias"] = {"tags_json_nulo": tags_null}
w(f"\n**Anomalias**: `tags_json` nulo (campo semiestructurado a parsear): "
  f"**{tags_null}**")

# ---------------------------------------------------------------------------
# 4. support_tickets.csv
# ---------------------------------------------------------------------------

tk = (spark.read.option("header", True).option("inferSchema", True)
      .csv(f"{LANDING}/support_tickets.csv"))
section(tk, "support_tickets", "support_tickets.csv - Tickets de soporte")
top_values(tk, "category", "support_tickets")
top_values(tk, "severity", "support_tickets")
date_range(tk, "created_at", "support_tickets")

abiertos = tk.filter(F.col("resolved_at").isNull()).count()
csat_null = tk.filter(F.col("csat").isNull()).count()
sla_br = tk.filter(F.col("sla_breached") == True).count()  # noqa: E712
res_previo = tk.filter(F.col("resolved_at") < F.col("created_at")).count()
metrics["support_tickets"]["anomalias"] = {
    "tickets_abiertos_resolved_at_nulo": abiertos,
    "csat_nulo": csat_null,
    "sla_breached_true": sla_br,
    "resolved_at_anterior_a_created_at": res_previo,
}
w(f"\n**Anomalias**: abiertos (`resolved_at` nulo): **{abiertos}**  ·  "
  f"`csat` nulo: **{csat_null}**  ·  SLA incumplido: **{sla_br}**  ·  "
  f"`resolved_at` < `created_at`: **{res_previo}**")

# ---------------------------------------------------------------------------
# 5. marketing_touches.csv
# ---------------------------------------------------------------------------

mkt = (spark.read.option("header", True).option("inferSchema", True)
       .csv(f"{LANDING}/marketing_touches.csv"))
section(mkt, "marketing_touches", "marketing_touches.csv - Interacciones de marketing")
top_values(mkt, "channel", "marketing_touches")
top_values(mkt, "campaign", "marketing_touches")
date_range(mkt, "timestamp", "marketing_touches")

conv_sin_click = mkt.filter((F.col("converted") == True) &
                            (F.col("clicked") == False)).count()  # noqa: E712
metrics["marketing_touches"]["anomalias"] = {
    "converted_sin_clicked": conv_sin_click
}
w(f"\n**Anomalias**: `converted=True` sin `clicked=True` "
  f"(inconsistencia logica del embudo): **{conv_sin_click}**")

# ---------------------------------------------------------------------------
# 6. nps_surveys.csv
# ---------------------------------------------------------------------------

nps = (spark.read.option("header", True).option("inferSchema", True)
       .csv(f"{LANDING}/nps_surveys.csv"))
section(nps, "nps_surveys", "nps_surveys.csv - Encuestas NPS")
date_range(nps, "survey_date", "nps_surveys")
top_values(nps, "comment", "nps_surveys", n=8)

nps_orgs = nps.select("org_id").distinct().count()
nps_multi = (nps.groupBy("org_id").count().filter(F.col("count") > 1).count())
nps_rango = nps.filter((F.col("nps_score") < -100) | (F.col("nps_score") > 100)).count()
metrics["nps_surveys"]["anomalias"] = {
    "orgs_encuestadas": nps_orgs,
    "orgs_con_mas_de_una_encuesta": nps_multi,
    "nps_fuera_de_rango_-100_100": nps_rango,
}
w(f"\n**Anomalias**: organizaciones encuestadas: **{nps_orgs}**  ·  "
  f"con mas de una encuesta (serie temporal -> SCD/ultimo valor): **{nps_multi}**  ·  "
  f"score fuera de rango: **{nps_rango}**")

# ---------------------------------------------------------------------------
# 7. billing_monthly.csv
# ---------------------------------------------------------------------------

bill = (spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{LANDING}/billing_monthly.csv"))
section(bill, "billing_monthly", "billing_monthly.csv - Facturacion mensual")
top_values(bill, "currency", "billing_monthly")
top_values(bill, "month", "billing_monthly")

cred_null = bill.filter(F.col("credits").isNull()).count()
sub_neg = bill.filter(F.col("subtotal") < 0).count()
fx = bill.agg(F.min("exchange_rate_to_usd").alias("min"),
              F.max("exchange_rate_to_usd").alias("max")).collect()[0]
fx_usd = (bill.filter(F.col("currency") == "USD")
          .filter(F.col("exchange_rate_to_usd") != 1.0).count())
metrics["billing_monthly"]["anomalias"] = {
    "credits_nulo": cred_null,
    "subtotal_negativo": sub_neg,
    "fx_min": float(fx["min"]), "fx_max": float(fx["max"]),
    "filas_USD_con_fx_distinto_de_1": fx_usd,
}
w(f"\n**Anomalias**: `credits` nulo: **{cred_null}**  ·  "
  f"`subtotal` negativo: **{sub_neg}**  ·  "
  f"rango `exchange_rate_to_usd`: **{fx['min']} - {fx['max']}**  ·  "
  f"filas en USD con FX != 1.0: **{fx_usd}**")

# ---------------------------------------------------------------------------
# 8. usage_events_stream/*.jsonl
# ---------------------------------------------------------------------------

ev_schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("timestamp", T.StringType()),
    T.StructField("org_id", T.StringType()),
    T.StructField("resource_id", T.StringType()),
    T.StructField("service", T.StringType()),
    T.StructField("region", T.StringType()),
    T.StructField("metric", T.StringType()),
    T.StructField("value", T.StringType()),          # deliberadamente String: tipo ambiguo
    T.StructField("unit", T.StringType()),
    T.StructField("cost_usd_increment", T.DoubleType()),
    T.StructField("schema_version", T.IntegerType()),
    T.StructField("carbon_kg", T.DoubleType()),      # solo v2
    T.StructField("genai_tokens", T.LongType()),     # solo v2 y service=genai
])

ev = (spark.read.schema(ev_schema)
      .json(f"{LANDING}/usage_events_stream/")
      .withColumn("source_file", F.input_file_name()))

w("\n## usage_events_stream/*.jsonl - Eventos de uso (fuente de streaming)\n")
n_files = (ev.select("source_file").distinct().count())
total_ev = ev.count()
w(f"Archivos leidos: **{n_files}**  ·  Eventos: **{total_ev}**\n")
w("> Esquema declarado explicitamente. `value` se lee como String a proposito: "
  "la fuente lo emite indistintamente como numero o como texto.\n")

_, rows_ev = profile_columns(ev.drop("source_file"), "usage_events")
w("\n".join(md_table(
    rows_ev, ["columna", "tipo", "nulos", "vacios", "pct_faltante", "distintos"]
)))

top_values(ev, "service", "usage_events")
top_values(ev, "metric", "usage_events")
top_values(ev, "unit", "usage_events")
top_values(ev, "region", "usage_events")

ev_ts = ev.withColumn("ts", F.to_timestamp("timestamp"))
date_range(ev_ts, "ts", "usage_events")

# --- evolucion de esquema v1 -> v2 ---
w("\n### Evolucion de esquema\n")
sv = (ev_ts.groupBy("schema_version")
      .agg(F.count("*").alias("eventos"),
           F.min("ts").alias("desde"), F.max("ts").alias("hasta"),
           F.count("carbon_kg").alias("con_carbon_kg"),
           F.count("genai_tokens").alias("con_genai_tokens"))
      .orderBy("schema_version").collect())
sv_rows = [{"schema_version": r["schema_version"], "eventos": r["eventos"],
            "desde": str(r["desde"]), "hasta": str(r["hasta"]),
            "con_carbon_kg": r["con_carbon_kg"],
            "con_genai_tokens": r["con_genai_tokens"]} for r in sv]
metrics["usage_events"]["schema_evolution"] = sv_rows
w("\n".join(md_table(sv_rows, ["schema_version", "eventos", "desde", "hasta",
                               "con_carbon_kg", "con_genai_tokens"])))

genai_v2 = ev.filter((F.col("service") == "genai") &
                     (F.col("schema_version") == 2)).count()
genai_v2_tok = ev.filter((F.col("service") == "genai") &
                         (F.col("schema_version") == 2) &
                         F.col("genai_tokens").isNotNull()).count()
tok_fuera = ev.filter((F.col("service") != "genai") &
                      F.col("genai_tokens").isNotNull()).count()
metrics["usage_events"]["regla_genai_tokens"] = {
    "genai_v2": genai_v2, "genai_v2_con_tokens": genai_v2_tok,
    "no_genai_con_tokens": tok_fuera,
}
w(f"\n**Regla derivada**: `genai_tokens` esta presente en **{genai_v2_tok}/{genai_v2}** "
  f"de los eventos `service=genai` con `schema_version=2`, y en **{tok_fuera}** "
  f"eventos de otros servicios. La condicion de presencia es "
  f"`schema_version=2 AND service='genai'`, no solo la version.")

# --- calidad ---
w("\n### Problemas de calidad detectados\n")
val_null = ev.filter(F.col("value").isNull()).count()
val_str = ev.filter(F.col("value").isNotNull() &
                    F.col("value").cast("double").isNull()).count()
val_no_num = ev.filter(F.col("value").isNotNull() &
                       (F.col("value").rlike(r"^-?\d+(\.\d+)?$") == False)).count()  # noqa: E712
unit_null_con_value = ev.filter(F.col("unit").isNull() &
                                F.col("value").isNotNull()).count()
cost_neg = ev.filter(F.col("cost_usd_increment") < -0.01).count()
cost_min = ev.agg(F.min("cost_usd_increment")).collect()[0][0]
dup_ev = total_ev - ev.select("event_id").distinct().count()

q = {
    "value_nulo": val_null,
    "value_no_casteable_a_double": val_str,
    "value_no_numerico_por_regex": val_no_num,
    "unit_nulo_con_value_presente": unit_null_con_value,
    "cost_usd_increment_menor_a_-0.01": cost_neg,
    "cost_usd_increment_minimo": float(cost_min) if cost_min is not None else None,
    "event_id_duplicados": dup_ev,
}
metrics["usage_events"]["calidad"] = q
for k, v in q.items():
    w(f"- `{k}`: **{v}**")

# --- outliers de costo (z-score y percentiles) ---
st = ev.agg(F.avg("cost_usd_increment").alias("mu"),
            F.stddev("cost_usd_increment").alias("sd")).collect()[0]
p = ev.approxQuantile("cost_usd_increment", [0.5, 0.95, 0.99], 0.001)
z3 = ev.filter(F.abs((F.col("cost_usd_increment") - st["mu"]) / st["sd"]) > 3).count()
metrics["usage_events"]["outliers"] = {
    "media": round(float(st["mu"]), 4), "desvio": round(float(st["sd"]), 4),
    "p50": round(p[0], 4), "p95": round(p[1], 4), "p99": round(p[2], 4),
    "eventos_z_mayor_3": z3,
}
w(f"\n**Distribucion de `cost_usd_increment`**: media {st['mu']:.4f}, "
  f"desvio {st['sd']:.4f}, p50 {p[0]:.4f}, p95 {p[1]:.4f}, p99 {p[2]:.4f}. "
  f"Eventos con |z| > 3: **{z3}**.")

# --- fragmentacion: los archivos NO son cortes temporales ---
w("\n### Fragmentacion de archivos y llegada tardia\n")
frag = (ev_ts.groupBy("source_file")
        .agg(F.count("*").alias("eventos"),
             F.min("ts").alias("ts_min"), F.max("ts").alias("ts_max"))
        .orderBy("source_file").collect())
frag_rows = [{"archivo": os.path.basename(r["source_file"]),
              "eventos": r["eventos"], "ts_min": str(r["ts_min"]),
              "ts_max": str(r["ts_max"]),
              "span_dias": (r["ts_max"] - r["ts_min"]).days} for r in frag]
metrics["usage_events"]["fragmentacion"] = frag_rows
w("\n".join(md_table(frag_rows, ["archivo", "eventos", "ts_min", "ts_max", "span_dias"])))
w("\n> Cada archivo cubre practicamente la ventana completa del dataset en lugar "
  "de un corte cronologico. La fragmentacion es aleatoria, no temporal: cada "
  "micro-lote traera eventos antiguos y recientes mezclados. Esto obliga a "
  "definir watermark y politica de late data, y hace que la deduplicacion por "
  "`event_id` deba ser inter-lote y no solo intra-lote.")

# ---------------------------------------------------------------------------
# 9. Integridad referencial entre fuentes
# ---------------------------------------------------------------------------

w("\n## Integridad referencial\n")
orgs_ids = orgs.select("org_id").distinct()
res_ids = res.select("resource_id").distinct()

ri_rows = []
for nombre, df in [("users", users), ("resources", res), ("support_tickets", tk),
                   ("marketing_touches", mkt), ("nps_surveys", nps),
                   ("billing_monthly", bill), ("usage_events", ev)]:
    d = df.select("org_id").distinct()
    huerfanas = d.join(orgs_ids, "org_id", "left_anti").count()
    ri_rows.append({"fuente": nombre, "orgs_distintas": d.count(),
                    "org_id_sin_match_en_customers_orgs": huerfanas})

ev_res_huerf = (ev.select("resource_id").distinct()
                .join(res_ids, "resource_id", "left_anti").count())
metrics["integridad_referencial"] = {
    "org_id": ri_rows,
    "usage_events_resource_id_sin_match": ev_res_huerf,
    "orgs_en_dimension": orgs_ids.count(),
}
w("\n".join(md_table(ri_rows, ["fuente", "orgs_distintas",
                               "org_id_sin_match_en_customers_orgs"])))
w(f"\n`resource_id` de eventos sin match en `resources.csv`: **{ev_res_huerf}** "
  f"(de {ev.select('resource_id').distinct().count()} distintos).")

# ---------------------------------------------------------------------------
# Cierre
# ---------------------------------------------------------------------------

with open(f"{EVIDENCE}/perfilado_landing.md", "w", encoding="utf-8") as f:
    f.write("\n".join(report) + "\n")
with open(f"{EVIDENCE}/perfilado_metrics.json", "w", encoding="utf-8") as f:
    json.dump(metrics, f, indent=2, ensure_ascii=False, default=str)

print(f"\n[OK] evidencia -> {EVIDENCE}/perfilado_landing.md")
print(f"[OK] metricas   -> {EVIDENCE}/perfilado_metrics.json")
spark.stop()

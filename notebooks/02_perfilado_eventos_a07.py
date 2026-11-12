"""
Entrega 2 - Cierre de la decisión abierta A-07.

Pregunta: ¿los archivos de usage_events_stream llegan ordenados en el tiempo?
En el TP1 se perfiló un solo archivo (events_part_0104). Este script lee los
120 archivos en modo batch, read-only, y responde con evidencia:

  1. Rango temporal de cada archivo (¿cada uno es un corte cronológico o una
     muestra de toda la ventana?).
  2. Qué fracción de eventos descartaría un watermark de N días si los
     archivos se procesan en orden, uno por micro-lote (maxFilesPerTrigger=1).
  3. Duplicados de event_id dentro y entre archivos (¿hace falta dedupe
     inter-lote?).
  4. Calidad del dataset completo, para contrastar con el perfil del TP1.

Uso:
    python notebooks/02_perfilado_eventos_a07.py
Salida:
    evidence/a07_orden_eventos.md   y   evidence/a07_orden_eventos.json
"""

import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from src.common.spark import get_spark  # noqa: E402
from src.schemas.events import EVENT_SCHEMA  # noqa: E402

LANDING = os.environ.get("LANDING_PATH", "data/landing")
EVIDENCE = os.environ.get("EVIDENCE_PATH", "evidence")
WATERMARKS_DAYS = [1, 2, 7, 14, 30, 60]

spark = get_spark("a07-orden-eventos")
ev = (spark.read.schema(EVENT_SCHEMA).json(f"{LANDING}/usage_events_stream/")
      .withColumn("file", F.regexp_extract(F.col("_metadata.file_name"), r"(\d{4})", 1).cast("int"))
      .withColumn("ts", F.to_timestamp("timestamp"))
      .cache())

out, m = [], {}


def w(s=""):
    out.append(s)
    print(s)


total = ev.count()
n_files = ev.select("file").distinct().count()
m.update(total_eventos=total, archivos=n_files)

# --- 1. rango temporal por archivo ------------------------------------------
per_file = (ev.groupBy("file")
            .agg(F.count("*").alias("n"), F.min("ts").alias("ts_min"),
                 F.max("ts").alias("ts_max"),
                 F.percentile_approx(F.unix_timestamp("ts"), 0.5).alias("p50"))
            .withColumn("span_dias",
                        (F.unix_timestamp("ts_max") - F.unix_timestamp("ts_min")) / 86400)
            .orderBy("file"))
rows = per_file.collect()
spans = [r["span_dias"] for r in rows]
corr = per_file.select(F.corr("file", "p50")).first()[0]
global_min, global_max = ev.agg(F.min("ts"), F.max("ts")).first()
m["span_por_archivo"] = {"min": round(min(spans), 1), "max": round(max(spans), 1),
                         "promedio": round(sum(spans) / len(spans), 1)}
m["correlacion_indice_archivo_vs_mediana_ts"] = round(corr, 4)
m["ventana_global"] = [str(global_min), str(global_max)]

# --- 2. simulación del watermark con un archivo por micro-lote --------------
# Structured Streaming fija, al cerrar cada micro-lote, watermark = max(ts visto) - delay.
# En el lote siguiente, los eventos con ts < watermark quedan fuera de toda
# operación con estado (agregación y dedupe). El primer lote no descarta nada.
w_order = Window.orderBy("file").rowsBetween(Window.unboundedPreceding, -1)
max_prev = (per_file.select("file", "ts_max")
            .withColumn("max_previo", F.max("ts_max").over(w_order)).select("file", "max_previo"))
ev_wm = ev.join(max_prev, "file")
sim = []
for d in WATERMARKS_DAYS:
    late = ev_wm.filter(F.col("ts") < F.col("max_previo") - F.expr(f"INTERVAL {d} DAYS")).count()
    sim.append({"watermark_dias": d, "descartados": late,
                "pct_descartado": round(100 * late / total, 1),
                "pct_procesado": round(100 * (total - late) / total, 1)})
m["simulacion_watermark"] = sim

# --- 3. duplicados ------------------------------------------------------------
dup_keys = ev.groupBy("event_id").agg(F.count("*").alias("n"),
                                      F.countDistinct("file").alias("archivos"))
dups = dup_keys.filter("n > 1")
m["event_id_distintos"] = dup_keys.count()
m["event_id_repetidos"] = dups.count()
m["filas_sobrantes_por_duplicado"] = int(dups.agg(F.sum(F.col("n") - 1)).first()[0] or 0)
m["duplicados_entre_archivos"] = dups.filter("archivos > 1").count()
m["duplicados_dentro_de_un_archivo"] = dups.filter("archivos = 1").count()
m["event_id_nulo"] = ev.filter(F.col("event_id").isNull()).count()
# ¿los duplicados son copias exactas o versiones distintas del mismo evento?
cols = [c for c in EVENT_SCHEMA.fieldNames()]
distinct_versions = (ev.join(dups.select("event_id"), "event_id")
                     .select(*cols).distinct().groupBy("event_id").count()
                     .filter("count > 1").count())
m["duplicados_con_contenido_distinto"] = distinct_versions

# --- 4. calidad del dataset completo --------------------------------------------
m["calidad"] = {
    "value_nulo": ev.filter(F.col("value").isNull()).count(),
    "value_no_casteable": ev.filter(F.col("value").isNotNull() &
                                    F.col("value").cast("double").isNull()).count(),
    "unit_nulo_con_value": ev.filter(F.col("unit").isNull() & F.col("value").isNotNull()).count(),
    "cost_menor_a_-0.01": ev.filter(F.col("cost_usd_increment") < -0.01).count(),
    "cost_min": ev.agg(F.min("cost_usd_increment")).first()[0],
    "cost_max": ev.agg(F.max("cost_usd_increment")).first()[0],
    "timestamp_no_parseable": ev.filter(F.col("ts").isNull()).count(),
}
m["schema_version"] = {str(r[0]): r[1] for r in ev.groupBy("schema_version").count().collect()}

# --- informe ---------------------------------------------------------------------
w("# A-07 · Orden temporal de los archivos de eventos")
w(f"\nGenerado: {datetime.now(timezone.utc).isoformat(timespec='seconds')} · "
  f"Spark {spark.version} · `{LANDING}/usage_events_stream/`")
w(f"\n**{n_files} archivos · {total:,} eventos** · ventana {global_min} → {global_max}")
w("\n## 1. Cada archivo cubre casi toda la ventana")
w(f"\nSpan por archivo: mín. {m['span_por_archivo']['min']} días, "
  f"máx. {m['span_por_archivo']['max']}, promedio {m['span_por_archivo']['promedio']}.")
w(f"Correlación entre número de archivo y mediana de su timestamp: **{corr:.3f}** "
  f"(≈ 0: el número de archivo no dice nada sobre la fecha de sus eventos).")
w("\n| archivo | eventos | ts_min | ts_max | span (días) |\n|---|---|---|---|---|")
for r in rows[:5] + rows[-3:]:
    w(f"| {r['file']:04d} | {r['n']} | {r['ts_min']} | {r['ts_max']} | {r['span_dias']:.1f} |")
w("\n(primeros 5 y últimos 3; el detalle completo está en el JSON)")
w("\n## 2. Descarte por watermark procesando un archivo por micro-lote\n")
w("| watermark | descartados | % descartado | % procesado |\n|---|---|---|---|")
for s in sim:
    w(f"| {s['watermark_dias']} días | {s['descartados']:,} | {s['pct_descartado']}% | "
      f"{s['pct_procesado']}% |")
w("\n## 3. Duplicados de `event_id`\n")
w(f"- Repetidos: **{m['event_id_repetidos']}** claves ({m['filas_sobrantes_por_duplicado']} filas sobrantes)")
w(f"- Entre archivos distintos: **{m['duplicados_entre_archivos']}** · dentro de un mismo archivo: "
  f"**{m['duplicados_dentro_de_un_archivo']}**")
w(f"- Con contenido distinto entre copias: **{distinct_versions}**")
w("\n## 4. Calidad del dataset completo\n")
for k, v in m["calidad"].items():
    w(f"- `{k}`: **{v}**")
w(f"- `schema_version`: {m['schema_version']}")

os.makedirs(EVIDENCE, exist_ok=True)
m["por_archivo"] = [{"archivo": r["file"], "eventos": r["n"], "ts_min": str(r["ts_min"]),
                     "ts_max": str(r["ts_max"]), "span_dias": round(r["span_dias"], 2)}
                    for r in rows]
with open(f"{EVIDENCE}/a07_orden_eventos.md", "w", encoding="utf-8") as fh:
    fh.write("\n".join(out) + "\n")
with open(f"{EVIDENCE}/a07_orden_eventos.json", "w", encoding="utf-8") as fh:
    json.dump(m, fh, indent=2, ensure_ascii=False, default=str)
spark.stop()

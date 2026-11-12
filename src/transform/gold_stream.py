"""Flujo C · Ruta streaming del mart org_daily_usage_by_service (capa rápida).

Lee Bronze de eventos como stream (un archivo por micro-lote), conforma con la
misma función que Silver, y agrega por (org, día, servicio) con un watermark de
7 días sobre la fecha del evento (D-008). Es donde se ejercita el manejo de late
data que pide la consigna:

  - un evento cuya ventana diaria cierre más de 7 días antes del máximo
    event_ts visto se descarta de la agregación. Spark 4 filtra los tardíos con
    el watermark del lote ANTERIOR (separa el watermark de tardíos del de
    desalojo de estado): los descartes empiezan en el tercer micro-lote;
  - el evento no se pierde: está en Bronze y en Silver, y la reconciliación
    (gold_job, flujo D) lo incorpora al mart oficial.

Salida: gold/org_daily_usage_by_service_speed, una fila por clave y micro-lote
(outputMode update), particionada por batch_id. foreachBatch reescribe la
partición de su batch_id, así que reintentar un lote no duplica (idempotencia
de foreachBatch). La vista vigente es la fila del último batch_id por clave.

Uso:
    python -m src.transform.gold_stream
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.config import Settings, load_settings
from src.common.spark import get_spark
from src.transform.gold import KEY, MART, daily_usage_aggs, round_money, usage_date_from_window
from src.transform.silver import conform_events

log = logging.getLogger("gold_stream")
JOB = "gold_stream"
EVENT_WATERMARK = "7 days"          # D-008
SPEED = f"{MART}_speed"


def speed_path(cfg: Settings) -> str:
    return f"{cfg.lake}/gold/{SPEED}"


def build_stream(spark: SparkSession, cfg: Settings, max_files: int = 1) -> DataFrame:
    bronze_path = f"{cfg.bronze}/usage_events"
    schema = spark.read.parquet(bronze_path).schema           # Bronze ya tiene esquema fijo
    events = (spark.readStream.schema(schema)
              .option("maxFilesPerTrigger", max_files)
              .parquet(bronze_path))
    orgs = spark.read.parquet(f"{cfg.lake}/silver/customers_orgs")   # join stream-estático
    conformed = conform_events(events, orgs, dedupe=False)
    return (conformed
            .withWatermark("event_ts", EVENT_WATERMARK)
            .groupBy("org_id", F.window("event_ts", "1 day"), "service")
            .agg(*daily_usage_aggs())
            .withColumn("usage_date", usage_date_from_window())
            .drop("window"))


def make_writer(cfg: Settings):
    def write_batch(df: DataFrame, batch_id: int) -> None:
        (round_money(df)
         .withColumn("source", F.lit("streaming"))
         .withColumn("processed_at", F.current_timestamp())
         .withColumn("batch_id", F.lit(batch_id))
         .coalesce(1).write
         .mode("overwrite")                    # dinámico: solo la partición de este batch_id
         .partitionBy("batch_id")
         .parquet(speed_path(cfg)))
    return write_batch


def latest_view(spark: SparkSession, cfg: Settings) -> DataFrame:
    """Valor vigente de la capa rápida: el último batch que tocó cada clave."""
    s = spark.read.parquet(speed_path(cfg))
    w = Window.partitionBy(*KEY).orderBy(F.desc("batch_id"))
    return s.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def run(spark: SparkSession, cfg: Settings, max_files: int = 1) -> dict:
    t0 = time.time()
    q = (build_stream(spark, cfg, max_files).writeStream
         .queryName(JOB)
         .outputMode("update")
         .foreachBatch(make_writer(cfg))
         .option("checkpointLocation", f"{cfg.checkpoints}/{JOB}")
         .trigger(availableNow=True)
         .start())
    q.awaitTermination()

    progress = [p for p in q.recentProgress if p["numInputRows"] > 0]
    rows_in = sum(p["numInputRows"] for p in progress)
    durations = [p["durationMs"].get("triggerExecution", 0) for p in progress]

    report = {"job": JOB, "watermark": f"event_ts, {EVENT_WATERMARK}",
              "ejecutado": datetime.now(timezone.utc).isoformat(timespec="seconds"),
              "esta_corrida": {
                  "micro_lotes": len(progress),
                  "eventos_leidos": rows_in,
                  "latencia_por_lote_ms": {"max": max(durations, default=0),
                                           "mediana": sorted(durations)[len(durations) // 2]
                                           if durations else 0},
                  "watermark_final": progress[-1]["eventTime"].get("watermark") if progress else None,
              }}
    if os.path.isdir(speed_path(cfg)):
        live = latest_view(spark, cfg)
        # Tardíos, medidos de forma exacta y acumulada: en outputMode update cada
        # clave se emite con su agregado completo, así que la suma de event_count
        # en la vista vigente es el total de eventos que entraron a la agregación.
        # (numRowsDroppedByWatermark de Spark no sirve para esto: los tardíos se
        # filtran antes del operador de estado y la métrica los subcuenta.)
        streamed = spark.read.parquet(f"{cfg.bronze}/usage_events").count()
        accepted = live.agg(F.sum("event_count")).first()[0] or 0
        report["late_data"] = {
            "eventos_en_bronze": streamed,
            "incorporados_a_la_capa_rapida": accepted,
            "descartados_por_watermark": streamed - accepted,
            "pct_descartado": round(100 * (streamed - accepted) / streamed, 1) if streamed else 0.0,
        }
        report["capa_rapida"] = {
            "claves": live.count(),
            "costo_usd": round(live.agg(F.sum("daily_cost_usd")).first()[0] or 0, 2),
            "rango_usage_date": [str(x) for x in live.agg(F.min("usage_date"),
                                                          F.max("usage_date")).first()],
        }
        gold = f"{cfg.lake}/gold/{MART}"
        if os.path.isdir(gold):
            total = spark.read.parquet(gold).agg(F.sum("daily_cost_usd")).first()[0]
            report["capa_rapida"]["cobertura_vs_reconciliado_pct"] = round(
                100 * report["capa_rapida"]["costo_usd"] / total, 1)
    report["segundos"] = round(time.time() - t0, 1)

    os.makedirs(f"{cfg.evidence}/runs", exist_ok=True)
    out = f"{cfg.evidence}/runs/{JOB}_{datetime.now(timezone.utc):%Y-%m-%d_%H%M%S}.json"
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    e, late = report["esta_corrida"], report.get("late_data", {})
    log.info("Streaming Gold: %d lotes, %d eventos leídos. Tardíos descartados (acumulado): %s "
             "(%s%%). Capa rápida: %s · Log -> %s", e["micro_lotes"], rows_in,
             late.get("descartados_por_watermark"), late.get("pct_descartado"),
             report.get("capa_rapida"), out)
    return report


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = get_spark(JOB)
    try:
        run(spark, load_settings())
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

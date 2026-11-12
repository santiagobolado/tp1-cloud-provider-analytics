"""Ingesta streaming: usage_events_stream/*.jsonl -> Bronze (Parquet por ingest_date).

Structured Streaming lee el directorio de Landing; cada archivo es un
micro-lote (maxFilesPerTrigger=1), que es lo que la fragmentación del dataset
busca simular.

Dos consultas sobre la misma fuente, cada una con su checkpoint:

  bronze      eventos válidos -> watermark sobre ingest_ts -> dedupe por event_id
              -> bronze/usage_events/ingest_date=YYYY-MM-DD/
  quarantine  eventos que no pueden entrar a Bronze, con su motivo
              -> quarantine/usage_events/ingest_date=.../regla=.../

Por qué el watermark de Bronze va sobre la hora de llegada y no sobre la del
evento (D-013): la deduplicación con watermark descarta sin dejar rastro todo
lo que considera tardío. Sobre event_ts, con un watermark de 7 días, eso es el
87,6% del dataset (D-012) y la reconciliación (D-004) se quedaría sin insumo.
ingest_ts siempre avanza, así que ningún evento llega "tarde" respecto de él:
Bronze conserva todo y descarta solo las re-entregas del mismo event_id dentro
de 7 días. El watermark sobre event_ts (D-008) se aplica en la agregación del
mart, donde descartar tardíos es aceptable porque Bronze los conserva.

Idempotencia: el checkpoint registra qué archivos ya se procesaron y el estado
de deduplicación; el sink de archivos de Spark registra qué se escribió
(_spark_metadata). Re-ejecutar no vuelve a procesar ni a escribir nada.

Uso:
    python -m src.ingest.stream_to_bronze                    # procesa lo pendiente y termina
    python -m src.ingest.stream_to_bronze --continuous 30    # queda escuchando, lote cada 30 s
Reinicio completo: borrar data/bronze/usage_events, data/quarantine/usage_events
y _checkpoints/stream_to_bronze.
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import Settings, load_settings
from src.common.spark import get_spark
from src.schemas.events import CORRUPT_COL, EVENT_SCHEMA, EVENT_SCHEMA_WITH_CORRUPT

log = logging.getLogger("stream_to_bronze")

DEDUP_WATERMARK = "7 days"     # D-013: ventana de re-entrega, sobre ingest_ts
JOB = "stream_to_bronze"


def read_events(spark: SparkSession, landing: str, max_files: int = 1) -> DataFrame:
    return (spark.readStream
            .schema(EVENT_SCHEMA_WITH_CORRUPT)                # nunca inferSchema
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_COL)
            .option("maxFilesPerTrigger", max_files)          # un archivo = un micro-lote
            .json(f"{landing}/usage_events_stream/")
            .withColumn("source_file", F.col("_metadata.file_path"))
            # En streaming, current_timestamp() es la hora del micro-lote:
            # todas las filas de un lote comparten ingest_ts.
            .withColumn("ingest_ts", F.current_timestamp())
            .withColumn("ingest_date", F.to_date("ingest_ts"))
            .withColumn("event_ts", F.try_to_timestamp(F.col("timestamp"))))


def quality_rule() -> F.Column:
    """Motivo por el que un evento no puede entrar a Bronze, o nulo si puede."""
    return (F.when(F.col(CORRUPT_COL).isNotNull(), F.lit("no_parseable"))
             .when(F.col("event_id").isNull(), F.lit("event_id_nulo"))
             .when(F.col("event_ts").isNull(), F.lit("timestamp_invalido")))


BRONZE_COLS = EVENT_SCHEMA.fieldNames() + ["event_ts", "source_file", "ingest_ts", "ingest_date"]


def bronze_stream(events: DataFrame) -> DataFrame:
    return (events
            .filter(quality_rule().isNull())
            .select(*BRONZE_COLS)
            .withWatermark("ingest_ts", DEDUP_WATERMARK)
            # Deduplica event_id entre micro-lotes y entre corridas (el estado
            # vive en el checkpoint) mientras las copias lleguen a menos de 7 días.
            .dropDuplicatesWithinWatermark(["event_id"]))


def quarantine_stream(events: DataFrame) -> DataFrame:
    record = F.to_json(F.struct(*EVENT_SCHEMA.fieldNames()))
    return (events
            .withColumn("regla", quality_rule())
            .filter(F.col("regla").isNotNull())
            .select(F.lit("usage_events").alias("entity"), "regla",
                    record.alias("record_json"), F.col(CORRUPT_COL).alias("raw_record"),
                    "source_file", "ingest_ts", "ingest_date"))


def start(df: DataFrame, path: str, checkpoint: str, partition_cols: list,
          name: str, trigger: dict):
    return (df.coalesce(1)                      # un archivo por partición y micro-lote (D-005)
            .writeStream
            .queryName(name)
            .format("parquet")
            .option("path", path)
            .option("checkpointLocation", checkpoint)
            .partitionBy(*partition_cols)
            .outputMode("append")
            .trigger(**trigger)
            .start())


def summarize(query) -> dict:
    batches = [p for p in query.recentProgress if p["numInputRows"] > 0]
    state = [b["stateOperators"][0] for b in batches if b.get("stateOperators")]
    return {
        "micro_lotes_con_datos": len(batches),
        "filas_entrada": sum(b["numInputRows"] for b in batches),
        "filas_por_lote": sorted({b["numInputRows"] for b in batches}),
        "claves_en_estado_al_final": state[-1]["numRowsTotal"] if state else 0,
        "descartadas_por_watermark": sum(s.get("numRowsDroppedByWatermark", 0) for s in state),
    }


def run(spark: SparkSession, cfg: Settings, continuous_seconds: int = 0,
        max_files: int = 1) -> dict:
    t0 = time.time()
    events = read_events(spark, cfg.landing, max_files)
    trigger = ({"processingTime": f"{continuous_seconds} seconds"} if continuous_seconds
               else {"availableNow": True})
    ck = f"{cfg.checkpoints}/{JOB}"
    q_bronze = start(bronze_stream(events), f"{cfg.bronze}/usage_events",
                     f"{ck}/bronze", ["ingest_date"], "bronze_usage_events", trigger)
    q_quar = start(quarantine_stream(events), f"{cfg.quarantine}/usage_events",
                   f"{ck}/quarantine", ["ingest_date", "regla"], "quarantine_usage_events",
                   trigger)
    log.info("Streaming iniciado (%s). Checkpoint: %s",
             "continuo" if continuous_seconds else "availableNow", ck)
    q_bronze.awaitTermination()
    q_quar.awaitTermination()

    bronze_total = spark.read.parquet(f"{cfg.bronze}/usage_events").count()
    quar_path = f"{cfg.quarantine}/usage_events"
    # Sin rechazos, el sink deja solo _spark_metadata y no hay esquema que leer.
    has_files = any(n.endswith(".parquet") for _, _, ns in os.walk(quar_path) for n in ns)
    quar = spark.read.parquet(quar_path) if has_files else None
    report = {
        "job": JOB,
        "spark_version": spark.version,
        "ejecutado": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "watermark_dedupe": f"ingest_ts, {DEDUP_WATERMARK}",
        "esta_corrida": {"bronze": summarize(q_bronze), "quarantine": summarize(q_quar)},
        "acumulado": {
            "filas_bronze": bronze_total,
            "event_id_distintos_bronze": spark.read.parquet(f"{cfg.bronze}/usage_events")
                                              .select("event_id").distinct().count(),
            "filas_quarantine": quar.count() if quar else 0,
            "quarantine_por_regla": ({r["regla"]: r["count"]
                                      for r in quar.groupBy("regla").count().collect()}
                                     if quar else {}),
        },
        "segundos": round(time.time() - t0, 1),
    }
    os.makedirs(f"{cfg.evidence}/runs", exist_ok=True)
    out = f"{cfg.evidence}/runs/{JOB}_{datetime.now(timezone.utc):%Y-%m-%d_%H%M%S}.json"
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    b = report["esta_corrida"]["bronze"]
    log.info("Esta corrida: %d micro-lotes, %d filas leídas. Acumulado en Bronze: %d (%d event_id "
             "distintos). Quarantine: %d. Log -> %s", b["micro_lotes_con_datos"], b["filas_entrada"],
             report["acumulado"]["filas_bronze"], report["acumulado"]["event_id_distintos_bronze"],
             report["acumulado"]["filas_quarantine"], out)
    return report


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--continuous", type=int, default=0, metavar="SEG",
                   help="quedar escuchando con un micro-lote cada SEG segundos (Ctrl+C para cortar)")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_settings()
    if not os.path.isdir(f"{cfg.landing}/usage_events_stream"):
        log.error("No existe %s/usage_events_stream", cfg.landing)
        return 1
    spark = get_spark(JOB)
    try:
        run(spark, cfg, args.continuous)
    except KeyboardInterrupt:
        log.info("Interrumpido. El checkpoint permite retomar donde quedó.")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

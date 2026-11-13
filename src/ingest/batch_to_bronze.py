"""Ingesta batch: Landing (CSV) -> Bronze (Parquet particionado por ingest_date).

Responsabilidad de Bronze (§8.2 del diseño): mismo grano que la fuente, tipado
explícito, deduplicación por clave natural y columnas técnicas. No limpia ni
normaliza: eso es Silver.

Reglas de la frontera Landing -> Bronze (§8.7). Ningún registro se descarta en
silencio: lo que no entra a Bronze va a quarantine con su motivo.

  no_parseable     la fila no respeta el esquema declarado
  clave_nula       falta (alguna parte de) la clave natural
  clave_duplicada  la clave ya apareció; se conserva una fila y el resto se aísla

Idempotencia (D-011): cada corrida reescribe solo su partición ingest_date.
Correr dos veces el mismo día deja el mismo resultado; correr otro día agrega
una partición nueva y conserva el historial.

Uso:
    python -m src.ingest.batch_to_bronze                  # las 7 fuentes, fecha de hoy
    INGEST_DATE=2026-11-12 python -m src.ingest.batch_to_bronze
    python -m src.ingest.batch_to_bronze --entities customers_orgs users
"""

import argparse
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
from src.schemas.batch import CORRUPT_COL, SOURCES, SOURCES_BY_ENTITY, BatchSource

log = logging.getLogger("batch_to_bronze")

QUARANTINE_COLS = ["entity", "regla", "detalle", "record_json", "raw_record",
                   "source_file", "ingest_ts", "ingest_date"]


class ConservationError(RuntimeError):
    """Se perdieron o se inventaron filas entre Landing y Bronze + quarantine."""


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def read_source(spark: SparkSession, src: BatchSource, landing: str) -> DataFrame:
    path = f"{landing}/{src.file}"
    if not os.path.exists(path):
        raise FileNotFoundError(f"No existe la fuente en Landing: {path}")
    df = (spark.read
          .schema(src.schema)
          .option("header", True)
          .option("mode", "PERMISSIVE")                     # no abortar: capturar
          .option("columnNameOfCorruptRecord", CORRUPT_COL)
          .option("escape", '"')                            # tags_json viene con "" (RFC 4180)
          .option("dateFormat", "yyyy-MM-dd")
          .csv(path)
          .withColumn("source_file", F.col("_metadata.file_path"))
          # Posición de la fila en el archivo: los splits de un archivo se leen
          # en orden, así que el id creciente respeta el orden de las líneas.
          .withColumn("_pos", F.monotonically_increasing_id()))
    # Spark no permite filtrar solo por la columna de corruptos sin materializar
    # antes el DataFrame; el cache además evita releer el CSV en cada conteo.
    return df.cache()


# ---------------------------------------------------------------------------
# Transform: reglas de la frontera Landing -> Bronze
# ---------------------------------------------------------------------------

def business_cols(src: BatchSource) -> list:
    return [c.name for c in src.schema.fields if c.name != CORRUPT_COL]


def split_by_quality(df: DataFrame, src: BatchSource) -> tuple:
    """Devuelve (válidos, rechazados). Cada fila cae en exactamente uno de los dos."""
    cols = business_cols(src)
    key = list(src.natural_key)
    record_json = F.to_json(F.struct(*cols))

    corrupt = F.col(CORRUPT_COL).isNotNull()
    key_null = F.lit(False)
    for k in key:
        key_null = key_null | F.col(k).isNull()

    parseable = df.filter(~corrupt)
    rejected_corrupt = (df.filter(corrupt)
                        .withColumn("regla", F.lit("no_parseable"))
                        .withColumn("detalle", F.lit("la fila no respeta el esquema declarado")))
    rejected_null = (parseable.filter(key_null)
                     .withColumn("regla", F.lit("clave_nula"))
                     .withColumn("detalle", F.lit(f"clave natural incompleta: {key}")))

    # Deduplicación: entre filas con la misma clave se conserva la primera
    # aparición en el archivo; las siguientes son re-entregas y van a quarantine.
    # (Ordenar por contenido sería determinístico pero arbitrario: podía
    # conservar la copia y aislar el original.)
    w = Window.partitionBy(*key).orderBy("_pos")
    ranked = parseable.filter(~key_null).withColumn("_rn", F.row_number().over(w))
    valid = ranked.filter(F.col("_rn") == 1).drop("_rn", "_pos", CORRUPT_COL)
    rejected_dup = (ranked.filter(F.col("_rn") > 1).drop("_rn")
                    .withColumn("regla", F.lit("clave_duplicada"))
                    .withColumn("detalle", F.lit(f"clave natural repetida: {key}")))

    rejected = (rejected_corrupt.unionByName(rejected_null).unionByName(rejected_dup)
                .withColumn("entity", F.lit(src.entity))
                .withColumn("record_json", record_json)
                .withColumnRenamed(CORRUPT_COL, "raw_record"))
    return valid, rejected


def add_technical_cols(df: DataFrame, ingest_ts: datetime, ingest_date: str) -> DataFrame:
    # ingest_ts es un único valor por corrida, calculado una vez en el driver:
    # todas las filas de la corrida comparten el mismo sello (linaje temporal).
    return (df.withColumn("ingest_ts", F.lit(ingest_ts).cast("timestamp"))
              .withColumn("ingest_date", F.lit(ingest_date).cast("date")))


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def write_bronze(df: DataFrame, path: str) -> None:
    # coalesce(1): a este volumen, un archivo por partición evita small files (D-005).
    # overwrite + partitionOverwriteMode=dynamic: reemplaza solo el ingest_date
    # de esta corrida y conserva los anteriores (D-011).
    (df.coalesce(1).write
       .mode("overwrite")
       .partitionBy("ingest_date")
       .parquet(path))


def write_quarantine(df: DataFrame, root: str, ingest_date: str) -> None:
    # Se reescribe el día completo de la entidad, no partición por partición:
    # si una regla tuvo rechazos en la corrida anterior y en esta no, el
    # overwrite dinámico dejaría la partición vieja huérfana.
    day_path = f"{root}/ingest_date={ingest_date}"
    (df.drop("ingest_date").coalesce(1).write
       .mode("overwrite")
       .option("partitionOverwriteMode", "static")
       .partitionBy("regla")
       .parquet(day_path))


def dir_stats(path: str) -> dict:
    files, size = 0, 0
    for base, _, names in os.walk(path):
        for n in names:
            if n.endswith(".parquet"):
                files += 1
                size += os.path.getsize(os.path.join(base, n))
    return {"archivos_parquet": files, "bytes": size}


# ---------------------------------------------------------------------------
# Orquestación
# ---------------------------------------------------------------------------

def ingest_source(spark: SparkSession, src: BatchSource, cfg: Settings,
                  ingest_ts: datetime) -> dict:
    t0 = time.time()
    raw = read_source(spark, src, cfg.landing)
    valid, rejected = split_by_quality(raw, src)
    valid = add_technical_cols(valid, ingest_ts, cfg.ingest_date)
    rejected = add_technical_cols(rejected, ingest_ts, cfg.ingest_date).select(*QUARANTINE_COLS)

    bronze_path = f"{cfg.bronze}/{src.entity}"
    quarantine_path = f"{cfg.quarantine}/{src.entity}"
    write_bronze(valid, bronze_path)
    write_quarantine(rejected, quarantine_path, cfg.ingest_date)

    # Conteos sobre lo efectivamente escrito, no sobre el plan en memoria.
    leidas = raw.count()
    escritas = (spark.read.parquet(bronze_path)
                .filter(F.col("ingest_date") == cfg.ingest_date).count())
    q_day = f"{quarantine_path}/ingest_date={cfg.ingest_date}"
    # Sin rechazos, Spark deja el directorio sin archivos Parquet: no hay qué leer.
    q = spark.read.parquet(q_day) if dir_stats(q_day)["archivos_parquet"] else None
    por_regla = {r["regla"]: r["count"] for r in q.groupBy("regla").count().collect()} if q else {}
    raw.unpersist()

    rechazadas = sum(por_regla.values())
    if leidas != escritas + rechazadas:
        raise ConservationError(
            f"{src.entity}: leídas={leidas} != bronze={escritas} + quarantine={rechazadas}")

    part = f"{bronze_path}/ingest_date={cfg.ingest_date}"
    stats = {
        "entity": src.entity,
        "landing_file": f"{cfg.landing}/{src.file}",
        "natural_key": list(src.natural_key),
        "filas_leidas": leidas,
        "filas_bronze": escritas,
        "filas_quarantine": rechazadas,
        "quarantine_por_regla": por_regla,
        "bronze_particion": part,
        **dir_stats(part),
        "segundos": round(time.time() - t0, 2),
    }
    log.info("%-18s leídas=%5d bronze=%5d quarantine=%d %s",
             src.entity, leidas, escritas, rechazadas, por_regla or "")
    return stats


def run(spark: SparkSession, cfg: Settings, entities=None) -> dict:
    sources = [SOURCES_BY_ENTITY[e] for e in entities] if entities else SOURCES
    ingest_ts = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    log.info("Corrida batch -> Bronze · ingest_date=%s · ingest_ts=%s",
             cfg.ingest_date, ingest_ts.isoformat())
    results = [ingest_source(spark, s, cfg, ingest_ts) for s in sources]
    report = {
        "job": "batch_to_bronze",
        "spark_version": spark.version,
        "ingest_date": cfg.ingest_date,
        "ingest_ts": ingest_ts.isoformat() + "Z",
        "fuentes": results,
    }
    out_dir = f"{cfg.evidence}/runs"
    os.makedirs(out_dir, exist_ok=True)
    out = f"{out_dir}/batch_to_bronze_{cfg.ingest_date}_{ingest_ts:%H%M%S}.json"
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    log.info("Log de corrida -> %s", out)
    return report


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--entities", nargs="*", choices=list(SOURCES_BY_ENTITY),
                   help="subconjunto de fuentes (por defecto, las 7)")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_settings()
    spark = get_spark("batch-to-bronze")
    try:
        run(spark, cfg, args.entities)
    except (FileNotFoundError, ConservationError) as exc:
        log.error("%s", exc)
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

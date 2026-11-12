"""Flujo D · Reconciliación: Silver -> Gold org_daily_usage_by_service (completo).

Recomputa el mart desde Silver, que contiene el 100% de los eventos (incluidos
los que la ruta streaming descartó por tardíos), y sobrescribe Gold. Es el mart
oficial: idempotente por construcción (§6.5) y marcado con last_reconciled_at
para que el consumo distinga un valor consolidado de uno provisorio (§6.7).

Uso:
    python -m src.transform.gold_job
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import Settings, load_settings
from src.common.spark import get_spark
from src.transform.gold import KEY, MART, aggregate_daily_usage, round_money

log = logging.getLogger("gold_job")
JOB = "gold_job"


class ConservationError(RuntimeError):
    pass


def gold_path(cfg: Settings, mart: str = MART) -> str:
    return f"{cfg.lake}/gold/{mart}"


def run(spark: SparkSession, cfg: Settings) -> dict:
    t0 = time.time()
    silver = spark.read.parquet(f"{cfg.lake}/silver/usage_events")
    reconciled_at = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    mart = (round_money(aggregate_daily_usage(silver))
            .withColumn("source", F.lit("reconciliacion"))
            .withColumn("last_reconciled_at", F.lit(reconciled_at).cast("timestamp")))

    # D-005: Gold diario por usage_date. 60 particiones, un archivo cada una.
    (mart.repartition("usage_date").write
         .mode("overwrite").option("partitionOverwriteMode", "static")
         .partitionBy("usage_date").parquet(gold_path(cfg)))

    g = spark.read.parquet(gold_path(cfg))
    n = g.count()
    distinct_keys = g.select(*KEY).distinct().count()
    cost_silver = silver.agg(F.sum("cost_usd")).first()[0]
    cost_gold = g.agg(F.sum("daily_cost_usd")).first()[0]
    events_gold = g.agg(F.sum("event_count")).first()[0]
    if distinct_keys != n:
        raise ConservationError(f"grano roto: {n} filas para {distinct_keys} claves")
    if events_gold != silver.count() or abs(cost_gold - cost_silver) > 0.01 * 60 * 6:
        raise ConservationError(f"Gold no cuadra con Silver: eventos {events_gold}, "
                                f"costo {cost_gold:.2f} vs {cost_silver:.2f}")
    files = sum(1 for _, _, ns in os.walk(gold_path(cfg)) for x in ns if x.endswith(".parquet"))
    report = {
        "job": JOB,
        "mart": MART,
        "last_reconciled_at": reconciled_at.isoformat() + "Z",
        "filas": n,
        "grano_unico": True,
        "orgs": g.select("org_id").distinct().count(),
        "dias": g.select("usage_date").distinct().count(),
        "servicios": g.select("service").distinct().count(),
        "eventos_agregados": events_gold,
        "costo_total_usd": round(cost_gold, 2),
        "costo_total_silver_usd": round(cost_silver, 2),
        "filas_con_carbon_kg_nulo": g.filter(F.col("carbon_kg").isNull()).count(),
        "filas_con_genai_tokens": g.filter(F.col("genai_tokens").isNotNull()).count(),
        "archivos_parquet": files,
        "bytes": sum(os.path.getsize(os.path.join(b, x)) for b, _, ns in os.walk(gold_path(cfg))
                     for x in ns if x.endswith(".parquet")),
        "segundos": round(time.time() - t0, 1),
    }
    os.makedirs(f"{cfg.evidence}/runs", exist_ok=True)
    out = f"{cfg.evidence}/runs/{JOB}_{datetime.now(timezone.utc):%Y-%m-%d_%H%M%S}.json"
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    log.info("Gold %s: %d filas (%d orgs × %d días × %d servicios posibles) · costo USD %.2f · "
             "Log -> %s", MART, n, report["orgs"], report["dias"], report["servicios"],
             cost_gold, out)
    return report


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = get_spark(JOB)
    try:
        run(spark, load_settings())
    except ConservationError as exc:
        log.error("%s", exc)
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Job batch Bronze -> Silver (eventos, customers_orgs, resources).

Recomputa Silver completo desde Bronze y lo sobrescribe: idempotente por
construcción, como la reconciliación de D-004. A este volumen (43.200 eventos)
recomputar todo cuesta segundos; acotarlo a las fechas tocadas queda en el
backlog (§12.6, reconciliación incremental).

Uso:
    python -m src.transform.silver_job
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
from src.transform.silver import (conform_events, conform_orgs, conform_resources,
                                  latest_snapshot, split_silver)

log = logging.getLogger("silver_job")
JOB = "silver_job"


class ConservationError(RuntimeError):
    pass


def silver_path(cfg: Settings, entity: str) -> str:
    return f"{cfg.lake}/silver/{entity}"


def run(spark: SparkSession, cfg: Settings) -> dict:
    t0 = time.time()
    b = cfg.bronze
    orgs = conform_orgs(latest_snapshot(spark.read.parquet(f"{b}/customers_orgs")),
                        latest_snapshot(spark.read.parquet(f"{b}/nps_surveys")))
    resources = conform_resources(latest_snapshot(spark.read.parquet(f"{b}/resources")))

    # Maestros: 80 y 400 filas, sin particionar (D-005).
    orgs.coalesce(1).write.mode("overwrite").parquet(silver_path(cfg, "customers_orgs"))
    resources.coalesce(1).write.mode("overwrite").parquet(silver_path(cfg, "resources"))

    bronze_ev = spark.read.parquet(f"{b}/usage_events")
    conformed = conform_events(bronze_ev, spark.read.parquet(silver_path(cfg, "customers_orgs")))
    conformed.cache()
    silver, quarantine = split_silver(conformed)

    # D-005: Silver de eventos por fecha del evento y servicio. repartition por
    # las mismas columnas deja un archivo por partición en lugar de uno por tarea.
    (silver.repartition("event_date", "service").write
           .mode("overwrite").option("partitionOverwriteMode", "static")
           .partitionBy("event_date", "service")
           .parquet(silver_path(cfg, "usage_events")))
    (quarantine.coalesce(1).write.mode("overwrite").option("partitionOverwriteMode", "static")
               .partitionBy("regla").parquet(f"{cfg.quarantine}/silver_usage_events"))

    # --- conteos sobre lo escrito ---
    n_bronze = bronze_ev.count()
    s = spark.read.parquet(silver_path(cfg, "usage_events"))
    n_silver = s.count()
    n_dup = conformed.filter("is_duplicate").count()
    conformed.unpersist()
    if n_bronze != n_silver + n_dup:
        raise ConservationError(f"bronze={n_bronze} != silver={n_silver} + duplicados={n_dup}")

    flags = {r["f"]: r["count"] for r in
             s.select(F.explode("quality_flags").alias("f")).groupBy("f").count().collect()}
    q = spark.read.parquet(f"{cfg.quarantine}/silver_usage_events")
    files = sum(1 for _, _, ns in os.walk(silver_path(cfg, "usage_events"))
                for n in ns if n.endswith(".parquet"))
    report = {
        "job": JOB,
        "ejecutado": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "eventos": {
            "filas_bronze": n_bronze, "filas_silver": n_silver, "duplicados_descartados": n_dup,
            "particiones_event_date_x_service": s.select("event_date", "service").distinct().count(),
            "archivos_parquet": files,
            "filas_con_algun_flag": s.filter(F.size("quality_flags") > 0).count(),
            "flags": dict(sorted(flags.items())),
            "costo_total_usd": round(s.agg(F.sum("cost_usd")).first()[0], 2),
            "costo_de_eventos_con_metrica_nula_usd": round(
                s.filter(F.col("value_num").isNull()).agg(F.sum("cost_usd")).first()[0] or 0, 2),
            "v1_con_carbon_kg": s.filter((F.col("schema_version") == 1)
                                         & F.col("carbon_kg").isNotNull()).count(),
        },
        "quarantine_silver_por_regla": {r["regla"]: r["count"]
                                        for r in q.groupBy("regla").count().collect()},
        "maestros": {
            "customers_orgs": spark.read.parquet(silver_path(cfg, "customers_orgs")).count(),
            "orgs_por_nps_source": {str(r["nps_source"]): r["count"] for r in
                                    spark.read.parquet(silver_path(cfg, "customers_orgs"))
                                    .groupBy("nps_source").count().collect()},
            "resources": spark.read.parquet(silver_path(cfg, "resources")).count(),
        },
        "segundos": round(time.time() - t0, 1),
    }
    os.makedirs(f"{cfg.evidence}/runs", exist_ok=True)
    out = f"{cfg.evidence}/runs/{JOB}_{datetime.now(timezone.utc):%Y-%m-%d_%H%M%S}.json"
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    e = report["eventos"]
    log.info("Silver eventos: %d de %d (dup=%d) · %d particiones · %d con flag %s",
             n_silver, n_bronze, n_dup, e["particiones_event_date_x_service"],
             e["filas_con_algun_flag"], e["flags"])
    log.info("Quarantine Silver: %s · Log -> %s", report["quarantine_silver_por_regla"], out)
    return report


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_settings()
    spark = get_spark(JOB)
    try:
        run(spark, cfg)
    except ConservationError as exc:
        log.error("%s", exc)
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

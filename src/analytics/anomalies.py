"""Componente analítico: cost_anomaly_mart (consigna §4.4 "Anomalías", §7.3; D-010).

Método: MAD (desviación absoluta mediana) sobre la serie diaria de cada
(org_id, service). Cada día se compara con el comportamiento habitual de esa
misma organización en ese mismo servicio:

    mediana = mediana(costo diario de la serie)
    MAD     = mediana(|costo diario - mediana|)
    score   = 0,6745 * (costo - mediana) / MAD
    anomalía si |score| > 3,5           (Iglewicz y Hoaglin)

Por qué así (D-010): media y desvío se contaminan con los picos que se buscan;
mediana y MAD resisten hasta un 50% de valores extremos. Y por serie, porque un
evento de `requests` cuesta 87 veces uno de `cpu_hours`: una línea de base
global marca como anómalo lo que simplemente es caro.

Detalles:
- Se usa el costo bruto (sin créditos). Los créditos se marcan aparte en
  has_credit_adjustment y no compiten con la detección de picos.
- Si la serie tiene menos de 20 días o MAD = 0, la línea de base es la del
  servicio (todas las organizaciones) y se registra en baseline_source.
- La línea de base usa la serie completa. Una ventana móvil es mejor en
  producción y queda en el backlog: con 60 días no cambia el resultado y Spark
  no tiene mediana sobre ventanas móviles sin un self-join.

Uso:
    python -m src.analytics.anomalies
"""

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

log = logging.getLogger("anomalies")
JOB = "anomalies"
MART = "cost_anomaly_mart"
THRESHOLD = 3.5
MIN_DAYS = 20
K = 0.6745
VALUE = "daily_cost_gross_usd"


def _median_and_mad(df: DataFrame, keys: list, prefix: str) -> DataFrame:
    med = df.groupBy(*keys).agg(F.median(VALUE).alias(f"{prefix}_median"),
                                F.count("*").alias(f"{prefix}_days"))
    mad = (df.join(med, keys)
             .groupBy(*keys)
             .agg(F.median(F.abs(F.col(VALUE) - F.col(f"{prefix}_median"))).alias(f"{prefix}_mad")))
    return med.join(mad, keys)


def score_anomalies(mart: DataFrame) -> DataFrame:
    """Gold org_daily_usage_by_service -> cost_anomaly_mart (mismo grano)."""
    serie = _median_and_mad(mart, ["org_id", "service"], "serie")
    servicio = _median_and_mad(mart, ["service"], "servicio")
    use_serie = (F.col("serie_days") >= MIN_DAYS) & (F.col("serie_mad") > 0)
    median = F.when(use_serie, F.col("serie_median")).otherwise(F.col("servicio_median"))
    mad = F.when(use_serie, F.col("serie_mad")).otherwise(F.col("servicio_mad"))
    score = F.when(mad > 0, F.lit(K) * (F.col(VALUE) - median) / mad)
    return (mart.join(serie, ["org_id", "service"]).join(F.broadcast(servicio), "service")
            .select(F.col("org_id"), F.col("usage_date").alias("date"), "service", "org_name",
                    "plan_tier", "daily_cost_usd", F.col(VALUE),
                    median.alias("baseline_median_usd"), mad.alias("baseline_mad_usd"),
                    F.when(mad.isNull() | (mad == 0), "sin_dispersion")
                     .when(use_serie, "serie").otherwise("servicio").alias("baseline_source"),
                    F.col("serie_days").alias("history_days"),
                    F.round(score, 3).alias("anomaly_score"),
                    F.coalesce(F.abs(score) > THRESHOLD, F.lit(False)).alias("is_anomaly"),
                    F.when(score > THRESHOLD, "pico").when(score < -THRESHOLD, "caida")
                     .alias("direction"),
                    (F.col("credit_adjustment_usd") < 0).alias("has_credit_adjustment"),
                    "credit_adjustment_usd", "event_count")
            .withColumn("method", F.lit(f"MAD por (org, service), |score| > {THRESHOLD}")))


def run(spark: SparkSession, cfg: Settings) -> dict:
    t0 = time.time()
    gold = spark.read.parquet(f"{cfg.lake}/gold/org_daily_usage_by_service")
    out_path = f"{cfg.lake}/gold/{MART}"
    (score_anomalies(gold).repartition("date").write.mode("overwrite")
        .option("partitionOverwriteMode", "static").partitionBy("date").parquet(out_path))
    a = spark.read.parquet(out_path).cache()

    n, n_anom = a.count(), a.filter("is_anomaly").count()
    # Comparación de métodos sobre los mismos datos (respalda D-010)
    st = gold.agg(F.mean(VALUE).alias("mu"), F.stddev(VALUE).alias("sd"),
                  F.median(VALUE).alias("med")).first()
    gmad = gold.agg(F.median(F.abs(F.col(VALUE) - st["med"]))).first()[0]
    z_global_df = gold.filter(F.abs((F.col(VALUE) - st["mu"]) / st["sd"]) > 3)
    mad_global_df = gold.filter(F.abs(K * (F.col(VALUE) - st["med"]) / gmad) > THRESHOLD)
    z_global, mad_global = z_global_df.count(), mad_global_df.count()

    # Sensibilidad: picos inyectados en el dataset (eventos > USD 50; mediana USD 1).
    silver = spark.read.parquet(f"{cfg.lake}/silver/usage_events")
    spikes = (silver.filter(F.col("cost_usd") > 50)
              .select("org_id", F.col("event_date").alias("date"), "service").distinct())
    detected = spikes.join(a.filter("is_anomaly"), ["org_id", "date", "service"]).count()
    n_spikes = spikes.count()

    def hits(df):
        return spikes.join(df.withColumnRenamed("usage_date", "date"),
                           ["org_id", "date", "service"]).count()

    top = (a.filter("is_anomaly").orderBy(F.desc("anomaly_score")).limit(10)
           .select("org_id", "date", "service", "daily_cost_gross_usd", "baseline_median_usd",
                   "anomaly_score").collect())
    report = {
        "job": JOB, "mart": MART, "metodo": f"MAD por (org_id, service), |score| > {THRESHOLD}",
        "filas": n, "series": a.select("org_id", "service").distinct().count(),
        "linea_de_base": {r["baseline_source"]: r["count"]
                          for r in a.groupBy("baseline_source").count().collect()},
        "anomalias": n_anom, "pct_anomalias": round(100 * n_anom / n, 2),
        "por_direccion": {str(r["direction"]): r["count"]
                          for r in a.filter("is_anomaly").groupBy("direction").count().collect()},
        "por_servicio": {r["service"]: r["count"]
                         for r in a.filter("is_anomaly").groupBy("service").count().collect()},
        "dias_con_credito": a.filter("has_credit_adjustment").count(),
        "comparacion_metodos_mismo_mart": {
            "z_score_global_|z|>3": {"marcadas": z_global, "picos_detectados": hits(z_global_df)},
            "mad_global_|score|>3.5": {"marcadas": mad_global,
                                       "picos_detectados": hits(mad_global_df)},
            "mad_por_serie_|score|>3.5 (elegido)": {"marcadas": n_anom,
                                                    "picos_detectados": detected},
        },
        "sensibilidad_picos_inyectados": {
            "celdas_con_evento_>50usd": n_spikes,
            "marcadas_como_anomalia": detected,
            "pct_detectado": round(100 * detected / n_spikes, 1) if n_spikes else None,
        },
        "top_10": [{k: (str(v) if k == "date" else (round(v, 2) if isinstance(v, float) else v))
                    for k, v in r.asDict().items()} for r in top],
        "segundos": round(time.time() - t0, 1),
    }
    a.unpersist()
    os.makedirs(f"{cfg.evidence}/runs", exist_ok=True)
    out = f"{cfg.evidence}/runs/{JOB}_{datetime.now(timezone.utc):%Y-%m-%d_%H%M%S}.json"
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    log.info("%s: %d anomalías de %d celdas (%.2f%%). Picos inyectados detectados: %s/%s. "
             "Log -> %s", MART, n_anom, n, report["pct_anomalias"], detected, n_spikes, out)
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

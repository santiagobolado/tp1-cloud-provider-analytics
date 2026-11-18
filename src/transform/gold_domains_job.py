"""Job batch Silver -> Gold de los marts de dominio (revenue, tickets, GenAI).

Recomputa los tres marts desde Silver y sobrescribe: idempotente por
construcción, como gold_job. Cada mart pasa dos controles antes de darse por
bueno; si alguno falla, el job termina con error y no se publica evidencia:

  - grano: una fila por clave (filas = claves distintas);
  - conservación: los totales del mart coinciden con los de Silver.

Uso:
    python -m src.transform.gold_domains_job
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
from src.transform.gold_domains import MARTS, genai_events

log = logging.getLogger("gold_domains_job")
JOB = "gold_domains_job"

# Particiones (D-005): revenue por mes (3) y GenAI por día, como el mart diario.
# Tickets va sin particionar: ~1.000 filas en 115 días serían archivos diminutos.
PARTITION = {"revenue_by_org_month": "month", "genai_tokens_by_org_date": "usage_date"}


class ConservationError(RuntimeError):
    pass


def gold_path(cfg: Settings, mart: str) -> str:
    return f"{cfg.lake}/gold/{mart}"


def _sum(df: DataFrame, col: str) -> float:
    return df.agg(F.sum(col)).first()[0] or 0


def _check(mart: str, name: str, gold_value, silver_value, tol: float = 0.0) -> None:
    if abs(gold_value - silver_value) > tol:
        raise ConservationError(f"{mart}: {name} gold={gold_value} != silver={silver_value}")


def conservation(mart: str, g: DataFrame, silver: DataFrame) -> dict:
    """Controles de conservación por mart; devuelve los totales verificados."""
    n = g.count()
    if n != g.select(*MARTS[mart][2]).distinct().count():
        raise ConservationError(f"{mart}: grano roto ({n} filas con claves repetidas)")
    if mart == "revenue_by_org_month":
        # el mart redondea a centavos por celda: tolerancia de medio centavo por fila
        _check(mart, "net_revenue_usd", _sum(g, "net_revenue_usd"), _sum(silver, "net_revenue_usd"), 0.005 * n)
        _check(mart, "facturas", _sum(g, "invoice_count"), silver.count())
        totals = {"net_revenue_usd": round(_sum(g, "net_revenue_usd"), 2),
                  "total_billed_usd": round(_sum(g, "total_billed_usd"), 2),
                  "credits_usd": round(_sum(g, "credits_usd"), 2),
                  "taxes_usd": round(_sum(g, "taxes_usd"), 2),
                  "facturas": silver.count()}
    elif mart == "tickets_by_org_date":
        _check(mart, "tickets", _sum(g, "ticket_count"), silver.count())
        _check(mart, "sla_breached", _sum(g, "sla_breached_count"), silver.filter("sla_breached").count())
        _check(mart, "csat_responses", _sum(g, "csat_responses"), silver.filter(F.col("csat").isNotNull()).count())
        totals = {"tickets": silver.count(),
                  "criticos": int(_sum(g.filter(F.col("severity") == "critical"), "ticket_count")),
                  "sla_breached": int(_sum(g, "sla_breached_count")),
                  "abiertos": int(_sum(g, "open_count")),
                  "csat_responses": int(_sum(g, "csat_responses"))}
    else:
        src = genai_events(silver)
        _check(mart, "genai_tokens", _sum(g, "genai_tokens"), _sum(src, "genai_tokens"))
        _check(mart, "eventos", _sum(g, "event_count"), src.count())
        _check(mart, "genai_cost_usd", _sum(g, "genai_cost_usd"), _sum(src, "cost_usd"), 0.0001 * n)
        totals = {"genai_tokens": int(_sum(g, "genai_tokens")),
                  "genai_cost_usd": round(_sum(g, "genai_cost_usd"), 2),
                  "eventos": src.count(),
                  "primer_dia": str(g.agg(F.min("usage_date")).first()[0])}
    return {"filas": n, "orgs": g.select("org_id").distinct().count(), **totals}


def run(spark: SparkSession, cfg: Settings) -> dict:
    t0 = time.time()
    report = {"job": JOB, "ejecutado": datetime.now(timezone.utc).isoformat(timespec="seconds"),
              "marts": {}}
    for mart, (build, entity, _) in MARTS.items():
        silver = spark.read.parquet(f"{cfg.lake}/silver/{entity}")
        df = build(silver)
        part = PARTITION.get(mart)
        if part:
            (df.repartition(part).write.mode("overwrite")
               .option("partitionOverwriteMode", "static").partitionBy(part)
               .parquet(gold_path(cfg, mart)))
        else:
            df.coalesce(1).write.mode("overwrite").parquet(gold_path(cfg, mart))
        g = spark.read.parquet(gold_path(cfg, mart))
        r = conservation(mart, g, silver)
        r["archivos_parquet"] = sum(1 for _, _, ns in os.walk(gold_path(cfg, mart))
                                    for x in ns if x.endswith(".parquet"))
        report["marts"][mart] = r
        log.info("Gold %s: %s", mart, r)
    report["segundos"] = round(time.time() - t0, 1)
    os.makedirs(f"{cfg.evidence}/runs", exist_ok=True)
    out = f"{cfg.evidence}/runs/{JOB}_{datetime.now(timezone.utc):%Y-%m-%d_%H%M%S}.json"
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    log.info("Log -> %s", out)
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

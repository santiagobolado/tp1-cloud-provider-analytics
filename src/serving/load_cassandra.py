"""Carga del mart FinOps a AstraDB / Cassandra (consigna §6.2 "Serving").

Pasos: crea las tablas si no existen (infra/cql/schema.cql), arma P1 y P2 desde
Gold y las carga con upsert. Con --with-speed primero carga la capa rápida
(source = 'streaming') y después el mart reconciliado la pisa: es la fusión de
las dos rutas de D-015, resuelta por el upsert de Cassandra.

Idempotencia: el log registra filas enviadas y COUNT(*) antes y después.
Re-ejecutar no cambia el conteo.

Uso:
    python -m src.serving.load_cassandra
    python -m src.serving.load_cassandra --with-speed
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

from src.common.config import load_settings
from src.common.spark import get_spark
from src.serving import cassandra_io as cio
from src.serving.tables import top_services_table, usage_table

log = logging.getLogger("load_cassandra")
JOB = "load_cassandra"


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--with-speed", action="store_true",
                   help="cargar primero la capa rápida y después el reconciliado")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        params = cio.connection_params()
    except cio.ServingConfigError as exc:
        log.error("%s", exc)
        return 1

    cfg = load_settings()
    spark = get_spark(JOB)
    t0 = time.time()
    try:
        tables = cio.apply_schema(params)
        log.info("Tablas listas en %s/%s: %s", params["mode"], params["keyspace"], tables)
        before = {t: cio.count_rows(params, t) for t in tables}
        gold = spark.read.parquet(f"{cfg.lake}/gold/org_daily_usage_by_service")
        sent = {}
        if args.with_speed:
            from src.transform.gold_stream import latest_view
            sent["speed->org_daily_usage_by_service"] = cio.load(
                usage_table(latest_view(spark, cfg)), "org_daily_usage_by_service", params)
        sent["org_daily_usage_by_service"] = cio.load(
            usage_table(gold), "org_daily_usage_by_service", params)
        sent["top_services_by_org_14d"] = cio.load(
            top_services_table(gold), "top_services_by_org_14d", params)
        after = {t: cio.count_rows(params, t) for t in tables}
    finally:
        spark.stop()

    report = {"job": JOB, "destino": params["mode"], "keyspace": params["keyspace"],
              "ejecutado": datetime.now(timezone.utc).isoformat(timespec="seconds"),
              "filas_enviadas": sent, "count_antes": before, "count_despues": after,
              "segundos": round(time.time() - t0, 1)}
    os.makedirs(f"{cfg.evidence}/runs", exist_ok=True)
    out = f"{cfg.evidence}/runs/{JOB}_{datetime.now(timezone.utc):%Y-%m-%d_%H%M%S}.json"
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    log.info("Enviadas %s · COUNT antes %s · después %s · Log -> %s", sent, before, after, out)
    return 0


if __name__ == "__main__":
    sys.exit(main())

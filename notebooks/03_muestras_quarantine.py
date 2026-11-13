"""
Entrega 2 - Muestras verificables de quarantine (consigna §6.2 "Generar
quarantine con muestras" y §6.4 "ejemplos verificables").

data/ no se versiona, así que este script exporta a evidence/ una muestra de
cada regla para que se pueda revisar sin correr el pipeline.

  Parte 1 · Silver (dataset real): lee data/quarantine/silver_usage_events,
            que genera `python -m src.transform.silver_job`.
  Parte 2 · Bronze (demostración): el dataset no trae filas que rompan el
            esquema o la clave, así que se copia Landing a un directorio
            temporal, se le agregan filas sucias conocidas y se corre la misma
            ingesta. Landing real no se modifica.

Uso:
    python notebooks/03_muestras_quarantine.py
Salida:
    evidence/quarantine_muestras.md
"""

import json
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F  # noqa: E402

from src.common.config import load_settings  # noqa: E402
from src.common.spark import get_spark  # noqa: E402
from src.ingest.batch_to_bronze import run as run_batch  # noqa: E402
from src.ingest.stream_to_bronze import run as run_stream  # noqa: E402

LAKE = os.environ.get("LAKE_PATH", "data")
LANDING = os.environ.get("LANDING_PATH", "data/landing")
EVIDENCE = os.environ.get("EVIDENCE_PATH", "evidence")
N = 3

SILVER_COLS = ["event_id", "org_id", "service", "metric", "value_raw", "unit", "cost_usd",
               "quality_flags"]

out = []


def w(s=""):
    out.append(s)


def table(rows, cols):
    w("| " + " | ".join(cols) + " |")
    w("|" + "---|" * len(cols))
    for r in rows:
        w("| " + " | ".join(str(r.get(c, "")).replace("|", "\\|") for c in cols) + " |")
    w()


spark = get_spark("muestras-quarantine")

w("# Muestras de quarantine")
w(f"\nGenerado: {datetime.now(timezone.utc).isoformat(timespec='seconds')} · "
  "`python notebooks/03_muestras_quarantine.py`\n")
w("Quarantine guarda, por regla, el registro y el motivo. En Bronze, lo que va a")
w("quarantine **no entra** a la zona. En Silver es una **copia de auditoría**: el evento")
w("sigue en Silver con su flag (D-009, D-014).\n")

# --- Parte 1: Silver, dataset real -------------------------------------------
q = spark.read.parquet(f"{LAKE}/quarantine/silver_usage_events")
silver = spark.read.parquet(f"{LAKE}/silver/usage_events")
w("## 1. Silver · dataset real\n")
counts = {r["regla"]: r["count"] for r in q.groupBy("regla").count().collect()}
w("| Regla | Registros | Tratamiento |")
w("|---|---|---|")
trat = {"value_nulo": "métrica en nulo, el costo promueve (D-009)",
        "unit_nulo_con_value": "unidad imputada desde la métrica (D-014)"}
def miles(n):
    return f"{n:,}".replace(",", ".")


for regla, n in sorted(counts.items()):
    w(f"| `{regla}` | {miles(n)} | {trat.get(regla, '')} |")
neg = silver.filter("is_credit_adjustment").count()
w(f"| `costo_negativo` | {miles(neg)} | flag de negocio, sin copia en quarantine (§8.7) |")
w()
for regla in sorted(counts):
    ids = [r["event_id"] for r in q.filter(F.col("regla") == regla)
           .orderBy("event_id").limit(N).collect()]
    rows = (silver.filter(F.col("event_id").isin(ids)).orderBy("event_id")
            .select(*SILVER_COLS).collect())
    w(f"### `{regla}` · {N} de {miles(counts[regla])} (cómo quedaron en Silver)\n")
    table([{**r.asDict(), "quality_flags": ", ".join(r["quality_flags"])} for r in rows],
          SILVER_COLS)
rows = (silver.filter("is_credit_adjustment").orderBy("cost_usd").limit(N)
        .select(*SILVER_COLS).collect())
w(f"### `costo_negativo` · los {N} más negativos (flag, no quarantine)\n")
table([{**r.asDict(), "quality_flags": ", ".join(r["quality_flags"])} for r in rows], SILVER_COLS)

# --- Parte 2: Bronze, demostración con filas sucias ----------------------------
tmp = tempfile.mkdtemp(prefix="quarantine_demo_")
try:
    land = os.path.join(tmp, "landing")
    shutil.copytree(LANDING, land, ignore=shutil.ignore_patterns("usage_events_stream"))
    with open(os.path.join(land, "customers_orgs.csv"), "a", encoding="utf-8") as fh:
        fh.write("org_demo_1,Fecha Rota,Media,us-east,pro,True,2025-13-45,rep_a,active,organic,10.0\n")
        fh.write(",Sin Clave,Media,us-east,pro,True,2025-06-01,rep_a,active,organic,10.0\n")
        fh.write("org_xaji0y6d,Duplicada,Media,us-east,pro,True,2025-06-01,rep_a,active,organic,10.0\n")
    ev = os.path.join(land, "usage_events_stream")
    os.makedirs(ev)
    base = {"event_id": "evt_demo_ok", "timestamp": "2025-08-01T10:00:00Z", "org_id": "org_xaji0y6d",
            "resource_id": "res_x", "service": "compute", "region": "us-east", "metric": "requests",
            "value": 1.0, "unit": "count", "cost_usd_increment": 1.0, "schema_version": 2,
            "carbon_kg": 0.1}
    with open(os.path.join(ev, "events_part_demo.jsonl"), "w", encoding="utf-8") as fh:
        fh.write(json.dumps(base) + "\n")
        fh.write(json.dumps({**base, "event_id": None}) + "\n")
        fh.write(json.dumps({**base, "event_id": "evt_demo_ts", "timestamp": "ayer"}) + "\n")
        fh.write('{"event_id": "evt_demo_roto", "timestamp": \n')
    cfg = load_settings(landing=land, lake=os.path.join(tmp, "lake"),
                        evidence=os.path.join(tmp, "ev"), checkpoints=os.path.join(tmp, "ck"),
                        ingest_date="2026-11-13")
    run_batch(spark, cfg, ["customers_orgs"])
    run_stream(spark, cfg)

    w("## 2. Bronze · demostración con filas sucias conocidas\n")
    w("El dataset real no tiene rechazos en Bronze (0 en las 8 fuentes). Para mostrar que")
    w("las reglas funcionan, se agregan estas filas a una **copia temporal** de Landing:\n")
    cols = ["entity", "regla", "detalle", "raw_record", "record_json"]
    qb = spark.read.parquet(f"{cfg.quarantine}/customers_orgs/ingest_date={cfg.ingest_date}")
    w("### `customers_orgs.csv` · 3 filas agregadas, 3 en quarantine, 80 en Bronze\n")
    table([{k: (v[:90] + "…" if isinstance(v, str) and len(v) > 90 else v)
            for k, v in r.asDict().items()} for r in qb.orderBy("regla").select(*cols).collect()],
          cols)
    qe = spark.read.parquet(f"{cfg.quarantine}/usage_events")
    w("### `usage_events_stream` · 4 líneas: 1 válida a Bronze, 3 en quarantine\n")
    cols = ["entity", "regla", "raw_record", "record_json"]
    table([{k: (v[:90] + "…" if isinstance(v, str) and len(v) > 90 else v)
            for k, v in r.asDict().items()} for r in qe.orderBy("regla").select(*cols).collect()],
          cols)
finally:
    shutil.rmtree(tmp, ignore_errors=True)
    spark.stop()

os.makedirs(EVIDENCE, exist_ok=True)
with open(f"{EVIDENCE}/quarantine_muestras.md", "w", encoding="utf-8") as fh:
    fh.write("\n".join(out) + "\n")
print(f"[OK] {EVIDENCE}/quarantine_muestras.md")

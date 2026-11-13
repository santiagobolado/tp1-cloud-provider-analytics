"""
Trazado de linaje de punta a punta (consigna §6.2 "linaje"; §5.4 del TP1).

Toma una celda del mart de anomalías y la sigue hacia atrás, zona por zona,
usando solo las columnas de linaje del pipeline (source_file, ingest_ts,
last_reconciled_at) hasta la línea exacta del archivo de Landing.

Celda por defecto: la anomalía org_okep7y6w · 2025-08-07 · compute (USD 191,90),
que además aparece en la consulta Q1 de AstraDB.

Uso:
    python notebooks/05_trazado_linaje.py [org_id fecha servicio]
Salida:
    evidence/linaje_trazado.md
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F  # noqa: E402

from src.common.spark import get_spark  # noqa: E402

LAKE = os.environ.get("LAKE_PATH", "data")
ORG, DAY, SVC = (sys.argv[1:4] if len(sys.argv) >= 4
                 else ("org_okep7y6w", "2025-08-07", "compute"))

spark = get_spark("linaje")
out = []


def w(s=""):
    out.append(s)


def rows_table(rows, cols):
    w("| " + " | ".join(cols) + " |")
    w("|" + "---|" * len(cols))
    for r in rows:
        w("| " + " | ".join(str(r[c]) for c in cols) + " |")
    w()


w(f"# Trazado de linaje · {ORG} · {DAY} · {SVC}")
w("\nGenerado con `python notebooks/05_trazado_linaje.py`. Cada paso usa solo columnas")
w("de linaje del pipeline; ninguna búsqueda es manual.\n")

a = (spark.read.parquet(f"{LAKE}/gold/cost_anomaly_mart")
     .filter((F.col("org_id") == ORG) & (F.col("date") == DAY) & (F.col("service") == SVC)))
w("## 1. Gold · `cost_anomaly_mart`\n")
rows_table(a.collect(), ["org_id", "date", "service", "daily_cost_gross_usd",
                         "baseline_median_usd", "anomaly_score", "is_anomaly"])

g = (spark.read.parquet(f"{LAKE}/gold/org_daily_usage_by_service")
     .filter((F.col("org_id") == ORG) & (F.col("usage_date") == DAY) & (F.col("service") == SVC)))
w("## 2. Gold · `org_daily_usage_by_service` (el mart del que sale la anomalía)\n")
rows_table(g.collect(), ["org_id", "usage_date", "service", "daily_cost_usd", "event_count",
                         "source", "last_reconciled_at"])
w("Esta misma fila está en AstraDB: `evidence/serving/` (Q1, primera fila) y "
  "`evidence/serving/q1_cql_console.png`.\n")

s = (spark.read.parquet(f"{LAKE}/silver/usage_events")
     .filter((F.col("org_id") == ORG) & (F.col("event_date") == DAY) & (F.col("service") == SVC))
     .orderBy("event_ts"))
ev = s.collect()
total = sum(r["cost_usd"] for r in ev)
w(f"## 3. Silver · `usage_events`: los {len(ev)} eventos que suman USD {total:.4f}\n")
rows_table(ev, ["event_id", "event_ts", "metric", "value_num", "cost_usd", "quality_flags"])

ids = [r["event_id"] for r in ev]
b = (spark.read.parquet(f"{LAKE}/bronze/usage_events").filter(F.col("event_id").isin(ids))
     .orderBy("event_ts"))
br = [{**r.asDict(), "source_file": "data/landing/" + r["source_file"].split("data/landing/")[-1]}
      for r in b.collect()]
w("Las tres llegaron en archivos distintos: la fragmentación aleatoria de D-012 en un caso concreto.\n"
  if len({r["source_file"] for r in br}) > 1 else "")
w("## 4. Bronze · `usage_events`: cuándo y desde qué archivo llegó cada evento\n")
rows_table(br, ["event_id", "value", "cost_usd_increment", "ingest_date", "ingest_ts",
                "source_file"])

w("## 5. Landing: la línea exacta en el archivo original\n")
w("| event_id | archivo | línea | contenido (inicio) |")
w("|---|---|---|---|")
for r in br:
    path = r["source_file"]
    with open(path, encoding="utf-8") as fh:
        for n, line in enumerate(fh, 1):
            if f'"{r["event_id"]}"' in line:
                w(f"| {r['event_id']} | `{os.path.basename(path)}` | {n} | "
                  f"`{line.strip()[:110]}…` |")
                break
w()
spikes = [r for r in ev if r["cost_usd"] > 50]
if spikes:
    w(f"**Lectura:** la anomalía la explica el evento `{spikes[0]['event_id']}` "
      f"(USD {spikes[0]['cost_usd']:.2f}), uno de los picos inyectados en el dataset.")

spark.stop()
os.makedirs("evidence", exist_ok=True)
with open("evidence/linaje_trazado.md", "w", encoding="utf-8") as fh:
    fh.write("\n".join(out) + "\n")
print("[OK] evidence/linaje_trazado.md")

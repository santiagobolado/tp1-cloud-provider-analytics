"""
Genera docs/diccionario_datos.md desde el catálogo (src/governance/catalog.py).

Tipos: de los Parquet reales en data/ si existen; si no, de las funciones del
pipeline sobre DataFrames vacíos. Tablas de Cassandra: de infra/cql/schema.cql.
Falla si alguna columna no está documentada (mismo control que el test).

Uso:
    python notebooks/04_diccionario_datos.py
"""

import os
import re
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.common.spark import get_spark  # noqa: E402
from src.governance.catalog import DATASETS, describe  # noqa: E402
from src.governance.schemas import expected_columns  # noqa: E402

LAKE = os.environ.get("LAKE_PATH", "data")
OUT = "docs/diccionario_datos.md"


def cql_types() -> dict:
    text = open("infra/cql/schema.cql", encoding="utf-8").read()
    out = {}
    for name, body in re.findall(r"CREATE TABLE IF NOT EXISTS (\w+) \((.*?)\n\)", text, re.S):
        for line in body.splitlines():
            m = re.match(r"\s+(\w+)\s+(\w+)", re.sub(r"--.*", "", line))
            if m and m.group(1) != "PRIMARY":
                out[(name, m.group(1))] = m.group(2)
    return out


spark = get_spark("diccionario")
expected = expected_columns(spark)
cql = cql_types()
missing = []
lines = ["# Diccionario de datos", "",
         f"Generado: {datetime.now(timezone.utc).isoformat(timespec='seconds')} desde "
         "`src/governance/catalog.py` con `python notebooks/04_diccionario_datos.py`.",
         "No editar a mano: se regenera, y `tests/test_governance.py` falla si una columna",
         "del pipeline no tiene descripción.", "",
         "| Dataset | Zona | Grano | Partición | Responsable | Retención |", "|---|---|---|---|---|---|"]
for name, d in DATASETS.items():
    lines.append(f"| [`{name}`](#{name.replace('.', '')}) | {d['zone']} | {d['grain']} | "
                 f"{d['partition']} | {d['owner']} | {d['retention']} |")
lines.append("")

for name, d in DATASETS.items():
    types = {}
    path = f"{LAKE}/{d['path']}"
    if name.startswith("cassandra."):
        table = d["path"].split(".")[1]
        types = {c: t for (tb, c), t in cql.items() if tb == table}
    elif os.path.isdir(path):
        try:
            types = dict(spark.read.parquet(path).dtypes)
        except Exception:                                        # quarantine vacía
            pass
    lines += [f"## {name}", "", d["purpose"], "",
              f"- **Ruta:** `{d['path']}` · **Zona:** {d['zone']} · **Grano:** {d['grain']}",
              f"- **Produce:** `{d['produced_by']}` · **Linaje (origen):** "
              + ", ".join(f"`{s}`" for s in d["sources"]),
              f"- **Partición:** {d['partition']} · **Retención:** {d['retention']} · "
              f"**Responsable:** {d['owner']}", "",
              "| Columna | Tipo | Descripción |", "|---|---|---|"]
    for c in expected[name]:
        desc = describe(name, c)
        if not desc:
            missing.append(f"{name}.{c}")
        lines.append(f"| `{c}` | {types.get(c, '—')} | {desc or '**SIN DOCUMENTAR**'} |")
    lines.append("")

spark.stop()
with open(OUT, "w", encoding="utf-8") as fh:
    fh.write("\n".join(lines) + "\n")
if missing:
    sys.exit(f"Columnas sin documentar: {missing}")
print(f"[OK] {OUT}: {len(DATASETS)} datasets, "
      f"{sum(len(v) for v in expected.values())} columnas documentadas")

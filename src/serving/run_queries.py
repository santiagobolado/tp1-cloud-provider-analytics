"""Ejecuta las consultas de infra/cql/queries.cql y guarda la evidencia.

Para cada consulta: el CQL, las filas devueltas y la latencia (objetivo O6:
< 1 s). Salida en evidence/serving/consultas_<fecha>.md, lista para el anexo C.

Uso:
    python -m src.serving.run_queries
"""

import logging
import os
import re
import sys
import time
from datetime import datetime, timezone

from src.common.config import load_settings
from src.serving import cassandra_io as cio

log = logging.getLogger("run_queries")


def titled_queries() -> list:
    """[(título, cql)] a partir de los comentarios '-- Qn ...' del archivo."""
    with open(os.path.join(cio.CQL_DIR, "queries.cql"), encoding="utf-8") as fh:
        text = fh.read()
    out = []
    for block in re.split(r"\n(?=-- Q\d)", text):
        m = re.match(r"-- (Q\d[^\n]*)", block)
        if m:
            cql = re.sub(r"--[^\n]*", "", block).strip().rstrip(";")
            out.append((m.group(1).strip(), " ".join(cql.split())))
    return out


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        params = cio.connection_params()
    except cio.ServingConfigError as exc:
        log.error("%s", exc)
        return 1
    cfg = load_settings()
    cluster, session = cio.connect(params)
    lines = [f"# Consultas CQL sobre {params['mode']} · keyspace `{params['keyspace']}`",
             f"\nEjecutado: {datetime.now(timezone.utc).isoformat(timespec='seconds')}\n"]
    try:
        for title, cql in titled_queries():
            session.execute(cql)                                  # calentamiento
            t0 = time.perf_counter()
            rows = list(session.execute(cql))
            ms = (time.perf_counter() - t0) * 1000
            log.info("%s · %d filas · %.0f ms", title, len(rows), ms)
            lines += [f"## {title}", "", "```sql", cql, "```", "",
                      f"**{len(rows)} filas · {ms:.0f} ms** (objetivo O6: < 1000 ms)", ""]
            if rows:
                cols = rows[0]._fields
                lines += ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
                lines += ["| " + " | ".join(str(round(v, 4) if isinstance(v, float) else v)
                                            for v in r) + " |" for r in rows]
            lines.append("")
    finally:
        cluster.shutdown()
    os.makedirs(f"{cfg.evidence}/serving", exist_ok=True)
    out = f"{cfg.evidence}/serving/consultas_{datetime.now(timezone.utc):%Y-%m-%d_%H%M%S}.md"
    with open(out, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    log.info("Evidencia -> %s", out)
    return 0


if __name__ == "__main__":
    sys.exit(main())

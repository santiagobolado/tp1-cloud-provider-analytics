"""Corre el pipeline completo en orden, con un solo comando.

Cada paso es el mismo módulo que se puede correr por separado (README): acá se
ejecutan como subprocesos para que cada uno tenga su propia SparkSession y el
pipeline se detenga en el primer paso que falle.

Uso:
    python -m src.pipeline                   # pasos 1 a 5 (sin serving)
    python -m src.pipeline --reset           # borra zonas derivadas y checkpoints antes
    python -m src.pipeline --with-serving    # agrega la carga a AstraDB y las consultas
"""

import argparse
import os
import shutil
import subprocess
import sys
import time

from src.common.config import load_settings

STEPS = [
    ("1. Batch -> Bronze", "src.ingest.batch_to_bronze", []),
    ("2. Streaming -> Bronze", "src.ingest.stream_to_bronze", []),
    ("3. Bronze -> Silver", "src.transform.silver_job", []),
    ("4a. Gold, capa rápida (flujo C)", "src.transform.gold_stream", []),
    ("4b. Gold, reconciliación (flujo D)", "src.transform.gold_job", []),
    ("5. Anomalías", "src.analytics.anomalies", []),
]
SERVING = [
    ("6a. Carga a AstraDB", "src.serving.load_cassandra", ["--with-speed"]),
    ("6b. Consultas CQL", "src.serving.run_queries", []),
]


def reset(cfg) -> None:
    """Las zonas derivadas y los checkpoints se regeneran desde Landing (inmutable).
    Borrar checkpoints sin borrar las zonas duplicaría eventos: se borra todo junto."""
    for path in [f"{cfg.lake}/{z}" for z in ("bronze", "silver", "gold", "quarantine")] + [cfg.checkpoints]:
        shutil.rmtree(path, ignore_errors=True)
    print(f"[reset] zonas derivadas y {cfg.checkpoints} borrados; Landing intacto")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--reset", action="store_true", help="empezar desde cero")
    p.add_argument("--with-serving", action="store_true", help="incluir AstraDB (paso 6)")
    args = p.parse_args(argv)
    cfg = load_settings()
    if args.reset:
        reset(cfg)
    steps = STEPS + (SERVING if args.with_serving else [])
    t0 = time.time()
    for name, module, extra in steps:
        print(f"\n=== {name} ({module})", flush=True)
        t = time.time()
        rc = subprocess.call([sys.executable, "-m", module, *extra], env=os.environ.copy())
        if rc != 0:
            print(f"[ERROR] {name} terminó con código {rc}. Pipeline detenido.")
            return rc
        print(f"=== {name}: OK en {time.time() - t:.0f} s", flush=True)
    print(f"\n[OK] Pipeline completo en {time.time() - t0:.0f} s. Logs en {cfg.evidence}/runs/")
    return 0


if __name__ == "__main__":
    sys.exit(main())

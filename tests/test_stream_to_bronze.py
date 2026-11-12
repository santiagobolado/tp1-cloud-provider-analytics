"""Pruebas del streaming usage_events_stream -> Bronze.

Usan 3 archivos reales en un Landing temporal para que corran rápido.
"""

import json
import os
import shutil

import pytest
from pyspark.sql import functions as F

from src.common.config import load_settings
from src.ingest.stream_to_bronze import run

FILES = ["events_part_0000.jsonl", "events_part_0001.jsonl", "events_part_0104.jsonl"]


@pytest.fixture()
def env(tmp_path, landing):
    src = os.path.join(landing, "usage_events_stream")
    dst = tmp_path / "landing" / "usage_events_stream"
    dst.mkdir(parents=True)
    for f in FILES:
        shutil.copy(os.path.join(src, f), dst / f)
    cfg = load_settings(landing=str(tmp_path / "landing"), lake=str(tmp_path / "lake"),
                        evidence=str(tmp_path / "evidence"),
                        checkpoints=str(tmp_path / "checkpoints"))
    return cfg, dst, src


def _bronze(spark, cfg):
    return spark.read.parquet(f"{cfg.bronze}/usage_events")


def test_un_archivo_por_microlote_y_nada_se_pierde(spark, env):
    cfg, _, _ = env
    r = run(spark, cfg)
    b = r["esta_corrida"]["bronze"]
    assert b["micro_lotes_con_datos"] == 3
    assert b["filas_por_lote"] == [360]
    assert b["descartadas_por_watermark"] == 0       # D-013: Bronze no descarta tardíos
    assert r["acumulado"]["filas_bronze"] == 1080


def test_reejecutar_sin_archivos_nuevos_no_hace_nada(spark, env):
    cfg, _, _ = env
    run(spark, cfg)
    r2 = run(spark, cfg)
    assert r2["esta_corrida"]["bronze"]["filas_entrada"] == 0
    assert r2["acumulado"]["filas_bronze"] == 1080


def test_reentrega_de_un_archivo_se_deduplica_por_event_id(spark, env):
    """Mismo contenido con otro nombre: el checkpoint no lo conoce, el dedupe sí."""
    cfg, dst, src = env
    run(spark, cfg)
    shutil.copy(os.path.join(src, FILES[0]), dst / "events_part_9999_reenvio.jsonl")
    r2 = run(spark, cfg)
    assert r2["esta_corrida"]["bronze"]["filas_entrada"] == 360    # se leyó...
    assert r2["acumulado"]["filas_bronze"] == 1080                 # ...pero no se escribió
    assert r2["acumulado"]["event_id_distintos_bronze"] == 1080


def test_eventos_invalidos_van_a_quarantine_con_motivo(spark, env):
    cfg, dst, _ = env
    good = {"event_id": "evt_ok", "timestamp": "2025-08-01T10:00:00Z", "org_id": "org_x",
            "resource_id": "res_x", "service": "compute", "region": "us-east",
            "metric": "requests", "value": 1.0, "unit": "count",
            "cost_usd_increment": 1.0, "schema_version": 2, "carbon_kg": 0.1}
    with open(dst / "events_part_9998_sucio.jsonl", "w", encoding="utf-8") as fh:
        fh.write(json.dumps(good) + "\n")
        fh.write(json.dumps({**good, "event_id": None}) + "\n")
        fh.write(json.dumps({**good, "event_id": "evt_bad_ts", "timestamp": "ayer"}) + "\n")
        fh.write('{"event_id": "evt_roto", "timestamp": \n')
    r = run(spark, cfg)
    assert r["acumulado"]["quarantine_por_regla"] == {
        "event_id_nulo": 1, "timestamp_invalido": 1, "no_parseable": 1}
    assert r["acumulado"]["filas_bronze"] == 1080 + 1
    q = spark.read.parquet(f"{cfg.quarantine}/usage_events")
    assert '"evt_roto"' in q.filter(F.col("regla") == "no_parseable").first()["raw_record"]


def test_bronze_conserva_el_dato_crudo_y_las_dos_versiones(spark, env):
    cfg, _, _ = env
    run(spark, cfg)
    b = _bronze(spark, cfg)
    assert dict(b.dtypes)["value"] == "string"                     # D-003
    assert b.filter(F.col("value") == "8.9427").count() == 1       # vino como texto en 0000
    v1 = b.filter(F.col("schema_version") == 1)
    assert v1.count() > 0 and v1.filter(F.col("carbon_kg").isNotNull()).count() == 0
    assert b.filter(F.col("source_file").contains("events_part_")).count() == 1080
    assert b.select("ingest_ts").distinct().count() == 3           # un sello por micro-lote

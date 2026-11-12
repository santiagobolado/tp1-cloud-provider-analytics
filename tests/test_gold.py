"""Pruebas del mart org_daily_usage_by_service (streaming + reconciliación)."""

import json
import os
import shutil
from datetime import date

import pytest
from pyspark.sql import functions as F

from src.common.config import load_settings
from src.ingest.batch_to_bronze import run as run_batch
from src.ingest.stream_to_bronze import run as run_stream_bronze
from src.transform.gold import KEY, aggregate_daily_usage
from src.transform.gold_job import gold_path, run as run_gold
from src.transform.gold_stream import latest_view, run as run_gold_stream
from src.transform.silver_job import run as run_silver

FILES = ["events_part_0000.jsonl", "events_part_0001.jsonl", "events_part_0002.jsonl"]
METRICS = ["daily_cost_usd", "requests", "cpu_hours", "storage_gb_hours", "event_count"]


@pytest.fixture(scope="module")
def small_lake(spark, landing, tmp_path_factory):
    """Lake chico con 2 archivos de eventos: Bronze, Silver y Gold reconciliado."""
    tmp = tmp_path_factory.mktemp("gold")
    dst = tmp / "landing"
    (dst / "usage_events_stream").mkdir(parents=True)
    for f in os.listdir(landing):
        if f.endswith(".csv"):
            shutil.copy(os.path.join(landing, f), dst / f)
    for f in FILES:
        shutil.copy(os.path.join(landing, "usage_events_stream", f), dst / "usage_events_stream" / f)
    cfg = load_settings(landing=str(dst), lake=str(tmp / "lake"), evidence=str(tmp / "ev"),
                        checkpoints=str(tmp / "ck"), ingest_date="2026-11-12")
    run_batch(spark, cfg, ["customers_orgs", "nps_surveys", "resources"])
    run_stream_bronze(spark, cfg)
    run_silver(spark, cfg)
    report = run_gold(spark, cfg)
    return cfg, report


def test_agregacion_sobre_filas_conocidas(spark):
    rows = [("o1", date(2025, 8, 1), "compute", 10.0, 100.0, 0.0, 0.0, None, 0.5, 5.0, False, "A", "pro"),
            ("o1", date(2025, 8, 1), "compute", -2.0, 0.0, 1.5, 0.0, None, None, 1.5, True, "A", "pro"),
            ("o1", date(2025, 8, 1), "compute", 3.0, None, 0.0, 0.0, None, 0.1, None, False, "A", "pro")]
    df = spark.createDataFrame(rows, "org_id string, event_date date, service string, cost_usd double, "
                               "requests double, cpu_hours double, storage_gb_hours double, "
                               "genai_tokens long, carbon_kg double, value_num double, "
                               "is_credit_adjustment boolean, org_name string, plan_tier string")
    r = aggregate_daily_usage(df).first()
    assert r["daily_cost_usd"] == 11.0                  # neto: 10 - 2 + 3
    assert r["daily_cost_gross_usd"] == 13.0
    assert r["credit_adjustment_usd"] == -2.0
    assert r["requests"] == 100.0                       # el nulo no anula la suma (D-009)
    assert r["cpu_hours"] == 1.5
    assert r["genai_tokens"] is None                    # sin GenAI: nulo, no 0
    assert r["carbon_kg"] == pytest.approx(0.6)
    assert (r["event_count"], r["events_metric_null"]) == (3, 1)


def test_grano_unico_y_cuadra_con_silver(spark, small_lake):
    cfg, report = small_lake
    g = spark.read.parquet(gold_path(cfg))
    assert g.count() == g.select(*KEY).distinct().count()
    assert report["eventos_agregados"] == 1080
    assert report["costo_total_usd"] == pytest.approx(report["costo_total_silver_usd"], abs=0.05)


def test_reconciliacion_es_idempotente(spark, small_lake):
    cfg, r1 = small_lake
    r2 = run_gold(spark, cfg)
    assert (r2["filas"], r2["costo_total_usd"]) == (r1["filas"], r1["costo_total_usd"])


def test_streaming_y_batch_calculan_igual_sin_tardios(spark, small_lake, tmp_path):
    """G-04: con un solo micro-lote no hay watermark previo, nada se descarta, y la
    ruta streaming debe dar exactamente el mart reconciliado."""
    cfg, _ = small_lake
    cfg1 = load_settings(landing=cfg.landing, lake=cfg.lake, evidence=str(tmp_path / "ev"),
                         checkpoints=str(tmp_path / "ck"), ingest_date=cfg.ingest_date)
    shutil.rmtree(f"{cfg.lake}/gold/org_daily_usage_by_service_speed", ignore_errors=True)
    r = run_gold_stream(spark, cfg1, max_files=10)
    assert r["esta_corrida"]["micro_lotes"] == 1
    assert r["late_data"]["descartados_por_watermark"] == 0
    speed = latest_view(spark, cfg1).select(*KEY, *METRICS)
    gold = spark.read.parquet(gold_path(cfg)).select(*KEY, *METRICS)
    assert speed.exceptAll(gold).count() == 0 and gold.exceptAll(speed).count() == 0


def test_tardios_se_descartan_en_streaming_y_los_recupera_la_reconciliacion(spark, small_lake, tmp_path):
    cfg, gold_report = small_lake
    cfg2 = load_settings(landing=cfg.landing, lake=cfg.lake, evidence=str(tmp_path / "ev"),
                         checkpoints=str(tmp_path / "ck"), ingest_date=cfg.ingest_date)
    shutil.rmtree(f"{cfg.lake}/gold/org_daily_usage_by_service_speed", ignore_errors=True)
    # 3 lotes. Spark 4 filtra tardíos con el watermark del lote anterior: el lote 0
    # fija el máximo, el 1 todavía no descarta y el 2 descarta lo viejo.
    r = run_gold_stream(spark, cfg2, max_files=1)
    e = r["esta_corrida"]
    assert e["micro_lotes"] == 3
    late = r["late_data"]
    assert late["eventos_en_bronze"] == 1080
    assert 250 < late["descartados_por_watermark"] < 360  # la mayoría del 3.er archivo
    assert r["capa_rapida"]["costo_usd"] < gold_report["costo_total_usd"]
    # y re-ejecutar con el mismo checkpoint no procesa nada ni duplica la capa rápida
    before = spark.read.parquet(f"{cfg.lake}/gold/org_daily_usage_by_service_speed").count()
    r2 = run_gold_stream(spark, cfg2, max_files=1)
    assert r2["esta_corrida"]["micro_lotes"] == 0
    assert spark.read.parquet(f"{cfg.lake}/gold/org_daily_usage_by_service_speed").count() == before

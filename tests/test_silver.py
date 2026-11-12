"""Pruebas de la conformación Bronze -> Silver.

Las reglas se prueban con eventos sintéticos de una fila cada uno, así cada
test documenta exactamente un caso de la §9.7 / D-009 / D-014.
"""

from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from src.schemas.events import EVENT_SCHEMA
from src.transform.silver import conform_events, conform_orgs, split_silver

BASE = {"event_id": "e1", "timestamp": "2025-08-01T10:00:00Z", "org_id": "org_a",
        "resource_id": "r1", "service": "compute", "region": "us-east",
        "metric": "requests", "value": "120.0", "unit": "count",
        "cost_usd_increment": 5.0, "schema_version": 2, "carbon_kg": 0.02,
        "genai_tokens": None}


@pytest.fixture()
def orgs(spark):
    df = spark.createDataFrame(
        [("org_a", "A", "Media", "us-east", "pro", True, date(2025, 5, 1), "rep_a",
          "active", "organic", 101.0, datetime(2026, 11, 12)),
         ("org_b", "B", "Media", "us-east", "pro", True, date(2025, 5, 1), "rep_a",
          "active", "organic", 30.0, datetime(2026, 11, 12))],
        "org_id string, org_name string, industry string, hq_region string, plan_tier string, "
        "is_enterprise boolean, signup_date date, sales_rep string, lifecycle_stage string, "
        "marketing_source string, nps_score double, ingest_ts timestamp")
    nps = spark.createDataFrame(
        [("org_a", date(2025, 6, 1), 10.0), ("org_a", date(2025, 8, 1), 25.0),
         ("org_a", date(2025, 8, 15), None)],
        "org_id string, survey_date date, nps_score double")
    return conform_orgs(df, nps)


def _conform(spark, orgs, *events):
    rows = [{**BASE, **e} for e in events]
    df = (spark.createDataFrame(rows, EVENT_SCHEMA)
          .withColumn("event_ts", F.to_timestamp("timestamp"))
          .withColumn("source_file", F.lit("f.jsonl"))
          .withColumn("ingest_ts", F.current_timestamp()))
    return conform_events(df, orgs)


def _one(spark, orgs, **override):
    return _conform(spark, orgs, override).first()


def test_value_texto_se_castea(spark, orgs):
    r = _one(spark, orgs, value="8.9427", metric="storage_gb_hours", unit="gb_hours")
    assert r["value_num"] == pytest.approx(8.9427)
    assert r["storage_gb_hours"] == pytest.approx(8.9427) and r["requests"] == 0.0
    assert r["value_raw"] == "8.9427"                     # el crudo se conserva


def test_value_nulo_anula_la_metrica_pero_no_el_costo(spark, orgs):
    """D-009: el rechazo opera sobre el campo derivado, no sobre el registro."""
    r = _one(spark, orgs, value=None)
    assert r["value_num"] is None and r["requests"] is None
    assert r["cost_usd"] == 5.0
    assert r["quality_flags"] == ["value_nulo"]


def test_unit_nulo_se_imputa_desde_la_metrica(spark, orgs):
    """D-014: la unidad es función de la métrica; se recupera y se marca."""
    r = _one(spark, orgs, unit=None, metric="cpu_hours", value="2.5")
    assert r["unit"] == "hours"
    assert r["cpu_hours"] == 2.5
    assert r["quality_flags"] == ["unit_nulo_con_value"]


def test_costo_negativo_es_flag_y_no_rechazo(spark, orgs):
    r = _one(spark, orgs, cost_usd_increment=-19.82)
    assert r["is_credit_adjustment"] is True
    assert r["quality_flags"] == ["costo_negativo"]
    borde = _one(spark, orgs, cost_usd_increment=-0.01)      # consigna: >= -0.01 es válido
    assert borde["is_credit_adjustment"] is False


def test_v1_se_promueve_al_esquema_ampliado_con_nulos(spark, orgs):
    r = _one(spark, orgs, schema_version=1, carbon_kg=None)
    assert r["carbon_kg"] is None and r["genai_tokens"] is None
    assert r["quality_flags"] == []


def test_regla_genai_es_compuesta(spark, orgs):
    """D-002: tokens  <=>  v2 AND service = genai. La regla ingenua daría falsos positivos."""
    ok_genai = _one(spark, orgs, service="genai", genai_tokens=500)
    ok_otro_v2 = _one(spark, orgs, service="compute", genai_tokens=None)
    falta = _one(spark, orgs, service="genai", genai_tokens=None)
    sobra = _one(spark, orgs, service="storage", genai_tokens=10)
    assert ok_genai["quality_flags"] == [] and ok_otro_v2["quality_flags"] == []
    assert falta["quality_flags"] == ["genai_tokens_inconsistente"]
    assert sobra["quality_flags"] == ["genai_tokens_inconsistente"]


def test_org_desconocida_no_se_pierde(spark, orgs):
    r = _one(spark, orgs, org_id="org_zzz")
    assert r["org_name"] is None
    assert r["quality_flags"] == ["org_sin_dimension"]


def test_duplicado_queda_una_vez_y_la_copia_va_a_quarantine(spark, orgs):
    conformed = _conform(spark, orgs, {}, {"source_file": "otro"}, {"event_id": "e2"})
    silver, quarantine = split_silver(conformed)
    assert silver.count() == 2
    q = quarantine.collect()
    assert [(r["regla"], r["destino_en_silver"]) for r in q] == [("event_id_duplicado", "descartado")]


def test_quarantine_es_copia_de_auditoria(spark, orgs):
    conformed = _conform(spark, orgs, {"value": None}, {"event_id": "e2", "cost_usd_increment": -5.0})
    silver, quarantine = split_silver(conformed)
    assert silver.count() == 2                               # nadie sale de Silver
    assert [r["regla"] for r in quarantine.collect()] == ["value_nulo"]   # costo_negativo no es defecto


def test_nps_ultima_encuesta_con_fallback_al_crm(orgs):
    rows = {r["org_id"]: r for r in orgs.collect()}
    a, b = rows["org_a"], rows["org_b"]
    # org_a: última encuesta con score (15/08 es nula) -> 25; el 101 del CRM se marca
    assert (a["nps_score"], a["nps_source"], str(a["nps_survey_date"])) == (25.0, "survey", "2025-08-01")
    assert a["quality_flags"] == ["nps_fuera_de_rango"]
    # org_b: nunca encuestada -> cae al CRM
    assert (b["nps_score"], b["nps_source"]) == (30.0, "crm")


def test_job_completo_sobre_el_dataset_real(spark, landing, tmp_path):
    from src.common.config import load_settings
    from src.ingest.batch_to_bronze import run as run_batch
    from src.ingest.stream_to_bronze import run as run_stream
    from src.transform.silver_job import run as run_silver
    cfg = load_settings(landing=landing, lake=str(tmp_path / "lake"),
                        evidence=str(tmp_path / "ev"), checkpoints=str(tmp_path / "ck"),
                        ingest_date="2026-11-12")
    run_batch(spark, cfg, ["customers_orgs", "nps_surveys", "resources"])
    run_stream(spark, cfg, max_files=40)                     # 3 lotes de 40 archivos: más rápido
    r1 = run_silver(spark, cfg)
    r2 = run_silver(spark, cfg)                              # idempotente
    assert r1["eventos"]["filas_silver"] == r2["eventos"]["filas_silver"] == 43200
    assert r1["eventos"]["flags"] == {"costo_negativo": 211, "unit_nulo_con_value": 2038,
                                      "value_nulo": 877}
    assert r1["eventos"]["v1_con_carbon_kg"] == 0
    assert r1["eventos"]["archivos_parquet"] == r1["eventos"]["particiones_event_date_x_service"]

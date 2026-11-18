"""Pruebas de los marts de dominio: revenue, tickets y GenAI (consigna §7.3).

Filas Silver sintéticas mínimas; el último test corre el job sobre el dataset
real y verifica grano, conservación e idempotencia.
"""

from datetime import date

import pytest

from src.transform.gold_domains import (genai_tokens_by_org_date, revenue_by_org_month,
                                        tickets_by_org_date)


def test_revenue_suma_por_org_y_mes(spark):
    silver = spark.createDataFrame([
        ("org_a", "A", "pro", date(2025, 6, 1), "USD", 1000.0, 1.0, 10.0, 210.0, 990.0, 1200.0, False),
        ("org_a", "A", "pro", date(2025, 7, 1), "EUR", -100.0, 1.1, 0.0, 23.1, -110.0, -86.9, True),
    ], "org_id string, org_name string, plan_tier string, month date, currency string, "
       "subtotal double, fx_applied double, credits_usd double, taxes_usd double, "
       "net_revenue_usd double, total_billed_usd double, is_credit_note boolean")
    rows = {r["month"]: r for r in revenue_by_org_month(silver).collect()}
    jun, jul = rows[date(2025, 6, 1)], rows[date(2025, 7, 1)]
    assert (jun["net_revenue_usd"], jun["total_billed_usd"], jun["fx_applied"]) == (990.0, 1200.0, 1.0)
    assert (jun["credits_usd"], jun["taxes_usd"], jun["subtotal_usd"]) == (10.0, 210.0, 1000.0)
    assert jul["subtotal_usd"] == pytest.approx(-110.0) and jul["has_credit_note"] is True


def test_revenue_con_monedas_mezcladas_no_inventa_una_tasa(spark):
    silver = spark.createDataFrame([
        ("org_a", "A", "pro", date(2025, 6, 1), "USD", 100.0, 1.0, 0.0, 21.0, 100.0, 121.0, False),
        ("org_a", "A", "pro", date(2025, 6, 1), "EUR", 100.0, 1.1, 0.0, 23.1, 110.0, 133.1, False),
    ], "org_id string, org_name string, plan_tier string, month date, currency string, "
       "subtotal double, fx_applied double, credits_usd double, taxes_usd double, "
       "net_revenue_usd double, total_billed_usd double, is_credit_note boolean")
    r = revenue_by_org_month(silver).first()
    assert (r["currency"], r["fx_applied"], r["invoice_count"]) == ("EUR,USD", None, 2)
    assert r["net_revenue_usd"] == 210.0


def test_tickets_tasa_de_sla_y_csat_solo_con_respuestas_validas(spark):
    silver = spark.createDataFrame([
        ("org_a", "A", "pro", date(2025, 8, 1), "critical", True, True, None),
        ("org_a", "A", "pro", date(2025, 8, 1), "critical", False, False, 4.0),
        ("org_a", "A", "pro", date(2025, 8, 1), "critical", False, False, 2.0),
        ("org_a", "A", "pro", date(2025, 8, 1), "low", False, False, 5.0),
    ], "org_id string, org_name string, plan_tier string, ticket_date date, severity string, "
       "is_open boolean, sla_breached boolean, csat double")
    rows = {r["severity"]: r for r in tickets_by_org_date(silver).collect()}
    c = rows["critical"]
    assert (c["ticket_count"], c["open_count"], c["sla_breached_count"]) == (3, 1, 1)
    assert c["sla_breach_rate"] == pytest.approx(0.3333)
    assert (c["csat_responses"], c["csat_avg"]) == (2, 3.0)     # el ticket sin CSAT no divide
    assert rows["low"]["sla_breach_rate"] == 0.0


def test_genai_solo_eventos_v2_de_genai(spark):
    silver = spark.createDataFrame([
        ("org_a", "A", "pro", date(2025, 8, 1), "genai", 2, 1000, 2.0),
        ("org_a", "A", "pro", date(2025, 8, 1), "genai", 2, 500, 1.0),
        ("org_a", "A", "pro", date(2025, 8, 1), "genai", 1, None, 9.0),     # v1: sin tokens
        ("org_a", "A", "pro", date(2025, 8, 1), "compute", 2, None, 5.0),
    ], "org_id string, org_name string, plan_tier string, event_date date, service string, "
       "schema_version int, genai_tokens long, cost_usd double")
    r = genai_tokens_by_org_date(silver).collect()
    assert len(r) == 1
    assert (r[0]["genai_tokens"], r[0]["genai_cost_usd"], r[0]["event_count"]) == (1500, 3.0, 2)
    assert r[0]["cost_per_1k_tokens_usd"] == 2.0


def test_job_sobre_el_dataset_real(spark, landing, tmp_path):
    from src.common.config import load_settings
    from src.ingest.batch_to_bronze import run as run_batch
    from src.ingest.stream_to_bronze import run as run_stream
    from src.transform.gold_domains_job import run as run_gold
    from src.transform.silver_job import run as run_silver
    cfg = load_settings(landing=landing, lake=str(tmp_path / "lake"),
                        evidence=str(tmp_path / "ev"), checkpoints=str(tmp_path / "ck"),
                        ingest_date="2026-11-18")
    run_batch(spark, cfg)
    run_stream(spark, cfg, max_files=40)
    run_silver(spark, cfg)
    r1, r2 = run_gold(spark, cfg)["marts"], run_gold(spark, cfg)["marts"]
    assert r1 == r2                                                     # idempotente
    rev, tk, ga = r1["revenue_by_org_month"], r1["tickets_by_org_date"], r1["genai_tokens_by_org_date"]
    assert (rev["filas"], rev["facturas"], rev["orgs"]) == (240, 240, 80)
    assert rev["net_revenue_usd"] == pytest.approx(132074.83, abs=0.5)
    assert (tk["tickets"], tk["criticos"], tk["sla_breached"], tk["abiertos"]) == (1000, 56, 95, 240)
    assert (ga["eventos"], ga["primer_dia"]) == (3132, "2025-07-18")

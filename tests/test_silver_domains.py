"""Pruebas de la Silver de dominio: billing, tickets, users y marketing (D-019, D-022).

Igual que test_silver.py: filas sintéticas mínimas, un caso por regla.
"""

from datetime import date, datetime

import pytest

from src.transform.silver_domains import (conform_billing, conform_marketing, conform_tickets,
                                          conform_users, quarantine_copies)

TS = datetime(2026, 11, 18)


@pytest.fixture()
def orgs(spark):
    return spark.createDataFrame([("org_a", "A", "pro")],
                                 "org_id string, org_name string, plan_tier string")


def _bronze(spark, schema, rows):
    return spark.createDataFrame([(*r, "f.csv", TS) for r in rows],
                                 f"{schema}, source_file string, ingest_ts timestamp")


# ---------------------------------------------------------------------------
# Billing
# ---------------------------------------------------------------------------

BILLING = ("invoice_id string, org_id string, month date, subtotal double, credits double, "
           "taxes double, currency string, exchange_rate_to_usd double")


def _billing(spark, orgs, *rows):
    out = conform_billing(_bronze(spark, BILLING, rows), orgs).collect()
    return {r["invoice_id"]: r for r in out}


def test_usd_fuerza_fx_1_y_credits_nulo_es_0(spark, orgs):
    """D-007: dólares a dólares es 1,0; credits nulo = sin crédito."""
    r = _billing(spark, orgs, ("i1", "org_a", date(2025, 6, 1), 1000.0, None, 210.0, "USD", 0.9))["i1"]
    assert r["fx_applied"] == 1.0 and r["credits"] == 0.0
    assert r["net_revenue_usd"] == 1000.0 and r["total_billed_usd"] == 1210.0
    assert r["quality_flags"] == ["fx_usd_ruidoso"]


def test_moneda_extranjera_usa_la_tasa_de_la_fuente(spark, orgs):
    """D-019: (subtotal − credits) × fx; impuestos y créditos también en USD."""
    r = _billing(spark, orgs, ("i1", "org_a", date(2025, 6, 1), 100.0, 10.0, 21.0, "EUR", 1.1))["i1"]
    assert r["net_revenue_usd"] == pytest.approx(99.0)
    assert r["total_billed_usd"] == pytest.approx(122.1)
    assert (r["credits_usd"], r["taxes_usd"]) == (pytest.approx(11.0), pytest.approx(23.1))
    assert r["quality_flags"] == []


def test_nota_de_credito_es_flag_y_no_rechazo(spark, orgs):
    r = _billing(spark, orgs, ("i1", "org_a", date(2025, 6, 1), -500.0, None, 105.0, "USD", 1.0))["i1"]
    assert r["is_credit_note"] is True and r["net_revenue_usd"] == -500.0
    assert r["quality_flags"] == ["nota_de_credito", "impuesto_signo_inconsistente"]


# ---------------------------------------------------------------------------
# Tickets
# ---------------------------------------------------------------------------

TICKETS = ("ticket_id string, org_id string, category string, severity string, created_at date, "
           "resolved_at date, csat double, sla_breached boolean")


def _tickets(spark, orgs, *rows):
    return conform_tickets(_bronze(spark, TICKETS, rows), orgs)


def test_nulos_semanticos_no_son_defectos(spark, orgs):
    """resolved_at nulo = abierto; csat nulo = sin respuesta. Ninguno va a quarantine."""
    df = _tickets(spark, orgs, ("t1", "org_a", "Billing", " Critical", date(2025, 8, 1), None, None, True))
    r = df.first()
    assert (r["severity"], r["category"]) == ("critical", "billing")
    assert r["is_open"] is True and r["resolution_days"] is None and r["csat"] is None
    assert r["quality_flags"] == []
    assert quarantine_copies("support_tickets", df).count() == 0


def test_csat_fuera_de_escala_anula_el_campo_no_el_ticket(spark, orgs):
    """D-022: escala 1-5. El 0, el 6 y el 7 observados se anulan y se auditan."""
    df = _tickets(spark, orgs,
                  ("t1", "org_a", "billing", "low", date(2025, 8, 1), date(2025, 8, 4), 6.0, False),
                  ("t2", "org_a", "billing", "low", date(2025, 8, 1), date(2025, 8, 2), 5.0, False))
    rows = {r["ticket_id"]: r for r in df.collect()}
    assert rows["t1"]["csat"] is None and rows["t1"]["csat_raw"] == 6.0
    assert rows["t1"]["resolution_days"] == 3
    assert rows["t1"]["quality_flags"] == ["csat_fuera_de_escala"]
    assert rows["t2"]["csat"] == 5.0 and rows["t2"]["quality_flags"] == []
    q = quarantine_copies("support_tickets", df).collect()
    assert [(r["record_id"], r["regla"]) for r in q] == [("t1", "csat_fuera_de_escala")]


def test_resuelto_antes_de_abierto_se_marca(spark, orgs):
    r = _tickets(spark, orgs, ("t1", "org_a", "billing", "low", date(2025, 8, 5),
                               date(2025, 8, 1), 3.0, False)).first()
    assert r["quality_flags"] == ["resolved_antes_de_created"]


# ---------------------------------------------------------------------------
# Users y marketing (O-5: invariantes entre columnas del TP1, §9.7)
# ---------------------------------------------------------------------------

USERS = ("user_id string, org_id string, email string, role string, active boolean, "
         "created_at date, last_login date")


def test_users_invariante_temporal_y_sin_email(spark, orgs):
    df = conform_users(_bronze(spark, USERS, [
        ("u1", "org_a", "u1@example.com", "Admin", True, date(2025, 7, 10), date(2025, 7, 1)),
        ("u2", "org_a", "u2@example.com", "admin", True, date(2025, 7, 10), None)]), orgs)
    assert "email" not in df.columns                        # minimización: no llega a Silver
    rows = {r["user_id"]: r for r in df.collect()}
    assert rows["u1"]["quality_flags"] == ["last_login_antes_de_created"]
    assert rows["u2"]["quality_flags"] == [] and rows["u2"]["has_logged_in"] is False
    assert rows["u1"]["role"] == "admin"


MKT = ("touch_id string, org_id string, campaign string, channel string, timestamp date, "
       "clicked boolean, converted boolean")


def test_marketing_conversion_sin_click_se_marca(spark, orgs):
    df = conform_marketing(_bronze(spark, MKT, [
        ("m1", "org_a", "welcome", "email", date(2025, 8, 1), False, True),
        ("m2", "org_a", "welcome", "email", date(2025, 8, 1), True, True),
        ("m3", "org_zzz", "welcome", "email", date(2025, 8, 1), False, False)]), orgs)
    rows = {r["touch_id"]: r for r in df.collect()}
    assert rows["m1"]["quality_flags"] == ["converted_sin_click"]
    assert rows["m2"]["quality_flags"] == []
    assert rows["m3"]["quality_flags"] == ["org_sin_dimension"]   # no se pierde
    assert quarantine_copies("marketing_touches", df).count() == 2

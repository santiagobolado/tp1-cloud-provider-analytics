"""Pruebas del serving que no necesitan una base: forma de las tablas, CQL y
manejo de credenciales. La carga real se verifica contra AstraDB (evidence/)."""

from datetime import date, timedelta

import pytest

from src.serving import cassandra_io as cio
from src.serving.run_queries import titled_queries
from src.serving.tables import (P1_COLS, P2_COLS, P3_COLS, P4_COLS, P5_COLS, genai_table,
                                revenue_table, tickets_severity_table, top_services_table,
                                usage_table)

MART_COLS = ("org_id string, usage_date date, service string, org_name string, plan_tier string, "
             "daily_cost_usd double, daily_cost_gross_usd double, credit_adjustment_usd double, "
             "requests double, cpu_hours double, storage_gb_hours double, genai_tokens long, "
             "carbon_kg double, event_count long, source string, last_reconciled_at timestamp")
D0 = date(2025, 8, 1)


def _mart(spark, rows):
    full = [(o, d, s, o, "pro", c, c, 0.0, 10.0, 0.0, 0.0, None, None, 1, "reconciliacion", None)
            for o, d, s, c in rows]
    return spark.createDataFrame(full, MART_COLS)


def test_p1_tiene_las_columnas_de_la_tabla(spark):
    t = usage_table(_mart(spark, [("o1", D0, "compute", 5.0)]))
    assert t.columns == P1_COLS


def test_p2_top_por_costo_en_ventana_de_14_dias(spark):
    rows = []
    for i in range(20):
        d = D0 + timedelta(days=i)
        rows += [("o1", d, "compute", 10.0), ("o1", d, "storage", 1.0)]
    rows.append(("o1", D0, "genai", 500.0))                  # fuera de la ventana del día 19
    t = top_services_table(_mart(spark, rows))
    assert t.columns == P2_COLS
    last = {r["rank"]: r for r in t.filter(t.as_of_date == D0 + timedelta(days=19)).collect()}
    assert [last[1]["service"], last[2]["service"]] == ["compute", "storage"]
    assert last[1]["cost_14d_usd"] == 140.0 and last[1]["days_with_usage"] == 14
    assert last[1]["window_start"] == D0 + timedelta(days=6)
    first = {r["rank"]: r["service"] for r in t.filter(t.as_of_date == D0 + timedelta(days=13)).collect()}
    assert first[1] == "genai"                               # dentro de la ventana del día 13
    # solo fechas de corte con 14 días completos de historia
    assert t.agg({"as_of_date": "min"}).first()[0] == D0 + timedelta(days=13)


def test_el_schema_cql_define_las_cinco_tablas_con_su_clave():
    stmts = cio.cql_statements("schema.cql")
    keys = ["PRIMARY KEY ((org_id), usage_date, service)", "PRIMARY KEY ((org_id, as_of_date), rank)",
            "PRIMARY KEY ((severity), ticket_date)", "PRIMARY KEY ((org_id), month)",
            "PRIMARY KEY ((org_id), usage_date)"]
    assert len(stmts) == 5
    assert all(k in s for k, s in zip(keys, stmts))


def test_el_cql_y_las_columnas_de_spark_coinciden():
    """Si una tabla CQL y su DataFrame divergen, la carga fallaría recién en AstraDB."""
    cols = [P1_COLS, P2_COLS, P3_COLS, P4_COLS, P5_COLS]
    for stmt, expected in zip(cio.cql_statements("schema.cql"), cols):
        body = stmt[stmt.index("(") + 1:stmt.index("PRIMARY KEY")]
        declared = [line.split()[0] for line in body.strip().splitlines() if line.strip()]
        assert declared == expected


def test_las_consultas_filtran_por_clave_de_particion():
    """Las 5 consultas de la consigna (más las variantes de total) sin ALLOW FILTERING."""
    qs = titled_queries()
    assert [t.split()[0] for t, _ in qs] == ["Q1", "Q2", "Q3", "Q3b", "Q4", "Q4b", "Q5", "Q5b"]
    for _, cql in qs:
        assert ("WHERE org_id =" in cql or "WHERE severity =" in cql)
        assert "ALLOW FILTERING" not in cql


TICKETS_MART = ("org_id string, ticket_date date, severity string, ticket_count long, "
                "open_count long, sla_breached_count long, csat_responses long, csat_sum double")


def test_q3_serie_completa_y_total_que_suma_conteos(spark):
    mart = spark.createDataFrame([
        ("o1", D0, "critical", 1, 0, 1, 1, 2.0),
        ("o2", D0, "low", 3, 1, 0, 2, 9.0),
        ("o1", D0 + timedelta(days=2), "low", 1, 0, 0, 0, None),
    ], TICKETS_MART)
    t = tickets_severity_table(mart)
    assert t.columns == P3_COLS
    rows = {(r["severity"], r["ticket_date"]): r for r in t.collect()}
    assert len(rows) == 3 * 3                       # (critical, low, all) × 3 días
    gap = rows[("critical", D0 + timedelta(days=1))]
    assert (gap["ticket_count"], gap["sla_breach_rate"]) == (0, None)
    a = rows[("all", D0)]
    # total = suma de conteos: 1 de 4 tickets, no el promedio de las tasas (0,5)
    assert (a["ticket_count"], a["sla_breached_count"], a["sla_breach_rate"]) == (4, 1, 0.25)
    assert (a["csat_avg"], a["org_count"]) == (3.67, 2)


def test_q4_y_q5_agregan_la_particion_all(spark):
    rev = spark.createDataFrame([
        ("o1", D0, "A", "pro", "USD", 1.0, 100.0, 0.0, 21.0, 100.0, 121.0, 1, False),
        ("o2", D0, "B", "pro", "ARS", 0.0015, 1.2, 0.0, 0.25, 1.2, 1.45, 1, False),
    ], "org_id string, month date, org_name string, plan_tier string, currency string, "
       "fx_applied double, subtotal_usd double, credits_usd double, taxes_usd double, "
       "net_revenue_usd double, total_billed_usd double, invoice_count long, has_credit_note boolean")
    r = {x["org_id"]: x for x in revenue_table(rev).collect()}
    assert revenue_table(rev).columns == P4_COLS
    assert (r["all"]["net_revenue_usd"], r["all"]["org_count"], r["all"]["currency"]) == (101.2, 2, "ARS,USD")
    assert r["all"]["fx_applied"] is None and r["o1"]["fx_applied"] == 1.0

    gen = spark.createDataFrame([
        ("o1", D0, "A", "pro", 1000, 2.0, 2.0, 2), ("o2", D0, "B", "pro", 3000, 1.0, 0.333, 1),
    ], "org_id string, usage_date date, org_name string, plan_tier string, genai_tokens long, "
       "genai_cost_usd double, cost_per_1k_tokens_usd double, event_count long")
    g = {x["org_id"]: x for x in genai_table(gen).collect()}
    assert genai_table(gen).columns == P5_COLS
    assert (g["all"]["genai_tokens"], g["all"]["genai_cost_usd"], g["all"]["event_count"]) == (4000, 3.0, 3)
    assert g["all"]["cost_per_1k_tokens_usd"] == 0.75


def test_sin_credenciales_falla_con_mensaje_claro(monkeypatch):
    for v in ["ASTRA_DB_BUNDLE_PATH", "ASTRA_DB_TOKEN", "CASSANDRA_HOST"]:
        monkeypatch.delenv(v, raising=False)
    with pytest.raises(cio.ServingConfigError, match="ASTRA_DB_BUNDLE_PATH"):
        cio.connection_params()


def test_bundle_inexistente_falla_antes_de_conectar(monkeypatch):
    monkeypatch.setenv("ASTRA_DB_BUNDLE_PATH", "/no/existe.zip")
    monkeypatch.setenv("ASTRA_DB_TOKEN", "AstraCS:fake")
    with pytest.raises(cio.ServingConfigError, match="Secure Connect Bundle"):
        cio.connection_params()


def test_las_consultas_usan_el_token_de_solo_lectura(monkeypatch, tmp_path):
    """D-025: la carga usa el de administrador; las consultas, el de solo lectura."""
    bundle = tmp_path / "secure-connect-x.zip"
    bundle.write_bytes(b"")
    monkeypatch.setenv("ASTRA_DB_BUNDLE_PATH", str(bundle))
    monkeypatch.setenv("ASTRA_DB_TOKEN", "AstraCS:admin")
    monkeypatch.setenv("ASTRA_DB_TOKEN_READONLY", "AstraCS:ro")
    assert cio.connection_params()["token"] == "AstraCS:admin"
    ro = cio.connection_params(read_only=True)
    assert (ro["token"], ro["token_role"]) == ("AstraCS:ro", "solo_lectura")
    monkeypatch.delenv("ASTRA_DB_TOKEN_READONLY")             # sin token RO: cae al admin y lo dice
    assert cio.connection_params(read_only=True)["token_role"] == "administrador"

"""Pruebas del serving que no necesitan una base: forma de las tablas, CQL y
manejo de credenciales. La carga real se verifica contra AstraDB (evidence/)."""

from datetime import date, timedelta

import pytest

from src.serving import cassandra_io as cio
from src.serving.run_queries import titled_queries
from src.serving.tables import P1_COLS, P2_COLS, top_services_table, usage_table

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


def test_el_schema_cql_define_las_dos_tablas_con_su_clave():
    stmts = cio.cql_statements("schema.cql")
    assert len(stmts) == 2
    assert "PRIMARY KEY ((org_id), usage_date, service)" in stmts[0]
    assert "PRIMARY KEY ((org_id, as_of_date), rank)" in stmts[1]


def test_las_consultas_filtran_por_clave_de_particion():
    qs = titled_queries()
    assert [t[:2] for t, _ in qs] == ["Q1", "Q2"]
    for _, cql in qs:
        assert "WHERE org_id =" in cql and "ALLOW FILTERING" not in cql


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

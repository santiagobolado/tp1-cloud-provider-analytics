"""Pruebas de la ingesta batch Landing -> Bronze.

Cubren lo que la consigna pide demostrar en esta frontera: tipado explícito,
columnas técnicas, deduplicación, quarantine con motivo, particionado,
idempotencia e inmutabilidad de Landing.
"""

import hashlib
import os
import shutil

import pytest
from pyspark.sql import functions as F

from src.common.config import load_settings
from src.ingest.batch_to_bronze import run

EXPECTED_ROWS = {
    "customers_orgs": 80, "users": 800, "resources": 400, "support_tickets": 1000,
    "marketing_touches": 1500, "nps_surveys": 92, "billing_monthly": 240,
}


def _cfg(tmp, landing, ingest_date="2026-11-12"):
    return load_settings(landing=landing, lake=str(tmp / "lake"),
                         evidence=str(tmp / "evidence"), ingest_date=ingest_date)


def _hash_dir(path):
    """Huella de todo Landing, subcarpetas incluidas (usage_events_stream/)."""
    h = hashlib.sha256()
    for base, dirs, names in sorted(os.walk(path)):
        dirs.sort()
        for name in sorted(names):
            full = os.path.join(base, name)
            with open(full, "rb") as fh:
                h.update(os.path.relpath(full, path).encode() + fh.read())
    return h.hexdigest()


@pytest.fixture(scope="module")
def first_run(spark, landing, tmp_path_factory):
    tmp = tmp_path_factory.mktemp("bronze")
    cfg = _cfg(tmp, landing)
    landing_hash = _hash_dir(landing)
    report = run(spark, cfg)
    return cfg, report, landing_hash


def test_las_siete_fuentes_llegan_completas(first_run):
    _, report, _ = first_run
    got = {r["entity"]: (r["filas_leidas"], r["filas_bronze"], r["filas_quarantine"])
           for r in report["fuentes"]}
    assert got == {e: (n, n, 0) for e, n in EXPECTED_ROWS.items()}


def test_tipado_explicito_y_columnas_tecnicas(spark, first_run):
    cfg, _, _ = first_run
    df = spark.read.parquet(f"{cfg.bronze}/billing_monthly")
    types = dict(df.dtypes)
    assert types["month"] == "date"
    assert types["subtotal"] == "double"
    assert types["ingest_ts"] == "timestamp"
    assert "_corrupt_record" not in types
    # Un único ingest_ts por corrida y source_file apuntando al archivo de Landing.
    assert df.select("ingest_ts").distinct().count() == 1
    assert df.filter(~F.col("source_file").endswith("landing/billing_monthly.csv")).count() == 0


def test_los_nulos_semanticos_se_conservan(spark, first_run):
    """Bronze no limpia: los nulos del perfilado tienen que seguir ahí."""
    cfg, _, _ = first_run
    tickets = spark.read.parquet(f"{cfg.bronze}/support_tickets")
    assert tickets.filter(F.col("resolved_at").isNull()).count() == 240
    assert tickets.filter(F.col("csat").isNull()).count() == 254
    billing = spark.read.parquet(f"{cfg.bronze}/billing_monthly")
    assert billing.filter(F.col("credits").isNull()).count() == 137


def test_tags_json_se_lee_con_el_escape_correcto(spark, first_run):
    cfg, _, _ = first_run
    tags = spark.read.parquet(f"{cfg.bronze}/resources").filter(F.col("tags_json").isNotNull())
    parsed = tags.select(F.from_json("tags_json", "array<string>").alias("t"))
    assert parsed.filter(F.col("t").isNull()).count() == 0     # todos son JSON válido


def test_un_archivo_por_particion(first_run):
    _, report, _ = first_run
    assert all(r["archivos_parquet"] == 1 for r in report["fuentes"])


def test_landing_no_se_modifica(first_run, landing):
    _, _, before = first_run
    assert _hash_dir(landing) == before


def test_reejecutar_el_mismo_dia_no_duplica(spark, first_run):
    cfg, _, _ = first_run
    report2 = run(spark, cfg)
    for r in report2["fuentes"]:
        total = spark.read.parquet(f"{cfg.bronze}/{r['entity']}").count()
        assert total == EXPECTED_ROWS[r["entity"]], r["entity"]
        assert r["archivos_parquet"] == 1


def test_otro_dia_agrega_particion_y_conserva_historial(spark, landing, tmp_path):
    cfg1 = _cfg(tmp_path, landing, "2026-11-12")
    cfg2 = _cfg(tmp_path, landing, "2026-11-13")
    run(spark, cfg1, ["customers_orgs"])
    run(spark, cfg2, ["customers_orgs"])
    df = spark.read.parquet(f"{cfg1.bronze}/customers_orgs")
    counts = {str(r["ingest_date"]): r["count"] for r in df.groupBy("ingest_date").count().collect()}
    assert counts == {"2026-11-12": 80, "2026-11-13": 80}


def test_filas_sucias_van_a_quarantine_con_motivo(spark, landing, tmp_path):
    """Una fecha inválida, una clave nula y una clave repetida: ninguna se pierde."""
    dirty = tmp_path / "landing"
    shutil.copytree(landing, dirty)
    with open(dirty / "customers_orgs.csv", "a", encoding="utf-8") as fh:
        fh.write("org_bad_date,X,Media,us-east,pro,True,2025-13-45,rep_a,active,organic,10.0\n")
        fh.write(",Sin Clave,Media,us-east,pro,True,2025-06-01,rep_a,active,organic,10.0\n")
        fh.write("org_xaji0y6d,Duplicada,Media,us-east,pro,True,2025-06-01,rep_a,active,organic,10.0\n")

    cfg = _cfg(tmp_path, str(dirty))
    report = run(spark, cfg, ["customers_orgs"])
    r = report["fuentes"][0]

    assert r["filas_leidas"] == 83
    assert r["filas_bronze"] == 80
    assert r["quarantine_por_regla"] == {"no_parseable": 1, "clave_nula": 1, "clave_duplicada": 1}

    q = spark.read.parquet(f"{cfg.quarantine}/customers_orgs/ingest_date={cfg.ingest_date}")
    raw = q.filter(F.col("regla") == "no_parseable").first()["raw_record"]
    assert "2025-13-45" in raw                         # se guarda la línea cruda
    assert q.filter(F.col("record_json").isNull()).count() == 0

    # Re-ejecutar sobre los mismos datos deja la quarantine igual, sin acumular.
    run(spark, cfg, ["customers_orgs"])
    q2 = spark.read.parquet(f"{cfg.quarantine}/customers_orgs/ingest_date={cfg.ingest_date}")
    assert q2.count() == 3

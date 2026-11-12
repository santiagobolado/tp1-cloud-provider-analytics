"""Pruebas del cost_anomaly_mart (D-010)."""

from datetime import date, timedelta

from src.analytics.anomalies import score_anomalies

COLS = ("org_id string, usage_date date, service string, org_name string, plan_tier string, "
        "daily_cost_usd double, daily_cost_gross_usd double, credit_adjustment_usd double, "
        "event_count long")


def _serie(org, service, values, start=date(2025, 7, 3), credit_on=None):
    rows = []
    for i, v in enumerate(values):
        credit = -5.0 if i == credit_on else 0.0
        rows.append((org, start + timedelta(days=i), service, org, "pro", v + credit, v, credit, 3))
    return rows


def _by_day(df):
    return {r["date"]: r for r in df.collect()}


def test_un_pico_en_una_serie_estable_se_detecta(spark):
    vals = [10.0 + (i % 5) * 0.4 for i in range(30)]
    vals[20] = 60.0
    out = _by_day(score_anomalies(spark.createDataFrame(_serie("o1", "compute", vals), COLS)))
    pico = out[date(2025, 7, 3) + timedelta(days=20)]
    assert pico["is_anomaly"] and pico["direction"] == "pico" and pico["baseline_source"] == "serie"
    assert sum(r["is_anomaly"] for r in out.values()) == 1


def test_lo_caro_no_es_anomalo_si_es_su_comportamiento_habitual(spark):
    """Una serie cara y estable no se marca: la línea de base es por serie, no global."""
    cara = [200.0 + (i % 3) for i in range(30)]
    barata = [1.0 + (i % 3) * 0.1 for i in range(30)]
    df = spark.createDataFrame(_serie("o1", "compute", cara) + _serie("o2", "compute", barata), COLS)
    assert score_anomalies(df).filter("is_anomaly").count() == 0


def test_serie_corta_usa_la_linea_de_base_del_servicio(spark):
    larga = [10.0 + (i % 4) for i in range(30)]
    corta = [10.0, 11.0, 12.0, 80.0]                     # 4 días < 20
    df = spark.createDataFrame(_serie("o1", "storage", larga) + _serie("o2", "storage", corta), COLS)
    out = [r for r in score_anomalies(df).collect() if r["org_id"] == "o2"]
    assert {r["baseline_source"] for r in out} == {"servicio"}
    assert [r["is_anomaly"] for r in sorted(out, key=lambda r: r["date"])] == [False, False, False, True]


def test_mad_cero_no_divide_por_cero(spark):
    plana = [5.0] * 25                                   # MAD = 0 -> cae al servicio
    otra = [20.0 + i for i in range(25)]
    df = spark.createDataFrame(_serie("o1", "genai", plana) + _serie("o2", "genai", otra), COLS)
    out = [r for r in score_anomalies(df).collect() if r["org_id"] == "o1"]
    assert {r["baseline_source"] for r in out} == {"servicio"}
    assert all(r["anomaly_score"] is not None for r in out)


def test_sin_dispersion_en_ningun_nivel_no_marca_ni_deja_nulos(spark):
    df = spark.createDataFrame(_serie("o1", "genai", [5.0] * 25), COLS)
    out = score_anomalies(df).collect()
    assert {r["baseline_source"] for r in out} == {"sin_dispersion"}
    assert all(r["is_anomaly"] is False for r in out)


def test_los_creditos_se_marcan_aparte_y_no_disparan_anomalias(spark):
    vals = [10.0 + (i % 5) * 0.4 for i in range(30)]
    out = _by_day(score_anomalies(spark.createDataFrame(
        _serie("o1", "compute", vals, credit_on=7), COLS)))
    dia = out[date(2025, 7, 3) + timedelta(days=7)]
    assert dia["has_credit_adjustment"] and not dia["is_anomaly"]

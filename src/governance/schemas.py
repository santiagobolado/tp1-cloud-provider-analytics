"""Columnas que produce cada dataset, derivadas del código del pipeline.

Se construye cada tabla sobre DataFrames vacíos con las mismas funciones que
usan los jobs, así el resultado no puede divergir de lo que el pipeline escribe.
Sirve al test de gobierno (toda columna documentada) y al diccionario de datos.
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from src.analytics.anomalies import score_anomalies
from src.ingest.batch_to_bronze import QUARANTINE_COLS
from src.ingest.stream_to_bronze import BRONZE_COLS, quarantine_stream
from src.schemas.batch import CORRUPT_COL, SOURCES
from src.schemas.events import EVENT_SCHEMA_WITH_CORRUPT
from src.serving.tables import P1_COLS, P2_COLS
from src.transform.gold import aggregate_daily_usage
from src.transform.gold_job import build_mart
from src.transform.gold_stream import speed_frame
from src.transform.silver import conform_events, conform_orgs, conform_resources, split_silver

TECH = [T.StructField("source_file", T.StringType()), T.StructField("ingest_ts", T.TimestampType()),
        T.StructField("ingest_date", T.DateType())]


def _empty(spark, fields):
    return spark.createDataFrame([], T.StructType(fields))


def expected_columns(spark: SparkSession) -> dict:
    cols = {}
    bronze = {}
    for src in SOURCES:
        fields = [f for f in src.schema.fields if f.name != CORRUPT_COL] + TECH
        bronze[src.entity] = _empty(spark, fields)
        cols[f"bronze.{src.entity}"] = bronze[src.entity].columns
        cols[f"quarantine.{src.entity}"] = QUARANTINE_COLS

    raw_events = _empty(spark, EVENT_SCHEMA_WITH_CORRUPT.fields + TECH
                        + [T.StructField("event_ts", T.TimestampType())])
    cols["bronze.usage_events"] = BRONZE_COLS
    cols["quarantine.usage_events"] = quarantine_stream(raw_events).columns

    orgs = conform_orgs(bronze["customers_orgs"], bronze["nps_surveys"])
    cols["silver.customers_orgs"] = orgs.columns
    cols["silver.resources"] = conform_resources(bronze["resources"]).columns
    bronze_events = raw_events.select(*BRONZE_COLS)
    silver, quarantine = split_silver(conform_events(bronze_events, orgs))
    cols["silver.usage_events"] = silver.columns
    cols["quarantine.silver_usage_events"] = quarantine.columns

    mart = build_mart(silver, datetime(2026, 1, 1))
    cols["gold.org_daily_usage_by_service"] = mart.columns
    cols["gold.org_daily_usage_by_service_speed"] = speed_frame(
        aggregate_daily_usage(silver), 0).columns
    cols["gold.cost_anomaly_mart"] = score_anomalies(mart).columns
    cols["cassandra.org_daily_usage_by_service"] = P1_COLS
    cols["cassandra.top_services_by_org_14d"] = P2_COLS
    return cols

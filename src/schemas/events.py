"""Esquema explícito de usage_events_stream/*.jsonl.

Es el esquema ampliado (v2): los eventos v1 llegan sin carbon_kg ni
genai_tokens y Spark los completa con nulo. `value` es String a propósito
(D-003): la fuente lo emite como número o como texto y un DoubleType
descartaría en silencio las variantes texto. El cast se hace en Silver.
"""

from pyspark.sql import types as T

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("timestamp", T.StringType()),      # ISO-8601 UTC; se parsea a event_ts
    T.StructField("org_id", T.StringType()),
    T.StructField("resource_id", T.StringType()),
    T.StructField("service", T.StringType()),
    T.StructField("region", T.StringType()),
    T.StructField("metric", T.StringType()),
    T.StructField("value", T.StringType()),          # D-003
    T.StructField("unit", T.StringType()),
    T.StructField("cost_usd_increment", T.DoubleType()),
    T.StructField("schema_version", T.IntegerType()),
    T.StructField("carbon_kg", T.DoubleType()),      # solo v2
    T.StructField("genai_tokens", T.LongType()),     # solo v2 y service = genai
])

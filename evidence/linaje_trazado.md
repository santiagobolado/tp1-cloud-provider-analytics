# Trazado de linaje · org_okep7y6w · 2025-08-07 · compute

Generado con `python notebooks/05_trazado_linaje.py`. Cada paso usa solo columnas
de linaje del pipeline; ninguna búsqueda es manual.

## 1. Gold · `cost_anomaly_mart`

| org_id | date | service | daily_cost_gross_usd | baseline_median_usd | anomaly_score | is_anomaly |
|---|---|---|---|---|---|---|
| org_okep7y6w | 2025-08-07 | compute | 191.8968 | 11.1426 | 44.374 | True |

## 2. Gold · `org_daily_usage_by_service` (el mart del que sale la anomalía)

| org_id | usage_date | service | daily_cost_usd | event_count | source | last_reconciled_at |
|---|---|---|---|---|---|---|
| org_okep7y6w | 2025-08-07 | compute | 191.8968 | 3 | reconciliacion | 2026-11-13 18:56:36 |

Esta misma fila está en AstraDB: `evidence/serving/` (Q1, primera fila) y `evidence/serving/q1_cql_console.png`.

## 3. Silver · `usage_events`: los 3 eventos que suman USD 191.8968

| event_id | event_ts | metric | value_num | cost_usd | quality_flags |
|---|---|---|---|---|---|
| evt_zjql5eyfdsd7 | 2025-08-07 01:33:00 | cpu_hours | 4.2643 | 0.4772 | [] |
| evt_jdl0otbr13dk | 2025-08-07 08:36:00 | requests | 101.0 | 182.2575 | [] |
| evt_2i7k0o0k8098 | 2025-08-07 12:39:00 | requests | 111.0 | 9.1621 | [] |

Las tres llegaron en archivos distintos: la fragmentación aleatoria de D-012 en un caso concreto.

## 4. Bronze · `usage_events`: cuándo y desde qué archivo llegó cada evento

| event_id | value | cost_usd_increment | ingest_date | ingest_ts | source_file |
|---|---|---|---|---|---|
| evt_zjql5eyfdsd7 | 4.2643 | 0.4772 | 2026-11-13 | 2026-11-13 18:52:59.859000 | data/landing/usage_events_stream/events_part_0110.jsonl |
| evt_jdl0otbr13dk | 101.0 | 182.2575 | 2026-11-13 | 2026-11-13 18:53:01.681000 | data/landing/usage_events_stream/events_part_0109.jsonl |
| evt_2i7k0o0k8098 | 111.0 | 9.1621 | 2026-11-13 | 2026-11-13 18:52:15.759000 | data/landing/usage_events_stream/events_part_0061.jsonl |

## 5. Landing: la línea exacta en el archivo original

| event_id | archivo | línea | contenido (inicio) |
|---|---|---|---|
| evt_zjql5eyfdsd7 | `events_part_0110.jsonl` | 206 | `{"event_id": "evt_zjql5eyfdsd7", "timestamp": "2025-08-07T01:33:00Z", "org_id": "org_okep7y6w", "resource_id":…` |
| evt_jdl0otbr13dk | `events_part_0109.jsonl` | 48 | `{"event_id": "evt_jdl0otbr13dk", "timestamp": "2025-08-07T08:36:00Z", "org_id": "org_okep7y6w", "resource_id":…` |
| evt_2i7k0o0k8098 | `events_part_0061.jsonl` | 93 | `{"event_id": "evt_2i7k0o0k8098", "timestamp": "2025-08-07T12:39:00Z", "org_id": "org_okep7y6w", "resource_id":…` |

**Lectura:** la anomalía la explica el evento `evt_jdl0otbr13dk` (USD 182.26), uno de los picos inyectados en el dataset.

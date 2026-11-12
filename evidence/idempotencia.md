# Idempotencia · pipeline completo ejecutado dos veces

Reinicio limpio (`rm -rf data/bronze data/silver data/gold data/quarantine _checkpoints`) y dos corridas
consecutivas de todos los jobs, sin cambios en Landing. Fuente: `evidence/runs/*.json`.

| Tabla | Corrida 1 | Corrida 2 | Detalle (1 / 2) |
|---|---|---|---|
| Bronze `customers_orgs` | 80 | 80 | 1 / 1 |
| Bronze `users` | 800 | 800 | 1 / 1 |
| Bronze `resources` | 400 | 400 | 1 / 1 |
| Bronze `support_tickets` | 1.000 | 1.000 | 1 / 1 |
| Bronze `marketing_touches` | 1.500 | 1.500 | 1 / 1 |
| Bronze `nps_surveys` | 92 | 92 | 1 / 1 |
| Bronze `billing_monthly` | 240 | 240 | 1 / 1 |
| Bronze `usage_events` (acumulado) | 43.200 | 43.200 | lotes procesados: 120 / 0 |
| Silver `usage_events` | 43.200 | 43.200 | 360 / 360 archivos |
| Gold capa rápida (claves vigentes) | 2.064 | 2.064 | lotes procesados: 120 / 0 |
| Gold `org_daily_usage_by_service` | 11.050 | 11.050 | USD 147.433,98 / 147.433,98 |
| Gold `cost_anomaly_mart` (anomalías) | 130 | 130 | 11.050 / 11.050 celdas |

**Variación: 0 en todas las tablas** (objetivo O4).

Mecanismo por capa: Bronze batch, overwrite dinámico por `ingest_date` (D-011); Bronze streaming,
checkpoint + dedupe por `event_id` (D-013); Silver y Gold, recomputación con overwrite; capa rápida,
checkpoint + `foreachBatch` que reescribe su `batch_id` (D-015). Serving: upsert por clave primaria (pendiente de AstraDB).

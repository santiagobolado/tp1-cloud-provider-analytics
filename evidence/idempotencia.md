# Idempotencia · pipeline completo ejecutado dos veces

Entorno de referencia: Google Colab (`notebooks/00_pipeline_colab.ipynb`, 13/11/2026). Reinicio limpio
de las zonas derivadas y los checkpoints, y dos corridas consecutivas de cada job sin cambios en Landing.
La carga a AstraDB también se ejecutó dos veces. Fuente: `evidence/runs/*.json`.

| Tabla | Corrida 1 | Corrida 2 | Detalle (1 / 2) |
|---|---|---|---|
| Bronze `customers_orgs` | 80 | 80 | 1 / 1 archivos |
| Bronze `users` | 800 | 800 | 1 / 1 archivos |
| Bronze `resources` | 400 | 400 | 1 / 1 archivos |
| Bronze `support_tickets` | 1.000 | 1.000 | 1 / 1 archivos |
| Bronze `marketing_touches` | 1.500 | 1.500 | 1 / 1 archivos |
| Bronze `nps_surveys` | 92 | 92 | 1 / 1 archivos |
| Bronze `billing_monthly` | 240 | 240 | 1 / 1 archivos |
| Bronze `usage_events` (acumulado) | 43.200 | 43.200 | micro-lotes procesados: 120 / 0 |
| Silver `usage_events` | 43.200 | 43.200 | 360 / 360 archivos |
| Gold `org_daily_usage_by_service` | 11.050 | 11.050 | USD 147.433,98 / 147.433,98 |
| AstraDB `org_daily_usage_by_service` (COUNT) | 11.050 | 11.050 | 2.ª carga: 11.050 filas reenviadas, COUNT 11.050 → 11.050 |
| AstraDB `top_services_by_org_14d` (COUNT) | 12.314 | 12.314 | 2.ª carga: 12.314 filas reenviadas, COUNT 12.314 → 12.314 |

**Variación: 0 en las 12 tablas** (objetivo O4).

Mecanismo por capa: Bronze batch, overwrite dinámico por `ingest_date` (D-011); Bronze streaming,
checkpoint + dedupe por `event_id` (D-013); Silver y Gold, recomputación con overwrite; capa rápida,
checkpoint + `foreachBatch` que reescribe su `batch_id` (D-015); AstraDB, upsert por clave primaria (D-017).

**Reproducibilidad (O7).** Las cifras de Colab coinciden exactamente con las del entorno de desarrollo
(43.200 eventos, 11.050 filas Gold, USD 147.433,98, 130 anomalías): el resultado no depende del entorno.

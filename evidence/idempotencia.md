# Idempotencia · pipeline completo ejecutado dos veces

Entorno de referencia: Google Colab (`notebooks/00_pipeline_colab.ipynb`, rama `entrega-final`,
18/11/2026). Reinicio limpio de las zonas derivadas y los checkpoints, y dos corridas consecutivas de
cada job sin cambios en Landing. La carga a AstraDB también se ejecutó dos veces.
Fuente: `evidence/runs/*_2026-11-18_*.json`.

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
| Silver `billing_monthly` | 240 | 240 | |
| Silver `support_tickets` | 1.000 | 1.000 | |
| Silver `users` | 800 | 800 | |
| Silver `marketing_touches` | 1.500 | 1.500 | |
| Gold `org_daily_usage_by_service` | 11.050 | 11.050 | USD 147.433,98 / 147.433,98 |
| Gold `revenue_by_org_month` | 240 | 240 | revenue neto USD 132.074,90 / 132.074,90 |
| Gold `tickets_by_org_date` | 984 | 984 | 1.000 tickets / 1.000 |
| Gold `genai_tokens_by_org_date` | 848 | 848 | 2.558.359 tokens / 2.558.359 |
| AstraDB `org_daily_usage_by_service` (COUNT) | 11.050 | 11.050 | 2.ª carga: 11.050 filas reenviadas |
| AstraDB `top_services_by_org_14d` (COUNT) | 12.314 | 12.314 | 2.ª carga: 12.314 filas reenviadas |
| AstraDB `tickets_by_severity_date` (COUNT) | 575 | 575 | 2.ª carga: 575 filas reenviadas |
| AstraDB `revenue_by_org_month` (COUNT) | 243 | 243 | 2.ª carga: 243 filas reenviadas |
| AstraDB `genai_tokens_by_org_date` (COUNT) | 893 | 893 | 2.ª carga: 893 filas reenviadas |

**Variación: 0 en las 23 tablas** (objetivo O4).

Mecanismo por capa: Bronze batch, overwrite dinámico por `ingest_date` (D-011); Bronze streaming,
checkpoint + dedupe por `event_id` (D-013); Silver y Gold, recomputación con overwrite (D-022, D-023);
capa rápida, checkpoint + `foreachBatch` que reescribe su `batch_id` (D-015); AstraDB, upsert por
clave primaria (D-017, D-024).

**Reproducibilidad (O7).** El 18/11 el pipeline también corrió desde cero en el entorno de desarrollo
(`python -m src.pipeline --reset`, 363 s, 64 tests OK). Todas las cifras coinciden con las de Colab:
43.200 eventos, 11.050 filas Gold, USD 147.433,98, 130 anomalías, los tres marts de dominio y la capa
rápida (2.067 claves, USD 22.251,20, 36.807 eventos tardíos descartados, 85,2%).

**Sobre la capa rápida.** En la 2.ª entrega (13/11) un clon nuevo dio una diferencia de ±0,1% en la
capa rápida (2.064 contra 2.067 claves): Structured Streaming toma los archivos según su fecha de
modificación, que cambia con cada clon, y el watermark descarta según ese orden. Esta vez coincidieron,
pero la diferencia puede volver a aparecer y es esperable: la cifra oficial es la del mart reconciliado,
que no varía (D-004). Dentro de un mismo entorno, re-ejecutar no cambia nada (checkpoint).

# Checklist — 2.ª entrega (16/11/2026, 18:30)

Estado al 13/11, 15:40. ✅ hecho · 🟡 parcial · ⬜ pendiente · ⛔ bloqueado

## Contra el checklist 9.2 de la consigna

| # | Ítem de la consigna | Estado | Evidencia / qué falta |
|---|---|---|---|
| 1 | Correcciones de la 1.ª entrega incorporadas | 🟡 | `docs/plan_correcciones.md`: C-02, C-03, C-04 hechas; faltan C-01 (documento corto), C-05 (diagrama), C-06 (cifras de volumen) |
| 2 | Arquitectura actualizada | ⬜ | Diagrama v2 al final, cuando el pipeline esté completo |
| 3 | Tres maestros batch en Bronze | ✅ | Las 7 fuentes. `src/ingest/batch_to_bronze.py`, D-011, `evidence/runs/batch_to_bronze_*` |
| 4 | Streaming con esquema, watermark, dedupe, late data y checkpoint | ✅ | Bronze (D-013) + agregación con watermark de 7 días: 36.808 tardíos medidos y recuperados por la reconciliación (D-015) |
| 5 | Silver para eventos y al menos un maestro | ✅ | Eventos + `customers_orgs` + `resources`; join con organizaciones. `src/transform/`, D-014 |
| 6 | Tres features calculadas | ✅ | `requests`, `cpu_hours`, `storage_gb_hours`, `cost_usd`, `genai_tokens`, `carbon_kg` en Silver; `daily_cost_usd` en Gold |
| 7 | Tres reglas de calidad y quarantine | ✅ | Las 3 de la consigna + 4 más: `event_id` (Bronze), `costo_negativo` 211, `unit_nulo_con_value` 2.038, `value_nulo` 877. Quarantine en Bronze y Silver |
| 8 | Mart `org_daily_usage_by_service` en Gold | ✅ | 11.050 filas, grano único, cuadra con Silver. Streaming + reconciliación (D-015) |
| 9 | Keyspace y tabla Cassandra/AstraDB poblada | ✅ | AstraDB `cloud_analytics` (AWS us-east-2): `org_daily_usage_by_service` 11.050 filas, `top_services_by_org_14d` 12.314 (D-017, `evidence/runs/load_cassandra_*`) |
| 10 | Dos consultas CQL con resultados | ✅ | Q1: 25 filas en 39 ms; Q2: Top-3 en 38 ms; coinciden con Gold. CQL, salidas y capturas de la CQL Console en `evidence/serving/` |
| 11 | Idempotencia demostrada | ✅ | Pipeline y carga a AstraDB ejecutados dos veces en Colab: variación 0 en 12 tablas (`evidence/idempotencia.md`) |
| 12 | README Quickstart, logs y evidencias | ✅ | Notebook de Colab validado desde entorno limpio, mismas cifras que el desarrollo (O7); logs JSON por corrida; 42 tests |
| 13 | Backlog final priorizado | ⬜ | Paso 10 |

Además, de la tabla §6.2: **Analítica o ML** ✅ `cost_anomaly_mart`: 130 anomalías, 54/54 picos inyectados detectados (D-016) · **Gobierno preliminar** ⬜ (paso 9).

## Contra el plan por pasos

| Paso | Componente | Día previsto | Estado |
|---|---|---|---|
| 0 | Fundación del repo, config, plan de correcciones | Jue 12 | ✅ |
| 1 | Batch → Bronze | Jue 12 | ✅ |
| 2 | Streaming → Bronze | Jue–Vie | ✅ (adelantado) + A-07 cerrada |
| 3 | Silver: eventos + maestros, v1/v2, features | Vie 13 | ✅ (adelantado) |
| 4 | Calidad y quarantine | con 1–3 | ✅ |
| 5 | Gold `org_daily_usage_by_service` (streaming + reconciliación) | Vie–Sáb | ✅ (adelantado) |
| 6 | AstraDB: keyspace, tablas, carga, 2 consultas | Sáb 14 | ✅ (adelantado) |
| 7 | Anomalías con MAD | Sáb 14 | ✅ (adelantado) |
| 8 | Idempotencia end-to-end | Dom 15 | ✅ (adelantado) |
| 9 | Gobierno, pruebas, logs | Dom 15 | 🟡 |
| 10 | Documento (≤12 págs.) + anexos + diagrama v2 + backlog | Dom–Lun | ⬜ |

**Ruta crítica:** el documento de 10–12 páginas con anexos (C-01), que responde al
feedback del profesor. El pipeline está completo y verificado en AstraDB.

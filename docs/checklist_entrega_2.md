# Checklist — 2.ª entrega (16/11/2026, 18:30)

Estado al 12/11, 01:10. ✅ hecho · 🟡 parcial · ⬜ pendiente · ⛔ bloqueado

## Contra el checklist 9.2 de la consigna

| # | Ítem de la consigna | Estado | Evidencia / qué falta |
|---|---|---|---|
| 1 | Correcciones de la 1.ª entrega incorporadas | 🟡 | `docs/plan_correcciones.md`: C-02, C-03, C-04 hechas; faltan C-01 (documento corto), C-05 (diagrama), C-06 (cifras de volumen) |
| 2 | Arquitectura actualizada | ⬜ | Diagrama v2 al final, cuando el pipeline esté completo |
| 3 | Tres maestros batch en Bronze | ✅ | Las 7 fuentes. `src/ingest/batch_to_bronze.py`, D-011, `evidence/runs/batch_to_bronze_*` |
| 4 | Streaming con esquema, watermark, dedupe, late data y checkpoint | 🟡 | Bronze listo (D-013, `evidence/runs/stream_to_bronze_*`). Falta medir los tardíos en la agregación del mart (paso 5) |
| 5 | Silver para eventos y al menos un maestro | ✅ | Eventos + `customers_orgs` + `resources`; join con organizaciones. `src/transform/`, D-014 |
| 6 | Tres features calculadas | ✅ | `requests`, `cpu_hours`, `storage_gb_hours`, `cost_usd`, `genai_tokens`, `carbon_kg` en Silver; `daily_cost_usd` en Gold |
| 7 | Tres reglas de calidad y quarantine | ✅ | Las 3 de la consigna + 4 más: `event_id` (Bronze), `costo_negativo` 211, `unit_nulo_con_value` 2.038, `value_nulo` 877. Quarantine en Bronze y Silver |
| 8 | Mart `org_daily_usage_by_service` en Gold | ⬜ | Paso 5 |
| 9 | Keyspace y tabla Cassandra/AstraDB poblada | ⛔ | **Falta crear la base en AstraDB** (paso 6) |
| 10 | Dos consultas CQL con resultados | ⛔ | Depende del 9 |
| 11 | Idempotencia demostrada | 🟡 | Batch, streaming y Silver probados con tests y doble corrida. Falta Gold y serving (paso 8) |
| 12 | README Quickstart, logs y evidencias | 🟡 | Quickstart de los pasos 1 a 3; logs JSON por corrida; 25 tests |
| 13 | Backlog final priorizado | ⬜ | Paso 10 |

Además, de la tabla §6.2: **Analítica o ML** ⬜ (paso 7, MAD de D-010) · **Gobierno preliminar** ⬜ (paso 9).

## Contra el plan por pasos

| Paso | Componente | Día previsto | Estado |
|---|---|---|---|
| 0 | Fundación del repo, config, plan de correcciones | Jue 12 | ✅ |
| 1 | Batch → Bronze | Jue 12 | ✅ |
| 2 | Streaming → Bronze | Jue–Vie | ✅ (adelantado) + A-07 cerrada |
| 3 | Silver: eventos + maestros, v1/v2, features | Vie 13 | ✅ (adelantado) |
| 4 | Calidad y quarantine | con 1–3 | ✅ |
| 5 | Gold `org_daily_usage_by_service` (streaming + reconciliación) | Vie–Sáb | ⬜ |
| 6 | AstraDB: keyspace, tablas, carga, 2 consultas | Sáb 14 | ⛔ base sin crear |
| 7 | Anomalías con MAD | Sáb 14 | ⬜ |
| 8 | Idempotencia end-to-end | Dom 15 | 🟡 |
| 9 | Gobierno, pruebas, logs | Dom 15 | 🟡 |
| 10 | Documento (≤12 págs.) + anexos + diagrama v2 + backlog | Dom–Lun | ⬜ |

**Ruta crítica:** AstraDB. Es lo único que depende de una acción externa y bloquea
dos ítems obligatorios (9 y 10).

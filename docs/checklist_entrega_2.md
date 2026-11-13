# Checklist — 2.ª entrega (16/11/2026, 18:30)

Estado al 13/11 (chequeo final). ✅ hecho · 🟡 parcial · ⬜ pendiente · ⛔ bloqueado

## Contra el checklist 9.2 de la consigna

| # | Ítem de la consigna | Estado | Evidencia / qué falta |
|---|---|---|---|
| 1 | Correcciones de la 1.ª entrega incorporadas | ✅ | C-01 a C-06 hechas: documento de 9 páginas + anexos A–E separados; cifras del TP1 corregidas (`docs/plan_correcciones.md`) |
| 2 | Arquitectura actualizada | ✅ | `docs/diagrama_arquitectura_v2`: solo lo implementado, lo pendiente rotulado como tal; tabla de cambios respecto de la v1 |
| 3 | Tres maestros batch en Bronze | ✅ | Las 7 fuentes. `src/ingest/batch_to_bronze.py`, D-011, `evidence/runs/batch_to_bronze_*` |
| 4 | Streaming con esquema, watermark, dedupe, late data y checkpoint | ✅ | Bronze (D-013) + agregación con watermark de 7 días: 36.808 tardíos medidos y recuperados por la reconciliación (D-015) |
| 5 | Silver para eventos y al menos un maestro | ✅ | Eventos + `customers_orgs` + `resources`; join con organizaciones. `src/transform/`, D-014 |
| 6 | Tres features calculadas | ✅ | `requests`, `cpu_hours`, `storage_gb_hours`, `cost_usd`, `genai_tokens`, `carbon_kg` en Silver; `daily_cost_usd` en Gold |
| 7 | Tres reglas de calidad y quarantine | ✅ | Las 3 de la consigna + 4 más. Muestras por regla en `evidence/quarantine_muestras.md` (Silver real + Bronze con filas sucias inyectadas) |
| 8 | Mart `org_daily_usage_by_service` en Gold | ✅ | 11.050 filas, grano único, cuadra con Silver. Streaming + reconciliación (D-015) |
| 9 | Keyspace y tabla Cassandra/AstraDB poblada | ✅ | AstraDB `cloud_analytics` (AWS us-east-2): `org_daily_usage_by_service` 11.050 filas, `top_services_by_org_14d` 12.314 (D-017, `evidence/runs/load_cassandra_*`) |
| 10 | Dos consultas CQL con resultados | ✅ | Q1: 25 filas en 39 ms; Q2: Top-3 en 38 ms; coinciden con Gold. CQL, salidas y capturas de la CQL Console en `evidence/serving/` |
| 11 | Idempotencia demostrada | ✅ | Pipeline y carga a AstraDB ejecutados dos veces en Colab: variación 0 en 12 tablas (`evidence/idempotencia.md`) |
| 12 | README Quickstart, logs y evidencias | ✅ | Colab validado desde entorno limpio (O7); `python -m src.pipeline --reset` corre todo con un comando; logs JSON por corrida; 47 tests |
| 13 | Backlog final priorizado | ✅ | `docs/backlog_final.md`: 13 obligatorios (80 h), 6 deseables (25 h), 7 fuera de alcance; congelamiento el 30/11 |

Además, de la tabla §6.2: **Analítica o ML** ✅ `cost_anomaly_mart`: 130 anomalías, 54/54 picos inyectados detectados (D-016) · **Gobierno preliminar** ✅ `docs/gobierno.md`: responsables, catálogo con control automático, diccionario (292 columnas), linaje trazado de punta a punta, controles y acceso.

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
| 9 | Gobierno, pruebas, logs | Dom 15 | ✅ (adelantado) |
| 10 | Documento (≤12 págs.) + anexos + diagrama v2 + backlog | Dom–Lun | ✅ (adelantado) |

**Ruta crítica:** el documento de 10–12 páginas con anexos (C-01), que responde al
feedback del profesor. El pipeline está completo y verificado en AstraDB.

## Auditoría general (13/11)

Revisión contra §6.2–§6.5 y §8 de la consigna y contra el feedback de la 1.ª entrega.

| # | Hallazgo | Estado |
|---|---|---|
| 1 | Muestras de quarantine no versionadas (`data/` está en `.gitignore`) | ✅ `evidence/quarantine_muestras.md`, regenerable con `notebooks/03_muestras_quarantine.py` |
| 2 | README con 4 enlaces a archivos borrados de la 1.ª entrega | ✅ corregido |
| 3 | README instalaba PySpark sin versión y no advertía sobre Windows | ✅ `requirements.txt` y nota de entorno |
| 4 | D-004 y D-008 seguían "pendientes de A-07" | ✅ referencia a D-012 |
| 5 | Sin un comando único para todo el pipeline | ✅ `python -m src.pipeline` |
| 6 | Dedupe de Bronze conservaba la fila por orden de contenido (podía aislar el original) | ✅ conserva la primera aparición; test de regresión |
| 7 | `main` 18 commits detrás de la rama de trabajo | ⬜ unir antes de la entrega, etiquetar `entrega-2` y apuntar el notebook a `main` |
| 8 | Clon limpio (`78becbf`): pipeline 6 min 32 s, 47 tests OK; Bronze, Silver, Gold y anomalías idénticos a Colab | ✅ |
| 9 | El documento decía "cifras idénticas" también para la capa rápida, que varía ±0,1% según el orden de los micro-lotes | ✅ redacción precisada en §8, tabla O7 y `evidence/idempotencia.md` |
| 10 | Barrido de secretos: sin tokens, bundles ni `.env` versionados (solo `AstraCS:fake` en un test) | ✅ |

## Contra §6.4 (criterios de aceptación)

| Criterio | Estado |
|---|---|
| Batch y streaming corren con los datos provistos | ✅ 7 fuentes + 120 archivos |
| Calidad y quarantine efectivas, con ejemplos verificables | ✅ `evidence/quarantine_muestras.md` |
| Mart FinOps en Gold y tabla AstraDB poblada | ✅ 11.050 filas en ambos |
| Dos consultas sobre AstraDB con resultados | ✅ capturas de la CQL Console |
| La re-ejecución no duplica | ✅ variación 0 en 12 tablas |
| Diagrama = lo implementado | ✅ v2, lo pendiente rotulado |
| README ejecutable desde entorno limpio | ✅ Colab y clon limpio |


# Gobierno preliminar · 2.ª entrega

Consigna §6.2: controles iniciales, metadatos, linaje, responsabilidades, acceso y
evidencias. Cada punto remite a algo ejecutable o verificable en el repositorio.

## 1. Responsabilidades

Roles del TP1 (§12.1), cada uno con un suplente. El responsable de un dataset
responde por su contenido, sus reglas y su evidencia.

| Rol | Es dueño de | Suplente |
|---|---|---|
| R1 Arquitectura y gobierno | Catálogo (`src/governance/catalog.py`), `DECISIONS.md`, diagrama, este documento | R4 |
| R2 Ingesta y calidad | Bronze, las tres zonas de quarantine, reglas de Landing → Bronze y Bronze → Silver | R3 |
| R3 Procesamiento y analítica | Silver, los marts de Gold, `cost_anomaly_mart` | R2 |
| R4 Serving y reproducibilidad | Tablas de AstraDB, credenciales, notebook de Colab, `src/pipeline.py` | R1 |

El dueño de cada uno de los 25 datasets está en el diccionario de datos.

## 2. Metadatos

**Catálogo como código.** `src/governance/catalog.py` describe cada dataset
(zona, ruta, grano, partición, responsable, origen, retención) y cada columna.
`docs/diccionario_datos.md` se genera desde ahí: 25 datasets, 292 columnas.

**Control automático.** `tests/test_governance.py` arma cada tabla con las mismas
funciones del pipeline y **falla si alguna columna no tiene descripción** o si el
catálogo y el pipeline no tienen los mismos datasets. Verificado: al borrar la
descripción de `silver_ts`, el test falla y la nombra.

**Columnas técnicas por zona.**

| Zona | Columnas | Para qué |
|---|---|---|
| Bronze | `source_file`, `ingest_ts`, `ingest_date` | De qué archivo y en qué corrida llegó cada fila |
| Silver | las de Bronze + `silver_ts`, `quality_flags` | Cuándo se conformó y qué reglas marcó |
| Gold | `source`, `last_reconciled_at` · capa rápida: `batch_id`, `processed_at` | Valor provisorio o consolidado, y de qué corrida (§6.7) |
| Quarantine | `entity`, `regla`, `detalle`, `record_json`, `raw_record` | Qué se aisló, por qué, y el dato tal como llegó |
| Cassandra | `source`, `last_reconciled_at`, `loaded_at` | Lo mismo en el serving, más la hora de carga |

## 3. Linaje

**Por dataset.**

```
landing/*.csv ──batch_to_bronze──▶ bronze.<fuente> ──silver_job──▶ silver.customers_orgs, silver.resources
                                                                          │ (join)
landing/usage_events_stream ──stream_to_bronze──▶ bronze.usage_events ──silver_job──▶ silver.usage_events
                                     │                                                        │
                                     └──gold_stream (flujo C)──▶ gold.…_speed                  └──gold_job (flujo D)──▶ gold.org_daily_usage_by_service
                                                                  │                                     │                 │
                                                                  └──────────load_cassandra─────────────┴──▶ AstraDB (P1, P2)
                                                                                                         └──anomalies──▶ gold.cost_anomaly_mart
```

**Por fila.** `evidence/linaje_trazado.md` toma la anomalía `org_okep7y6w · 2025-08-07
· compute` (USD 191,90, score 44,4) y la sigue con las columnas de linaje: mart →
3 eventos de Silver → Bronze, con su `ingest_ts` y `source_file` → la línea exacta de
cada evento en Landing. El pico lo explica `evt_jdl0otbr13dk` (USD 182,26), en
`events_part_0109.jsonl`, línea 48. Se regenera con `notebooks/05_trazado_linaje.py`.

## 4. Controles de calidad

Dos tipos: los que **aíslan o marcan** registros y los que **detienen el job** si
algo no cuadra. Ningún registro se descarta en silencio (§8.1, principio 5).

| Frontera | Control | Acción | Resultado en el dataset |
|---|---|---|---|
| Landing → Bronze | Esquema explícito; fila no parseable | quarantine `no_parseable` | 0 |
| | Clave natural nula / repetida | quarantine `clave_nula` / `clave_duplicada` | 0 / 0 |
| | `event_id` nulo, timestamp inválido | quarantine | 0 |
| | Re-entrega del mismo `event_id` (streaming) | dedupe con watermark sobre `ingest_ts` | probado con re-entrega |
| | **Conservación**: leídas = Bronze + quarantine | detiene el job | se cumple en las 8 fuentes |
| Bronze → Silver | `value` nulo o no numérico | métrica nula, costo promueve, copia a quarantine | 877 |
| | `unit` nulo con `value` | unidad imputada, flag, copia | 2.038 |
| | `cost_usd_increment < -0,01` | flag `is_credit_adjustment` | 211 |
| | `genai_tokens` ⇔ v2 ∧ genai; `org_id` sin dimensión; duplicado | flag + copia | 0 |
| | **Conservación**: Bronze = Silver + duplicados | detiene el job | 43.200 = 43.200 + 0 |
| Silver → Gold | **Grano único** y **cuadre** de eventos y costo con Silver | detiene el job | 11.050 claves; USD 147.433,98 |
| Gold → Cassandra | `COUNT(*)` antes y después de cada carga | evidencia de idempotencia | 11.050 / 12.314, sin cambios |
| Landing | Inmutabilidad (huella SHA-256) | test | sin cambios |

Muestras por regla: `evidence/quarantine_muestras.md`.

## 5. Acceso y seguridad

| Recurso | Lee | Escribe |
|---|---|---|
| Landing | todos los jobs | nadie (inmutable; lo verifica un test) |
| Bronze / Quarantine | jobs de Silver; R2 para auditoría | solo `batch_to_bronze` y `stream_to_bronze` |
| Silver / Gold | jobs siguientes; analistas (exploración ad hoc) | solo el job que produce cada dataset |
| AstraDB | consumo (BI, consultas Q1 y Q2) | solo `load_cassandra` |

- **Credenciales.** Solo por variables de entorno (`config/.env.example`) o Colab
  Secrets. El `.gitignore` excluye `.env`, `*secure-connect*.zip` y `*.pem`.
  `tests/test_governance.py` revisa que ningún archivo versionado contenga un token
  de AstraDB, una clave privada o credenciales de AWS.
- **Mínimo privilegio en AstraDB.** La carga usa un token *Database Administrator*,
  porque crea las tablas. El consumo debería usar un token de solo lectura, distinto.
  Queda en el backlog para la entrega final.
- **Datos personales.** `users.email` es el único dato personal del dataset. Llega
  solo a Bronze: Silver, Gold y el serving no lo incluyen.
- **Límite del entorno.** En Colab el lake vive en el disco de la sesión, sin
  permisos por usuario. La matriz de arriba se cumple por construcción del código,
  no por controles del sistema de archivos. En producción, con S3, GCS o ADLS, se
  aplicaría con políticas de IAM por zona.

## 6. Observabilidad

Cada job escribe `evidence/runs/<job>_<fecha>_<hora>.json` con:
- filas leídas, escritas y rechazadas por regla;
- rutas, archivos Parquet y bytes;
- duración;
- en streaming: micro-lotes, tardíos y latencia por lote.

Los 47 tests y la doble ejecución (`evidence/idempotencia.md`) completan la evidencia.

## 7. Retención

Definida en el TP1 (§8.5). **No se puede ejercitar con 4 meses de datos**; queda
declarada en el diccionario por dataset:

| Zona | Retención |
|---|---|
| Landing | indefinida |
| Bronze y Silver | 24 meses |
| Gold | 36 meses |
| Quarantine | 6 meses |
| Checkpoints y capa rápida | vida del job |

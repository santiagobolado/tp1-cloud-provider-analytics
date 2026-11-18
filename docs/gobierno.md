# Gobierno · versión final

Consigna §7.2 punto 7: calidad, quarantine, metadatos, linaje, responsabilidades,
seguridad y observabilidad. Cada punto remite a algo ejecutable o verificable en
el repositorio. Versión preliminar: 2.ª entrega (16/11); esta versión suma los
maestros de dominio, los tres marts nuevos, Q3 a Q5 y el token de solo lectura.

## 1. Responsabilidades

Roles del TP1 (§12.1), cada uno con un suplente. El responsable de un dataset
responde por su contenido, sus reglas y su evidencia.

| Rol | Es dueño de | Suplente |
|---|---|---|
| R1 Arquitectura y gobierno | Catálogo (`src/governance/catalog.py`), `DECISIONS.md`, diagrama, este documento | R4 |
| R2 Ingesta y calidad | Bronze, las tres zonas de quarantine, reglas de Landing → Bronze y Bronze → Silver | R3 |
| R3 Procesamiento y analítica | Silver, los marts de Gold, `cost_anomaly_mart` | R2 |
| R4 Serving y reproducibilidad | Tablas de AstraDB, credenciales, notebook de Colab, `src/pipeline.py` | R1 |

El dueño de cada uno de los 39 datasets está en el diccionario de datos.

## 2. Metadatos

**Catálogo como código.** `src/governance/catalog.py` describe cada dataset
(zona, ruta, grano, partición, responsable, origen, retención) y cada columna.
`docs/diccionario_datos.md` se genera desde ahí: 39 datasets, 447 columnas.

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
landing/*.csv ──batch_to_bronze──▶ bronze.<fuente> ──silver_job──▶ silver.customers_orgs ─┐ (join en todas las Silver)
                                                                ├─▶ silver.resources       │
                                                                ├─▶ silver.billing_monthly ─┼─gold_domains_job─▶ gold.revenue_by_org_month ──┐
                                                                ├─▶ silver.support_tickets ─┼─gold_domains_job─▶ gold.tickets_by_org_date ───┤
                                                                └─▶ silver.users, silver.marketing_touches (invariantes, D-022)              │
landing/usage_events_stream ──stream_to_bronze──▶ bronze.usage_events ──silver_job──▶ silver.usage_events ─gold_domains_job─▶ gold.genai_… ┤
                                     │                                                        │                                             │
                                     └──gold_stream (flujo C)──▶ gold.…_speed                  └──gold_job (flujo D)──▶ gold.org_daily_usage_by_service
                                                                  │                                     │                 │                 │
                                                                  └──────────load_cassandra─────────────┴─────────────────┴────────────────┴─▶ AstraDB (Q1 a Q5)
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
| Bronze → Silver (maestros, D-022) | CSAT fuera de 1-5 | CSAT de consumo nulo, flag, copia | 40 |
| | `last_login < created_at`; `converted` sin `clicked` | flag + copia (invariantes del TP1) | 232; 96 |
| | `resolved_at < created_at`; `org_id` sin dimensión | flag + copia | 0; 0 |
| | FX ruidoso en USD; nota de crédito; impuesto con signo opuesto | flag de negocio, sin copia | 160; 13; 13 |
| | **Conservación**: Bronze = Silver en cada maestro | detiene el job | 240 / 1.000 / 800 / 1.500 |
| Silver → Gold | **Grano único** y **cuadre** de eventos y costo con Silver | detiene el job | 11.050 claves; USD 147.433,98 |
| | Marts de dominio (D-023): grano único y cuadre de facturas, revenue, tickets, SLA, CSAT, tokens y costo | detiene el job | 240 / 984 / 848 filas |
| Gold → Cassandra | `COUNT(*)` antes y después de cada carga | evidencia de idempotencia | 5 tablas sin cambios en la 2.ª carga |
| Landing | Inmutabilidad (huella SHA-256) | test | sin cambios |

Muestras por regla: `evidence/quarantine_muestras.md`.

## 5. Acceso y seguridad

| Recurso | Lee | Escribe |
|---|---|---|
| Landing | todos los jobs | nadie (inmutable; lo verifica un test) |
| Bronze / Quarantine | jobs de Silver; R2 para auditoría | solo `batch_to_bronze` y `stream_to_bronze` |
| Silver / Gold | jobs siguientes; analistas (exploración ad hoc) | solo el job que produce cada dataset |
| AstraDB | consumo (BI, consultas Q1 a Q5), con token de solo lectura | solo `load_cassandra`, con token de administrador |

- **Credenciales.** Solo por variables de entorno (`config/.env.example`) o Colab
  Secrets. El `.gitignore` excluye `.env`, `*secure-connect*.zip` y `*.pem`.
  `tests/test_governance.py` revisa que ningún archivo versionado contenga un token
  de AstraDB, una clave privada o credenciales de AWS.
- **Mínimo privilegio en AstraDB (D-025).** La carga usa un token *Database
  Administrator*, porque crea las tablas. Las consultas usan otro token, con rol
  *Read Only User* (`ASTRA_DB_TOKEN_READONLY`). `run_queries` lo verifica en cada
  corrida: intenta un borrado inocuo y registra la respuesta. Evidencia del 18/11:
  "Token usado: solo_lectura · ¿puede escribir? no (Unauthorized)"
  (`evidence/serving/consultas_2026-11-18_194849.md`).
- **Datos personales.** `users.email` es el único dato personal del dataset. Llega
  solo a Bronze: `silver.users` lo excluye a propósito (minimización, D-022) y ni
  Gold ni el serving lo incluyen.
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

Además, cada consulta de serving registra su latencia (Q1 a Q5: 37 a 41 ms en AstraDB)
y el token usado. Los 64 tests y la doble ejecución (`evidence/idempotencia.md`, variación
0 en 23 tablas) completan la evidencia.

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
